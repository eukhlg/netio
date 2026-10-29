//go:build linux

package udptransport

import (
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmsghdr is struct mmsghdr; Go pads it like C.
type mmsghdr struct {
	hdr unix.Msghdr
	n   uint32
}

// receiver reads batches with recvmmsg directly, so a datagram costs no
// allocation: x/net parses every source address into a new *net.UDPAddr.
// Addresses come from a per-receiver cache instead and are shared between
// datagrams from one peer; callers must treat Message.Peer as read-only.
type receiver struct {
	raw   syscall.RawConn
	hdrs  []mmsghdr
	iovs  []unix.Iovec
	names []unix.RawSockaddrAny
	peers map[netip.AddrPort]*net.UDPAddr
	// One call's state, so the RawConn callback is a method value built once
	// instead of a closure allocated per read.
	call     func(uintptr) bool
	count, n int
	errno    syscall.Errno
}

// peerCacheLimit bounds the cache; it is cleared when full.
const peerCacheLimit = 1024

func newReceiver(conn net.PacketConn, size int) *receiver {
	udp, ok := conn.(*net.UDPConn)
	if !ok || udp.RemoteAddr() != nil {
		return nil
	}
	raw, err := udp.SyscallConn()
	if err != nil {
		return nil
	}
	r := &receiver{raw: raw, hdrs: make([]mmsghdr, size), iovs: make([]unix.Iovec, size), names: make([]unix.RawSockaddrAny, size), peers: make(map[netip.AddrPort]*net.UDPAddr)}
	r.call = r.recvmmsg
	return r
}

func (r *receiver) recvmmsg(fd uintptr) bool {
	r1, _, e := unix.Syscall6(unix.SYS_RECVMMSG, fd, uintptr(unsafe.Pointer(&r.hdrs[0])), uintptr(r.count), 0, 0, 0)
	if e == unix.EAGAIN || e == unix.EWOULDBLOCK {
		return false
	}
	r.n, r.errno = int(r1), e
	return true
}

// read follows Conn.Read's contract for the messages it is given.
func (r *receiver) read(messages []Message) (int, error) {
	if len(messages) > len(r.hdrs) {
		messages = messages[:len(r.hdrs)]
	}
	for i := range messages {
		h := &r.hdrs[i]
		*h = mmsghdr{}
		r.iovs[i] = unix.Iovec{}
		if p := messages[i].Payload; len(p) > 0 {
			r.iovs[i].Base = &p[0]
			r.iovs[i].SetLen(len(p))
		}
		h.hdr.Iov = &r.iovs[i]
		h.hdr.SetIovlen(1)
		h.hdr.Name = (*byte)(unsafe.Pointer(&r.names[i]))
		h.hdr.Namelen = uint32(unsafe.Sizeof(r.names[i]))
	}
	r.count, r.n, r.errno = len(messages), 0, 0
	err := r.raw.Read(r.call)
	n, errno := r.n, r.errno
	for i := range messages {
		r.iovs[i].Base = nil
	}
	if err != nil {
		return 0, err
	}
	if errno != 0 {
		return 0, errno
	}
	if n < 0 || n > len(messages) {
		return 0, syscall.EIO
	}
	for i := 0; i < n; i++ {
		h := &r.hdrs[i]
		messages[i].N = int(h.n)
		messages[i].Truncated = h.hdr.Flags&unix.MSG_TRUNC != 0 || messages[i].N > len(messages[i].Payload)
		messages[i].Peer = r.peer(&r.names[i])
	}
	return n, nil
}

func (r *receiver) peer(name *unix.RawSockaddrAny) net.Addr {
	var key netip.AddrPort
	switch name.Addr.Family {
	case unix.AF_INET:
		sa := (*unix.RawSockaddrInet4)(unsafe.Pointer(name))
		key = netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), port(sa.Port))
	case unix.AF_INET6:
		sa := (*unix.RawSockaddrInet6)(unsafe.Pointer(name))
		if sa.Scope_id != 0 { // zoned link-local: rare, not cached
			zone := ""
			if ifi, err := net.InterfaceByIndex(int(sa.Scope_id)); err == nil {
				zone = ifi.Name
			}
			return &net.UDPAddr{IP: append(net.IP(nil), sa.Addr[:]...), Port: int(port(sa.Port)), Zone: zone}
		}
		key = netip.AddrPortFrom(netip.AddrFrom16(sa.Addr), port(sa.Port))
	default:
		return nil
	}
	if addr, ok := r.peers[key]; ok {
		return addr
	}
	if len(r.peers) >= peerCacheLimit {
		clear(r.peers)
	}
	addr := net.UDPAddrFromAddrPort(key)
	if key.Addr().Is4() {
		addr.IP = addr.IP.To16() // the 16-byte form x/net reports
	}
	r.peers[key] = addr
	return addr
}

// port converts the network-order port of a raw sockaddr.
func port(p uint16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&p))
	return uint16(b[0])<<8 | uint16(b[1])
}
