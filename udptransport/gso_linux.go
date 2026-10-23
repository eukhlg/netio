//go:build linux

package udptransport

import (
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"unsafe"
)

// Linux UDP_MAX_SEGMENTS and the conservative IPv4 UDP payload limit.
const maxGSOSegments = 64
const maxGSOPayload = 65507

func segmentControlSize() int { return unix.CmsgSpace(2) }
func segmentControl(buf []byte, size int) []byte {
	clear(buf)
	h := (*unix.Cmsghdr)(unsafe.Pointer(&buf[0]))
	h.Level = unix.SOL_UDP
	h.Type = unix.UDP_SEGMENT
	h.SetLen(unix.CmsgLen(2))
	binary.NativeEndian.PutUint16(buf[unix.CmsgLen(0):], uint16(size))
	return buf[:unix.CmsgSpace(2)]
}
func probeGSO(conn net.PacketConn) bool {
	udp, ok := conn.(*net.UDPConn)
	if !ok || udp == nil || udp.RemoteAddr() != nil {
		return false
	}
	raw, err := udp.SyscallConn()
	if err != nil {
		return false
	}
	supported := false
	err = raw.Control(func(fd uintptr) {
		_, e := unix.GetsockoptInt(int(fd), unix.SOL_UDP, unix.UDP_SEGMENT)
		supported = e == nil
	})
	return err == nil && supported
}
func gsoUnsupported(err error) bool {
	return errors.Is(err, unix.EIO) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP)
}
