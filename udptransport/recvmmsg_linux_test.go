//go:build linux

package udptransport

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// Direct recvmmsg keeps Read's contract: valid prefix, lengths, truncation and
// source addresses, with one shared address per peer.
func TestReceiverBatchPeersAndTruncation(t *testing.T) {
	rx, a, b := listen(t, "udp4"), listen(t, "udp4"), listen(t, "udp4")
	c := New(rx)
	if c.recv == nil {
		t.Fatal("receiver not used on a Linux UDP socket")
	}
	sends := []struct {
		from *net.UDPConn
		data []byte
	}{{a, []byte{1, 2}}, {b, []byte{3}}, {a, bytes.Repeat([]byte{4}, 40)}, {a, []byte{5, 5, 5}}}
	for _, s := range sends {
		if _, err := s.from.WriteTo(s.data, rx.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	rx.SetReadDeadline(time.Now().Add(time.Second))
	slots := make([]Message, 8)
	for i := range slots {
		slots[i].Payload = make([]byte, 10)
	}
	got := 0
	var peerA net.Addr
	for got < len(sends) {
		n, err := c.Read(slots[:len(sends)-got])
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			s, m := sends[got], slots[i]
			from := s.from.LocalAddr().(*net.UDPAddr)
			peer, ok := m.Peer.(*net.UDPAddr)
			if !ok || !peer.IP.Equal(from.IP) || peer.Port != from.Port {
				t.Fatalf("datagram %d from %v, want %v", got, m.Peer, from)
			}
			if s.from == a {
				if peerA != nil && peerA != m.Peer {
					t.Fatal("peer address not shared")
				}
				peerA = m.Peer
			}
			truncated := len(s.data) > len(m.Payload)
			if m.Truncated != truncated || (!truncated && (m.N != len(s.data) || !bytes.Equal(m.Payload[:m.N], s.data))) {
				t.Fatalf("datagram %d: n=%d truncated=%t", got, m.N, m.Truncated)
			}
			got++
		}
	}
}

func TestReceiverReadAllocatesNothing(t *testing.T) {
	rx, tx := listen(t, "udp4"), listen(t, "udp4")
	c := New(rx)
	for i := 0; i < 300; i++ {
		if _, err := tx.WriteTo([]byte{byte(i)}, rx.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	rx.SetReadDeadline(time.Now().Add(time.Second))
	slots := []Message{{Payload: make([]byte, 16)}}
	read := func() {
		if n, err := c.Read(slots); n != 1 || err != nil {
			t.Fatalf("%d %v", n, err)
		}
	}
	read()
	if allocs := testing.AllocsPerRun(200, read); allocs != 0 {
		t.Fatalf("Read allocates %v per call", allocs)
	}
}

func TestReceiverClosedSocket(t *testing.T) {
	rx := listen(t, "udp4")
	c := New(rx)
	rx.Close()
	if _, err := c.Read([]Message{{Payload: make([]byte, 16)}}); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
