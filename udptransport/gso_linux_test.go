//go:build linux

package udptransport

import (
	"bytes"
	"golang.org/x/net/ipv6"
	"syscall"
	"testing"
	"time"
)

func TestGSOLoopback(t *testing.T) {
	rx, tx := listen(t, "udp4"), listen(t, "udp4")
	c, _ := NewWithBatchSize(tx, 128)
	if !c.GSOEnabled() {
		t.Skip("kernel has no UDP_SEGMENT")
	}
	messages := make([]Message, 64)
	for i := range messages {
		messages[i] = Message{Payload: bytes.Repeat([]byte{byte(i)}, 1000), Peer: rx.LocalAddr()}
	}
	messages[63].Payload = messages[63].Payload[:500]
	if n, e := c.Write(messages); e != nil || n != 64 || !c.GSOEnabled() {
		t.Fatalf("%d %v GSO=%v", n, e, c.GSOEnabled())
	}
	rx.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 2000)
	for i, m := range messages {
		n, _, err := rx.ReadFrom(b)
		if err != nil || !bytes.Equal(b[:n], m.Payload) {
			t.Fatalf("datagram %d: %d %v", i, n, err)
		}
	}
}

func TestGSOZeroProgressFallback(t *testing.T) {
	peer := listen(t, "udp4").LocalAddr()
	for _, failure := range []error{syscall.EIO, syscall.EINVAL, syscall.ENOTSUP} {
		c := New(listen(t, "udp4"))
		c.gso.Store(true)
		calls := 0
		c.batch = scriptedBatch{writes: func(m []ipv6.Message) (int, error) {
			calls++
			if calls == 1 {
				return -1, failure
			}
			if len(m) != 3 {
				t.Fatal("lost original messages")
			}
			for _, msg := range m {
				if len(msg.Buffers) != 1 || len(msg.OOB) != 0 {
					t.Fatal("retry still coalesced")
				}
			}
			return 3, nil
		}}
		n, err := c.Write([]Message{{Payload: []byte{1}, Peer: peer}, {Payload: []byte{2}, Peer: peer}, {Payload: []byte{3}, Peer: peer}})
		if n != 3 || err != nil || calls != 2 || c.GSOEnabled() {
			t.Fatal(n, err, calls)
		}
	}
}
