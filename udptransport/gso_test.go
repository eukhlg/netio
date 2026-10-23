package udptransport

import (
	"errors"
	"golang.org/x/net/ipv6"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestGSOCoalescing(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 123}
	other := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 124}
	for _, tt := range []struct {
		name   string
		sizes  []int
		change int
		want   []int
	}{
		{"equal", []int{100, 100, 100}, -1, []int{3}},
		{"peer", []int{100, 100, 100}, 1, []int{1, 1, 1}},
		{"short last", []int{100, 50, 100}, -1, []int{2, 1}},
		{"larger", []int{50, 100, 100}, -1, []int{1, 2}},
		{"zero", []int{100, 0, 100}, -1, []int{1, 1, 1}},
		{"bytes", []int{33000, 33000}, -1, []int{1, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := NewWithBatchSize(nil, 128)
			c.gso.Store(true)
			m := make([]Message, len(tt.sizes))
			for i, n := range tt.sizes {
				m[i] = Message{Payload: make([]byte, n), Peer: peer}
				if i == tt.change {
					m[i].Peer = other
				}
			}
			n, _ := c.prepareWrites(m)
			if n != len(tt.want) {
				t.Fatalf("groups %d want %v", n, tt.want)
			}
			for i, w := range tt.want {
				if len(c.tx[i].Buffers) != w {
					t.Fatalf("group %d", i)
				}
				for j, b := range c.tx[i].Buffers {
					start := 0
					if i > 0 {
						start = c.txEnds[i-1]
					}
					if len(b) > 0 && &b[0] != &m[start+j].Payload[0] {
						t.Fatal("copied payload")
					}
				}
			}
		})
	}
	c, _ := NewWithBatchSize(nil, 128)
	c.gso.Store(true)
	m := make([]Message, 128)
	for i := range m {
		m[i] = Message{Payload: []byte{1}, Peer: peer}
	}
	if n, _ := c.prepareWrites(m); n != 2 || c.txEnds[0] != 64 || c.txEnds[1] != 128 {
		t.Fatal("segment limit")
	}
}

func TestGSOPartialAndFallback(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 123}
	m := []Message{{Payload: make([]byte, 100), Peer: peer}, {Payload: make([]byte, 100), Peer: peer}, {Payload: make([]byte, 200), Peer: peer}, {Payload: make([]byte, 200), Peer: peer}}
	for _, failure := range []error{nil, io.ErrClosedPipe, syscall.EIO, syscall.EINVAL, syscall.ENOTSUP} {
		if failure != nil && failure != io.ErrClosedPipe && !gsoUnsupported(failure) {
			continue
		}
		c, _ := NewWithBatchSize(nil, 32)
		c.gso.Store(true)
		calls := 0
		c.batch = scriptedBatch{writes: func(b []ipv6.Message) (int, error) {
			calls++
			if calls == 1 {
				if len(b) != 2 {
					t.Fatal(len(b))
				}
				return 1, failure
			}
			if len(b) != 2 || len(b[0].OOB) != 0 || len(b[0].Buffers[0]) != 200 {
				t.Fatal("bad retry")
			}
			return len(b), nil
		}}
		n, err := c.Write(m)
		if failure == nil || errors.Is(failure, io.ErrClosedPipe) {
			if n != 2 || err != failure || calls != 1 {
				t.Fatalf("%d %v %d", n, err, calls)
			}
		} else {
			if n != 4 || err != nil || calls != 2 || c.GSOEnabled() {
				t.Fatalf("%d %v %d", n, err, calls)
			}
		}
	}
}
func TestDisableGSO(t *testing.T) {
	c, err := NewWithOptions(listen(t, "udp4"), 32, Options{DisableGSO: true})
	if err != nil || c.GSOEnabled() {
		t.Fatal(err)
	}
	c.batch = scriptedBatch{writes: func(m []ipv6.Message) (int, error) {
		if len(m) != 2 || len(m[0].OOB) != 0 {
			t.Fatal("GSO enabled")
		}
		return len(m), nil
	}}
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 123}
	if n, e := c.Write([]Message{{Payload: []byte{1}, Peer: peer}, {Payload: []byte{2}, Peer: peer}}); n != 2 || e != nil {
		t.Fatal(n, e)
	}
}
