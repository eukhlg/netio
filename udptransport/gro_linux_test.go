//go:build linux

package udptransport

import (
	"bytes"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"
	"io"
	"testing"
	"unsafe"
)

func TestGROPendingSegments(t *testing.T) {
	c, err := NewWithOptions(listen(t, "udp4"), 2, Options{EnableGRO: true})
	if err != nil {
		t.Fatal(err)
	}
	if !c.GROEnabled() {
		t.Skip("no UDP_GRO")
	}
	calls := 0
	c.batch = scriptedBatch{reads: func(m []ipv6.Message) (int, error) {
		calls++
		if calls > 1 {
			t.Fatal("read before pending drained")
		}
		copy(m[0].Buffers[0], []byte{1, 1, 2, 2, 3})
		m[0].N = 5
		control := segmentControl(m[0].OOB, 2)
		(*unix.Cmsghdr)(unsafe.Pointer(&control[0])).Type = unix.UDP_GRO
		m[0].NN = len(control)
		copy(m[1].Buffers[0], []byte{4})
		m[1].N = 1
		return 2, io.EOF
	}}
	for i, want := range [][]byte{{1, 1}, {2, 2}, {3}, {4}} {
		slots := []Message{{Payload: make([]byte, 10)}}
		n, err := c.Read(slots)
		if n != 1 || slots[0].Truncated || !bytes.Equal(slots[0].Payload[:slots[0].N], want) {
			t.Fatalf("%d %v %+v", n, err, slots)
		}
		if (i == 3 && err != io.EOF) || (i < 3 && err != nil) {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}
func TestGRODefaultOff(t *testing.T) {
	if New(listen(t, "udp4")).GROEnabled() {
		t.Fatal("default GRO")
	}
}
