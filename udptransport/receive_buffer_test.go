package udptransport

import (
	"errors"
	"syscall"
	"testing"
)

func TestReceiveBufferSocket(t *testing.T) {
	c := listen(t, "udp4")
	initial, forced, err := SetReceiveBuffer(c, 0)
	if err != nil || initial <= 0 || forced {
		t.Fatalf("default: %d %v %v", initial, forced, err)
	}
	for _, request := range []int{0, -1} {
		got, forced, err := SetReceiveBuffer(c, request)
		if err != nil || got != initial || forced {
			t.Fatalf("read only: %d %v %v", got, forced, err)
		}
	}
	effective, _, err := SetReceiveBuffer(c, 32768)
	if err != nil || effective < 32768 {
		t.Fatalf("set: %d %v", effective, err)
	}
	readback, forced, err := SetReceiveBuffer(c, 0)
	if err != nil || readback != effective || forced {
		t.Fatalf("readback: %d %v %v", readback, forced, err)
	}
}

func TestReceiveBufferUnsupported(t *testing.T) {
	c := newSenderSocket()
	if _, _, err := SetReceiveBuffer(c, 65536); err == nil {
		t.Fatal("unsupported socket accepted")
	}
	select {
	case <-c.closed:
		t.Fatal("unsupported socket changed")
	default:
	}
}

func TestReceiveBufferStopsOnPlainFailure(t *testing.T) {
	c := listen(t, "udp4")
	raw, _ := c.SyscallConn()
	calls := 0
	_, _, err := receiveBuffer(raw, 65536, func(uintptr, int, int) error { calls++; return syscall.EINVAL }, func(uintptr, int) (int, error) { t.Fatal("read after failure"); return 0, nil })
	if !errors.Is(err, syscall.EINVAL) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestReceiveBufferReadOnlyDoesNotSet(t *testing.T) {
	c := listen(t, "udp4")
	raw, _ := c.SyscallConn()
	for _, request := range []int{0, -1} {
		effective, forced, err := receiveBuffer(raw, request, func(uintptr, int, int) error { t.Fatal("set on read only request"); return nil }, socketGetInt)
		if err != nil || effective <= 0 || forced {
			t.Fatalf("%d %v %v", effective, forced, err)
		}
	}
}
