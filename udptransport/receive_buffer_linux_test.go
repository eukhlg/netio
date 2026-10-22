package udptransport

import (
	"errors"
	"syscall"
	"testing"
)

func TestReceiveBufferForceDenied(t *testing.T) {
	for _, denied := range []error{syscall.EPERM, syscall.EACCES} {
		c := listen(t, "udp4")
		raw, _ := c.SyscallConn()
		var options []int
		effective, forced, err := receiveBuffer(raw, 65536, func(fd uintptr, option, value int) error {
			options = append(options, option)
			if option == syscall.SO_RCVBUFFORCE {
				return denied
			}
			return socketSetInt(fd, option, value)
		}, socketGetInt)
		if err != nil || forced || effective != 131072 || len(options) != 2 || options[0] != syscall.SO_RCVBUF || options[1] != syscall.SO_RCVBUFFORCE {
			t.Fatalf("%d %v %v options=%v", effective, forced, err, options)
		}
	}
}

func TestReceiveBufferUnexpectedForceFailure(t *testing.T) {
	c := listen(t, "udp4")
	raw, _ := c.SyscallConn()
	_, _, err := receiveBuffer(raw, 65536, func(fd uintptr, option, value int) error {
		if option == syscall.SO_RCVBUFFORCE {
			return syscall.EINVAL
		}
		return socketSetInt(fd, option, value)
	}, func(uintptr, int) (int, error) { t.Fatal("read after unexpected failure"); return 0, nil })
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}
