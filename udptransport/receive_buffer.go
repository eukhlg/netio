package udptransport

import (
	"errors"
	"fmt"
	"net"
	"syscall"
)

// SetReceiveBuffer sets only the receive buffer and returns the kernel's raw
// SO_RCVBUF value (Linux doubles positive requests). Nonpositive requests only
// read the current value. forced reports a successful Linux SO_RCVBUFFORCE.
// The socket remains owned by the caller. Unsupported connections are untouched.
func SetReceiveBuffer(conn net.PacketConn, bytes int) (effective int, forced bool, err error) {
	var raw syscall.RawConn
	switch c := conn.(type) {
	case *net.UDPConn:
		if c == nil {
			return 0, false, errors.New("receive buffer: nil UDP connection")
		}
		raw, err = c.SyscallConn()
	case *net.IPConn:
		if c == nil {
			return 0, false, errors.New("receive buffer: nil IP connection")
		}
		raw, err = c.SyscallConn()
	default:
		return 0, false, fmt.Errorf("receive buffer: unsupported connection type %T", conn)
	}
	if err != nil {
		return 0, false, fmt.Errorf("receive buffer: %w", err)
	}
	return receiveBuffer(raw, bytes, socketSetInt, socketGetInt)
}

// Keep syscall injection local to the call: concurrent users cannot replace
// process-wide socket operations, including in tests.
func receiveBuffer(raw syscall.RawConn, bytes int, set func(uintptr, int, int) error, get func(uintptr, int) (int, error)) (effective int, forced bool, err error) {
	controlErr := raw.Control(func(fd uintptr) {
		if bytes > 0 {
			if err = set(fd, receiveBufferOption, bytes); err != nil {
				return
			}
			if receiveBufferForceOption != 0 {
				err = set(fd, receiveBufferForceOption, bytes)
				if err == nil {
					forced = true
				} else if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
					err = nil
				} else {
					return
				}
			}
		}
		effective, err = get(fd, receiveBufferOption)
	})
	if controlErr != nil {
		err = controlErr
	}
	if err != nil {
		err = fmt.Errorf("receive buffer: %w", err)
	}
	return
}
