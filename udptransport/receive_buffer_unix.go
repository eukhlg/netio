//go:build !windows

package udptransport

import "syscall"

const receiveBufferOption = syscall.SO_RCVBUF

func socketSetInt(fd uintptr, option, value int) error {
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, option, value)
}
func socketGetInt(fd uintptr, option int) (int, error) {
	return syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, option)
}
