package udptransport

import "syscall"

const receiveBufferOption = syscall.SO_RCVBUF

func socketSetInt(fd uintptr, option, value int) error {
	return syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, option, value)
}
func socketGetInt(fd uintptr, option int) (int, error) {
	return syscall.GetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, option)
}
