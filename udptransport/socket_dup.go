package udptransport

import (
	"errors"
	"net"
	"os"
)

// DuplicatePacketConn returns a second descriptor for the same socket: same
// bound address and kernel socket, but its own Go poller entry and write lock.
// Closing either descriptor leaves the other usable; the socket closes with
// the last one. Platforms or connections without descriptor duplication
// return an error, and callers may share the original connection instead.
func DuplicatePacketConn(conn net.PacketConn) (net.PacketConn, error) {
	filer, ok := conn.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, errors.New("udp transport: connection cannot be duplicated")
	}
	file, err := filer.File()
	if err != nil {
		return nil, err
	}
	defer file.Close() // FilePacketConn duplicates it again
	return net.FilePacketConn(file)
}
