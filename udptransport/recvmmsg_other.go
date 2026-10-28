//go:build !linux

package udptransport

import "net"

type receiver struct{}

func newReceiver(net.PacketConn, int) *receiver { return nil }

func (*receiver) read([]Message) (int, error) { return 0, nil }
