//go:build !linux

package udptransport

import "net"

const maxGSOSegments = 64
const maxGSOPayload = 65507

func segmentControlSize() int                    { return 0 }
func segmentControl(buf []byte, size int) []byte { return nil }
func probeGSO(net.PacketConn) bool               { return false }
func gsoUnsupported(error) bool                  { return false }
