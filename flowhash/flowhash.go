// Package flowhash maps IPv4 packets to flows for sharded processing. A flow
// always maps to the same shard, so per-flow order survives parallel stages
// without reordering buffers.
package flowhash

import (
	"encoding/binary"
	"hash/maphash"
)

// seed is random per process: clients choose their inner ports and must not be
// able to pin many flows onto one shard with precomputed collisions.
var seed = maphash.MakeSeed()

// IPv4 hashes an IPv4 packet without allocating. Unfragmented TCP and UDP use
// the 5-tuple (source, destination, protocol, ports). IP fragments (MF set or
// a non-zero offset, which includes the first fragment) and other protocols
// use the 3-tuple, so every fragment of a datagram hashes alike. The DF bit is
// ignored. Malformed packets hash their available prefix.
func IPv4(packet []byte) uint32 {
	var key [13]byte
	if len(packet) < 20 || packet[0]>>4 != 4 {
		n := copy(key[:], packet)
		return fold(maphash.Bytes(seed, key[:n]))
	}
	copy(key[:8], packet[12:20])
	protocol := packet[9]
	key[8] = protocol
	size := 9
	headerLength := int(packet[0]&15) * 4
	fragment := binary.BigEndian.Uint16(packet[6:8])&0x3fff != 0
	if !fragment && (protocol == 6 || protocol == 17) && headerLength >= 20 && len(packet) >= headerLength+4 {
		copy(key[9:13], packet[headerLength:headerLength+4])
		size = 13
	}
	return fold(maphash.Bytes(seed, key[:size]))
}

func fold(h uint64) uint32 { return uint32(h ^ h>>32) }

// Shard maps a hash onto n shards (n >= 1) by multiplication, so n need not be
// a power of two and no division is needed.
func Shard(hash uint32, n int) int {
	if n <= 1 {
		return 0
	}
	return int(uint64(hash) * uint64(n) >> 32)
}
