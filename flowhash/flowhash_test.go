package flowhash

import (
	"encoding/binary"
	"testing"
)

func packet(protocol byte, sourcePort, destinationPort uint16, flags uint16) []byte {
	p := make([]byte, 40)
	p[0] = 0x45
	binary.BigEndian.PutUint16(p[2:4], 40)
	binary.BigEndian.PutUint16(p[6:8], flags)
	p[9] = protocol
	copy(p[12:16], []byte{10, 0, 0, 2})
	copy(p[16:20], []byte{10, 0, 0, 1})
	binary.BigEndian.PutUint16(p[20:22], sourcePort)
	binary.BigEndian.PutUint16(p[22:24], destinationPort)
	return p
}

func TestFlowKeys(t *testing.T) {
	tcp := IPv4(packet(6, 40000, 5201, 0))
	if IPv4(packet(6, 40000, 5201, 0)) != tcp {
		t.Fatal("same flow hashed differently")
	}
	if IPv4(packet(6, 40000, 5201, 0x4000)) != tcp {
		t.Fatal("DF bit changed the flow")
	}
	distinct := map[uint32]bool{}
	for port := uint16(40000); port < 40064; port++ {
		distinct[IPv4(packet(17, port, 53, 0))] = true
	}
	if len(distinct) < 60 {
		t.Fatalf("ports barely affect the hash: %d distinct of 64", len(distinct))
	}
	// Every fragment of a datagram, including the first, uses the 3-tuple.
	first := IPv4(packet(17, 40000, 53, 0x2000))
	later := IPv4(packet(17, 0, 0, 0x0010))
	if first != later || first != IPv4(packet(17, 1, 2, 0x2005)) {
		t.Fatal("fragments of one datagram hashed differently")
	}
	if IPv4(packet(1, 1, 2, 0)) != IPv4(packet(1, 3, 4, 0)) {
		t.Fatal("non-TCP/UDP packets must use the 3-tuple")
	}
	_ = IPv4(nil)
	_ = IPv4([]byte{0x45, 0})
	truncated := packet(6, 1, 2, 0)[:22]
	_ = IPv4(truncated)
}

func TestShardRange(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 7} {
		for _, h := range []uint32{0, 1, 1 << 31, ^uint32(0)} {
			s := Shard(h, n)
			if s < 0 || (n > 0 && s >= n) || (n <= 1 && s != 0) {
				t.Fatalf("Shard(%d, %d) = %d", h, n, s)
			}
		}
	}
	counts := make([]int, 3)
	for port := uint16(0); port < 3000; port++ {
		counts[Shard(IPv4(packet(6, port, 80, 0)), 3)]++
	}
	for i, c := range counts {
		if c < 800 {
			t.Fatalf("shard %d got %d of 3000 flows", i, c)
		}
	}
}

func TestIPv4DoesNotAllocate(t *testing.T) {
	p := packet(6, 1, 2, 0)
	if n := testing.AllocsPerRun(1000, func() { _ = IPv4(p) }); n != 0 {
		t.Fatalf("IPv4 allocated %.1f times", n)
	}
}

func BenchmarkIPv4(b *testing.B) {
	p := packet(6, 1, 2, 0)
	for b.Loop() {
		_ = IPv4(p)
	}
}
