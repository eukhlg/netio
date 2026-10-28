package udptransport

import (
	"net"
	"testing"
	"time"
)

func TestDuplicatePacketConnSharesSocket(t *testing.T) {
	receiver, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	original, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := DuplicatePacketConn(original)
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	if duplicate.LocalAddr().String() != original.LocalAddr().String() {
		t.Fatalf("duplicate bound to %v, original %v", duplicate.LocalAddr(), original.LocalAddr())
	}
	// Closing one descriptor leaves the socket usable through the other.
	original.Close()
	if _, err := duplicate.WriteTo([]byte{7}, receiver.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	receiver.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 4)
	n, peer, err := receiver.ReadFrom(buf)
	if err != nil || n != 1 || buf[0] != 7 || peer.String() != duplicate.LocalAddr().String() {
		t.Fatalf("n=%d peer=%v err=%v", n, peer, err)
	}
	if _, err := DuplicatePacketConn(newSenderSocket()); err == nil {
		t.Fatal("non-duplicable connection accepted")
	}
}
