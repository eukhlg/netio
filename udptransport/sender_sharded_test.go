package udptransport

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/eukhlg/netio/flowhash"
)

func testShards(writers []Writer) []Shard {
	shards := make([]Shard, len(writers))
	for i, w := range writers {
		shards[i] = Shard{Conn: newSenderSocket(), Writer: w}
	}
	return shards
}

// countingOwner checks the ownership contract: one release per accepted datagram.
type countingOwner struct {
	released *atomic.Int64
	once     atomic.Int32
}

func (o *countingOwner) Release() {
	if o.once.Add(1) != 1 {
		panic("double release")
	}
	o.released.Add(1)
}

func TestShardedSenderPreservesPerFlowOrder(t *testing.T) {
	const shards, flows, perFlow = 3, 12, 200
	var mu sync.Mutex
	seen := map[uint32][]int{}
	writers := make([]Writer, shards)
	for i := range writers {
		writers[i] = senderWriter(func(m []Message) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			for _, msg := range m {
				flow := uint32(msg.Payload[0])
				seen[flow] = append(seen[flow], int(msg.Payload[1])<<8|int(msg.Payload[2]))
			}
			return len(m), nil
		})
	}
	s, err := NewShardedSender(context.Background(), testShards(writers), 8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var producers sync.WaitGroup
	for f := 0; f < flows; f++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := 0; i < perFlow; i++ {
				// One producer per flow, as the router dispatches one flow from one goroutine.
				d := Datagram{Payload: []byte{byte(f), byte(i >> 8), byte(i)}, Flow: uint32(f) * 0x9e3779b9}
				if err := s.Enqueue(context.Background(), d); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	producers.Wait()
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for f := 0; f < flows; f++ {
		got := seen[uint32(f)]
		if len(got) != perFlow {
			t.Fatalf("flow %d: %d of %d datagrams", f, len(got), perFlow)
		}
		for i, v := range got {
			if v != i {
				t.Fatalf("flow %d reordered at %d: %d", f, i, v)
			}
		}
	}
}

func TestShardedSenderOwnershipUnderConcurrency(t *testing.T) {
	var released, accepted atomic.Int64
	var written atomic.Int64
	writers := make([]Writer, 4)
	for i := range writers {
		writers[i] = senderWriter(func(m []Message) (int, error) {
			written.Add(int64(len(m)))
			return len(m), nil
		})
	}
	s, err := NewShardedSender(context.Background(), testShards(writers), 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	var producers sync.WaitGroup
	for p := 0; p < 8; p++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			r := rand.New(rand.NewPCG(uint64(p), 7))
			for i := 0; i < 2000; i++ {
				owner := &countingOwner{released: &released}
				eligible := func() bool { return true }
				if r.IntN(5) == 0 {
					eligible = func() bool { return false }
				}
				err := s.Enqueue(context.Background(), Datagram{Payload: []byte{1}, Owner: owner, Eligible: eligible, Flow: r.Uint32()})
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrIneligible), errors.Is(err, context.Canceled), errors.Is(err, net.ErrClosed):
					owner.Release() // rejected: the caller still owns it
					accepted.Add(1) // counted with the sender's releases below
				default:
					t.Error(err)
					return
				}
			}
		}()
	}
	// Close while producers run: queued datagrams must still be released once,
	// and later enqueues are rejected back to their callers.
	for accepted.Load() < 4000 {
	}
	s.Close()
	producers.Wait()
	if released.Load() != accepted.Load() {
		t.Fatalf("released %d of %d owned datagrams", released.Load(), accepted.Load())
	}
}

func TestShardedSenderCloseReleasesQueued(t *testing.T) {
	var released, accepted atomic.Int64
	block := make(chan struct{})
	writers := []Writer{
		senderWriter(func(m []Message) (int, error) { <-block; return len(m), nil }),
		senderWriter(func(m []Message) (int, error) { <-block; return len(m), nil }),
	}
	s, err := NewShardedSender(context.Background(), testShards(writers), 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var producers sync.WaitGroup
	for p := 0; p < 4; p++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := 0; ; i++ {
				owner := &countingOwner{released: &released}
				if err := s.Enqueue(ctx, Datagram{Payload: []byte{1}, Owner: owner, Flow: uint32(p*1000 + i)}); err != nil {
					return // rejected datagrams stay with the caller and are not counted
				}
				accepted.Add(1)
			}
		}()
	}
	for accepted.Load() < 8 {
	}
	cancel()
	close(block)
	s.Close()
	producers.Wait()
	if released.Load() != accepted.Load() {
		t.Fatalf("released %d of %d accepted", released.Load(), accepted.Load())
	}
}

func TestShardedSenderFailureStopsAllShards(t *testing.T) {
	failure := errors.New("boom")
	writers := []Writer{
		senderWriter(func(m []Message) (int, error) { return 0, failure }),
		senderWriter(func(m []Message) (int, error) { return len(m), nil }),
	}
	s, err := NewShardedSender(context.Background(), testShards(writers), 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	failing := uint32(0)
	for flowhash.Shard(failing, 2) != 0 {
		failing++
	}
	if err := s.Enqueue(context.Background(), Datagram{Payload: []byte{1}, Flow: failing}); err != nil {
		t.Fatal(err)
	}
	waitSender(t, s.Done())
	waitSender(t, s.shards[1].Done())
	if err := s.Err(); !errors.Is(err, failure) {
		t.Fatalf("Err = %v", err)
	}
	if err := s.Enqueue(context.Background(), Datagram{Payload: []byte{1}, Flow: failing + 1<<31}); err == nil {
		t.Fatal("stopped sender accepted a datagram")
	}
}
