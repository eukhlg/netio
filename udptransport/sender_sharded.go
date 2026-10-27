package udptransport

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/eukhlg/netio/flowhash"
)

// ShardedSender spreads datagrams over independent Senders writing to one
// socket, so the kernel send work of different flows runs on different CPUs.
// Datagram.Flow picks the shard: one flow always uses one shard and keeps its
// order, while different flows may interleave on the wire. Each shard follows
// the Datagram ownership contract on its own; a failure of any shard stops all.
type ShardedSender struct {
	shards  []*Sender
	cancel  context.CancelFunc
	stopped sync.WaitGroup
}

// NewShardedSender uses one batch writer per shard. writers[i] keeps its own
// message metadata and offload state; all of them must write to conn, which
// the ShardedSender owns and closes. One writer gives exactly Sender behavior.
func NewShardedSender(ctx context.Context, conn net.PacketConn, writers []Writer, capacity, batchSize int) (*ShardedSender, error) {
	if len(writers) < 1 {
		return nil, errors.New("udp sender: at least one shard required")
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &ShardedSender{cancel: cancel}
	for _, writer := range writers {
		shard, err := NewSenderWithWriter(ctx, conn, writer, capacity, batchSize)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.shards = append(s.shards, shard)
		s.stopped.Add(1)
		go func() {
			defer s.stopped.Done()
			<-shard.stopped
			cancel() // one failed shard closes the socket; stop the others too
		}()
	}
	return s, nil
}

// Shards reports the number of independent senders.
func (s *ShardedSender) Shards() int { return len(s.shards) }

// Enqueue queues d on the shard selected by d.Flow, with Sender semantics.
func (s *ShardedSender) Enqueue(ctx context.Context, d Datagram) error {
	return s.shards[flowhash.Shard(d.Flow, len(s.shards))].Enqueue(ctx, d)
}

// Flush returns after every shard completed the datagrams queued before it.
func (s *ShardedSender) Flush(ctx context.Context) error {
	for _, shard := range s.shards {
		if err := shard.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Err reports the first shard failure, or cancellation once stopped.
func (s *ShardedSender) Err() error {
	var stopped error
	for _, shard := range s.shards {
		if err := shard.Err(); err != nil {
			if !errors.Is(err, context.Canceled) {
				return err
			}
			stopped = err
		}
	}
	return stopped
}

// Close stops every shard, releasing queued datagrams, and joins them.
func (s *ShardedSender) Close() {
	s.cancel()
	for _, shard := range s.shards {
		shard.Close()
	}
	s.stopped.Wait()
}
