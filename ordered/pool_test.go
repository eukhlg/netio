package ordered

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type item struct {
	n        int
	done     bool
	consumed *atomic.Int32 // complete or discard, exactly once
}

func (i item) consume(t *testing.T) {
	if i.consumed.Add(1) != 1 {
		t.Error("item consumed twice")
	}
}

func TestPoolCompletesInSubmissionOrder(t *testing.T) {
	var got []int
	var mu sync.Mutex
	p := New(context.Background(), 4, 16,
		func(i item) item {
			time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond) // finish out of order
			i.done = true
			return i
		},
		func(_ context.Context, i item) {
			if !i.done {
				t.Error("completed before work")
			}
			mu.Lock()
			got = append(got, i.n)
			mu.Unlock()
		},
		func(item) { t.Error("discard on a running pool") })
	const count = 2000
	var producers sync.WaitGroup
	var order sync.Mutex // the caller's own ordering, as ESPBridge.outboundMu
	next := 0
	for w := 0; w < 4; w++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for {
				order.Lock()
				if next == count {
					order.Unlock()
					return
				}
				n := next
				next++
				if err := p.Submit(item{n: n, consumed: new(atomic.Int32)}); err != nil {
					t.Error(err)
				}
				order.Unlock()
			}
		}()
	}
	producers.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == count || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	p.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != count {
		t.Fatalf("%d of %d completed", len(got), count)
	}
	for i, n := range got {
		if n != i {
			t.Fatalf("position %d completed item %d", i, n)
		}
	}
}

// Every accepted item is completed or discarded exactly once, also when the
// pool closes while full, workers run and completion is blocked.
func TestPoolCloseConsumesEverythingOnce(t *testing.T) {
	var submitted, consumed atomic.Int64
	block := make(chan struct{})
	p := New(context.Background(), 3, 8,
		func(i item) item { return i },
		func(ctx context.Context, i item) {
			select {
			case <-block:
			case <-ctx.Done():
			}
			i.consume(t)
			consumed.Add(1)
		},
		func(i item) { i.consume(t); consumed.Add(1) })
	var producers sync.WaitGroup
	for w := 0; w < 4; w++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for {
				if err := p.Submit(item{consumed: new(atomic.Int32)}); err != nil {
					return // rejected: the item stays with the caller
				}
				submitted.Add(1)
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // fill the pool behind the blocked completion
	p.Close()
	close(block)
	producers.Wait()
	if submitted.Load() != consumed.Load() {
		t.Fatalf("submitted %d, consumed %d", submitted.Load(), consumed.Load())
	}
}

func TestPoolStopsWithParentContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var discarded atomic.Int32
	p := New(ctx, 2, 4, func(i item) item { return i }, func(context.Context, item) {}, func(item) { discarded.Add(1) })
	cancel()
	<-p.Context().Done()
	if err := p.Submit(item{}); err != ErrClosed || discarded.Load() != 0 {
		t.Fatalf("err=%v discarded=%d (rejected items stay with the caller)", err, discarded.Load())
	}
	p.Close()
	p.Close() // idempotent
}

func TestPoolDrainCompletesAcceptedItems(t *testing.T) {
	var completed atomic.Int32
	p := New(context.Background(), 3, 8,
		func(i item) item { time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond); return i },
		func(context.Context, item) { completed.Add(1) },
		func(item) { t.Error("drain discarded an accepted item") })
	for i := 0; i < 100; i++ {
		if err := p.Submit(item{n: i}); err != nil {
			t.Fatal(err)
		}
	}
	p.Drain()
	if completed.Load() != 100 {
		t.Fatalf("%d of 100 completed", completed.Load())
	}
	var rejected atomic.Int32
	q := New(context.Background(), 1, 2, func(i item) item { return i }, func(context.Context, item) {}, func(item) { rejected.Add(1) })
	q.Drain()
	if err := q.Submit(item{}); err != ErrClosed || rejected.Load() != 0 {
		t.Fatalf("submit after drain: %v rejected=%d", err, rejected.Load())
	}
}
