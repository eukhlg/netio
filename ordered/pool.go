// Package ordered runs per-packet work in parallel and hands the results on
// strictly in submission order. It is the shared compute stage of the data
// path: ESP receive (decrypt, then in-order replay commit) and ESP send
// (reserve in order, seal in parallel, enqueue in order) both build on it.
package ordered

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Pool runs work on several goroutines and passes each item to complete on
// one goroutine, in the order Submit accepted them. At most capacity items are
// inside the pool at once (queued, working, or finished and waiting behind an
// earlier one), so Submit blocks while it is full.
//
// Ownership follows udptransport.Sender: a successful Submit transfers the
// item, which then ends in exactly one call to complete or, once the pool
// stops, to discard; after a failed Submit the caller still owns it.
type Pool[T any] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	jobs      chan job[T]
	completed chan job[T]
	permits   chan struct{}
	next      uint64
	submitMu  sync.Mutex
	work      func(T) T
	complete  func(context.Context, T)
	discard   func(T)
	workers   sync.WaitGroup
	done      chan struct{}
	closeOnce sync.Once
	draining  atomic.Bool
}

type job[T any] struct {
	ticket uint64
	item   T
}

// ErrClosed reports a Submit after the pool stopped.
var ErrClosed = errors.New("ordered: pool closed")

// New starts workers goroutines and the completion goroutine. work may run
// concurrently for different items and returns the item to complete; complete
// runs for one item at a time and should give up when its context ends;
// discard frees an item that will not complete. Items pass by value, so a
// small struct moves through the pool without allocating. The pool stops when
// ctx ends or Close is called.
func New[T any](ctx context.Context, workers, capacity int, work func(T) T, complete func(context.Context, T), discard func(T)) *Pool[T] {
	if workers < 1 {
		workers = 1
	}
	if capacity < workers {
		capacity = workers
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool[T]{ctx: ctx, cancel: cancel, jobs: make(chan job[T], capacity), completed: make(chan job[T], capacity), permits: make(chan struct{}, capacity), work: work, complete: complete, discard: discard, done: make(chan struct{})}
	p.workers.Add(workers)
	for i := 0; i < workers; i++ {
		go p.run()
	}
	go p.commit(capacity)
	return p
}

// Context ends when the pool stops.
func (p *Pool[T]) Context() context.Context { return p.ctx }

func (p *Pool[T]) run() {
	defer p.workers.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case j := <-p.jobs:
			j.item = p.work(j.item)
			select {
			case p.completed <- j:
			case <-p.ctx.Done():
				p.discard(j.item)
				return
			}
		}
	}
}

// Submit queues item behind every item accepted before it. Concurrent callers
// are ordered by whoever enters first; callers that need a specific order
// (such as sequence numbers assigned outside) must serialize themselves.
func (p *Pool[T]) Submit(item T) error {
	if p.draining.Load() {
		return ErrClosed
	}
	select {
	case p.permits <- struct{}{}:
	case <-p.ctx.Done():
		return ErrClosed
	}
	p.submitMu.Lock()
	defer p.submitMu.Unlock()
	if p.ctx.Err() != nil || p.draining.Load() {
		<-p.permits
		return ErrClosed
	}
	// A permit guarantees room: at most capacity jobs exist in the pool.
	p.jobs <- job[T]{ticket: p.next, item: item}
	p.next++
	return nil
}

// commit completes items in ticket order. Tickets in flight never exceed
// capacity, so a ring indexed by ticket holds every item finished early.
func (p *Pool[T]) commit(capacity int) {
	defer close(p.done)
	ring := make([]job[T], capacity)
	present := make([]bool, capacity)
	size := uint64(capacity)
	next := uint64(0)
	for {
		select {
		case <-p.ctx.Done():
			for i := range ring {
				if present[i] {
					p.discard(ring[i].item)
					ring[i], present[i] = job[T]{}, false
				}
			}
			return
		case j := <-p.completed:
			slot := j.ticket % size
			ring[slot], present[slot] = j, true
			for present[next%size] {
				slot := next % size
				head := ring[slot]
				ring[slot], present[slot] = job[T]{}, false
				if p.ctx.Err() != nil {
					p.discard(head.item)
				} else {
					p.complete(p.ctx, head.item)
				}
				<-p.permits
				next++
			}
		}
	}
}

// Drain stops accepting items, waits until every accepted item has completed,
// then closes the pool. If the pool stops first (its context ends), the items
// still inside are discarded instead.
func (p *Pool[T]) Drain() {
	p.draining.Store(true)
	p.submitMu.Lock() // no Submit is between its checks and its send any more
	p.submitMu.Unlock()
	for i := 0; i < cap(p.permits); i++ {
		select {
		case p.permits <- struct{}{}: // free only once its item completed
		case <-p.ctx.Done():
			p.Close()
			return
		}
	}
	p.Close()
}

// Close stops the pool, joins its goroutines and discards every item still
// inside. Items already passed to complete are not affected.
func (p *Pool[T]) Close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.workers.Wait()
		<-p.done
		p.submitMu.Lock()
		defer p.submitMu.Unlock()
		for {
			select {
			case j := <-p.jobs:
				p.discard(j.item)
			case j := <-p.completed:
				p.discard(j.item)
			default:
				return
			}
		}
	})
}
