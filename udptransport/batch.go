// Package udptransport provides message batching without changing UDP framing.
package udptransport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"

	"golang.org/x/net/ipv6"
)

// DefaultBatchSize bounds queue draining and receive storage unless configured.
const DefaultBatchSize = 32

// BatchSize is retained for source compatibility. New code should use
// DefaultBatchSize or the size selected at construction.
const BatchSize = DefaultBatchSize

// ValidateBatchSize accepts bounded batches without changing packet ordering.
func ValidateBatchSize(size int) error {
	if size < 1 || size > 256 {
		return fmt.Errorf("udp batch size must be between 1 and 256: %d", size)
	}
	return nil
}

// Message borrows caller-owned storage until the operation returns. N and
// Truncated are receive outputs. Each message is one UDP datagram.
type Message struct {
	Payload   []byte
	Peer      net.Addr
	N         int
	Truncated bool
}

type batchSocket interface {
	ReadBatch([]ipv6.Message, int) (int, error)
	WriteBatch([]ipv6.Message, int) (int, error)
}

// Conn leaves deadlines and socket ownership with its caller. One reader and
// one writer may operate concurrently. Metadata is kept independently per side.
type Conn struct {
	conn                        net.PacketConn
	batch                       batchSocket
	readFallback, writeFallback atomic.Bool
	rx, tx                      []ipv6.Message
	rxBuffers, txBuffers        [][1][]byte
	gso                         atomic.Bool
	txVectors                   [][]byte
	txEnds                      []int
	txOOB                       [][]byte
}

func New(conn net.PacketConn) *Conn {
	c, _ := NewWithBatchSize(conn, DefaultBatchSize)
	return c
}

// NewWithBatchSize allocates all message metadata once, before I/O starts.
type Options struct {
	// DisableGSO preserves ordinary datagram sends even when the kernel supports GSO.
	DisableGSO bool
}

func NewWithBatchSize(conn net.PacketConn, size int) (*Conn, error) {
	return NewWithOptions(conn, size, Options{})
}

func NewWithOptions(conn net.PacketConn, size int, options Options) (*Conn, error) {
	if err := ValidateBatchSize(size); err != nil {
		return nil, err
	}
	c := &Conn{conn: conn, batch: newBatchSocket(conn), rx: make([]ipv6.Message, size), tx: make([]ipv6.Message, size), rxBuffers: make([][1][]byte, size), txBuffers: make([][1][]byte, size)}
	for i := range c.rx {
		c.rx[i].Buffers = c.rxBuffers[i][:]
		c.tx[i].Buffers = c.txBuffers[i][:]
	}
	c.txVectors = make([][]byte, size)
	c.txEnds = make([]int, size)
	c.txOOB = make([][]byte, size)
	for i := range c.txOOB {
		c.txOOB[i] = make([]byte, segmentControlSize())
	}
	c.gso.Store(!options.DisableGSO && probeGSO(conn))
	return c, nil
}

// GSOEnabled reports probed support; a runtime failure disables it for this Conn.
func (c *Conn) GSOEnabled() bool { return c.gso.Load() }
func (c *Conn) BatchSize() int {
	if c.batch == nil {
		return 1
	}
	return len(c.rx)
}
func unsupported(err error) bool {
	return errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP)
}

// Read receives into independent caller buffers. Only entries before n are
// valid, including when an error accompanies progress. Truncated entries must
// be discarded. The buffers cannot be reused until their consumers finish.
func (c *Conn) Read(messages []Message) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	if len(messages) > len(c.rx) {
		messages = messages[:len(c.rx)]
	}
	if c.batch != nil && !c.readFallback.Load() {
		for i := range messages {
			c.rx[i].Buffers[0] = messages[i].Payload
			c.rx[i].N = 0
			c.rx[i].Flags = 0
			c.rx[i].Addr = nil
		}
		n, err := c.batch.ReadBatch(c.rx[:len(messages)], 0)
		if n < 0 && err != nil {
			n = 0
		} // x/net may return -1 after EAGAIN followed by a deadline.
		if n < 0 || n > len(messages) {
			for i := range messages {
				c.rx[i].Buffers[0] = nil
				c.rx[i].Addr = nil
			}
			return 0, io.ErrUnexpectedEOF
		}
		for i := 0; i < n; i++ {
			m := c.rx[i]
			messages[i].N = m.N
			messages[i].Peer = m.Addr
			messages[i].Truncated = messageTruncated(m.Flags) || m.N < 0 || m.N > len(messages[i].Payload)
		}
		for i := range messages {
			c.rx[i].Buffers[0] = nil
			c.rx[i].Addr = nil
		}
		if n != 0 || !unsupported(err) {
			if n == 0 && err == nil {
				err = io.ErrNoProgress
			}
			return n, err
		}
		c.readFallback.Store(true)
	}
	m := &messages[0]
	var err error
	if udp, ok := c.conn.(*net.UDPConn); ok {
		var flags int
		m.N, _, flags, m.Peer, err = udp.ReadMsgUDP(m.Payload, nil)
		m.Truncated = messageTruncated(flags)
	} else {
		m.N, m.Peer, err = c.conn.ReadFrom(m.Payload)
		// PacketConn cannot expose MSG_TRUNC; conservatively reject a full buffer.
		m.Truncated = m.N == len(m.Payload)
	}
	m.Truncated = m.Truncated || m.N < 0 || m.N > len(m.Payload)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Write returns the successfully transmitted prefix. The caller must advance
// by n before retrying; an error or zero progress must never cause a busy loop.
// This method never releases or retains payload storage.
func (c *Conn) Write(messages []Message) (int, error) {
	if len(messages) == 0 {
		return 0, nil
	}
	if len(messages) > len(c.rx) {
		messages = messages[:len(c.rx)]
	}
	if c.batch != nil && !c.writeFallback.Load() {
		count, hasGSO := c.prepareWrites(messages)
		n, err := c.batch.WriteBatch(c.tx[:count], 0)
		if n < 0 && err != nil {
			n = 0
		}
		if n < 0 || n > count {
			c.clearWrites(count)
			return 0, io.ErrShortWrite
		}
		sent := 0
		if n > 0 {
			sent = c.txEnds[n-1]
		}
		c.clearWrites(count)
		if hasGSO && gsoUnsupported(err) {
			c.gso.Store(false)
			// sendmmsg reports only its successful prefix. Never retransmit it.
			more, retryErr := c.Write(messages[sent:])
			return sent + more, retryErr
		}
		n = sent
		if n < 0 && err != nil {
			n = 0
		}
		if n < 0 || n > len(messages) {
			return 0, io.ErrShortWrite
		}
		if n != 0 || !unsupported(err) {
			if n == 0 && err == nil {
				err = io.ErrNoProgress
			}
			return n, err
		}
		c.writeFallback.Store(true)
	}
	m := messages[0]
	n, err := c.conn.WriteTo(m.Payload, m.Peer)
	if err != nil {
		return 0, err
	}
	if n != len(m.Payload) {
		return 0, io.ErrShortWrite
	}
	return 1, nil
}

func samePeer(a, b net.Addr) bool {
	x, ok := a.(*net.UDPAddr)
	y, yes := b.(*net.UDPAddr)
	return ok && yes && x != nil && y != nil && x.Port == y.Port && x.Zone == y.Zone && x.IP.Equal(y.IP)
}

func (c *Conn) prepareWrites(messages []Message) (int, bool) {
	count, used := 0, false
	for start := 0; start < len(messages); {
		end, size, total := start+1, len(messages[start].Payload), len(messages[start].Payload)
		if c.gso.Load() && size > 0 && size <= maxGSOPayload {
			for end < len(messages) && end-start < maxGSOSegments {
				next := messages[end]
				if !samePeer(messages[start].Peer, next.Peer) || len(next.Payload) == 0 || len(next.Payload) > size || total+len(next.Payload) > maxGSOPayload {
					break
				}
				total += len(next.Payload)
				end++
				if len(next.Payload) < size {
					break
				}
			}
		}
		for i := start; i < end; i++ {
			c.txVectors[i] = messages[i].Payload
		}
		c.tx[count].Buffers = c.txVectors[start:end]
		c.tx[count].Addr = messages[start].Peer
		c.tx[count].OOB = nil
		if end-start > 1 {
			c.tx[count].OOB = segmentControl(c.txOOB[count], size)
			used = true
		}
		c.txEnds[count] = end
		count++
		start = end
	}
	return count, used
}
func (c *Conn) clearWrites(count int) {
	for i := 0; i < count; i++ {
		c.tx[i].Buffers = nil
		c.tx[i].Addr = nil
		c.tx[i].OOB = nil
	}
	for i := range c.txVectors {
		c.txVectors[i] = nil
	}
}
