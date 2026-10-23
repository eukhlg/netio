package udptransport

import (
	"io"
	"net"
)

// Internal storage keeps unread segments alive across calls, without retaining
// caller buffers. A new socket read only happens after every pending segment.
func (c *Conn) readGRO(messages []Message) (int, error) {
	if c.groIndex == c.groCount {
		c.groIndex, c.groCount, c.groOffset = 0, 0, 0
		for i := range c.rx {
			c.rx[i].Buffers[0] = c.groBuffers[i]
			c.rx[i].OOB = c.groOOB[i]
			c.rx[i].N = 0
			c.rx[i].NN = 0
			c.rx[i].Flags = 0
			c.rx[i].Addr = nil
		}
		var n int
		var err error
		if c.batch != nil && !c.readFallback.Load() {
			n, err = c.batch.ReadBatch(c.rx, 0)
			if n < 0 && err != nil {
				n = 0
			}
			if n == 0 && unsupported(err) {
				c.readFallback.Store(true)
			}
		}
		if c.batch == nil || c.readFallback.Load() {
			udp := c.conn.(*net.UDPConn)
			m := &c.rx[0]
			m.N, m.NN, m.Flags, m.Addr, err = udp.ReadMsgUDP(c.groBuffers[0], c.groOOB[0])
			if err == nil {
				n = 1
			}
		}
		if n < 0 || n > len(c.rx) {
			return 0, io.ErrUnexpectedEOF
		}
		if n == 0 {
			if err == nil {
				err = io.ErrNoProgress
			}
			return 0, err
		}
		c.groCount, c.groErr = n, err
	}
	n := 0
	for n < len(messages) && c.groIndex < c.groCount {
		m := &c.rx[c.groIndex]
		invalid := messageTruncated(m.Flags) || controlTruncated(m.Flags) || m.N < 0 || m.N > len(c.groBuffers[c.groIndex]) || m.NN < 0 || m.NN > len(m.OOB)
		segment := 0
		if !invalid {
			var err error
			segment, err = groSegmentSize(m.OOB[:m.NN])
			invalid = err != nil
		}
		end := m.N
		if invalid {
			end = 0
		} else if segment > 0 && c.groOffset+segment < end {
			end = c.groOffset + segment
		}
		slot := &messages[n]
		slot.Peer = m.Addr
		slot.Truncated = invalid
		if !invalid {
			slot.N = copy(slot.Payload, c.groBuffers[c.groIndex][c.groOffset:end])
			slot.Truncated = slot.N != end-c.groOffset
		} else {
			slot.N = 0
		}
		n++
		if invalid || end == m.N {
			m.Addr = nil
			c.groIndex++
			c.groOffset = 0
		} else {
			c.groOffset = end
		}
	}
	if c.groIndex == c.groCount {
		err := c.groErr
		c.groErr = nil
		return n, err
	}
	return n, nil
}
