package syncedstate

import (
	"sync/atomic"
	"time"
)

// outbound is a queued message plus the time it entered the client's send
// buffer, so the write loop can report how long frames wait behind each other.
type outbound struct {
	msg        Message
	enqueuedAt time.Time
}

// clientStats counts per-connection send work. Counters are cumulative; the
// maxima reset on every stats report so each log line describes one interval.
type clientStats struct {
	enqueued     atomic.Uint64
	written      atomic.Uint64
	bytesWritten atomic.Uint64
	slowWrites   atomic.Uint64
	queueHigh    atomic.Int64
	maxQueueWait atomic.Int64
	maxWrite     atomic.Int64
}

func (s *clientStats) recordEnqueue(depth int) {
	s.enqueued.Add(1)
	storeMax(&s.queueHigh, int64(depth))
}

func (s *clientStats) recordWrite(bytes int, queueWait, duration, slowThreshold time.Duration) {
	s.written.Add(1)
	s.bytesWritten.Add(uint64(bytes))
	storeMax(&s.maxQueueWait, int64(queueWait))
	storeMax(&s.maxWrite, int64(duration))
	if slowThreshold > 0 && duration > slowThreshold {
		s.slowWrites.Add(1)
	}
}

func storeMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func millis(ns int64) float64 {
	return float64(ns) / float64(time.Millisecond)
}

// logStats emits one "client stats" record and resets the interval maxima.
func (c *client) logStats() {
	if c.manager == nil || c.manager.logger == nil {
		return
	}
	c.manager.mu.RLock()
	subscriptions := len(c.subscriptions)
	c.manager.mu.RUnlock()

	s := &c.stats
	c.manager.logger.Info("client stats",
		"component", "handler",
		"client", c.id,
		"subscriptions", subscriptions,
		"enqueued", s.enqueued.Load(),
		"written", s.written.Load(),
		"bytes", s.bytesWritten.Load(),
		"queue_depth", len(c.send),
		"queue_high", s.queueHigh.Swap(0),
		"max_queue_wait_ms", millis(s.maxQueueWait.Swap(0)),
		"max_write_ms", millis(s.maxWrite.Swap(0)),
		"slow_writes", s.slowWrites.Load(),
		"buffer_full_total", c.manager.bufferFull.Load(),
	)
}

// BufferFullEvents returns how many client connections were closed because
// their send buffer overflowed since the manager was created.
func (m *Manager) BufferFullEvents() uint64 {
	return m.bufferFull.Load()
}
