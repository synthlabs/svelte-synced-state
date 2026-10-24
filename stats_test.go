package syncedstate

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlerLogsClientStats(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	key, err := Define(manager, "StatsState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	server := httptest.NewServer(manager.Handler(WithStatsInterval(20*time.Millisecond), WithSlowWriteThreshold(time.Hour)))
	defer server.Close()

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "1", Name: "StatsState"})
	if snapshot := readMessage(t, conn); snapshot.Type != MessageSnapshot {
		t.Fatalf("first message = %+v, want snapshot", snapshot)
	}
	for i := 0; i < 3; i++ {
		if err := key.Update(context.Background(), func(s *testState) { s.Count++ }); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		if update := readMessage(t, conn); update.Type != MessageUpdate {
			t.Fatalf("message %d = %+v, want update", i, update)
		}
	}

	record := waitForLog(t, logger, func(r logRecord) bool {
		if r.msg != "client stats" {
			return false
		}
		written, _ := attrValue(r, "written")
		count, _ := written.(uint64)
		return count >= 4
	})
	if bytes, _ := attrValue(record, "bytes"); bytes.(uint64) == 0 {
		t.Fatalf("bytes = %v, want > 0", bytes)
	}
	if subscriptions, _ := attrValue(record, "subscriptions"); subscriptions.(int) != 1 {
		t.Fatalf("subscriptions = %v, want 1", subscriptions)
	}
	if slow, _ := attrValue(record, "slow_writes"); slow.(uint64) != 0 {
		t.Fatalf("slow_writes = %v, want 0", slow)
	}
	for _, name := range []string{"queue_depth", "queue_high", "max_queue_wait_ms", "max_write_ms", "buffer_full_total"} {
		if _, ok := attrValue(record, name); !ok {
			t.Fatalf("record %+v lacks %s", record, name)
		}
	}
}

func TestEnqueueTracksQueueHighWater(t *testing.T) {
	c := &client{send: make(chan outbound, 2), done: make(chan struct{})}
	defer close(c.done)

	for _, id := range []string{"a", "b"} {
		if ok := c.enqueue(context.Background(), Message{Type: MessageUpdate, ID: id}); !ok {
			t.Fatalf("enqueue %s returned false", id)
		}
	}
	if got := c.stats.enqueued.Load(); got != 2 {
		t.Fatalf("enqueued = %d, want 2", got)
	}
	if got := c.stats.queueHigh.Load(); got != 2 {
		t.Fatalf("queueHigh = %d, want 2", got)
	}
	first := <-c.send
	if first.msg.ID != "a" || first.enqueuedAt.IsZero() {
		t.Fatalf("first item = %+v", first)
	}
	<-c.send
}

func TestBufferFullEventsAreCounted(t *testing.T) {
	serverConn, _ := dialAcceptedConn(t)
	manager := NewManager(WithLogger(&capturingLogger{}))
	c := &client{
		manager: manager,
		conn:    serverConn,
		send:    make(chan outbound, 1),
		done:    make(chan struct{}),
	}

	c.send <- outbound{msg: Message{Type: MessageUpdate, ID: "first"}}
	c.enqueue(context.Background(), Message{Type: MessageUpdate, ID: "second"})

	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("client was not closed after send buffer filled")
	}
	if got := manager.BufferFullEvents(); got != 1 {
		t.Fatalf("BufferFullEvents = %d, want 1", got)
	}
}
