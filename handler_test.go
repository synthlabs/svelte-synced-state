package syncedstate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func dialClient(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close(websocket.StatusNormalClosure, "test done")
	})
	return conn
}

func dialAcceptedConn(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()

	accepted := make(chan *websocket.Conn, 1)
	acceptErr := make(chan error, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	clientConn := dialClient(t, wsURL(server.URL))

	var serverConn *websocket.Conn
	select {
	case serverConn = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("accept timeout")
	}
	t.Cleanup(func() {
		_ = serverConn.Close(websocket.StatusNormalClosure, "test done")
	})
	return serverConn, clientConn
}

func writeMessage(t *testing.T, conn *websocket.Conn, msg Message) {
	t.Helper()

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readMessage(t *testing.T, conn *websocket.Conn) Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("message type = %v", typ)
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return msg
}

func readMessageWithin(t *testing.T, conn *websocket.Conn, timeout time.Duration) (Message, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	typ, data, err := conn.Read(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return Message{}, false
		}
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("message type = %v", typ)
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return msg, true
}

func writeRawMessage(t *testing.T, conn *websocket.Conn, data string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
		t.Fatalf("write raw: %v", err)
	}
}

func decodeValue[T any](t *testing.T, msg Message) T {
	t.Helper()

	var value T
	if err := json.Unmarshal(msg.Value, &value); err != nil {
		t.Fatalf("value unmarshal: %v", err)
	}
	return value
}

func TestClientEnqueueClosesWhenSendBufferFull(t *testing.T) {
	serverConn, _ := dialAcceptedConn(t)
	c := &client{
		conn: serverConn,
		send: make(chan Message, 1),
		done: make(chan struct{}),
	}

	c.send <- Message{Type: MessageUpdate, ID: "first"}
	if ok := c.enqueue(context.Background(), Message{Type: MessageUpdate, ID: "second"}); !ok {
		t.Fatal("enqueue returned false before context cancellation")
	}

	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("client was not closed after send buffer filled")
	}
}

func TestClientEnqueueBlocksWhenSendBufferFull(t *testing.T) {
	serverConn, _ := dialAcceptedConn(t)
	c := &client{
		conn:              serverConn,
		send:              make(chan Message, 1),
		blockOnFullBuffer: true,
		done:              make(chan struct{}),
	}

	c.send <- Message{Type: MessageUpdate, ID: "first"}
	enqueued := make(chan bool, 1)
	go func() {
		enqueued <- c.enqueue(context.Background(), Message{Type: MessageUpdate, ID: "second"})
	}()

	select {
	case <-enqueued:
		t.Fatal("enqueue completed while send buffer was full")
	case <-time.After(50 * time.Millisecond):
	}

	if msg := <-c.send; msg.ID != "first" {
		t.Fatalf("first queued message = %+v", msg)
	}

	select {
	case ok := <-enqueued:
		if !ok {
			t.Fatal("enqueue returned false without context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue did not complete after send buffer space was available")
	}

	if msg := <-c.send; msg.ID != "second" {
		t.Fatalf("second queued message = %+v", msg)
	}
}

func TestClientEnqueueStopsBlockingWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		send:              make(chan Message, 1),
		blockOnFullBuffer: true,
		done:              make(chan struct{}),
	}
	defer close(c.done)

	c.send <- Message{Type: MessageUpdate, ID: "first"}
	enqueued := make(chan bool, 1)
	go func() {
		enqueued <- c.enqueue(ctx, Message{Type: MessageUpdate, ID: "second"})
	}()

	select {
	case <-enqueued:
		t.Fatal("enqueue completed while send buffer was full")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case ok := <-enqueued:
		if ok {
			t.Fatal("enqueue returned true after context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue did not stop after context cancellation")
	}
}

func TestHandlerSubscribeSnapshotAndBroadcast(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	first := dialClient(t, wsURL(server.URL))
	second := dialClient(t, wsURL(server.URL))

	writeMessage(t, first, Message{Type: MessageSubscribe, ID: "first-sub", Name: "TestState"})
	firstSnapshot := readMessage(t, first)
	if firstSnapshot.Type != MessageSnapshot || firstSnapshot.ID != "first-sub" || firstSnapshot.Version != 1 {
		t.Fatalf("first snapshot = %+v", firstSnapshot)
	}

	writeMessage(t, second, Message{Type: MessageSubscribe, ID: "second-sub", Name: "TestState"})
	secondSnapshot := readMessage(t, second)
	if secondSnapshot.Type != MessageSnapshot || secondSnapshot.ID != "second-sub" || secondSnapshot.Version != 1 {
		t.Fatalf("second snapshot = %+v", secondSnapshot)
	}

	if err := key.Update(context.Background(), func(state *testState) {
		state.Count = 2
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	for _, conn := range []*websocket.Conn{first, second} {
		msg := readMessage(t, conn)
		if msg.Type != MessageUpdate || msg.Name != "TestState" || msg.Version != 2 {
			t.Fatalf("update = %+v", msg)
		}
		var value testState
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			t.Fatalf("value unmarshal: %v", err)
		}
		if value.Count != 2 {
			t.Fatalf("value = %+v", value)
		}
	}
}

func TestHandlerPingsKeepHealthyClientsAlive(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler(WithPingInterval(50 * time.Millisecond)))
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))

	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "sub", Name: "TestState"})

	// Read continuously so the client answers server pings — a real browser always
	// processes incoming frames; a coder/websocket client only auto-pongs while a
	// Read is in flight. Collect updates for the assertion below.
	updates := make(chan Message, 16)
	readErr := make(chan error, 1)
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			typ, data, err := conn.Read(ctx)
			cancel()
			if err != nil {
				readErr <- err
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			var msg Message
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if msg.Type == MessageUpdate {
				select {
				case updates <- msg:
				default:
				}
			}
		}
	}()

	// Wait well past several ping intervals; a healthy client must stay connected.
	// If the ping path wrongly closed the connection, readErr fires here.
	time.Sleep(250 * time.Millisecond)
	select {
	case err := <-readErr:
		t.Fatalf("connection closed during ping window: %v", err)
	default:
	}

	if err := key.Update(context.Background(), func(state *testState) {
		state.Count = 2
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	select {
	case msg := <-updates:
		if msg.Type != MessageUpdate || msg.Version != 2 {
			t.Fatalf("update = %+v", msg)
		}
	case err := <-readErr:
		t.Fatalf("connection closed before update arrived: %v", err)
	case <-time.After(time.Second):
		t.Fatal("update not received after pings")
	}
}

func TestHandlerSetFromClient(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "sub", Name: "TestState"})
	_ = readMessage(t, conn)

	writeMessage(t, conn, Message{
		Type:    MessageSet,
		ID:      "set",
		Name:    "TestState",
		Version: 2,
		Value:   json.RawMessage(`{"name":"client","count":7}`),
	})
	update := readMessage(t, conn)
	if update.Type != MessageUpdate || update.ID != "set" || update.Version != 2 {
		t.Fatalf("update = %+v", update)
	}

	value, meta, err := key.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if value.Name != "client" || value.Count != 7 || meta.Version != 2 {
		t.Fatalf("value=%+v meta=%+v", value, meta)
	}
}

func TestHandlerStaleSetReturnsLatestSnapshotWithError(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "sub", Name: "TestState"})
	_ = readMessage(t, conn)

	if err := key.Set(context.Background(), testState{Name: "server", Count: 2}); err != nil {
		t.Fatalf("server set: %v", err)
	}
	_ = readMessage(t, conn)

	writeMessage(t, conn, Message{
		Type:    MessageSet,
		ID:      "stale",
		Name:    "TestState",
		Version: 2,
		Value:   json.RawMessage(`{"name":"client","count":7}`),
	})

	snapshot := readMessage(t, conn)
	if snapshot.Type != MessageSnapshot || snapshot.ID != "stale" || snapshot.Version != 2 || snapshot.Error != ErrVersionConflict.Error() {
		t.Fatalf("stale snapshot = %+v", snapshot)
	}
	value := decodeValue[testState](t, snapshot)
	if value.Name != "server" || value.Count != 2 {
		t.Fatalf("stale snapshot value = %+v", value)
	}

	stored, meta, err := key.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if stored.Name != "server" || stored.Count != 2 || meta.Version != 2 {
		t.Fatalf("stored value=%+v meta=%+v", stored, meta)
	}
}

func TestHandlerSetWithoutVersionReturnsLatestSnapshotWithError(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "sub", Name: "TestState"})
	_ = readMessage(t, conn)

	writeMessage(t, conn, Message{
		Type:  MessageSet,
		ID:    "missing-version",
		Name:  "TestState",
		Value: json.RawMessage(`{"name":"client","count":7}`),
	})

	snapshot := readMessage(t, conn)
	if snapshot.Type != MessageSnapshot || snapshot.ID != "missing-version" || snapshot.Version != 1 || snapshot.Error != ErrVersionConflict.Error() {
		t.Fatalf("missing-version snapshot = %+v", snapshot)
	}
	value := decodeValue[testState](t, snapshot)
	if value.Name != "initial" || value.Count != 1 {
		t.Fatalf("missing-version snapshot value = %+v", value)
	}

	stored, meta, err := key.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if stored.Name != "initial" || stored.Count != 1 || meta.Version != 1 {
		t.Fatalf("stored value=%+v meta=%+v", stored, meta)
	}
}

func TestHandlerSnapshotRequestDoesNotSubscribe(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSnapshot, ID: "snapshot", Name: "TestState"})

	snapshot := readMessage(t, conn)
	if snapshot.Type != MessageSnapshot || snapshot.ID != "snapshot" || snapshot.Name != "TestState" || snapshot.Version != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	value := decodeValue[testState](t, snapshot)
	if value.Name != "initial" || value.Count != 1 {
		t.Fatalf("snapshot value = %+v", value)
	}

	if err := key.Update(context.Background(), func(state *testState) {
		state.Count = 2
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if msg, ok := readMessageWithin(t, conn, 50*time.Millisecond); ok {
		t.Fatalf("unexpected message after snapshot-only request: %+v", msg)
	}
}

func TestHandlerUnsubscribeStopsBroadcast(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	first := dialClient(t, wsURL(server.URL))
	second := dialClient(t, wsURL(server.URL))

	writeMessage(t, first, Message{Type: MessageSubscribe, ID: "first-sub", Name: "TestState"})
	_ = readMessage(t, first)
	writeMessage(t, second, Message{Type: MessageSubscribe, ID: "second-sub", Name: "TestState"})
	_ = readMessage(t, second)

	writeMessage(t, first, Message{Type: MessageUnsubscribe, ID: "first-unsub", Name: "TestState"})
	writeMessage(t, first, Message{Type: MessageSnapshot, ID: "first-barrier", Name: "TestState"})
	barrier := readMessage(t, first)
	if barrier.Type != MessageSnapshot || barrier.ID != "first-barrier" || barrier.Name != "TestState" {
		t.Fatalf("barrier snapshot = %+v", barrier)
	}

	if err := key.Update(context.Background(), func(state *testState) {
		state.Count = 2
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	update := readMessage(t, second)
	if update.Type != MessageUpdate || update.Name != "TestState" || update.Version != 2 {
		t.Fatalf("second update = %+v", update)
	}
	if msg, ok := readMessageWithin(t, first, 50*time.Millisecond); ok {
		t.Fatalf("unexpected message after unsubscribe: %+v", msg)
	}
}

func TestHandlerErrors(t *testing.T) {
	manager := NewManager()
	if _, err := Define(manager, "TestState", testState{}); err != nil {
		t.Fatalf("define: %v", err)
	}
	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "missing", Name: "Missing"})

	msg := readMessage(t, conn)
	if msg.Type != MessageError || msg.ID != "missing" || msg.Name != "Missing" {
		t.Fatalf("missing error = %+v", msg)
	}

	writeMessage(t, conn, Message{Type: MessageSet, ID: "missing-value", Name: "TestState"})
	msg = readMessage(t, conn)
	if msg.Type != MessageError || msg.ID != "missing-value" || msg.Name != "TestState" || msg.Error != ErrMissingValue.Error() {
		t.Fatalf("missing value error = %+v", msg)
	}

	writeMessage(t, conn, Message{Type: MessageType("bogus"), ID: "unknown", Name: "TestState"})
	msg = readMessage(t, conn)
	if msg.Type != MessageError || msg.ID != "unknown" || msg.Name != "TestState" || msg.Error != errUnknownMessageType.Error() {
		t.Fatalf("unknown type error = %+v", msg)
	}

	writeRawMessage(t, conn, "{")
	msg = readMessage(t, conn)
	if msg.Type != MessageError || msg.Error == "" {
		t.Fatalf("malformed json error = %+v", msg)
	}
}

func TestHandlerBroadcastsOnlyToSubscribedKey(t *testing.T) {
	manager := NewManager()
	firstKey, err := Define(manager, "First", testState{Name: "first"})
	if err != nil {
		t.Fatalf("define first: %v", err)
	}
	secondKey, err := Define(manager, "Second", testState{Name: "second"})
	if err != nil {
		t.Fatalf("define second: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	firstClient := dialClient(t, wsURL(server.URL))
	secondClient := dialClient(t, wsURL(server.URL))

	writeMessage(t, firstClient, Message{Type: MessageSubscribe, ID: "first-sub", Name: "First"})
	_ = readMessage(t, firstClient)
	writeMessage(t, secondClient, Message{Type: MessageSubscribe, ID: "second-sub", Name: "Second"})
	_ = readMessage(t, secondClient)

	if err := firstKey.Set(context.Background(), testState{Name: "first", Count: 1}); err != nil {
		t.Fatalf("set first: %v", err)
	}
	firstUpdate := readMessage(t, firstClient)
	if firstUpdate.Type != MessageUpdate || firstUpdate.Name != "First" || firstUpdate.Version != 2 {
		t.Fatalf("first update = %+v", firstUpdate)
	}

	if err := secondKey.Set(context.Background(), testState{Name: "second", Count: 1}); err != nil {
		t.Fatalf("set second: %v", err)
	}
	secondUpdate := readMessage(t, secondClient)
	if secondUpdate.Type != MessageUpdate || secondUpdate.Name != "Second" || secondUpdate.Version != 2 {
		t.Fatalf("second update = %+v", secondUpdate)
	}
	if msg, ok := readMessageWithin(t, firstClient, 50*time.Millisecond); ok {
		t.Fatalf("unexpected first client message after second update: %+v", msg)
	}
}

func TestHandlerMultipleClientsSeeConcurrentServerUpdates(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "Counter", testState{Name: "counter"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	lookup, err := Lookup[testState](manager, "Counter")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	server := httptest.NewServer(manager.Handler(WithSendBuffer(128)))
	t.Cleanup(server.Close)

	first := dialClient(t, wsURL(server.URL))
	second := dialClient(t, wsURL(server.URL))
	writeMessage(t, first, Message{Type: MessageSubscribe, ID: "first-sub", Name: "Counter"})
	_ = readMessage(t, first)
	writeMessage(t, second, Message{Type: MessageSubscribe, ID: "second-sub", Name: "Counter"})
	_ = readMessage(t, second)

	errs := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	update := func(key *Key[testState]) {
		defer wg.Done()

		<-start
		locked, err := key.Lock(context.Background())
		if err != nil {
			errs <- err
			return
		}
		locked.Value().Count++
		err = locked.Sync(context.Background())
		locked.Unlock()
		if err != nil {
			errs <- err
		}
	}

	wg.Add(2)
	go update(key)
	go update(lookup)
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("server update: %v", err)
	}

	assertUpdates := func(conn *websocket.Conn) {
		t.Helper()

		seenVersions := make(map[uint64]testState)
		for range 2 {
			msg := readMessage(t, conn)
			if msg.Type != MessageUpdate || msg.Name != "Counter" {
				t.Fatalf("update = %+v", msg)
			}
			seenVersions[msg.Version] = decodeValue[testState](t, msg)
		}
		for version := uint64(2); version <= 3; version++ {
			value, ok := seenVersions[version]
			if !ok {
				t.Fatalf("missing version %d in updates: %+v", version, seenVersions)
			}
			if value.Count != int(version-1) {
				t.Fatalf("version %d value = %+v", version, value)
			}
		}
	}

	assertUpdates(first)
	assertUpdates(second)

	value, meta, err := key.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if value.Count != 2 || meta.Version != 3 {
		t.Fatalf("snapshot value=%+v meta=%+v", value, meta)
	}
}

func TestHandlerWildcardSubscribeReceivesFutureIndexedUpdates(t *testing.T) {
	manager := NewManager()
	if _, err := Define(manager, "Ready", testState{Name: "ready"}); err != nil {
		t.Fatalf("define ready: %v", err)
	}
	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "wildcard-sub", Name: "customer:*"})
	writeMessage(t, conn, Message{Type: MessageSnapshot, ID: "barrier", Name: "Ready"})
	if msg := readMessage(t, conn); msg.Type != MessageSnapshot || msg.ID != "barrier" {
		t.Fatalf("barrier snapshot = %+v", msg)
	}

	key, err := DefineIndexed(manager, "customer", "123", testState{Name: "customer"})
	if err != nil {
		t.Fatalf("define indexed: %v", err)
	}
	if err := key.Set(context.Background(), testState{Name: "customer", Count: 1}); err != nil {
		t.Fatalf("set indexed: %v", err)
	}

	update := readMessage(t, conn)
	if update.Type != MessageUpdate || update.Name != "customer:123" || update.Version != 2 {
		t.Fatalf("wildcard update = %+v", update)
	}
	value := decodeValue[testState](t, update)
	if value.Count != 1 {
		t.Fatalf("wildcard update value = %+v", value)
	}
}

func TestHandlerWildcardFanoutAndExactFanout(t *testing.T) {
	manager := NewManager()
	customer, err := DefineIndexed(manager, "customer", "123", testState{Name: "customer"})
	if err != nil {
		t.Fatalf("define customer: %v", err)
	}
	order, err := DefineIndexed(manager, "order", "123", testState{Name: "order"})
	if err != nil {
		t.Fatalf("define order: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	exact := dialClient(t, wsURL(server.URL))
	wildcard := dialClient(t, wsURL(server.URL))

	writeMessage(t, exact, Message{Type: MessageSubscribe, ID: "exact-sub", Name: "customer:123"})
	_ = readMessage(t, exact)
	writeMessage(t, wildcard, Message{Type: MessageSubscribe, ID: "wildcard-sub", Name: "customer:*"})
	writeMessage(t, wildcard, Message{Type: MessageSnapshot, ID: "wildcard-barrier", Name: "customer:123"})
	if msg := readMessage(t, wildcard); msg.Type != MessageSnapshot || msg.ID != "wildcard-barrier" {
		t.Fatalf("wildcard barrier snapshot = %+v", msg)
	}

	if err := customer.Set(context.Background(), testState{Name: "customer", Count: 1}); err != nil {
		t.Fatalf("set customer: %v", err)
	}
	for _, conn := range []*websocket.Conn{exact, wildcard} {
		update := readMessage(t, conn)
		if update.Type != MessageUpdate || update.Name != "customer:123" || update.Version != 2 {
			t.Fatalf("customer update = %+v", update)
		}
	}

	if err := order.Set(context.Background(), testState{Name: "order", Count: 1}); err != nil {
		t.Fatalf("set order: %v", err)
	}
	if msg, ok := readMessageWithin(t, wildcard, 50*time.Millisecond); ok {
		t.Fatalf("unexpected wildcard message for other scope: %+v", msg)
	}
}

func TestHandlerWildcardDeduplicatesClientSubscribedToExactAndWildcard(t *testing.T) {
	manager := NewManager()
	key, err := DefineIndexed(manager, "customer", "123", testState{Name: "customer"})
	if err != nil {
		t.Fatalf("define indexed: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "exact-sub", Name: "customer:123"})
	_ = readMessage(t, conn)
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "wildcard-sub", Name: "customer:*"})
	writeMessage(t, conn, Message{Type: MessageSnapshot, ID: "wildcard-barrier", Name: "customer:123"})
	if msg := readMessage(t, conn); msg.Type != MessageSnapshot || msg.ID != "wildcard-barrier" {
		t.Fatalf("wildcard barrier snapshot = %+v", msg)
	}

	if err := key.Set(context.Background(), testState{Name: "customer", Count: 1}); err != nil {
		t.Fatalf("set indexed: %v", err)
	}
	update := readMessage(t, conn)
	if update.Type != MessageUpdate || update.Name != "customer:123" || update.Version != 2 {
		t.Fatalf("update = %+v", update)
	}
	if msg, ok := readMessageWithin(t, conn, 50*time.Millisecond); ok {
		t.Fatalf("duplicate update = %+v", msg)
	}
}

func TestHandlerWildcardUnsubscribeStopsDelivery(t *testing.T) {
	manager := NewManager()
	key, err := DefineIndexed(manager, "customer", "123", testState{Name: "customer"})
	if err != nil {
		t.Fatalf("define indexed: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSubscribe, ID: "wildcard-sub", Name: "customer:*"})
	writeMessage(t, conn, Message{Type: MessageUnsubscribe, ID: "wildcard-unsub", Name: "customer:*"})
	writeMessage(t, conn, Message{Type: MessageSnapshot, ID: "barrier", Name: "customer:123"})
	_ = readMessage(t, conn)

	if err := key.Set(context.Background(), testState{Name: "customer", Count: 1}); err != nil {
		t.Fatalf("set indexed: %v", err)
	}
	if msg, ok := readMessageWithin(t, conn, 50*time.Millisecond); ok {
		t.Fatalf("unexpected wildcard message after unsubscribe: %+v", msg)
	}
}

func TestHandlerWildcardSnapshotAndSetReturnErrors(t *testing.T) {
	manager := NewManager()
	if _, err := DefineIndexed(manager, "customer", "123", testState{Name: "customer"}); err != nil {
		t.Fatalf("define indexed: %v", err)
	}

	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeMessage(t, conn, Message{Type: MessageSnapshot, ID: "wildcard-snapshot", Name: "customer:*"})
	msg := readMessage(t, conn)
	if msg.Type != MessageError || msg.ID != "wildcard-snapshot" || msg.Error != ErrWildcardName.Error() {
		t.Fatalf("wildcard snapshot error = %+v", msg)
	}

	writeMessage(t, conn, Message{
		Type:  MessageSet,
		ID:    "wildcard-set",
		Name:  "customer:*",
		Value: json.RawMessage(`{"name":"customer","count":1}`),
	})
	msg = readMessage(t, conn)
	if msg.Type != MessageError || msg.ID != "wildcard-set" || msg.Error != ErrWildcardName.Error() {
		t.Fatalf("wildcard set error = %+v", msg)
	}
}
