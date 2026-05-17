package syncedstate

import (
	"context"
	"encoding/json"
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
		Type:  MessageSet,
		ID:    "set",
		Name:  "TestState",
		Value: json.RawMessage(`{"name":"client","count":7}`),
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
