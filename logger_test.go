package syncedstate

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type logRecord struct {
	level LogLevel
	msg   string
	args  []any
}

type capturingLogger struct {
	mu      sync.Mutex
	records []logRecord
}

func (l *capturingLogger) Debug(msg string, args ...any) {
	l.record(LevelDebug, msg, args...)
}

func (l *capturingLogger) Info(msg string, args ...any) {
	l.record(LevelInfo, msg, args...)
}

func (l *capturingLogger) Warn(msg string, args ...any) {
	l.record(LevelWarn, msg, args...)
}

func (l *capturingLogger) Error(msg string, args ...any) {
	l.record(LevelError, msg, args...)
}

func (l *capturingLogger) record(level LogLevel, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.records = append(l.records, logRecord{
		level: level,
		msg:   msg,
		args:  append([]any(nil), args...),
	})
}

func (l *capturingLogger) snapshot() []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]logRecord(nil), l.records...)
}

func waitForLog(t *testing.T, logger *capturingLogger, matches func(logRecord) bool) logRecord {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		for _, record := range logger.snapshot() {
			if matches(record) {
				return record
			}
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for log; records=%+v", logger.snapshot())
		case <-ticker.C:
		}
	}
}

func attrValue(record logRecord, key string) (any, bool) {
	for i := 0; i+1 < len(record.args); i += 2 {
		if record.args[i] == key {
			return record.args[i+1], true
		}
	}
	return nil, false
}

func mustLogPayload(t *testing.T, payload logPayload) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal log payload: %v", err)
	}
	return raw
}

func TestClientLogPayloadDispatchesByLevel(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	c := &client{id: 7, manager: manager}
	clientTime := "2026-06-19T10:00:00.123Z"

	c.handleMessage(nil, Message{
		Type: MessageLog,
		Value: mustLogPayload(t, logPayload{
			Level:     LevelTrace,
			Message:   "hello from ui",
			Timestamp: clientTime,
			Scope:     "ui",
		}),
	})

	record := waitForLog(t, logger, func(record logRecord) bool {
		return record.msg == "hello from ui"
	})
	if record.level != LevelDebug {
		t.Fatalf("level = %v, want %v", record.level, LevelDebug)
	}
	if got, _ := attrValue(record, "source"); got != "client" {
		t.Fatalf("source attr = %v", got)
	}
	if got, _ := attrValue(record, "scope"); got != "ui" {
		t.Fatalf("scope attr = %v", got)
	}
	if got, ok := attrValue(record, "client_time"); !ok {
		t.Fatal("missing client_time attr")
	} else if parsed, ok := got.(time.Time); !ok || parsed.Format(time.RFC3339Nano) != "2026-06-19T10:00:00.123Z" {
		t.Fatalf("client_time attr = %#v", got)
	}
}

func TestClientLogPayloadDefaultsInvalidLevelAndIgnoresBadTimestamp(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	c := &client{id: 7, manager: manager}

	c.handleMessage(nil, Message{
		Type: MessageLog,
		Value: mustLogPayload(t, logPayload{
			Level:     LogLevel(99),
			Message:   "bad timestamp",
			Timestamp: "not a time",
			Scope:     "ui",
		}),
	})

	record := waitForLog(t, logger, func(record logRecord) bool {
		return record.msg == "bad timestamp"
	})
	if record.level != LevelInfo {
		t.Fatalf("level = %v, want %v", record.level, LevelInfo)
	}
	if _, ok := attrValue(record, "client_time"); ok {
		t.Fatal("unexpected client_time attr")
	}
}

func TestMalformedClientLogPayloadWarnsWithoutClientReply(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	writeRawMessage(t, conn, `{"type":"log","value":"not an object"}`)

	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelWarn && record.msg == "malformed log payload from client"
	})
	if msg, ok := readMessageWithin(t, conn, 50*time.Millisecond); ok {
		t.Fatalf("unexpected reply to malformed log payload: %+v", msg)
	}
}

func TestDefaultLoggerLevelFiltersOutput(t *testing.T) {
	var out bytes.Buffer
	logger := newDefaultLogger(&out, LevelWarn)

	logger.Debug("debug hidden")
	logger.Info("info hidden")
	logger.Warn("warn visible")
	logger.Error("error visible")

	text := out.String()
	if strings.Contains(text, "debug hidden") || strings.Contains(text, "info hidden") {
		t.Fatalf("low-level log was not filtered: %s", text)
	}
	if !strings.Contains(text, "warn visible") || !strings.Contains(text, "error visible") {
		t.Fatalf("expected warn and error logs in output: %s", text)
	}
}

func TestSlogLoggerSatisfiesLogger(t *testing.T) {
	var _ Logger = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

func TestBackendInstrumentationLogsCoreEvents(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	key, err := Define(manager, "TestState", testState{Name: "initial"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	if _, err := Define(manager, "TestState", testState{}); err != ErrAlreadyDefined {
		t.Fatalf("duplicate define error = %v", err)
	}
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelWarn && record.msg == "state already defined"
	})

	if _, err := Lookup[testState](manager, "Missing"); err != ErrNotFound {
		t.Fatalf("missing lookup error = %v", err)
	}
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelDebug && record.msg == "lookup miss"
	})

	if err := key.Set(nil, testState{Name: "set"}, WithVersion(2)); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelDebug && record.msg == "committed write"
	})
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelDebug && record.msg == "broadcast"
	})

	if err := key.Update(nil, func(state *testState) {
		state.Count = 99
	}, WithVersion(2)); err != ErrVersionConflict {
		t.Fatalf("conflicting update error = %v", err)
	}
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelWarn && record.msg == "version conflict"
	})
}

func TestHandlerInstrumentationLogsConnectAndDisconnect(t *testing.T) {
	logger := &capturingLogger{}
	manager := NewManager(WithLogger(logger))
	server := httptest.NewServer(manager.Handler())
	t.Cleanup(server.Close)

	conn := dialClient(t, wsURL(server.URL))
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelInfo && record.msg == "client connected"
	})

	_ = conn.Close(websocket.StatusNormalClosure, "test done")
	waitForLog(t, logger, func(record logRecord) bool {
		return record.level == LevelInfo && record.msg == "client disconnected"
	})
}
