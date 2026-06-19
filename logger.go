package syncedstate

import (
	"io"
	"log/slog"
	"os"
	"time"
)

type LogLevel int

const (
	LevelTrace LogLevel = 1
	LevelDebug LogLevel = 2
	LevelInfo  LogLevel = 3
	LevelWarn  LogLevel = 4
	LevelError LogLevel = 5
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type logPayload struct {
	Level     LogLevel `json:"level"`
	Message   string   `json:"message"`
	Timestamp string   `json:"timestamp"`
	Scope     string   `json:"scope"`
}

func defaultLogger(level LogLevel) Logger {
	return newDefaultLogger(os.Stdout, level)
}

func newDefaultLogger(w io.Writer, level LogLevel) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: toSlogLevel(mapLogLevel(level)),
	}))
}

func mapLogLevel(level LogLevel) LogLevel {
	if level == LevelTrace {
		return LevelDebug
	}
	if !validLogLevel(level) {
		return LevelInfo
	}
	return level
}

func validLogLevel(level LogLevel) bool {
	return level >= LevelTrace && level <= LevelError
}

func toSlogLevel(level LogLevel) slog.Level {
	switch mapLogLevel(level) {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func clientLogAttrs(payload logPayload) []any {
	attrs := []any{
		"source", "client",
		"scope", payload.Scope,
	}
	if payload.Timestamp != "" {
		if clientTime, err := time.Parse(time.RFC3339Nano, payload.Timestamp); err == nil {
			attrs = append(attrs, "client_time", clientTime)
		}
	}
	return attrs
}
