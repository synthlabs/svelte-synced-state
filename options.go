package syncedstate

import (
	"time"

	"github.com/coder/websocket"
)

type Option func(*Manager)

type KeyOption func(*keyConfig)

type keyConfig struct{}

type WriteOption func(*writeConfig)

type writeConfig struct {
	checkVersion bool
	version      uint64
}

type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	acceptOptions     websocket.AcceptOptions
	sendBuffer        int
	blockOnFullBuffer bool
	writeTimeout      time.Duration
	pingInterval      time.Duration
}

func defaultHandlerConfig() handlerConfig {
	return handlerConfig{
		sendBuffer:   32,
		writeTimeout: 10 * time.Second,
	}
}

func WithOriginPatterns(patterns ...string) HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.acceptOptions.OriginPatterns = append([]string(nil), patterns...)
	}
}

func WithSendBuffer(size int) HandlerOption {
	return func(cfg *handlerConfig) {
		if size > 0 {
			cfg.sendBuffer = size
		}
	}
}

func WithBlockOnFullBuffer() HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.blockOnFullBuffer = true
	}
}

func WithWriteTimeout(timeout time.Duration) HandlerOption {
	return func(cfg *handlerConfig) {
		if timeout > 0 {
			cfg.writeTimeout = timeout
		}
	}
}

// WithPingInterval enables server-side keepalive: the handler pings each client on a
// ticker and closes the connection if a pong is not received within the interval. This
// surfaces half-open / silently-dropped links (laptop sleep, NAT idle-reap) as a real
// close event so the client can reconnect. Zero (the default) disables keepalive.
func WithPingInterval(interval time.Duration) HandlerOption {
	return func(cfg *handlerConfig) {
		if interval > 0 {
			cfg.pingInterval = interval
		}
	}
}

func WithVersion(version uint64) WriteOption {
	return func(cfg *writeConfig) {
		cfg.checkVersion = true
		cfg.version = version
	}
}

func writeOptions(opts []WriteOption) writeConfig {
	cfg := writeConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
