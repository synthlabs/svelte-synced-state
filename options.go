package syncedstate

import (
	"time"

	"github.com/coder/websocket"
)

type Option func(*Manager)

type KeyOption func(*keyConfig)

type keyConfig struct{}

type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	acceptOptions websocket.AcceptOptions
	sendBuffer    int
	writeTimeout  time.Duration
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

func WithWriteTimeout(timeout time.Duration) HandlerOption {
	return func(cfg *handlerConfig) {
		if timeout > 0 {
			cfg.writeTimeout = timeout
		}
	}
}
