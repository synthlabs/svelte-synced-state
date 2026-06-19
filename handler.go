package syncedstate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type client struct {
	id                uint64
	manager           *Manager
	conn              *websocket.Conn
	send              chan Message
	blockOnFullBuffer bool
	subscriptions     map[string]struct{}
	done              chan struct{}
	closeOnce         sync.Once
}

func (m *Manager) Handler(opts ...HandlerOption) http.Handler {
	cfg := defaultHandlerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &cfg.acceptOptions)
		if err != nil {
			return
		}

		c := &client{
			id:                m.nextClient.Add(1),
			manager:           m,
			conn:              conn,
			send:              make(chan Message, cfg.sendBuffer),
			blockOnFullBuffer: cfg.blockOnFullBuffer,
			subscriptions:     make(map[string]struct{}),
			done:              make(chan struct{}),
		}

		m.logger.Info("client connected", "component", "handler", "client", c.id)
		ctx := r.Context()
		go c.writeLoop(ctx, cfg)
		c.readLoop(ctx)
	})
}

func (c *client) readLoop(ctx context.Context) {
	defer func() {
		c.close(websocket.StatusNormalClosure, "closed")
		c.manager.removeClient(c)
		c.manager.logger.Info("client disconnected", "component", "handler", "client", c.id)
	}()

	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			c.manager.logger.Warn("bad websocket frame", "component", "handler", "client", c.id, "err", errUnexpectedMessageType)
			c.enqueue(ctx, errorMessage("", "", errUnexpectedMessageType))
			continue
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.manager.logger.Warn("malformed websocket message", "component", "handler", "client", c.id, "err", err)
			c.enqueue(ctx, errorMessage("", "", err))
			continue
		}
		c.manager.logger.Debug("message", "component", "handler", "client", c.id, "type", msg.Type, "name", msg.Name)
		c.handleMessage(ctx, msg)
	}
}

func (c *client) handleMessage(ctx context.Context, msg Message) {
	switch msg.Type {
	case MessageSubscribe:
		if err := c.manager.subscribe(c, msg.Name); err != nil {
			c.manager.logger.Warn("subscribe failed", "component", "handler", "client", c.id, "name", msg.Name, "err", err)
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		if isWildcardAddress(msg.Name) {
			return
		}
		e, _ := c.manager.entry(msg.Name)
		snapshot, err := e.snapshotMessage(MessageSnapshot, msg.ID)
		if err != nil {
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.enqueue(ctx, snapshot)
	case MessageUnsubscribe:
		c.manager.unsubscribe(c, msg.Name)
	case MessageSnapshot:
		if err := rejectWildcardName(msg.Name); err != nil {
			c.manager.logger.Warn("snapshot rejected", "component", "handler", "client", c.id, "name", msg.Name, "err", err)
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		e, err := c.manager.entry(msg.Name)
		if err != nil {
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		snapshot, err := e.snapshotMessage(MessageSnapshot, msg.ID)
		if err != nil {
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.enqueue(ctx, snapshot)
	case MessageSet:
		if err := rejectWildcardName(msg.Name); err != nil {
			c.manager.logger.Warn("set rejected", "component", "handler", "client", c.id, "name", msg.Name, "err", err)
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		e, err := c.manager.entry(msg.Name)
		if err != nil {
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		update, err := e.setRaw(msg.Value, msg.ID, WithVersion(msg.Version))
		if err != nil {
			if errors.Is(err, ErrVersionConflict) {
				c.manager.logger.Warn("version conflict", "component", "handler", "client", c.id, "name", msg.Name, "expected", msg.Version)
				snapshot, snapshotErr := e.snapshotMessage(MessageSnapshot, msg.ID)
				if snapshotErr != nil {
					c.enqueue(ctx, errorMessage(msg.ID, msg.Name, snapshotErr))
					return
				}
				snapshot.Error = err.Error()
				c.enqueue(ctx, snapshot)
				return
			}
			c.enqueue(ctx, errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.manager.broadcast(ctx, update)
	case MessageLog:
		c.handleLogMessage(msg)
	default:
		c.manager.logger.Warn("unknown message type", "component", "handler", "client", c.id, "type", msg.Type, "name", msg.Name)
		c.enqueue(ctx, errorMessage(msg.ID, msg.Name, errUnknownMessageType))
	}
}

func (c *client) handleLogMessage(msg Message) {
	var payload logPayload
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		c.manager.logger.Warn("malformed log payload from client", "component", "handler", "client", c.id, "err", err)
		return
	}

	attrs := clientLogAttrs(payload)
	switch mapLogLevel(payload.Level) {
	case LevelDebug:
		c.manager.logger.Debug(payload.Message, attrs...)
	case LevelInfo:
		c.manager.logger.Info(payload.Message, attrs...)
	case LevelWarn:
		c.manager.logger.Warn(payload.Message, attrs...)
	case LevelError:
		c.manager.logger.Error(payload.Message, attrs...)
	default:
		c.manager.logger.Info(payload.Message, attrs...)
	}
}

func rejectWildcardName(name string) error {
	if _, wildcard, err := parseWildcardAddress(name); err != nil {
		return err
	} else if wildcard {
		return ErrWildcardName
	}
	return nil
}

func (c *client) writeLoop(ctx context.Context, cfg handlerConfig) {
	defer c.close(websocket.StatusNormalClosure, "closed")

	// Keepalive: ping on a ticker and close if no pong arrives within the interval.
	// This runs inside the single write loop so pings never race message writes.
	var pings <-chan time.Time
	if cfg.pingInterval > 0 {
		ticker := time.NewTicker(cfg.pingInterval)
		defer ticker.Stop()
		pings = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case msg := <-c.send:
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			writeCtx, cancel := context.WithTimeout(ctx, cfg.writeTimeout)
			err = c.conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}
		case <-pings:
			// A missed pong makes Ping return within this deadline; closing here
			// surfaces the dead link so the client can reconnect.
			pingCtx, cancel := context.WithTimeout(ctx, cfg.pingInterval)
			err := c.conn.Ping(pingCtx)
			cancel()
			if err != nil {
				c.manager.logger.Warn("ping timeout", "component", "handler", "client", c.id, "err", err)
				return
			}
		}
	}
}

func (c *client) enqueue(ctx context.Context, msg Message) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-c.done:
		return true
	default:
	}

	if c.blockOnFullBuffer {
		select {
		case c.send <- msg:
		case <-c.done:
		case <-ctx.Done():
			return false
		}
		return true
	}

	select {
	case c.send <- msg:
	case <-c.done:
	default:
		if c.manager != nil && c.manager.logger != nil {
			c.manager.logger.Warn("send buffer full", "component", "handler", "client", c.id)
		}
		c.close(websocket.StatusPolicyViolation, "send buffer full")
	}
	return true
}

func (c *client) close(code websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close(code, reason)
	})
}
