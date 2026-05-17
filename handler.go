package syncedstate

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

type client struct {
	id            uint64
	manager       *Manager
	conn          *websocket.Conn
	send          chan Message
	subscriptions map[string]struct{}
	done          chan struct{}
	closeOnce     sync.Once
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
			id:            m.nextClient.Add(1),
			manager:       m,
			conn:          conn,
			send:          make(chan Message, cfg.sendBuffer),
			subscriptions: make(map[string]struct{}),
			done:          make(chan struct{}),
		}

		ctx := r.Context()
		go c.writeLoop(ctx, cfg)
		c.readLoop(ctx)
	})
}

func (c *client) readLoop(ctx context.Context) {
	defer func() {
		c.close(websocket.StatusNormalClosure, "closed")
		c.manager.removeClient(c)
	}()

	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			c.enqueue(errorMessage("", "", errUnexpectedMessageType))
			continue
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.enqueue(errorMessage("", "", err))
			continue
		}
		c.handleMessage(ctx, msg)
	}
}

func (c *client) handleMessage(ctx context.Context, msg Message) {
	switch msg.Type {
	case MessageSubscribe:
		if err := c.manager.subscribe(c, msg.Name); err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		if isWildcardAddress(msg.Name) {
			return
		}
		e, _ := c.manager.entry(msg.Name)
		snapshot, err := e.snapshotMessage(MessageSnapshot, msg.ID)
		if err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.enqueue(snapshot)
	case MessageUnsubscribe:
		c.manager.unsubscribe(c, msg.Name)
	case MessageSnapshot:
		if err := rejectWildcardName(msg.Name); err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		e, err := c.manager.entry(msg.Name)
		if err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		snapshot, err := e.snapshotMessage(MessageSnapshot, msg.ID)
		if err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.enqueue(snapshot)
	case MessageSet:
		if err := rejectWildcardName(msg.Name); err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		e, err := c.manager.entry(msg.Name)
		if err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		update, err := e.setRaw(msg.Value, msg.ID)
		if err != nil {
			c.enqueue(errorMessage(msg.ID, msg.Name, err))
			return
		}
		c.manager.broadcast(ctx, update)
	default:
		c.enqueue(errorMessage(msg.ID, msg.Name, errUnknownMessageType))
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
		}
	}
}

func (c *client) enqueue(msg Message) {
	select {
	case <-c.done:
		return
	default:
	}

	select {
	case c.send <- msg:
	case <-c.done:
	default:
		c.close(websocket.StatusPolicyViolation, "send buffer full")
	}
}

func (c *client) close(code websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close(code, reason)
	})
}
