package syncedstate

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
)

type Manager struct {
	mu          sync.RWMutex
	storage     storage
	subscribers map[string]map[*client]struct{}
	nextClient  atomic.Uint64
	logger      Logger
	logLevel    LogLevel
}

func NewManager(opts ...Option) *Manager {
	m := &Manager{
		storage:     newMemoryStorage(),
		subscribers: make(map[string]map[*client]struct{}),
		logLevel:    LevelInfo,
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.logger == nil {
		m.logger = defaultLogger(m.logLevel)
	}

	return m
}

func Define[T any](manager *Manager, name string, initial T, opts ...KeyOption) (*Key[T], error) {
	cfg := keyConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	e := newEntry(name, initial)
	if err := manager.storage.define(name, e); err != nil {
		if errors.Is(err, ErrAlreadyDefined) {
			manager.logger.Warn("state already defined", "component", "storage", "name", name)
		}
		return nil, err
	}

	return &Key[T]{manager: manager, entry: e}, nil
}

func Lookup[T any](manager *Manager, name string) (*Key[T], error) {
	e, err := manager.storage.lookup(name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			manager.logger.Debug("lookup miss", "component", "storage", "name", name)
		}
		return nil, err
	}

	if e.typ != reflect.TypeOf((*T)(nil)).Elem() {
		return nil, ErrTypeMismatch
	}

	return &Key[T]{manager: manager, entry: e}, nil
}

func (m *Manager) entry(name string) (*entry, error) {
	return m.storage.lookup(name)
}

func (m *Manager) subscribe(c *client, name string) error {
	if _, wildcard, err := parseWildcardAddress(name); err != nil {
		return err
	} else if !wildcard {
		if _, err := m.entry(name); err != nil {
			return err
		}
	}

	m.mu.Lock()
	if _, ok := m.subscribers[name]; !ok {
		m.subscribers[name] = make(map[*client]struct{})
	}
	m.subscribers[name][c] = struct{}{}
	c.subscriptions[name] = struct{}{}
	subscriberCount := len(m.subscribers[name])
	subscriptionCount := len(c.subscriptions)
	m.mu.Unlock()

	m.logger.Debug("subscribe", "component", "manager", "client", c.id, "name", name, "subscribers", subscriberCount, "subscriptions", subscriptionCount)
	return nil
}

func (m *Manager) unsubscribe(c *client, name string) {
	m.mu.Lock()
	if subscribers, ok := m.subscribers[name]; ok {
		delete(subscribers, c)
		if len(subscribers) == 0 {
			delete(m.subscribers, name)
		}
	}
	delete(c.subscriptions, name)
	remaining := len(c.subscriptions)
	m.mu.Unlock()

	m.logger.Debug("unsubscribe", "component", "manager", "client", c.id, "name", name, "subscriptions", remaining)
}

func (m *Manager) removeClient(c *client) {
	m.mu.Lock()

	removed := len(c.subscriptions)
	for name := range c.subscriptions {
		if subscribers, ok := m.subscribers[name]; ok {
			delete(subscribers, c)
			if len(subscribers) == 0 {
				delete(m.subscribers, name)
			}
		}
	}
	m.mu.Unlock()

	m.logger.Debug("remove client", "component", "manager", "client", c.id, "subscriptions", removed)
}

func (m *Manager) broadcast(ctx context.Context, msg Message) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	subscriptionNames := []string{msg.Name}
	if wildcard, ok := wildcardForIndexedAddress(msg.Name); ok {
		subscriptionNames = append(subscriptionNames, wildcard)
	}

	m.mu.RLock()
	clients := make([]*client, 0, len(m.subscribers[msg.Name]))
	seen := make(map[*client]struct{})
	for _, name := range subscriptionNames {
		for c := range m.subscribers[name] {
			if _, ok := seen[c]; ok {
				continue
			}
			seen[c] = struct{}{}
			clients = append(clients, c)
		}
	}
	m.mu.RUnlock()

	m.logger.Debug("broadcast", "component", "manager", "name", msg.Name, "recipients", len(clients))
	for _, c := range clients {
		if !c.enqueue(ctx, msg) {
			return
		}
	}
}
