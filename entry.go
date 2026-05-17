package syncedstate

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"
)

type Meta struct {
	Name    string
	Version uint64
}

type entry struct {
	name    string
	typ     reflect.Type
	mu      sync.Mutex
	value   any
	version uint64

	marshalLocked   func() (json.RawMessage, error)
	unmarshalLocked func(json.RawMessage) error
}

func newEntry[T any](name string, initial T) *entry {
	box := new(T)
	*box = initial

	return &entry{
		name:    name,
		typ:     reflect.TypeOf((*T)(nil)).Elem(),
		value:   box,
		version: 1,
		marshalLocked: func() (json.RawMessage, error) {
			return json.Marshal(*box)
		},
		unmarshalLocked: func(raw json.RawMessage) error {
			var next T
			if err := json.Unmarshal(raw, &next); err != nil {
				return err
			}
			*box = next
			return nil
		},
	}
}

func (e *entry) snapshotMessage(msgType MessageType, id string) (Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	raw, err := e.marshalLocked()
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type:    msgType,
		ID:      id,
		Name:    e.name,
		Version: e.version,
		Value:   raw,
	}, nil
}

func (e *entry) setRaw(raw json.RawMessage, id string, opts ...WriteOption) (Message, error) {
	if raw == nil {
		return Message{}, ErrMissingValue
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	version, err := e.nextVersionLocked(writeOptions(opts))
	if err != nil {
		return Message{}, err
	}

	if err := e.unmarshalLocked(raw); err != nil {
		return Message{}, err
	}

	next, err := e.marshalLocked()
	if err != nil {
		return Message{}, err
	}

	e.version = version
	return Message{
		Type:    MessageUpdate,
		ID:      id,
		Name:    e.name,
		Version: version,
		Value:   next,
	}, nil
}

func (e *entry) nextVersionLocked(cfg writeConfig) (uint64, error) {
	next := e.version + 1
	if cfg.checkVersion && cfg.version != next {
		return 0, ErrVersionConflict
	}
	return next, nil
}

func (e *entry) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if e.mu.TryLock() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

type Key[T any] struct {
	manager *Manager
	entry   *entry
}

func (k *Key[T]) Name() string {
	return k.entry.name
}

func (k *Key[T]) Update(ctx context.Context, update func(*T), opts ...WriteOption) error {
	if err := k.entry.lock(ctx); err != nil {
		return err
	}

	var raw json.RawMessage
	var version uint64
	var err error
	cfg := writeOptions(opts)
	func() {
		defer k.entry.mu.Unlock()

		version, err = k.entry.nextVersionLocked(cfg)
		if err != nil {
			return
		}

		ptr := k.entry.value.(*T)
		update(ptr)
		raw, err = k.entry.marshalLocked()
		if err == nil {
			k.entry.version = version
		}
	}()

	if err != nil {
		return err
	}

	k.manager.broadcast(ctx, Message{
		Type:    MessageUpdate,
		Name:    k.entry.name,
		Version: version,
		Value:   raw,
	})
	return nil
}

func (k *Key[T]) Set(ctx context.Context, value T, opts ...WriteOption) error {
	if err := k.entry.lock(ctx); err != nil {
		return err
	}

	var raw json.RawMessage
	var version uint64
	var err error
	cfg := writeOptions(opts)
	func() {
		defer k.entry.mu.Unlock()

		version, err = k.entry.nextVersionLocked(cfg)
		if err != nil {
			return
		}

		ptr := k.entry.value.(*T)
		*ptr = value
		raw, err = k.entry.marshalLocked()
		if err == nil {
			k.entry.version = version
		}
	}()

	if err != nil {
		return err
	}

	k.manager.broadcast(ctx, Message{
		Type:    MessageUpdate,
		Name:    k.entry.name,
		Version: version,
		Value:   raw,
	})
	return nil
}

func (k *Key[T]) Snapshot(ctx context.Context) (T, Meta, error) {
	if err := k.entry.lock(ctx); err != nil {
		var snapshot T
		return snapshot, Meta{Name: k.entry.name}, err
	}
	raw, err := k.entry.marshalLocked()
	version := k.entry.version
	k.entry.mu.Unlock()

	var snapshot T
	if err != nil {
		return snapshot, Meta{Name: k.entry.name, Version: version}, err
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return snapshot, Meta{Name: k.entry.name, Version: version}, err
	}

	return snapshot, Meta{Name: k.entry.name, Version: version}, nil
}

func (k *Key[T]) Lock(ctx context.Context) (*Locked[T], error) {
	if err := k.entry.lock(ctx); err != nil {
		return nil, err
	}
	raw, err := k.entry.marshalLocked()
	if err != nil {
		k.entry.mu.Unlock()
		return nil, err
	}
	return &Locked[T]{
		manager:  k.manager,
		entry:    k.entry,
		value:    k.entry.value.(*T),
		original: append(json.RawMessage(nil), raw...),
		locked:   true,
	}, nil
}

type Locked[T any] struct {
	manager  *Manager
	entry    *entry
	value    *T
	original json.RawMessage
	locked   bool
}

func (l *Locked[T]) Value() *T {
	return l.value
}

func (l *Locked[T]) Sync(ctx context.Context, opts ...WriteOption) error {
	if !l.locked {
		return ErrClosed
	}

	version, err := l.entry.nextVersionLocked(writeOptions(opts))
	if err != nil {
		if len(l.original) > 0 {
			if restoreErr := l.entry.unmarshalLocked(l.original); restoreErr != nil {
				return restoreErr
			}
		}
		return err
	}

	raw, err := l.entry.marshalLocked()
	if err != nil {
		return err
	}

	l.entry.version = version
	l.original = append(l.original[:0], raw...)
	l.manager.broadcast(ctx, Message{
		Type:    MessageUpdate,
		Name:    l.entry.name,
		Version: version,
		Value:   raw,
	})
	return nil
}

func (l *Locked[T]) Unlock() {
	if !l.locked {
		return
	}

	l.locked = false
	l.entry.mu.Unlock()
}
