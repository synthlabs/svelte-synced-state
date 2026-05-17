package syncedstate

import (
	"fmt"
	"strings"
)

type SingletonScope[T any] struct {
	name string
}

func NewSingletonScope[T any](name string) (SingletonScope[T], error) {
	if err := validateScopePart("scope", name); err != nil {
		return SingletonScope[T]{}, err
	}
	return SingletonScope[T]{name: name}, nil
}

func MustSingletonScope[T any](name string) SingletonScope[T] {
	scope, err := NewSingletonScope[T](name)
	if err != nil {
		panic(err)
	}
	return scope
}

func (s SingletonScope[T]) Name() string {
	return s.name
}

func (s SingletonScope[T]) Address() string {
	return s.name
}

func (s SingletonScope[T]) Define(manager *Manager, initial T, opts ...KeyOption) (*Key[T], error) {
	if err := validateScopePart("scope", s.name); err != nil {
		return nil, err
	}
	return Define(manager, s.Address(), initial, opts...)
}

func (s SingletonScope[T]) Lookup(manager *Manager) (*Key[T], error) {
	if err := validateScopePart("scope", s.name); err != nil {
		return nil, err
	}
	return Lookup[T](manager, s.Address())
}

type IndexedScope[T any] struct {
	name string
}

func NewIndexedScope[T any](name string) (IndexedScope[T], error) {
	if err := validateScopePart("scope", name); err != nil {
		return IndexedScope[T]{}, err
	}
	return IndexedScope[T]{name: name}, nil
}

func MustIndexedScope[T any](name string) IndexedScope[T] {
	scope, err := NewIndexedScope[T](name)
	if err != nil {
		panic(err)
	}
	return scope
}

func (s IndexedScope[T]) Name() string {
	return s.name
}

func (s IndexedScope[T]) Address(id string) (string, error) {
	return indexedAddress(s.name, id)
}

func (s IndexedScope[T]) MustAddress(id string) string {
	address, err := s.Address(id)
	if err != nil {
		panic(err)
	}
	return address
}

func (s IndexedScope[T]) Wildcard() string {
	return s.name + ":*"
}

func (s IndexedScope[T]) Define(manager *Manager, id string, initial T, opts ...KeyOption) (*Key[T], error) {
	address, err := s.Address(id)
	if err != nil {
		return nil, err
	}
	return Define(manager, address, initial, opts...)
}

func (s IndexedScope[T]) Lookup(manager *Manager, id string) (*Key[T], error) {
	address, err := s.Address(id)
	if err != nil {
		return nil, err
	}
	return Lookup[T](manager, address)
}

func DefineSingleton[T any](manager *Manager, name string, initial T, opts ...KeyOption) (*Key[T], error) {
	scope, err := NewSingletonScope[T](name)
	if err != nil {
		return nil, err
	}
	return scope.Define(manager, initial, opts...)
}

func LookupSingleton[T any](manager *Manager, name string) (*Key[T], error) {
	scope, err := NewSingletonScope[T](name)
	if err != nil {
		return nil, err
	}
	return scope.Lookup(manager)
}

func DefineIndexed[T any](manager *Manager, scopeName, id string, initial T, opts ...KeyOption) (*Key[T], error) {
	scope, err := NewIndexedScope[T](scopeName)
	if err != nil {
		return nil, err
	}
	return scope.Define(manager, id, initial, opts...)
}

func LookupIndexed[T any](manager *Manager, scopeName, id string) (*Key[T], error) {
	scope, err := NewIndexedScope[T](scopeName)
	if err != nil {
		return nil, err
	}
	return scope.Lookup(manager, id)
}

func validateScopePart(label, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidScope, label)
	}
	if strings.ContainsAny(value, ":*") {
		return fmt.Errorf("%w: %s %q cannot contain ':' or '*'", ErrInvalidScope, label, value)
	}
	return nil
}

func indexedAddress(scope, id string) (string, error) {
	if err := validateScopePart("scope", scope); err != nil {
		return "", err
	}
	if err := validateScopePart("id", id); err != nil {
		return "", err
	}
	return scope + ":" + id, nil
}

func wildcardAddress(scope string) (string, error) {
	if err := validateScopePart("scope", scope); err != nil {
		return "", err
	}
	return scope + ":*", nil
}

func parseWildcardAddress(address string) (string, bool, error) {
	if !strings.HasSuffix(address, ":*") {
		return "", false, nil
	}

	scope := strings.TrimSuffix(address, ":*")
	if err := validateScopePart("scope", scope); err != nil {
		return "", true, err
	}
	return scope, true, nil
}

func isWildcardAddress(address string) bool {
	_, ok, err := parseWildcardAddress(address)
	return ok && err == nil
}

func wildcardForIndexedAddress(address string) (string, bool) {
	scope, id, ok := strings.Cut(address, ":")
	if !ok || strings.Contains(id, ":") {
		return "", false
	}
	if err := validateScopePart("scope", scope); err != nil {
		return "", false
	}
	if err := validateScopePart("id", id); err != nil {
		return "", false
	}
	wildcard, err := wildcardAddress(scope)
	if err != nil {
		return "", false
	}
	return wildcard, true
}
