package syncedstate

import "errors"

var (
	ErrAlreadyDefined = errors.New("syncedstate: state name already defined")
	ErrClosed         = errors.New("syncedstate: locked state already closed")
	ErrInvalidScope   = errors.New("syncedstate: invalid state scope")
	ErrMissingValue   = errors.New("syncedstate: message value is required")
	ErrNotFound       = errors.New("syncedstate: state name not found")
	ErrTypeMismatch   = errors.New("syncedstate: state type mismatch")
	ErrWildcardName   = errors.New("syncedstate: wildcard state name requires an exact indexed state name")
)
