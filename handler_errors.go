package syncedstate

import "errors"

var (
	errUnexpectedMessageType = errors.New("syncedstate: expected text websocket message")
	errUnknownMessageType    = errors.New("syncedstate: unknown message type")
)
