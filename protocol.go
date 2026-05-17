package syncedstate

import "encoding/json"

type MessageType string

const (
	MessageSubscribe   MessageType = "subscribe"
	MessageUnsubscribe MessageType = "unsubscribe"
	MessageSet         MessageType = "set"
	MessageSnapshot    MessageType = "snapshot"
	MessageUpdate      MessageType = "update"
	MessageError       MessageType = "error"
)

type Message struct {
	Type    MessageType     `json:"type"`
	ID      string          `json:"id,omitempty"`
	Name    string          `json:"name,omitempty"`
	Version uint64          `json:"version,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func errorMessage(id, name string, err error) Message {
	return Message{
		Type:  MessageError,
		ID:    id,
		Name:  name,
		Error: err.Error(),
	}
}
