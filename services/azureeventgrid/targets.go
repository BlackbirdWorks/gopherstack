package azureeventgrid

import "time"

// Message is what a sink receives for one delivered event.
type Message struct {
	ID          string
	Subject     string
	ContentType string
	Body        []byte
}

// Targets are the in-emulator sinks an event subscription can deliver into.
// Implementations live in internal/app so this package imports no sibling service.
type Targets interface {
	SendServiceBus(queue, topic string, m Message) error
	SendEventHub(namespace, hub string, m Message) error
	SendStorageQueue(account, queue, text string, ttl time.Duration) error
	PutBlob(account, container, blob string, data []byte, contentType string) error
}

type noTargets struct{}

func (noTargets) SendServiceBus(string, string, Message) error { return ErrTargetUnavailable }
func (noTargets) SendEventHub(string, string, Message) error   { return ErrTargetUnavailable }
func (noTargets) SendStorageQueue(string, string, string, time.Duration) error {
	return ErrTargetUnavailable
}

func (noTargets) PutBlob(string, string, string, []byte, string) error { return ErrTargetUnavailable }
