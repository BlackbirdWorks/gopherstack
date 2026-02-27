package firehose

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrNotFound is returned when a delivery stream is not found.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrAlreadyExists is returned when a delivery stream already exists.
	ErrAlreadyExists = errors.New("ResourceInUseException")
)

// DeliveryStream represents a Kinesis Firehose delivery stream.
type DeliveryStream struct {
	Name      string
	ARN       string
	Status    string
	AccountID string
	Region    string
	Records   [][]byte
}

// InMemoryBackend is the in-memory store for Firehose resources.
type InMemoryBackend struct {
	streams   map[string]*DeliveryStream
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:   make(map[string]*DeliveryStream),
		accountID: accountID,
		region:    region,
	}
}

// CreateDeliveryStream creates a new delivery stream.
func (b *InMemoryBackend) CreateDeliveryStream(name string) (*DeliveryStream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; ok {
		return nil, fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, name)
	}

	arn := fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", b.region, b.accountID, name)
	s := &DeliveryStream{
		Name:      name,
		ARN:       arn,
		Status:    "ACTIVE",
		Records:   [][]byte{},
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.streams[name] = s

	cp := *s

	return &cp, nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	delete(b.streams, name)

	return nil
}

// DescribeDeliveryStream returns a delivery stream by name.
func (b *InMemoryBackend) DescribeDeliveryStream(name string) (*DeliveryStream, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s, ok := b.streams[name]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	cp := *s

	return &cp, nil
}

// ListDeliveryStreams returns all delivery stream names.
func (b *InMemoryBackend) ListDeliveryStreams() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}

	return names
}

// PutRecord appends a record to the delivery stream.
func (b *InMemoryBackend) PutRecord(streamName string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.Records = append(s.Records, data)

	return nil
}

// PutRecordBatch appends multiple records to the delivery stream.
func (b *InMemoryBackend) PutRecordBatch(streamName string, records [][]byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return 0, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.Records = append(s.Records, records...)

	return 0, nil
}
