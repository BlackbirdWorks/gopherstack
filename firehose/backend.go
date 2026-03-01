package firehose

import (
	"fmt"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a delivery stream is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a delivery stream already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
)

// DeliveryStream represents a Kinesis Firehose delivery stream.
type DeliveryStream struct {
	Tags      *tags.Tags
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
	mu        *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:   make(map[string]*DeliveryStream),
		accountID: accountID,
		region:    region,
		mu: lockmetrics.New("firehose"),
	}
}

// CreateDeliveryStream creates a new delivery stream.
func (b *InMemoryBackend) CreateDeliveryStream(name string) (*DeliveryStream, error) {
	b.mu.Lock("CreateDeliveryStream")
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; ok {
		return nil, fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, name)
	}

	streamARN := arn.Build("firehose", b.region, b.accountID, "deliverystream/"+name)
	s := &DeliveryStream{
		Name:      name,
		ARN:       streamARN,
		Status:    "ACTIVE",
		Records:   [][]byte{},
		Tags:      tags.New("firehose." + name + ".tags"),
		AccountID: b.accountID,
		Region:    b.region,
	}
	b.streams[name] = s

	cp := *s

	return &cp, nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(name string) error {
	b.mu.Lock("DeleteDeliveryStream")
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	delete(b.streams, name)

	return nil
}

// DescribeDeliveryStream returns a delivery stream by name.
func (b *InMemoryBackend) DescribeDeliveryStream(name string) (*DeliveryStream, error) {
	b.mu.RLock("DescribeDeliveryStream")
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
	b.mu.RLock("ListDeliveryStreams")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}

	return names
}

// PutRecord appends a record to the delivery stream.
func (b *InMemoryBackend) PutRecord(streamName string, data []byte) error {
	b.mu.Lock("PutRecord")
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
	b.mu.Lock("PutRecordBatch")
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return 0, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.Records = append(s.Records, records...)

	return 0, nil
}

// ListTagsForDeliveryStream returns tags for a delivery stream.
func (b *InMemoryBackend) ListTagsForDeliveryStream(name string) (map[string]string, error) {
	b.mu.RLock("ListTagsForDeliveryStream")
	defer b.mu.RUnlock()

	s, ok := b.streams[name]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return s.Tags.Clone(), nil
}

// TagDeliveryStream adds or updates tags on a delivery stream.
func (b *InMemoryBackend) TagDeliveryStream(name string, kv map[string]string) error {
	b.mu.Lock("TagDeliveryStream")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.Merge(kv)

	return nil
}

// UntagDeliveryStream removes tag keys from a delivery stream.
func (b *InMemoryBackend) UntagDeliveryStream(name string, keys []string) error {
	b.mu.Lock("UntagDeliveryStream")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.DeleteKeys(keys)

	return nil
}
