package awsconfig

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("NoSuchConfigurationRecorder", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("MaxNumberOfConfigurationRecordersExceededException", awserr.ErrAlreadyExists)
)

// ConfigurationRecorder represents an AWS Config configuration recorder.
type ConfigurationRecorder struct {
	Name    string
	RoleARN string
	Status  string // PENDING or ACTIVE
}

// DeliveryChannel represents an AWS Config delivery channel.
type DeliveryChannel struct {
	Name     string
	S3Bucket string
	SNSArn   string
}

// InMemoryBackend is the in-memory store for AWS Config resources.
type InMemoryBackend struct {
	recorders map[string]*ConfigurationRecorder
	channels  map[string]*DeliveryChannel
	mu        *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		recorders: make(map[string]*ConfigurationRecorder),
		channels:  make(map[string]*DeliveryChannel),
		mu:        lockmetrics.New("awsconfig"),
	}
}

// PutConfigurationRecorder creates or updates a configuration recorder.
func (b *InMemoryBackend) PutConfigurationRecorder(name, roleARN string) error {
	b.mu.Lock("PutConfigurationRecorder")
	defer b.mu.Unlock()

	b.recorders[name] = &ConfigurationRecorder{Name: name, RoleARN: roleARN, Status: "PENDING"}

	return nil
}

// DescribeConfigurationRecorders returns all configuration recorders.
func (b *InMemoryBackend) DescribeConfigurationRecorders() []ConfigurationRecorder {
	b.mu.RLock("DescribeConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorder, 0, len(b.recorders))
	for _, r := range b.recorders {
		out = append(out, *r)
	}

	return out
}

// StartConfigurationRecorder starts a configuration recorder.
func (b *InMemoryBackend) StartConfigurationRecorder(name string) error {
	b.mu.Lock("StartConfigurationRecorder")
	defer b.mu.Unlock()

	r, ok := b.recorders[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	r.Status = "ACTIVE"

	return nil
}

// PutDeliveryChannel creates or updates a delivery channel.
func (b *InMemoryBackend) PutDeliveryChannel(name, s3Bucket, snsArn string) error {
	b.mu.Lock("PutDeliveryChannel")
	defer b.mu.Unlock()

	b.channels[name] = &DeliveryChannel{Name: name, S3Bucket: s3Bucket, SNSArn: snsArn}

	return nil
}

// DescribeDeliveryChannels returns all delivery channels.
func (b *InMemoryBackend) DescribeDeliveryChannels() []DeliveryChannel {
	b.mu.RLock("DescribeDeliveryChannels")
	defer b.mu.RUnlock()

	out := make([]DeliveryChannel, 0, len(b.channels))
	for _, c := range b.channels {
		out = append(out, *c)
	}

	return out
}
