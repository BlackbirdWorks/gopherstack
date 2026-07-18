package rekognition

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	processorRunning = "RUNNING"
	processorStopped = "STOPPED"

	maxStreamProcessorsPerPage = 1000

	maxNameLen = 128
)

// validateStreamProcessorName checks that a stream processor name is within AWS length limits.
func validateStreamProcessorName(name string) error {
	if name == "" || len(name) > maxNameLen {
		return fmt.Errorf("%w: Name must be between 1 and %d characters", ErrValidation, maxNameLen)
	}

	return nil
}

func (b *InMemoryBackend) streamProcessorARN(name string) string {
	return arn.Build("rekognition", b.region, b.accountID, fmt.Sprintf("streamprocessor/%s", name))
}

// CreateStreamProcessor creates a new stream processor.
func (b *InMemoryBackend) CreateStreamProcessor(
	name, roleARN string,
	tags map[string]string,
) (*StreamProcessor, error) {
	if err := validateStreamProcessorName(name); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateStreamProcessor")
	defer b.mu.Unlock()

	if b.streamProcessors.Has(name) {
		return nil, ErrStreamProcessorAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	arn := b.streamProcessorARN(name)
	p := &storedStreamProcessor{
		Name:               name,
		StreamProcessorARN: arn,
		RoleARN:            roleARN,
		Status:             processorStopped,
		CreationTimestamp:  time.Now(),
		Tags:               tagsCopy,
	}
	b.streamProcessors.Put(p)

	if len(tagsCopy) > 0 {
		b.tags[arn] = tagsCopy
	}

	return p.toStreamProcessor(), nil
}

// DeleteStreamProcessor deletes a stream processor.
func (b *InMemoryBackend) DeleteStreamProcessor(name string) error {
	b.mu.Lock("DeleteStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors.Get(name)
	if !exists {
		return ErrStreamProcessorNotFound
	}

	b.streamProcessors.Delete(name)
	delete(b.tags, p.StreamProcessorARN)

	return nil
}

// DescribeStreamProcessor returns details about a stream processor.
func (b *InMemoryBackend) DescribeStreamProcessor(name string) (*StreamProcessor, error) {
	b.mu.RLock("DescribeStreamProcessor")
	defer b.mu.RUnlock()

	p, exists := b.streamProcessors.Get(name)
	if !exists {
		return nil, ErrStreamProcessorNotFound
	}

	return p.toStreamProcessor(), nil
}

// ListStreamProcessors returns a paginated list of stream processors.
func (b *InMemoryBackend) ListStreamProcessors(maxResults int32, nextToken string) ([]*StreamProcessor, string, error) {
	b.mu.RLock("ListStreamProcessors")
	defer b.mu.RUnlock()

	result, outToken := paginateTable(
		b.streamProcessors, maxResults, maxStreamProcessorsPerPage, nextToken,
		streamProcessorKeyFn, (*storedStreamProcessor).toStreamProcessor,
	)

	return result, outToken, nil
}

// StartStreamProcessor starts a stream processor.
func (b *InMemoryBackend) StartStreamProcessor(name string) error {
	b.mu.Lock("StartStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors.Get(name)
	if !exists {
		return ErrStreamProcessorNotFound
	}

	p.Status = processorRunning

	return nil
}

// StopStreamProcessor stops a stream processor.
func (b *InMemoryBackend) StopStreamProcessor(name string) error {
	b.mu.Lock("StopStreamProcessor")
	defer b.mu.Unlock()

	p, exists := b.streamProcessors.Get(name)
	if !exists {
		return ErrStreamProcessorNotFound
	}

	p.Status = processorStopped

	return nil
}

// UpdateStreamProcessor updates a stream processor (no-op for metadata).
func (b *InMemoryBackend) UpdateStreamProcessor(name string) error {
	b.mu.Lock("UpdateStreamProcessor")
	defer b.mu.Unlock()

	if !b.streamProcessors.Has(name) {
		return ErrStreamProcessorNotFound
	}

	return nil
}
