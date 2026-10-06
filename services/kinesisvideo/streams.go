package kinesisvideo

import (
	"fmt"
	"maps"
	"time"
)

// CreateStream creates a stream in CREATING; it settles to ACTIVE after transitionDelay.
func (b *InMemoryBackend) CreateStream(
	accountID, region, name, deviceName, mediaType, kmsKeyID, defaultStorageTier string,
	dataRetentionInHours int32,
	tags map[string]string,
) (*Stream, error) {
	if err := validateResourceName(name, maxStreamNameLen); err != nil {
		return nil, err
	}

	if dataRetentionInHours < minDataRetention || dataRetentionInHours > maxDataRetention {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	if b.streams.Has(name) {
		return nil, ErrStreamAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	s := &Stream{
		Name:                 name,
		ARN:                  streamARN(region, accountID, name, now.UnixMilli()),
		Status:               statusCreating,
		PendingUntil:         now.Add(transitionDelay),
		Version:              newVersion(),
		CreationTime:         now,
		DeviceName:           deviceName,
		KmsKeyID:             kmsKeyID,
		MediaType:            mediaType,
		DefaultStorageTier:   defaultStorageTier,
		DataRetentionInHours: dataRetentionInHours,
		Tags:                 t,
	}

	b.streams.Put(s)

	return s.clone(), nil
}

// DescribeStream returns the most current information about a stream.
func (b *InMemoryBackend) DescribeStream(name, streamARN string) (*Stream, error) {
	b.mu.Lock("DescribeStream")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	return s.clone(), nil
}

// ListStreams returns streams matching condition, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListStreams(
	nextToken string,
	maxResults int,
	condition *StreamNameCondition,
) ([]*Stream, string, error) {
	b.mu.Lock("ListStreams")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	var op, value string
	if condition != nil {
		op, value = condition.ComparisonOperator, condition.ComparisonValue
	}

	data, next := listByName(b.streams.All(), func(s *Stream) string { return s.Name }, (*Stream).clone,
		op, value, nextToken, maxResults, defaultListStreamsLimit)

	return data, next, nil
}

// UpdateStream updates a stream's metadata under optimistic-lock (CurrentVersion).
func (b *InMemoryBackend) UpdateStream(name, streamARN, currentVersion, deviceName, mediaType string) error {
	b.mu.Lock("UpdateStream")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if s.Version != currentVersion {
		return ErrVersionMismatch
	}

	s.markUpdating(time.Now().UTC())

	if deviceName != "" {
		s.DeviceName = deviceName
	}

	if mediaType != "" {
		s.MediaType = mediaType
	}

	s.Version = newVersion()

	return nil
}

// DeleteStream marks a stream DELETING under optimistic-lock (CurrentVersion, when supplied).
func (b *InMemoryBackend) DeleteStream(streamARN, currentVersion string) error {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked("", streamARN)
	if err != nil {
		return err
	}

	if currentVersion != "" && s.Version != currentVersion {
		return ErrVersionMismatch
	}

	if s.Status != statusDeleting {
		s.Status = statusDeleting
		s.PendingUntil = time.Now().UTC().Add(transitionDelay)
	}

	return nil
}

// UpdateDataRetention increases or decreases a stream's data retention period.
func (b *InMemoryBackend) UpdateDataRetention(
	name, streamARN, currentVersion, operation string,
	changeInHours int32,
) error {
	b.mu.Lock("UpdateDataRetention")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if s.Version != currentVersion {
		return ErrVersionMismatch
	}

	next := s.DataRetentionInHours

	switch operation {
	case operationIncreaseDataRetention:
		next += changeInHours
	case operationDecreaseDataRetention:
		next -= changeInHours
	default:
		return ErrValidation
	}

	if next < minDataRetention || next > maxDataRetention {
		return ErrValidation
	}

	s.DataRetentionInHours = next
	s.markUpdating(time.Now().UTC())
	s.Version = newVersion()

	return nil
}

// GetDataEndpoint returns an emulator-hosted, AWS-shaped data-plane endpoint
// for the stream. The KVS data plane (PutMedia/GetMedia/...) is out of scope
// for this backend -- see PARITY.md -- so the endpoint is wire-accurate but
// not backed by a functioning media data plane.
func (b *InMemoryBackend) GetDataEndpoint(name, streamARN, apiName, region string) (string, error) {
	b.mu.Lock("GetDataEndpoint")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("https://s-%s.kinesisvideo.%s.amazonaws.com", shortHash(s.ARN+apiName), region), nil
}

// DescribeImageGenerationConfiguration returns a stream's image generation config.
func (b *InMemoryBackend) DescribeImageGenerationConfiguration(name, streamARN string) (*ImageGenerationConfig, error) {
	b.mu.Lock("DescribeImageGenerationConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	return s.clone().ImageGeneration, nil
}

// UpdateImageGenerationConfiguration sets or clears a stream's image generation config.
func (b *InMemoryBackend) UpdateImageGenerationConfiguration(name, streamARN string, cfg *ImageGenerationConfig) error {
	b.mu.Lock("UpdateImageGenerationConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	s.ImageGeneration = cfg

	return nil
}

// DescribeNotificationConfiguration returns a stream's notification config.
func (b *InMemoryBackend) DescribeNotificationConfiguration(name, streamARN string) (*NotificationConfig, error) {
	b.mu.Lock("DescribeNotificationConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	return s.clone().Notification, nil
}

// UpdateNotificationConfiguration sets or clears a stream's notification config.
func (b *InMemoryBackend) UpdateNotificationConfiguration(name, streamARN string, cfg *NotificationConfig) error {
	b.mu.Lock("UpdateNotificationConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	s.Notification = cfg

	return nil
}
