package kinesisvideo

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateStream creates a new Kinesis video stream. New streams become ACTIVE
// immediately -- see PARITY.md for the CREATING/UPDATING/DELETING transient
// states this backend deliberately does not model.
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

	if b.streams.Has(name) {
		return nil, ErrStreamAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	s := &Stream{
		Name:                 name,
		ARN:                  streamARN(region, accountID, name, now.UnixMilli()),
		Status:               statusActive,
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
	b.mu.RLock("DescribeStream")
	defer b.mu.RUnlock()

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
	b.mu.RLock("ListStreams")
	defer b.mu.RUnlock()

	all := b.streams.All()

	matched := make([]*Stream, 0, len(all))

	for _, s := range all {
		if condition != nil && condition.ComparisonOperator == comparisonOperatorBeginsWith {
			if !strings.HasPrefix(s.Name, condition.ComparisonValue) {
				continue
			}
		}

		matched = append(matched, s.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// UpdateStream updates a stream's metadata under optimistic-lock (CurrentVersion).
func (b *InMemoryBackend) UpdateStream(name, streamARN, currentVersion, deviceName, mediaType string) error {
	b.mu.Lock("UpdateStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if s.Version != currentVersion {
		return ErrVersionMismatch
	}

	if deviceName != "" {
		s.DeviceName = deviceName
	}

	if mediaType != "" {
		s.MediaType = mediaType
	}

	s.Version = newVersion()

	return nil
}

// DeleteStream deletes a stream under optimistic-lock (CurrentVersion, when supplied).
func (b *InMemoryBackend) DeleteStream(streamARN, currentVersion string) error {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked("", streamARN)
	if err != nil {
		return err
	}

	if currentVersion != "" && s.Version != currentVersion {
		return ErrVersionMismatch
	}

	b.streams.Delete(s.Name)

	return nil
}

// UpdateDataRetention increases or decreases a stream's data retention period.
func (b *InMemoryBackend) UpdateDataRetention(
	name, streamARN, currentVersion, operation string,
	changeInHours int32,
) error {
	b.mu.Lock("UpdateDataRetention")
	defer b.mu.Unlock()

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
	s.Version = newVersion()

	return nil
}

// GetDataEndpoint returns an emulator-hosted, AWS-shaped data-plane endpoint
// for the stream. The KVS data plane (PutMedia/GetMedia/...) is out of scope
// for this backend -- see PARITY.md -- so the endpoint is wire-accurate but
// not backed by a functioning media data plane.
func (b *InMemoryBackend) GetDataEndpoint(name, streamARN, apiName, region string) (string, error) {
	b.mu.RLock("GetDataEndpoint")
	defer b.mu.RUnlock()

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("https://s-%s.kinesisvideo.%s.amazonaws.com", shortHash(s.ARN+apiName), region), nil
}

// DescribeImageGenerationConfiguration returns a stream's image generation config.
func (b *InMemoryBackend) DescribeImageGenerationConfiguration(name, streamARN string) (*ImageGenerationConfig, error) {
	b.mu.RLock("DescribeImageGenerationConfiguration")
	defer b.mu.RUnlock()

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

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	s.ImageGeneration = cfg

	return nil
}

// DescribeNotificationConfiguration returns a stream's notification config.
func (b *InMemoryBackend) DescribeNotificationConfiguration(name, streamARN string) (*NotificationConfig, error) {
	b.mu.RLock("DescribeNotificationConfiguration")
	defer b.mu.RUnlock()

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

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	s.Notification = cfg

	return nil
}
