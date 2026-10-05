package kinesisvideo

import (
	"maps"
	"time"
)

// CreateSignalingChannel creates a channel in CREATING; it settles to ACTIVE after transitionDelay.
func (b *InMemoryBackend) CreateSignalingChannel(
	accountID, region, name, channelType string,
	messageTTLSeconds int32,
	tags map[string]string,
) (*Channel, error) {
	if err := validateResourceName(name, maxChannelNameLen); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if channelType == "" {
		channelType = channelTypeSingleMaster
	}

	if messageTTLSeconds == 0 {
		messageTTLSeconds = defaultMessageTTLSecs
	}

	b.mu.Lock("CreateSignalingChannel")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	if b.channels.Has(name) {
		return nil, ErrChannelAlreadyExists
	}

	now := time.Now().UTC()

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	c := &Channel{
		Name:              name,
		ARN:               channelARN(region, accountID, name, now.UnixMilli()),
		Type:              channelType,
		Status:            statusCreating,
		PendingUntil:      now.Add(transitionDelay),
		Version:           newVersion(),
		CreationTime:      now,
		MessageTTLSeconds: messageTTLSeconds,
		Tags:              t,
	}

	b.channels.Put(c)

	return c.clone(), nil
}

// DescribeSignalingChannel returns the most current information about a channel.
func (b *InMemoryBackend) DescribeSignalingChannel(name, channelARN string) (*Channel, error) {
	b.mu.Lock("DescribeSignalingChannel")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked(name, channelARN)
	if err != nil {
		return nil, err
	}

	return c.clone(), nil
}

// ListSignalingChannels returns channels matching condition, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListSignalingChannels(
	nextToken string,
	maxResults int,
	condition *ChannelNameCondition,
) ([]*Channel, string, error) {
	b.mu.Lock("ListSignalingChannels")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	var op, value string
	if condition != nil {
		op, value = condition.ComparisonOperator, condition.ComparisonValue
	}

	data, next := listByName(b.channels.All(), func(c *Channel) string { return c.Name }, (*Channel).clone,
		op, value, nextToken, maxResults, defaultListChannelsLimit)

	return data, next, nil
}

// UpdateSignalingChannel updates a channel's SingleMasterConfiguration under
// optimistic-lock (CurrentVersion).
func (b *InMemoryBackend) UpdateSignalingChannel(channelARN, currentVersion string, messageTTLSeconds *int32) error {
	b.mu.Lock("UpdateSignalingChannel")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked("", channelARN)
	if err != nil {
		return err
	}

	if c.Version != currentVersion {
		return ErrVersionMismatch
	}

	if messageTTLSeconds != nil {
		c.MessageTTLSeconds = *messageTTLSeconds
	}

	c.markUpdating(time.Now().UTC())

	c.Version = newVersion()

	return nil
}

// DeleteSignalingChannel marks a channel DELETING under optimistic-lock (CurrentVersion, when supplied).
func (b *InMemoryBackend) DeleteSignalingChannel(channelARN, currentVersion string) error {
	b.mu.Lock("DeleteSignalingChannel")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked("", channelARN)
	if err != nil {
		return err
	}

	if currentVersion != "" && c.Version != currentVersion {
		return ErrVersionMismatch
	}

	if c.Status != statusDeleting {
		c.Status = statusDeleting
		c.PendingUntil = time.Now().UTC().Add(transitionDelay)
	}

	return nil
}
