package iotanalytics

import (
	"context"
	"maps"
	"time"
)

// cloneChannelStorage deep-copies a ChannelStorage pointer.
func cloneChannelStorage(s *ChannelStorage) *ChannelStorage {
	if s == nil {
		return nil
	}

	cp := *s

	if s.ServiceManagedS3 != nil {
		sm := *s.ServiceManagedS3
		cp.ServiceManagedS3 = &sm
	}

	if s.CustomerManagedS3 != nil {
		cm := *s.CustomerManagedS3
		cp.CustomerManagedS3 = &cm
	}

	return &cp
}

// cloneChannel returns a deep copy of c.
func cloneChannel(c *Channel) *Channel {
	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)
	cp.Storage = cloneChannelStorage(c.Storage)
	cp.RetentionPeriod = cloneRetentionPeriod(c.RetentionPeriod)

	return &cp
}

// CreateChannel creates a new IoT Analytics channel.
func (b *InMemoryBackend) CreateChannel(
	ctx context.Context,
	name string,
	tags map[string]string,
	storage *ChannelStorage,
	retention *RetentionPeriod,
) (*Channel, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	if err := validateRetentionPeriod(retention); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if b.channels.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "channel", name)
	c := &Channel{
		Name:            name,
		ARN:             arn,
		Status:          statusActive,
		CreationTime:    now,
		LastUpdate:      now,
		Tags:            make(map[string]string),
		Storage:         cloneChannelStorage(storage),
		RetentionPeriod: cloneRetentionPeriod(retention),
	}
	maps.Copy(c.Tags, tags)
	b.channels.Put(c)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneChannel(c), nil
}

// DescribeChannel returns channel metadata.
func (b *InMemoryBackend) DescribeChannel(name string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return nil, ErrChannelNotFound
	}

	return cloneChannel(c), nil
}

// UpdateChannel updates a channel's storage configuration, retention period, and last update time.
func (b *InMemoryBackend) UpdateChannel(name string, storage *ChannelStorage, retention *RetentionPeriod) error {
	if err := validateRetentionPeriod(retention); err != nil {
		return err
	}

	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return ErrChannelNotFound
	}

	c.LastUpdate = epochSeconds(time.Now())

	if storage != nil {
		c.Storage = cloneChannelStorage(storage)
	}

	if retention != nil {
		c.RetentionPeriod = cloneRetentionPeriod(retention)
	}

	return nil
}

// DeleteChannel deletes a channel and its associated messages.
func (b *InMemoryBackend) DeleteChannel(name string) error {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return ErrChannelNotFound
	}

	delete(b.tags, c.ARN)
	b.channels.Delete(name)
	delete(b.channelMessages, name)

	return nil
}

// ListChannels returns all channels sorted by name.
func (b *InMemoryBackend) ListChannels() []*Channel {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	items := b.channels.Snapshot()
	result := make([]*Channel, 0, len(items))

	for _, c := range items {
		result = append(result, cloneChannel(c))
	}

	return result
}

// AddChannelInternal seeds a channel by name (test helper).
func (b *InMemoryBackend) AddChannelInternal(name string) *Channel {
	c, _ := b.CreateChannel(b.svcCtx, name, nil, nil, nil)

	return c
}
