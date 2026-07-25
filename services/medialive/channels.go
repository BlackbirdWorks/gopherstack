package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Channel operations ---

// CreateChannel creates a new channel.
func (b *InMemoryBackend) CreateChannel(
	name, channelClass, roleArn string,
	anywhereSettings ChannelAnywhereSettings,
	tags map[string]string,
) (*Channel, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	if channelClass == "" {
		channelClass = channelClassStandard
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if anywhereSettings.ClusterID != "" && !b.clusters.Has(anywhereSettings.ClusterID) {
		return nil, fmt.Errorf(
			"%w: cluster %s not found", ErrInvalidParameter, anywhereSettings.ClusterID,
		)
	}

	id := newID()
	ch := &storedChannel{
		ARN:              b.channelARN(id),
		ID:               id,
		Name:             name,
		ChannelClass:     channelClass,
		RoleARN:          roleArn,
		State:            stateIdle,
		Tags:             copyTags(tags),
		AnywhereSettings: anywhereSettings,
	}

	b.channels.Put(ch)

	return ch.toChannel(), nil
}

// DescribeChannel returns a channel by ID.
func (b *InMemoryBackend) DescribeChannel(channelID string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return ch.toChannel(), nil
}

// UpdateChannel updates a channel's mutable fields.
func (b *InMemoryBackend) UpdateChannel(
	channelID, name, roleArn string,
	anywhereSettings ChannelAnywhereSettings,
	hasAnywhereSettings bool,
) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if name != "" {
		ch.Name = name
	}

	if roleArn != "" {
		ch.RoleARN = roleArn
	}

	if hasAnywhereSettings {
		if anywhereSettings.ClusterID != "" && !b.clusters.Has(anywhereSettings.ClusterID) {
			return nil, fmt.Errorf(
				"%w: cluster %s not found", ErrInvalidParameter, anywhereSettings.ClusterID,
			)
		}

		ch.AnywhereSettings = anywhereSettings
	}

	return ch.toChannel(), nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(channelID string) (*Channel, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State == stateRunning {
		return nil, fmt.Errorf("%w: channel must be idle before deleting", ErrConflict)
	}

	ch.State = stateDeleted
	b.channels.Delete(channelID)

	return ch.toChannel(), nil
}

// ListChannels returns a paginated list of channels.
func (b *InMemoryBackend) ListChannels(
	maxResults int,
	nextToken string,
) ([]*ChannelSummary, string, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	all := b.channels.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ChannelSummary, 0, len(pg.Data))
	for _, ch := range pg.Data {
		summaries = append(summaries, ch.toSummary())
	}

	return summaries, pg.Next, nil
}

// StartChannel transitions a channel toward RUNNING.
// The stored state advances immediately to RUNNING (deterministic emulation), but
// the API response carries STARTING to match the real AWS intermediate-state contract.
func (b *InMemoryBackend) StartChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StartChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateIdle {
		return nil, fmt.Errorf("%w: channel must be idle to start", ErrConflict)
	}

	ch.State = stateRunning

	result := ch.toChannel()
	result.State = stateStarting

	return result, nil
}

// StopChannel transitions a channel toward IDLE.
// The stored state advances immediately to IDLE (deterministic emulation), but
// the API response carries STOPPING to match the real AWS intermediate-state contract.
func (b *InMemoryBackend) StopChannel(channelID string) (*Channel, error) {
	b.mu.Lock("StopChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if ch.State != stateRunning {
		return nil, fmt.Errorf("%w: channel must be running to stop", ErrConflict)
	}

	ch.State = stateIdle

	result := ch.toChannel()
	result.State = stateStopping

	return result, nil
}

// --- Alerts and versions ---

// ListAlerts returns alerts for a channel (always empty in emulation).
func (b *InMemoryBackend) ListAlerts(channelID string) ([]map[string]any, error) {
	b.mu.RLock("ListAlerts")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelID) {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return []map[string]any{}, nil
}

// ListVersions returns the available channel engine versions.
func (b *InMemoryBackend) ListVersions() []ChannelEngineVersion {
	return []ChannelEngineVersion{
		{Version: channelEngineVersion, ExpirationDate: ""},
	}
}

// --- Channel lifecycle extras ---

// UpdateChannelClass changes a channel's class.
func (b *InMemoryBackend) UpdateChannelClass(channelID, channelClass string) (*Channel, error) {
	b.mu.Lock("UpdateChannelClass")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	if channelClass == "" {
		return nil, fmt.Errorf("%w: channelClass required", ErrInvalidParameter)
	}

	ch.ChannelClass = channelClass

	return ch.toChannel(), nil
}

// RestartChannelPipelines restarts a channel's pipelines (returns the channel).
func (b *InMemoryBackend) RestartChannelPipelines(
	channelID string,
	_ []string,
) (*Channel, error) {
	b.mu.RLock("RestartChannelPipelines")
	defer b.mu.RUnlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return ch.toChannel(), nil
}

// DescribeThumbnails returns the channel (thumbnails are not emulated as image data).
func (b *InMemoryBackend) DescribeThumbnails(channelID string) (*Channel, error) {
	b.mu.RLock("DescribeThumbnails")
	defer b.mu.RUnlock()

	ch, ok := b.channels.Get(channelID)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelID)
	}

	return ch.toChannel(), nil
}
