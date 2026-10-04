package kinesisvideo

import (
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateSignalingChannel creates a new signaling channel. New channels become
// ACTIVE immediately -- see PARITY.md.
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
		Status:            statusActive,
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
	b.mu.RLock("DescribeSignalingChannel")
	defer b.mu.RUnlock()

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
	b.mu.RLock("ListSignalingChannels")
	defer b.mu.RUnlock()

	all := b.channels.All()

	matched := make([]*Channel, 0, len(all))

	for _, c := range all {
		if condition != nil && condition.ComparisonOperator == comparisonOperatorBeginsWith {
			if !strings.HasPrefix(c.Name, condition.ComparisonValue) {
				continue
			}
		}

		matched = append(matched, c.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// UpdateSignalingChannel updates a channel's SingleMasterConfiguration under
// optimistic-lock (CurrentVersion).
func (b *InMemoryBackend) UpdateSignalingChannel(channelARN, currentVersion string, messageTTLSeconds *int32) error {
	b.mu.Lock("UpdateSignalingChannel")
	defer b.mu.Unlock()

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

	c.Version = newVersion()

	return nil
}

// DeleteSignalingChannel deletes a channel under optimistic-lock (CurrentVersion, when supplied).
func (b *InMemoryBackend) DeleteSignalingChannel(channelARN, currentVersion string) error {
	b.mu.Lock("DeleteSignalingChannel")
	defer b.mu.Unlock()

	c, err := b.resolveChannelLocked("", channelARN)
	if err != nil {
		return err
	}

	if currentVersion != "" && c.Version != currentVersion {
		return ErrVersionMismatch
	}

	b.channels.Delete(c.Name)

	return nil
}
