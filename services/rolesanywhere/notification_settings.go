package rolesanywhere

import (
	"context"
	"time"
)

// PutNotificationSettings sets notification settings on a trust anchor.
func (b *InMemoryBackend) PutNotificationSettings(
	ctx context.Context,
	trustAnchorID string,
	settings []NotificationSetting,
) (*TrustAnchor, error) {
	b.mu.Lock("PutNotificationSettings")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, trustAnchorID))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	nsStore := b.notificationSettingsStore(region)
	existing := nsStore[trustAnchorID]

	for _, ns := range settings {
		updated := false

		for i, e := range existing {
			if e.Event == ns.Event && e.Channel == ns.Channel {
				existing[i] = ns
				updated = true

				break
			}
		}

		if !updated {
			existing = append(existing, ns)
		}
	}

	nsStore[trustAnchorID] = existing
	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}

// ResetNotificationSettings removes specified notification settings from a trust anchor.
func (b *InMemoryBackend) ResetNotificationSettings(
	ctx context.Context,
	trustAnchorID string,
	keys []NotificationSettingKey,
) (*TrustAnchor, error) {
	b.mu.Lock("ResetNotificationSettings")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, trustAnchorID))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	nsStore := b.notificationSettingsStore(region)
	existing := nsStore[trustAnchorID]
	filtered := existing[:0]

	for _, e := range existing {
		removed := false

		for _, k := range keys {
			if e.Event == k.Event && e.Channel == k.Channel {
				removed = true

				break
			}
		}

		if !removed {
			filtered = append(filtered, e)
		}
	}

	nsStore[trustAnchorID] = filtered
	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}

// GetNotificationSettings returns notification settings for a trust anchor.
func (b *InMemoryBackend) GetNotificationSettings(ctx context.Context, trustAnchorID string) []NotificationSetting {
	b.mu.RLock("GetNotificationSettings")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	nsStore := b.notificationSettings[region]

	if nsStore == nil {
		return nil
	}

	src := nsStore[trustAnchorID]
	out := make([]NotificationSetting, len(src))
	copy(out, src)

	return out
}
