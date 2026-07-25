package rolesanywhere

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) trustAnchorARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("trust-anchor/%s", id))
}

// CreateTrustAnchor creates a new trust anchor. enabled defaults to true when
// nil, matching the AWS CreateTrustAnchorRequest.enabled default.
//
// Real AWS Roles Anywhere has no uniqueness constraint on trust anchor names
// (only ResourceNotFoundException/ValidationException/AccessDeniedException
// are modeled for CreateTrustAnchor -- there is no ConflictException shape
// anywhere in the service at all), so duplicate names are accepted, matching
// the real API; the identifier of record is the generated ID/ARN.
func (b *InMemoryBackend) CreateTrustAnchor(
	ctx context.Context,
	name string,
	source TrustAnchorSource,
	tags []TagEntry,
	enabled *bool,
	notificationSettings []NotificationSetting,
) (*TrustAnchor, error) {
	if name == "" || source.SourceType == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTrustAnchor")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	id := uuid.NewString()
	now := time.Now().UTC()
	ta := &TrustAnchor{
		TrustAnchorID:  id,
		TrustAnchorArn: b.trustAnchorARN(region, id),
		Name:           name,
		Source:         source,
		region:         region,
		Enabled:        enabled == nil || *enabled,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.trustAnchors.Put(ta)

	if len(tags) > 0 {
		b.tagsStore(region)[ta.TrustAnchorArn] = cloneTags(tags)
	}

	if len(notificationSettings) > 0 {
		b.putNotificationSettingsLocked(region, id, notificationSettings)
	}

	return copyTrustAnchor(ta), nil
}

// GetTrustAnchor returns the trust anchor with the given ID.
func (b *InMemoryBackend) GetTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error) {
	b.mu.RLock("GetTrustAnchor")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, id))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	return copyTrustAnchor(ta), nil
}

// ListTrustAnchors returns all trust anchors in the request region.
func (b *InMemoryBackend) ListTrustAnchors(
	ctx context.Context,
	pageToken string,
	maxResults int,
) ([]*TrustAnchor, string, error) {
	b.mu.RLock("ListTrustAnchors")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.trustAnchorsByRegion,
		getRegion(ctx, b.defaultRegion),
		copyTrustAnchor,
		func(t *TrustAnchor) string { return t.Name },
		func(t *TrustAnchor) string { return t.TrustAnchorID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

// DeleteTrustAnchor removes a trust anchor and returns its state immediately
// before deletion, matching AWS's DeleteTrustAnchorResponse.trustAnchor. Its
// notification settings and tags (both held in separate ARN/ID-keyed maps,
// not on the TrustAnchor struct itself -- see store.go) are cascade-deleted
// so no ghost rows survive the trust anchor they belonged to.
func (b *InMemoryBackend) DeleteTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error) {
	b.mu.Lock("DeleteTrustAnchor")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, id))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	snap := copyTrustAnchor(ta)
	b.trustAnchors.Delete(regionKey(region, id))
	delete(b.notificationSettingsStore(region), id)
	delete(b.tagsStore(region), ta.TrustAnchorArn)

	return snap, nil
}

// UpdateTrustAnchor updates name and/or source of a trust anchor.
func (b *InMemoryBackend) UpdateTrustAnchor(
	ctx context.Context,
	id, name string,
	source *TrustAnchorSource,
) (*TrustAnchor, error) {
	b.mu.Lock("UpdateTrustAnchor")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, id))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	if name != "" {
		ta.Name = name
	}

	if source != nil {
		ta.Source = *source
	}

	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}

// EnableTrustAnchor enables a trust anchor.
func (b *InMemoryBackend) EnableTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error) {
	return b.setTrustAnchorEnabled(ctx, id, true)
}

// DisableTrustAnchor disables a trust anchor.
func (b *InMemoryBackend) DisableTrustAnchor(ctx context.Context, id string) (*TrustAnchor, error) {
	return b.setTrustAnchorEnabled(ctx, id, false)
}

func (b *InMemoryBackend) setTrustAnchorEnabled(ctx context.Context, id string, enabled bool) (*TrustAnchor, error) {
	b.mu.Lock("setTrustAnchorEnabled")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	ta, exists := b.trustAnchors.Get(regionKey(region, id))
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	ta.Enabled = enabled
	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}
