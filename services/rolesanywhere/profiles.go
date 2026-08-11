package rolesanywhere

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) profileARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("profile/%s", id))
}

// CreateProfile creates a new profile. enabled defaults to true when nil,
// matching the AWS CreateProfileRequest.enabled default (the same pattern
// CreateTrustAnchor and ImportCrl already use).
//
// Real AWS Roles Anywhere has no uniqueness constraint on profile names
// (CreateProfile only models ValidationException/AccessDeniedException --
// there is no ConflictException shape anywhere in the service), so duplicate
// names are accepted, matching the real API.
//
// roleArns must be non-nil (CreateProfileInput.RoleArns is "This member is
// required" and validateOpCreateProfileInput rejects nil); an explicitly
// empty slice is accepted since the RoleArnList shape itself declares min:0.
func (b *InMemoryBackend) CreateProfile(
	ctx context.Context,
	name string,
	roleArns []string,
	tags []TagEntry,
	durationSeconds *int32,
	managedPolicyArns []string,
	sessionPolicy string,
	requireInstanceProperties bool,
	enabled *bool,
	acceptRoleSessionName *bool,
) (*Profile, error) {
	if name == "" || roleArns == nil {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	id := uuid.NewString()
	now := time.Now().UTC()
	p := &Profile{
		ProfileID:                 id,
		ProfileArn:                b.profileARN(region, id),
		Name:                      name,
		CreatedBy:                 b.accountID,
		region:                    region,
		RoleArns:                  append([]string(nil), roleArns...),
		Enabled:                   enabled == nil || *enabled,
		CreatedAt:                 now,
		UpdatedAt:                 now,
		DurationSeconds:           durationSeconds,
		ManagedPolicyArns:         append([]string(nil), managedPolicyArns...),
		SessionPolicy:             sessionPolicy,
		RequireInstanceProperties: requireInstanceProperties,
		AcceptRoleSessionName:     acceptRoleSessionName != nil && *acceptRoleSessionName,
	}

	b.profiles.Put(p)

	if len(tags) > 0 {
		b.tagsStore(region)[p.ProfileArn] = cloneTags(tags)
	}

	return copyProfile(p), nil
}

// GetProfile returns the profile with the given ID.
func (b *InMemoryBackend) GetProfile(ctx context.Context, id string) (*Profile, error) {
	b.mu.RLock("GetProfile")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, id))
	if !exists {
		return nil, ErrProfileNotFound
	}

	return copyProfile(p), nil
}

// ListProfiles returns all profiles in the request region.
func (b *InMemoryBackend) ListProfiles(
	ctx context.Context,
	pageToken string,
	maxResults int,
) ([]*Profile, string, error) {
	b.mu.RLock("ListProfiles")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.profilesByRegion,
		getRegion(ctx, b.defaultRegion),
		copyProfile,
		func(p *Profile) string { return p.Name },
		func(p *Profile) string { return p.ProfileID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

// DeleteProfile removes a profile and returns its state immediately before
// deletion, matching AWS's DeleteProfileResponse.profile. Its attribute
// mappings and tags (both held in separate ID/ARN-keyed maps, not on the
// Profile struct itself -- see store.go) are cascade-deleted so no ghost
// rows survive the profile they belonged to.
func (b *InMemoryBackend) DeleteProfile(ctx context.Context, id string) (*Profile, error) {
	b.mu.Lock("DeleteProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, id))
	if !exists {
		return nil, ErrProfileNotFound
	}

	snap := copyProfile(p)
	b.profiles.Delete(regionKey(region, id))
	delete(b.attributeMappingsStore(region), id)
	delete(b.tagsStore(region), p.ProfileArn)

	return snap, nil
}

// UpdateProfile updates a profile's fields.
func (b *InMemoryBackend) UpdateProfile(
	ctx context.Context,
	id, name string,
	roleArns []string,
	durationSeconds *int32,
	managedPolicyArns []string,
	sessionPolicy string,
	requireInstanceProperties *bool,
	acceptRoleSessionName *bool,
) (*Profile, error) {
	b.mu.Lock("UpdateProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, id))
	if !exists {
		return nil, ErrProfileNotFound
	}

	if name != "" {
		p.Name = name
	}

	if roleArns != nil {
		p.RoleArns = append([]string(nil), roleArns...)
	}

	if durationSeconds != nil {
		p.DurationSeconds = durationSeconds
	}

	if managedPolicyArns != nil {
		p.ManagedPolicyArns = append([]string(nil), managedPolicyArns...)
	}

	if sessionPolicy != "" {
		p.SessionPolicy = sessionPolicy
	}

	if requireInstanceProperties != nil {
		p.RequireInstanceProperties = *requireInstanceProperties
	}

	if acceptRoleSessionName != nil {
		p.AcceptRoleSessionName = *acceptRoleSessionName
	}

	p.UpdatedAt = time.Now().UTC()

	return copyProfile(p), nil
}

// EnableProfile enables a profile.
func (b *InMemoryBackend) EnableProfile(ctx context.Context, id string) (*Profile, error) {
	return b.setProfileEnabled(ctx, id, true)
}

// DisableProfile disables a profile.
func (b *InMemoryBackend) DisableProfile(ctx context.Context, id string) (*Profile, error) {
	return b.setProfileEnabled(ctx, id, false)
}

func (b *InMemoryBackend) setProfileEnabled(ctx context.Context, id string, enabled bool) (*Profile, error) {
	b.mu.Lock("setProfileEnabled")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, id))
	if !exists {
		return nil, ErrProfileNotFound
	}

	p.Enabled = enabled
	p.UpdatedAt = time.Now().UTC()

	return copyProfile(p), nil
}
