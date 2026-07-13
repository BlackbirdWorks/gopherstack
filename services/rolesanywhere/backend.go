package rolesanywhere

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

var (
	// ErrTrustAnchorNotFound is returned when a trust anchor does not exist.
	ErrTrustAnchorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTrustAnchorAlreadyExists is returned when creating a duplicate trust anchor.
	ErrTrustAnchorAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrProfileNotFound is returned when a profile does not exist.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileAlreadyExists is returned when creating a duplicate profile.
	ErrProfileAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrCrlNotFound is returned when a CRL does not exist.
	ErrCrlNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrCrlAlreadyExists is returned when creating a duplicate CRL.
	ErrCrlAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrSubjectNotFound is returned when a subject does not exist.
	ErrSubjectNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// TrustAnchorSource defines the source of a trust anchor.
type TrustAnchorSource struct {
	// SourceData is a map of source-type-specific fields.
	SourceData map[string]string `json:"sourceData,omitempty"`
	SourceType string            `json:"sourceType"`
}

// TrustAnchor represents an IAM Roles Anywhere trust anchor.
type TrustAnchor struct {
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
	Source         TrustAnchorSource `json:"source"`
	TrustAnchorID  string            `json:"trustAnchorId"`
	TrustAnchorArn string            `json:"trustAnchorArn"`
	Name           string            `json:"name"`
	// region is the store.Table composite-key qualifier (see regionKey); it
	// is not part of the wire API (Roles Anywhere trust anchors are
	// region-scoped but the AWS TrustAnchor shape carries no Region field of
	// its own -- the region is only ever recoverable from the request
	// context).
	region  string
	Tags    []TagEntry `json:"tags,omitempty"`
	Enabled bool       `json:"enabled"`
}

// TagEntry is a key-value tag pair (Roles Anywhere uses list-based tags).
type TagEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Crl represents an IAM Roles Anywhere Certificate Revocation List.
type Crl struct {
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	CrlID          string    `json:"crlId"`
	CrlArn         string    `json:"crlArn"`
	Name           string    `json:"name"`
	TrustAnchorArn string    `json:"trustAnchorArn"`
	// region is the store.Table composite-key qualifier (see regionKey); see
	// TrustAnchor.region's doc comment for why it is unexported.
	region  string
	CrlData []byte `json:"crlData,omitempty"`
	Enabled bool   `json:"enabled"`
}

// Subject represents an IAM Roles Anywhere subject (authenticating certificate).
type Subject struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	LastSeenAt  time.Time `json:"lastSeenAt"`
	SubjectID   string    `json:"subjectId"`
	SubjectArn  string    `json:"subjectArn"`
	X509Subject string    `json:"x509Subject"`
	// region is the store.Table composite-key qualifier (see regionKey); see
	// TrustAnchor.region's doc comment for why it is unexported.
	region  string
	Enabled bool `json:"enabled"`
}

// MappingRule is a single rule mapping a certificate field specifier to a session attribute.
type MappingRule struct {
	Specifier string `json:"specifier"`
}

// AttributeMapping maps a certificate field to session attribute rules.
type AttributeMapping struct {
	CertificateField string        `json:"certificateField"`
	MappingRules     []MappingRule `json:"mappingRules"`
}

// NotificationSetting holds a notification configuration for a trust anchor.
type NotificationSetting struct {
	Threshold *int32 `json:"threshold,omitempty"`
	Event     string `json:"event"`
	Channel   string `json:"channel,omitempty"`
	Enabled   bool   `json:"enabled"`
}

// NotificationSettingKey identifies a notification setting to reset.
type NotificationSettingKey struct {
	Event   string `json:"event"`
	Channel string `json:"channel,omitempty"`
}

// Profile represents an IAM Roles Anywhere profile.
type Profile struct {
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	DurationSeconds *int32    `json:"durationSeconds,omitempty"`
	ProfileID       string    `json:"profileId"`
	ProfileArn      string    `json:"profileArn"`
	Name            string    `json:"name"`
	SessionPolicy   string    `json:"sessionPolicy,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); see
	// TrustAnchor.region's doc comment for why it is unexported.
	region                    string
	Tags                      []TagEntry `json:"tags,omitempty"`
	RoleArns                  []string   `json:"roleArns"`
	ManagedPolicyArns         []string   `json:"managedPolicyArns,omitempty"`
	RequireInstanceProperties bool       `json:"requireInstanceProperties,omitempty"`
	Enabled                   bool       `json:"enabled"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// trustAnchors, profiles, crls, and subjects were previously
// map[region]map[id]*T; Phase 3.3 replaces each with a flat *store.Table
// keyed by the composite "region|id" string (see regionKey), with a
// companion *store.Index grouping entries by region -- see store_setup.go's
// registerAllTables doc for the full rationale and why all four are "dirty"
// (unregistered on registry) tables. tags, attributeMappings, and
// notificationSettings remain plain region-nested maps: each holds a slice
// value ([]TagEntry / []AttributeMapping / []NotificationSetting), not a
// *T, so there is nothing for store.Table to key on.
type InMemoryBackend struct {
	mu                   *lockmetrics.RWMutex
	registry             *store.Registry
	trustAnchors         *store.Table[TrustAnchor]
	trustAnchorsByRegion *store.Index[TrustAnchor]
	profiles             *store.Table[Profile]
	profilesByRegion     *store.Index[Profile]
	crls                 *store.Table[Crl]
	crlsByRegion         *store.Index[Crl]
	subjects             *store.Table[Subject]
	subjectsByRegion     *store.Index[Subject]
	tags                 map[string]map[string][]TagEntry            // region → resourceARN → tags
	attributeMappings    map[string]map[string][]AttributeMapping    // region → profileID → mappings
	notificationSettings map[string]map[string][]NotificationSetting // region → trustAnchorID → settings
	accountID            string
	defaultRegion        string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                   lockmetrics.New("rolesanywhere"),
		accountID:            accountID,
		defaultRegion:        region,
		registry:             store.NewRegistry(),
		tags:                 make(map[string]map[string][]TagEntry),
		attributeMappings:    make(map[string]map[string][]AttributeMapping),
		notificationSettings: make(map[string]map[string][]NotificationSetting),
	}

	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") used
// by trustAnchors, profiles, crls, and subjects.
func regionKey(region, id string) string { return region + "|" + id }

// ---- per-region lazy store helpers (for the maps left unconverted) ----

func (b *InMemoryBackend) tagsStore(region string) map[string][]TagEntry {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]TagEntry)
	}

	return b.tags[region]
}

func (b *InMemoryBackend) attributeMappingsStore(region string) map[string][]AttributeMapping {
	if b.attributeMappings[region] == nil {
		b.attributeMappings[region] = make(map[string][]AttributeMapping)
	}

	return b.attributeMappings[region]
}

func (b *InMemoryBackend) notificationSettingsStore(region string) map[string][]NotificationSetting {
	if b.notificationSettings[region] == nil {
		b.notificationSettings[region] = make(map[string][]NotificationSetting)
	}

	return b.notificationSettings[region]
}

// listByRegionIndex is a generic helper for paginated listing of region-keyed
// resources held in a *store.Index. It reads idx.Get(region), copies each
// item, sorts by sortKey, then paginates.
func listByRegionIndex[T any](
	idx *store.Index[T],
	region string,
	copyFn func(*T) *T,
	sortKey func(*T) string,
	getID func(*T) string,
	pageToken string,
	maxResults int,
) ([]*T, string) {
	group := idx.Get(region)
	all := make([]*T, 0, len(group))

	for _, item := range group {
		all = append(all, copyFn(item))
	}

	sort.Slice(all, func(i, j int) bool {
		return sortKey(all[i]) < sortKey(all[j])
	})

	start, next := paginate(all, pageToken, maxResults, getID)

	return all[start:next], nextTokenFromSlice(all, next, getID)
}

// ---- ARN builders ----

func (b *InMemoryBackend) trustAnchorARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("trust-anchor/%s", id))
}

func (b *InMemoryBackend) profileARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("profile/%s", id))
}

func (b *InMemoryBackend) crlARN(region, id string) string {
	return arn.Build("rolesanywhere", region, b.accountID, fmt.Sprintf("crl/%s", id))
}

// ---- Trust Anchor operations ----

// CreateTrustAnchor creates a new trust anchor. enabled defaults to true when
// nil, matching the AWS CreateTrustAnchorRequest.enabled default.
func (b *InMemoryBackend) CreateTrustAnchor(
	ctx context.Context,
	name string,
	source TrustAnchorSource,
	tags []TagEntry,
	enabled *bool,
) (*TrustAnchor, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTrustAnchor")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, ta := range b.trustAnchorsByRegion.Get(region) {
		if ta.Name == name {
			return nil, ErrTrustAnchorAlreadyExists
		}
	}

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
		Tags:           cloneTags(tags),
	}

	b.trustAnchors.Put(ta)

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
// before deletion, matching AWS's DeleteTrustAnchorResponse.trustAnchor.
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

// ---- Profile operations ----

// CreateProfile creates a new profile.
func (b *InMemoryBackend) CreateProfile(
	ctx context.Context,
	name string,
	roleArns []string,
	tags []TagEntry,
	durationSeconds *int32,
	managedPolicyArns []string,
	sessionPolicy string,
	requireInstanceProperties bool,
) (*Profile, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, p := range b.profilesByRegion.Get(region) {
		if p.Name == name {
			return nil, ErrProfileAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	p := &Profile{
		ProfileID:                 id,
		ProfileArn:                b.profileARN(region, id),
		Name:                      name,
		region:                    region,
		RoleArns:                  append([]string(nil), roleArns...),
		Enabled:                   true,
		CreatedAt:                 now,
		UpdatedAt:                 now,
		Tags:                      cloneTags(tags),
		DurationSeconds:           durationSeconds,
		ManagedPolicyArns:         append([]string(nil), managedPolicyArns...),
		SessionPolicy:             sessionPolicy,
		RequireInstanceProperties: requireInstanceProperties,
	}

	b.profiles.Put(p)

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
// deletion, matching AWS's DeleteProfileResponse.profile.
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

// ---- Tag operations ----

// TagResource adds tags to a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags []TagEntry) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))
	tagStore := b.tagsStore(region)
	existing := tagStore[resourceARN]

	for _, newTag := range tags {
		updated := false

		for i, t := range existing {
			if t.Key == newTag.Key {
				existing[i].Value = newTag.Value
				updated = true

				break
			}
		}

		if !updated {
			existing = append(existing, newTag)
		}
	}

	tagStore[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))
	tagStore := b.tagsStore(region)
	existing := tagStore[resourceARN]
	keySet := make(map[string]bool, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = true
	}

	filtered := existing[:0]

	for _, t := range existing {
		if !keySet[t.Key] {
			filtered = append(filtered, t)
		}
	}

	tagStore[resourceARN] = filtered

	return nil
}

// ListTagsForResource returns tags for a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) ([]TagEntry, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))

	return cloneTags(b.tags[region][resourceARN]), nil
}

// ---- CRL operations ----

// ImportCrl imports a new CRL.
func (b *InMemoryBackend) ImportCrl(
	ctx context.Context,
	name string,
	crlData []byte,
	trustAnchorArn string,
	enabled bool,
	tags []TagEntry,
) (*Crl, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("ImportCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, c := range b.crlsByRegion.Get(region) {
		if c.Name == name {
			return nil, ErrCrlAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	crl := &Crl{
		CrlID:          id,
		CrlArn:         b.crlARN(region, id),
		Name:           name,
		region:         region,
		CrlData:        crlData,
		TrustAnchorArn: trustAnchorArn,
		Enabled:        enabled,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.crls.Put(crl)

	if len(tags) > 0 {
		b.tagsStore(region)[crl.CrlArn] = cloneTags(tags)
	}

	return copyCrl(crl), nil
}

// GetCrl returns a CRL by ID.
func (b *InMemoryBackend) GetCrl(ctx context.Context, id string) (*Crl, error) {
	b.mu.RLock("GetCrl")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	return copyCrl(crl), nil
}

// ListCrls returns all CRLs with optional pagination.
func (b *InMemoryBackend) ListCrls(ctx context.Context, pageToken string, maxResults int) ([]*Crl, string, error) {
	b.mu.RLock("ListCrls")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.crlsByRegion,
		getRegion(ctx, b.defaultRegion),
		copyCrl,
		func(c *Crl) string { return c.Name },
		func(c *Crl) string { return c.CrlID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

// UpdateCrl updates a CRL's name and/or data.
func (b *InMemoryBackend) UpdateCrl(ctx context.Context, id, name string, crlData []byte) (*Crl, error) {
	b.mu.Lock("UpdateCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	if name != "" {
		crl.Name = name
	}

	if len(crlData) > 0 {
		crl.CrlData = crlData
	}

	crl.UpdatedAt = time.Now().UTC()

	return copyCrl(crl), nil
}

// DeleteCrl removes a CRL.
func (b *InMemoryBackend) DeleteCrl(ctx context.Context, id string) (*Crl, error) {
	b.mu.Lock("DeleteCrl")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	snap := copyCrl(crl)
	b.crls.Delete(regionKey(region, id))

	return snap, nil
}

// EnableCrl enables a CRL.
func (b *InMemoryBackend) EnableCrl(ctx context.Context, id string) (*Crl, error) {
	return b.setCrlEnabled(ctx, id, true)
}

// DisableCrl disables a CRL.
func (b *InMemoryBackend) DisableCrl(ctx context.Context, id string) (*Crl, error) {
	return b.setCrlEnabled(ctx, id, false)
}

func (b *InMemoryBackend) setCrlEnabled(ctx context.Context, id string, enabled bool) (*Crl, error) {
	b.mu.Lock("setCrlEnabled")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	crl, exists := b.crls.Get(regionKey(region, id))
	if !exists {
		return nil, ErrCrlNotFound
	}

	crl.Enabled = enabled
	crl.UpdatedAt = time.Now().UTC()

	return copyCrl(crl), nil
}

// ---- Subject operations ----

// GetSubject returns a subject by ID.
func (b *InMemoryBackend) GetSubject(ctx context.Context, id string) (*Subject, error) {
	b.mu.RLock("GetSubject")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	s, exists := b.subjects.Get(regionKey(region, id))
	if !exists {
		return nil, ErrSubjectNotFound
	}

	return copySubject(s), nil
}

// ListSubjects returns all subjects with optional pagination.
func (b *InMemoryBackend) ListSubjects(
	ctx context.Context,
	pageToken string,
	maxResults int,
) ([]*Subject, string, error) {
	b.mu.RLock("ListSubjects")
	defer b.mu.RUnlock()

	items, token := listByRegionIndex(
		b.subjectsByRegion,
		getRegion(ctx, b.defaultRegion),
		copySubject,
		func(s *Subject) string { return s.SubjectID },
		func(s *Subject) string { return s.SubjectID },
		pageToken,
		maxResults,
	)

	return items, token, nil
}

// ---- Attribute mapping operations ----

// PutAttributeMapping adds or replaces a certificate field mapping on a profile.
func (b *InMemoryBackend) PutAttributeMapping(
	ctx context.Context,
	profileID, certificateField string,
	rules []MappingRule,
) (*Profile, error) {
	b.mu.Lock("PutAttributeMapping")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, profileID))
	if !exists {
		return nil, ErrProfileNotFound
	}

	amStore := b.attributeMappingsStore(region)
	mappings := amStore[profileID]
	updated := false

	for i, m := range mappings {
		if m.CertificateField == certificateField {
			mappings[i].MappingRules = append([]MappingRule(nil), rules...)
			updated = true

			break
		}
	}

	if !updated {
		mappings = append(mappings, AttributeMapping{
			CertificateField: certificateField,
			MappingRules:     append([]MappingRule(nil), rules...),
		})
	}

	amStore[profileID] = mappings

	return copyProfile(p), nil
}

// DeleteAttributeMapping removes a certificate field mapping (and optional specifiers) from a profile.
func (b *InMemoryBackend) DeleteAttributeMapping(
	ctx context.Context,
	profileID, certificateField string,
	specifiers []string,
) (*Profile, error) {
	b.mu.Lock("DeleteAttributeMapping")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	p, exists := b.profiles.Get(regionKey(region, profileID))
	if !exists {
		return nil, ErrProfileNotFound
	}

	amStore := b.attributeMappingsStore(region)

	if len(specifiers) == 0 {
		amStore[profileID] = removeFieldMapping(amStore[profileID], certificateField)
	} else {
		amStore[profileID] = removeSpecifiers(amStore[profileID], certificateField, specifiers)
	}

	return copyProfile(p), nil
}

// removeFieldMapping returns mappings with the named certificateField removed entirely.
func removeFieldMapping(mappings []AttributeMapping, certificateField string) []AttributeMapping {
	filtered := mappings[:0]

	for _, m := range mappings {
		if m.CertificateField != certificateField {
			filtered = append(filtered, m)
		}
	}

	return filtered
}

// removeSpecifiers returns mappings with the named specifiers removed from certificateField's rules.
func removeSpecifiers(mappings []AttributeMapping, certificateField string, specifiers []string) []AttributeMapping {
	specSet := make(map[string]bool, len(specifiers))

	for _, s := range specifiers {
		specSet[s] = true
	}

	for i, m := range mappings {
		if m.CertificateField != certificateField {
			continue
		}

		filtered := m.MappingRules[:0]

		for _, r := range m.MappingRules {
			if !specSet[r.Specifier] {
				filtered = append(filtered, r)
			}
		}

		mappings[i].MappingRules = filtered
	}

	return mappings
}

// GetAttributeMappings returns the attribute mappings for a profile.
func (b *InMemoryBackend) GetAttributeMappings(ctx context.Context, profileID string) []AttributeMapping {
	b.mu.RLock("GetAttributeMappings")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	amStore := b.attributeMappings[region]

	if amStore == nil {
		return nil
	}

	src := amStore[profileID]
	out := make([]AttributeMapping, len(src))
	copy(out, src)

	return out
}

// ---- Notification settings operations ----

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

// ---- Lifecycle ----

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// trustAnchors, profiles, crls, and subjects are "dirty" tables
	// deliberately NOT on b.registry (see store_setup.go's registerAllTables
	// doc), so each needs its own Reset() call here rather than going
	// through b.registry.ResetAll().
	b.registry.ResetAll()
	b.trustAnchors.Reset()
	b.profiles.Reset()
	b.crls.Reset()
	b.subjects.Reset()
	b.tags = make(map[string]map[string][]TagEntry)
	b.attributeMappings = make(map[string]map[string][]AttributeMapping)
	b.notificationSettings = make(map[string]map[string][]NotificationSetting)
}

// Region returns the backend's default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot and Restore (implementing persistence.Persistable) live in
// persistence.go, alongside the "dirty" DTO registry they need for
// trustAnchors/profiles/crls/subjects (see store_setup.go's registerAllTables
// doc for why those four tables are dirty).

// ---- helpers ----

func cloneTags(tags []TagEntry) []TagEntry {
	if tags == nil {
		return nil
	}

	out := make([]TagEntry, len(tags))
	copy(out, tags)

	return out
}

func copyTrustAnchor(ta *TrustAnchor) *TrustAnchor {
	cp := *ta
	cp.Tags = cloneTags(ta.Tags)

	src := ta.Source
	if src.SourceData != nil {
		sd := make(map[string]string, len(src.SourceData))
		maps.Copy(sd, src.SourceData)
		src.SourceData = sd
	}

	cp.Source = src

	return &cp
}

func copyProfile(p *Profile) *Profile {
	cp := *p
	cp.Tags = cloneTags(p.Tags)
	cp.RoleArns = append([]string(nil), p.RoleArns...)
	cp.ManagedPolicyArns = append([]string(nil), p.ManagedPolicyArns...)

	return &cp
}

func copyCrl(c *Crl) *Crl {
	cp := *c
	cp.CrlData = append([]byte(nil), c.CrlData...)

	return &cp
}

func copySubject(s *Subject) *Subject {
	cp := *s

	return &cp
}

// paginate returns the start and end indices for a page of results.
// T must be a pointer type. getID extracts the ID used as a page token.
func paginate[T any](all []T, pageToken string, maxResults int, getID func(T) string) (int, int) {
	start := 0

	if pageToken != "" {
		for i, item := range all {
			if getID(item) == pageToken {
				start = i

				break
			}
		}
	}

	end := len(all)

	if maxResults > 0 && start+maxResults < end {
		end = start + maxResults
	}

	return start, end
}

// nextTokenFromSlice returns the ID of the element at index next (the first
// item of the next page), or "" when next is at/after the end of the slice and
// there are no further pages. The page token therefore identifies the first
// item of the following page, which paginate() locates via getID.
func nextTokenFromSlice[T any](all []T, next int, getID func(T) string) string {
	if next < 0 || next >= len(all) {
		return ""
	}

	return getID(all[next])
}
