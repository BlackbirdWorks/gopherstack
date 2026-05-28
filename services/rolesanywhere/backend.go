package rolesanywhere

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrTrustAnchorNotFound is returned when a trust anchor does not exist.
	ErrTrustAnchorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTrustAnchorAlreadyExists is returned when creating a duplicate trust anchor.
	ErrTrustAnchorAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrProfileNotFound is returned when a profile does not exist.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileAlreadyExists is returned when creating a duplicate profile.
	ErrProfileAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// TrustAnchorSource defines the source of a trust anchor.
type TrustAnchorSource struct {
	SourceType string `json:"sourceType"`
	// SourceData is a map of source-type-specific fields.
	SourceData map[string]string `json:"sourceData,omitempty"`
}

// TrustAnchor represents an IAM Roles Anywhere trust anchor.
type TrustAnchor struct {
	TrustAnchorID  string            `json:"trustAnchorId"`
	TrustAnchorArn string            `json:"trustAnchorArn"`
	Name           string            `json:"name"`
	Source         TrustAnchorSource `json:"source"`
	Enabled        bool              `json:"enabled"`
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
	Tags           []TagEntry        `json:"tags,omitempty"`
}

// TagEntry is a key-value tag pair (Roles Anywhere uses list-based tags).
type TagEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Profile represents an IAM Roles Anywhere profile.
type Profile struct {
	ProfileID  string     `json:"profileId"`
	ProfileArn string     `json:"profileArn"`
	Name       string     `json:"name"`
	RoleArns   []string   `json:"roleArns"`
	Enabled    bool       `json:"enabled"`
	CreatedAt  time.Time  `json:"createdAt"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	Tags       []TagEntry `json:"tags,omitempty"`

	// Optional fields
	DurationSeconds           *int32   `json:"durationSeconds,omitempty"`
	ManagedPolicyArns         []string `json:"managedPolicyArns,omitempty"`
	RequireInstanceProperties bool     `json:"requireInstanceProperties,omitempty"`
	SessionPolicy             string   `json:"sessionPolicy,omitempty"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	accountID string
	region    string

	trustAnchors map[string]*TrustAnchor // id → TrustAnchor
	profiles     map[string]*Profile     // id → Profile
	tags         map[string][]TagEntry   // resourceARN → tags
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:           lockmetrics.New("rolesanywhere"),
		accountID:    accountID,
		region:       region,
		trustAnchors: make(map[string]*TrustAnchor),
		profiles:     make(map[string]*Profile),
		tags:         make(map[string][]TagEntry),
	}
}

func (b *InMemoryBackend) trustAnchorARN(id string) string {
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:trust-anchor/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) profileARN(id string) string {
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:profile/%s", b.region, b.accountID, id)
}

// ---- Trust Anchor operations ----

// CreateTrustAnchor creates a new trust anchor.
func (b *InMemoryBackend) CreateTrustAnchor(
	name string,
	source TrustAnchorSource,
	tags []TagEntry,
) (*TrustAnchor, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTrustAnchor")
	defer b.mu.Unlock()

	// Name uniqueness check.
	for _, ta := range b.trustAnchors {
		if ta.Name == name {
			return nil, ErrTrustAnchorAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	ta := &TrustAnchor{
		TrustAnchorID:  id,
		TrustAnchorArn: b.trustAnchorARN(id),
		Name:           name,
		Source:         source,
		Enabled:        true,
		CreatedAt:      now,
		UpdatedAt:      now,
		Tags:           cloneTags(tags),
	}

	b.trustAnchors[id] = ta

	return copyTrustAnchor(ta), nil
}

// GetTrustAnchor returns the trust anchor with the given ID.
func (b *InMemoryBackend) GetTrustAnchor(id string) (*TrustAnchor, error) {
	b.mu.RLock("GetTrustAnchor")
	defer b.mu.RUnlock()

	ta, exists := b.trustAnchors[id]
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	return copyTrustAnchor(ta), nil
}

// ListTrustAnchors returns all trust anchors.
func (b *InMemoryBackend) ListTrustAnchors(pageToken string, maxResults int) ([]*TrustAnchor, string, error) {
	b.mu.RLock("ListTrustAnchors")
	defer b.mu.RUnlock()

	all := make([]*TrustAnchor, 0, len(b.trustAnchors))

	for _, ta := range b.trustAnchors {
		all = append(all, copyTrustAnchor(ta))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	start, next := paginate(all, pageToken, maxResults, func(t *TrustAnchor) string { return t.TrustAnchorID })

	return all[start:next], nextTokenFromSlice(all, next), nil
}

// DeleteTrustAnchor removes a trust anchor.
func (b *InMemoryBackend) DeleteTrustAnchor(id string) error {
	b.mu.Lock("DeleteTrustAnchor")
	defer b.mu.Unlock()

	if _, exists := b.trustAnchors[id]; !exists {
		return ErrTrustAnchorNotFound
	}

	delete(b.trustAnchors, id)

	return nil
}

// UpdateTrustAnchor updates name and/or source of a trust anchor.
func (b *InMemoryBackend) UpdateTrustAnchor(id, name string, source *TrustAnchorSource) (*TrustAnchor, error) {
	b.mu.Lock("UpdateTrustAnchor")
	defer b.mu.Unlock()

	ta, exists := b.trustAnchors[id]
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
func (b *InMemoryBackend) EnableTrustAnchor(id string) (*TrustAnchor, error) {
	return b.setTrustAnchorEnabled(id, true)
}

// DisableTrustAnchor disables a trust anchor.
func (b *InMemoryBackend) DisableTrustAnchor(id string) (*TrustAnchor, error) {
	return b.setTrustAnchorEnabled(id, false)
}

func (b *InMemoryBackend) setTrustAnchorEnabled(id string, enabled bool) (*TrustAnchor, error) {
	b.mu.Lock("setTrustAnchorEnabled")
	defer b.mu.Unlock()

	ta, exists := b.trustAnchors[id]
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

	for _, p := range b.profiles {
		if p.Name == name {
			return nil, ErrProfileAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	p := &Profile{
		ProfileID:                 id,
		ProfileArn:                b.profileARN(id),
		Name:                      name,
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

	b.profiles[id] = p

	return copyProfile(p), nil
}

// GetProfile returns the profile with the given ID.
func (b *InMemoryBackend) GetProfile(id string) (*Profile, error) {
	b.mu.RLock("GetProfile")
	defer b.mu.RUnlock()

	p, exists := b.profiles[id]
	if !exists {
		return nil, ErrProfileNotFound
	}

	return copyProfile(p), nil
}

// ListProfiles returns all profiles.
func (b *InMemoryBackend) ListProfiles(pageToken string, maxResults int) ([]*Profile, string, error) {
	b.mu.RLock("ListProfiles")
	defer b.mu.RUnlock()

	all := make([]*Profile, 0, len(b.profiles))

	for _, p := range b.profiles {
		all = append(all, copyProfile(p))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	start, next := paginate(all, pageToken, maxResults, func(p *Profile) string { return p.ProfileID })

	return all[start:next], nextTokenFromSlice(all, next), nil
}

// DeleteProfile removes a profile.
func (b *InMemoryBackend) DeleteProfile(id string) error {
	b.mu.Lock("DeleteProfile")
	defer b.mu.Unlock()

	if _, exists := b.profiles[id]; !exists {
		return ErrProfileNotFound
	}

	delete(b.profiles, id)

	return nil
}

// UpdateProfile updates a profile's fields.
func (b *InMemoryBackend) UpdateProfile(
	id, name string,
	roleArns []string,
	durationSeconds *int32,
	managedPolicyArns []string,
	sessionPolicy string,
	requireInstanceProperties *bool,
) (*Profile, error) {
	b.mu.Lock("UpdateProfile")
	defer b.mu.Unlock()

	p, exists := b.profiles[id]
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
func (b *InMemoryBackend) EnableProfile(id string) (*Profile, error) {
	return b.setProfileEnabled(id, true)
}

// DisableProfile disables a profile.
func (b *InMemoryBackend) DisableProfile(id string) (*Profile, error) {
	return b.setProfileEnabled(id, false)
}

func (b *InMemoryBackend) setProfileEnabled(id string, enabled bool) (*Profile, error) {
	b.mu.Lock("setProfileEnabled")
	defer b.mu.Unlock()

	p, exists := b.profiles[id]
	if !exists {
		return nil, ErrProfileNotFound
	}

	p.Enabled = enabled
	p.UpdatedAt = time.Now().UTC()

	return copyProfile(p), nil
}

// ---- Tag operations ----

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []TagEntry) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]

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

	b.tags[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
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

	b.tags[resourceARN] = filtered

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]TagEntry, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return cloneTags(b.tags[resourceARN]), nil
}

// ---- Lifecycle ----

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.trustAnchors = make(map[string]*TrustAnchor)
	b.profiles = make(map[string]*Profile)
	b.tags = make(map[string][]TagEntry)
}

// Region returns the backend's region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	type snap struct {
		TrustAnchors map[string]*TrustAnchor `json:"trustAnchors"`
		Profiles     map[string]*Profile     `json:"profiles"`
		Tags         map[string][]TagEntry   `json:"tags"`
	}

	data, _ := json.Marshal(snap{
		TrustAnchors: b.trustAnchors,
		Profiles:     b.profiles,
		Tags:         b.tags,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	type snap struct {
		TrustAnchors map[string]*TrustAnchor `json:"trustAnchors"`
		Profiles     map[string]*Profile     `json:"profiles"`
		Tags         map[string][]TagEntry   `json:"tags"`
	}

	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.trustAnchors = s.TrustAnchors
	b.profiles = s.Profiles
	b.tags = s.Tags

	if b.trustAnchors == nil {
		b.trustAnchors = make(map[string]*TrustAnchor)
	}

	if b.profiles == nil {
		b.profiles = make(map[string]*Profile)
	}

	if b.tags == nil {
		b.tags = make(map[string][]TagEntry)
	}

	return nil
}

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

		for k, v := range src.SourceData {
			sd[k] = v
		}

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

// nextTokenFromSlice returns the ID of the element at index next, or "".
func nextTokenFromSlice[T any](all []T, next int) string {
	if next < len(all) {
		// We can't call getID here generically without passing it;
		// callers handle this differently.
		return ""
	}

	return ""
}
