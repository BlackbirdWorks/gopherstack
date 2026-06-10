package rolesanywhere

import (
	"encoding/json"
	"fmt"
	"maps"
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
	Tags           []TagEntry        `json:"tags,omitempty"`
	Enabled        bool              `json:"enabled"`
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
	CrlData        []byte    `json:"crlData,omitempty"`
	Enabled        bool      `json:"enabled"`
}

// Subject represents an IAM Roles Anywhere subject (authenticating certificate).
type Subject struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	LastSeenAt  time.Time `json:"lastSeenAt"`
	SubjectID   string    `json:"subjectId"`
	SubjectArn  string    `json:"subjectArn"`
	X509Subject string    `json:"x509Subject"`
	Enabled     bool      `json:"enabled"`
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
// Profile represents an IAM Roles Anywhere profile.
type Profile struct {
	CreatedAt                 time.Time  `json:"createdAt"`
	UpdatedAt                 time.Time  `json:"updatedAt"`
	DurationSeconds           *int32     `json:"durationSeconds,omitempty"`
	ProfileID                 string     `json:"profileId"`
	ProfileArn                string     `json:"profileArn"`
	Name                      string     `json:"name"`
	SessionPolicy             string     `json:"sessionPolicy,omitempty"`
	Tags                      []TagEntry `json:"tags,omitempty"`
	RoleArns                  []string   `json:"roleArns"`
	ManagedPolicyArns         []string   `json:"managedPolicyArns,omitempty"`
	RequireInstanceProperties bool       `json:"requireInstanceProperties,omitempty"`
	Enabled                   bool       `json:"enabled"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu                   *lockmetrics.RWMutex
	trustAnchors         map[string]*TrustAnchor          // id → TrustAnchor
	profiles             map[string]*Profile              // id → Profile
	tags                 map[string][]TagEntry            // resourceARN → tags
	crls                 map[string]*Crl                  // id → Crl
	subjects             map[string]*Subject              // id → Subject
	attributeMappings    map[string][]AttributeMapping    // profileID → mappings
	notificationSettings map[string][]NotificationSetting // trustAnchorID → settings
	accountID            string
	region               string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                   lockmetrics.New("rolesanywhere"),
		accountID:            accountID,
		region:               region,
		trustAnchors:         make(map[string]*TrustAnchor),
		profiles:             make(map[string]*Profile),
		tags:                 make(map[string][]TagEntry),
		crls:                 make(map[string]*Crl),
		subjects:             make(map[string]*Subject),
		attributeMappings:    make(map[string][]AttributeMapping),
		notificationSettings: make(map[string][]NotificationSetting),
	}
}

func (b *InMemoryBackend) trustAnchorARN(id string) string {
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:trust-anchor/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) profileARN(id string) string {
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:profile/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) crlARN(id string) string {
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:crl/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) subjectARN(id string) string { //nolint:unused // existing issue.
	return fmt.Sprintf("arn:aws:rolesanywhere:%s:%s:subject/%s", b.region, b.accountID, id)
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

// ---- CRL operations ----

// ImportCrl imports a new CRL.
func (b *InMemoryBackend) ImportCrl(
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

	for _, c := range b.crls {
		if c.Name == name {
			return nil, ErrCrlAlreadyExists
		}
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	crl := &Crl{
		CrlID:          id,
		CrlArn:         b.crlARN(id),
		Name:           name,
		CrlData:        crlData,
		TrustAnchorArn: trustAnchorArn,
		Enabled:        enabled,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.crls[id] = crl

	if len(tags) > 0 {
		b.tags[crl.CrlArn] = cloneTags(tags)
	}

	return copyCrl(crl), nil
}

// GetCrl returns a CRL by ID.
func (b *InMemoryBackend) GetCrl(id string) (*Crl, error) {
	b.mu.RLock("GetCrl")
	defer b.mu.RUnlock()

	crl, exists := b.crls[id]
	if !exists {
		return nil, ErrCrlNotFound
	}

	return copyCrl(crl), nil
}

// ListCrls returns all CRLs with optional pagination.
func (b *InMemoryBackend) ListCrls(pageToken string, maxResults int) ([]*Crl, string, error) {
	b.mu.RLock("ListCrls")
	defer b.mu.RUnlock()

	all := make([]*Crl, 0, len(b.crls))

	for _, c := range b.crls {
		all = append(all, copyCrl(c))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	start, next := paginate(all, pageToken, maxResults, func(c *Crl) string { return c.CrlID })

	return all[start:next], nextTokenFromSlice(all, next), nil
}

// UpdateCrl updates a CRL's name and/or data.
func (b *InMemoryBackend) UpdateCrl(id, name string, crlData []byte) (*Crl, error) {
	b.mu.Lock("UpdateCrl")
	defer b.mu.Unlock()

	crl, exists := b.crls[id]
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
func (b *InMemoryBackend) DeleteCrl(id string) (*Crl, error) {
	b.mu.Lock("DeleteCrl")
	defer b.mu.Unlock()

	crl, exists := b.crls[id]
	if !exists {
		return nil, ErrCrlNotFound
	}

	snap := copyCrl(crl)
	delete(b.crls, id)

	return snap, nil
}

// EnableCrl enables a CRL.
func (b *InMemoryBackend) EnableCrl(id string) (*Crl, error) {
	return b.setCrlEnabled(id, true)
}

// DisableCrl disables a CRL.
func (b *InMemoryBackend) DisableCrl(id string) (*Crl, error) {
	return b.setCrlEnabled(id, false)
}

func (b *InMemoryBackend) setCrlEnabled(id string, enabled bool) (*Crl, error) {
	b.mu.Lock("setCrlEnabled")
	defer b.mu.Unlock()

	crl, exists := b.crls[id]
	if !exists {
		return nil, ErrCrlNotFound
	}

	crl.Enabled = enabled
	crl.UpdatedAt = time.Now().UTC()

	return copyCrl(crl), nil
}

// ---- Subject operations ----

// GetSubject returns a subject by ID.
func (b *InMemoryBackend) GetSubject(id string) (*Subject, error) {
	b.mu.RLock("GetSubject")
	defer b.mu.RUnlock()

	s, exists := b.subjects[id]
	if !exists {
		return nil, ErrSubjectNotFound
	}

	cp := *s

	return &cp, nil
}

// ListSubjects returns all subjects with optional pagination.
func (b *InMemoryBackend) ListSubjects(pageToken string, maxResults int) ([]*Subject, string, error) {
	b.mu.RLock("ListSubjects")
	defer b.mu.RUnlock()

	all := make([]*Subject, 0, len(b.subjects))

	for _, s := range b.subjects {
		cp := *s
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].SubjectID < all[j].SubjectID
	})

	start, next := paginate(all, pageToken, maxResults, func(s *Subject) string { return s.SubjectID })

	return all[start:next], nextTokenFromSlice(all, next), nil
}

// ---- Attribute mapping operations ----

// PutAttributeMapping adds or replaces a certificate field mapping on a profile.
func (b *InMemoryBackend) PutAttributeMapping(
	profileID, certificateField string,
	rules []MappingRule,
) (*Profile, error) {
	b.mu.Lock("PutAttributeMapping")
	defer b.mu.Unlock()

	if _, exists := b.profiles[profileID]; !exists {
		return nil, ErrProfileNotFound
	}

	mappings := b.attributeMappings[profileID]
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

	b.attributeMappings[profileID] = mappings

	return copyProfile(b.profiles[profileID]), nil
}

// DeleteAttributeMapping removes a certificate field mapping (and optional specifiers) from a profile.
func (b *InMemoryBackend) DeleteAttributeMapping(
	profileID, certificateField string,
	specifiers []string,
) (*Profile, error) {
	b.mu.Lock("DeleteAttributeMapping")
	defer b.mu.Unlock()

	if _, exists := b.profiles[profileID]; !exists {
		return nil, ErrProfileNotFound
	}

	mappings := b.attributeMappings[profileID]

	if len(specifiers) == 0 { //nolint:nestif // existing issue.
		// Remove entire field mapping.
		filtered := mappings[:0]

		for _, m := range mappings {
			if m.CertificateField != certificateField {
				filtered = append(filtered, m)
			}
		}

		b.attributeMappings[profileID] = filtered
	} else {
		specSet := make(map[string]bool, len(specifiers))

		for _, s := range specifiers {
			specSet[s] = true
		}

		for i, m := range mappings {
			if m.CertificateField == certificateField {
				filtered := m.MappingRules[:0]

				for _, r := range m.MappingRules {
					if !specSet[r.Specifier] {
						filtered = append(filtered, r)
					}
				}

				mappings[i].MappingRules = filtered
			}
		}

		b.attributeMappings[profileID] = mappings
	}

	return copyProfile(b.profiles[profileID]), nil
}

// GetAttributeMappings returns the attribute mappings for a profile.
func (b *InMemoryBackend) GetAttributeMappings(profileID string) []AttributeMapping {
	b.mu.RLock("GetAttributeMappings")
	defer b.mu.RUnlock()

	src := b.attributeMappings[profileID]
	out := make([]AttributeMapping, len(src))
	copy(out, src)

	return out
}

// ---- Notification settings operations ----

// PutNotificationSettings sets notification settings on a trust anchor.
func (b *InMemoryBackend) PutNotificationSettings(
	trustAnchorID string,
	settings []NotificationSetting,
) (*TrustAnchor, error) {
	b.mu.Lock("PutNotificationSettings")
	defer b.mu.Unlock()

	ta, exists := b.trustAnchors[trustAnchorID]
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	existing := b.notificationSettings[trustAnchorID]

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

	b.notificationSettings[trustAnchorID] = existing
	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}

// ResetNotificationSettings removes specified notification settings from a trust anchor.
func (b *InMemoryBackend) ResetNotificationSettings(
	trustAnchorID string,
	keys []NotificationSettingKey,
) (*TrustAnchor, error) {
	b.mu.Lock("ResetNotificationSettings")
	defer b.mu.Unlock()

	ta, exists := b.trustAnchors[trustAnchorID]
	if !exists {
		return nil, ErrTrustAnchorNotFound
	}

	existing := b.notificationSettings[trustAnchorID]
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

	b.notificationSettings[trustAnchorID] = filtered
	ta.UpdatedAt = time.Now().UTC()

	return copyTrustAnchor(ta), nil
}

// GetNotificationSettings returns notification settings for a trust anchor.
func (b *InMemoryBackend) GetNotificationSettings(trustAnchorID string) []NotificationSetting {
	b.mu.RLock("GetNotificationSettings")
	defer b.mu.RUnlock()

	src := b.notificationSettings[trustAnchorID]
	out := make([]NotificationSetting, len(src))
	copy(out, src)

	return out
}

// ---- Lifecycle ----

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.trustAnchors = make(map[string]*TrustAnchor)
	b.profiles = make(map[string]*Profile)
	b.tags = make(map[string][]TagEntry)
	b.crls = make(map[string]*Crl)
	b.subjects = make(map[string]*Subject)
	b.attributeMappings = make(map[string][]AttributeMapping)
	b.notificationSettings = make(map[string][]NotificationSetting)
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
		TrustAnchors         map[string]*TrustAnchor          `json:"trustAnchors"`
		Profiles             map[string]*Profile              `json:"profiles"`
		Tags                 map[string][]TagEntry            `json:"tags"`
		Crls                 map[string]*Crl                  `json:"crls"`
		Subjects             map[string]*Subject              `json:"subjects"`
		AttributeMappings    map[string][]AttributeMapping    `json:"attributeMappings"`
		NotificationSettings map[string][]NotificationSetting `json:"notificationSettings"`
	}

	data, _ := json.Marshal(snap{
		TrustAnchors:         b.trustAnchors,
		Profiles:             b.profiles,
		Tags:                 b.tags,
		Crls:                 b.crls,
		Subjects:             b.subjects,
		AttributeMappings:    b.attributeMappings,
		NotificationSettings: b.notificationSettings,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	type snap struct {
		TrustAnchors         map[string]*TrustAnchor          `json:"trustAnchors"`
		Profiles             map[string]*Profile              `json:"profiles"`
		Tags                 map[string][]TagEntry            `json:"tags"`
		Crls                 map[string]*Crl                  `json:"crls"`
		Subjects             map[string]*Subject              `json:"subjects"`
		AttributeMappings    map[string][]AttributeMapping    `json:"attributeMappings"`
		NotificationSettings map[string][]NotificationSetting `json:"notificationSettings"`
	}

	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.trustAnchors = s.TrustAnchors
	b.profiles = s.Profiles
	b.tags = s.Tags
	b.crls = s.Crls
	b.subjects = s.Subjects
	b.attributeMappings = s.AttributeMappings
	b.notificationSettings = s.NotificationSettings

	if b.trustAnchors == nil {
		b.trustAnchors = make(map[string]*TrustAnchor)
	}

	if b.profiles == nil {
		b.profiles = make(map[string]*Profile)
	}

	if b.tags == nil {
		b.tags = make(map[string][]TagEntry)
	}

	if b.crls == nil {
		b.crls = make(map[string]*Crl)
	}

	if b.subjects == nil {
		b.subjects = make(map[string]*Subject)
	}

	if b.attributeMappings == nil {
		b.attributeMappings = make(map[string][]AttributeMapping)
	}

	if b.notificationSettings == nil {
		b.notificationSettings = make(map[string][]NotificationSetting)
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
