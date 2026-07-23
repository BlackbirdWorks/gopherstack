package rolesanywhere

import (
	"maps"
	"time"
)

// TrustAnchorSource defines the source of a trust anchor.
type TrustAnchorSource struct {
	// SourceData is a map of source-type-specific fields.
	SourceData map[string]string `json:"sourceData,omitempty"`
	SourceType string            `json:"sourceType"`
}

// TrustAnchor represents an IAM Roles Anywhere trust anchor.
//
// Note: real AWS TrustAnchorDetail carries no "tags" field at all -- tags are
// visible only via ListTagsForResource/TagResource/UntagResource, keyed by
// ARN in InMemoryBackend.tags (see store.go). This struct therefore has no
// Tags field of its own; a prior version did, which both invented a
// non-existent "tags" key on every Create/Get/List/Update/Enable/Disable/
// Delete response AND caused the exposed value to permanently desync from
// TagResource/UntagResource (which write to the separate ARN-keyed map).
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
	Enabled bool `json:"enabled"`
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
//
// ConfiguredBy mirrors AWS's NotificationSettingDetail.configuredBy (the
// principal that configured the setting -- rolesanywhere.amazonaws.com for
// AWS-default settings, or the account ID for customer-configured ones).
// gopherstack never seeds default settings, so every setting present here
// was configured via PutNotificationSettings and ConfiguredBy is always the
// backend's account ID.
type NotificationSetting struct {
	Threshold    *int32 `json:"threshold,omitempty"`
	Event        string `json:"event"`
	Channel      string `json:"channel,omitempty"`
	ConfiguredBy string `json:"configuredBy,omitempty"`
	Enabled      bool   `json:"enabled"`
}

// NotificationSettingKey identifies a notification setting to reset.
type NotificationSettingKey struct {
	Event   string `json:"event"`
	Channel string `json:"channel,omitempty"`
}

// Profile represents an IAM Roles Anywhere profile.
//
// Note: like TrustAnchor, this carries no Tags field -- real AWS
// ProfileDetail has none either; see TrustAnchor's doc comment.
type Profile struct {
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	DurationSeconds *int32    `json:"durationSeconds,omitempty"`
	ProfileID       string    `json:"profileId"`
	ProfileArn      string    `json:"profileArn"`
	Name            string    `json:"name"`
	SessionPolicy   string    `json:"sessionPolicy,omitempty"`
	// CreatedBy is the AWS account that created the profile
	// (ProfileDetail.createdBy); this is a single-account emulator, so it is
	// always the backend's own account ID.
	CreatedBy string `json:"createdBy,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); see
	// TrustAnchor.region's doc comment for why it is unexported.
	region                    string
	RoleArns                  []string `json:"roleArns"`
	ManagedPolicyArns         []string `json:"managedPolicyArns,omitempty"`
	RequireInstanceProperties bool     `json:"requireInstanceProperties,omitempty"`
	Enabled                   bool     `json:"enabled"`
	// AcceptRoleSessionName determines whether a custom role session name
	// will be accepted in a temporary credential request
	// (CreateProfileInput/UpdateProfileInput/ProfileDetail.
	// acceptRoleSessionName). Only meaningful to the separate CreateSession
	// data-plane API, which gopherstack does not implement, but the field
	// itself is stored/echoed for wire parity.
	AcceptRoleSessionName bool `json:"acceptRoleSessionName,omitempty"`
}

// ---- copy helpers ----

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
