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
