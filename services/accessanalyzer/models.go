package accessanalyzer

import (
	"encoding/json"
	"maps"
	"time"
)

// AnalyzerType represents the type of an Access Analyzer analyzer.
type AnalyzerType string

const (
	AnalyzerTypeAccount                  AnalyzerType = "ACCOUNT"
	AnalyzerTypeOrganization             AnalyzerType = "ORGANIZATION"
	AnalyzerTypeAccountUnusedAccess      AnalyzerType = "ACCOUNT_UNUSED_ACCESS"
	AnalyzerTypeOrganizationUnusedAccess AnalyzerType = "ORGANIZATION_UNUSED_ACCESS"
)

// AnalyzerStatus represents the status of an analyzer.
type AnalyzerStatus string

const (
	AnalyzerStatusActive   AnalyzerStatus = "ACTIVE"
	AnalyzerStatusCreating AnalyzerStatus = "CREATING"
	AnalyzerStatusDisabled AnalyzerStatus = "DISABLED"
	AnalyzerStatusFailed   AnalyzerStatus = "FAILED"
)

// FindingStatus represents the status of a finding.
type FindingStatus string

const (
	FindingStatusActive   FindingStatus = "ACTIVE"
	FindingStatusArchived FindingStatus = "ARCHIVED"
	FindingStatusResolved FindingStatus = "RESOLVED"
)

// FilterCriterion is a single criterion in a finding filter.
type FilterCriterion struct {
	Contains []string `json:"contains,omitempty"`
	Eq       []string `json:"eq,omitempty"`
	Exists   *bool    `json:"exists,omitempty"`
	Neq      []string `json:"neq,omitempty"`
}

// Analyzer represents an IAM Access Analyzer analyzer.
//
// Configuration holds the raw wire body of the AnalyzerConfiguration union
// (CreateAnalyzer/UpdateAnalyzer request, GetAnalyzer/UpdateAnalyzer
// response) exactly as the client sent it -- e.g.
// {"unusedAccess":{"unusedAccessAge":90}} or
// {"internalAccess":{"analysisRule":{...}}}. gopherstack stores it opaquely
// (no semantic interpretation of unused-access/internal-access analysis
// rules) rather than fabricating partial behavior for it; this still avoids
// silently dropping client-supplied configuration on the floor, which the
// previous no-op UpdateAnalyzer did.
type Analyzer struct {
	CreatedAt              time.Time         `json:"createdAt"`
	Tags                   map[string]string `json:"tags,omitempty"`
	LastResourceAnalyzedAt *time.Time        `json:"lastResourceAnalyzedAt,omitempty"`
	Arn                    string            `json:"arn"`
	Name                   string            `json:"name"`
	Type                   AnalyzerType      `json:"type"`
	Status                 AnalyzerStatus    `json:"status"`
	Configuration          json.RawMessage   `json:"configuration,omitempty"`
}

// ArchiveRule represents an archive rule for an analyzer.
//
// AnalyzerName identifies the owning analyzer. It is never part of the wire
// response (archiveRuleToJSON in handler_archive_rules.go builds that by
// hand), so it is tagged json:"-"; it exists purely so archiveRuleKeyFn
// (store_setup.go) can derive the composite "analyzerName|ruleName"
// store.Table key from the value alone, matching the primary key of the
// map[string]map[string]*ArchiveRule this type used to live in.
type ArchiveRule struct {
	Filter       map[string]FilterCriterion `json:"filter"`
	CreatedAt    time.Time                  `json:"createdAt"`
	UpdatedAt    time.Time                  `json:"updatedAt"`
	RuleName     string                     `json:"ruleName"`
	AnalyzerName string                     `json:"-"`
}

// Finding represents a single IAM Access Analyzer finding.
type Finding struct {
	UpdatedAt    time.Time         `json:"updatedAt"`
	CreatedAt    time.Time         `json:"createdAt"`
	Principal    map[string]string `json:"principal,omitempty"`
	Condition    map[string]string `json:"condition,omitempty"`
	IsPublic     *bool             `json:"isPublic,omitempty"`
	ID           string            `json:"id"`
	AnalyzerArn  string            `json:"analyzerArn"`
	Status       FindingStatus     `json:"status"`
	ResourceType string            `json:"resourceType"`
	ResourceArn  string            `json:"resourceArn"`
	Action       []string          `json:"action,omitempty"`
}

// PolicyGenerationStatus represents the status of a policy generation job.
type PolicyGenerationStatus string

const (
	PolicyGenerationStatusRunning   PolicyGenerationStatus = "RUNNING"
	PolicyGenerationStatusSucceeded PolicyGenerationStatus = "SUCCEEDED"
	PolicyGenerationStatusFailed    PolicyGenerationStatus = "FAILED"
	PolicyGenerationStatusCanceled  PolicyGenerationStatus = "CANCELED"
)

// PolicyGeneration represents a policy generation job.
type PolicyGeneration struct {
	CompletedOn  *time.Time
	StartedOn    time.Time
	JobID        string
	PrincipalArn string
	Status       PolicyGenerationStatus
}

// AccessPreviewStatus represents the status of an access preview.
type AccessPreviewStatus string

const (
	AccessPreviewStatusCompleted AccessPreviewStatus = "COMPLETED"
	AccessPreviewStatusCreating  AccessPreviewStatus = "CREATING"
	AccessPreviewStatusFailed    AccessPreviewStatus = "FAILED"
)

// AccessPreview represents an access preview.
type AccessPreview struct {
	CreatedAt   time.Time
	ID          string
	AnalyzerArn string
	Status      AccessPreviewStatus
}

// AnalyzedResource represents a resource analyzed by an analyzer.
type AnalyzedResource struct {
	AnalyzedAt   time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ResourceArn  string
	ResourceType string
	AnalyzerArn  string
	IsPublic     bool
}

// FindingRecommendation represents a recommendation record for a finding.
type FindingRecommendation struct {
	CompletedAt        *time.Time
	StartedAt          time.Time
	ID                 string
	AnalyzerArn        string
	RecommendationType string
	Status             string
}

// ---- copy/clone helpers ----

// cloneTags returns a copy of a string map (nil-safe).
func cloneTags(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// cloneFilter returns a copy of a filter map (nil-safe).
func cloneFilter(f map[string]FilterCriterion) map[string]FilterCriterion {
	if f == nil {
		return nil
	}

	out := make(map[string]FilterCriterion, len(f))
	maps.Copy(out, f)

	return out
}

func copyAnalyzer(a *Analyzer) *Analyzer {
	cp := *a
	cp.Tags = cloneTags(a.Tags)
	cp.Configuration = append(json.RawMessage(nil), a.Configuration...)

	return &cp
}

func copyArchiveRule(r *ArchiveRule) *ArchiveRule {
	cp := *r
	cp.Filter = cloneFilter(r.Filter)

	return &cp
}

func copyFinding(f *Finding) *Finding {
	cp := *f
	cp.Action = append([]string(nil), f.Action...)
	cp.Principal = cloneTags(f.Principal)
	cp.Condition = cloneTags(f.Condition)

	return &cp
}

func copyPolicyGeneration(pg *PolicyGeneration) *PolicyGeneration {
	cp := *pg

	if pg.CompletedOn != nil {
		t := *pg.CompletedOn
		cp.CompletedOn = &t
	}

	return &cp
}

func copyAccessPreview(ap *AccessPreview) *AccessPreview {
	cp := *ap

	return &cp
}
