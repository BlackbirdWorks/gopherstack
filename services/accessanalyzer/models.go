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

// FindingSortCriteria mirrors types.SortCriteria (ListFindings/
// ListFindingsV2 request member "sort"). AttributeName is matched against
// the same finding attributes matchesFindingFilter honours ("status",
// "resourceType", "resource", "id") -- see sortFindings.
type FindingSortCriteria struct {
	AttributeName string `json:"attributeName"`
	OrderBy       string `json:"orderBy"`
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
// Values match types.JobStatus (aws-sdk-go-v2/service/accessanalyzer
// types/enums.go:390-397, v1.51.4) -- StartPolicyGeneration completes
// synchronously here, so Running is declared for completeness but never
// currently assigned.
type PolicyGenerationStatus string

const (
	PolicyGenerationStatusRunning   PolicyGenerationStatus = "IN_PROGRESS"
	PolicyGenerationStatusSucceeded PolicyGenerationStatus = "SUCCEEDED"
	PolicyGenerationStatusFailed    PolicyGenerationStatus = "FAILED"
	PolicyGenerationStatusCanceled  PolicyGenerationStatus = "CANCELED"
)

// PolicyGenerationTrail mirrors types.Trail (request side) and
// types.TrailProperties (response side) -- the two are field-identical
// (types/types.go:2357 and :2376, v1.51.4), so gopherstack models them once.
type PolicyGenerationTrail struct {
	AllRegions    *bool
	CloudTrailArn string
	Regions       []string
}

// PolicyGenerationCloudTrailDetails is the request-supplied
// types.CloudTrailDetails (types/types.go:493), stored so GetGeneratedPolicy
// can echo it back as types.CloudTrailProperties (types/types.go:2375) --
// same fields minus AccessRole, which has no response-side counterpart.
type PolicyGenerationCloudTrailDetails struct {
	StartTime  time.Time
	EndTime    time.Time
	AccessRole string
	Trails     []PolicyGenerationTrail
}

// PolicyGeneration represents a policy generation job.
type PolicyGeneration struct {
	CompletedOn       *time.Time
	CloudTrailDetails *PolicyGenerationCloudTrailDetails
	StartedOn         time.Time
	JobID             string
	PrincipalArn      string
	Status            PolicyGenerationStatus
}

// AccessPreviewStatus represents the status of an access preview.
type AccessPreviewStatus string

const (
	AccessPreviewStatusCompleted AccessPreviewStatus = "COMPLETED"
	AccessPreviewStatusCreating  AccessPreviewStatus = "CREATING"
	AccessPreviewStatusFailed    AccessPreviewStatus = "FAILED"
)

// AccessPreview represents an access preview. Configurations is the raw
// per-resource-ARN access control configuration submitted with
// CreateAccessPreview (api_op_CreateAccessPreview.go:39-43, a 13-member
// union per resource type -- types.Configuration). gopherstack's finding
// generation for a preview reuses the analyzer's existing findings
// (ListAccessPreviewFindings in access_previews.go) rather than deriving
// findings from Configurations' semantic content, so it is stored and
// echoed back opaquely as submitted instead of decoded into the full union.
type AccessPreview struct {
	CreatedAt      time.Time
	Configurations map[string]json.RawMessage
	ID             string
	AnalyzerArn    string
	Status         AccessPreviewStatus
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

// RecommendationTypeUnusedPermission is the sole known types.RecommendationType
// value (aws-sdk-go-v2/service/accessanalyzer types/enums.go:579, v1.51.4).
const RecommendationTypeUnusedPermission = "UnusedPermissionRecommendation"

// FindingRecommendation represents a recommendation record for a finding.
type FindingRecommendation struct {
	CompletedAt        *time.Time
	StartedAt          time.Time
	ID                 string
	AnalyzerArn        string
	ResourceArn        string
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

	if pg.CloudTrailDetails != nil {
		ctd := *pg.CloudTrailDetails
		ctd.Trails = append([]PolicyGenerationTrail(nil), pg.CloudTrailDetails.Trails...)
		cp.CloudTrailDetails = &ctd
	}

	return &cp
}

func copyAccessPreview(ap *AccessPreview) *AccessPreview {
	cp := *ap
	if ap.Configurations != nil {
		cp.Configurations = maps.Clone(ap.Configurations)
	}

	return &cp
}
