package accessanalyzer

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

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

// ErrPolicyGenerationNotFound is returned when a policy generation job is not found.
var ErrPolicyGenerationNotFound = newNotFoundErr("PolicyGenerationNotFound")

// ErrAccessPreviewNotFound is returned when an access preview is not found.
var ErrAccessPreviewNotFound = newNotFoundErr("AccessPreviewNotFound")

// ErrAnalyzedResourceNotFound is returned when an analyzed resource is not found.
var ErrAnalyzedResourceNotFound = newNotFoundErr("AnalyzedResourceNotFound")

func newNotFoundErr(msg string) error {
	return &notFoundErr{msg: msg}
}

type notFoundErr struct{ msg string } //nolint:errname // existing issue.

func (e *notFoundErr) Error() string { return e.msg }

// ApplyArchiveRule applies all archive rules for an analyzer to its findings.
// Findings that match any archive rule filter are archived.
func (b *InMemoryBackend) ApplyArchiveRule(analyzerArn, ruleName string) error {
	b.mu.Lock("ApplyArchiveRule")
	defer b.mu.Unlock()

	var analyzer *Analyzer

	for _, a := range b.analyzers {
		if a.Arn == analyzerArn {
			analyzer = a

			break
		}
	}

	if analyzer == nil {
		return ErrAnalyzerNotFound
	}

	rules := b.archiveRules[analyzer.Name]
	if ruleName != "" {
		if _, ok := rules[ruleName]; !ok {
			return ErrArchiveRuleNotFound
		}
	}

	now := time.Now().UTC()

	for _, f := range b.findings[analyzer.Name] {
		if f.Status != FindingStatusActive {
			continue
		}

		f.Status = FindingStatusArchived
		f.UpdatedAt = now
	}

	return nil
}

// StartPolicyGeneration creates a new policy generation job.
func (b *InMemoryBackend) StartPolicyGeneration(principalArn string) (*PolicyGeneration, error) {
	b.mu.Lock("StartPolicyGeneration")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	completed := now

	pg := &PolicyGeneration{
		JobID:        uuid.NewString(),
		PrincipalArn: principalArn,
		Status:       PolicyGenerationStatusSucceeded,
		StartedOn:    now,
		CompletedOn:  &completed,
	}

	b.policyGenerations[pg.JobID] = pg

	return copyPolicyGeneration(pg), nil
}

// GetPolicyGeneration returns a policy generation job by ID.
func (b *InMemoryBackend) GetPolicyGeneration(jobID string) (*PolicyGeneration, error) {
	b.mu.RLock("GetPolicyGeneration")
	defer b.mu.RUnlock()

	pg, ok := b.policyGenerations[jobID]
	if !ok {
		return nil, ErrPolicyGenerationNotFound
	}

	return copyPolicyGeneration(pg), nil
}

// CancelPolicyGeneration cancels a policy generation job.
func (b *InMemoryBackend) CancelPolicyGeneration(jobID string) error {
	b.mu.Lock("CancelPolicyGeneration")
	defer b.mu.Unlock()

	pg, ok := b.policyGenerations[jobID]
	if !ok {
		return ErrPolicyGenerationNotFound
	}

	pg.Status = PolicyGenerationStatusCanceled

	return nil
}

// ListPolicyGenerations returns all policy generation jobs, optionally filtered by principalArn.
func (b *InMemoryBackend) ListPolicyGenerations(principalArn string) ([]*PolicyGeneration, error) {
	b.mu.RLock("ListPolicyGenerations")
	defer b.mu.RUnlock()

	result := make([]*PolicyGeneration, 0, len(b.policyGenerations))

	for _, pg := range b.policyGenerations {
		if principalArn != "" && pg.PrincipalArn != principalArn {
			continue
		}

		result = append(result, copyPolicyGeneration(pg))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].JobID < result[j].JobID
	})

	return result, nil
}

// CreateAccessPreview creates a new access preview.
func (b *InMemoryBackend) CreateAccessPreview(analyzerArn string) (*AccessPreview, error) {
	b.mu.Lock("CreateAccessPreview")
	defer b.mu.Unlock()

	var found bool

	for _, a := range b.analyzers {
		if a.Arn == analyzerArn {
			found = true

			break
		}
	}

	if !found {
		return nil, ErrAnalyzerNotFound
	}

	now := time.Now().UTC()
	ap := &AccessPreview{
		ID:          uuid.NewString(),
		AnalyzerArn: analyzerArn,
		Status:      AccessPreviewStatusCompleted,
		CreatedAt:   now,
	}

	b.accessPreviews[ap.ID] = ap

	return copyAccessPreview(ap), nil
}

// GetAccessPreview returns an access preview by ID.
func (b *InMemoryBackend) GetAccessPreview(accessPreviewID string) (*AccessPreview, error) {
	b.mu.RLock("GetAccessPreview")
	defer b.mu.RUnlock()

	ap, ok := b.accessPreviews[accessPreviewID]
	if !ok {
		return nil, ErrAccessPreviewNotFound
	}

	return copyAccessPreview(ap), nil
}

// ListAccessPreviews returns all access previews for a given analyzerArn.
func (b *InMemoryBackend) ListAccessPreviews(analyzerArn string) ([]*AccessPreview, error) {
	b.mu.RLock("ListAccessPreviews")
	defer b.mu.RUnlock()

	result := make([]*AccessPreview, 0)

	for _, ap := range b.accessPreviews {
		if analyzerArn != "" && ap.AnalyzerArn != analyzerArn {
			continue
		}

		result = append(result, copyAccessPreview(ap))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// ListAccessPreviewFindings returns findings from the analyzer associated with the preview.
func (b *InMemoryBackend) ListAccessPreviewFindings(
	accessPreviewID string,
	maxResults int,
	nextToken string,
) ([]*Finding, string, error) {
	b.mu.RLock("ListAccessPreviewFindings")
	defer b.mu.RUnlock()

	ap, ok := b.accessPreviews[accessPreviewID]
	if !ok {
		return nil, "", ErrAccessPreviewNotFound
	}

	// Find the analyzer by ARN.
	var analyzerName string

	for _, a := range b.analyzers {
		if a.Arn == ap.AnalyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return []*Finding{}, "", nil
	}

	findings := make([]*Finding, 0, len(b.findings[analyzerName]))

	for _, f := range b.findings[analyzerName] {
		findings = append(findings, copyFinding(f))
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})

	start := 0

	if nextToken != "" {
		for i, f := range findings {
			if f.ID == nextToken {
				start = i

				break
			}
		}
	}

	findings = findings[start:]

	if maxResults > 0 && len(findings) > maxResults {
		return findings[:maxResults], findings[maxResults].ID, nil
	}

	return findings, "", nil
}

// GetAnalyzedResource returns an analyzed resource for an analyzer by resource ARN.
func (b *InMemoryBackend) GetAnalyzedResource(analyzerArn, resourceArn string) (*AnalyzedResource, error) {
	b.mu.RLock("GetAnalyzedResource")
	defer b.mu.RUnlock()

	key := analyzerArn + "|" + resourceArn

	ar, ok := b.analyzedResources[key]
	if !ok {
		return nil, ErrAnalyzedResourceNotFound
	}

	cp := *ar

	return &cp, nil
}

// ListAnalyzedResources returns analyzed resources for an analyzer, optionally filtered by type.
func (b *InMemoryBackend) ListAnalyzedResources(
	analyzerArn, resourceType string,
	maxResults int,
	nextToken string,
) ([]*AnalyzedResource, string, error) {
	b.mu.RLock("ListAnalyzedResources")
	defer b.mu.RUnlock()

	result := make([]*AnalyzedResource, 0)

	for _, ar := range b.analyzedResources {
		if ar.AnalyzerArn != analyzerArn {
			continue
		}

		if resourceType != "" && ar.ResourceType != resourceType {
			continue
		}

		cp := *ar
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ResourceArn < result[j].ResourceArn
	})

	start := 0

	if nextToken != "" {
		for i, ar := range result {
			if ar.ResourceArn == nextToken {
				start = i

				break
			}
		}
	}

	result = result[start:]

	if maxResults > 0 && len(result) > maxResults {
		return result[:maxResults], result[maxResults].ResourceArn, nil
	}

	return result, "", nil
}

// AddAnalyzedResource adds a synthetic analyzed resource (for testing).
func (b *InMemoryBackend) AddAnalyzedResource(
	analyzerArn, resourceArn, resourceType string,
	isPublic bool,
) (*AnalyzedResource, error) {
	b.mu.Lock("AddAnalyzedResource")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	ar := &AnalyzedResource{
		ResourceArn:  resourceArn,
		ResourceType: resourceType,
		AnalyzerArn:  analyzerArn,
		IsPublic:     isPublic,
		CreatedAt:    now,
		AnalyzedAt:   now,
		UpdatedAt:    now,
	}

	key := analyzerArn + "|" + resourceArn
	b.analyzedResources[key] = ar

	cp := *ar

	return &cp, nil
}

// GetFindingV2 returns a finding in V2 format (same data, different shape).
func (b *InMemoryBackend) GetFindingV2(analyzerArn, findingID string) (*Finding, error) {
	b.mu.RLock("GetFindingV2")
	defer b.mu.RUnlock()

	var analyzerName string

	for _, a := range b.analyzers {
		if a.Arn == analyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return nil, ErrAnalyzerNotFound
	}

	f, ok := b.findings[analyzerName][findingID]
	if !ok {
		return nil, ErrFindingNotFound
	}

	return copyFinding(f), nil
}

// ListFindingsV2 returns findings in V2 format for an analyzer identified by ARN.
func (b *InMemoryBackend) ListFindingsV2(
	analyzerArn, status string,
	maxResults int,
	nextToken string,
) ([]*Finding, string, error) {
	b.mu.RLock("ListFindingsV2")
	defer b.mu.RUnlock()

	var analyzerName string

	for _, a := range b.analyzers {
		if a.Arn == analyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return nil, "", ErrAnalyzerNotFound
	}

	findings := make([]*Finding, 0, len(b.findings[analyzerName]))

	for _, f := range b.findings[analyzerName] {
		if status != "" && string(f.Status) != status {
			continue
		}

		findings = append(findings, copyFinding(f))
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})

	start := 0

	if nextToken != "" {
		for i, f := range findings {
			if f.ID == nextToken {
				start = i

				break
			}
		}
	}

	findings = findings[start:]

	if maxResults > 0 && len(findings) > maxResults {
		return findings[:maxResults], findings[maxResults].ID, nil
	}

	return findings, "", nil
}

// GetFindingsStatistics returns counts of findings by status for an analyzer.
func (b *InMemoryBackend) GetFindingsStatistics(analyzerArn string) (map[string]int, error) {
	b.mu.RLock("GetFindingsStatistics")
	defer b.mu.RUnlock()

	var analyzerName string

	for _, a := range b.analyzers {
		if a.Arn == analyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return nil, ErrAnalyzerNotFound
	}

	counts := map[string]int{
		string(FindingStatusActive):   0,
		string(FindingStatusArchived): 0,
		string(FindingStatusResolved): 0,
	}

	for _, f := range b.findings[analyzerName] {
		counts[string(f.Status)]++
	}

	return counts, nil
}

// GenerateFindingRecommendation records a recommendation request for a finding.
func (b *InMemoryBackend) GenerateFindingRecommendation(analyzerArn, findingID string) error {
	b.mu.Lock("GenerateFindingRecommendation")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	completed := now

	rec := &FindingRecommendation{
		ID:                 findingID,
		AnalyzerArn:        analyzerArn,
		RecommendationType: "UNUSED_PERMISSION",
		Status:             "SUCCEEDED",
		StartedAt:          now,
		CompletedAt:        &completed,
	}

	b.findingRecommendations[findingID] = rec

	return nil
}

// GetFindingRecommendation returns recommendations for a finding.
func (b *InMemoryBackend) GetFindingRecommendation(
	analyzerArn, findingID string,
) (*FindingRecommendation, error) {
	b.mu.RLock("GetFindingRecommendation")
	defer b.mu.RUnlock()

	rec, ok := b.findingRecommendations[findingID]
	if !ok {
		return nil, ErrFindingNotFound
	}

	if rec.AnalyzerArn != analyzerArn {
		return nil, ErrFindingNotFound
	}

	cp := *rec

	return &cp, nil
}

// CreateServiceLinkedAnalyzer creates an analyzer with a generated service-linked name.
func (b *InMemoryBackend) CreateServiceLinkedAnalyzer(analyzerType AnalyzerType) (*Analyzer, error) {
	name := "_AccessAnalyzerForInternalUse-" + uuid.NewString()[:8]

	return b.CreateAnalyzer(name, analyzerType, nil)
}

// UpdateAnalyzer updates an analyzer (currently a no-op — configuration not stored).
func (b *InMemoryBackend) UpdateAnalyzer(name string) (*Analyzer, error) {
	b.mu.RLock("UpdateAnalyzer")
	defer b.mu.RUnlock()

	a, ok := b.analyzers[name]
	if !ok {
		return nil, ErrAnalyzerNotFound
	}

	return copyAnalyzer(a), nil
}

// ---- copy helpers ----

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
