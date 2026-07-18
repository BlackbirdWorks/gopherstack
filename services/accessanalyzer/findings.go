package accessanalyzer

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// AddFinding adds a synthetic finding to an analyzer (for testing / resource scan simulation).
func (b *InMemoryBackend) AddFinding(
	analyzerName, resourceType, resourceArn string,
	action []string,
	principal map[string]string,
	isPublic *bool,
) (*Finding, error) {
	b.mu.Lock("AddFinding")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	analyzerARN := b.analyzerARN(analyzerName)
	now := time.Now().UTC()
	f := &Finding{
		ID:           uuid.NewString(),
		AnalyzerArn:  analyzerARN,
		Status:       FindingStatusActive,
		ResourceType: resourceType,
		ResourceArn:  resourceArn,
		Action:       append([]string(nil), action...),
		Principal:    cloneTags(principal),
		IsPublic:     isPublic,
		UpdatedAt:    now,
		CreatedAt:    now,
	}

	b.findings.Put(f)

	return copyFinding(f), nil
}

// GetFinding returns a finding by ID.
func (b *InMemoryBackend) GetFinding(analyzerName, findingID string) (*Finding, error) {
	b.mu.RLock("GetFinding")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	f, exists := b.findings.Get(findingID)
	if !exists || findingAnalyzerIndexKeyFn(f) != analyzerName {
		return nil, ErrFindingNotFound
	}

	return copyFinding(f), nil
}

// ListFindings returns findings for an analyzer, optionally filtered.
func (b *InMemoryBackend) ListFindings(
	analyzerName string,
	_ map[string]FilterCriterion,
	status string,
	maxResults int,
	nextToken string,
) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, "", ErrAnalyzerNotFound
	}

	group := b.findingsByAnalyzer.Get(analyzerName)
	findings := make([]*Finding, 0, len(group))

	for _, f := range group {
		if status != "" && string(f.Status) != status {
			continue
		}

		findings = append(findings, copyFinding(f))
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})

	// Simple token-based pagination by finding ID prefix.
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

// UpdateFindings archives or marks active the specified findings.
func (b *InMemoryBackend) UpdateFindings(
	analyzerName string,
	findingIDs []string,
	status FindingStatus,
) error {
	b.mu.Lock("UpdateFindings")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return ErrAnalyzerNotFound
	}

	now := time.Now().UTC()

	for _, id := range findingIDs {
		if f, exists := b.findings.Get(id); exists && findingAnalyzerIndexKeyFn(f) == analyzerName {
			f.Status = status
			f.UpdatedAt = now
		}
	}

	return nil
}

// GetFindingV2 returns a finding in V2 format (same data, different shape).
func (b *InMemoryBackend) GetFindingV2(analyzerArn, findingID string) (*Finding, error) {
	b.mu.RLock("GetFindingV2")
	defer b.mu.RUnlock()

	var analyzerName string

	for _, a := range b.analyzers.All() {
		if a.Arn == analyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return nil, ErrAnalyzerNotFound
	}

	f, ok := b.findings.Get(findingID)
	if !ok || findingAnalyzerIndexKeyFn(f) != analyzerName {
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

	for _, a := range b.analyzers.All() {
		if a.Arn == analyzerArn {
			analyzerName = a.Name

			break
		}
	}

	if analyzerName == "" {
		return nil, "", ErrAnalyzerNotFound
	}

	group := b.findingsByAnalyzer.Get(analyzerName)
	findings := make([]*Finding, 0, len(group))

	for _, f := range group {
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

	for _, a := range b.analyzers.All() {
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

	for _, f := range b.findingsByAnalyzer.Get(analyzerName) {
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

	b.findingRecommendations.Put(rec)

	return nil
}

// GetFindingRecommendation returns recommendations for a finding.
func (b *InMemoryBackend) GetFindingRecommendation(
	analyzerArn, findingID string,
) (*FindingRecommendation, error) {
	b.mu.RLock("GetFindingRecommendation")
	defer b.mu.RUnlock()

	rec, ok := b.findingRecommendations.Get(findingID)
	if !ok {
		return nil, ErrFindingNotFound
	}

	if rec.AnalyzerArn != analyzerArn {
		return nil, ErrFindingNotFound
	}

	cp := *rec

	return &cp, nil
}
