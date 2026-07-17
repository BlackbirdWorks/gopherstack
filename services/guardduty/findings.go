package guardduty

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GetFindings returns findings by IDs.
func (b *InMemoryBackend) GetFindings(detectorID string, findingIDs []string) ([]*Finding, error) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	results := make([]*Finding, 0, len(findingIDs))

	for _, id := range findingIDs {
		f, ok := b.findings.Get(detectorKey(detectorID, id))
		if !ok {
			return nil, ErrFindingNotFound
		}

		results = append(results, f)
	}

	return results, nil
}

// ListFindings returns finding IDs for a detector.
func (b *InMemoryBackend) ListFindings(detectorID string) ([]string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.findingsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, f := range items {
		ids[i] = f.ID
	}

	slices.Sort(ids)

	return ids, nil
}

// ArchiveFindings marks findings as archived.
func (b *InMemoryBackend) ArchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("ArchiveFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.Archived = true
			f.UpdatedAt = now
		}
	}

	return nil
}

// UnarchiveFindings marks findings as unarchived.
func (b *InMemoryBackend) UnarchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("UnarchiveFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.Archived = false
			f.UpdatedAt = now
		}
	}

	return nil
}

// CreateSampleFindings creates sample findings for a detector.
func (b *InMemoryBackend) CreateSampleFindings(detectorID string, findingTypes []string) error {
	b.mu.Lock("CreateSampleFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if len(findingTypes) == 0 {
		findingTypes = []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"}
	}

	for _, ft := range findingTypes {
		id := strings.ReplaceAll(uuid.New().String(), "-", "")
		b.findings.Put(&Finding{
			AccountID:     b.accountID,
			Arn:           b.findingARN(detectorID, id),
			CreatedAt:     now,
			Description:   "Sample finding: " + ft,
			DetectorID:    detectorID,
			ID:            id,
			Region:        b.region,
			Severity:      defaultFindingSeverity,
			Title:         "Sample: " + ft,
			Type:          ft,
			UpdatedAt:     now,
			SchemaVersion: "2.0",
			Service: FindingService{
				Archived:       false,
				Count:          1,
				DetectorID:     detectorID,
				EventFirstSeen: now,
				EventLastSeen:  now,
				ResourceRole:   "TARGET",
				ServiceName:    "guardduty",
			},
			Resource: FindingResource{
				ResourceType: "AccessKey",
			},
		})
	}

	return nil
}

// GetFindingsStatistics returns finding statistics for a detector.
func (b *InMemoryBackend) GetFindingsStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetFindingsStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	countBySeverity := map[string]int{}

	for _, f := range b.findingsByDetector.Get(detectorID) {
		key := fmt.Sprintf("%.1f", f.Severity)
		countBySeverity[key]++
	}

	return map[string]any{
		"findingStatistics": map[string]any{
			"countBySeverity": countBySeverity,
		},
	}, nil
}

// UpdateFindingsFeedback updates the feedback for findings.
func (b *InMemoryBackend) UpdateFindingsFeedback(detectorID string, findingIDs []string, feedback string) error {
	b.mu.Lock("UpdateFindingsFeedback")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.UserFeedback = feedback
			f.UpdatedAt = now
		}
	}

	return nil
}
