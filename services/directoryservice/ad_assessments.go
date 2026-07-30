package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// assessmentStatusSuccess is the real types.Assessment.Status value ("Valid
// values include SUCCESS, FAILED, PENDING, and IN_PROGRESS") this backend
// produces: every assessment here completes synchronously and successfully,
// since there is no real environment-compatibility check to fail. Previously
// this backend used the non-enum value "Completed" here -- a genuine
// wire-value bug (not just a missing field), found and fixed as part of
// gopherstack-10hx since CreateHybridAD needs a real success status to gate
// on.
const assessmentStatusSuccess = "SUCCESS"

// StartADAssessment starts an AD assessment of an existing directory (the
// only mode this backend supports -- StartADAssessmentInput.AssessmentConfiguration,
// the directory-less pre-creation mode real CreateHybridAD callers normally
// use, is not accepted; see PARITY.md). Callers must hold b.mu (write lock).
func (b *InMemoryBackend) startADAssessmentLocked(region, directoryID string) (string, error) {
	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("a-%s", uuid.NewString()[:10])
	b.adAssessmentPut(&storedADAssessment{
		AssessmentID: id,
		DirectoryID:  directoryID,
		Status:       assessmentStatusSuccess,
		// AWS's real Assessment/AssessmentSummary.ReportType is "CUSTOMER" or
		// "SYSTEM": CUSTOMER means the assessment was started directly via
		// StartADAssessment (as every assessment in this backend is, since
		// CreateHybridAD's SYSTEM-triggered assessment flow is not modeled
		// -- see PARITY.md), so CUSTOMER is the real value here, not a
		// placeholder.
		AssessType: "CUSTOMER",
		Region:     region,
		StartTime:  time.Now().UTC(),
		// Snapshotted so a later CreateHybridAD can source real (not
		// fabricated) descriptive fields for the hybrid directory it derives
		// from this assessment -- see the storedADAssessment doc comment.
		SourceDirectoryName:        d.Name,
		SourceDirectoryShortName:   d.ShortName,
		SourceDirectoryDescription: d.Description,
		SourceDirectoryEdition:     d.Edition,
	})

	return id, nil
}

// StartADAssessment starts an AD assessment.
func (b *InMemoryBackend) StartADAssessment(ctx context.Context, directoryID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartADAssessment")
	defer b.mu.Unlock()

	return b.startADAssessmentLocked(region, directoryID)
}

// DeleteADAssessment deletes an AD assessment.
func (b *InMemoryBackend) DeleteADAssessment(ctx context.Context, directoryID, assessmentID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteADAssessment")
	defer b.mu.Unlock()

	a, ok := b.adAssessmentGet(region, assessmentID)
	if !ok || a.DirectoryID != directoryID {
		return ErrAssessmentNotFound
	}

	b.adAssessmentDelete(region, assessmentID)

	return nil
}

// DescribeADAssessment returns details of an AD assessment.
func (b *InMemoryBackend) DescribeADAssessment(
	ctx context.Context,
	directoryID, assessmentID string,
) (*ADAssessmentInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeADAssessment")
	defer b.mu.RUnlock()

	a, ok := b.adAssessmentGet(region, assessmentID)
	if !ok || a.DirectoryID != directoryID {
		return nil, ErrAssessmentNotFound
	}

	return &ADAssessmentInfo{
		AssessmentID: a.AssessmentID,
		DirectoryID:  a.DirectoryID,
		Status:       a.Status,
		AssessType:   a.AssessType,
		Region:       a.Region,
		StartTime:    a.StartTime,
	}, nil
}

// ListADAssessments returns AD assessments for a directory.
func (b *InMemoryBackend) ListADAssessments(
	ctx context.Context,
	directoryID string,
	limit int32,
	nextToken string,
) ([]ADAssessmentInfo, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListADAssessments")
	defer b.mu.RUnlock()

	var ids []string
	for _, a := range b.adAssessmentsInRegion(region) {
		if directoryID != "" && a.DirectoryID != directoryID {
			continue
		}
		ids = append(ids, a.AssessmentID)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]ADAssessmentInfo, 0, end-start)
	for _, id := range ids[start:end] {
		a, _ := b.adAssessmentGet(region, id)
		result = append(result, ADAssessmentInfo{
			AssessmentID: a.AssessmentID,
			DirectoryID:  a.DirectoryID,
			Status:       a.Status,
			AssessType:   a.AssessType,
			Region:       a.Region,
			StartTime:    a.StartTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}
