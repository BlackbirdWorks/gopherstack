package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// StartADAssessment starts an AD assessment.
func (b *InMemoryBackend) StartADAssessment(ctx context.Context, directoryID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartADAssessment")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("a-%s", uuid.NewString()[:10])
	b.adAssessmentPut(&storedADAssessment{
		AssessmentID: id,
		DirectoryID:  directoryID,
		Status:       "Completed",
		// AWS's real Assessment/AssessmentSummary.ReportType is "CUSTOMER" or
		// "SYSTEM": CUSTOMER means the assessment was started directly via
		// StartADAssessment (as every assessment in this backend is, since
		// CreateHybridAD's SYSTEM-triggered assessment flow is not modeled
		// -- see PARITY.md), so CUSTOMER is the real value here, not a
		// placeholder.
		AssessType: "CUSTOMER",
		Region:     region,
		StartTime:  time.Now().UTC(),
	})

	return id, nil
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
