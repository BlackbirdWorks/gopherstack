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

// startADAssessmentLocked starts an AD assessment of an existing directory
// (the only mode this backend supports -- CreateHybridAD's AssessmentId must
// trace back to an assessment of an existing directory; see hybrid_ad.go and
// PARITY.md). cfg is the real, optional AssessmentConfiguration request
// member (nil for callers with no configuration to report, e.g.
// UpdateHybridAD's internally-triggered assessment, which has no
// AssessmentConfiguration input in the real API either). Callers must hold
// b.mu (write lock).
func (b *InMemoryBackend) startADAssessmentLocked(
	region, directoryID string,
	cfg *ADAssessmentConfiguration,
) (string, error) {
	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return "", ErrDirectoryNotFound
	}

	now := time.Now().UTC()
	id := fmt.Sprintf("a-%s", uuid.NewString()[:10])
	rec := &storedADAssessment{
		AssessmentID: id,
		DirectoryID:  directoryID,
		Status:       assessmentStatusSuccess,
		// AWS's real Assessment/AssessmentSummary.ReportType is "CUSTOMER" or
		// "SYSTEM": CUSTOMER means the assessment was started directly via
		// StartADAssessment (as every assessment in this backend is, since
		// CreateHybridAD's SYSTEM-triggered assessment flow is not modeled
		// -- see PARITY.md), so CUSTOMER is the real value here, not a
		// placeholder.
		AssessType:         "CUSTOMER",
		Region:             region,
		StartTime:          now,
		LastUpdateDateTime: now,
		// Snapshotted so a later CreateHybridAD can source real (not
		// fabricated) descriptive fields for the hybrid directory it derives
		// from this assessment -- see the storedADAssessment doc comment.
		SourceDirectoryName:        d.Name,
		SourceDirectoryShortName:   d.ShortName,
		SourceDirectoryDescription: d.Description,
		SourceDirectoryEdition:     d.Edition,
	}

	if cfg != nil {
		rec.DNSName = cfg.DNSName
		rec.VPCID = cfg.VPCID
		rec.CustomerDNSIPs = cfg.CustomerDNSIPs
		rec.InstanceIDs = cfg.InstanceIDs
		rec.SecurityGroupIDs = cfg.SecurityGroupIDs
		rec.SubnetIDs = cfg.SubnetIDs
	}

	b.adAssessmentPut(rec)

	return id, nil
}

// StartADAssessment starts an AD assessment. cfg is the real, optional
// StartADAssessmentInput.AssessmentConfiguration member.
func (b *InMemoryBackend) StartADAssessment(
	ctx context.Context,
	directoryID string,
	cfg *ADAssessmentConfiguration,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartADAssessment")
	defer b.mu.Unlock()

	return b.startADAssessmentLocked(region, directoryID, cfg)
}

// DeleteADAssessment deletes an AD assessment. Real DeleteADAssessmentInput
// (directoryservice@v1.41.4 api_op_DeleteADAssessment.go) is {AssessmentId}
// only -- assessment IDs are globally addressable, not directory-scoped.
func (b *InMemoryBackend) DeleteADAssessment(ctx context.Context, assessmentID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteADAssessment")
	defer b.mu.Unlock()

	if _, ok := b.adAssessmentGet(region, assessmentID); !ok {
		return ErrAssessmentNotFound
	}

	b.adAssessmentDelete(region, assessmentID)

	return nil
}

// DescribeADAssessment returns details of an AD assessment. Real
// DescribeADAssessmentInput (api_op_DescribeADAssessment.go) is
// {AssessmentId} only -- same rationale as DeleteADAssessment.
func (b *InMemoryBackend) DescribeADAssessment(
	ctx context.Context,
	assessmentID string,
) (*ADAssessmentInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeADAssessment")
	defer b.mu.RUnlock()

	a, ok := b.adAssessmentGet(region, assessmentID)
	if !ok {
		return nil, ErrAssessmentNotFound
	}

	return &ADAssessmentInfo{
		AssessmentID:       a.AssessmentID,
		DirectoryID:        a.DirectoryID,
		Status:             a.Status,
		AssessType:         a.AssessType,
		Region:             a.Region,
		StartTime:          a.StartTime,
		LastUpdateDateTime: a.LastUpdateDateTime,
		DNSName:            a.DNSName,
		VPCID:              a.VPCID,
		CustomerDNSIPs:     a.CustomerDNSIPs,
		InstanceIDs:        a.InstanceIDs,
		SecurityGroupIDs:   a.SecurityGroupIDs,
		SubnetIDs:          a.SubnetIDs,
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
			AssessmentID:       a.AssessmentID,
			DirectoryID:        a.DirectoryID,
			Status:             a.Status,
			AssessType:         a.AssessType,
			Region:             a.Region,
			StartTime:          a.StartTime,
			LastUpdateDateTime: a.LastUpdateDateTime,
			DNSName:            a.DNSName,
			VPCID:              a.VPCID,
			CustomerDNSIPs:     a.CustomerDNSIPs,
			InstanceIDs:        a.InstanceIDs,
			SecurityGroupIDs:   a.SecurityGroupIDs,
			SubnetIDs:          a.SubnetIDs,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}
