package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// CancelReplicationTaskAssessmentRun cancels a single premigration assessment run.
func (b *InMemoryBackend) CancelReplicationTaskAssessmentRun(
	ctx context.Context,
	replicationTaskAssessmentRunArn string,
) error {
	if replicationTaskAssessmentRunArn == "" {
		return fmt.Errorf("%w: ReplicationTaskAssessmentRunArn is required", ErrValidation)
	}

	b.mu.Lock("CancelReplicationTaskAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	run, ok := b.assessmentRuns.Get(regionKey(region, replicationTaskAssessmentRunArn))
	if !ok {
		return fmt.Errorf(
			"%w: assessment run %s not found",
			ErrNotFound,
			replicationTaskAssessmentRunArn,
		)
	}

	run.Status = statusCancelling

	return nil
}

// StartAssessmentRun creates and stores a new premigration assessment run.
func (b *InMemoryBackend) StartAssessmentRun(
	ctx context.Context,
	taskArn, _, _, assessmentRunName string,
) (*AssessmentRun, error) {
	b.mu.Lock("StartAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, taskArn)); !ok {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArn)
	}

	runARN := arn.Build("dms", region, b.accountID, "assessment-run:"+uuid.NewString())
	run := &AssessmentRun{
		ReplicationTaskAssessmentRunArn: runARN,
		ReplicationTaskArn:              taskArn,
		AssessmentRunName:               assessmentRunName,
		Status:                          statusRunning,
		Region:                          region,
	}
	b.assessmentRuns.Put(run)
	cp := *run

	return &cp, nil
}

// DeleteAssessmentRun removes a stored assessment run.
func (b *InMemoryBackend) DeleteAssessmentRun(ctx context.Context, runArn string) (*AssessmentRun, error) {
	b.mu.Lock("DeleteAssessmentRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	run, ok := b.assessmentRuns.Get(regionKey(region, runArn))
	if !ok {
		return nil, fmt.Errorf("%w: assessment run %s not found", ErrNotFound, runArn)
	}

	cp := *run
	b.assessmentRuns.Delete(regionKey(region, runArn))

	return &cp, nil
}

// DescribeAssessmentRuns returns stored assessment runs, optionally filtered by task ARN.
func (b *InMemoryBackend) DescribeAssessmentRuns(ctx context.Context, taskArn string) ([]*AssessmentRun, error) {
	b.mu.RLock("DescribeAssessmentRuns")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.assessmentRunsByRegion.Get(region)
	list := make([]*AssessmentRun, 0, len(items))

	for _, run := range items {
		if taskArn != "" && run.ReplicationTaskArn != taskArn {
			continue
		}

		cp := *run
		list = append(list, &cp)
	}

	return list, nil
}
