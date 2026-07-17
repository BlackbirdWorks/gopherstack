package batch

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// SubmitServiceJob creates a new service job in SUBMITTED status.
func (b *InMemoryBackend) SubmitServiceJob(
	ctx context.Context,
	name, serviceEnv string,
	tags map[string]string,
) (*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("SubmitServiceJob")
	defer b.mu.Unlock()

	tagsCopy := tagsCloneOrEmpty(tags)
	now := time.Now().UnixMilli()
	jobID := uuid.NewString()
	jobARN := arn.Build("batch", region, b.accountID, "service-job/"+jobID)

	sj := &ServiceJob{
		region:             region,
		ServiceJobID:       jobID,
		ServiceJobArn:      jobARN,
		ServiceJobName:     name,
		ServiceEnvironment: serviceEnv,
		Status:             jobStatusSubmitted,
		CreatedAt:          now,
		Tags:               tagsCopy,
	}
	b.serviceJobs.Put(sj)
	cp := *sj

	return &cp, nil
}

// DescribeServiceJob returns a single service job by ID.
func (b *InMemoryBackend) DescribeServiceJob(ctx context.Context, serviceJobID string) (*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeServiceJob")
	defer b.mu.RUnlock()

	sj, ok := b.serviceJobs.Get(regionKey(region, serviceJobID))
	if !ok {
		return nil, fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	cp := *sj
	cp.Tags = tagsCloneOrEmpty(sj.Tags)

	return &cp, nil
}

// ListServiceJobs returns service jobs, optionally filtered by service environment.
func (b *InMemoryBackend) ListServiceJobs(ctx context.Context, serviceEnv string) ([]*ServiceJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListServiceJobs")
	defer b.mu.RUnlock()

	group := b.serviceJobsByRegion.Get(region)
	list := make([]*ServiceJob, 0, len(group))

	for _, sj := range group {
		if serviceEnv != "" && sj.ServiceEnvironment != serviceEnv {
			continue
		}
		cp := *sj
		cp.Tags = tagsCloneOrEmpty(sj.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	return list, nil
}

// TerminateServiceJob marks a service job as FAILED.
func (b *InMemoryBackend) TerminateServiceJob(ctx context.Context, serviceJobID, reason string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TerminateServiceJob")
	defer b.mu.Unlock()

	sj, ok := b.serviceJobs.Get(regionKey(region, serviceJobID))
	if !ok {
		return fmt.Errorf("%w: service job %s not found", ErrNotFound, serviceJobID)
	}

	now := time.Now().UnixMilli()
	sj.Status = jobStatusFailed
	sj.StatusReason = reason
	sj.StoppedAt = &now

	return nil
}
