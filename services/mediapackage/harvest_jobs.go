package mediapackage

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateHarvestJob creates a new harvest job record.
func (b *InMemoryBackend) CreateHarvestJob(
	id, originEndpointID, startTime, endTime string,
	s3Dest S3Destination,
) (*HarvestJob, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	if originEndpointID == "" {
		return nil, fmt.Errorf("%w: OriginEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHarvestJob")
	defer b.mu.Unlock()

	if b.harvestJobs.Has(id) {
		return nil, fmt.Errorf("%w: harvest job %q already exists", ErrConflict, id)
	}

	ep, ok := b.originEndpoints.Get(originEndpointID)
	if !ok {
		return nil, fmt.Errorf("%w: origin endpoint %q not found", ErrNotFound, originEndpointID)
	}

	job := &storedHarvestJob{
		ARN:              b.buildHarvestJobARN(id),
		ChannelID:        ep.ChannelID,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		EndTime:          endTime,
		ID:               id,
		OriginEndpointID: originEndpointID,
		S3Destination: &storedS3Destination{
			BucketName:  s3Dest.BucketName,
			ManifestKey: s3Dest.ManifestKey,
			RoleArn:     s3Dest.RoleArn,
		},
		StartTime: startTime,
		Status:    harvestJobStatusSucceeded,
	}

	b.harvestJobs.Put(job)

	return job.toHarvestJob(), nil
}

// DescribeHarvestJob returns a harvest job by ID.
func (b *InMemoryBackend) DescribeHarvestJob(id string) (*HarvestJob, error) {
	b.mu.RLock("DescribeHarvestJob")
	defer b.mu.RUnlock()

	job, ok := b.harvestJobs.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: harvest job %q not found", ErrNotFound, id)
	}

	return job.toHarvestJob(), nil
}

// ListHarvestJobs returns a paginated list of harvest jobs, optionally filtered by channel or status.
func (b *InMemoryBackend) ListHarvestJobs(
	includeChannelID, includeStatus string,
	maxResults int,
	nextToken string,
) ([]*HarvestJob, string, error) {
	b.mu.RLock("ListHarvestJobs")
	defer b.mu.RUnlock()

	snap := b.harvestJobs.Snapshot()

	all := make([]*storedHarvestJob, 0, len(snap))
	for _, job := range snap {
		if includeChannelID != "" && job.ChannelID != includeChannelID {
			continue
		}

		if includeStatus != "" && job.Status != includeStatus {
			continue
		}

		all = append(all, job)
	}

	p := page.New(all, nextToken, maxResults, defaultMaxResults)

	jobs := make([]*HarvestJob, 0, len(p.Data))
	for _, job := range p.Data {
		jobs = append(jobs, job.toHarvestJob())
	}

	return jobs, p.Next, nil
}
