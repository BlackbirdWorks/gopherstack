package rekognition

import (
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	maxAsyncJobs         = 10_000
	maxMediaAnalysisJobs = 10_000

	// jobStatusSucceeded is the terminal success status shared by async video
	// jobs, media analysis jobs, and face liveness sessions.
	jobStatusSucceeded = "SUCCEEDED"

	// asyncJobSettledPolls is the PollCount past which GetAsyncJob no longer advances a job.
	asyncJobSettledPolls = 2
)

// =============================================================================
// Async Jobs
// =============================================================================

// evictOneIfAtCapacity deletes an arbitrary entry from t when it is already
// at (or over) max, mirroring the original map's "evict a random entry"
// eviction (Go map iteration order is unspecified, matching store.Table's
// own unspecified Range order).
func evictOneIfAtCapacity[V any](t *store.Table[V], maxLen int, keyFn func(*V) string) {
	if t.Len() < maxLen {
		return
	}

	t.Range(func(v *V) bool {
		t.Delete(keyFn(v))

		return false
	})
}

// StartAsyncJob creates a new async video analysis job.
func (b *InMemoryBackend) StartAsyncJob(params StartAsyncJobParams) (string, error) {
	jobID, started, err := b.startAsyncJob(params)
	if err != nil {
		return "", err
	}

	if started && params.NotificationTopicARN != "" {
		b.publishJobNotification(buildJobNotification(jobID, params))
	}

	return jobID, nil
}

// startAsyncJob reports whether a new job was created (not an idempotent replay).
func (b *InMemoryBackend) startAsyncJob(params StartAsyncJobParams) (string, bool, error) {
	b.mu.Lock("StartAsyncJob")
	defer b.mu.Unlock()

	if params.ClientRequestToken != "" {
		var prior *storedAsyncJob

		b.asyncJobs.Range(func(j *storedAsyncJob) bool {
			if j.ClientRequestToken == params.ClientRequestToken && j.JobType == params.JobType {
				prior = j

				return false
			}

			return true
		})

		if prior != nil {
			if !prior.sameStartParams(params) {
				return "", false, ErrIdempotentParameterMismatch
			}

			return prior.JobID, false, nil
		}
	}

	evictOneIfAtCapacity(b.asyncJobs, maxAsyncJobs, asyncJobKeyFn)

	jobID := uuid.NewString()
	status, polls := "IN_PROGRESS", 0

	if params.NotificationTopicARN != "" {
		status, polls = jobStatusSucceeded, asyncJobSettledPolls
	}

	b.asyncJobs.Put(&storedAsyncJob{
		JobID:              jobID,
		ClientRequestToken: params.ClientRequestToken,
		JobType:            params.JobType,
		CollectionID:       params.CollectionID,
		JobStatus:          status,
		PollCount:          polls,
		JobTag:             params.JobTag,
		VideoS3Bucket:      params.VideoS3Bucket,
		VideoS3Name:        params.VideoS3Name,
		VideoS3Version:     params.VideoS3Version,
		SegmentTypes:       params.SegmentTypes,
	})

	return jobID, true, nil
}

// GetAsyncJob returns an async job by ID, simulating state progression on each poll.
func (b *InMemoryBackend) GetAsyncJob(jobID string) (*AsyncJob, error) {
	b.mu.Lock("GetAsyncJob")
	defer b.mu.Unlock()

	job, exists := b.asyncJobs.Get(jobID)
	if !exists {
		return nil, ErrAsyncJobNotFound
	}

	switch job.PollCount {
	case 0:
		job.PollCount++
	case 1:
		job.PollCount++
		job.JobStatus = jobStatusSucceeded
	}

	return &AsyncJob{
		JobID:          job.JobID,
		JobStatus:      job.JobStatus,
		JobTag:         job.JobTag,
		VideoS3Bucket:  job.VideoS3Bucket,
		VideoS3Name:    job.VideoS3Name,
		VideoS3Version: job.VideoS3Version,
		SegmentTypes:   job.SegmentTypes,
	}, nil
}

// =============================================================================
// MediaAnalysis Jobs
// =============================================================================

// StartMediaAnalysisJob creates a new media analysis job.
func (b *InMemoryBackend) StartMediaAnalysisJob(jobName string, params StartMediaAnalysisJobParams) (string, error) {
	b.mu.Lock("StartMediaAnalysisJob")
	defer b.mu.Unlock()

	if params.ClientRequestToken != "" {
		var prior *storedMediaAnalysisJob

		b.mediaAnalysisJobs.Range(func(j *storedMediaAnalysisJob) bool {
			if j.ClientRequestToken == params.ClientRequestToken {
				prior = j

				return false
			}

			return true
		})

		if prior != nil {
			if prior.JobName != jobName || !prior.sameStartParams(params) {
				return "", ErrIdempotentParameterMismatch
			}

			return prior.JobID, nil
		}
	}

	evictOneIfAtCapacity(b.mediaAnalysisJobs, maxMediaAnalysisJobs, mediaAnalysisJobKeyFn)

	jobID := uuid.NewString()
	b.mediaAnalysisJobs.Put(&storedMediaAnalysisJob{
		ClientRequestToken:                   params.ClientRequestToken,
		KmsKeyID:                             params.KmsKeyID,
		CreationTimestamp:                    time.Now(),
		JobID:                                jobID,
		JobName:                              jobName,
		Status:                               jobStatusSucceeded,
		InputS3Bucket:                        params.InputS3Bucket,
		InputS3Name:                          params.InputS3Name,
		InputS3Version:                       params.InputS3Version,
		OutputConfigS3Bucket:                 params.OutputConfigS3Bucket,
		OutputConfigS3KeyPrefix:              params.OutputConfigS3KeyPrefix,
		DetectModerationLabelsProjectVersion: params.DetectModerationLabelsProjectVersion,
		DetectModerationLabelsMinConfidence:  params.DetectModerationLabelsMinConfidence,
		HasDetectModerationLabels:            params.HasDetectModerationLabels,
	})

	return jobID, nil
}

// GetMediaAnalysisJob returns a media analysis job by ID.
func (b *InMemoryBackend) GetMediaAnalysisJob(jobID string) (*MediaAnalysisJob, error) {
	b.mu.RLock("GetMediaAnalysisJob")
	defer b.mu.RUnlock()

	job, exists := b.mediaAnalysisJobs.Get(jobID)
	if !exists {
		return nil, ErrMediaAnalysisJobNotFound
	}

	return job.toMediaAnalysisJob(), nil
}

// ListMediaAnalysisJobs returns a paginated list of media analysis jobs.
func (b *InMemoryBackend) ListMediaAnalysisJobs(
	maxResults int32, nextToken string,
) ([]*MediaAnalysisJob, string, error) {
	b.mu.RLock("ListMediaAnalysisJobs")
	defer b.mu.RUnlock()

	const maxPerPage = 100

	result, outToken := paginateTable(
		b.mediaAnalysisJobs, maxResults, maxPerPage, nextToken,
		mediaAnalysisJobKeyFn, (*storedMediaAnalysisJob).toMediaAnalysisJob,
	)

	return result, outToken, nil
}
