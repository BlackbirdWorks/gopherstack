package textract

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// DetectDocumentText performs synchronous text detection and returns proper blocks.
func (b *InMemoryBackend) DetectDocumentText(_ context.Context, documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// StartDocumentTextDetection creates an async text detection job.
func (b *InMemoryBackend) StartDocumentTextDetection(ctx context.Context, documentURI string) (*DocumentJob, error) {
	return b.StartDocumentTextDetectionWithOptions(ctx, documentURI, nil, nil, "", "")
}

// StartDocumentTextDetectionWithOptions creates an async text detection job with options.
func (b *InMemoryBackend) StartDocumentTextDetectionWithOptions(
	ctx context.Context,
	documentURI string,
	outputConfig *OutputConfig,
	notificationChannel *NotificationChannel,
	jobTag, clientRequestToken string,
) (*DocumentJob, error) {
	region := getRegion(ctx, b.region)

	var result *DocumentJob
	var done bool
	var key string

	func() {
		b.mu.Lock("StartDocumentTextDetection")
		defer b.mu.Unlock()

		// Idempotency: if token already seen, return existing job.
		if clientRequestToken != "" {
			if existingID, ok := b.clientTokenToJobIDStore(region)[clientRequestToken]; ok {
				if existing, ok2 := b.jobs.Get(regionKey(region, existingID)); ok2 {
					result = cloneJob(existing)
					done = true

					return
				}
			}
		}

		jobID := uuid.NewString()
		job := &DocumentJob{
			Region:              region,
			JobID:               jobID,
			JobStatus:           jobStatusInProgress,
			JobType:             jobTypeTextDetection,
			CreationTime:        time.Now(),
			Blocks:              syntheticBlocks(documentURI),
			OutputConfig:        outputConfig,
			NotificationChannel: notificationChannel,
			JobTag:              jobTag,
			ClientRequestToken:  clientRequestToken,
		}
		b.jobs.Put(job)
		trimJobsIfNeeded(b.jobs, b.jobsByRegion, region, b.maxJobs)

		if clientRequestToken != "" {
			b.clientTokenToJobIDStore(region)[clientRequestToken] = jobID
		}

		if b.asyncJobDelay == 0 {
			job.JobStatus = jobStatusSucceeded
			result = cloneJob(job)
			done = true

			return
		}

		key = jobKey(job)
	}()

	if done {
		return result, nil
	}

	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("StartDocumentTextDetection-complete")
		defer b.mu.Unlock()

		if j, ok := b.jobs.Get(key); ok {
			j.JobStatus = jobStatusSucceeded
		}
	})

	func() {
		b.mu.RLock("StartDocumentTextDetection-read")
		defer b.mu.RUnlock()

		stored, _ := b.jobs.Get(key)
		result = cloneJob(stored)
	}()

	return result, nil
}

// GetDocumentTextDetection retrieves the results of a text detection job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetDocumentTextDetection(ctx context.Context, jobID string) (*DocumentJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetDocumentTextDetection")
	defer b.mu.RUnlock()

	job, ok := b.jobs.Get(regionKey(region, jobID))
	if !ok || job.JobType != jobTypeTextDetection {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
}

// ListJobs returns all stored jobs for the request region, sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs(ctx context.Context) []DocumentJob {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	jobs := b.jobsByRegion.Get(region)
	out := make([]DocumentJob, 0, len(jobs))

	for _, j := range jobs {
		out = append(out, *cloneJob(j))
	}

	// Sort newest first by creation time.
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreationTime.After(out[j].CreationTime)
	})

	return out
}
