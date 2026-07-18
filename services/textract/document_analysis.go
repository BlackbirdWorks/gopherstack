package textract

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// AnalyzeDocument performs a synchronous document analysis and returns blocks
// based on the requested feature types.
func (b *InMemoryBackend) AnalyzeDocument(_ context.Context, documentURI string) []Block {
	return syntheticBlocks(documentURI)
}

// AnalyzeDocumentWithFeatures performs synchronous document analysis using feature types.
func (b *InMemoryBackend) AnalyzeDocumentWithFeatures(
	_ context.Context,
	documentURI string,
	featureTypes []string,
	queries *QueriesConfig,
) []Block {
	return analyzeDocumentBlocks(documentURI, featureTypes, queries)
}

// StartDocumentAnalysis creates an async document analysis job.
func (b *InMemoryBackend) StartDocumentAnalysis(ctx context.Context, documentURI string) (*DocumentJob, error) {
	return b.StartDocumentAnalysisWithOptions(ctx, documentURI, nil, nil, nil, "", "")
}

// StartDocumentAnalysisWithOptions creates an async document analysis job with full options.
func (b *InMemoryBackend) StartDocumentAnalysisWithOptions(
	ctx context.Context,
	documentURI string,
	featureTypes []string,
	queries *QueriesConfig,
	outputConfig *OutputConfig,
	jobTag, clientRequestToken string,
) (*DocumentJob, error) {
	region := getRegion(ctx, b.region)

	var result *DocumentJob
	var done bool
	var key string

	func() {
		b.mu.Lock("StartDocumentAnalysis")
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
		blocks := analyzeDocumentBlocks(documentURI, featureTypes, queries)
		job := &DocumentJob{
			Region:             region,
			JobID:              jobID,
			JobStatus:          jobStatusInProgress,
			JobType:            jobTypeDocumentAnalysis,
			CreationTime:       time.Now(),
			Blocks:             blocks,
			OutputConfig:       outputConfig,
			JobTag:             jobTag,
			ClientRequestToken: clientRequestToken,
		}
		b.jobs.Put(job)
		trimJobsIfNeeded(b.jobs, b.jobsByRegion, region, b.maxJobs)

		if clientRequestToken != "" {
			b.clientTokenToJobIDStore(region)[clientRequestToken] = jobID
		}

		if b.asyncJobDelay == 0 {
			// Synchronous mode (tests): complete immediately under the same lock.
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

	// Transition to SUCCEEDED after a short delay.
	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("StartDocumentAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.jobs.Get(key); ok {
			j.JobStatus = jobStatusSucceeded
		}
	})

	func() {
		b.mu.RLock("StartDocumentAnalysis-read")
		defer b.mu.RUnlock()

		stored, _ := b.jobs.Get(key)
		result = cloneJob(stored)
	}()

	return result, nil
}

// GetDocumentAnalysis retrieves the results of a document analysis job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetDocumentAnalysis(ctx context.Context, jobID string) (*DocumentJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetDocumentAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.jobs.Get(regionKey(region, jobID))
	if !ok || job.JobType != jobTypeDocumentAnalysis {
		return nil, fmt.Errorf("%w: job %s not found", ErrJobNotFound, jobID)
	}

	return cloneJob(job), nil
}

// cloneJob returns a deep copy of a DocumentJob.
func cloneJob(j *DocumentJob) *DocumentJob {
	cp := *j
	cp.Blocks = make([]Block, len(j.Blocks))
	copy(cp.Blocks, j.Blocks)

	if j.Warnings != nil {
		cp.Warnings = make([]WarningBlock, len(j.Warnings))
		copy(cp.Warnings, j.Warnings)
	}

	return &cp
}

// trimJobsIfNeeded removes the oldest jobs in region once the per-region job
// count exceeds maxJobs. Caller must hold the write lock. byRegion.Get
// returns an index-owned slice that Table.Delete mutates in place, so it is
// cloned before the delete loop below (see pkgs/store's Index.Get doc).
func trimJobsIfNeeded(t *store.Table[DocumentJob], byRegion *store.Index[DocumentJob], region string, maxJobs int) {
	entries := slices.Clone(byRegion.Get(region))
	if len(entries) <= maxJobs {
		return
	}

	sort.Slice(entries, func(i, k int) bool {
		return entries[i].CreationTime.Before(entries[k].CreationTime)
	})

	excess := len(entries) - maxJobs
	for i := range excess {
		t.Delete(jobKey(entries[i]))
	}
}
