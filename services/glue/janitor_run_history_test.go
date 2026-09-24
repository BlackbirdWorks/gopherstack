package glue_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestPruneOldJobRuns locks in the fix for the unbounded-memory-growth leak in
// b.jobRuns: every StartJobRun call appended to the slice forever, with no
// eviction. AWS Glue documents job run history as retained for 90 days;
// pruneOldJobRunsLocked now evicts terminal runs older than that on the next
// StartJobRun call for the same job.
func TestPruneOldJobRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		age         time.Duration
		wantEvicted bool
	}{
		{name: "evicted_past_90_days", age: 91 * 24 * time.Hour, wantEvicted: true},
		{name: "kept_within_90_days", age: 89 * 24 * time.Hour, wantEvicted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := glue.NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Close()

				const jobName = "history-prune-job"

				_, err := b.CreateJob(glue.Job{
					Name:    jobName,
					Role:    "arn:aws:iam::000000000000:role/glue",
					Command: glue.JobCommand{Name: "glueetl"},
				})
				require.NoError(t, err)

				b.AddJobRunInternal(&glue.JobRun{
					ID:          "jr-old",
					JobName:     jobName,
					JobRunState: "SUCCEEDED",
					CompletedOn: float64(time.Now().Add(-tt.age).Unix()),
				})

				runs, err := b.GetJobRuns(jobName)
				require.NoError(t, err)
				require.Len(t, runs, 1, "old run must be present before the next StartJobRun")

				_, err = b.StartJobRun(jobName, nil)
				require.NoError(t, err)

				runs, err = b.GetJobRuns(jobName)
				require.NoError(t, err)

				if tt.wantEvicted {
					assert.Len(t, runs, 1, "stale run must be pruned, leaving only the new one")
					assert.NotEqual(t, "jr-old", runs[0].ID)
				} else {
					assert.Len(t, runs, 2, "run within retention must be kept alongside the new one")
				}
			})
		})
	}
}

// TestPruneOldCrawlHistory locks in the equivalent fix for b.crawlHistory:
// StartCrawler appended a new entry every time with no eviction. AWS Glue
// documents crawler run history retention at 90 days; pruneOldCrawlHistoryLocked
// now evicts finished entries older than that on the next StartCrawler call.
func TestPruneOldCrawlHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sleep       time.Duration
		wantHistLen int
	}{
		{name: "evicted_past_90_days", sleep: 91 * 24 * time.Hour, wantHistLen: 1},
		{name: "kept_within_90_days", sleep: 89 * 24 * time.Hour, wantHistLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := glue.NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Close()

				const name = "history-prune-crawler"

				_, err := b.CreateCrawler(name, "arn:aws:iam::000000000000:role/glue", "", glue.CrawlerTarget{}, nil)
				require.NoError(t, err)

				require.NoError(t, b.StartCrawler(name))
				time.Sleep(300 * time.Millisecond)
				_, err = b.GetCrawler(name) // lazily advances RUNNING -> READY, closing crawl #1
				require.NoError(t, err)

				hist, err := b.ListCrawls(name)
				require.NoError(t, err)
				require.Len(t, hist, 1, "first crawl must be recorded before the second run")

				time.Sleep(tt.sleep)

				require.NoError(t, b.StartCrawler(name)) // triggers pruneOldCrawlHistoryLocked
				time.Sleep(300 * time.Millisecond)
				_, err = b.GetCrawler(name)
				require.NoError(t, err)

				hist, err = b.ListCrawls(name)
				require.NoError(t, err)
				assert.Len(t, hist, tt.wantHistLen)
			})
		})
	}
}
