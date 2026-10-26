package glue

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// crawlJob is one in-flight crawl: its snapshot plus the ops it recorded.
type crawlJob struct {
	run *crawlRun
	// name is the crawler this job belongs to.
	name string
}

// startCrawlLocked snapshots c and registers a job to run after b.mu is
// released. b.mu must be held.
func (b *InMemoryBackend) startCrawlLocked(c *Crawler) {
	job := &crawlJob{name: c.Name, run: b.snapshotCrawlLocked(c)}
	b.crawlInFlight[c.Name] = job
	b.pendingCrawls = append(b.pendingCrawls, job)
}

// runCrawlJobs performs the unlocked I/O of each job, then commits its results
// under the lock. Must be called without b.mu held.
func (b *InMemoryBackend) runCrawlJobs(jobs []*crawlJob, now time.Time) {
	for _, job := range jobs {
		err := job.run.crawlAllTargets()

		events := func() []runCompletion {
			b.mu.Lock("finishCrawl")
			defer b.mu.Unlock()

			b.finishCrawlJobLocked(job, err, now)
			b.settleWorkflowRunsLocked(now)

			ev := b.triggerEvents
			b.triggerEvents = nil

			return ev
		}()

		b.dispatchTriggerEvents(events)
	}
}

// finishCrawlJobLocked applies a finished job unless the run was stopped,
// reset, or superseded meanwhile (a stopped run's results are discarded).
// b.mu must be held.
func (b *InMemoryBackend) finishCrawlJobLocked(job *crawlJob, crawlErr error, now time.Time) {
	if b.crawlInFlight[job.name] != job {
		return
	}

	delete(b.crawlInFlight, job.name)

	c, ok := b.crawlers.Get(job.name)
	if !ok || c.State != stateRunning {
		return
	}

	c.State = stateReady
	c.LastUpdated = float64(now.Unix())

	dbGone := c.DatabaseName != "" && !b.databases.Has(strs.Fold(c.DatabaseName))
	if crawlErr == nil && dbGone {
		crawlErr = ErrNotFound
	}

	if crawlErr != nil {
		var stats crawlStats
		if !dbGone {
			stats = job.run.commit(b, false)
		}

		b.failCrawlHistoryLocked(job.name, crawlErr.Error(), stats, now)
		b.setLastCrawlLocked(c, "FAILED")
		b.recordCrawlCompletionLocked(job.name, "FAILED")

		return
	}

	stats := job.run.commit(b, true)
	b.finishCrawlHistoryLocked(job.name, crawlerStateCompleted, stats, now)
	b.setLastCrawlLocked(c, "SUCCEEDED")
	b.recordCrawlCompletionLocked(job.name, "SUCCEEDED")
}
