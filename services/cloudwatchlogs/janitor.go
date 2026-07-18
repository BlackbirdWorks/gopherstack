package cloudwatchlogs

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = time.Minute
	cwlWorkerService       = "cloudwatchlogs"
	retentionSweeperName   = "RetentionSweeper"
)

// Janitor is the CloudWatch Logs background worker that enforces retention policies
// by evicting log events that have aged past their log group's RetentionInDays setting.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Janitor for the given backend.
// A zero interval falls back to the default of one minute.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, cwlWorkerService)
	g.Ticker(retentionSweeperName, j.Interval, j.TaskTimeout, j.sweepRetention)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single retention sweep. Primarily intended for tests.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepRetention(ctx)
}

// sweepRetention removes log events that have aged past the log group's
// RetentionInDays policy. It iterates over all log groups and streams,
// trimming events whose timestamp predates the retention cutoff.
// Stream metadata (FirstEventTimestamp, LastEventTimestamp, LastIngestionTime)
// is updated to reflect the remaining events.
//
// The sweep is split into two phases per group: a read-lock phase that builds
// the eviction plan (CPU-intensive filtering), then a write-lock phase that
// applies only the computed results — minimising write-lock hold time.
func (j *Janitor) sweepRetention(ctx context.Context) {
	evicted := 0
	now := time.Now()
	for _, target := range j.retentionTargets(now) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Phase 1: build eviction plan under read lock (no mutations).
		plan := j.buildEvictionPlan(target.region, target.groupName, target.cutoffMs)
		if len(plan) == 0 {
			continue
		}

		// Phase 2: apply the plan under write lock (minimal critical section).
		func() {
			j.Backend.mu.Lock("JanitorSweepRetention")
			defer j.Backend.mu.Unlock()
			evicted += j.applyEvictionPlan(target.region, target.groupName, plan)
		}()
	}

	telemetry.RecordWorkerTask(cwlWorkerService, retentionSweeperName, "success")

	if evicted == 0 {
		return
	}

	telemetry.RecordWorkerItems(cwlWorkerService, retentionSweeperName, evicted)
	logger.Load(ctx).InfoContext(ctx,
		"CloudWatch Logs janitor: evicted log events past retention policy",
		"evicted", evicted)
}

type retentionTarget struct {
	region    string
	groupName string
	cutoffMs  int64
}

func (j *Janitor) retentionTargets(now time.Time) []retentionTarget {
	j.Backend.mu.RLock("JanitorRetentionTargets")
	defer j.Backend.mu.RUnlock()

	var targets []retentionTarget
	for _, group := range j.Backend.groups.All() {
		days := j.Backend.settings.MaxRetentionDays
		if group.RetentionInDays != nil && *group.RetentionInDays > 0 {
			days = int(*group.RetentionInDays)
		}

		if days <= 0 {
			continue
		}

		targets = append(targets, retentionTarget{
			region:    group.region,
			groupName: group.LogGroupName,
			cutoffMs:  now.AddDate(0, 0, -days).UnixMilli(),
		})
	}

	return targets
}

// streamEvictionPlan holds the pre-computed result of filtering one stream's events.
type streamEvictionPlan struct {
	streamName   string
	kept         []*OutputLogEvent
	evictedBytes int64
	evictedCount int
}

// buildEvictionPlan scans all streams in groupName under a READ lock and returns
// a plan of which streams need updating and what the new event slice should be.
// Streams with no evictions are omitted from the plan.
func (j *Janitor) buildEvictionPlan(region, groupName string, cutoffMs int64) []streamEvictionPlan {
	j.Backend.mu.RLock("JanitorBuildEvictionPlan")
	defer j.Backend.mu.RUnlock()

	groupStreams := j.Backend.streamsInGroup(region, groupName)
	if len(groupStreams) == 0 {
		return nil
	}

	var plan []streamEvictionPlan
	for _, stream := range groupStreams {
		evts := stream.events
		kept := make([]*OutputLogEvent, 0, len(evts))
		var evictedBytes int64
		var evictedCount int
		for _, ev := range evts {
			if ev.Timestamp >= cutoffMs {
				kept = append(kept, ev)
			} else {
				evictedCount++
				evictedBytes += int64(len(ev.Message))
			}
		}
		if evictedCount == 0 {
			continue
		}
		plan = append(plan, streamEvictionPlan{
			streamName:   stream.LogStreamName,
			kept:         kept,
			evictedBytes: evictedBytes,
			evictedCount: evictedCount,
		})
	}

	return plan
}

// applyEvictionPlan applies a pre-computed eviction plan under a WRITE lock.
// Returns the total number of evicted events.
// Must be called with the backend write lock held.
func (j *Janitor) applyEvictionPlan(region, groupName string, plan []streamEvictionPlan) int {
	evicted := 0

	group, _ := j.Backend.groupGet(region, groupName)

	for _, entry := range plan {
		stream, ok := j.Backend.streamGet(region, groupName, entry.streamName)
		if !ok {
			continue
		}

		stream.events = entry.kept
		evicted += entry.evictedCount

		if entry.evictedBytes > 0 {
			stream.StoredBytes -= entry.evictedBytes
			if group != nil {
				group.StoredBytes -= entry.evictedBytes
			}
		}

		updateStreamTimestamps(stream, entry.kept)
	}

	return evicted
}

// updateStreamTimestamps recomputes FirstEventTimestamp, LastEventTimestamp, and
// LastIngestionTime from the given slice of remaining events.
// An empty slice clears all three fields.
func updateStreamTimestamps(stream *LogStream, events []*OutputLogEvent) {
	if len(events) == 0 {
		stream.FirstEventTimestamp = nil
		stream.LastEventTimestamp = nil
		stream.LastIngestionTime = nil

		return
	}

	first := events[0].Timestamp
	last := events[0].Timestamp
	ingestion := events[0].IngestionTime

	for _, ev := range events[1:] {
		if ev.Timestamp < first {
			first = ev.Timestamp
		}
		if ev.Timestamp > last {
			last = ev.Timestamp
		}
		if ev.IngestionTime > ingestion {
			ingestion = ev.IngestionTime
		}
	}

	stream.FirstEventTimestamp = &first
	stream.LastEventTimestamp = &last
	stream.LastIngestionTime = &ingestion
}
