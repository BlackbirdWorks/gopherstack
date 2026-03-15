package cloudwatchlogs

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval = time.Minute
	cwlWorkerService       = "cloudwatchlogs"
	retentionSweeperName   = "RetentionSweeper"
)

// Janitor is the CloudWatch Logs background worker that enforces retention policies
// by evicting log events that have aged past their log group's RetentionInDays setting.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.sweepRetention(ctx)
		}
	}
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
func (j *Janitor) sweepRetention(ctx context.Context) {
	j.Backend.mu.Lock("JanitorSweepRetention")

	evicted := 0

	for groupName, group := range j.Backend.groups {
		if group.RetentionInDays == nil || *group.RetentionInDays <= 0 {
			continue
		}

		cutoffMs := time.Now().
			AddDate(0, 0, -int(*group.RetentionInDays)).
			UnixMilli()

		evicted += j.sweepGroupStreams(groupName, cutoffMs)
	}

	j.Backend.mu.Unlock()

	telemetry.RecordWorkerTask(cwlWorkerService, retentionSweeperName, "success")

	if evicted == 0 {
		return
	}

	telemetry.RecordWorkerItems(cwlWorkerService, retentionSweeperName, evicted)
	logger.Load(ctx).InfoContext(ctx,
		"CloudWatch Logs janitor: evicted log events past retention policy",
		"evicted", evicted)
}

// sweepGroupStreams evicts events older than cutoffMs for all streams in groupName.
// Returns the number of evicted events. Must be called with the backend write lock held.
func (j *Janitor) sweepGroupStreams(groupName string, cutoffMs int64) int {
	evicted := 0

	for streamName, evts := range j.Backend.events[groupName] {
		kept := make([]*OutputLogEvent, 0, len(evts))
		for _, ev := range evts {
			if ev.Timestamp >= cutoffMs {
				kept = append(kept, ev)
			} else {
				evicted++
			}
		}

		if len(kept) == len(evts) {
			continue
		}

		j.Backend.events[groupName][streamName] = kept

		// Update stream metadata to reflect the events that remain.
		stream := j.Backend.streams[groupName][streamName]
		if stream != nil {
			updateStreamTimestamps(stream, kept)
		}
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
