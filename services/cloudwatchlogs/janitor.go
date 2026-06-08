package cloudwatchlogs

import (
	"context"
	"slices"
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepRetention(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
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
	evicted := 0
	now := time.Now()
	for _, target := range j.retentionTargets(now) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		j.Backend.mu.Lock("JanitorSweepRetention")
		evicted += j.sweepGroupStreams(target.region, target.groupName, target.cutoffMs)
		j.Backend.mu.Unlock()
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

	regions := make([]string, 0, len(j.Backend.groups))
	for region := range j.Backend.groups {
		regions = append(regions, region)
	}
	slices.Sort(regions)

	var targets []retentionTarget
	for _, region := range regions {
		regionGroups := j.Backend.groups[region]
		groupNames := make([]string, 0, len(regionGroups))
		for groupName := range regionGroups {
			groupNames = append(groupNames, groupName)
		}
		slices.Sort(groupNames)

		for _, groupName := range groupNames {
			group := regionGroups[groupName]
			days := j.Backend.settings.MaxRetentionDays
			if group.RetentionInDays != nil && *group.RetentionInDays > 0 {
				days = int(*group.RetentionInDays)
			}

			if days <= 0 {
				continue
			}

			targets = append(targets, retentionTarget{
				region:    region,
				groupName: groupName,
				cutoffMs:  now.AddDate(0, 0, -days).UnixMilli(),
			})
		}
	}

	return targets
}

// sweepGroupStreams evicts events older than cutoffMs for all streams in groupName.
// Returns the number of evicted events. Must be called with the backend write lock held.
func (j *Janitor) sweepGroupStreams(region, groupName string, cutoffMs int64) int {
	evicted := 0

	regionEvents := j.Backend.events[region]
	regionStreams := j.Backend.streams[region]
	regionGroups := j.Backend.groups[region]

	for streamName, evts := range regionEvents[groupName] {
		kept := make([]*OutputLogEvent, 0, len(evts))
		evictedBytes := int64(0)
		for _, ev := range evts {
			if ev.Timestamp >= cutoffMs {
				kept = append(kept, ev)
			} else {
				evicted++
				evictedBytes += int64(len(ev.Message))
			}
		}

		if len(kept) == len(evts) {
			continue
		}

		regionEvents[groupName][streamName] = kept

		if evictedBytes > 0 {
			stream := regionStreams[groupName][streamName]
			if stream != nil {
				stream.StoredBytes -= evictedBytes
			}
			if g := regionGroups[groupName]; g != nil {
				g.StoredBytes -= evictedBytes
			}
		}

		// Update stream metadata to reflect the events that remain.
		stream := regionStreams[groupName][streamName]
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
