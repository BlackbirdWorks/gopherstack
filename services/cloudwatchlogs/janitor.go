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

		for streamName, evts := range j.Backend.events[groupName] {
			kept := make([]*OutputLogEvent, 0, len(evts))
			for _, ev := range evts {
				if ev.Timestamp >= cutoffMs {
					kept = append(kept, ev)
				} else {
					evicted++
				}
			}

			j.Backend.events[groupName][streamName] = kept
		}
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
