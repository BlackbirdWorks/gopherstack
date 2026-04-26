package sqs

import (
	"context"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultSQSJanitorInterval = time.Minute
	sqsJanitorService         = "sqs"
	sqsJanitorComponent       = "MessageRetentionSweeper"
	msPerSecond               = 1000
)

// Janitor is the SQS background worker that deletes messages that have exceeded
// their MessageRetentionPeriod.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new SQS Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSQSJanitorInterval
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
			j.sweepExpiredMessages(ctx)
		}
	}
}

// sweepExpiredMessages removes messages that have exceeded the queue's MessageRetentionPeriod.
func (j *Janitor) sweepExpiredMessages(ctx context.Context) {
	j.Backend.mu.Lock("sweepExpiredMessages")
	defer j.Backend.mu.Unlock()

	now := time.Now().UnixMilli()
	purgedTotal := 0

	for _, q := range j.Backend.queues {
		retentionStr, ok := q.Attributes[attrMessageRetentionPeriod]
		if !ok {
			retentionStr = strconv.Itoa(defaultMessageRetentionPeriod)
		}
		retentionSecs, err := strconv.Atoi(retentionStr)
		if err != nil {
			retentionSecs = defaultMessageRetentionPeriod
		}
		retentionMs := int64(retentionSecs) * msPerSecond

		// Filter available messages
		var activeMsgs []*Message
		for _, msg := range q.messages {
			if now-msg.SentTimestamp > retentionMs {
				purgedTotal++
			} else {
				activeMsgs = append(activeMsgs, msg)
			}
		}
		q.messages = activeMsgs

		// Filter in-flight messages
		var activeInFlight []*InFlightMessage
		for _, inflight := range q.inFlightMessages {
			if now-inflight.Msg.SentTimestamp > retentionMs {
				purgedTotal++
			} else {
				activeInFlight = append(activeInFlight, inflight)
			}
		}
		q.inFlightMessages = activeInFlight
	}

	if purgedTotal > 0 {
		telemetry.RecordWorkerItems(sqsJanitorService, sqsJanitorComponent, purgedTotal)
		logger.Load(ctx).InfoContext(ctx, "SQS janitor: expired messages purged", "count", purgedTotal)
	}
	telemetry.RecordWorkerTask(sqsJanitorService, sqsJanitorComponent, "success")
}
