package sqs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// queueSnapshot captures all serialisable fields of a Queue.
type queueSnapshot struct {
	DeduplicationIDs    map[string]time.Time             `json:"deduplicationIDs"`
	Attributes          map[string]string                `json:"attributes"`
	Tags                *tags.Tags                       `json:"tags,omitempty"`
	Permissions         map[string]*QueuePermissionEntry `json:"permissions,omitempty"`
	DeduplicationMsgIDs map[string]string                `json:"deduplicationMsgIDs"`
	Name                string                           `json:"name"`
	URL                 string                           `json:"url"`
	Region              string                           `json:"region,omitempty"`
	Messages            []*Message                       `json:"messages"`
	InFlightMessages    []*InFlightMessage               `json:"inFlightMessages"`
	MaxReceiveCount     int                              `json:"maxReceiveCount"`
	IsFIFO              bool                             `json:"isFIFO"`
}

// moveTaskSnapshot captures the serialisable state of a completed/cancelled/failed move task.
// Running tasks are NOT persisted because their goroutines cannot be resumed after restart.
type moveTaskSnapshot struct {
	TaskHandle    string         `json:"taskHandle"`
	SourceArn     string         `json:"sourceArn"`
	DestArn       string         `json:"destArn"`
	FailureReason string         `json:"failureReason,omitempty"`
	Status        MoveTaskStatus `json:"status"`
	StartedAt     int64          `json:"startedAt"`
	MovedCount    int64          `json:"movedCount"`
	TotalCount    int64          `json:"totalCount"`
	MaxPerSec     int32          `json:"maxPerSec,omitempty"`
}

type backendSnapshot struct {
	Queues    map[string]*queueSnapshot `json:"queues"`
	AccountID string                    `json:"accountID"`
	Region    string                    `json:"region"`
	MoveTasks []*moveTaskSnapshot       `json:"moveTasks,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	queues := make(map[string]*queueSnapshot, len(b.queues))
	for k, q := range b.queues {
		queues[k] = &queueSnapshot{
			DeduplicationIDs:    q.DeduplicationIDs,
			Attributes:          q.Attributes,
			Tags:                q.Tags,
			Permissions:         q.Permissions,
			Messages:            q.messages,
			InFlightMessages:    q.inFlightMessages,
			DeduplicationMsgIDs: q.deduplicationMsgIDs,
			Name:                q.Name,
			URL:                 q.URL,
			Region:              q.Region,
			MaxReceiveCount:     q.MaxReceiveCount,
			IsFIFO:              q.IsFIFO,
		}
	}

	// Persist terminal move tasks (COMPLETED/CANCELLED/FAILED) so task history
	// survives restarts. RUNNING tasks are skipped because the goroutine cannot
	// be resumed, and CANCELLING is a transient state that resolves to CANCELLED.
	var moveTasks []*moveTaskSnapshot
	for _, t := range b.moveTasks {
		t.mu.Lock()
		status := t.status
		// Skip non-terminal tasks — only snapshot what can be meaningfully restored.
		if status != MoveTaskStatusCompleted && status != MoveTaskStatusCancelled && status != MoveTaskStatusFailed {
			t.mu.Unlock()

			continue
		}

		snap := &moveTaskSnapshot{
			TaskHandle:    t.taskHandle,
			SourceArn:     t.sourceArn,
			DestArn:       t.destArn,
			FailureReason: t.failureReason,
			Status:        status,
			StartedAt:     t.startedAt,
			MovedCount:    t.movedCount,
			TotalCount:    t.totalCount,
			MaxPerSec:     t.maxPerSec,
		}
		t.mu.Unlock()

		moveTasks = append(moveTasks, snap)
	}

	snap := backendSnapshot{
		Queues:    queues,
		MoveTasks: moveTasks,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// DLQ pointers are re-wired after all queues are reconstructed by re-applying
// each queue's RedrivePolicy attribute.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Queues == nil {
		snap.Queues = make(map[string]*queueSnapshot)
	}

	b.queues = make(map[string]*Queue, len(snap.Queues))

	// Restore region first so we can default per-queue regions when missing.
	if snap.Region != "" {
		b.region = snap.Region
	}

	for _, qs := range snap.Queues {
		region := qs.Region
		if region == "" {
			region = b.effectiveRegion("")
		}

		b.queues[queueKey(region, qs.Name)] = restoreQueueFromSnapshot(qs, region)
	}

	// Restore terminal move task history. A no-op cancel function is used because
	// these tasks are already in a terminal state and their goroutines are gone.
	b.moveTasks = make(map[string]*moveTaskState)

	for _, ts := range snap.MoveTasks {
		b.moveTasks[ts.TaskHandle] = &moveTaskState{
			cancel:        func() {},
			taskHandle:    ts.TaskHandle,
			sourceArn:     ts.SourceArn,
			destArn:       ts.DestArn,
			failureReason: ts.FailureReason,
			status:        ts.Status,
			startedAt:     ts.StartedAt,
			movedCount:    ts.MovedCount,
			totalCount:    ts.TotalCount,
			maxPerSec:     ts.MaxPerSec,
		}
	}

	// Re-wire DLQ pointers now that every queue has been reconstructed. We must
	// do this after the second pass because a queue's RedrivePolicy can target
	// any other queue regardless of restore iteration order.
	for _, q := range b.queues {
		applyRedrivePolicy(q, q.Attributes, b)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreQueueFromSnapshot rebuilds a Queue from its persisted snapshot.
func restoreQueueFromSnapshot(qs *queueSnapshot, region string) *Queue {
	if qs.DeduplicationIDs == nil {
		qs.DeduplicationIDs = make(map[string]time.Time)
	}

	if qs.Attributes == nil {
		qs.Attributes = make(map[string]string)
	}

	if qs.DeduplicationMsgIDs == nil {
		qs.DeduplicationMsgIDs = make(map[string]string)
	}

	if qs.Permissions == nil {
		qs.Permissions = make(map[string]*QueuePermissionEntry)
	}

	inFlightByHandle := make(map[string]*InFlightMessage, len(qs.InFlightMessages))
	for _, inf := range qs.InFlightMessages {
		inFlightByHandle[inf.ReceiptHandle] = inf
	}

	now := time.Now()
	delayedCount := 0

	for _, msg := range qs.Messages {
		if now.Before(msg.VisibleAt) {
			delayedCount++
		}
	}

	return &Queue{
		DeduplicationIDs:    qs.DeduplicationIDs,
		Attributes:          qs.Attributes,
		Tags:                qs.Tags,
		Permissions:         qs.Permissions,
		messages:            qs.Messages,
		inFlightMessages:    qs.InFlightMessages,
		inFlightByHandle:    inFlightByHandle,
		deduplicationMsgIDs: qs.DeduplicationMsgIDs,
		Name:                qs.Name,
		URL:                 qs.URL,
		Region:              region,
		MaxReceiveCount:     qs.MaxReceiveCount,
		IsFIFO:              qs.IsFIFO,
		notify:              make(chan struct{}),
		delayedCount:        delayedCount,
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
