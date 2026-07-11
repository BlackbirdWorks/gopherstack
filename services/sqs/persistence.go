package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// sqsSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to queueSnapshot, moveTaskSnapshot, or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape (e.g. a field's meaning or type changes). Restore compares
// this against the persisted value and discards (rather than attempts to
// partially decode) any mismatch — see Restore below.
const sqsSnapshotVersion = 2

// queueSnapshot captures all serialisable fields of a Queue. It exists as a
// separate DTO (rather than JSON tags directly on Queue) because Queue also
// carries live, non-serialisable state — an open notify channel, a mutex, a
// self-referential dlq pointer rebuilt post-restore from RedrivePolicy, and
// short-lived caches (fifoSendTimes, receiveAttempts) — that must never be
// part of an on-disk snapshot.
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
	// FifoSeqCounter is the last-issued FIFO SequenceNumber counter. Persisting
	// it prevents newly sent messages after a restore from reusing (or
	// regressing behind) sequence numbers already handed out to consumers
	// before the snapshot was taken.
	FifoSeqCounter uint64 `json:"fifoSeqCounter,omitempty"`
	// LastPurgedAtUnixMilli persists the PurgeQueue cooldown deadline so a
	// restore immediately followed by a PurgeQueue call still honours AWS's
	// 60-second between-purge cooldown instead of resetting it to zero.
	LastPurgedAtUnixMilli int64 `json:"lastPurgedAtUnixMilli,omitempty"`
	MaxReceiveCount       int   `json:"maxReceiveCount"`
	IsFIFO                bool  `json:"isFIFO"`
}

// queueSnapshotKey is the [store.Table] key function used for the ephemeral
// DTO table built inside Snapshot/Restore. It mirrors queueTableKey so the
// on-disk table is keyed identically to the live b.queues table.
func queueSnapshotKey(qs *queueSnapshot) string {
	return queueKey(qs.Region, qs.Name)
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

// moveTaskSnapshotKey is the [store.Table] key function for the ephemeral
// move-task DTO table, mirroring moveTaskTableKey.
func moveTaskSnapshotKey(ts *moveTaskSnapshot) string {
	return ts.TaskHandle
}

// backendSnapshot is the top-level on-disk shape for the SQS backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] — currently "queues" ([]*queueSnapshot) and
// "moveTasks" ([]*moveTaskSnapshot). Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Build a throwaway DTO registry purely to reuse store's deterministic,
	// type-erased JSON encoding (store.Registry.SnapshotAll) instead of
	// hand-rolling the marshal step. This is intentionally separate from the
	// live b.registry: Queue/moveTaskState carry fields (channels, mutexes,
	// cancel funcs, a dlq back-pointer) that cannot round-trip through JSON,
	// and only terminal move tasks are meant to survive a restart — a direct
	// snapshot of the live tables could not express either constraint.
	dtoReg := store.NewRegistry()
	queueDTOs := store.Register(dtoReg, "queues", store.New(queueSnapshotKey))
	moveDTOs := store.Register(dtoReg, "moveTasks", store.New(moveTaskSnapshotKey))

	for _, q := range b.queues.Snapshot() {
		q.mu.Lock()
		var lastPurgedAtMillis int64
		if !q.lastPurgedAt.IsZero() {
			lastPurgedAtMillis = q.lastPurgedAt.UnixMilli()
		}
		fifoSeqCounter := q.fifoSeqCounter
		q.mu.Unlock()

		queueDTOs.Put(&queueSnapshot{
			DeduplicationIDs:      q.DeduplicationIDs,
			Attributes:            q.Attributes,
			Tags:                  q.Tags,
			Permissions:           q.Permissions,
			Messages:              q.messages,
			InFlightMessages:      q.inFlightMessages,
			DeduplicationMsgIDs:   q.deduplicationMsgIDs,
			Name:                  q.Name,
			URL:                   q.URL,
			Region:                q.Region,
			MaxReceiveCount:       q.MaxReceiveCount,
			IsFIFO:                q.IsFIFO,
			FifoSeqCounter:        fifoSeqCounter,
			LastPurgedAtUnixMilli: lastPurgedAtMillis,
		})
	}

	// Persist terminal move tasks (COMPLETED/CANCELLED/FAILED) so task history
	// survives restarts. RUNNING tasks are skipped because the goroutine cannot
	// be resumed, and CANCELLING is a transient state that resolves to CANCELLED.
	b.moveTasks.Range(func(t *moveTaskState) bool {
		t.mu.Lock()
		status := t.status
		if status != MoveTaskStatusCompleted && status != MoveTaskStatusCancelled && status != MoveTaskStatusFailed {
			t.mu.Unlock()

			return true
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

		moveDTOs.Put(snap)

		return true
	})

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal failure
		// here would indicate a programming error rather than bad input data.
		// Log and skip the snapshot rather than panic, matching the
		// persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx,
			"sqs: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   sqsSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "sqs", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// DLQ pointers are re-wired after all queues are reconstructed by re-applying
// each queue's RedrivePolicy attribute.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sqs", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sqsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"sqs: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sqsSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	dtoReg := store.NewRegistry()
	queueDTOs := store.Register(dtoReg, "queues", store.New(queueSnapshotKey))
	moveDTOs := store.Register(dtoReg, "moveTasks", store.New(moveTaskSnapshotKey))

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("sqs: restore snapshot tables: %w", err)
	}

	// Restore region first so we can default per-queue regions when missing.
	if snap.Region != "" {
		b.region = snap.Region
	}

	liveQueues := make([]*Queue, 0, queueDTOs.Len())

	for _, qs := range queueDTOs.All() {
		region := qs.Region
		if region == "" {
			region = b.effectiveRegion("")
		}

		liveQueues = append(liveQueues, restoreQueueFromSnapshot(qs, region))
	}

	b.queues.Restore(liveQueues)

	// Restore terminal move task history. A no-op cancel function is used because
	// these tasks are already in a terminal state and their goroutines are gone.
	liveMoveTasks := make([]*moveTaskState, 0, moveDTOs.Len())

	for _, ts := range moveDTOs.All() {
		liveMoveTasks = append(liveMoveTasks, &moveTaskState{
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
		})
	}

	b.moveTasks.Restore(liveMoveTasks)

	// Re-wire DLQ pointers now that every queue has been reconstructed. We must
	// do this after the second pass because a queue's RedrivePolicy can target
	// any other queue regardless of restore iteration order.
	for _, q := range b.queues.All() {
		_ = applyRedrivePolicy(q, q.Attributes, b)
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

	var lastPurgedAt time.Time
	if qs.LastPurgedAtUnixMilli != 0 {
		lastPurgedAt = time.UnixMilli(qs.LastPurgedAtUnixMilli)
	}

	q := &Queue{
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
		fifoSeqCounter:      qs.FifoSeqCounter,
		lastPurgedAt:        lastPurgedAt,
	}

	// The background janitor (pruneState) skips non-FIFO queues whose
	// hasActivity flag is false to avoid scanning idle queues (#59 follow-on).
	// A restored queue with pending visible/in-flight messages must be marked
	// active so retention expiry and visibility-timeout requeue resume without
	// waiting for a new SendMessage to flip the flag — otherwise those
	// messages are stuck until the next producer write to that specific queue.
	if len(qs.Messages) > 0 || len(qs.InFlightMessages) > 0 {
		q.hasActivity.Store(true)
	}

	return q
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
