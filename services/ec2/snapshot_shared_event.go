package ec2

import (
	"context"
	"sync/atomic"
)

// SnapshotSharedEvent is the EBS "shareSnapshot" notification: SharedWith accounts were granted
// create-volume permission on SnapshotID, which OwnerID owns in Region.
type SnapshotSharedEvent struct {
	Region      string
	SnapshotID  string
	OwnerID     string
	Description string
	SharedWith  []string
}

// SnapshotSharedListener receives snapshot-shared events after the permission change is committed.
type SnapshotSharedListener func(ctx context.Context, evt SnapshotSharedEvent)

// handlerHooks is shared by a home handler and its regional siblings.
type handlerHooks struct {
	snapshotShared atomic.Pointer[SnapshotSharedListener]
}

// SetSnapshotSharedListener installs the listener for this handler and every regional sibling.
func (h *Handler) SetSnapshotSharedListener(l SnapshotSharedListener) {
	if l == nil {
		h.hooks.snapshotShared.Store(nil)

		return
	}

	h.hooks.snapshotShared.Store(&l)
}

func (h *Handler) emitSnapshotShared(ctx context.Context, snapshotID string, sharedWith []string) {
	lp := h.hooks.snapshotShared.Load()
	if lp == nil || len(sharedWith) == 0 {
		return
	}

	snaps, err := h.Backend.DescribeSnapshots([]string{snapshotID})
	if err != nil || len(snaps) == 0 {
		return
	}

	evt := SnapshotSharedEvent{
		SnapshotID:  snapshotID,
		OwnerID:     snaps[0].OwnerID,
		Description: snaps[0].Description,
		SharedWith:  append([]string(nil), sharedWith...),
	}

	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		evt.Region = mem.Region

		if evt.OwnerID == "" {
			evt.OwnerID = mem.AccountID
		}
	}

	if evt.OwnerID == "" {
		evt.OwnerID = h.AccountID
	}

	(*lp)(ctx, evt)
}
