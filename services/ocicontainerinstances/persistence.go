package ocicontainerinstances

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Instances map[string]*Instance    `json:"instances"`
	WorkReqs  map[string]*WorkRequest `json:"workRequests"`
	Auth      []ociauth.KeyRecord     `json:"authKeys"`
	Version   int                     `json:"version"`
	Seq       int64                   `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys and image pull secrets.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocicontainerinstances", backendSnapshot{
		Version: snapshotVersion, Instances: b.instances, WorkReqs: b.workReqs, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state. Containers do not survive a restart: instances that were running come back INACTIVE.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocicontainerinstances", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocicontainerinstances: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.instances, b.workReqs, b.seq = snap.Instances, snap.WorkReqs, snap.Seq
	b.containers = map[string]string{}

	for id, in := range b.instances {
		for _, c := range in.Containers {
			b.containers[c.ID] = id
		}

		b.settleRestored(in)
	}

	for _, wr := range b.workReqs {
		if wr.Status == wrInProgress && wr.FinishAt == nil {
			now := b.nowUTC()
			wr.Status, wr.FinishedAt, wr.ErrCode, wr.ErrMsg = wrFailed, &now, "Interrupted", "restored from a snapshot"
		}
	}
	b.mu.Unlock()
	b.Auth.Load(snap.Auth)

	return nil
}

// settleRestored resolves engine-driven states that cannot continue after a restart. Callers hold the write lock.
func (b *InMemoryBackend) settleRestored(in *Instance) {
	if b.engine == nil || in.TransitionAt != nil {
		return
	}

	switch in.State {
	case stateActive, stateUpdating:
		b.setState(in, stateInactive, "Restored from a snapshot; start the container instance to run its containers")
	case stateCreating:
		b.setState(in, stateFailed, "Restored from a snapshot before the containers started")
	case stateDeleting:
		b.setState(in, stateDeleted, "")
	}
}

func checkSnapshot(snap *backendSnapshot) error {
	if snap.Instances == nil {
		snap.Instances = map[string]*Instance{}
	}

	if snap.WorkReqs == nil {
		snap.WorkReqs = map[string]*WorkRequest{}
	}

	for id, in := range snap.Instances {
		if in == nil {
			return fmt.Errorf("%w: instance %q", ErrSnapshotNull, id)
		}

		for _, c := range in.Containers {
			if c == nil {
				return fmt.Errorf("%w: container of %q", ErrSnapshotNull, id)
			}
		}
	}

	for id, wr := range snap.WorkReqs {
		if wr == nil {
			return fmt.Errorf("%w: work request %q", ErrSnapshotNull, id)
		}
	}

	return nil
}
