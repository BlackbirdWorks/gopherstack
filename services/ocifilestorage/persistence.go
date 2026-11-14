package ocifilestorage

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
	FileSystems  table[*FileSystem]        `json:"fileSystems"`
	MountTargets table[*MountTarget]       `json:"mountTargets"`
	ExportSets   table[*ExportSet]         `json:"exportSets"`
	Exports      table[*Export]            `json:"exports"`
	Snapshots    table[*Snapshot]          `json:"snapshots"`
	Policies     table[*Policy]            `json:"policies"`
	Replications table[*Replication]       `json:"replications"`
	Targets      table[*ReplicationTarget] `json:"replicationTargets"`
	Connectors   table[*Connector]         `json:"connectors"`
	Quotas       table[*QuotaRule]         `json:"quotaRules"`
	Tokens       map[string]string         `json:"retryTokens"`
	Auth         []ociauth.KeyRecord       `json:"authKeys"`
	Version      int                       `json:"version"`
	Seq          int64                     `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys. File data and the NFS listeners are not
// part of it: data lives in the data directory.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocifilestorage", backendSnapshot{
		Version: snapshotVersion, FileSystems: b.fss, MountTargets: b.mts, ExportSets: b.sets, Exports: b.exps,
		Snapshots: b.snaps, Policies: b.pols, Replications: b.reps, Targets: b.tgts, Connectors: b.conns,
		Quotas: b.quotas, Tokens: b.tokens, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state and restarts the NFS server of every ACTIVE mount target.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocifilestorage", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocifilestorage: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.fss, b.mts, b.sets, b.exps = snap.FileSystems, snap.MountTargets, snap.ExportSets, snap.Exports
	b.snaps, b.pols, b.reps = snap.Snapshots, snap.Policies, snap.Replications
	b.tgts, b.conns, b.quotas = snap.Targets, snap.Connectors, snap.Quotas
	b.tokens, b.seq = snap.Tokens, snap.Seq

	if b.dp != nil {
		b.dp.StopMounts()
	}

	b.restoreDataPlane()
	b.mu.Unlock()
	b.Auth.Load(snap.Auth)

	return nil
}

func (b *InMemoryBackend) restoreDataPlane() {
	if b.dp == nil {
		return
	}

	for id, e := range b.fss {
		if e.W.State != stDeleted && e.W.State != stDeleting {
			_ = b.dp.CreateFS(id)
		}
	}

	for _, e := range b.mts {
		b.mountTargetSettled(e)
	}
}

func nilEntry[W resource](kind string, t table[W]) error {
	for id, e := range t {
		var zero W

		if e == nil || any(e.W) == any(zero) {
			return fmt.Errorf("%w: %s %q", ErrNullEntry, kind, id)
		}
	}

	return nil
}

func checkSnapshot(snap *backendSnapshot) error {
	if snap.Tokens == nil {
		snap.Tokens = map[string]string{}
	}

	snap.FileSystems, snap.MountTargets = orEmpty(snap.FileSystems), orEmpty(snap.MountTargets)
	snap.ExportSets, snap.Exports, snap.Snapshots = orEmpty(
		snap.ExportSets,
	), orEmpty(
		snap.Exports,
	), orEmpty(
		snap.Snapshots,
	)
	snap.Policies, snap.Replications, snap.Targets = orEmpty(
		snap.Policies,
	), orEmpty(
		snap.Replications,
	), orEmpty(
		snap.Targets,
	)
	snap.Connectors, snap.Quotas = orEmpty(snap.Connectors), orEmpty(snap.Quotas)

	for _, err := range []error{
		nilEntry("file system", snap.FileSystems), nilEntry("mount target", snap.MountTargets),
		nilEntry("export set", snap.ExportSets), nilEntry("export", snap.Exports), nilEntry("snapshot", snap.Snapshots),
		nilEntry("policy", snap.Policies), nilEntry("replication", snap.Replications),
		nilEntry("replication target", snap.Targets), nilEntry("connector", snap.Connectors),
		nilEntry("quota rule", snap.Quotas),
	} {
		if err != nil {
			return err
		}
	}

	return nil
}

func orEmpty[W resource](t table[W]) table[W] {
	if t == nil {
		return table[W]{}
	}

	return t
}
