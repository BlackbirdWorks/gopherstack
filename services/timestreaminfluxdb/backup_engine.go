package timestreaminfluxdb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// restoreSource is the backup ID a restore replays; empty when the backup holds no engine data.
func (bk *DBBackup) restoreSource() string {
	if bk.HasData {
		return bk.ID
	}

	return ""
}

// stampBackupConfigs copies cfgs with the schedule clock starting at now.
func stampBackupConfigs(cfgs []BackupConfig, now time.Time) []BackupConfig {
	out := make([]BackupConfig, len(cfgs))

	for i, c := range cfgs {
		c.LastRun = now
		out[i] = c
	}

	return out
}

func stampedPlacement(p Placement, now time.Time) Placement {
	p.BackupConfigs = stampBackupConfigs(p.BackupConfigs, now)

	return p
}

// startBackupLocked completes bk: with a running source server the backup is taken by influx backup,
// otherwise it completes after the lifecycle dwell.
func (b *InMemoryBackend) startBackupLocked(bk *DBBackup) {
	h, ok := b.live[bk.SourceID]
	if !ok || b.engine == nil || b.closed {
		bk.Pending = b.pend(statusCreating, false)

		return
	}

	bk.Pending = b.pend(statusCreating, true)
	id, token, eng := bk.ID, bk.Token, b.engine

	b.wg.Go(func() {
		ctx, cancel := context.WithTimeout(b.bg, engineStartTimeout)
		defer cancel()

		b.finishBackup(id, eng.Backup(ctx, h, token, id))
	})
}

func (b *InMemoryBackend) finishBackup(id string, err error) {
	b.mu.Lock("finishBackup")
	defer b.mu.Unlock()

	bk, ok := b.backups.Get(id)
	if !ok || bk.Pending.Kind == statusDeleting {
		return
	}

	if err != nil {
		logger.Load(b.bgOrBackground()).WarnContext(b.bgOrBackground(),
			"timestreaminfluxdb: engine backup failed", "backup", id, "error", err)

		bk.Pending = Pending{Kind: statusFailed, Until: b.now().Add(farFuture)}

		return
	}

	bk.HasData = true
	bk.Pending = Pending{}
}

// restoreEngineLocked replaces the server of inst with one restored from bk.
func (b *InMemoryBackend) restoreEngineLocked(inst *DBInstance, bk *DBBackup) {
	if bk.restoreSource() == "" {
		return
	}

	inst.RestoreFrom = bk.ID

	if !b.recreateEngineLocked(inst, statusRestoring) {
		inst.RestoreFrom = ""
	}
}

func (b *InMemoryBackend) restoreMembersLocked(clusterID string, bk *DBBackup) {
	for _, m := range b.membersLocked(clusterID) {
		b.restoreEngineLocked(m, bk)
	}
}
