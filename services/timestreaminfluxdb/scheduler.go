package timestreaminfluxdb

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	schedulerInterval = 30 * time.Second
	logShipInterval   = time.Minute
	backupKeyPrefix   = "automated-"
)

// SetLogSink enables LogDeliveryConfiguration: container logs are written to the configured S3 bucket.
func (b *InMemoryBackend) SetLogSink(s billingexport.ObjectStore) {
	b.mu.Lock("SetLogSink")
	defer b.mu.Unlock()

	b.logSink = s
}

// StartScheduler runs RunScheduled every interval (30s when zero) until Close.
func (b *InMemoryBackend) StartScheduler(interval time.Duration) {
	if interval <= 0 {
		interval = schedulerInterval
	}

	b.mu.Lock("StartScheduler")
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.ensureBGLocked()

	ctx := b.bg

	b.wg.Go(func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.RunScheduled(ctx)
			}
		}
	})
}

// RunScheduled performs the work that has come due on the backend clock: automated backups and their
// retention, maintenance windows and log delivery.
func (b *InMemoryBackend) RunScheduled(ctx context.Context) {
	b.mu.Lock("RunScheduled")

	if b.closed {
		b.mu.Unlock()

		return
	}

	b.purgeLocked()

	now := b.now()
	targets := b.targetsLocked()

	b.runBackupsLocked(targets, now)
	b.expireBackupsLocked(now)
	b.runMaintenanceLocked(now)
	ships := b.logShipmentsLocked(now)
	b.mu.Unlock()

	b.shipLogs(ctx, ships)
}

// target is a standalone instance or a cluster: the resources that own a backup schedule.
type target struct {
	p       *Placement
	id      string
	name    string
	created time.Time
	sc      scope
}

func (b *InMemoryBackend) targetsLocked() []target {
	var out []target

	for _, i := range b.instances.All() {
		if i.ClusterID == "" && !b.gone(i.Pending) {
			out = append(out, target{
				p: &i.Placement, id: i.ID, name: i.Name, created: i.CreatedAt,
				sc: scope{account: i.AccountID, region: i.Region},
			})
		}
	}

	for _, c := range b.clusters.All() {
		if !b.gone(c.Pending) {
			out = append(out, target{
				p: &c.Placement, id: c.ID, name: c.Name, created: c.CreatedAt,
				sc: scope{account: c.AccountID, region: c.Region},
			})
		}
	}

	slices.SortFunc(out, func(x, y target) int { return cmp.Compare(x.id, y.id) })

	return out
}

func (b *InMemoryBackend) runBackupsLocked(targets []target, now time.Time) {
	for _, t := range targets {
		for i := range t.p.BackupConfigs {
			cfg := &t.p.BackupConfigs[i]

			if backupDue(cfg, t.created, now) && b.autoBackupLocked(t, cfg, now) {
				cfg.LastRun = now
			}
		}
	}
}

func backupDue(cfg *BackupConfig, created, now time.Time) bool {
	if !cfg.Enabled {
		return false
	}

	base := cfg.LastRun
	if base.IsZero() {
		base = created
	}

	next := nextBackup(base, cfg.Type)

	return !next.IsZero() && !next.After(now)
}

// autoBackupLocked takes one scheduled backup of t; false when the resource cannot be backed up right now.
func (b *InMemoryBackend) autoBackupLocked(t target, cfg *BackupConfig, now time.Time) bool {
	bk, err := b.snapshotLocked(t.sc, t.id)
	if err != nil || b.backupCountLocked(t.sc) >= maxBackups {
		return false
	}

	id := newID()
	bk.ID, bk.ARN = id, t.sc.arn("db-backup/"+id)
	bk.Name = fmt.Sprintf("%s%s-%s", backupKeyPrefix, t.name, now.UTC().Format("20060102150405"))
	bk.AccountID, bk.Region, bk.Type = t.sc.account, t.sc.region, cfg.Type
	bk.RetentionDays, bk.CreatedAt = cfg.RetentionDays, now
	b.backups.Put(bk)
	b.startBackupLocked(bk)

	return true
}

// expireBackupsLocked deletes scheduled backups past their retention.
func (b *InMemoryBackend) expireBackupsLocked(now time.Time) {
	for _, bk := range b.backups.All() {
		if bk.Type == backupOnDemand || bk.RetentionDays <= 0 || b.gone(bk.Pending) ||
			bk.Pending.Kind == statusDeleting || b.backupState(bk) == backupInProgress {
			continue
		}

		if now.After(bk.CreatedAt.AddDate(0, 0, int(bk.RetentionDays))) {
			bk.Pending = b.pend(statusDeleting, false)
			removeDir(b.backupDirLocked(bk.ID))
		}
	}
}

// runMaintenanceLocked records a maintenance run for every resource whose window has come round.
func (b *InMemoryBackend) runMaintenanceLocked(now time.Time) {
	for _, i := range b.instances.All() {
		if i.ClusterID == "" && !b.gone(i.Pending) &&
			maintenanceDue(i.Placement.Maintenance, i.LastMaintenance, i.CreatedAt, now) {
			i.LastMaintenance = now
		}
	}

	for _, c := range b.clusters.All() {
		if !b.gone(c.Pending) && maintenanceDue(c.Placement.Maintenance, c.LastMaintenance, c.CreatedAt, now) {
			c.LastMaintenance = now
		}
	}
}

func maintenanceDue(m *Maintenance, last, created, now time.Time) bool {
	base := last
	if base.IsZero() {
		base = created
	}

	next := nextMaintenance(base, m)

	return !next.IsZero() && !next.After(now)
}

type logShipment struct {
	eng    Engine
	sink   billingexport.ObjectStore
	bucket string
	prefix string
	h      EngineHandle
}

// logShipmentsLocked picks the engine-backed instances whose log delivery interval has elapsed.
func (b *InMemoryBackend) logShipmentsLocked(now time.Time) []logShipment {
	if b.engine == nil || b.logSink == nil {
		return nil
	}

	var out []logShipment

	for _, i := range b.instances.All() {
		ld := i.Placement.LogDelivery
		h, live := b.live[i.ID]

		if ld == nil || !ld.Enabled || !live || b.state(i.Pending) != statusAvailable {
			continue
		}

		last := i.LastLogShip
		if last.IsZero() {
			last = i.CreatedAt
		}

		if now.Sub(last) >= logShipInterval {
			i.LastLogShip = now
			out = append(out, logShipment{
				eng: b.engine, sink: b.logSink, h: h, bucket: ld.Bucket, prefix: "timestream-influxdb/" + i.ID,
			})
		}
	}

	return out
}

// shipLogs writes each server's captured output to its bucket; a missing bucket keeps the logs buffered.
func (b *InMemoryBackend) shipLogs(ctx context.Context, ships []logShipment) {
	for _, s := range ships {
		if !billingexport.BucketExists(ctx, s.sink, s.bucket) {
			continue
		}

		data := s.eng.DrainLogs(s.h)
		if len(data) == 0 {
			continue
		}

		key := fmt.Sprintf("%s/%s.log", s.prefix, b.now().UTC().Format("2006/01/02/150405"))
		if err := billingexport.Put(ctx, s.sink, s.bucket, key, data, "text/plain"); err != nil {
			logger.Load(ctx).
				WarnContext(ctx, "timestreaminfluxdb: log delivery failed", "bucket", s.bucket, "error", err)
		}
	}
}
