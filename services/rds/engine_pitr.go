package rds

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	defaultRestorePointInterval = 5 * time.Minute
	maxRestorePoints            = 48
	restorePointPrefix          = "p:"
	hoursPerDay                 = 24
)

// restorePoint is a logical dump of a unit taken at a known time; a restore picks the closest one at or before
// the requested time.
type restorePoint struct {
	at   time.Time
	data engineData
}

// restoreWindow bounds the times a point-in-time restore may target.
type restoreWindow struct {
	earliest time.Time
	latest   time.Time
}

// restoreRequest is the time a point-in-time restore targets; the zero value means the latest state.
type restoreRequest struct {
	at     time.Time
	latest bool
}

func (r restoreRequest) explicit() bool { return !r.at.IsZero() }

func (e *dbEngine) now() time.Time { return e.cfg.Now().UTC() }

func restorePointKey(unitKey string, at time.Time) string {
	return restorePointPrefix + unitKey + "@" + strconv.FormatInt(at.UnixNano(), 10)
}

func parseRestorePointKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, restorePointPrefix)
	if !ok {
		return "", false
	}

	unit, _, found := strings.Cut(rest, "@")

	return unit, found
}

// loadDumps restores snapshot content and restore points persisted by a previous run.
func (e *dbEngine) loadDumps() {
	for _, rec := range e.store.load() {
		d := engineData{dump: rec.Dump, password: rec.Password, dbName: rec.DBName, captured: true}

		if unit, ok := parseRestorePointKey(rec.Key); ok {
			e.points[unit] = append(e.points[unit], restorePoint{at: rec.At, data: d})

			continue
		}

		e.data[rec.Key] = d
	}

	for unit := range e.points {
		pts := e.points[unit]
		slices.SortFunc(pts, func(a, b restorePoint) int { return a.at.Compare(b.at) })

		if n := len(pts); n > 0 && pts[n-1].at.After(e.checked[unit]) {
			e.checked[unit] = pts[n-1].at
		}
	}
}

func (b *InMemoryBackend) startRestorePointLoop() {
	e := b.engine
	if e.cfg.RestorePointInterval < 0 {
		return
	}

	interval := e.cfg.RestorePointInterval
	if interval == 0 {
		interval = defaultRestorePointInterval
	}

	stop := b.stopCh

	e.wg.Go(func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			select {
			case <-stop:
				return
			case <-t.C:
				b.recordRestorePoints()
			}
		}
	})
}

// recordRestorePoints dumps every ready unit that has automated backups enabled.
func (b *InMemoryBackend) recordRestorePoints() {
	b.mu.RLock("rdsRestorePointTargets")

	var lbs []*liveDB

	if b.engine != nil {
		for _, lb := range b.engine.units {
			lbs = append(lbs, lb)
		}
	}

	b.mu.RUnlock()

	for _, lb := range lbs {
		b.recordRestorePoint(lb)
	}
}

func (b *InMemoryBackend) restorePointTarget(lb *liveDB, force bool) (unitInfo, bool) {
	b.mu.RLock("rdsRestorePointTarget")
	defer b.mu.RUnlock()

	e := b.engine
	if e == nil || e.execBroken.Load() || !b.attachedLocked(lb) || !lb.ready || lb.containerID == "" {
		return unitInfo{}, false
	}

	if _, can := e.execer(); !can {
		return unitInfo{}, false
	}

	if b.unitRetentionLocked(lb) <= 0 && !force && len(e.points[lb.key]) == 0 {
		return unitInfo{}, false
	}

	return b.unitInfoLocked(lb), true
}

// recordRestorePoint dumps lb now and keeps the result as a restore point (or just extends the previous one
// when nothing changed).
func (b *InMemoryBackend) recordRestorePoint(lb *liveDB) { b.captureRestorePoint(lb, false) }

// recordRestorePointForced captures a restore point even when automated backups are off, so a promoted replica
// keeps its own data across a restart.
func (b *InMemoryBackend) recordRestorePointForced(lb *liveDB) { b.captureRestorePoint(lb, true) }

// captureRestorePoint is serialized per unit: a caller that dedupes against an in-flight capture must not
// return before that capture is persisted.
func (b *InMemoryBackend) captureRestorePoint(lb *liveDB, force bool) {
	if lb.capture != nil {
		select {
		case lb.capture <- struct{}{}:
			defer func() { <-lb.capture }()
		case <-lb.gone:
			return
		}
	}

	in, ok := b.restorePointTarget(lb, force)
	if !ok {
		return
	}

	ctx, cancel := lb.opContext(b.engine.cfg.StartTimeout)
	defer cancel()

	dump, err := b.engine.dumpUnit(ctx, in)
	if errors.Is(err, container.ErrExecUnsupported) {
		b.engine.execBroken.Store(true)

		return
	}

	if err != nil {
		logger.Load(ctx).DebugContext(ctx, "rds: restore point dump failed", "unit", lb.key, "error", err)

		return
	}

	d := engineData{dump: dump, password: in.login.Password, dbName: in.login.DBName, captured: true}

	added, dropped := b.appendRestorePoint(lb, d)
	if added != nil {
		if serr := b.engine.store.save(restorePointKey(lb.key, added.at), added.at, added.data); serr != nil {
			logger.Load(ctx).WarnContext(ctx, "rds: persisting restore point failed", "unit", lb.key, "error", serr)
		}
	}

	for _, k := range dropped {
		b.engine.store.remove(k)
	}
}

// appendRestorePoint records d for lb; added is nil when d matches the previous point.
func (b *InMemoryBackend) appendRestorePoint(lb *liveDB, d engineData) (*restorePoint, []string) {
	b.mu.Lock("rdsRestorePoint")
	defer b.mu.Unlock()

	e := b.engine
	if !b.attachedLocked(lb) {
		return nil, nil
	}

	now := e.now()
	e.checked[lb.key] = now

	pts := e.points[lb.key]
	if n := len(pts); n > 0 && sameDump(pts[n-1].data.dump, d.dump) && pts[n-1].data.password == d.password {
		return nil, nil
	}

	p := restorePoint{at: now, data: d}
	pts = append(pts, p)

	kept, dropped := prunePoints(pts, now, b.unitRetentionLocked(lb))
	e.points[lb.key] = kept

	keys := make([]string, 0, len(dropped))
	for _, dp := range dropped {
		keys = append(keys, restorePointKey(lb.key, dp.at))
	}

	return &p, keys
}

// sameDump compares two dumps; SQL Server backups embed timestamps, so they compare by their fingerprint line.
func sameDump(a, b string) bool {
	if strings.HasPrefix(a, sqlServerFingerprint) && strings.HasPrefix(b, sqlServerFingerprint) {
		fa, _, _ := strings.Cut(a, "\n")
		fb, _, _ := strings.Cut(b, "\n")

		return fa == fb
	}

	return a == b
}

// prunePoints drops points older than the retention window (keeping the one that still covers its start) and
// caps the history length.
func prunePoints(pts []restorePoint, now time.Time, retentionDays int) ([]restorePoint, []restorePoint) {
	start := 0

	if retentionDays > 0 {
		cutoff := now.Add(-time.Duration(retentionDays) * hoursPerDay * time.Hour)

		i := slices.IndexFunc(pts, func(p restorePoint) bool { return !p.at.Before(cutoff) })
		if i < 0 {
			i = len(pts)
		}

		start = max(0, i-1)
	}

	start = max(start, len(pts)-maxRestorePoints)

	return pts[start:], pts[:start]
}

func (b *InMemoryBackend) unitRetentionLocked(lb *liveDB) int {
	if strings.HasPrefix(lb.key, unitInstancePrefix) {
		if inst, ok := b.instances.Get(normalizeID(lb.resourceID)); ok {
			return inst.BackupRetentionPeriod
		}

		return 0
	}

	if c, ok := b.clusters.Get(normalizeID(lb.resourceID)); ok {
		return c.BackupRetentionPeriod
	}

	return 0
}

func (b *InMemoryBackend) clockNow() time.Time {
	if b.engine != nil {
		return b.engine.now()
	}

	return time.Now().UTC()
}

// dumpsRestorable reports whether lb's history is built from real dumps rather than creation time.
func (b *InMemoryBackend) dumpsRestorable(lb *liveDB) bool {
	e := b.engine
	if e == nil || lb == nil || e.execBroken.Load() {
		return false
	}

	_, can := e.execer()

	return can
}

// restoreWindowLocked is the range of restorable times for the resource behind lb (nil for metadata-only).
func (b *InMemoryBackend) restoreWindowLocked(
	lb *liveDB, created time.Time, retention int,
) (restoreWindow, error) {
	now := b.clockNow()
	cutoff := now.Add(-time.Duration(retention) * hoursPerDay * time.Hour)

	if !b.dumpsRestorable(lb) {
		return restoreWindow{earliest: maxTime(created, cutoff).Truncate(time.Second), latest: now}, nil
	}

	pts := b.engine.points[lb.key]
	if len(pts) == 0 {
		return restoreWindow{}, fmt.Errorf("%w: no restorable data has been captured yet", ErrInvalidRestore)
	}

	return restoreWindow{
		earliest: maxTime(pts[0].at, cutoff).Truncate(time.Second),
		latest:   maxTime(b.engine.checked[lb.key], pts[len(pts)-1].at),
	}, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}

	return b
}

// restoreSeedLocked resolves the data a point-in-time restore of lb loads, validating the requested time.
func (b *InMemoryBackend) restoreSeedLocked(
	lb *liveDB, req restoreRequest, created time.Time, retention int,
) (engineSeed, error) {
	if !req.explicit() && !req.latest {
		return liveSeedLocked(lb), nil
	}

	if retention <= 0 {
		return engineSeed{}, fmt.Errorf("%w: automated backups are not enabled on the source",
			ErrPointInTimeRestoreNotEnabled)
	}

	if req.latest {
		return liveSeedLocked(lb), nil
	}

	w, err := b.restoreWindowLocked(lb, created, retention)
	if err != nil {
		return engineSeed{}, err
	}

	if req.at.Before(w.earliest) || req.at.After(w.latest) {
		return engineSeed{}, fmt.Errorf("%w: restore time %s is outside the restorable range %s to %s",
			ErrInvalidRestore, rfc(req.at), rfc(w.earliest), rfc(w.latest))
	}

	if !b.dumpsRestorable(lb) {
		return engineSeed{}, nil
	}

	return b.seedAtLocked(lb, req.at)
}

func (b *InMemoryBackend) seedAtLocked(lb *liveDB, at time.Time) (engineSeed, error) {
	var found *restorePoint

	pts := b.engine.points[lb.key]
	for i := range pts {
		if pts[i].at.After(at) {
			break
		}

		found = &pts[i]
	}

	if found == nil {
		return engineSeed{}, fmt.Errorf("%w: no restorable data at %s", ErrInvalidRestore, at.Format(time.RFC3339))
	}

	d := found.data

	return engineSeed{dump: d.dump, password: d.password, dbName: d.dbName}, nil
}

// latestPointSeedLocked is the seed that brings a relaunched unit back to its last captured state.
func (b *InMemoryBackend) latestPointSeedLocked(unitKey string) engineSeed {
	if b.engine == nil {
		return engineSeed{}
	}

	pts := b.engine.points[unitKey]
	if len(pts) == 0 {
		return engineSeed{}
	}

	d := pts[len(pts)-1].data

	return engineSeed{dump: d.dump, password: d.password, dbName: d.dbName}
}

func (b *InMemoryBackend) dropPointsLocked(unitKey string) {
	e := b.engine
	if e == nil {
		return
	}

	for _, p := range e.points[unitKey] {
		e.store.remove(restorePointKey(unitKey, p.at))
	}

	delete(e.points, unitKey)
	delete(e.checked, unitKey)
}

// overlayInstanceRestorableRLocked fills the reported LatestRestorableTime on a describe copy.
func (b *InMemoryBackend) overlayInstanceRestorableRLocked(inst *DBInstance) {
	if inst.DBClusterIdentifier != "" || inst.BackupRetentionPeriod <= 0 {
		return
	}

	w, err := b.restoreWindowLocked(b.instanceUnitLocked(inst), inst.InstanceCreateTime, inst.BackupRetentionPeriod)
	if err == nil {
		inst.LatestRestorableTime = w.latest
	}
}

// overlayClusterRestorableRLocked fills the reported restorable range on a describe copy.
func (b *InMemoryBackend) overlayClusterRestorableRLocked(c *DBCluster) {
	if c.BackupRetentionPeriod <= 0 {
		return
	}

	lb := b.clusterUnitLocked(c.DBClusterIdentifier)

	w, err := b.restoreWindowLocked(lb, c.ClusterCreateTime, c.BackupRetentionPeriod)
	if err == nil {
		c.EarliestRestorableTime, c.LatestRestorableTime = w.earliest, w.latest
	}
}

func (o DBInstanceOptions) restoreRequest() restoreRequest {
	return restoreRequest{at: o.RestoreTime.UTC(), latest: o.UseLatestRestorableTime}
}

func (o DBClusterOptions) restoreRequest() restoreRequest {
	return restoreRequest{at: o.RestoreTime.UTC(), latest: o.UseLatestRestorableTime}
}

func rfc(t time.Time) string { return t.Format(time.RFC3339) }
