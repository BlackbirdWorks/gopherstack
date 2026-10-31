package gcpsql

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const kindLatestRecovery = "sql#getLatestRecoveryTime"

// RecoveryTimes is the sql.projects.instances.getLatestRecoveryTime response.
type RecoveryTimes struct {
	Kind                 string `json:"kind"`
	EarliestRecoveryTime string `json:"earliestRecoveryTime"`
	LatestRecoveryTime   string `json:"latestRecoveryTime"`
}

func pitrOn(version string, settings map[string]any) bool {
	bc := asMap(settings["backupConfiguration"])

	key := "binaryLogEnabled"
	if isPG(version) {
		key = "pointInTimeRecoveryEnabled"
	}

	v, _ := bc[key].(bool)

	return v
}

// syncPITR starts or stops log archiving so the engine matches the instance's PITR setting.
func (b *InMemoryBackend) syncPITR(ctx context.Context, rec *instRec, eng Engine) error {
	b.mu.RLock("syncPITR")
	on := pitrOn(rec.Inst.DatabaseVersion, rec.Inst.Settings) && !b.isReplica(rec)
	had := rec.PitrBase != ""
	b.mu.RUnlock()

	rc, ok := eng.(Recoverer)

	switch {
	case !on && had && ok:
		if err := rc.DisablePITR(ctx); err != nil {
			return err
		}

		b.setPitrBase(rec, time.Time{})
	case !on:
		b.setPitrBase(rec, time.Time{})
	case eng == nil || !ok:
		if !had {
			b.setPitrBase(rec, b.now())
		}
	default:
		t, err := rc.EnablePITR(ctx, b.exportableDBs(rec, nil))
		if errors.Is(err, errNoPITR) {
			logger.Load(ctx).WarnContext(ctx, "gcpsql: point-in-time recovery needs volumes, window unavailable",
				"instance", rec.Inst.Name)
			b.setPitrBase(rec, time.Time{})

			return nil
		}

		if err != nil {
			return err
		}

		b.setPitrBase(rec, t)
	}

	return nil
}

func (b *InMemoryBackend) setPitrBase(rec *instRec, t time.Time) {
	b.mu.Lock("setPitrBase")
	defer b.mu.Unlock()

	if t.IsZero() {
		rec.PitrBase = ""

		return
	}

	rec.PitrBase = stamp(t)
}

// LatestRecoveryTime reports the window point-in-time recovery can target.
func (b *InMemoryBackend) LatestRecoveryTime(project, name string) (*RecoveryTimes, error) {
	b.mu.RLock("LatestRecoveryTime")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if rec.PitrBase == "" {
		return nil, errPITRDisabled()
	}

	return &RecoveryTimes{
		Kind: kindLatestRecovery, EarliestRecoveryTime: rec.PitrBase, LatestRecoveryTime: stamp(b.now()),
	}, nil
}

func errPITRDisabled() *APIError {
	return errInvalid("Point-in-time recovery is not enabled for this instance.")
}

// recoveryTarget parses the cloneContext time; ok is false for a plain clone.
func recoveryTarget(in CloneInput) (time.Time, bool, error) {
	if in.PointInTime != "" {
		t, err := time.Parse(time.RFC3339Nano, in.PointInTime)
		if err != nil {
			return time.Time{}, false, errInvalid("Failed to parse date/time %q; use RFC 3339 in UTC.", in.PointInTime)
		}

		return t.UTC(), true, nil
	}

	if in.PitrTimestampMs != "" && in.PitrTimestampMs != "0" {
		ms, err := strconv.ParseInt(in.PitrTimestampMs, 10, 64)
		if err != nil {
			return time.Time{}, false, errInvalid("Invalid pitrTimestampMs %q.", in.PitrTimestampMs)
		}

		return time.UnixMilli(ms).UTC(), true, nil
	}

	return time.Time{}, false, nil
}

// checkRecoveryLocked rejects times outside the instance's recovery window.
func (b *InMemoryBackend) checkRecoveryLocked(src *instRec, at time.Time) error {
	if b.isReplica(src) || src.PitrBase == "" {
		return errPITRDisabled()
	}

	earliest, err := time.Parse(timeLayout, src.PitrBase)
	if err != nil {
		return errPITRDisabled()
	}

	if latest := b.now(); at.After(latest) {
		return errInvalid("The timestamp for point-in-time recovery is after the latest recovery time of %s.",
			stamp(latest))
	}

	if at.Before(earliest) {
		return errInvalid("The timestamp for point-in-time recovery is before the earliest recovery time of %s.",
			src.PitrBase)
	}

	return nil
}

func (b *InMemoryBackend) populateRecovery(ctx context.Context, src, dst *instRec, at time.Time) error {
	if srcEng := b.engineOf(src); srcEng != nil {
		rc, ok := srcEng.(Recoverer)
		if !ok {
			return errNoPITR
		}

		r, err := rc.PrepareRecovery(ctx, at)
		if err != nil {
			return err
		}

		b.mu.Lock("populateRecovery")
		dst.recovery = &r
		b.mu.Unlock()
	}

	return b.bringUp(ctx, dst)
}

// alignDatabases makes dst's database records match what the engine holds.
func (b *InMemoryBackend) alignDatabases(ctx context.Context, rec *instRec, eng Engine) error {
	names, err := eng.ListDatabases(ctx)
	if err != nil {
		return err
	}

	b.mu.Lock("alignDatabases")
	defer b.mu.Unlock()

	for name, d := range rec.Databases {
		if !d.System && !contains(names, name) {
			delete(rec.Databases, name)
		}
	}

	for _, n := range names {
		if _, ok := rec.Databases[n]; !ok {
			rec.Databases[n] = newDatabase(rec, n, "", "", isSystemDatabase(rec.Inst.DatabaseVersion, n))
		}
	}

	return nil
}
