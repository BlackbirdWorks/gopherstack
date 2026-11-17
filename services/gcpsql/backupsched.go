package gcpsql

import (
	"sort"
	"strconv"
	"time"
)

const (
	backupOnDemand  = "ON_DEMAND"
	backupAutomated = "AUTOMATED"
	maxRetained     = 365
	scheduleEvery   = time.Minute
	hhmmLen         = 5
)

// backupWindowStart returns the start of today's backup window for a settings.backupConfiguration.startTime of
// "HH:MM" (UTC), and whether automated backups are enabled.
func backupWindowStart(settings map[string]any, now time.Time) (time.Time, bool) {
	bc := asMap(settings["backupConfiguration"])
	if on, _ := bc["enabled"].(bool); !on {
		return time.Time{}, false
	}

	hhmm, _ := bc["startTime"].(string)
	if len(hhmm) != hhmmLen || hhmm[2] != ':' {
		hhmm = "00:00"
	}

	h, herr := strconv.Atoi(hhmm[:2])
	m, merr := strconv.Atoi(hhmm[3:])

	if herr != nil || merr != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		h, m = 0, 0
	}

	u := now.UTC()

	return time.Date(u.Year(), u.Month(), u.Day(), h, m, 0, 0, time.UTC), true
}

// retainedBackups is settings.backupConfiguration.backupRetentionSettings.retainedBackups for COUNT retention.
func retainedBackups(settings map[string]any) int {
	rs := asMap(asMap(settings["backupConfiguration"])["backupRetentionSettings"])

	n := int(policyInt(rs["retainedBackups"]))
	if n < 1 {
		return retainDays
	}

	return min(n, maxRetained)
}

// pruneAutomated deletes automated backups beyond the retained count, oldest first; the caller holds b.mu.
func pruneAutomated(rec *instRec) {
	var runs []*backupRec

	for _, r := range rec.Backups {
		if r.Run.Type == backupAutomated && r.Run.Status != "RUNNING" {
			runs = append(runs, r)
		}
	}

	keep := retainedBackups(rec.Inst.Settings)
	if len(runs) <= keep {
		return
	}

	sort.Slice(runs, func(i, j int) bool { return runs[i].Run.ID > runs[j].Run.ID })

	for _, r := range runs[keep:] {
		delete(rec.Backups, r.Run.ID)
	}
}

func (rec *instRec) hasAutomatedSince(window time.Time) bool {
	for _, r := range rec.Backups {
		if r.Run.Type != backupAutomated {
			continue
		}

		if at, err := time.Parse(timeLayout, r.Run.WindowStartTime); err == nil && !at.Before(window) {
			return true
		}
	}

	return false
}

// RunScheduledBackups starts the automated backup of every running primary whose backup window for the day of
// now has begun and has none yet, and returns how many it started.
func (b *InMemoryBackend) RunScheduledBackups(now time.Time) int {
	b.mu.Lock("RunScheduledBackups")
	defer b.mu.Unlock()

	started := 0

	b.sweepBackupsLocked(now)

	for _, rec := range b.instances {
		if rec.gone || rec.Inst.State != stateRunnable || rec.Inst.MasterInstanceName != "" {
			continue
		}

		window, on := backupWindowStart(rec.Inst.Settings, now)
		if !on || now.Before(window) || rec.hasAutomatedSince(window) {
			continue
		}

		b.startBackupLocked(rec, rec.Inst.Project, "", backupAutomated, stamp(window))

		started++
	}

	return started
}

// StartBackupScheduler runs RunScheduledBackups every interval until the backend is closed; zero uses a minute.
func (b *InMemoryBackend) StartBackupScheduler(every time.Duration) {
	if every <= 0 {
		every = scheduleEvery
	}

	b.mu.Lock("StartBackupScheduler")
	defer b.mu.Unlock()

	b.schedEvery = every
	b.startSchedulerLocked()
}

func (b *InMemoryBackend) startSchedulerLocked() {
	if b.schedEvery <= 0 {
		return
	}

	ctx, every := b.runCtx, b.schedEvery

	b.quotaWG.Go(func() {
		tick := time.NewTicker(every)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				b.RunScheduledBackups(b.clockNow())
			}
		}
	})
}

func (b *InMemoryBackend) clockNow() time.Time {
	b.mu.RLock("clockNow")
	defer b.mu.RUnlock()

	return b.now()
}
