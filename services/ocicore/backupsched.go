package ocicore

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	periodHour      = "ONE_HOUR"
	periodDay       = "ONE_DAY"
	periodWeek      = "ONE_WEEK"
	periodMonth     = "ONE_MONTH"
	periodYear      = "ONE_YEAR"
	sourceScheduled = "SCHEDULED"
	tzRegional      = "REGIONAL_DATA_CENTER_TIME"
	regionalZone    = "America/New_York"
	offsetNumeric   = "NUMERIC_SECONDS"
	hoursPerDay     = 24
	daysPerWeek     = 7
)

func weekdayNames() []string {
	return []string{"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}
}

func monthNames() []string {
	return []string{
		"JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER",
		"NOVEMBER", "DECEMBER",
	}
}

func scheduleZone(s BackupSchedule) *time.Location {
	if s.TimeZone == tzRegional {
		if loc, err := time.LoadLocation(regionalZone); err == nil {
			return loc
		}
	}

	return time.UTC
}

func indexOf(list []string, v string, def int) int {
	for i, x := range list {
		if strings.EqualFold(x, v) {
			return i
		}
	}

	return def
}

func intervalStart(period string, t time.Time) time.Time {
	y, mo, d := t.Date()

	switch period {
	case periodHour:
		return t.Truncate(time.Hour)
	case periodWeek:
		back := (int(t.Weekday()) + daysPerWeek - 1) % daysPerWeek

		return time.Date(y, mo, d-back, 0, 0, 0, 0, t.Location())
	case periodMonth:
		return time.Date(y, mo, 1, 0, 0, 0, 0, t.Location())
	case periodYear:
		return time.Date(y, time.January, 1, 0, 0, 0, 0, t.Location())
	}

	return time.Date(y, mo, d, 0, 0, 0, 0, t.Location())
}

func nextInterval(period string, start time.Time) time.Time {
	switch period {
	case periodHour:
		return start.Add(time.Hour)
	case periodWeek:
		return start.AddDate(0, 0, daysPerWeek)
	case periodMonth:
		return start.AddDate(0, 1, 0)
	case periodYear:
		return start.AddDate(1, 0, 0)
	}

	return start.AddDate(0, 0, 1)
}

func clampDay(start time.Time, day int) time.Time {
	last := start.AddDate(0, 1, -1).Day()
	if day < 1 {
		day = 1
	}

	return start.AddDate(0, 0, min(day, last)-1)
}

// fireIn is the backup time inside the interval that begins at start.
func fireIn(s BackupSchedule, start time.Time) time.Time {
	if s.OffsetType != "STRUCTURED" {
		return start.Add(time.Duration(s.OffsetSeconds) * time.Second)
	}

	hour := time.Duration(s.HourOfDay) * time.Hour

	switch s.Period {
	case periodHour:
		return start
	case periodWeek:
		return start.AddDate(0, 0, indexOf(weekdayNames(), s.DayOfWeek, 0)).Add(hour)
	case periodMonth:
		return clampDay(start, s.DayOfMonth).Add(hour)
	case periodYear:
		m := start.AddDate(0, indexOf(monthNames(), s.Month, 0), 0)

		return clampDay(m, s.DayOfMonth).Add(hour)
	}

	return start.Add(hour)
}

// nextFire is the first fire time of a schedule strictly after t.
func nextFire(s BackupSchedule, t time.Time) time.Time {
	t = t.In(scheduleZone(s))
	start := intervalStart(s.Period, t)

	for {
		if f := fireIn(s, start); f.After(t) {
			return f
		}

		start = nextInterval(s.Period, start)
	}
}

// latestDue is the newest fire time of a schedule in (since, now]; the zero time means none is due.
func latestDue(s BackupSchedule, since, now time.Time) time.Time {
	var due time.Time

	for f := nextFire(s, since); !f.After(now); f = nextFire(s, f) {
		due = f
	}

	return due
}

// sweepBackupSchedules takes the backups of every assigned policy schedule that fell due and ends expired ones.
func (b *InMemoryBackend) sweepBackupSchedules(now time.Time) {
	for _, a := range liveOf(b, func(*BackupPolicyAssignment) bool { return true }) {
		b.runAssignment(a, now)
	}

	for _, r := range b.res {
		if at := r.base().Internal.ExpireAt; at != nil && !at.After(now) && live(r) {
			b.markTerminating(r)
		}
	}
}

func (b *InMemoryBackend) runAssignment(a *BackupPolicyAssignment, now time.Time) {
	pol, aerr := b.findPolicy(a.PolicyID)
	if aerr != nil {
		return
	}

	in := a.Internal

	for i, s := range pol.Schedules {
		since, ok := in.ScheduleRuns[i]
		if !ok {
			since = a.TimeCreated
		}

		due := latestDue(s, since, now)
		if due.IsZero() {
			continue
		}

		if in.ScheduleRuns == nil {
			in.ScheduleRuns = map[int]time.Time{}
		}

		in.ScheduleRuns[i] = due

		if aerr = b.scheduledBackup(a.AssetID, s, now); aerr != nil {
			ctx := context.Background()
			logger.Load(ctx).WarnContext(ctx, "ocicore: scheduled backup skipped", slog.String("asset", a.AssetID),
				slog.String("reason", aerr.Message))
		}
	}
}

func (b *InMemoryBackend) scheduledBackup(assetID string, s BackupSchedule, now time.Time) *APIError {
	var r Resource

	switch ocidKind(assetID) {
	case kindVolume:
		vol, aerr := availableSource[*Volume](b, assetID, "volume")
		if aerr != nil {
			return aerr
		}

		bk, aerr := b.snapshotVolume(vol, CreateVolumeBackupIn{Type: s.BackupType}, vol.CompartmentID)
		if aerr != nil {
			return aerr
		}

		bk.SourceType, r = sourceScheduled, bk
	case kindBootVolume:
		bv, aerr := availableSource[*BootVolume](b, assetID, "boot volume")
		if aerr != nil {
			return aerr
		}

		bk, aerr := b.snapshotBootVolume(bv, CreateBootVolumeBackupIn{Type: s.BackupType}, bv.CompartmentID)
		if aerr != nil {
			return aerr
		}

		bk.SourceType, r = sourceScheduled, bk
	case kindVolumeGroup:
		g, aerr := getLive[*VolumeGroup](b, assetID)
		if aerr != nil {
			return aerr
		}

		gb, aerr := b.snapshotGroup(g, CreateVolumeGroupBackupIn{Type: s.BackupType}, g.CompartmentID)
		if aerr != nil {
			return aerr
		}

		gb.SourceType, r = sourceScheduled, gb
	default:
		return errParam("assetId " + assetID + " is not a volume, boot volume or volume group")
	}

	if s.RetentionSeconds > 0 {
		at := now.Add(time.Duration(s.RetentionSeconds) * time.Second)
		r.base().Internal.ExpireAt = &at
	}

	return nil
}
