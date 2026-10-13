package lightsail

import (
	"strconv"
	"strings"
	"time"
)

const (
	minutesPerDay          = 24 * 60
	minutesPerWeek         = 7 * minutesPerDay
	minWindowMinutes       = 30
	minMasterPasswordLen   = 8
	maxMySQLPasswordLen    = 41
	maxOtherPasswordLen    = 128
	maintenanceWindowParts = 2
	backupWindowFields     = 2
	timeFieldParts         = 2
	maintenanceFieldParts  = 3
	maxHour                = 23
	maxMinute              = 59
)

var weekdayIndex = map[string]int{ //nolint:gochecknoglobals // static lookup table
	"sun": int(time.Sunday), "mon": int(time.Monday), "tue": int(time.Tuesday), "wed": int(time.Wednesday),
	"thu": int(time.Thursday), "fri": int(time.Friday), "sat": int(time.Saturday),
}

// PendingDatabaseChanges holds UpdateRelationalDatabase changes deferred to the
// next maintenance window.
type PendingDatabaseChanges struct {
	ApplyAt                 time.Time
	MasterUserPasswordSetAt time.Time
	BackupRetentionEnabled  *bool
	MasterUserPassword      string
	BlueprintID             string
	EngineVersion           string
}

func parseClock(s string) (int, bool) {
	parts := strings.Split(s, ":")
	if len(parts) != timeFieldParts {
		return 0, false
	}

	h, errH := strconv.Atoi(parts[0])
	m, errM := strconv.Atoi(parts[1])

	if errH != nil || errM != nil || h < 0 || h > maxHour || m < 0 || m > maxMinute ||
		len(parts[0]) != 2 || len(parts[1]) != 2 {
		return 0, false
	}

	return h*60 + m, true
}

func parseWeekClock(s string) (int, bool) {
	parts := strings.SplitN(s, ":", maintenanceFieldParts)
	if len(parts) != maintenanceFieldParts {
		return 0, false
	}

	day, ok := weekdayIndex[strings.ToLower(parts[0])]
	if !ok {
		return 0, false
	}

	clock, ok := parseClock(parts[1] + ":" + parts[2])
	if !ok {
		return 0, false
	}

	return day*minutesPerDay + clock, true
}

// parseMaintenanceWindow parses ddd:hh24:mi-ddd:hh24:mi into start minute-of-week
// and length in minutes.
func parseMaintenanceWindow(s string) (int, int, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != maintenanceWindowParts {
		return 0, 0, false
	}

	start, ok1 := parseWeekClock(parts[0])
	end, ok2 := parseWeekClock(parts[1])

	if !ok1 || !ok2 {
		return 0, 0, false
	}

	length := (end - start + minutesPerWeek) % minutesPerWeek

	return start, length, true
}

func parseBackupWindow(s string) (int, int, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != backupWindowFields {
		return 0, 0, false
	}

	start, ok1 := parseClock(parts[0])
	end, ok2 := parseClock(parts[1])

	if !ok1 || !ok2 {
		return 0, 0, false
	}

	length := (end - start + minutesPerDay) % minutesPerDay

	return start, length, true
}

// weekSegments splits the circular week interval [start, start+length) into
// linear segments.
func weekSegments(start, length int) [][2]int {
	end := start + length
	if end <= minutesPerWeek {
		return [][2]int{{start, end}}
	}

	return [][2]int{{start, minutesPerWeek}, {0, end - minutesPerWeek}}
}

func windowsOverlap(backup, maintenance string) bool {
	bs, bl, ok1 := parseBackupWindow(backup)
	ms, ml, ok2 := parseMaintenanceWindow(maintenance)

	if !ok1 || !ok2 {
		return false
	}

	for day := range 7 {
		for _, a := range weekSegments((day*minutesPerDay+bs)%minutesPerWeek, bl) {
			for _, m := range weekSegments(ms, ml) {
				if a[0] < m[1] && m[0] < a[1] {
					return true
				}
			}
		}
	}

	return false
}

func validateDatabaseWindows(backup, maintenance string) error {
	if backup != "" {
		if _, length, ok := parseBackupWindow(backup); !ok || length < minWindowMinutes {
			return validationError("preferredBackupWindow must be hh24:mi-hh24:mi and at least 30 minutes")
		}
	}

	if maintenance != "" {
		if _, length, ok := parseMaintenanceWindow(maintenance); !ok || length < minWindowMinutes {
			return validationError("preferredMaintenanceWindow must be ddd:hh24:mi-ddd:hh24:mi and at least 30 minutes")
		}
	}

	if backup != "" && maintenance != "" && windowsOverlap(backup, maintenance) {
		return validationError("preferredBackupWindow must not conflict with preferredMaintenanceWindow")
	}

	return nil
}

// validateMasterPassword applies the documented constraints: 8-41 characters for
// MySQL (8-128 otherwise), printable ASCII excluding "/", """ and "@".
func validateMasterPassword(engine, password string) error {
	maxLen := maxOtherPasswordLen
	if engine == "mysql" {
		maxLen = maxMySQLPasswordLen
	}

	if len(password) < minMasterPasswordLen || len(password) > maxLen {
		return validationError("masterUserPassword length is out of range for engine " + engine)
	}

	for _, r := range password {
		if r < ' ' || r > '~' || r == '/' || r == '"' || r == '@' {
			return validationError(`masterUserPassword may only contain printable ASCII except "/", """ and "@"`)
		}
	}

	return nil
}

// nextWindowStart returns the first start of the weekly maintenance window
// strictly after now.
func nextWindowStart(now time.Time, window string) time.Time {
	start, _, ok := parseMaintenanceWindow(window)
	if !ok {
		return now
	}

	nowMin := int(now.Weekday())*minutesPerDay + now.Hour()*60 + now.Minute()
	delta := (start - nowMin + minutesPerWeek) % minutesPerWeek

	if delta == 0 {
		delta = minutesPerWeek
	}

	base := now.Truncate(time.Minute)

	return base.Add(time.Duration(delta) * time.Minute)
}

// applyDue promotes pending changes whose window has started. Mutates r.
func (r *RelationalDatabase) applyDue(now time.Time) {
	p := r.Pending
	if p == nil || now.Before(p.ApplyAt) {
		return
	}

	if p.BackupRetentionEnabled != nil {
		if r.BackupRetentionEnabled && !*p.BackupRetentionEnabled {
			r.LatestRestorableTime = p.ApplyAt
		}

		r.BackupRetentionEnabled = *p.BackupRetentionEnabled
	}

	if p.MasterUserPassword != "" {
		r.setMasterPassword(p.MasterUserPassword, p.MasterUserPasswordSetAt)
	}

	if p.EngineVersion != "" {
		r.EngineVersion = p.EngineVersion
		r.BlueprintID = p.BlueprintID
	}

	r.Pending = nil
}

func (r *RelationalDatabase) setMasterPassword(password string, at time.Time) {
	r.PreviousMasterUserPassword = r.MasterUserPassword
	r.PreviousMasterUserPasswordSetAt = r.MasterUserPasswordSetAt
	r.MasterUserPassword = password
	r.MasterUserPasswordSetAt = at
}

func (r *RelationalDatabase) pending() *PendingDatabaseChanges {
	if r.Pending == nil {
		r.Pending = &PendingDatabaseChanges{}
	}

	return r.Pending
}
