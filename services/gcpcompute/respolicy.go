package gcpcompute

import (
	"regexp"
	"strconv"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collResPolicy  = "resourcePolicies"
	resPolicyReady = "READY"
	defDuration    = "PT1H"
	keepSnapshots  = "KEEP_AUTO_SNAPSHOTS"
	hoursPerDay    = 24
)

var (
	hhmmRE      = regexp.MustCompile(`^(\d{2}):(\d{2})(?:([+-]\d{1,2}))?$`)
	cronFieldRE = regexp.MustCompile(`^[0-9A-Za-z*,/\-?]+$`)
)

const (
	snapStartStep = 4
	daysPerWeek   = 7
)

func validWeekday(d string) bool {
	switch d {
	case "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY":
		return true
	}

	return false
}

func validOnDiskDelete(v string) bool {
	switch v {
	case "APPLY_RETENTION_POLICY", keepSnapshots, "UNSPECIFIED_ON_SOURCE_DISK_DELETE":
		return true
	}

	return false
}

func resPolicySpec() lbSpec[computeapi.ResourcePolicy] {
	return lbSpec[computeapi.ResourcePolicy]{
		coll: collResPolicy, kind: "compute#resourcePolicy", noun: "resource_policy",
		m: func(b *InMemoryBackend) map[string]*computeapi.ResourcePolicy { return b.resPolicies },
		meta: func(v *computeapi.ResourcePolicy) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepResourcePolicy,
	}
}

func resPolicyKind(p *computeapi.ResourcePolicy) string {
	switch {
	case p.SnapshotSchedulePolicy != nil:
		return "snapshotSchedulePolicy"
	case p.InstanceSchedulePolicy != nil:
		return "instanceSchedulePolicy"
	case p.GroupPlacementPolicy != nil:
		return "groupPlacementPolicy"
	case p.DiskConsistencyGroupPolicy != nil:
		return "diskConsistencyGroupPolicy"
	case p.WorkloadPolicy != nil:
		return "workloadPolicy"
	}

	return ""
}

func prepResourcePolicy(_ *InMemoryBackend, _ lbCtx, in *computeapi.ResourcePolicy, _ map[string]any) *APIError {
	n := 0

	for _, set := range []bool{
		in.SnapshotSchedulePolicy != nil, in.InstanceSchedulePolicy != nil, in.GroupPlacementPolicy != nil,
		in.DiskConsistencyGroupPolicy != nil, in.WorkloadPolicy != nil,
	} {
		if set {
			n++
		}
	}

	if n != 1 {
		return errInvalid("Invalid value for field 'resource': exactly one of snapshotSchedulePolicy, " +
			"instanceSchedulePolicy, groupPlacementPolicy, diskConsistencyGroupPolicy or workloadPolicy must be set.")
	}

	in.Status = resPolicyReady

	switch {
	case in.SnapshotSchedulePolicy != nil:
		return validSnapshotPolicy(in.SnapshotSchedulePolicy)
	case in.InstanceSchedulePolicy != nil:
		return validInstanceSchedule(in.InstanceSchedulePolicy)
	case in.GroupPlacementPolicy != nil:
		return validGroupPlacement(in.GroupPlacementPolicy)
	}

	return nil
}

// utcStart parses HH:MM with an optional hour offset (13:00-5) and returns the UTC hour and minute.
func utcStart(field, raw string) (int, int, *APIError) {
	m := hhmmRE.FindStringSubmatch(raw)
	if m == nil {
		return 0, 0, errField(field, raw, "Must be in the format HH:MM.")
	}

	h, _ := strconv.Atoi(m[1])
	mi, _ := strconv.Atoi(m[2])

	if h > hoursPerDay-1 || mi > 59 {
		return 0, 0, errField(field, raw, "Must be in the format HH:MM.")
	}

	if m[3] != "" {
		off, _ := strconv.Atoi(m[3])
		h = ((h+off)%hoursPerDay + hoursPerDay) % hoursPerDay
	}

	return h, mi, nil
}

func normSnapshotStart(field string, start *string) *APIError {
	h, mi, err := utcStart(field, *start)
	if err != nil {
		return err
	}

	if h%snapStartStep != 0 || mi != 0 {
		return errField(field, *start, "Must resolve to one of 00:00, 04:00, 08:00, 12:00, 16:00 or 20:00 UTC.")
	}

	*start = formatHHMM(h)

	return nil
}

func formatHHMM(h int) string { return pad2(h) + ":00" }

func pad2(n int) string {
	if n < 10 { //nolint:mnd // two-digit padding
		return "0" + strconv.Itoa(n)
	}

	return strconv.Itoa(n)
}

func validSnapshotPolicy(sp *computeapi.ResourcePolicySnapshotSchedulePolicy) *APIError {
	const f = "resource.snapshotSchedulePolicy"

	sc := sp.Schedule
	if sc == nil {
		return errRequired(f + ".schedule")
	}

	set := 0

	for _, ok := range []bool{sc.DailySchedule != nil, sc.HourlySchedule != nil, sc.WeeklySchedule != nil} {
		if ok {
			set++
		}
	}

	if set != 1 {
		return errInvalid("Invalid value for field '%s.schedule': exactly one of dailySchedule, hourlySchedule "+
			"and weeklySchedule must be set.", f)
	}

	if err := validCycle(sc, f+".schedule"); err != nil {
		return err
	}

	if r := sp.RetentionPolicy; r != nil {
		if r.MaxRetentionDays < 1 {
			return errField(f+".retentionPolicy.maxRetentionDays", strconv.FormatInt(r.MaxRetentionDays, 10),
				"Must be at least 1.")
		}

		if r.OnSourceDiskDelete == "" {
			r.OnSourceDiskDelete = keepSnapshots
		}

		if !validOnDiskDelete(r.OnSourceDiskDelete) {
			return errField(f+".retentionPolicy.onSourceDiskDelete", r.OnSourceDiskDelete, "")
		}
	}

	if sprops := sp.SnapshotProperties; sprops != nil {
		if err := validLabels(sprops.Labels); err != nil {
			return err
		}

		if len(sprops.StorageLocations) > 1 {
			return errField(f+".snapshotProperties.storageLocations", "", "At most one storage location is allowed.")
		}
	}

	return nil
}

func validCycle(sc *computeapi.ResourcePolicySnapshotSchedulePolicySchedule, f string) *APIError {
	switch {
	case sc.DailySchedule != nil:
		d := sc.DailySchedule
		if d.DaysInCycle < 1 {
			return errField(f+".dailySchedule.daysInCycle", strconv.FormatInt(d.DaysInCycle, 10), "Must be at least 1.")
		}

		d.Duration = defDuration

		return normSnapshotStart(f+".dailySchedule.startTime", &d.StartTime)
	case sc.HourlySchedule != nil:
		hs := sc.HourlySchedule
		if hs.HoursInCycle < 1 || hs.HoursInCycle > hoursPerDay || hoursPerDay%hs.HoursInCycle != 0 {
			return errField(f+".hourlySchedule.hoursInCycle", strconv.FormatInt(hs.HoursInCycle, 10),
				"Must be a divisor of 24.")
		}

		h, mi, err := utcStart(f+".hourlySchedule.startTime", hs.StartTime)
		if err != nil {
			return err
		}

		if mi != 0 {
			return errField(f+".hourlySchedule.startTime", hs.StartTime, "Minutes must be 00.")
		}

		hs.StartTime, hs.Duration = formatHHMM(h), defDuration

		return nil
	}

	return validWeekly(sc.WeeklySchedule, f+".weeklySchedule")
}

func validWeekly(w *computeapi.ResourcePolicyWeeklyCycle, f string) *APIError {
	if len(w.DayOfWeeks) < 1 || len(w.DayOfWeeks) > daysPerWeek {
		return errField(f+".dayOfWeeks", "", "Between 1 and 7 days are required.")
	}

	seen := map[string]bool{}

	for _, d := range w.DayOfWeeks {
		if !validWeekday(d.Day) || seen[d.Day] {
			return errField(f+".dayOfWeeks.day", d.Day, "Must be a unique day from MONDAY to SUNDAY.")
		}

		seen[d.Day] = true
		d.Duration = defDuration

		if err := normSnapshotStart(f+".dayOfWeeks.startTime", &d.StartTime); err != nil {
			return err
		}
	}

	return nil
}

func validCron(field, expr string) *APIError {
	fields := 0

	for _, tok := range splitFields(expr) {
		if !cronFieldRE.MatchString(tok) {
			return errField(field, expr, "Not a valid unix-cron expression.")
		}

		fields++
	}

	if fields != 5 { //nolint:mnd // minute hour day-of-month month day-of-week
		return errField(field, expr, "A unix-cron expression has five fields.")
	}

	return nil
}

func splitFields(s string) []string {
	var out []string

	cur := ""

	for _, c := range s {
		if c == ' ' || c == '\t' {
			if cur != "" {
				out, cur = append(out, cur), ""
			}

			continue
		}

		cur += string(c)
	}

	if cur != "" {
		out = append(out, cur)
	}

	return out
}

func validInstanceSchedule(is *computeapi.ResourcePolicyInstanceSchedulePolicy) *APIError {
	const f = "resource.instanceSchedulePolicy"

	if is.VmStartSchedule == nil && is.VmStopSchedule == nil {
		return errInvalid("Invalid value for field '%s': at least one of vmStartSchedule and vmStopSchedule "+
			"must be set.", f)
	}

	for name, s := range map[string]*computeapi.ResourcePolicyInstanceSchedulePolicySchedule{
		"vmStartSchedule": is.VmStartSchedule, "vmStopSchedule": is.VmStopSchedule,
	} {
		if s == nil {
			continue
		}

		if err := validCron(f+"."+name+".schedule", s.Schedule); err != nil {
			return err
		}
	}

	if is.TimeZone == "" {
		is.TimeZone = "UTC"
	}

	if _, err := time.LoadLocation(is.TimeZone); err != nil {
		return errField(f+".timeZone", is.TimeZone, "Must be a time zone name from the tz database.")
	}

	var start, exp time.Time

	for _, t := range []struct {
		dst *time.Time
		val string
		key string
	}{{&start, is.StartTime, "startTime"}, {&exp, is.ExpirationTime, "expirationTime"}} {
		if t.val == "" {
			continue
		}

		v, err := time.Parse(time.RFC3339, t.val)
		if err != nil {
			return errField(f+"."+t.key, t.val, "Must be an RFC3339 timestamp.")
		}

		*t.dst = v
	}

	if !start.IsZero() && !exp.IsZero() && !exp.After(start) {
		return errField(f+".expirationTime", is.ExpirationTime, "Must be after startTime.")
	}

	return nil
}

func validGroupPlacement(g *computeapi.ResourcePolicyGroupPlacementPolicy) *APIError {
	switch g.Collocation {
	case "", "COLLOCATED", "UNSPECIFIED_COLLOCATION":
	default:
		return errField("resource.groupPlacementPolicy.collocation", g.Collocation, "")
	}

	if g.VmCount < 0 || g.AvailabilityDomainCount < 0 {
		return errInvalid("Invalid value for field 'resource.groupPlacementPolicy': counts cannot be negative.")
	}

	return nil
}
