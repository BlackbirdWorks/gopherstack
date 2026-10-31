package azurearm

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	aksMaintType         = "maintenanceConfigurations"
	aksMaintDefault      = "default"
	aksMaintAutoUpg      = "aksManagedAutoUpgradeSchedule"
	aksMaintNodeOS       = "aksManagedNodeOSUpgradeSchedule"
	aksMaintMinHours     = 4
	aksMaintMaxHours     = 24
	aksMaintMaxHourIdx   = 23
	aksMaintMaxMinute    = 59
	aksMaintMaxOffset    = 14
	aksMaintMaxDay       = 31
	aksMaxIntervalDays   = 7
	aksMaxIntervalWeeks  = 4
	aksMaxIntervalMonths = 6
	aksSchedDaily        = "daily"
	aksSchedWeekly       = "weekly"
	aksSchedAbsMonthly   = "absoluteMonthly"
	aksSchedRelMonthly   = "relativeMonthly"
	aksMaintDefaultOff   = "+00:00"
	aksFieldMaintWin     = "maintenanceWindow"
	aksFieldInterval     = "intervalMonths"
	aksFieldDayOfWeek    = "dayOfWeek"
	aksFieldStartDate    = "startDate"
	aksFieldStart        = "start"
	aksFieldEnd          = "end"
	aksFieldTimeInWeek   = "timeInWeek"
	aksFieldNotAllowed   = "notAllowedTime"
)

//nolint:gochecknoglobals // static read-only catalog
var (
	aksMaintNames = []string{aksMaintDefault, aksMaintAutoUpg, aksMaintNodeOS}
	aksWeekDays   = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	aksWeekIndex  = []string{"First", "Second", "Third", "Fourth", "Last"}
	aksClockRe    = regexp.MustCompile(`^(\d{2}):(\d{2})$`)
	aksOffsetRe   = regexp.MustCompile(`^(-|\+)(\d{2}):(\d{2})$`)
)

type storedAKSMaint struct {
	Created  time.Time      `json:"created"`
	Modified time.Time      `json:"modified"`
	Props    map[string]any `json:"props"`
	Name     string         `json:"name"`
}

func (c *storedAKSCluster) childID(kind, name string) string {
	return c.armID() + "/" + kind + "/" + name
}

func (c *storedAKSCluster) maintBody(m *storedAKSMaint) map[string]any {
	return map[string]any{
		"id": c.childID(aksMaintType, m.Name), fieldName: m.Name,
		fieldType:          namespaceMicrosoftContainerService + "/" + aksClusterType + "/" + aksMaintType,
		fieldProperties:    acaClone(m.Props),
		acaFieldSystemData: acaSystemData(m.Created, m.Modified),
	}
}

func (p *ContainerServiceProvider) putMaint(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putMaint")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	name, ok := canonicalIn(aksMaintNames, id.Names[1])
	if !ok {
		return nil, aksBad("Maintenance configuration name '%s' is not supported: use '%s', '%s' or '%s'.",
			id.Names[1], aksMaintDefault, aksMaintAutoUpg, aksMaintNodeOS)
	}

	now := p.now().UTC()

	props, err := aksCheckMaint(name, mapOf(body[fieldProperties]), now)
	if err != nil {
		return nil, err
	}

	p.mu.Lock("putMaint")
	defer p.mu.Unlock()

	if c.Maint == nil {
		c.Maint = map[string]*storedAKSMaint{}
	}

	rec := &storedAKSMaint{Name: name, Props: props, Created: now, Modified: now}
	if old := c.Maint[strings.ToLower(name)]; old != nil {
		rec.Created = old.Created
	}

	c.Maint[strings.ToLower(name)] = rec

	return c.maintBody(rec), nil
}

func (p *ContainerServiceProvider) getMaint(id ResourceID) (map[string]any, error) {
	p.mu.RLock("getMaint")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	m, found := c.Maint[strings.ToLower(id.Names[1])]
	if !found {
		return nil, ErrResourceNotFound
	}

	return c.maintBody(m), nil
}

func (p *ContainerServiceProvider) listMaint(id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("listMaint")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	out := []map[string]any{}
	for _, k := range sortedKeys(c.Maint) {
		out = append(out, c.maintBody(c.Maint[k]))
	}

	return out, nil
}

func (p *ContainerServiceProvider) deleteMaint(id ResourceID) error {
	p.opMu.Lock("deleteMaint")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteMaint")
	defer p.mu.Unlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return aksNotFound(id.ResourceGroup, id.Names[0])
	}

	key := strings.ToLower(id.Names[1])
	if _, found := c.Maint[key]; !found {
		return ErrResourceNotFound
	}

	delete(c.Maint, key)

	return nil
}

// aksCheckMaint validates a maintenance configuration against the 2026-08-01 schema and the schedule types Learn
// allows per configuration name, and returns the properties as stored.
func aksCheckMaint(name string, props map[string]any, now time.Time) (map[string]any, error) {
	window, tiw, nat := mapOf(props[aksFieldMaintWin]), props[aksFieldTimeInWeek], props[aksFieldNotAllowed]
	out := map[string]any{}

	if name != aksMaintDefault && (tiw != nil || nat != nil) {
		return nil, aksBad("%s only supports maintenanceWindow; timeInWeek and notAllowedTime are for the '%s' "+
			"configuration.", name, aksMaintDefault)
	}

	if window == nil && tiw == nil {
		need := "a maintenanceWindow"
		if name == aksMaintDefault {
			need += " or timeInWeek"
		}

		return nil, aksBad("Maintenance configuration '%s' needs %s.", name, need)
	}

	if window != nil {
		w, err := aksCheckWindow(name, window, now)
		if err != nil {
			return nil, err
		}

		out[aksFieldMaintWin] = w
	}

	if tiw != nil {
		v, err := aksCheckTimeInWeek(tiw)
		if err != nil {
			return nil, err
		}

		out[aksFieldTimeInWeek] = v
	}

	if nat != nil {
		v, err := aksCheckNotAllowedTime(nat)
		if err != nil {
			return nil, err
		}

		out[aksFieldNotAllowed] = v
	}

	return out, nil
}

func aksCheckWindow(name string, w map[string]any, now time.Time) (map[string]any, error) {
	out, err := aksWindowTimes(w, now)
	if err != nil {
		return nil, err
	}

	hours, ok := aksWhole(w["durationHours"])
	if !ok || hours < aksMaintMinHours || hours > aksMaintMaxHours {
		return nil, aksBad("maintenanceWindow.durationHours is required and must be between %d and %d.",
			aksMaintMinHours, aksMaintMaxHours)
	}

	out["durationHours"] = hours

	sched, err := aksCheckSchedule(name, mapOf(w["schedule"]))
	if err != nil {
		return nil, err
	}

	out["schedule"] = sched

	if raw := acaList(w["notAllowedDates"]); len(raw) > 0 {
		spans, spanErr := aksCheckDateSpans(raw)
		if spanErr != nil {
			return nil, spanErr
		}

		out["notAllowedDates"] = spans
	}

	return out, nil
}

// aksWindowTimes validates startTime, utcOffset and startDate, filling the documented defaults.
func aksWindowTimes(w map[string]any, now time.Time) (map[string]any, error) {
	out := map[string]any{}

	start := strOf(w, "startTime")
	if m := aksClockRe.FindStringSubmatch(start); m == nil || aksAtoi(m[1]) > aksMaintMaxHourIdx ||
		aksAtoi(m[2]) > aksMaintMaxMinute {
		return nil, aksBad("maintenanceWindow.startTime is required and must be a time of day between 00:00 and 23:59.")
	}

	out["startTime"] = start

	off := aksMaintDefaultOff
	if v := strOf(w, "utcOffset"); v != "" {
		m := aksOffsetRe.FindStringSubmatch(v)
		if m == nil || aksAtoi(m[2]) > aksMaintMaxOffset || aksAtoi(m[3]) > aksMaintMaxMinute {
			return nil, aksBad("maintenanceWindow.utcOffset '%s' must look like +05:30 or -07:00.", v)
		}

		off = v
	}

	out["utcOffset"] = off

	date := now.Format(time.DateOnly)
	if v := strOf(w, aksFieldStartDate); v != "" {
		if _, err := time.Parse(time.DateOnly, v); err != nil {
			return nil, aksBad("maintenanceWindow.startDate '%s' must be a date like 2026-01-31.", v)
		}

		date = v
	}

	out[aksFieldStartDate] = date

	return out, nil
}

func aksCheckDateSpans(raw []any) ([]any, error) {
	out := make([]any, 0, len(raw))

	for _, it := range raw {
		m := mapOf(it)
		s, serr := time.Parse(time.DateOnly, strOf(m, aksFieldStart))
		e, eerr := time.Parse(time.DateOnly, strOf(m, aksFieldEnd))

		if serr != nil || eerr != nil {
			return nil, aksBad("notAllowedDates entries need start and end dates like 2026-01-31.")
		}

		if e.Before(s) {
			return nil, aksBad("notAllowedDates end '%s' is before its start '%s'.", strOf(m, aksFieldEnd),
				strOf(m, aksFieldStart))
		}

		out = append(out, map[string]any{aksFieldStart: strOf(m, aksFieldStart), aksFieldEnd: strOf(m, aksFieldEnd)})
	}

	return out, nil
}

// aksCheckSchedule enforces exactly one schedule type, limited per configuration name as Learn tabulates:
// default allows only weekly.
func aksCheckSchedule(name string, s map[string]any) (map[string]any, error) {
	var kinds []string

	for _, k := range []string{aksSchedDaily, aksSchedWeekly, aksSchedAbsMonthly, aksSchedRelMonthly} {
		if s[k] != nil {
			kinds = append(kinds, k)
		}
	}

	if len(kinds) != 1 {
		return nil, aksBad("maintenanceWindow.schedule must specify exactly one of daily, weekly, absoluteMonthly or " +
			"relativeMonthly.")
	}

	kind := kinds[0]
	if name == aksMaintDefault && kind != aksSchedWeekly {
		return nil, aksBad("The '%s' maintenance configuration only supports a weekly schedule.", aksMaintDefault)
	}

	body := mapOf(s[kind])
	out := map[string]any{}

	switch kind {
	case aksSchedDaily:
		n, err := aksBounded(body, "intervalDays", "daily", aksMaxIntervalDays)
		out["intervalDays"] = n

		return map[string]any{kind: out}, err
	case aksSchedWeekly:
		wk, err := aksBounded(body, "intervalWeeks", "weekly", aksMaxIntervalWeeks)
		if err != nil {
			return nil, err
		}

		day, err := aksEnum(body, aksFieldDayOfWeek, "weekly", aksWeekDays)
		out["intervalWeeks"], out[aksFieldDayOfWeek] = wk, day

		return map[string]any{kind: out}, err
	case aksSchedAbsMonthly:
		mo, err := aksBounded(body, aksFieldInterval, "absoluteMonthly", aksMaxIntervalMonths)
		if err != nil {
			return nil, err
		}

		dom, err := aksBounded(body, "dayOfMonth", "absoluteMonthly", aksMaintMaxDay)
		out[aksFieldInterval], out["dayOfMonth"] = mo, dom

		return map[string]any{kind: out}, err
	default:
		mo, err := aksBounded(body, aksFieldInterval, "relativeMonthly", aksMaxIntervalMonths)
		if err != nil {
			return nil, err
		}

		idx, err := aksEnum(body, "weekIndex", "relativeMonthly", aksWeekIndex)
		if err != nil {
			return nil, err
		}

		day, err := aksEnum(body, aksFieldDayOfWeek, "relativeMonthly", aksWeekDays)
		out[aksFieldInterval], out["weekIndex"], out[aksFieldDayOfWeek] = mo, idx, day

		return map[string]any{kind: out}, err
	}
}

func aksBounded(m map[string]any, field, sched string, hi int) (int, error) {
	n, ok := aksWhole(m[field])
	if !ok || n < 1 || n > hi {
		return 0, aksBad("schedule.%s.%s is required and must be between 1 and %d.", sched, field, hi)
	}

	return n, nil
}

func aksEnum(m map[string]any, field, sched string, allowed []string) (string, error) {
	v, ok := canonicalIn(allowed, strOf(m, field))
	if !ok {
		return "", aksBad("schedule.%s.%s is required and must be one of %s.", sched, field,
			strings.Join(allowed, ", "))
	}

	return v, nil
}

func aksCheckTimeInWeek(raw any) ([]any, error) {
	out := []any{}

	for _, it := range acaList(raw) {
		m := mapOf(it)

		day, err := aksEnum(m, "day", aksFieldTimeInWeek, aksWeekDays)
		if err != nil {
			return nil, err
		}

		slots := []any{}

		for _, h := range acaList(m["hourSlots"]) {
			n, ok := aksWhole(h)
			if !ok || n < 0 || n > aksMaintMaxHourIdx {
				return nil, aksBad("timeInWeek.hourSlots entries must be hours between 0 and %d.", aksMaintMaxHourIdx)
			}

			slots = append(slots, n)
		}

		out = append(out, map[string]any{"day": day, "hourSlots": slots})
	}

	return out, nil
}

func aksCheckNotAllowedTime(raw any) ([]any, error) {
	out := []any{}

	for _, it := range acaList(raw) {
		m := mapOf(it)
		s, serr := time.Parse(time.RFC3339, strOf(m, aksFieldStart))
		e, eerr := time.Parse(time.RFC3339, strOf(m, aksFieldEnd))

		if serr != nil || eerr != nil {
			return nil, aksBad("notAllowedTime entries need start and end timestamps like 2026-01-31T10:00:00Z.")
		}

		if e.Before(s) {
			return nil, aksBad("notAllowedTime end '%s' is before its start '%s'.", strOf(m, aksFieldEnd),
				strOf(m, aksFieldStart))
		}

		out = append(out, map[string]any{aksFieldStart: strOf(m, aksFieldStart), aksFieldEnd: strOf(m, aksFieldEnd)})
	}

	return out, nil
}

// aksWhole reads a JSON number that must be a whole number.
func aksWhole(v any) (int, bool) {
	f, ok := v.(float64)
	if !ok || f != math.Trunc(f) || math.Abs(f) > math.MaxInt32 {
		return 0, false
	}

	return int(f), true
}

func aksAtoi(s string) int {
	n, _ := strconv.Atoi(s)

	return n
}
