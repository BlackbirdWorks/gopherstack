package arczonalshift

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	windowRE = regexp.MustCompile(`^(?i)([a-z]{3}):(\d{2}):(\d{2})-([a-z]{3}):(\d{2}):(\d{2})$`)
	dateRE   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

func weekdays() []string { return []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"} }

// PracticeView is the wire form of a practice run configuration.
type PracticeView struct {
	OutcomeAlarms  []Alarm  `json:"outcomeAlarms"`
	AllowedWindows []string `json:"allowedWindows,omitempty"`
	BlockedDates   []string `json:"blockedDates,omitempty"`
	BlockedWindows []string `json:"blockedWindows,omitempty"`
	BlockingAlarms []Alarm  `json:"blockingAlarms,omitempty"`
}

// PracticeOut is the output of the practice run configuration operations.
type PracticeOut struct {
	Config               *PracticeView `json:"practiceRunConfiguration,omitempty"`
	Arn                  string        `json:"arn"`
	Name                 string        `json:"name"`
	ZonalAutoshiftStatus string        `json:"zonalAutoshiftStatus"`
}

// minuteOfWeek parses MON:18:30 style parts to minutes since Monday 00:00.
func minuteOfWeek(day, hh, mm string) (int, bool) {
	d := slices.Index(weekdays(), strings.ToLower(day))
	h, m := atoi(hh), atoi(mm)

	if d < 0 || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}

	return d*24*60 + h*60 + m, true
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}

	return n
}

type window struct{ start, end int }

func parseWindow(s string) (window, bool) {
	m := windowRE.FindStringSubmatch(s)
	if m == nil || !strings.EqualFold(m[1], m[4]) {
		return window{}, false
	}

	start, ok1 := minuteOfWeek(m[1], m[2], m[3])
	end, ok2 := minuteOfWeek(m[4], m[5], m[6])

	return window{start, end}, ok1 && ok2 && end > start
}

func parseWindows(list []string, reason string) ([]window, error) {
	out := make([]window, 0, len(list))

	for _, s := range list {
		w, ok := parseWindow(s)
		if !ok {
			return nil, invalidReason(reason, "%q is not a valid same-day window (expected DAY:HH:MM-DAY:HH:MM)", s)
		}

		out = append(out, w)
	}

	return out, nil
}

func (b *InMemoryBackend) validateAlarms(alarms []Alarm, required bool) error {
	if required && len(alarms) == 0 {
		return invalidReason("InvalidAlarmCondition", "outcomeAlarms must contain an alarm")
	}

	for _, a := range alarms {
		if a.Type != "CLOUDWATCH" {
			return invalidReason("InvalidConditionType", "Alarm type must be CLOUDWATCH")
		}

		if _, found := b.alarmState(a.AlarmIdentifier); !found {
			return invalidReason("InvalidAlarmCondition", "CloudWatch alarm %q does not exist", a.AlarmIdentifier)
		}
	}

	return nil
}

func validateSchedule(c *PracticeConfig) error {
	allowed, err := parseWindows(c.AllowedWindows, "InvalidPracticeAllowedWindow")
	if err != nil {
		return err
	}

	blocked, err := parseWindows(c.BlockedWindows, "InvalidPracticeBlocker")
	if err != nil {
		return err
	}

	for _, d := range c.BlockedDates {
		if _, perr := time.Parse(time.DateOnly, d); perr != nil || !dateRE.MatchString(d) {
			return invalidReason("InvalidPracticeBlocker", "%q is not a valid YYYY-MM-DD date", d)
		}
	}

	for _, a := range allowed {
		for _, bl := range blocked {
			if a.start < bl.end && bl.start < a.end {
				return invalidReason("InvalidPracticeWindows", "An allowed window overlaps a blocked window")
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) practiceOut(m managedResource, cfg *PracticeConfig) PracticeOut {
	out := PracticeOut{Arn: m.ARN, Name: m.Name, ZonalAutoshiftStatus: b.autoshiftStatus(m.ARN)}
	if cfg != nil {
		out.Config = &PracticeView{
			OutcomeAlarms: cfg.OutcomeAlarms, AllowedWindows: cfg.AllowedWindows, BlockedDates: cfg.BlockedDates,
			BlockedWindows: cfg.BlockedWindows, BlockingAlarms: cfg.BlockingAlarms,
		}
	}

	return out
}

func (b *InMemoryBackend) autoshiftStatus(resource string) string {
	if r, ok := b.resources.Get(resource); ok {
		return r.Autoshift
	}

	return autoshiftDisabled
}

// CreatePracticeRunConfiguration defines the practice run schedule and alarms for a managed resource.
func (b *InMemoryBackend) CreatePracticeRunConfiguration(cfg PracticeConfig) (PracticeOut, error) {
	m, err := b.managedFor(cfg.Resource)
	if err != nil {
		return PracticeOut{}, err
	}

	if err = b.validateAlarms(cfg.OutcomeAlarms, true); err != nil {
		return PracticeOut{}, err
	}

	if err = b.validateAlarms(cfg.BlockingAlarms, false); err != nil {
		return PracticeOut{}, err
	}

	if err = validateSchedule(&cfg); err != nil {
		return PracticeOut{}, err
	}

	b.mu.Lock("CreatePracticeRunConfiguration")
	defer b.mu.Unlock()

	if b.configs.Has(cfg.Resource) {
		return PracticeOut{}, conflictReason("PracticeConfigurationAlreadyExists", "",
			"A practice run configuration already exists for %s", cfg.Resource)
	}

	b.configs.Put(&cfg)

	return b.practiceOut(m, &cfg), nil
}

// UpdatePracticeRunConfiguration replaces the supplied parts of a practice run configuration.
func (b *InMemoryBackend) UpdatePracticeRunConfiguration(
	resource string,
	patch PracticeConfig,
	set map[string]bool,
) (PracticeOut, error) {
	m, err := b.managedFor(resource)
	if err != nil {
		return PracticeOut{}, err
	}

	b.mu.Lock("UpdatePracticeRunConfiguration")
	defer b.mu.Unlock()

	cur, ok := b.configs.Get(resource)
	if !ok {
		return PracticeOut{}, conflictReason("PracticeConfigurationDoesNotExist", "",
			"No practice run configuration exists for %s", resource)
	}

	next := *cur
	if set["outcomeAlarms"] {
		next.OutcomeAlarms = patch.OutcomeAlarms
	}

	if set["blockingAlarms"] {
		next.BlockingAlarms = patch.BlockingAlarms
	}

	if set["allowedWindows"] {
		next.AllowedWindows = patch.AllowedWindows
	}

	if set["blockedWindows"] {
		next.BlockedWindows = patch.BlockedWindows
	}

	if set["blockedDates"] {
		next.BlockedDates = patch.BlockedDates
	}

	if err = b.validateAlarms(next.OutcomeAlarms, true); err != nil {
		return PracticeOut{}, err
	}

	if err = b.validateAlarms(next.BlockingAlarms, false); err != nil {
		return PracticeOut{}, err
	}

	if err = validateSchedule(&next); err != nil {
		return PracticeOut{}, err
	}

	b.configs.Put(&next)

	return b.practiceOut(m, &next), nil
}

// DeletePracticeRunConfiguration removes a configuration; zonal autoshift must be disabled first.
func (b *InMemoryBackend) DeletePracticeRunConfiguration(resource string) (PracticeOut, error) {
	m, err := b.managedFor(resource)
	if err != nil {
		return PracticeOut{}, err
	}

	b.mu.Lock("DeletePracticeRunConfiguration")
	defer b.mu.Unlock()

	if !b.configs.Has(resource) {
		return PracticeOut{}, conflictReason("PracticeConfigurationDoesNotExist", "",
			"No practice run configuration exists for %s", resource)
	}

	if b.autoshiftStatus(resource) == autoshiftEnabled {
		return PracticeOut{}, conflictReason("ZonalAutoshiftActive", "",
			"Zonal autoshift is enabled for %s; disable it before deleting the practice run configuration", resource)
	}

	b.configs.Delete(resource)

	return b.practiceOut(m, nil), nil
}

// UpdateZonalAutoshiftConfiguration enables or disables zonal autoshift for a managed resource.
func (b *InMemoryBackend) UpdateZonalAutoshiftConfiguration(resource, status string) (string, error) {
	if status != autoshiftEnabled && status != autoshiftDisabled {
		return "", invalidReason("InvalidStatus", "zonalAutoshiftStatus must be ENABLED or DISABLED")
	}

	if _, err := b.managedFor(resource); err != nil {
		return "", err
	}

	b.mu.Lock("UpdateZonalAutoshiftConfiguration")
	defer b.mu.Unlock()

	if status == autoshiftEnabled && !b.configs.Has(resource) {
		return "", conflictReason("PracticeConfigurationDoesNotExist", "",
			"Zonal autoshift requires a practice run configuration for %s", resource)
	}

	b.resources.Put(&ResourceSettings{Resource: resource, Autoshift: status})

	return status, nil
}

// blocker is the conflict reason (and detail) that forbids starting a practice run now.
type blocker struct{ reason, detail string }

// blockedReason returns the blocker for a practice run starting now; the zero value means allowed.
func (b *InMemoryBackend) blockedReason(cfg *PracticeConfig, now time.Time) blocker {
	for _, a := range cfg.BlockingAlarms {
		if state, found := b.alarmState(a.AlarmIdentifier); found && state == alarmStateAlarm {
			return blocker{"PracticeBlockingAlarmsRed", "blocking alarm " + a.AlarmIdentifier + " is in ALARM"}
		}
	}

	for _, a := range cfg.OutcomeAlarms {
		if state, found := b.alarmState(a.AlarmIdentifier); found && state == alarmStateAlarm {
			return blocker{"PracticeOutcomeAlarmsRed", "outcome alarm " + a.AlarmIdentifier + " is in ALARM"}
		}
	}

	if slices.Contains(cfg.BlockedDates, now.Format(time.DateOnly)) {
		return blocker{"PracticeInBlockedDates", "today is a blocked date"}
	}

	at := minuteOfWeek2(now)

	blocked, _ := parseWindows(cfg.BlockedWindows, "")
	if slices.ContainsFunc(blocked, func(w window) bool { return at >= w.start && at < w.end }) {
		return blocker{"PracticeInBlockedWindows", "the current time is inside a blocked window"}
	}

	allowed, _ := parseWindows(cfg.AllowedWindows, "")
	if len(allowed) > 0 && !slices.ContainsFunc(allowed, func(w window) bool { return at >= w.start && at < w.end }) {
		return blocker{"PracticeOutsideAllowedWindows", "the current time is outside every allowed window"}
	}

	return blocker{}
}

func minuteOfWeek2(t time.Time) int {
	t = t.UTC()
	day := (int(t.Weekday()) + len(weekdays()) - 1) % len(weekdays())

	return day*24*60 + t.Hour()*60 + t.Minute()
}

// StartPracticeRun starts a practice run zonal shift if the practice configuration allows it right now.
func (b *InMemoryBackend) StartPracticeRun(resource, awayFrom, comment string) (ShiftView, error) {
	if resource == "" || awayFrom == "" || comment == "" {
		return ShiftView{}, invalidReason("MissingValue", "awayFrom, comment and resourceIdentifier are required")
	}

	m, err := b.managedFor(resource)
	if err != nil {
		return ShiftView{}, err
	}

	az, ok := m.resolveAZ(awayFrom)
	if !ok {
		return ShiftView{}, invalidReason("InvalidAz", "Availability Zone %q is not used by %s", awayFrom, resource)
	}

	b.mu.Lock("StartPracticeRun")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	cfg, ok := b.configs.Get(resource)
	if !ok {
		return ShiftView{}, conflictReason("PracticeConfigurationDoesNotExist", "",
			"No practice run configuration exists for %s", resource)
	}

	if blk := b.blockedReason(cfg, now); blk.reason != "" {
		return ShiftView{}, conflictReason(
			blk.reason, "", "A practice run cannot start for %s: %s", resource, blk.detail)
	}

	if err = b.checkNoConflict(m, az, now); err != nil {
		return ShiftView{}, err
	}

	s := &Shift{
		ID: uuid.NewString(), Resource: resource, AwayFrom: az, Comment: comment, Type: shiftPractice,
		StartTime: now, ExpiryTime: now.Add(b.practiceDuration), Outcome: outcomePending,
	}
	b.shifts.Put(s)

	return viewOf(s, now), nil
}
