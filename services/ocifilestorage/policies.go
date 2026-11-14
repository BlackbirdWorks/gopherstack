package ocifilestorage

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxSchedules   = 10
	periodHourly   = "HOURLY"
	periodDaily    = "DAILY"
	periodWeekly   = "WEEKLY"
	periodMonthly  = "MONTHLY"
	periodYearly   = "YEARLY"
	tzUTC          = "UTC"
	tzRegional     = "REGIONAL_DATA_CENTER_TIME"
	regionalZone   = "America/New_York"
	hoursPerDayInt = 24
	lastHourOfDay  = 23
	maxDayOfMonth  = 31
	searchDays     = 366 * 8
	secondsPerHour = 3600
	secondsPerDay  = 86400
)

//nolint:gochecknoglobals // immutable lookup tables
var (
	weekdays = map[string]time.Weekday{
		"SUNDAY": time.Sunday, "MONDAY": time.Monday, "TUESDAY": time.Tuesday, "WEDNESDAY": time.Wednesday,
		"THURSDAY": time.Thursday, "FRIDAY": time.Friday, "SATURDAY": time.Saturday,
	}
	months = map[string]time.Month{
		"JANUARY": time.January, "FEBRUARY": time.February, "MARCH": time.March, "APRIL": time.April, "MAY": time.May,
		"JUNE": time.June, "JULY": time.July, "AUGUST": time.August, "SEPTEMBER": time.September,
		"OCTOBER": time.October, "NOVEMBER": time.November, "DECEMBER": time.December,
	}
)

type createPolicyIn struct {
	tagsIn
	DisplayName        *string    `json:"displayName"`
	AvailabilityDomain string     `json:"availabilityDomain"`
	CompartmentID      string     `json:"compartmentId"`
	PolicyPrefix       string     `json:"policyPrefix"`
	Schedules          []schedule `json:"schedules"`
	Locks              []lock     `json:"locks"`
}

type updatePolicyIn struct {
	tagsIn
	DisplayName  *string     `json:"displayName"`
	PolicyPrefix *string     `json:"policyPrefix"`
	Schedules    *[]schedule `json:"schedules"`
}

// PolicyFilter selects snapshot policies in a list.
type PolicyFilter = MTFilter

func (b *InMemoryBackend) polView(e *entry[*Policy]) Policy {
	v := *e.W
	v.Int, v.base = nil, cloneBase(e.W.base)
	v.Schedules = slices.Clone(e.W.Schedules)

	return v
}

func polSummary(v Policy) map[string]any {
	return project(v, "availabilityDomain", "compartmentId", "id", "lifecycleState", "timeCreated", "displayName",
		"locks", "policyPrefix", "freeformTags", "definedTags", "systemTags")
}

func scheduleZone(tz string) *time.Location {
	if tz == tzRegional {
		if loc, err := time.LoadLocation(regionalZone); err == nil {
			return loc
		}
	}

	return time.UTC
}

// normalizeSchedules validates schedules and fills the fields the service chooses when they are unset.
func normalizeSchedules(in []schedule, now time.Time) ([]schedule, *APIError) {
	if len(in) > maxSchedules {
		return nil, errParam("a policy has at most 10 schedules")
	}

	out := make([]schedule, 0, len(in))

	for _, s := range in {
		n, aerr := normalizeSchedule(s, now)
		if aerr != nil {
			return nil, aerr
		}

		out = append(out, n)
	}

	return out, nil
}

func normalizeSchedule(s schedule, now time.Time) (schedule, *APIError) {
	if !slices.Contains([]string{periodHourly, periodDaily, periodWeekly, periodMonthly, periodYearly}, s.Period) {
		return s, errParam("schedule period must be HOURLY, DAILY, WEEKLY, MONTHLY or YEARLY")
	}

	if s.TimeZone == "" {
		s.TimeZone = tzUTC
	}

	if s.TimeZone != tzUTC && s.TimeZone != tzRegional {
		return s, errParam("schedule timeZone must be UTC or REGIONAL_DATA_CENTER_TIME")
	}

	if s.TimeScheduleStart == nil {
		start := now
		s.TimeScheduleStart = &start
	}

	if s.RetentionDurationInSeconds != nil && *s.RetentionDurationInSeconds <= 0 {
		return s, errParam("retentionDurationInSeconds must be positive")
	}

	if aerr := checkLockDuration(s.LockDurationDetails); aerr != nil {
		return s, aerr
	}

	local := now.In(scheduleZone(s.TimeZone))

	return fillSchedule(s, local)
}

func fillSchedule(s schedule, local time.Time) (schedule, *APIError) {
	if s.Period == periodHourly {
		if s.HourOfDay != nil || s.DayOfWeek != "" || s.DayOfMonth != nil || s.Month != "" {
			return s, errParam("an HOURLY schedule takes no hourOfDay, dayOfWeek, dayOfMonth or month")
		}

		return s, nil
	}

	if s.HourOfDay == nil {
		h := local.Hour()
		s.HourOfDay = &h
	}

	if *s.HourOfDay < 0 || *s.HourOfDay > lastHourOfDay {
		return s, errParam("hourOfDay must be between 0 and 23")
	}

	if aerr := fillWeekday(&s, local); aerr != nil {
		return s, aerr
	}

	if aerr := fillDayOfMonth(&s, local); aerr != nil {
		return s, aerr
	}

	return s, fillMonth(&s, local)
}

func fillWeekday(s *schedule, local time.Time) *APIError {
	if s.Period != periodWeekly {
		return nil
	}

	if s.DayOfWeek == "" {
		s.DayOfWeek = strings.ToUpper(local.Weekday().String())
	}

	if _, ok := weekdays[s.DayOfWeek]; !ok {
		return errParam("dayOfWeek must be a weekday name such as MONDAY")
	}

	return nil
}

func fillDayOfMonth(s *schedule, local time.Time) *APIError {
	if s.Period != periodMonthly && s.Period != periodYearly {
		return nil
	}

	if s.DayOfMonth == nil {
		d := local.Day()
		s.DayOfMonth = &d
	}

	if *s.DayOfMonth < 1 || *s.DayOfMonth > maxDayOfMonth {
		return errParam("dayOfMonth must be between 1 and 31")
	}

	return nil
}

func fillMonth(s *schedule, local time.Time) *APIError {
	if s.Period != periodYearly {
		return nil
	}

	if s.Month == "" {
		s.Month = strings.ToUpper(local.Month().String())
	}

	if _, ok := months[s.Month]; !ok {
		return errParam("month must be a month name such as JANUARY")
	}

	return nil
}

// nextOccurrence returns the first scheduled time strictly after t that is not before the schedule start.
func nextOccurrence(s schedule, t time.Time) time.Time {
	loc := scheduleZone(s.TimeZone)
	start := s.TimeScheduleStart.In(loc)
	from := t.In(loc)

	if from.Before(start) {
		from = start.Add(-time.Second)
	}

	if s.Period == periodHourly {
		k := max(int64(from.Sub(start)/time.Hour)+1, 0)

		return start.Add(time.Duration(k) * time.Hour).UTC()
	}

	day := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, loc)

	for range searchDays {
		c := time.Date(day.Year(), day.Month(), day.Day(), *s.HourOfDay, start.Minute(), start.Second(), 0, loc)

		if c.After(from) && !c.Before(start) && matchesDay(s, c) {
			return c.UTC()
		}

		day = day.AddDate(0, 0, 1)
	}

	return time.Time{}
}

func matchesDay(s schedule, c time.Time) bool {
	switch s.Period {
	case periodWeekly:
		return c.Weekday() == weekdays[s.DayOfWeek]
	case periodMonthly:
		return c.Day() == *s.DayOfMonth
	case periodYearly:
		return c.Month() == months[s.Month] && c.Day() == *s.DayOfMonth
	}

	return true
}

func isoDuration(sec int64) string {
	d, rem := sec/secondsPerDay, sec%secondsPerDay
	h, rem := rem/secondsPerHour, rem%secondsPerHour
	m, s := rem/secondsPerMinute, rem%secondsPerMinute

	var sb strings.Builder

	sb.WriteString("P")

	if d > 0 {
		fmt.Fprintf(&sb, "%dD", d)
	}

	if h > 0 || m > 0 || s > 0 {
		sb.WriteString("T")

		if h > 0 {
			fmt.Fprintf(&sb, "%dH", h)
		}

		if m > 0 {
			fmt.Fprintf(&sb, "%dM", m)
		}

		if s > 0 {
			fmt.Fprintf(&sb, "%dS", s)
		}
	}

	return sb.String()
}

func (p *Policy) snapshotName(s schedule, at time.Time) string {
	var parts []string

	for _, x := range []string{p.PolicyPrefix, s.SchedulePrefix} {
		if x != "" {
			parts = append(parts, x)
		}
	}

	if len(parts) == 0 {
		parts = append(parts, "FSS_"+p.DisplayName)
	}

	parts = append(
		parts,
		at.UTC().Format("20060102150405"),
		strings.ToUpper(s.Period[:1])+strings.ToLower(s.Period[1:]),
	)

	if s.RetentionDurationInSeconds != nil {
		parts = append(parts, isoDuration(*s.RetentionDurationInSeconds))
	}

	return strings.Join(parts, "_")
}

// CreatePolicy creates a file system snapshot policy.
func (b *InMemoryBackend) CreatePolicy(in createPolicyIn, token string) (Policy, string, error) {
	if in.AvailabilityDomain == "" {
		return Policy{}, "", errMissing("availabilityDomain is required")
	}

	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if id := b.retryHit(kindPolicy, token); id != "" {
		if e, ok := b.pols[id]; ok {
			return b.polView(e), e.Etag, nil
		}
	}

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return Policy{}, "", aerr
	}

	now := b.nowUTC()
	name := "policy-" + now.Format("20060102-150405")

	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	scheds, aerr := normalizeSchedules(in.Schedules, now)
	if aerr != nil {
		return Policy{}, "", aerr
	}

	for _, l := range in.Locks {
		if aerr = checkLock(l); aerr != nil {
			return Policy{}, "", aerr
		}
	}

	if aerr = checkName(name); aerr != nil {
		return Policy{}, "", aerr
	}

	p := &Policy{
		base: b.newBase(kindPolicy, in.tagsIn, in.Locks), CompartmentID: in.CompartmentID,
		AvailabilityDomain: in.AvailabilityDomain, DisplayName: name, PolicyPrefix: in.PolicyPrefix, Schedules: scheds,
		Int: &polInt{CreatedAt: now, LastRun: lastRuns(len(scheds), now)},
	}
	e := put(b, b.pols, p, stActive)
	b.remember(kindPolicy, token, p.ID)

	return b.polView(e), e.Etag, nil
}

func lastRuns(n int, at time.Time) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = at
	}

	return out
}

// GetPolicy returns one snapshot policy.
func (b *InMemoryBackend) GetPolicy(id string) (Policy, string, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	e, aerr := b.pols.get(id)
	if aerr != nil {
		return Policy{}, "", aerr
	}

	return b.polView(e), e.Etag, nil
}

// ListPolicies lists snapshot policies of a compartment and availability domain.
func (b *InMemoryBackend) ListPolicies(f PolicyFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	return listScoped(b, b.pols, f, o, scope[*Policy]{
		fields: func(w *Policy) (string, string, string) { return w.CompartmentID, w.AvailabilityDomain, w.DisplayName },
		render: func(e *entry[*Policy]) map[string]any { return polSummary(b.polView(e)) },
	})
}

// UpdatePolicy changes a snapshot policy.
func (b *InMemoryBackend) UpdatePolicy(id, ifMatch string, in updatePolicyIn) (Policy, string, error) {
	b.mu.Lock("UpdatePolicy")
	defer b.mu.Unlock()

	e, aerr := b.pols.usable(id)
	if aerr != nil {
		return Policy{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, false)); aerr != nil {
		return Policy{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return Policy{}, "", aerr
		}

		e.W.DisplayName = *in.DisplayName
	}

	if in.PolicyPrefix != nil {
		e.W.PolicyPrefix = *in.PolicyPrefix
	}

	if in.Schedules != nil {
		now := b.nowUTC()

		scheds, nerr := normalizeSchedules(*in.Schedules, now)
		if nerr != nil {
			return Policy{}, "", nerr
		}

		e.W.Schedules, e.W.Int.LastRun = scheds, lastRuns(len(scheds), now)
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.polView(e), e.Etag, nil
}

// DeletePolicy deletes a policy and detaches it from the file systems that use it.
func (b *InMemoryBackend) DeletePolicy(id, ifMatch string, override bool) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	e, aerr := b.pols.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override)); aerr != nil {
		return aerr
	}

	for _, fs := range b.fss {
		if fs.W.FilesystemSnapshotPolicyID == id {
			fs.W.FilesystemSnapshotPolicyID = ""
			fs.Etag = newEtag()
		}
	}

	begin(b, e, stDeleting, stDeleted)

	return nil
}

// SetPolicyPaused pauses (INACTIVE) or resumes (ACTIVE) a policy.
func (b *InMemoryBackend) SetPolicyPaused(id, ifMatch string, pause bool) (Policy, string, error) {
	b.mu.Lock("SetPolicyPaused")
	defer b.mu.Unlock()

	e, aerr := b.pols.usable(id)
	if aerr != nil {
		return Policy{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, false)); aerr != nil {
		return Policy{}, "", aerr
	}

	want, from := stInactive, stActive

	if !pause {
		want, from = stActive, stInactive
	}

	if e.W.State != from {
		return Policy{}, "", errState("The policy " + id + " is " + e.W.State + " and cannot move to " + want)
	}

	e.W.State = want
	if !pause {
		e.W.Int.LastRun = lastRuns(len(e.W.Schedules), b.nowUTC())
	}

	e.Etag = newEtag()

	return b.polView(e), e.Etag, nil
}

// ChangePolicyCompartment moves a snapshot policy.
func (b *InMemoryBackend) ChangePolicyCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangePolicyCompartment")
	defer b.mu.Unlock()

	e, aerr := b.pols.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch),
		guardChange(&e.W.base, false),
		b.checkCompartment(compartment),
	); aerr != nil {
		return aerr
	}

	e.W.CompartmentID = compartment
	e.Etag = newEtag()

	return nil
}

// sweepPolicies takes the snapshots that the schedules of ACTIVE policies have made due.
func (b *InMemoryBackend) sweepPolicies(now time.Time) {
	for _, pe := range b.pols {
		p := pe.W
		if p.State != stActive || p.Int == nil || len(p.Int.LastRun) != len(p.Schedules) {
			continue
		}

		for i, s := range p.Schedules {
			due, ok := latestDue(s, p.Int.LastRun[i], now)
			if !ok {
				continue
			}

			p.Int.LastRun[i] = due
			b.policySnapshots(p, s, due)
		}
	}
}

// latestDue returns the newest occurrence in (last, now], if any.
func latestDue(s schedule, last, now time.Time) (time.Time, bool) {
	var due time.Time

	for t := nextOccurrence(s, last); !t.IsZero() && !t.After(now); t = nextOccurrence(s, t) {
		due = t
	}

	return due, !due.IsZero()
}

func (b *InMemoryBackend) policySnapshots(p *Policy, s schedule, at time.Time) {
	for _, fe := range b.fss {
		if fe.W.FilesystemSnapshotPolicyID != p.ID || fe.W.State != stActive {
			continue
		}

		in := createSnapshotIn{
			FileSystemID:        fe.W.ID,
			Name:                p.snapshotName(s, at),
			LockDurationDetails: s.LockDurationDetails,
		}

		if s.RetentionDurationInSeconds != nil {
			exp := at.Add(time.Duration(*s.RetentionDurationInSeconds) * time.Second)
			in.ExpirationTime = &exp
		}

		if in.ExpirationTime != nil && !in.ExpirationTime.After(b.nowUTC()) {
			in.ExpirationTime = nil

			continue
		}

		_, _ = b.takeSnapshot(in, snapPolicy, p.ID)
	}
}
