package arczonalshift

import (
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

var expiresInRE = regexp.MustCompile(`^(\d+)([mh])$`)

// ShiftView is the wire form of a zonal shift (summary and Start/Update/Cancel output).
type ShiftView struct {
	AwayFrom           string  `json:"awayFrom"`
	Comment            string  `json:"comment"`
	ResourceIdentifier string  `json:"resourceIdentifier"`
	Status             string  `json:"status"`
	ZonalShiftID       string  `json:"zonalShiftId"`
	PracticeRunOutcome string  `json:"practiceRunOutcome,omitempty"`
	ShiftType          string  `json:"shiftType,omitempty"`
	ExpiryTime         float64 `json:"expiryTime"`
	StartTime          float64 `json:"startTime"`
}

func viewOf(s *Shift, now time.Time) ShiftView {
	v := ShiftView{
		AwayFrom:           s.AwayFrom,
		Comment:            s.Comment,
		ResourceIdentifier: s.Resource,
		Status:             s.status(now),
		ZonalShiftID:       s.ID,
		ShiftType:          s.Type,
		ExpiryTime:         awstime.Epoch(s.ExpiryTime),
		StartTime:          awstime.Epoch(s.StartTime),
	}
	if s.Type == shiftPractice {
		v.PracticeRunOutcome = s.Outcome
	}

	return v
}

// parseExpiresIn converts "20m"/"3h" to a duration within 1 minute .. 72 hours.
func parseExpiresIn(s string) (time.Duration, error) {
	m := expiresInRE.FindStringSubmatch(s)
	if m == nil {
		return 0, invalidReason(
			"InvalidExpiresIn",
			"ExpiresIn must be a whole number followed by m (minutes) or h (hours)",
		)
	}

	n, err := strconv.Atoi(m[1])
	if err != nil || n < 1 {
		return 0, invalidReason("InvalidExpiresIn", "ExpiresIn must be at least one minute")
	}

	unit := time.Minute
	if m[2] == "h" {
		unit = time.Hour
	}

	if n > int(maxShiftDuration/unit) {
		return 0, invalidReason("InvalidExpiresIn", "ExpiresIn must not exceed 72 hours")
	}

	return time.Duration(n) * unit, nil
}

func (b *InMemoryBackend) activeShifts(resource, shiftType string, now time.Time) []*Shift {
	var out []*Shift

	for _, s := range b.shifts.All() {
		if s.Resource == resource && s.status(now) == statusActive && (shiftType == "" || s.Type == shiftType) {
			out = append(out, s)
		}
	}

	return sortedShifts(out)
}

// resolveAZ maps a zone name or ID to the resource's zone ID.
func (m managedResource) resolveAZ(away string) (string, bool) {
	for i, id := range m.ZoneIDs {
		if away == id || away == m.ZoneNames[i] {
			return id, true
		}
	}

	return "", false
}

// settleLocked finalises the outcome of practice runs that have ended; callers hold the write lock.
func (b *InMemoryBackend) settleLocked(now time.Time) {
	for _, s := range b.shifts.All() {
		if s.Type != shiftPractice || s.Outcome != outcomePending || s.status(now) == statusActive {
			continue
		}

		s.Outcome = b.practiceOutcome(s, now)
	}
}

func (b *InMemoryBackend) practiceOutcome(s *Shift, now time.Time) string {
	if s.status(now) == statusCanceled {
		return outcomeInterrupted
	}

	if cfg, ok := b.configs.Get(s.Resource); ok {
		for _, a := range cfg.OutcomeAlarms {
			if state, found := b.alarmState(a.AlarmIdentifier); found && state == alarmStateAlarm {
				return outcomeFailed
			}
		}
	}

	return outcomeSucceeded
}

// StartZonalShift moves traffic for a managed resource away from one Availability Zone until it expires.
func (b *InMemoryBackend) StartZonalShift(resource, awayFrom, comment, expiresIn string) (ShiftView, error) {
	if comment == "" || awayFrom == "" || resource == "" {
		return ShiftView{}, invalidReason(
			"MissingValue",
			"awayFrom, comment, expiresIn and resourceIdentifier are required",
		)
	}

	d, err := parseExpiresIn(expiresIn)
	if err != nil {
		return ShiftView{}, err
	}

	m, err := b.managedFor(resource)
	if err != nil {
		return ShiftView{}, err
	}

	az, ok := m.resolveAZ(awayFrom)
	if !ok {
		return ShiftView{}, invalidReason("InvalidAz", "Availability Zone %q is not used by %s", awayFrom, resource)
	}

	b.mu.Lock("StartZonalShift")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	if err = b.checkNoConflict(m, az, now); err != nil {
		return ShiftView{}, err
	}

	s := &Shift{
		ID: uuid.NewString(), Resource: resource, AwayFrom: az, Comment: comment, Type: shiftZonal,
		StartTime: now, ExpiryTime: now.Add(d),
	}
	b.shifts.Put(s)

	return viewOf(s, now), nil
}

// checkNoConflict rejects a duplicate shift for the same zone, or one that would leave the resource with no zone.
func (b *InMemoryBackend) checkNoConflict(m managedResource, az string, now time.Time) error {
	away := map[string]bool{az: true}

	for _, s := range b.activeShifts(m.ARN, shiftZonal, now) {
		if s.AwayFrom == az {
			return conflictReason("ZonalShiftAlreadyExists", s.ID,
				"A zonal shift away from %s already exists for %s", az, m.ARN)
		}

		away[s.AwayFrom] = true
	}

	if len(m.ZoneIDs) > 0 && len(away) >= len(m.ZoneIDs) {
		return conflictReason("SimultaneousZonalShiftsConflict", "",
			"Shifting away from %s would leave %s with no Availability Zone serving traffic", az, m.ARN)
	}

	return nil
}

func (b *InMemoryBackend) shiftByID(id string) (*Shift, error) {
	s, ok := b.shifts.Get(id)
	if !ok {
		return nil, restroute.NotFound("Zonal shift %s was not found", id)
	}

	return s, nil
}

// UpdateZonalShift changes the comment and/or resets the expiry of an active zonal shift.
func (b *InMemoryBackend) UpdateZonalShift(id, comment, expiresIn string) (ShiftView, error) {
	if comment == "" && expiresIn == "" {
		return ShiftView{}, invalidReason("MissingValue", "Specify comment, expiresIn or both")
	}

	var d time.Duration

	if expiresIn != "" {
		var err error
		if d, err = parseExpiresIn(expiresIn); err != nil {
			return ShiftView{}, err
		}
	}

	b.mu.Lock("UpdateZonalShift")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	s, err := b.shiftByID(id)
	if err != nil {
		return ShiftView{}, err
	}

	if s.Type != shiftZonal {
		return ShiftView{}, invalidReason(
			"AutoshiftUpdateNotAllowed",
			"Only customer zonal shifts can be updated; %s is a %s",
			id,
			s.Type,
		)
	}

	if s.status(now) != statusActive {
		return ShiftView{}, conflictReason(
			"ZonalShiftStatusNotActive",
			id,
			"Zonal shift %s is %s and cannot be updated",
			id,
			s.status(now),
		)
	}

	if comment != "" {
		s.Comment = comment
	}

	if expiresIn != "" {
		s.ExpiryTime = now.Add(d)
	}

	return viewOf(s, now), nil
}

// CancelZonalShift ends an active customer zonal shift immediately.
func (b *InMemoryBackend) CancelZonalShift(id string) (ShiftView, error) {
	return b.cancel(id, shiftZonal, "CancelZonalShift")
}

// CancelPracticeRun ends an active practice run immediately.
func (b *InMemoryBackend) CancelPracticeRun(id string) (ShiftView, error) {
	return b.cancel(id, shiftPractice, "CancelPracticeRun")
}

func (b *InMemoryBackend) cancel(id, wantType, op string) (ShiftView, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	s, err := b.shiftByID(id)
	if err != nil {
		return ShiftView{}, err
	}

	if s.Type != wantType {
		return ShiftView{}, invalidReason("UnsupportedPracticeCancelShiftType",
			"Shift %s is a %s and cannot be canceled with %s", id, s.Type, op)
	}

	if s.status(now) != statusActive {
		return ShiftView{}, conflictReason(
			"ZonalShiftStatusNotActive",
			id,
			"Zonal shift %s is %s and cannot be canceled",
			id,
			s.status(now),
		)
	}

	s.CanceledAt = &now
	if s.Type == shiftPractice {
		s.Outcome = outcomeInterrupted
	}

	return viewOf(s, now), nil
}

// ListZonalShifts lists shifts, optionally filtered by status and resource.
func (b *InMemoryBackend) ListZonalShifts(status, resource, token string, maxResults int) ([]ShiftView, string, error) {
	switch status {
	case "", statusActive, statusExpired, statusCanceled:
	default:
		return nil, "", invalidReason("InvalidStatus", "status must be ACTIVE, EXPIRED or CANCELED")
	}

	b.mu.Lock("ListZonalShifts")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.settleLocked(now)

	var all []*Shift

	for _, s := range b.shifts.All() {
		if (status == "" || s.status(now) == status) && (resource == "" || s.Resource == resource) {
			all = append(all, s)
		}
	}

	pg, err := paginate(sortedShifts(all), token, maxResults)
	if err != nil {
		return nil, "", err
	}

	out := make([]ShiftView, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, viewOf(s, now))
	}

	return out, pg.Next, nil
}
