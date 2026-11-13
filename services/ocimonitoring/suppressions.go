package ocimonitoring

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	levelAlarm        = "ALARM"
	levelDimension    = "DIMENSION"
	targetAlarm       = "ALARM"
	targetCompartment = "COMPARTMENT"
	suppActive        = "ACTIVE"
	suppDeleted       = "DELETED"
	condRecurrence    = "RECURRENCE"
	maxDimensionsLen  = 4000
	suppScope         = "CreateAlarmSuppression"
)

// SuppressionCond is one precondition of an alarm suppression; only RECURRENCE exists.
type SuppressionCond struct {
	Type       string `json:"type"`
	Recurrence string `json:"recurrence"`
	Duration   string `json:"duration"`
}

// AlarmSuppression suppresses notifications of one alarm or of every alarm in a compartment.
type AlarmSuppression struct {
	TimeCreated   time.Time         `json:"timeCreated"`
	TimeUpdated   time.Time         `json:"timeUpdated"`
	From          time.Time         `json:"timeSuppressFrom"`
	Until         time.Time         `json:"timeSuppressUntil"`
	DeletedAt     *time.Time        `json:"deletedAt,omitempty"`
	Tags          Tags              `json:"tags"`
	Dimensions    map[string]string `json:"dimensions,omitempty"`
	ID            string            `json:"id"`
	CompartmentID string            `json:"compartmentId"`
	TargetType    string            `json:"targetType"`
	TargetAlarm   string            `json:"targetAlarmId,omitempty"`
	TargetComp    string            `json:"targetCompartmentId,omitempty"`
	Level         string            `json:"level"`
	Name          string            `json:"displayName"`
	Description   string            `json:"description,omitempty"`
	State         string            `json:"state"`
	Etag          string            `json:"etag"`
	Conditions    []SuppressionCond `json:"conditions,omitempty"`
	Seq           int64             `json:"seq"`
	Subtree       bool              `json:"subtree,omitempty"`
}

type suppressionTargetWire struct {
	AlarmID       *string `json:"alarmId,omitempty"`
	CompartmentID *string `json:"compartmentId,omitempty"`
	InSubtree     *bool   `json:"compartmentIdInSubtree,omitempty"`
	TargetType    string  `json:"targetType"`
}

type suppressionCondWire struct {
	ConditionType         string `json:"conditionType"`
	SuppressionRecurrence string `json:"suppressionRecurrence"`
	SuppressionDuration   string `json:"suppressionDuration"`
}

// CreateSuppressionIn is the CreateAlarmSuppression request body.
type CreateSuppressionIn struct {
	Tags
	From        *time.Time             `json:"timeSuppressFrom"`
	Until       *time.Time             `json:"timeSuppressUntil"`
	Target      *suppressionTargetWire `json:"alarmSuppressionTarget"`
	Name        *string                `json:"displayName"`
	Description *string                `json:"description"`
	Dimensions  map[string]string      `json:"dimensions"`
	RetryToken  string                 `json:"-"`
	Level       string                 `json:"level"`
	Conditions  []suppressionCondWire  `json:"suppressionConditions"`
}

// AlarmSuppressionWire is the AlarmSuppression and AlarmSuppressionSummary representation.
type AlarmSuppressionWire struct {
	TimeSuppressFrom  time.Time                 `json:"timeSuppressFrom"`
	TimeSuppressUntil time.Time                 `json:"timeSuppressUntil"`
	TimeCreated       time.Time                 `json:"timeCreated"`
	TimeUpdated       time.Time                 `json:"timeUpdated"`
	Target            suppressionTargetWire     `json:"alarmSuppressionTarget"`
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	Dimensions        map[string]string         `json:"dimensions,omitempty"`
	ID                string                    `json:"id"`
	CompartmentID     string                    `json:"compartmentId"`
	Level             string                    `json:"level"`
	DisplayName       string                    `json:"displayName"`
	LifecycleState    string                    `json:"lifecycleState"`
	Description       *string                   `json:"description,omitempty"`
	Conditions        []suppressionCondWire     `json:"suppressionConditions,omitempty"`
}

func targetView(s *AlarmSuppression) suppressionTargetWire {
	if s.TargetType == targetAlarm {
		return suppressionTargetWire{TargetType: targetAlarm, AlarmID: &s.TargetAlarm}
	}

	sub := s.Subtree

	return suppressionTargetWire{TargetType: targetCompartment, CompartmentID: &s.TargetComp, InSubtree: &sub}
}

func condsView(cs []SuppressionCond) []suppressionCondWire {
	out := make([]suppressionCondWire, 0, len(cs))

	for _, c := range cs {
		out = append(out, suppressionCondWire{
			ConditionType: c.Type, SuppressionRecurrence: c.Recurrence, SuppressionDuration: c.Duration,
		})
	}

	return out
}

func alarmSuppressionView(s *AlarmSuppression) AlarmSuppressionWire {
	w := AlarmSuppressionWire{
		ID: s.ID, CompartmentID: s.CompartmentID, Target: targetView(s), Level: s.Level, DisplayName: s.Name,
		TimeSuppressFrom: s.From, TimeSuppressUntil: s.Until, LifecycleState: s.State, TimeCreated: s.TimeCreated,
		TimeUpdated: s.TimeUpdated, Conditions: condsView(s.Conditions), Dimensions: s.Dimensions,
		FreeformTags: s.Tags.FreeformTags, DefinedTags: s.Tags.DefinedTags,
	}

	if s.Description != "" {
		w.Description = &s.Description
	}

	return w
}

func validateSuppressionConds(cs []suppressionCondWire) ([]SuppressionCond, *APIError) {
	if len(cs) > 1 {
		return nil, errParam("only one recurrence condition is supported within suppressionConditions")
	}

	var out []SuppressionCond

	for _, c := range cs {
		if c.ConditionType != condRecurrence {
			return nil, errParam("suppressionConditions conditionType must be RECURRENCE")
		}

		if _, aerr := parseRecurrence(c.SuppressionRecurrence, c.SuppressionDuration); aerr != nil {
			return nil, aerr
		}

		out = append(out, SuppressionCond{
			Type: c.ConditionType, Recurrence: c.SuppressionRecurrence, Duration: c.SuppressionDuration,
		})
	}

	return out, nil
}

func validateSuppressionIn(in *CreateSuppressionIn) (string, []SuppressionCond, *APIError) {
	if in.Target == nil || in.Name == nil || in.From == nil || in.Until == nil {
		return "", nil, errMissing(
			"alarmSuppressionTarget, displayName, timeSuppressFrom and timeSuppressUntil are required",
		)
	}

	if aerr := validName(*in.Name); aerr != nil {
		return "", nil, aerr
	}

	if !in.From.Before(*in.Until) {
		return "", nil, errParam("timeSuppressFrom must be before timeSuppressUntil")
	}

	level := strings.ToUpper(in.Level)

	switch level {
	case "":
		level = levelDimension
	case levelAlarm, levelDimension:
	default:
		return "", nil, errParam("level must be ALARM or DIMENSION")
	}

	if level == levelDimension && len(in.Dimensions) == 0 {
		return "", nil, errMissing("dimensions is required when level is DIMENSION and cannot be an empty object")
	}

	if raw, _ := json.Marshal(in.Dimensions); len(raw) > maxDimensionsLen {
		return "", nil, errParam("dimensions values are too long")
	}

	conds, aerr := validateSuppressionConds(in.Conditions)

	return level, conds, aerr
}

func (b *InMemoryBackend) resolveTarget(s *AlarmSuppression, t *suppressionTargetWire) *APIError {
	switch strings.ToUpper(t.TargetType) {
	case targetAlarm:
		if t.AlarmID == nil || *t.AlarmID == "" {
			return errMissing("alarmSuppressionTarget.alarmId is required for targetType ALARM")
		}

		a, aerr := b.liveAlarm(*t.AlarmID)
		if aerr != nil {
			return aerr
		}

		s.TargetType, s.TargetAlarm, s.CompartmentID = targetAlarm, a.ID, a.CompartmentID
	case targetCompartment:
		if t.CompartmentID == nil || *t.CompartmentID == "" {
			return errMissing("alarmSuppressionTarget.compartmentId is required for targetType COMPARTMENT")
		}

		sub := t.InSubtree != nil && *t.InSubtree
		if aerr := b.checkSubtree(*t.CompartmentID, sub, "compartmentIdInSubtree"); aerr != nil {
			return aerr
		}

		s.TargetType, s.TargetComp, s.CompartmentID, s.Subtree = targetCompartment, *t.CompartmentID, *t.CompartmentID, sub
	default:
		return errParam("alarmSuppressionTarget.targetType must be ALARM or COMPARTMENT")
	}

	return nil
}

// CreateAlarmSuppression stores a suppression of one alarm or of a compartment's alarms.
func (b *InMemoryBackend) CreateAlarmSuppression(in CreateSuppressionIn) (AlarmSuppressionWire, string, error) {
	level, conds, aerr := validateSuppressionIn(&in)
	if aerr != nil {
		return AlarmSuppressionWire{}, "", aerr
	}

	b.mu.Lock("CreateAlarmSuppression")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup(suppScope, in.RetryToken, b.nowUTC()); ok {
		s, live := b.suppressions[v]
		if !live || s.State == suppDeleted {
			return AlarmSuppressionWire{}, "", errInvalidatedToken()
		}

		return alarmSuppressionView(s), s.Etag, nil
	}

	if aerr = b.checkTags(in.Tags); aerr != nil {
		return AlarmSuppressionWire{}, "", aerr
	}

	now := b.nowUTC()
	s := &AlarmSuppression{
		ID: newOCID("alarmsuppression"), Level: level, Name: *in.Name, Description: strVal(in.Description),
		From: in.From.UTC(), Until: in.Until.UTC(), Dimensions: in.Dimensions, Conditions: conds, Tags: in.Tags,
		State: suppActive, TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Seq: b.nextSeq(),
	}

	if aerr = b.resolveTarget(s, in.Target); aerr != nil {
		return AlarmSuppressionWire{}, "", aerr
	}

	b.suppressions[s.ID] = s
	b.retry.Remember(suppScope, in.RetryToken, s.ID, now)

	return alarmSuppressionView(s), s.Etag, nil
}

// GetAlarmSuppression returns one suppression, including a deleted one.
func (b *InMemoryBackend) GetAlarmSuppression(id string) (AlarmSuppressionWire, string, error) {
	b.mu.RLock("GetAlarmSuppression")
	defer b.mu.RUnlock()

	s, ok := b.suppressions[id]
	if !ok {
		return AlarmSuppressionWire{}, "", errNotFound(id)
	}

	return alarmSuppressionView(s), s.Etag, nil
}

// DeleteAlarmSuppression marks a suppression DELETED; it stays readable and in the history.
func (b *InMemoryBackend) DeleteAlarmSuppression(id, ifMatch string) error {
	b.mu.Lock("DeleteAlarmSuppression")
	defer b.mu.Unlock()

	s, ok := b.suppressions[id]
	if !ok || s.State == suppDeleted {
		return errNotFound(id)
	}

	if aerr := checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	now := b.nowUTC()
	s.State, s.DeletedAt, s.TimeUpdated, s.Etag = suppDeleted, &now, now, newEtag()

	return nil
}

// SuppressionFilter narrows ListAlarmSuppressions.
type SuppressionFilter struct {
	AlarmID, Name, State, Level, Compartment, TargetType string
	Subtree, All                                         bool
}

func (b *InMemoryBackend) matchSuppression(s *AlarmSuppression, f SuppressionFilter, alarm *Alarm) bool {
	if (f.Name != "" && s.Name != f.Name) || (f.Level != "" && !strs.Equal(s.Level, f.Level)) ||
		!strs.Equal(s.State, f.State) {
		return false
	}

	if f.AlarmID != "" {
		return s.TargetType == targetAlarm && s.TargetAlarm == f.AlarmID ||
			f.All && b.affects(s, alarm)
	}

	switch {
	case strs.Equal(f.TargetType, targetAlarm):
		return s.TargetType == targetAlarm && b.inCompartment(s.CompartmentID, f.Compartment, f.Subtree)
	case strs.Equal(f.TargetType, targetCompartment):
		return s.TargetType == targetCompartment && b.inCompartment(s.TargetComp, f.Compartment, f.Subtree)
	}

	return b.inCompartment(s.CompartmentID, f.Compartment, f.Subtree)
}

func compareSuppressions(a, c *AlarmSuppression, by string) int {
	switch by {
	case "displayName":
		return compareStr(a.Name, c.Name)
	case "timeSuppressFrom":
		return a.From.Compare(c.From)
	}

	return int(a.Seq - c.Seq)
}

// ListAlarmSuppressions lists suppressions by alarm or by compartment.
func (b *InMemoryBackend) ListAlarmSuppressions(
	f SuppressionFilter,
	o ListOpts,
) (page.Page[AlarmSuppressionWire], error) {
	if f.State == "" {
		f.State = suppActive
	}

	if f.AlarmID == "" && f.Compartment == "" {
		return page.Page[AlarmSuppressionWire]{}, errMissing("alarmId or compartmentId is required")
	}

	if f.All && f.AlarmID == "" {
		return page.Page[AlarmSuppressionWire]{}, errParam("isAllSuppressions requires alarmId")
	}

	b.mu.RLock("ListAlarmSuppressions")
	defer b.mu.RUnlock()

	var alarm *Alarm

	if f.AlarmID != "" {
		var aerr *APIError
		if alarm, aerr = b.liveAlarm(f.AlarmID); aerr != nil {
			return page.Page[AlarmSuppressionWire]{}, aerr
		}
	} else if aerr := b.checkSubtree(f.Compartment, f.Subtree, "compartmentIdInSubtree"); aerr != nil {
		return page.Page[AlarmSuppressionWire]{}, aerr
	}

	var all []*AlarmSuppression

	for _, s := range b.suppressions {
		if b.matchSuppression(s, f, alarm) {
			all = append(all, s)
		}
	}

	pg := paginate(all, o, compareSuppressions, "timeCreated")

	out := make([]AlarmSuppressionWire, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, alarmSuppressionView(s))
	}

	return page.Page[AlarmSuppressionWire]{Data: out, Next: pg.Next}, nil
}

// affects reports whether the suppression targets the alarm directly or through its compartment.
func (b *InMemoryBackend) affects(s *AlarmSuppression, a *Alarm) bool {
	if a == nil {
		return false
	}

	if s.TargetType == targetAlarm {
		return s.TargetAlarm == a.ID
	}

	return b.inCompartment(a.CompartmentID, s.TargetComp, s.Subtree)
}

func (s *AlarmSuppression) recurrence() *recurrence {
	if len(s.Conditions) == 0 {
		return nil
	}

	r, _ := parseRecurrence(s.Conditions[0].Recurrence, s.Conditions[0].Duration)

	return r
}

// activeAt reports whether the suppression is in effect at t.
func (s *AlarmSuppression) activeAt(t time.Time) bool {
	if s.State != suppActive || t.Before(s.From) || t.After(s.Until) {
		return false
	}

	r := s.recurrence()

	return r == nil || len(r.spans(s.From, s.Until, t, t)) > 0
}

// activeFor returns the suppressions in effect for the alarm at t; the caller holds the lock.
func (b *InMemoryBackend) activeFor(a *Alarm, t time.Time) []*AlarmSuppression {
	var out []*AlarmSuppression

	for _, s := range b.suppressions {
		if s.activeAt(t) && b.affects(s, a) {
			out = append(out, s)
		}
	}

	return out
}

// dimsMatch reports whether every suppression dimension is present with the same value in the stream.
func dimsMatch(want, have map[string]string) bool {
	for k, v := range want {
		if hv, ok := have[k]; !ok || hv != v {
			return false
		}
	}

	return true
}

// suppresses reports whether any of the suppressions covers a stream with these dimensions.
func suppresses(sups []*AlarmSuppression, dims map[string]string) bool {
	return slices.ContainsFunc(sups, func(s *AlarmSuppression) bool {
		return s.Level == levelAlarm || dimsMatch(s.Dimensions, dims)
	})
}

// RemoveAlarmSuppression clears the suppression property of an alarm.
func (b *InMemoryBackend) RemoveAlarmSuppression(alarmID, ifMatch string) error {
	b.mu.Lock("RemoveAlarmSuppression")
	defer b.mu.Unlock()

	a, aerr := b.liveAlarm(alarmID)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return aerr
	}

	a.Suppression, a.Etag, a.TimeUpdated = nil, newEtag(), b.nowUTC()

	return nil
}

// SuppressionHistoryIn is the SummarizeAlarmSuppressionHistory request body.
type SuppressionHistoryIn struct {
	Dimensions map[string]string `json:"dimensions"`
	From       *time.Time        `json:"timeSuppressFromGreaterThanOrEqualTo"`
	Before     *time.Time        `json:"timeSuppressFromLessThan"`
}

// SuppressionHistoryItemWire is one AlarmSuppressionHistoryItem.
type SuppressionHistoryItemWire struct {
	TimeEffectiveFrom  time.Time             `json:"timeEffectiveFrom"`
	TimeEffectiveUntil time.Time             `json:"timeEffectiveUntil"`
	Target             suppressionTargetWire `json:"alarmSuppressionTarget"`
	Dimensions         map[string]string     `json:"dimensions,omitempty"`
	SuppressionID      string                `json:"suppressionId"`
	Level              string                `json:"level"`
	DisplayName        string                `json:"displayName"`
	Description        *string               `json:"description,omitempty"`
	Conditions         []suppressionCondWire `json:"suppressionConditions,omitempty"`
}

func historyItem(s *AlarmSuppression, from, until time.Time) SuppressionHistoryItemWire {
	w := SuppressionHistoryItemWire{
		SuppressionID: s.ID, Target: targetView(s), Level: s.Level, DisplayName: s.Name, Dimensions: s.Dimensions,
		TimeEffectiveFrom: from, TimeEffectiveUntil: until, Conditions: condsView(s.Conditions),
	}

	if s.Description != "" {
		w.Description = &s.Description
	}

	return w
}

// effectiveSpans returns when the suppression took effect up to now, cut short by deletion.
func (s *AlarmSuppression) effectiveSpans(now time.Time) []span {
	until := s.Until
	if s.DeletedAt != nil && s.DeletedAt.Before(until) {
		until = *s.DeletedAt
	}

	if until.After(now) {
		until = now
	}

	if s.From.After(until) {
		return nil
	}

	if r := s.recurrence(); r != nil {
		return r.spans(s.From, until, s.From, until)
	}

	return []span{{start: s.From, end: until}}
}

func suppressionInHistory(s *AlarmSuppression, in SuppressionHistoryIn) bool {
	if (in.From != nil && s.From.Before(*in.From)) || (in.Before != nil && !s.From.Before(*in.Before)) {
		return false
	}

	return s.Level == levelAlarm || len(in.Dimensions) == 0 || dimsMatch(s.Dimensions, in.Dimensions)
}

// SummarizeAlarmSuppressionHistory lists when suppressions affected the alarm, including deleted ones.
func (b *InMemoryBackend) SummarizeAlarmSuppressionHistory(
	alarmID string,
	in SuppressionHistoryIn,
	o ListOpts,
) (page.Page[SuppressionHistoryItemWire], error) {
	b.mu.RLock("SummarizeAlarmSuppressionHistory")
	defer b.mu.RUnlock()

	now := b.nowUTC()

	for _, t := range []*time.Time{in.From, in.Before} {
		if t != nil && t.After(now) {
			return page.Page[SuppressionHistoryItemWire]{}, errParam(
				"a suppression history time filter cannot be a future time",
			)
		}
	}

	a, aerr := b.liveAlarm(alarmID)
	if aerr != nil {
		return page.Page[SuppressionHistoryItemWire]{}, aerr
	}

	var all []SuppressionHistoryItemWire

	for _, s := range b.suppressions {
		if !b.affects(s, a) || !suppressionInHistory(s, in) {
			continue
		}

		for _, sp := range s.effectiveSpans(now) {
			all = append(all, historyItem(s, sp.start, sp.end))
		}
	}

	slices.SortStableFunc(all, func(x, y SuppressionHistoryItemWire) int {
		return x.TimeEffectiveFrom.Compare(y.TimeEffectiveFrom)
	})

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}
