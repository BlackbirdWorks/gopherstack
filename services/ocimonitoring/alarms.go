package ocimonitoring

import (
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	destTopicPrefix  = "ocid1.onstopic."
	destStreamPrefix = "ocid1.stream."
	minDuration      = time.Minute
	maxPending       = time.Hour
	maxRepeat        = 30 * 24 * time.Hour
	defaultPending   = "PT1M"
	onlyResolution   = "1m"
	formatRaw        = "RAW"
	formatPretty     = "PRETTY_JSON"
	formatONS        = "ONS_OPTIMIZED"
)

var isoDurationRe = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$`)

// parseISODuration parses the ISO 8601 day/hour/minute/second durations alarms accept.
func parseISODuration(s string) (time.Duration, bool) {
	m := isoDurationRe.FindStringSubmatch(s)
	if m == nil || s == "P" || strings.HasSuffix(s, "T") {
		return 0, false
	}

	var d time.Duration

	for i, unit := range []time.Duration{24 * time.Hour, time.Hour, time.Minute, time.Second} {
		if m[i+1] == "" {
			continue
		}

		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return 0, false
		}

		d += time.Duration(n) * unit
	}

	return d, true
}

// SuppressionWire is the alarm suppression window.
type SuppressionWire struct {
	TimeSuppressFrom  time.Time `json:"timeSuppressFrom"`
	TimeSuppressUntil time.Time `json:"timeSuppressUntil"`
	Description       string    `json:"description,omitempty"`
}

// CreateAlarmIn is the CreateAlarm request body.
type CreateAlarmIn struct {
	Tags
	InSubtree     *bool             `json:"metricCompartmentIdInSubtree"`
	PerDimension  *bool             `json:"isNotificationsPerMetricDimensionEnabled"`
	Enabled       *bool             `json:"isEnabled"`
	Suppression   *SuppressionWire  `json:"suppression"`
	ResourceGroup *string           `json:"resourceGroup"`
	Resolution    *string           `json:"resolution"`
	Pending       *string           `json:"pendingDuration"`
	Body          *string           `json:"body"`
	MessageFormat *string           `json:"messageFormat"`
	Repeat        *string           `json:"repeatNotificationDuration"`
	RuleName      *string           `json:"ruleName"`
	NotifVersion  *string           `json:"notificationVersion"`
	NotifTitle    *string           `json:"notificationTitle"`
	EvalSlack     *string           `json:"evaluationSlackDuration"`
	AlarmSummary  *string           `json:"alarmSummary"`
	Name          string            `json:"displayName"`
	Compartment   string            `json:"compartmentId"`
	MetricComp    string            `json:"metricCompartmentId"`
	Namespace     string            `json:"namespace"`
	Query         string            `json:"query"`
	Severity      string            `json:"severity"`
	Destinations  []string          `json:"destinations"`
	Overrides     []json.RawMessage `json:"overrides"`
}

// UpdateAlarmIn is the UpdateAlarm request body.
type UpdateAlarmIn struct {
	Tags
	Name          *string           `json:"displayName"`
	Compartment   *string           `json:"compartmentId"`
	MetricComp    *string           `json:"metricCompartmentId"`
	InSubtree     *bool             `json:"metricCompartmentIdInSubtree"`
	Namespace     *string           `json:"namespace"`
	ResourceGroup *string           `json:"resourceGroup"`
	Query         *string           `json:"query"`
	Resolution    *string           `json:"resolution"`
	Pending       *string           `json:"pendingDuration"`
	Severity      *string           `json:"severity"`
	Body          *string           `json:"body"`
	PerDimension  *bool             `json:"isNotificationsPerMetricDimensionEnabled"`
	MessageFormat *string           `json:"messageFormat"`
	Destinations  []string          `json:"destinations"`
	Repeat        *string           `json:"repeatNotificationDuration"`
	Suppression   *SuppressionWire  `json:"suppression"`
	Enabled       *bool             `json:"isEnabled"`
	RuleName      *string           `json:"ruleName"`
	NotifVersion  *string           `json:"notificationVersion"`
	NotifTitle    *string           `json:"notificationTitle"`
	EvalSlack     *string           `json:"evaluationSlackDuration"`
	AlarmSummary  *string           `json:"alarmSummary"`
	Overrides     []json.RawMessage `json:"overrides"`
}

// AlarmWire is the Alarm response shape.
type AlarmWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	Suppression    *SuppressionWire          `json:"suppression,omitempty"`
	InSubtree      *bool                     `json:"metricCompartmentIdInSubtree,omitempty"`
	PerDimension   *bool                     `json:"isNotificationsPerMetricDimensionEnabled,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	RuleName       *string                   `json:"ruleName,omitempty"`
	NotifVersion   *string                   `json:"notificationVersion,omitempty"`
	NotifTitle     *string                   `json:"notificationTitle,omitempty"`
	EvalSlack      *string                   `json:"evaluationSlackDuration,omitempty"`
	AlarmSummary   *string                   `json:"alarmSummary,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"displayName"`
	CompartmentID  string                    `json:"compartmentId"`
	MetricComp     string                    `json:"metricCompartmentId"`
	Namespace      string                    `json:"namespace"`
	ResourceGroup  string                    `json:"resourceGroup,omitempty"`
	Query          string                    `json:"query"`
	Resolution     string                    `json:"resolution"`
	Pending        string                    `json:"pendingDuration"`
	Severity       string                    `json:"severity"`
	Body           string                    `json:"body,omitempty"`
	MessageFormat  string                    `json:"messageFormat"`
	Repeat         string                    `json:"repeatNotificationDuration,omitempty"`
	LifecycleState string                    `json:"lifecycleState"`
	Destinations   []string                  `json:"destinations"`
	Overrides      []json.RawMessage         `json:"overrides,omitempty"`
	Enabled        bool                      `json:"isEnabled"`
}

// AlarmSummaryWire is the ListAlarms item.
type AlarmSummaryWire struct {
	Suppression    *SuppressionWire          `json:"suppression,omitempty"`
	PerDimension   *bool                     `json:"isNotificationsPerMetricDimensionEnabled,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"displayName"`
	CompartmentID  string                    `json:"compartmentId"`
	MetricComp     string                    `json:"metricCompartmentId"`
	Namespace      string                    `json:"namespace"`
	ResourceGroup  string                    `json:"resourceGroup,omitempty"`
	Query          string                    `json:"query"`
	Severity       string                    `json:"severity"`
	LifecycleState string                    `json:"lifecycleState"`
	Destinations   []string                  `json:"destinations"`
	Enabled        bool                      `json:"isEnabled"`
}

// AlarmStatusWire is the ListAlarmsStatus item.
type AlarmStatusWire struct {
	TimestampTriggered time.Time        `json:"timestampTriggered"`
	Suppression        *SuppressionWire `json:"suppression,omitempty"`
	ID                 string           `json:"id"`
	Name               string           `json:"displayName"`
	Severity           string           `json:"severity"`
	Status             string           `json:"status"`
}

func suppressionView(s *SuppressionSpec) *SuppressionWire {
	if s == nil {
		return nil
	}

	return &SuppressionWire{TimeSuppressFrom: s.From, TimeSuppressUntil: s.Until, Description: s.Description}
}

func extra(a *Alarm, k string) *string {
	if v, ok := a.Extra[k]; ok {
		return &v
	}

	return nil
}

func alarmView(a *Alarm) AlarmWire {
	return AlarmWire{
		ID: a.ID, Name: a.Name, CompartmentID: a.CompartmentID, MetricComp: a.MetricCompartmentID,
		InSubtree: a.InSubtree, Namespace: a.Namespace, ResourceGroup: a.ResourceGroup, Query: a.Query,
		Resolution: a.Resolution, Pending: a.PendingDuration, Severity: a.Severity, Body: a.Body,
		PerDimension: a.PerDimension, MessageFormat: a.MessageFormat, Destinations: a.Destinations,
		Repeat: a.RepeatNotification, Suppression: suppressionView(a.Suppression), Enabled: a.Enabled,
		LifecycleState: stateActive, FreeformTags: a.Tags.FreeformTags, DefinedTags: a.Tags.DefinedTags,
		Overrides: a.Overrides, RuleName: extra(a, "ruleName"), NotifVersion: extra(a, "notificationVersion"),
		NotifTitle: extra(a, "notificationTitle"), EvalSlack: extra(a, "evaluationSlackDuration"),
		AlarmSummary: extra(a, "alarmSummary"), TimeCreated: a.TimeCreated, TimeUpdated: a.TimeUpdated,
	}
}

func validSeverity(s string) (string, *APIError) {
	for _, v := range []string{"CRITICAL", "ERROR", "WARNING", "INFO"} {
		if strs.Equal(s, v) {
			return v, nil
		}
	}

	return "", errParam("severity must be CRITICAL, ERROR, WARNING or INFO")
}

func validFormat(f *string, destinations []string) (string, *APIError) {
	if f == nil || *f == "" {
		return formatRaw, nil
	}

	for _, v := range []string{formatRaw, formatPretty, formatONS} {
		if strs.Equal(*f, v) {
			if v != formatRaw {
				for _, d := range destinations {
					if !strings.HasPrefix(d, destTopicPrefix) {
						return "", errParam(
							"messageFormat " + v + " is only available when the destinations are Notifications topics",
						)
					}
				}
			}

			return v, nil
		}
	}

	return "", errParam("messageFormat must be RAW, PRETTY_JSON or ONS_OPTIMIZED")
}

func validDuration(name string, s *string, hi time.Duration) *APIError {
	if s == nil {
		return nil
	}

	d, ok := parseISODuration(*s)
	if !ok || d < minDuration || d > hi {
		return errParam(name + " must be an ISO 8601 duration between " + minDuration.String() + " and " + hi.String())
	}

	return nil
}

func validSuppression(s *SuppressionWire) *APIError {
	if s != nil && !s.TimeSuppressFrom.Before(s.TimeSuppressUntil) {
		return errParam("suppression.timeSuppressFrom must be before timeSuppressUntil")
	}

	return nil
}

// validateQuery requires an alarm query to carry a trigger rule: a threshold, or absent().
func validateQuery(query string) *APIError {
	q, err := ParseMQL(query)
	if err != nil {
		aerr, _ := err.(*APIError) //nolint:errorlint // ParseMQL returns only *APIError

		return aerr
	}

	if q.Pred == nil && q.Stat != statAbsent {
		return errParam("an alarm query needs a threshold condition such as > 80, or the absent() statistic")
	}

	return nil
}

func (b *InMemoryBackend) validateDestinations(dests []string) *APIError {
	if len(dests) == 0 {
		return errMissing("destinations must contain at least one destination")
	}

	var topics, streams int

	for _, d := range dests {
		switch {
		case strings.HasPrefix(d, destTopicPrefix):
			topics++

			if b.topicChecker != nil && !b.topicChecker(d) {
				return errParam("destination " + d + " does not reference an existing Notifications topic")
			}
		case strings.HasPrefix(d, destStreamPrefix):
			streams++
		default:
			return errParam("destination " + d + " must be the OCID of a Notifications topic or a Streaming stream")
		}
	}

	if topics > 1 || streams > 1 {
		return errParam("at most one destination per destination service is allowed")
	}

	return nil
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func setExtra(a *Alarm, k string, v *string) {
	if v == nil {
		return
	}

	if a.Extra == nil {
		a.Extra = map[string]string{}
	}

	a.Extra[k] = *v
}

// CreateAlarm validates and stores an alarm; it starts OK.
func (b *InMemoryBackend) CreateAlarm(in CreateAlarmIn) (AlarmWire, string, error) {
	if in.Compartment == "" || in.MetricComp == "" || in.Namespace == "" || in.Query == "" || in.Enabled == nil {
		return AlarmWire{}, "", errMissing(
			"displayName, compartmentId, metricCompartmentId, namespace, query, severity, " +
				"destinations and isEnabled are required",
		)
	}

	sev, aerr := validSeverity(in.Severity)
	if aerr != nil {
		return AlarmWire{}, "", aerr
	}

	if aerr = firstErr(
		validName(
			in.Name,
		),
		validateQuery(in.Query),
		validDuration("pendingDuration", in.Pending, maxPending),
		validDuration("repeatNotificationDuration", in.Repeat, maxRepeat),
		validSuppression(in.Suppression),
		validResolution(in.Resolution),
	); aerr != nil {
		return AlarmWire{}, "", aerr
	}

	format, aerr := validFormat(in.MessageFormat, in.Destinations)
	if aerr != nil {
		return AlarmWire{}, "", aerr
	}

	b.mu.Lock("CreateAlarm")
	defer b.mu.Unlock()

	if aerr = b.validateDestinations(in.Destinations); aerr != nil {
		return AlarmWire{}, "", aerr
	}

	now := b.nowUTC()
	a := &Alarm{
		ID: newOCID("alarm"), Name: in.Name, CompartmentID: in.Compartment, MetricCompartmentID: in.MetricComp,
		Namespace: in.Namespace, ResourceGroup: strVal(in.ResourceGroup), Query: in.Query, Severity: sev,
		Body: strVal(in.Body), Resolution: onlyResolution, PendingDuration: orDefault(in.Pending, defaultPending),
		RepeatNotification: strVal(in.Repeat), MessageFormat: format, Destinations: in.Destinations,
		Enabled: *in.Enabled, InSubtree: in.InSubtree, PerDimension: in.PerDimension, Overrides: in.Overrides,
		Suppression: suppressionSpec(in.Suppression), Tags: in.Tags, Status: statusOK, StatusSince: now,
		TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Seq: b.nextSeq(),
	}

	for k, v := range map[string]*string{
		"ruleName": in.RuleName, "notificationVersion": in.NotifVersion, "notificationTitle": in.NotifTitle,
		"evaluationSlackDuration": in.EvalSlack, "alarmSummary": in.AlarmSummary,
	} {
		setExtra(a, k, v)
	}

	a.History = []HistoryEntry{
		{Timestamp: now, TimestampTriggered: now, To: statusOK, Summary: "The alarm state is OK"},
	}
	b.alarms[a.ID] = a

	return alarmView(a), a.Etag, nil
}

func validResolution(r *string) *APIError {
	if r != nil && *r != onlyResolution {
		return errParam("resolution must be 1m")
	}

	return nil
}

func validName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

func orDefault(p *string, d string) string {
	if p == nil || *p == "" {
		return d
	}

	return *p
}

func suppressionSpec(s *SuppressionWire) *SuppressionSpec {
	if s == nil {
		return nil
	}

	return &SuppressionSpec{
		From:        s.TimeSuppressFrom.UTC(),
		Until:       s.TimeSuppressUntil.UTC(),
		Description: s.Description,
	}
}

func (b *InMemoryBackend) liveAlarm(id string) (*Alarm, *APIError) {
	a, ok := b.alarms[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return a, nil
}

// GetAlarm returns one alarm and its etag.
func (b *InMemoryBackend) GetAlarm(id string) (AlarmWire, string, error) {
	b.mu.RLock("GetAlarm")
	defer b.mu.RUnlock()

	a, aerr := b.liveAlarm(id)
	if aerr != nil {
		return AlarmWire{}, "", aerr
	}

	return alarmView(a), a.Etag, nil
}

// AlarmFilter narrows ListAlarms and ListAlarmsStatus.
type AlarmFilter struct {
	Compartment, Name, State string
}

func (b *InMemoryBackend) filtered(f AlarmFilter) ([]*Alarm, *APIError) {
	if f.Compartment == "" {
		return nil, errMissing("compartmentId is required")
	}

	var out []*Alarm

	for _, a := range b.alarms {
		if a.CompartmentID == f.Compartment && (f.Name == "" || a.Name == f.Name) &&
			(f.State == "" || strs.Equal(f.State, stateActive)) {
			out = append(out, a)
		}
	}

	return out, nil
}

func compareAlarms(a, c *Alarm, by string) int {
	if by == "severity" {
		return severityRank(a.Severity) - severityRank(c.Severity)
	}

	if by == "displayName" {
		return compareStr(a.Name, c.Name)
	}

	return int(a.Seq - c.Seq)
}

func severityRank(s string) int {
	return slices.Index([]string{"CRITICAL", "ERROR", "WARNING", "INFO"}, s)
}

// ListAlarms lists the alarms of a compartment.
func (b *InMemoryBackend) ListAlarms(f AlarmFilter, o ListOpts) (page.Page[AlarmSummaryWire], error) {
	b.mu.RLock("ListAlarms")
	defer b.mu.RUnlock()

	all, aerr := b.filtered(f)
	if aerr != nil {
		return page.Page[AlarmSummaryWire]{}, aerr
	}

	pg := paginate(all, o, compareAlarms, "displayName")

	out := make([]AlarmSummaryWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		out = append(out, AlarmSummaryWire{
			ID: a.ID, Name: a.Name, CompartmentID: a.CompartmentID, MetricComp: a.MetricCompartmentID,
			Namespace: a.Namespace, ResourceGroup: a.ResourceGroup, Query: a.Query, Severity: a.Severity,
			LifecycleState: stateActive, Destinations: a.Destinations, Enabled: a.Enabled,
			Suppression: suppressionView(a.Suppression), PerDimension: a.PerDimension,
			FreeformTags: a.Tags.FreeformTags, DefinedTags: a.Tags.DefinedTags,
		})
	}

	return page.Page[AlarmSummaryWire]{Data: out, Next: pg.Next}, nil
}

// ListAlarmsStatus lists the evaluation status of the alarms of a compartment.
func (b *InMemoryBackend) ListAlarmsStatus(f AlarmFilter, o ListOpts) (page.Page[AlarmStatusWire], error) {
	b.mu.RLock("ListAlarmsStatus")
	defer b.mu.RUnlock()

	all, aerr := b.filtered(f)
	if aerr != nil {
		return page.Page[AlarmStatusWire]{}, aerr
	}

	pg := paginate(all, o, compareAlarms, "displayName")
	now := b.nowUTC()

	out := make([]AlarmStatusWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		status := a.Status
		if a.suppressedAt(now) {
			status = statusSuspended
		}

		out = append(out, AlarmStatusWire{
			ID: a.ID, Name: a.Name, Severity: a.Severity, Status: status, TimestampTriggered: a.StatusSince,
			Suppression: suppressionView(a.Suppression),
		})
	}

	return page.Page[AlarmStatusWire]{Data: out, Next: pg.Next}, nil
}

func (a *Alarm) suppressedAt(now time.Time) bool {
	return a.Suppression != nil && !now.Before(a.Suppression.From) && now.Before(a.Suppression.Until)
}

// UpdateAlarm changes the supplied fields of an alarm.
func (b *InMemoryBackend) UpdateAlarm(id, ifMatch string, in UpdateAlarmIn) (AlarmWire, string, error) {
	if aerr := firstErr(
		validDuration("pendingDuration", in.Pending, maxPending),
		validDuration("repeatNotificationDuration", in.Repeat, maxRepeat),
		validSuppression(in.Suppression), validResolution(in.Resolution), optName(in.Name), optQuery(in.Query),
	); aerr != nil {
		return AlarmWire{}, "", aerr
	}

	b.mu.Lock("UpdateAlarm")
	defer b.mu.Unlock()

	a, aerr := b.liveAlarm(id)
	if aerr != nil {
		return AlarmWire{}, "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return AlarmWire{}, "", aerr
	}

	if aerr = b.applyAlarmUpdate(a, in); aerr != nil {
		return AlarmWire{}, "", aerr
	}

	a.TimeUpdated, a.Etag = b.nowUTC(), newEtag()

	return alarmView(a), a.Etag, nil
}

func optName(n *string) *APIError {
	if n == nil {
		return nil
	}

	return validName(*n)
}

func optQuery(q *string) *APIError {
	if q == nil {
		return nil
	}

	return validateQuery(*q)
}

func (b *InMemoryBackend) applyAlarmUpdate(a *Alarm, in UpdateAlarmIn) *APIError {
	dests := a.Destinations
	if in.Destinations != nil {
		dests = in.Destinations
		if aerr := b.validateDestinations(dests); aerr != nil {
			return aerr
		}
	}

	format := a.MessageFormat
	if in.MessageFormat != nil || in.Destinations != nil {
		f := in.MessageFormat
		if f == nil {
			f = &a.MessageFormat
		}

		var aerr *APIError
		if format, aerr = validFormat(f, dests); aerr != nil {
			return aerr
		}
	}

	if in.Severity != nil {
		sev, aerr := validSeverity(*in.Severity)
		if aerr != nil {
			return aerr
		}

		a.Severity = sev
	}

	a.Destinations, a.MessageFormat = dests, format
	applyScalars(a, in)
	a.Tags.merge(in.Tags)

	return nil
}

// applyScalars copies the optional plain fields of an update onto the alarm.
func applyScalars(a *Alarm, in UpdateAlarmIn) {
	assign(&a.Name, in.Name)
	assign(&a.CompartmentID, in.Compartment)
	assign(&a.MetricCompartmentID, in.MetricComp)
	assign(&a.Namespace, in.Namespace)
	assign(&a.ResourceGroup, in.ResourceGroup)
	assign(&a.Body, in.Body)
	assign(&a.PendingDuration, in.Pending)
	assign(&a.RepeatNotification, in.Repeat)
	assign(&a.Query, in.Query)

	if in.Enabled != nil {
		a.Enabled = *in.Enabled
	}

	if in.InSubtree != nil {
		a.InSubtree = in.InSubtree
	}

	if in.PerDimension != nil {
		a.PerDimension = in.PerDimension
	}

	if in.Suppression != nil {
		a.Suppression = suppressionSpec(in.Suppression)
	}

	if in.Overrides != nil {
		a.Overrides = in.Overrides
	}

	for k, v := range map[string]*string{
		"ruleName": in.RuleName, "notificationVersion": in.NotifVersion, "notificationTitle": in.NotifTitle,
		"evaluationSlackDuration": in.EvalSlack, "alarmSummary": in.AlarmSummary,
	} {
		setExtra(a, k, v)
	}
}

func assign(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}

// DeleteAlarm removes an alarm.
func (b *InMemoryBackend) DeleteAlarm(id, ifMatch string) error {
	b.mu.Lock("DeleteAlarm")
	defer b.mu.Unlock()

	a, aerr := b.liveAlarm(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.alarms, id)

	return nil
}

// ChangeAlarmCompartment moves an alarm to another compartment.
func (b *InMemoryBackend) ChangeAlarmCompartment(id, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeAlarmCompartment")
	defer b.mu.Unlock()

	a, aerr := b.liveAlarm(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return aerr
	}

	a.CompartmentID, a.Etag, a.TimeUpdated = compartment, newEtag(), b.nowUTC()

	return nil
}

type historyEntryWire struct {
	Timestamp          time.Time  `json:"timestamp"`
	TimestampTriggered *time.Time `json:"timestampTriggered,omitempty"`
	AlarmSummary       string     `json:"alarmSummary"`
	Summary            string     `json:"summary"`
}

// AlarmHistoryWire is the GetAlarmHistory response body.
type AlarmHistoryWire struct {
	AlarmID   string             `json:"alarmId"`
	Entries   []historyEntryWire `json:"entries"`
	IsEnabled bool               `json:"isEnabled"`
}

// HistoryFilter narrows GetAlarmHistory.
type HistoryFilter struct {
	From, To *time.Time
	Type     string
}

func (b *InMemoryBackend) GetAlarmHistory(id string, f HistoryFilter, o ListOpts) (AlarmHistoryWire, string, error) {
	switch f.Type {
	case "", "STATE_HISTORY", "STATE_TRANSITION_HISTORY", "RULE_HISTORY", "RULE_TRANSITION_HISTORY":
	default:
		return AlarmHistoryWire{}, "", errParam("alarmHistorytype is not a valid history type")
	}

	b.mu.RLock("GetAlarmHistory")
	defer b.mu.RUnlock()

	a, aerr := b.liveAlarm(id)
	if aerr != nil {
		return AlarmHistoryWire{}, "", aerr
	}

	pg := page.New(historyEntries(a, f), o.Page, o.Limit, defaultListLimit)
	if pg.Data == nil {
		pg.Data = []historyEntryWire{}
	}

	return AlarmHistoryWire{AlarmID: a.ID, Entries: pg.Data, IsEnabled: a.Enabled}, pg.Next, nil
}

// historyEntries renders the recorded transitions as state history and/or state transition history entries.
func historyEntries(a *Alarm, f HistoryFilter) []historyEntryWire {
	summary := a.Name
	if v := extra(a, "alarmSummary"); v != nil {
		summary = *v
	}

	var all []historyEntryWire

	for _, h := range a.History {
		if (f.From != nil && h.Timestamp.Before(*f.From)) || (f.To != nil && !h.Timestamp.Before(*f.To)) {
			continue
		}

		if f.Type == "" || f.Type == "STATE_HISTORY" {
			all = append(all, historyEntryWire{Timestamp: h.Timestamp, AlarmSummary: summary, Summary: h.Summary})
		}

		if h.From != "" && (f.Type == "" || f.Type == "STATE_TRANSITION_HISTORY") {
			trig := h.TimestampTriggered
			all = append(all, historyEntryWire{
				Timestamp: h.Timestamp, TimestampTriggered: &trig, AlarmSummary: summary,
				Summary: "State transitioned from " + titleCase(h.From) + " to " + titleCase(h.To),
			})
		}
	}

	return all
}

func titleCase(s string) string {
	if s == "" {
		return s
	}

	return s[:1] + strings.ToLower(s[1:])
}

func (h *Handler) createAlarm(q *req) error {
	var in CreateAlarmIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateAlarm(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getAlarm(q *req) error {
	v, etag, err := h.Backend.GetAlarm(q.alarmID())

	return reply(q, v, etag, err)
}

func (h *Handler) alarmFilter(q *req) AlarmFilter {
	return AlarmFilter{
		Compartment: q.query("compartmentId"),
		Name:        q.query("displayName"),
		State:       q.query("lifecycleState"),
	}
}

func (h *Handler) listAlarms(q *req) error {
	pg, err := h.Backend.ListAlarms(h.alarmFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) listAlarmsStatus(q *req) error {
	pg, err := h.Backend.ListAlarmsStatus(h.alarmFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateAlarm(q *req) error {
	var in UpdateAlarmIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateAlarm(q.alarmID(), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteAlarm(q *req) error {
	if err := h.Backend.DeleteAlarm(q.alarmID(), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) changeAlarmCompartment(q *req) error {
	var in struct {
		CompartmentID string `json:"compartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeAlarmCompartment(q.alarmID(), q.ifMatch(), in.CompartmentID); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}

func parseQueryTime(raw string) (*time.Time, *APIError) {
	if raw == "" {
		return nil, nil
	}

	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, errParam("timestamps must be RFC 3339")
	}

	return &t, nil
}

func (h *Handler) getAlarmHistory(q *req) error {
	from, aerr := parseQueryTime(q.query("timestampGreaterThanOrEqualTo"))
	if aerr != nil {
		return aerr
	}

	to, aerr := parseQueryTime(q.query("timestampLessThan"))
	if aerr != nil {
		return aerr
	}

	out, next, err := h.Backend.GetAlarmHistory(q.alarmID(),
		HistoryFilter{From: from, To: to, Type: q.query("alarmHistorytype")}, q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(next)

	return q.json(http.StatusOK, out, "")
}
