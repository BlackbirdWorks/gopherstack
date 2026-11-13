package gcpmonitoring

import (
	"context"
	"encoding/json/v2"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	promEvalStep     = 30 * time.Second
	maxRuleGroupLen  = 2048
	defaultAutoClose = 7 * 24 * time.Hour
)

var (
	promAlertNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	tmplLabelRe     = regexp.MustCompile(`\{\{\s*\$labels\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}`)
	tmplValueRe     = regexp.MustCompile(`\{\{\s*\$value\s*\}\}`)
)

// LogEntry is one Cloud Logging entry as the field tree of its JSON form (camelCase names).
type LogEntry struct {
	Timestamp time.Time
	Fields    map[string]any
	InsertID  string
}

// LogEntrySource reads log entries matching a Cloud Logging filter from a project, oldest first.
// It is the read-only seam to Cloud Logging for condition_matched_log.
type LogEntrySource interface {
	LogEntries(project, filter string, notBefore time.Time) []LogEntry
}

// SetLogEntrySource wires log entries from Cloud Logging for log-match alert conditions.
func (b *InMemoryBackend) SetLogEntrySource(s LogEntrySource) {
	b.mu.Lock("SetLogEntrySource")
	b.entries = s
	b.mu.Unlock()
}

type pendingAlert struct {
	since  time.Time
	labels promql.Labels
	value  float64
}

// condRuntime is the volatile evaluation state of one alert condition.
type condRuntime struct {
	lastEval time.Time
	lastNote time.Time
	logMark  time.Time
	active   map[string]*pendingAlert
	logSeen  map[string]struct{}
	obs      []observation
}

func (b *InMemoryBackend) runtimeLocked(cond string) *condRuntime {
	rt, ok := b.condRT[cond]
	if !ok {
		rt = &condRuntime{active: map[string]*pendingAlert{}, logSeen: map[string]struct{}{}}
		b.condRT[cond] = rt
	}

	return rt
}

type promRun struct {
	vec promql.Vector
	ran bool
}

// external holds the results of conditions that are evaluated outside the backend lock.
type external struct {
	prom map[string]promRun
	logs map[string][]LogEntry
}

func promIntervalOf(c *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition) time.Duration {
	if d := c.GetEvaluationInterval().AsDuration(); d > 0 {
		return d
	}

	return promEvalStep
}

type extJob struct {
	mark    time.Time
	cond    *policyCondition
	project string
}

// collectJobsLocked lists the PromQL conditions that are due and the log conditions with their read marks.
type jobSet struct {
	src   LogEntrySource
	proms []extJob
	logs  []extJob
}

func (b *InMemoryBackend) collectJobs(now time.Time) jobSet {
	var js jobSet

	b.mu.RLock("collectJobs")
	defer b.mu.RUnlock()

	for _, name := range sortedKeys(b.policies) {
		p := b.policies[name]
		if !policyEnabled(p) {
			continue
		}

		project, _, _ := splitName(name, collAlertPolicies)

		for _, c := range p.GetConditions() {
			switch t := c.GetCondition().(type) {
			case *monitoringpb.AlertPolicy_Condition_ConditionPrometheusQueryLanguage:
				rt := b.condRT[c.GetName()]

				every := promIntervalOf(t.ConditionPrometheusQueryLanguage)
				if rt == nil || rt.lastEval.IsZero() || now.Sub(rt.lastEval) >= every {
					js.proms = append(js.proms, extJob{cond: c, project: project})
				}
			case *monitoringpb.AlertPolicy_Condition_ConditionMatchedLog:
				mark := p.GetCreationRecord().GetMutateTime().AsTime()
				if rt := b.condRT[c.GetName()]; rt != nil && !rt.logMark.IsZero() {
					mark = rt.logMark
				}

				js.logs = append(js.logs, extJob{cond: c, project: project, mark: mark})
			}
		}
	}

	js.src = b.entries

	return js
}

func (b *InMemoryBackend) runExternal(ctx context.Context, now time.Time) external {
	js := b.collectJobs(now)
	ext := external{prom: map[string]promRun{}, logs: map[string][]LogEntry{}}
	stores := map[string]promStorage{}

	for _, j := range js.proms {
		st, ok := stores[j.project]
		if !ok {
			st = b.promStorageFor(j.project)
			stores[j.project] = st
		}

		q := j.cond.GetConditionPrometheusQueryLanguage().GetQuery()

		v, err := (&promql.Engine{Ctx: ctx, Store: st}).Instant(q, now)
		if err != nil {
			continue
		}

		vec, _ := v.(promql.Vector)
		ext.prom[j.cond.GetName()] = promRun{vec: vec, ran: true}
	}

	if js.src == nil {
		return ext
	}

	for _, j := range js.logs {
		ext.logs[j.cond.GetName()] = js.src.LogEntries(j.project, j.cond.GetConditionMatchedLog().GetFilter(), j.mark)
	}

	return ext
}

func expandTemplate(tmpl string, labels promql.Labels, value float64) string {
	out := tmplLabelRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		return labels[tmplLabelRe.FindStringSubmatch(m)[1]]
	})

	return tmplValueRe.ReplaceAllString(out, formatPromValue(value))
}

func promAlertLabels(
	c *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition,
	s promql.VectorSample,
) promql.Labels {
	l := promql.Labels{}

	maps.Copy(l, s.Metric)

	for k, v := range c.GetLabels() {
		l[k] = expandTemplate(v, s.Metric, s.V)
	}

	return l
}

func observationOf(id string, l promql.Labels, value float64, met bool) observation {
	ml := map[string]string{}

	for k, v := range l {
		if k != promql.NameLabel && k != labelMonitoredRs {
			ml[k] = v
		}
	}

	return observation{
		id: id, resKey: id, metricType: l[promql.NameLabel], metricLabels: ml,
		resourceType: l[labelMonitoredRs], value: value, met: met,
	}
}

// promObservationsLocked turns a PromQL evaluation into per-alert observations; an alert fires once it has
// been active for the condition's duration.
func (b *InMemoryBackend) promObservationsLocked(
	c *policyCondition, pc *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition,
	run promRun, now time.Time,
) []observation {
	rt := b.runtimeLocked(c.GetName())
	if !run.ran {
		return rt.obs
	}

	rt.lastEval = now
	seen := map[string]bool{}
	obs := make([]observation, 0, len(run.vec))

	for _, s := range run.vec {
		labels := promAlertLabels(pc, s)
		id := "prom|" + promql.Fingerprint(labels)
		seen[id] = true

		st, ok := rt.active[id]
		if !ok {
			st = &pendingAlert{since: now}
			rt.active[id] = st
		}

		st.labels, st.value = labels, s.V
		obs = append(obs, observationOf(id, labels, s.V, now.Sub(st.since) >= pc.GetDuration().AsDuration()))
	}

	for id, st := range rt.active {
		if !seen[id] {
			obs = append(obs, observationOf(id, st.labels, st.value, false))
			delete(rt.active, id)
		}
	}

	sort.Slice(obs, func(i, j int) bool { return obs[i].id < obs[j].id })
	rt.obs = obs

	return obs
}

type extractor struct {
	re   *regexp.Regexp
	path []string
}

var (
	extractRe       = regexp.MustCompile(`^\s*EXTRACT\(\s*([^)\s]+)\s*\)\s*$`)
	regexpExtractRe = regexp.MustCompile(`^\s*REGEXP_EXTRACT\(\s*([^,\s]+)\s*,\s*"((?:[^"\\]|\\.)*)"\s*\)\s*$`)
)

func parseLogExtractor(src string) (*extractor, bool) {
	if m := extractRe.FindStringSubmatch(src); m != nil {
		return &extractor{path: strings.Split(m[1], ".")}, true
	}

	if m := regexpExtractRe.FindStringSubmatch(src); m != nil {
		re, err := regexp.Compile(m[2])
		if err != nil {
			return nil, false
		}

		return &extractor{path: strings.Split(m[1], "."), re: re}, true
	}

	return nil, false
}

func (x *extractor) value(fields map[string]any) (string, bool) {
	var cur any = fields

	for _, seg := range x.path {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}

		if cur, ok = m[seg]; !ok {
			return "", false
		}
	}

	s := scalarText(cur)
	if x.re == nil {
		return s, true
	}

	m := x.re.FindStringSubmatch(s)

	switch {
	case m == nil:
		return "", false
	case len(m) > 1:
		return m[1], true
	}

	return m[0], true
}

func scalarText(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	}

	raw, _ := json.Marshal(v, jsonx.V1())

	return string(raw)
}

func validateLogExtractors(path string, m map[string]string) error {
	for k, v := range m {
		if _, ok := parseLogExtractor(v); !ok {
			return errInvalid("Field %s.label_extractors[%s] had an invalid value of %q: use EXTRACT(field) or "+
				"REGEXP_EXTRACT(field, \"regex\").", path, k, v)
		}
	}

	return nil
}

func extractedLabels(m map[string]string, e LogEntry) map[string]string {
	out := map[string]string{}

	for k, src := range m {
		if x, ok := parseLogExtractor(src); ok {
			if v, found := x.value(e.Fields); found {
				out[k] = v
			}
		}
	}

	return out
}

// logConditionsLocked opens an incident per distinct combination of extracted labels for new matching entries and
// closes incidents that saw no match for the auto-close period.
func (b *InMemoryBackend) logConditionsLocked(
	project string, p *monitoringpb.AlertPolicy, ext external, now time.Time,
) []notification {
	var out []notification

	autoClose := defaultAutoClose
	if d := p.GetAlertStrategy().GetAutoClose().AsDuration(); d > 0 {
		autoClose = d
	}

	limit := p.GetAlertStrategy().GetNotificationRateLimit().GetPeriod().AsDuration()

	for _, c := range p.GetConditions() {
		lm := c.GetConditionMatchedLog()
		if lm == nil {
			continue
		}

		rt := b.runtimeLocked(c.GetName())
		out = append(out, b.matchLogEntriesLocked(project, p, c, rt, ext.logs[c.GetName()], limit, now)...)
		b.autoCloseLocked(p, c, autoClose, now)
	}

	return out
}

type logScope struct {
	p       *monitoringpb.AlertPolicy
	c       *policyCondition
	rt      *condRuntime
	project string
	limit   time.Duration
}

func (b *InMemoryBackend) matchLogEntriesLocked(
	project string, p *monitoringpb.AlertPolicy, c *policyCondition, rt *condRuntime, hits []LogEntry,
	limit time.Duration, now time.Time,
) []notification {
	var out []notification

	if rt.logMark.IsZero() {
		rt.logMark = p.GetCreationRecord().GetMutateTime().AsTime()
	}

	mark := rt.logMark
	sc := logScope{project: project, p: p, c: c, rt: rt, limit: limit}

	for _, e := range hits {
		if e.Timestamp.Before(rt.logMark) {
			continue
		}

		if _, dup := rt.logSeen[e.InsertID]; dup && e.Timestamp.Equal(rt.logMark) {
			continue
		}

		if e.Timestamp.After(mark) {
			mark = e.Timestamp
		}

		if n, ok := b.openLogIncidentLocked(sc, e, now); ok {
			out = append(out, n)
		}
	}

	if mark.After(rt.logMark) {
		rt.logMark = mark
		clear(rt.logSeen)
	}

	for _, e := range hits {
		if e.Timestamp.Equal(rt.logMark) {
			rt.logSeen[e.InsertID] = struct{}{}
		}
	}

	return out
}

// openLogIncidentLocked records one matching entry: it refreshes the open incident of its label combination or
// opens a new one, and reports the notification to send, if any.
func (b *InMemoryBackend) openLogIncidentLocked(sc logScope, e LogEntry, now time.Time) (notification, bool) {
	labels := extractedLabels(sc.c.GetConditionMatchedLog().GetLabelExtractors(), e)
	o := observationOf("log|"+promql.Fingerprint(labels), labels, 0, true)
	key := sc.p.GetName() + "|" + sc.c.GetName() + "|" + o.id

	if inc := b.openIncidentLocked(key); inc != nil {
		inc.LastSeen = now

		return notification{}, false
	}

	if b.snoozedLocked(sc.p.GetName(), o, now) {
		return notification{}, false
	}

	inc := b.newIncidentLocked(sc.project, sc.p, sc.c, key, o, now)
	inc.LastSeen = now
	b.incidents[inc.ID].LastSeen = now

	opens, _ := policyPrompts(sc.p)
	limited := sc.limit > 0 && !sc.rt.lastNote.IsZero() && now.Sub(sc.rt.lastNote) < sc.limit

	if !opens || limited {
		return notification{}, false
	}

	sc.rt.lastNote = now

	return b.notificationLocked(sc.p, inc, true), true
}

func (b *InMemoryBackend) autoCloseLocked(
	p *monitoringpb.AlertPolicy,
	c *policyCondition,
	after time.Duration,
	now time.Time,
) {
	for _, inc := range b.incidents {
		if inc.State != stateOpen || inc.Policy != p.GetName() || inc.Condition != c.GetName() {
			continue
		}

		last := inc.LastSeen
		if last.IsZero() {
			last = inc.StartedAt
		}

		if now.Sub(last) >= after {
			inc.State, inc.EndedAt = stateClosed, now
		}
	}
}

func validatePromCondition(path string, c *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition) error {
	if c.GetQuery() == "" {
		return errInvalid("Field %s.query had an invalid value: a query is required.", path)
	}

	if _, err := promql.Parse(c.GetQuery()); err != nil {
		return errInvalid("Field %s.query had an invalid value of %q: %s", path, c.GetQuery(), err.Error())
	}

	if d := c.GetDuration(); d != nil && d.AsDuration() < 0 {
		return errInvalid("Field %s.duration had an invalid value: the duration must not be negative.", path)
	}

	if iv := c.GetEvaluationInterval(); iv != nil {
		if d := iv.AsDuration(); d <= 0 || d%promEvalStep != 0 {
			return errInvalid("Field %s.evaluation_interval had an invalid value of %q: it must be a positive "+
				"multiple of 30 seconds.", path, d)
		}
	}

	for k := range c.GetLabels() {
		if !promAlertNameRe.MatchString(k) {
			return errInvalid("Field %s.labels had an invalid label name %q.", path, k)
		}
	}

	if len(c.GetRuleGroup()) > maxRuleGroupLen {
		return errInvalid("Field %s.rule_group had an invalid value: at most %d characters.", path, maxRuleGroupLen)
	}

	if r := c.GetAlertRule(); r != "" && (len(r) > maxRuleGroupLen || !promAlertNameRe.MatchString(r)) {
		return errInvalid("Field %s.alert_rule had an invalid value of %q: it must be a valid Prometheus label name.",
			path, r)
	}

	return nil
}
