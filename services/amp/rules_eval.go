package amp

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	metricAlerts      = "ALERTS"
	metricAlertsState = "ALERTS_FOR_STATE"
	labelAlertName    = "alertname"
	labelAlertState   = "alertstate"

	healthOK      = "ok"
	healthErr     = "err"
	healthUnknown = "unknown"

	stateInactive = "inactive"
	statePending  = "pending"
	stateFiring   = "firing"

	resolvedRetention = 15 * time.Minute
	resendDelay       = time.Minute
	minResolveTimeout = time.Minute
	endsAtIntervals   = 4
	restoreLookback   = 5 * time.Minute
	maxAlertsPerRule  = 100000
)

var (
	errLimitExceeded = errors.New("exceeded limit")
	errDupLabelset   = errors.New("vector contains metrics with the same labelset after applying rule labels")
)

type alertInstance struct {
	activeAt        time.Time
	firedAt         time.Time
	resolvedAt      time.Time
	lastSent        time.Time
	keepFiringSince time.Time
	labels          promql.Labels
	annotations     map[string]string
	state           string
	value           float64
}

type ruleRuntime struct {
	lastEval time.Time
	alerts   map[string]*alertInstance
	prevOut  map[string]promql.Labels
	health   string
	lastErr  string
	rule     parsedRule
	evalDur  time.Duration
}

type groupRuntime struct {
	next      time.Time
	lastEval  time.Time
	wsID      string
	ns        string
	src       []byte
	rules     []*ruleRuntime
	def       parsedGroup
	lastDur   time.Duration
	evaluated bool
}

func groupKey(ws, ns, name string) string { return ws + "|" + ns + "|" + name }

func newGroupRuntime(ws, ns string, src []byte, def parsedGroup, now time.Time) *groupRuntime {
	g := &groupRuntime{wsID: ws, ns: ns, src: src, def: def, next: now}

	for _, pr := range def.rules {
		g.rules = append(g.rules, &ruleRuntime{
			rule: pr, health: healthUnknown, alerts: map[string]*alertInstance{}, prevOut: map[string]promql.Labels{},
		})
	}

	return g
}

// Alert is one alert handed from a rule to the workspace's alert manager.
type Alert struct {
	StartsAt     time.Time
	EndsAt       time.Time
	Labels       promql.Labels
	Annotations  map[string]string
	GeneratorURL string
}

type alertBatch struct {
	wsID   string
	alerts []Alert
}

// syncGroupsLocked reconciles the rule runtimes with the stored namespaces.
func (b *InMemoryBackend) syncGroupsLocked(now time.Time) {
	live := map[string]bool{}

	for _, n := range b.namespaces.All() {
		if !b.nsRunnable(n) {
			continue
		}

		for _, def := range b.parsedFor(n) {
			k := groupKey(n.WorkspaceID, n.Name, def.name)
			live[k] = true

			if g, ok := b.groups[k]; ok && bytes.Equal(g.src, n.Doc.Data) {
				continue
			}

			b.groups[k] = newGroupRuntime(n.WorkspaceID, n.Name, n.Doc.Data, def, now)
		}
	}

	for k := range b.groups {
		if !live[k] {
			delete(b.groups, k)
		}
	}

	for k := range b.parsed {
		if _, ok := b.namespaces.Get(k); !ok {
			delete(b.parsed, k)
		}
	}
}

func (b *InMemoryBackend) nsRunnable(n *RuleGroupsNamespace) bool {
	if n.Doc.Pending.Kind == statusDeleting || b.state(n.Doc.Pending) == statusCreating {
		return false
	}

	w, ok := b.workspaces.Get(n.WorkspaceID)

	return ok && w.Pending.Kind != statusDeleting && b.state(w.Pending) == statusActive
}

// evaluateRulesLocked runs every due group and returns the alerts to forward.
func (b *InMemoryBackend) evaluateRulesLocked(now time.Time) []alertBatch {
	b.syncGroupsLocked(now)

	var batches []alertBatch

	for _, g := range b.groups {
		if now.Before(g.next) {
			continue
		}

		if alerts := b.evalGroupLocked(g, now); len(alerts) > 0 {
			batches = append(batches, alertBatch{wsID: g.wsID, alerts: alerts})
		}
	}

	return batches
}

func (b *InMemoryBackend) queryOffset(g *groupRuntime) time.Duration {
	if g.def.queryOffset > 0 {
		return g.def.queryOffset
	}

	if w, ok := b.workspaces.Get(g.wsID); ok {
		return time.Duration(w.Config.RuleQueryOffsetInSeconds) * time.Second
	}

	return 0
}

func (b *InMemoryBackend) evalGroupLocked(g *groupRuntime, now time.Time) []Alert {
	start := time.Now()
	qt := now.Add(-b.queryOffset(g))

	alerts := make([]Alert, 0, len(g.rules))

	for _, r := range g.rules {
		alerts = append(alerts, b.evalRuleLocked(g, r, now, qt)...)
	}

	g.evaluated = true
	g.lastEval = now
	g.lastDur = time.Since(start)
	g.next = now.Add(g.def.interval)

	return alerts
}

func (b *InMemoryBackend) evalRuleLocked(g *groupRuntime, r *ruleRuntime, now, qt time.Time) []Alert {
	start := time.Now()
	eng := &promql.Engine{Store: wsStorage{b: b, wsID: g.wsID}}

	var (
		alerts []Alert
		err    error
	)

	vec, err := evalVector(eng, r.rule.expr, qt)
	if err == nil {
		if r.rule.def.Record != "" {
			err = b.recordLocked(g, r, vec, now)
		} else {
			alerts, err = b.alertLocked(g, r, vec, now)
		}
	}

	r.lastEval = now
	r.evalDur = time.Since(start)
	r.health, r.lastErr = healthOK, ""

	if err != nil {
		r.health, r.lastErr = healthErr, err.Error()
	}

	return alerts
}

func evalVector(eng *promql.Engine, expr promql.Expr, qt time.Time) (promql.Vector, error) {
	v, err := eng.InstantExpr(expr, qt)
	if err != nil {
		return nil, err
	}

	switch x := v.(type) {
	case promql.Vector:
		return x, nil
	case promql.Scalar:
		return promql.Vector{{Metric: promql.Labels{}, T: x.T, V: x.V}}, nil
	}

	return nil, fmt.Errorf("%w: rule expression returned %s", promql.ErrEval, promql.ResultType(v))
}

// writeOutputsLocked stores this round's samples and ends the series that disappeared since the last one.
func (b *InMemoryBackend) writeOutputsLocked(
	g *groupRuntime,
	r *ruleRuntime,
	outs []promql.Labels,
	vals []float64,
	now time.Time,
) {
	t := now.UnixMilli()
	cur := make(map[string]promql.Labels, len(outs))

	for i, l := range outs {
		cur[promql.Fingerprint(l)] = l
		b.appendSampleLocked(g.wsID, l, promql.Sample{T: t, V: vals[i]})
	}

	for fp, l := range r.prevOut {
		if _, still := cur[fp]; !still {
			b.appendSampleLocked(g.wsID, l, promql.Sample{T: t, V: promql.StaleNaN()})
		}
	}

	r.prevOut = cur
}

func (b *InMemoryBackend) recordLocked(g *groupRuntime, r *ruleRuntime, vec promql.Vector, now time.Time) error {
	if g.def.limit > 0 && len(vec) > g.def.limit {
		return fmt.Errorf("%w: %d series, limit %d", errLimitExceeded, len(vec), g.def.limit)
	}

	if len(vec) > maxAlertsPerRule {
		return errLimitExceeded
	}

	outs := make([]promql.Labels, 0, len(vec))
	vals := make([]float64, 0, len(vec))
	seen := map[string]bool{}

	for _, s := range vec {
		if !isFinite(s.V) {
			continue
		}

		l := s.Metric.Clone()
		l[promql.NameLabel] = r.rule.def.Record
		maps.Copy(l, r.rule.def.Labels)

		fp := promql.Fingerprint(l)
		if seen[fp] {
			return errDupLabelset
		}

		seen[fp] = true

		outs = append(outs, l)
		vals = append(vals, s.V)
	}

	b.writeOutputsLocked(g, r, outs, vals, now)

	return nil
}

type activeAlert struct {
	labels      promql.Labels
	annotations map[string]string
	value       float64
}

func expandRule(r *ruleRuntime, s promql.VectorSample) activeAlert {
	base := s.Metric.Clone()
	base[labelAlertName] = r.rule.def.Alert
	data := templateData{Labels: base, Value: s.V, ExternalLabels: map[string]string{}}

	lbls := s.Metric.Clone()
	lbls[labelAlertName] = r.rule.def.Alert

	for k, v := range r.rule.def.Labels {
		lbls[k] = expandTemplate(v, data)
	}

	delete(lbls, promql.NameLabel)

	ann := make(map[string]string, len(r.rule.def.Annotations))
	for k, v := range r.rule.def.Annotations {
		ann[k] = expandTemplate(v, data)
	}

	return activeAlert{labels: lbls, annotations: ann, value: s.V}
}

func (b *InMemoryBackend) alertLocked(
	g *groupRuntime,
	r *ruleRuntime,
	vec promql.Vector,
	now time.Time,
) ([]Alert, error) {
	if g.def.limit > 0 && len(vec) > g.def.limit {
		return nil, fmt.Errorf("%w: %d alerts, limit %d", errLimitExceeded, len(vec), g.def.limit)
	}

	if len(vec) > maxAlertsPerRule {
		return nil, errLimitExceeded
	}

	active := make(map[string]activeAlert, len(vec))

	for _, s := range vec {
		a := expandRule(r, s)
		fp := promql.Fingerprint(a.labels)

		if _, dup := active[fp]; dup {
			return nil, errDupLabelset
		}

		active[fp] = a
	}

	b.advanceAlerts(g, r, active, now)
	b.writeAlertSeries(g, r, now)

	return collectAlerts(g, r, now), nil
}

func (b *InMemoryBackend) advanceAlerts(g *groupRuntime, r *ruleRuntime, active map[string]activeAlert, now time.Time) {
	for fp, a := range active {
		inst, ok := r.alerts[fp]
		if !ok || inst.state == stateInactive {
			inst = &alertInstance{labels: a.labels, activeAt: now, state: statePending}
			if !g.evaluated {
				inst.activeAt = b.restoredActiveAt(g.wsID, a.labels, now)
			}

			r.alerts[fp] = inst
		}

		inst.value, inst.annotations, inst.keepFiringSince = a.value, a.annotations, time.Time{}

		if inst.state == statePending && now.Sub(inst.activeAt) >= r.rule.forDur {
			inst.state, inst.firedAt = stateFiring, now
		}
	}

	for fp, inst := range r.alerts {
		if _, still := active[fp]; !still {
			resolveInstance(r, fp, inst, now)
		}
	}
}

func resolveInstance(r *ruleRuntime, fp string, inst *alertInstance, now time.Time) {
	switch {
	case inst.state == statePending:
		delete(r.alerts, fp)
	case inst.state == stateFiring && r.rule.keepFiringFor > 0:
		if inst.keepFiringSince.IsZero() {
			inst.keepFiringSince = now
		}

		if now.Sub(inst.keepFiringSince) >= r.rule.keepFiringFor {
			inst.state, inst.resolvedAt = stateInactive, now
		}
	case inst.state == stateFiring:
		inst.state, inst.resolvedAt = stateInactive, now
	case now.Sub(inst.resolvedAt) > resolvedRetention:
		delete(r.alerts, fp)
	}
}

func (b *InMemoryBackend) restoredActiveAt(wsID string, lbls promql.Labels, now time.Time) time.Time {
	want := lbls.Clone()
	want[promql.NameLabel] = metricAlertsState
	fp := promql.Fingerprint(want)
	minT, maxT := now.Add(-restoreLookback).UnixMilli(), now.UnixMilli()

	for _, rec := range b.seriesByWS.Get(wsID) {
		if rec.Labels[promql.NameLabel] != metricAlertsState || promql.Fingerprint(rec.Labels) != fp {
			continue
		}

		for _, s := range slices.Backward(rec.Samples) {
			if s.T > maxT || promql.IsStale(s.V) {
				continue
			}

			if s.T >= minT && s.V > 0 && int64(s.V) <= now.Unix() {
				return time.Unix(int64(s.V), 0).UTC()
			}

			break
		}
	}

	return now
}

func (b *InMemoryBackend) writeAlertSeries(g *groupRuntime, r *ruleRuntime, now time.Time) {
	var (
		outs []promql.Labels
		vals []float64
	)

	for _, inst := range r.alerts {
		if inst.state == stateInactive {
			continue
		}

		l := inst.labels.Clone()
		l[promql.NameLabel], l[labelAlertState] = metricAlerts, inst.state
		outs, vals = append(outs, l), append(vals, 1)

		f := inst.labels.Clone()
		f[promql.NameLabel] = metricAlertsState
		outs, vals = append(outs, f), append(vals, float64(inst.activeAt.Unix()))
	}

	b.writeOutputsLocked(g, r, outs, vals, now)
}

func collectAlerts(g *groupRuntime, r *ruleRuntime, now time.Time) []Alert {
	hold := max(endsAtIntervals*g.def.interval, minResolveTimeout)

	var out []Alert

	for fp, inst := range r.alerts {
		if inst.state == statePending || !needsSending(inst, now) {
			continue
		}

		a := Alert{
			Labels: inst.labels.Clone(), Annotations: maps.Clone(inst.annotations),
			StartsAt: inst.firedAt, EndsAt: now.Add(hold),
		}
		if inst.state == stateInactive {
			a.EndsAt = inst.resolvedAt
		}

		inst.lastSent = now
		r.alerts[fp] = inst
		out = append(out, a)
	}

	return out
}

func needsSending(inst *alertInstance, now time.Time) bool {
	if inst.state == stateInactive && inst.resolvedAt.After(inst.lastSent) {
		return true
	}

	return inst.lastSent.Add(resendDelay).Before(now) || inst.lastSent.IsZero()
}

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
