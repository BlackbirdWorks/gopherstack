package gcpmonitoring

import (
	"context"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

const (
	defaultTriggerCount = 1
	percentScale        = 100.0
	defaultGrid         = time.Minute
)

// observation is the state of one time series against one condition.
type observation struct {
	metricLabels   map[string]string
	resourceLabels map[string]string
	id             string
	resKey         string
	metricType     string
	resourceType   string
	value          float64
	threshold      float64
	met            bool
}

type notification struct {
	pub      Publisher
	policy   *monitoringpb.AlertPolicy
	targets  []target
	incident Incident
	opened   bool
}

func resourceKeyOf(r *monitoredres.MonitoredResource) string {
	var sb strings.Builder

	sb.WriteString(r.GetType())
	writeLabels(&sb, r.GetLabels())

	return sb.String()
}

func compare(cmp monitoringpb.ComparisonType, v, threshold float64) bool {
	switch cmp {
	case monitoringpb.ComparisonType_COMPARISON_GT:
		return v > threshold
	case monitoringpb.ComparisonType_COMPARISON_GE:
		return v >= threshold
	case monitoringpb.ComparisonType_COMPARISON_LT:
		return v < threshold
	case monitoringpb.ComparisonType_COMPARISON_LE:
		return v <= threshold
	case monitoringpb.ComparisonType_COMPARISON_EQ:
		return v == threshold
	case monitoringpb.ComparisonType_COMPARISON_NE:
		return v != threshold
	case monitoringpb.ComparisonType_COMPARISON_UNSPECIFIED:
		return false
	}

	return false
}

func periodOf(aggs []*monitoringpb.Aggregation) time.Duration {
	if len(aggs) > 0 && aggs[0].GetPerSeriesAligner() != alignNone {
		p := aggs[0].GetAlignmentPeriod().AsDuration()
		if len(aggs) > 1 && aggs[1].GetPerSeriesAligner() != alignNone {
			p = aggs[1].GetAlignmentPeriod().AsDuration()
		}

		return p
	}

	return defaultGrid
}

func conditionQuery(project, filterSrc string, aggs []*monitoringpb.Aggregation, win window) (query, bool) {
	f, err := parseFilter(filterSrc)
	if err != nil {
		return query{}, false
	}

	q := query{project: project, filter: f, win: win}
	if len(aggs) > 0 {
		q.agg = aggs[0]
	}

	if len(aggs) > 1 {
		q.sec = aggs[1]
	}

	return q, true
}

// held returns the latest point value at or before t within the hold window.
func held(pts []*monitoringpb.Point, t time.Time, hold time.Duration) (float64, bool) {
	for i := range slices.Backward(pts) {
		e := endOf(pts[i])
		if e.After(t) {
			continue
		}

		if t.Sub(e) >= hold {
			return 0, false
		}

		v := number(pts[i].GetValue())

		return v, true
	}

	return 0, false
}

func observeThreshold(s *aSeries, t *monitoringpb.AlertPolicy_Condition_MetricThreshold, now time.Time) observation {
	period := periodOf(t.GetAggregations())
	hold := max(period, staleness)
	steps := max(1, int((t.GetDuration().AsDuration()+period-1)/period))
	o := observation{
		id: seriesID(s), resKey: resourceKeyOf(s.resource), metricType: s.metric.GetType(),
		metricLabels: s.metric.GetLabels(), resourceType: s.resource.GetType(), resourceLabels: s.resource.GetLabels(),
		threshold: t.GetThresholdValue(), met: true,
	}

	for k := range steps {
		v, ok := held(s.pts, now.Add(-time.Duration(k)*period), hold)
		if !ok || !compare(t.GetComparison(), v, t.GetThresholdValue()) {
			o.met = false
		}

		if k == 0 {
			o.value = v
		}
	}

	return o
}

func (b *InMemoryBackend) thresholdObservationsLocked(
	project string, t *monitoringpb.AlertPolicy_Condition_MetricThreshold, now time.Time, extra []*seriesRec,
) []observation {
	period := periodOf(t.GetAggregations())
	span := t.GetDuration().AsDuration() + max(period, staleness)

	q, ok := conditionQuery(project, t.GetFilter(), t.GetAggregations(), window{start: now.Add(-span), end: now})
	if !ok {
		return nil
	}

	q.extra = extra

	series, err := b.selectLocked(q)
	if err != nil {
		return nil
	}

	out := make([]observation, 0, len(series))
	for _, s := range series {
		out = append(out, observeThreshold(s, t, now))
	}

	return applyTrigger(out, t.GetTrigger())
}

func (b *InMemoryBackend) absentObservationsLocked(
	project string, a *monitoringpb.AlertPolicy_Condition_MetricAbsence, now time.Time, extra []*seriesRec,
) []observation {
	q, ok := conditionQuery(project, a.GetFilter(), nil, window{start: now.Add(-maxPointAge), end: now})
	if !ok {
		return nil
	}

	q.extra = extra

	series, err := b.selectLocked(q)
	if err != nil {
		return nil
	}

	out := make([]observation, 0, len(series))

	for _, s := range series {
		last := endOf(s.pts[len(s.pts)-1])
		out = append(out, observation{
			id: seriesID(s), resKey: resourceKeyOf(s.resource), metricType: s.metric.GetType(),
			metricLabels: s.metric.GetLabels(), resourceType: s.resource.GetType(),
			resourceLabels: s.resource.GetLabels(), met: !last.After(now.Add(-a.GetDuration().AsDuration())),
		})
	}

	return applyTrigger(out, a.GetTrigger())
}

// applyTrigger clears the met flags unless enough of the series violate the condition.
func applyTrigger(obs []observation, trig *monitoringpb.AlertPolicy_Condition_Trigger) []observation {
	met := 0

	for _, o := range obs {
		if o.met {
			met++
		}
	}

	need := defaultTriggerCount

	switch tt := trig.GetType().(type) {
	case *monitoringpb.AlertPolicy_Condition_Trigger_Count:
		need = int(tt.Count)
	case *monitoringpb.AlertPolicy_Condition_Trigger_Percent:
		if len(obs) > 0 && float64(met)/float64(len(obs))*percentScale >= tt.Percent && met > 0 {
			return obs
		}

		return clearMet(obs)
	}

	if met < need {
		return clearMet(obs)
	}

	return obs
}

func clearMet(obs []observation) []observation {
	for i := range obs {
		obs[i].met = false
	}

	return obs
}

func (b *InMemoryBackend) conditionObservationsLocked(
	project string, c *policyCondition, now time.Time, extra []*seriesRec,
) []observation {
	switch cond := c.GetCondition().(type) {
	case *monitoringpb.AlertPolicy_Condition_ConditionThreshold:
		return b.thresholdObservationsLocked(project, cond.ConditionThreshold, now, extra)
	case *monitoringpb.AlertPolicy_Condition_ConditionAbsent:
		return b.absentObservationsLocked(project, cond.ConditionAbsent, now, extra)
	}

	return nil
}

type verdict struct {
	met      map[string]observation
	evidence map[string]struct{}
	conds    map[string]*policyCondition
}

// combine turns per-condition observations into the set of incident keys that are firing and the keys that
// have fresh data (and so may be closed).
func combine(p *monitoringpb.AlertPolicy, per [][]observation) verdict {
	v := verdict{met: map[string]observation{}, evidence: map[string]struct{}{}, conds: map[string]*policyCondition{}}

	switch p.GetCombiner() {
	case combineAnd:
		combineAll(p, per, &v, func(observation) string { return "all" })
	case combineAndMatch:
		combineMatching(p, per, &v)
	case combineOr, monitoringpb.AlertPolicy_COMBINE_UNSPECIFIED:
		combineAny(p, per, &v)
	}

	return v
}

func combineAny(p *monitoringpb.AlertPolicy, per [][]observation, v *verdict) {
	for i, obs := range per {
		c := p.GetConditions()[i]

		for _, o := range obs {
			key := c.GetName() + "|" + o.id
			v.evidence[key], v.conds[key] = struct{}{}, c

			if o.met {
				v.met[key] = o
			}
		}
	}
}

func combineAll(p *monitoringpb.AlertPolicy, per [][]observation, v *verdict, keyOf func(observation) string) {
	all := len(per) > 0

	for i, obs := range per {
		hit := false

		for _, o := range obs {
			v.evidence[keyOf(o)] = struct{}{}

			if o.met {
				hit = true

				if _, has := v.met[keyOf(o)]; !has && i == 0 {
					v.met[keyOf(o)], v.conds[keyOf(o)] = o, p.GetConditions()[0]
				}
			}
		}

		all = all && hit
	}

	if !all {
		clear(v.met)
	}
}

func combineMatching(p *monitoringpb.AlertPolicy, per [][]observation, v *verdict) {
	counts := map[string]int{}
	first := map[string]observation{}

	for i, obs := range per {
		seen := map[string]bool{}

		for _, o := range obs {
			v.evidence[o.resKey] = struct{}{}

			if o.met && !seen[o.resKey] {
				seen[o.resKey] = true
				counts[o.resKey]++

				if i == 0 {
					first[o.resKey] = o
				}
			}
		}
	}

	for k, n := range counts {
		if n == len(per) {
			v.met[k], v.conds[k] = first[k], p.GetConditions()[0]
		}
	}
}

// reconcileLocked opens incidents for newly firing keys and closes recovered ones.
func (b *InMemoryBackend) reconcileLocked(
	project string, p *monitoringpb.AlertPolicy, v verdict, now time.Time,
) []notification {
	var out []notification

	opens, closes := policyPrompts(p)

	for key, o := range v.met {
		full := p.GetName() + "|" + key
		if b.openIncidentLocked(full) != nil {
			continue
		}

		if b.snoozedLocked(p.GetName(), o, now) {
			continue
		}

		inc := b.newIncidentLocked(project, p, v.conds[key], full, o, now)
		if opens {
			out = append(out, b.notificationLocked(p, inc, true))
		}
	}

	for _, inc := range b.incidents {
		if inc.State != stateOpen || inc.Policy != p.GetName() {
			continue
		}

		key := strings.TrimPrefix(inc.Key, p.GetName()+"|")
		_, still := v.met[key]
		_, fresh := v.evidence[key]

		if still || !fresh {
			continue
		}

		inc.State, inc.EndedAt = stateClosed, now
		snoozed := b.snoozedLocked(p.GetName(), observation{
			metricLabels: inc.MetricLabels, resourceLabels: inc.ResourceLabels,
		}, now)

		if closes && !snoozed {
			out = append(out, b.notificationLocked(p, *inc, false))
		}
	}

	return out
}

func (b *InMemoryBackend) newIncidentLocked(
	project string, p *monitoringpb.AlertPolicy, c *policyCondition, key string, o observation, now time.Time,
) Incident {
	inc := &Incident{
		ID: newIncidentID(), Key: key, Project: project, Policy: p.GetName(), PolicyDisplayName: p.GetDisplayName(),
		Condition: c.GetName(), ConditionDisplayName: c.GetDisplayName(), State: stateOpen, StartedAt: now,
		MetricType: o.metricType, MetricLabels: o.metricLabels, ResourceType: o.resourceType,
		ResourceLabels: o.resourceLabels, Observed: o.value, Threshold: o.threshold,
	}
	b.incidents[inc.ID] = inc
	b.pruneIncidentsLocked()

	return cloneIncident(inc)
}

// closeOrphansLocked closes incidents whose policy was deleted or disabled.
func (b *InMemoryBackend) closeOrphansLocked(now time.Time) {
	for _, inc := range b.incidents {
		if inc.State != stateOpen {
			continue
		}

		if p, ok := b.policies[inc.Policy]; !ok || !policyEnabled(p) {
			inc.State, inc.EndedAt = stateClosed, now
		}
	}
}

// evaluateAlerts evaluates every enabled policy against the stored series at the backend clock's time.
func (b *InMemoryBackend) evaluateAlerts(ctx context.Context) {
	extras := b.logSeriesForPolicies()

	b.mu.Lock("evaluateAlerts")
	now := b.now()
	b.closeOrphansLocked(now)

	var pending []notification

	for _, name := range sortedKeys(b.policies) {
		p := b.policies[name]
		if !policyEnabled(p) {
			continue
		}

		project, _, _ := splitName(name, collAlertPolicies)
		per := make([][]observation, len(p.GetConditions()))

		for i, c := range p.GetConditions() {
			per[i] = b.conditionObservationsLocked(project, c, now, extras[project])
		}

		pending = append(pending, b.reconcileLocked(project, p, combine(p, per), now)...)
	}

	b.mu.Unlock()

	for _, n := range pending {
		b.deliver(ctx, n)
	}
}

// logSeriesForPolicies materialises log-based series for projects whose policies may query them.
func (b *InMemoryBackend) logSeriesForPolicies() map[string][]*seriesRec {
	b.mu.RLock("logSeriesForPolicies")

	need := map[string]struct{}{}

	for name, p := range b.policies {
		project, _, _ := splitName(name, collAlertPolicies)

		for _, c := range p.GetConditions() {
			if mayMatchLogMetrics(conditionFilter(c)) {
				need[project] = struct{}{}
			}
		}
	}

	now := b.now()
	b.mu.RUnlock()

	out := make(map[string][]*seriesRec, len(need))
	for project := range need {
		out[project] = b.logSeries(project, b.logMetrics(project), now, logHistory)
	}

	return out
}

func conditionFilter(c *policyCondition) string {
	switch cond := c.GetCondition().(type) {
	case *monitoringpb.AlertPolicy_Condition_ConditionThreshold:
		return cond.ConditionThreshold.GetFilter()
	case *monitoringpb.AlertPolicy_Condition_ConditionAbsent:
		return cond.ConditionAbsent.GetFilter()
	}

	return ""
}

// Tick runs one round of uptime checks and alert evaluation at the backend clock's current time.
func (b *InMemoryBackend) Tick(ctx context.Context) {
	b.runUptimeChecks(ctx)
	b.evaluateAlerts(ctx)
}

// RunLoop calls Tick every interval until ctx is cancelled.
func (b *InMemoryBackend) RunLoop(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.Tick(ctx)
		}
	}
}
