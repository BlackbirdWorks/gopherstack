package gcpmonitoring

import (
	"maps"
	"net/http"
	"net/url"
	"sort"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	stateInactive = "inactive"
	statePending  = "pending"
	stateFiring   = "firing"
	labelAlertNm  = "alertname"
	healthOK      = "ok"
	typeAlerting  = "alerting"
	typeRecording = "recording"
)

type promAlertJSON struct {
	Labels      promql.Labels     `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"`
	ActiveAt    string            `json:"activeAt"`
	Value       string            `json:"value"`
}

type promRuleJSON struct {
	Labels         promql.Labels     `json:"labels"`
	Annotations    map[string]string `json:"annotations"`
	Name           string            `json:"name"`
	Query          string            `json:"query"`
	State          string            `json:"state"`
	Health         string            `json:"health"`
	Type           string            `json:"type"`
	LastEvaluation string            `json:"lastEvaluation"`
	Alerts         []promAlertJSON   `json:"alerts"`
	Duration       float64           `json:"duration"`
	EvaluationTime float64           `json:"evaluationTime"`
}

type promGroupJSON struct {
	Name           string         `json:"name"`
	File           string         `json:"file"`
	LastEvaluation string         `json:"lastEvaluation"`
	Rules          []promRuleJSON `json:"rules"`
	Interval       float64        `json:"interval"`
	EvaluationTime float64        `json:"evaluationTime"`
}

func ruleName(c *policyCondition, pc *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition) string {
	if pc.GetAlertRule() != "" {
		return pc.GetAlertRule()
	}

	return c.GetDisplayName()
}

func promTime(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00Z"
	}

	return t.UTC().Format(time.RFC3339Nano)
}

func (b *InMemoryBackend) promRuleLocked(
	p *monitoringpb.AlertPolicy, c *policyCondition,
	pc *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition, now time.Time,
) promRuleJSON {
	name := ruleName(c, pc)
	rule := promRuleJSON{
		Name: name, Query: pc.GetQuery(), Duration: pc.GetDuration().AsDuration().Seconds(), State: stateInactive,
		Health: healthOK, Type: typeAlerting, Labels: promql.Labels{}, Annotations: map[string]string{},
		Alerts: []promAlertJSON{}, LastEvaluation: promTime(time.Time{}),
	}

	maps.Copy(rule.Labels, pc.GetLabels())

	rt := b.condRT[c.GetName()]
	if rt == nil || !policyEnabled(p) {
		return rule
	}

	rule.LastEvaluation = promTime(rt.lastEval)

	ids := make([]string, 0, len(rt.active))
	for id := range rt.active {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		st := rt.active[id]
		state := statePending

		if now.Sub(st.since) >= pc.GetDuration().AsDuration() {
			state = stateFiring
		}

		l := promql.Labels{labelAlertNm: name}
		for k, v := range st.labels {
			if k != promql.NameLabel {
				l[k] = v
			}
		}

		rule.Alerts = append(rule.Alerts, promAlertJSON{
			Labels: l, Annotations: map[string]string{}, State: state, ActiveAt: promTime(st.since),
			Value: formatPromValue(st.value),
		})

		if state == stateFiring || rule.State == stateInactive {
			rule.State = state
		}
	}

	return rule
}

// promRulesLocked groups the PromQL conditions of a project's alert policies the way a rule file would.
func (b *InMemoryBackend) promRulesLocked(project string, now time.Time) []promGroupJSON {
	groups := map[string]*promGroupJSON{}

	for _, name := range sortedKeys(b.policies) {
		p := b.policies[name]
		if pr, _, _ := splitName(name, collAlertPolicies); pr != project {
			continue
		}

		for _, c := range p.GetConditions() {
			pc := c.GetConditionPrometheusQueryLanguage()
			if pc == nil {
				continue
			}

			gname := pc.GetRuleGroup()
			if gname == "" {
				gname = p.GetDisplayName()
			}

			g, ok := groups[gname]
			if !ok {
				g = &promGroupJSON{
					Name: gname, Rules: []promRuleJSON{}, Interval: promIntervalOf(pc).Seconds(),
					LastEvaluation: promTime(time.Time{}),
				}
				groups[gname] = g
			}

			rule := b.promRuleLocked(p, c, pc, now)
			if rule.LastEvaluation > g.LastEvaluation {
				g.LastEvaluation = rule.LastEvaluation
			}

			g.Rules = append(g.Rules, rule)
		}
	}

	out := make([]promGroupJSON, 0, len(groups))
	for _, n := range sortedKeys(groups) {
		out = append(out, *groups[n])
	}

	return out
}

func (b *InMemoryBackend) promRules(w http.ResponseWriter, project string, vals url.Values) {
	b.mu.RLock("promRules")
	groups := b.promRulesLocked(project, b.now())
	b.mu.RUnlock()

	if vals.Get("type") == typeRecording {
		groups = []promGroupJSON{}
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: map[string]any{"groups": groups}})
}

func (b *InMemoryBackend) promAlerts(w http.ResponseWriter, project string) {
	b.mu.RLock("promAlerts")
	groups := b.promRulesLocked(project, b.now())
	b.mu.RUnlock()

	alerts := []promAlertJSON{}

	for _, g := range groups {
		for _, r := range g.Rules {
			alerts = append(alerts, r.Alerts...)
		}
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: map[string]any{"alerts": alerts}})
}
