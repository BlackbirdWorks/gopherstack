package amp

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// IngestMetadata stores the metric metadata of a remote-write request.
func (b *InMemoryBackend) IngestMetadata(ctx context.Context, wsID string, metas []metricMeta) error {
	if len(metas) == 0 {
		return nil
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("IngestMetadata")
	defer b.mu.Unlock()

	if err := b.dataWorkspaceLocked(sc, wsID); err != nil {
		return err
	}

	for _, m := range metas {
		b.metadata.Put(&MetadataRecord{
			Key: metadataKey(wsID, m.Metric), WorkspaceID: wsID, Metric: m.Metric,
			Type: m.Type, Help: m.Help, Unit: m.Unit,
		})
	}

	return nil
}

type metadataJSON struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// metadataFamilies returns stored metadata by metric family, optionally for one metric and capped at limit families.
func (b *InMemoryBackend) metadataFamilies(
	ctx context.Context, wsID, metric string, limit int,
) (map[string][]metadataJSON, error) {
	b.mu.RLock("Metadata")
	defer b.mu.RUnlock()

	if err := b.dataWorkspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return nil, err
	}

	recs := slices.Clone(b.metadataByWS.Get(wsID))
	slices.SortFunc(recs, func(a, c *MetadataRecord) int { return cmp.Compare(a.Metric, c.Metric) })

	out := map[string][]metadataJSON{}

	for _, r := range recs {
		if (metric != "" && r.Metric != metric) || (limit > 0 && len(out) >= limit) {
			continue
		}

		out[r.Metric] = []metadataJSON{{Type: r.Type, Help: r.Help, Unit: r.Unit}}
	}

	return out, nil
}

type alertJSON struct {
	Labels      promql.Labels     `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"`
	ActiveAt    string            `json:"activeAt"`
	Value       string            `json:"value"`
}

type ruleJSON struct {
	Labels         promql.Labels     `json:"labels,omitempty"`
	Annotations    map[string]string `json:"annotations,omitempty"`
	LastError      string            `json:"lastError,omitempty"`
	Name           string            `json:"name"`
	Query          string            `json:"query"`
	Health         string            `json:"health"`
	Type           string            `json:"type"`
	State          string            `json:"state,omitempty"`
	LastEvaluation string            `json:"lastEvaluation"`
	Alerts         []alertJSON       `json:"alerts,omitempty"`
	Duration       float64           `json:"duration,omitempty"`
	KeepFiringFor  float64           `json:"keepFiringFor,omitempty"`
	EvaluationTime float64           `json:"evaluationTime"`
}

type groupJSON struct {
	Name           string     `json:"name"`
	File           string     `json:"file"`
	LastEvaluation string     `json:"lastEvaluation"`
	Rules          []ruleJSON `json:"rules"`
	Interval       float64    `json:"interval"`
	Limit          int        `json:"limit"`
	EvaluationTime float64    `json:"evaluationTime"`
}

func timeJSON(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00Z"
	}

	return t.UTC().Format(time.RFC3339Nano)
}

func (inst *alertInstance) json() alertJSON {
	return alertJSON{
		Labels: inst.labels, Annotations: inst.annotations, State: inst.state,
		ActiveAt: timeJSON(inst.activeAt), Value: formatValue(inst.value),
	}
}

func liveAlerts(r *ruleRuntime) []*alertInstance {
	var out []*alertInstance

	for _, inst := range r.alerts {
		if inst.state != stateInactive {
			out = append(out, inst)
		}
	}

	slices.SortFunc(out, func(a, c *alertInstance) int {
		return cmp.Compare(promql.Fingerprint(a.labels), promql.Fingerprint(c.labels))
	})

	return out
}

func ruleState(live []*alertInstance) string {
	state := stateInactive

	for _, inst := range live {
		if inst.state == stateFiring {
			return stateFiring
		}

		state = statePending
	}

	return state
}

func (r *ruleRuntime) json() ruleJSON {
	def := r.rule.def
	out := ruleJSON{
		Query: def.Expr, Health: r.health, LastError: r.lastErr, Labels: def.Labels,
		LastEvaluation: timeJSON(r.lastEval), EvaluationTime: r.evalDur.Seconds(),
	}

	if def.Record != "" {
		out.Name, out.Type = def.Record, "recording"

		return out
	}

	live := liveAlerts(r)
	out.Name, out.Type, out.State = def.Alert, "alerting", ruleState(live)
	out.Annotations = def.Annotations
	out.Duration, out.KeepFiringFor = r.rule.forDur.Seconds(), r.rule.keepFiringFor.Seconds()
	out.Alerts = make([]alertJSON, 0, len(live))

	for _, inst := range live {
		out.Alerts = append(out.Alerts, inst.json())
	}

	return out
}

func (g *groupRuntime) json() groupJSON {
	out := groupJSON{
		Name: g.def.name, File: g.ns, Interval: g.def.interval.Seconds(), Limit: g.def.limit,
		LastEvaluation: timeJSON(g.lastEval), EvaluationTime: g.lastDur.Seconds(),
		Rules: make([]ruleJSON, 0, len(g.rules)),
	}

	for _, r := range g.rules {
		out.Rules = append(out.Rules, r.json())
	}

	return out
}

// rulesStatus reports the evaluated rule groups of a workspace.
func (b *InMemoryBackend) rulesStatus(ctx context.Context, wsID string) ([]groupJSON, error) {
	b.mu.RLock("RulesStatus")
	defer b.mu.RUnlock()

	if err := b.dataWorkspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return nil, err
	}

	var groups []*groupRuntime

	for _, g := range b.groups {
		if g.wsID == wsID {
			groups = append(groups, g)
		}
	}

	slices.SortFunc(groups, func(a, c *groupRuntime) int {
		return cmp.Or(cmp.Compare(a.ns, c.ns), cmp.Compare(a.def.name, c.def.name))
	})

	out := make([]groupJSON, 0, len(groups))
	for _, g := range groups {
		out = append(out, g.json())
	}

	return out, nil
}

// alertsStatus lists the pending and firing alerts of a workspace.
func (b *InMemoryBackend) alertsStatus(ctx context.Context, wsID string) ([]alertJSON, error) {
	groups, err := b.rulesStatus(ctx, wsID)
	if err != nil {
		return nil, err
	}

	out := []alertJSON{}

	for _, g := range groups {
		for _, r := range g.Rules {
			out = append(out, r.Alerts...)
		}
	}

	return out, nil
}

type amAlertJSON struct {
	Labels       promql.Labels     `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	Status       amAlertStatus     `json:"status"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	UpdatedAt    string            `json:"updatedAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
	Receivers    []amReceiverName  `json:"receivers"`
}

type amAlertStatus struct {
	State       string   `json:"state"`
	SilencedBy  []string `json:"silencedBy"`
	InhibitedBy []string `json:"inhibitedBy"`
}

type amReceiverName struct {
	Name string `json:"name"`
}

func (b *InMemoryBackend) managerLocked(sc scope, wsID string) (*alertManager, error) {
	if err := b.dataWorkspaceLocked(sc, wsID); err != nil {
		return nil, err
	}

	m := b.ams[wsID]
	if m == nil {
		return nil, notFound(resAlertManager, wsID, "No alert manager definition is running for %s", wsID)
	}

	return m, nil
}

// alertManagerAlerts lists the alerts the workspace's alert manager holds.
func (b *InMemoryBackend) alertManagerAlerts(ctx context.Context, wsID string) ([]amAlertJSON, error) {
	b.mu.RLock("AlertManagerAlerts")
	m, err := b.managerLocked(b.scopeOf(ctx), wsID)
	now := b.now()
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return m.view(now), nil
}

// ReceiveAlerts hands alerts posted to the alert manager API to the workspace's alert manager.
func (b *InMemoryBackend) ReceiveAlerts(ctx context.Context, wsID string, alerts []Alert) error {
	b.mu.RLock("ReceiveAlerts")
	m, err := b.managerLocked(b.scopeOf(ctx), wsID)
	now := b.now()
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	m.Receive(alerts, now)

	return nil
}

func (m *alertManager) view(now time.Time) []amAlertJSON {
	alerts := m.List()

	m.mu.RLock("view")
	defer m.mu.RUnlock()

	out := make([]amAlertJSON, 0, len(alerts))

	for _, a := range alerts {
		if a.resolved(now) {
			continue
		}

		routes := matchRoutes(m.cfg.root, a.Labels)
		recv := make([]amReceiverName, 0, len(routes))

		for _, r := range routes {
			recv = append(recv, amReceiverName{Name: r.receiver})
		}

		out = append(out, amAlertJSON{
			Labels: a.Labels, Annotations: a.Annotations, StartsAt: timeJSON(a.StartsAt), EndsAt: timeJSON(a.EndsAt),
			UpdatedAt: timeJSON(now), GeneratorURL: a.GeneratorURL, Fingerprint: fingerprintHex(a.Labels),
			Status:    amAlertStatus{State: "active", SilencedBy: []string{}, InhibitedBy: []string{}},
			Receivers: recv,
		})
	}

	return out
}

func fingerprintHex(l promql.Labels) string {
	sum := sha256.Sum256([]byte(promql.Fingerprint(l)))

	return hex.EncodeToString(sum[:8])
}
