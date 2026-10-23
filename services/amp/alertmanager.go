package amp

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	maxStoredAlerts   = 100000
	maxAlertLabels    = 128
	maxAlertValueSize = 4096
)

type notifyState struct {
	at     time.Time
	firing map[string]bool
}

type aggGroup struct {
	nextFlush time.Time
	labels    promql.Labels
	alerts    map[string]Alert
	state     map[int]*notifyState
	route     *amRoute
}

// alertManager is one workspace's alert dispatcher: routing, grouping, inhibition and notification timing.
type alertManager struct {
	mu     *lockmetrics.RWMutex
	cfg    *amConfig
	alerts map[string]Alert
	groups map[string]*aggGroup
	src    []byte
}

func newAlertManager(cfg *amConfig, src []byte) *alertManager {
	return &alertManager{
		mu: lockmetrics.New(serviceName + ".alertmanager"), cfg: cfg, src: src,
		alerts: map[string]Alert{}, groups: map[string]*aggGroup{},
	}
}

func (a Alert) resolved(now time.Time) bool { return !a.EndsAt.IsZero() && !a.EndsAt.After(now) }

func validAlert(a Alert) bool {
	if len(a.Labels) == 0 || len(a.Labels) > maxAlertLabels {
		return false
	}

	for k, v := range a.Labels {
		if k == "" || len(k) > maxAlertValueSize || len(v) > maxAlertValueSize {
			return false
		}
	}

	return true
}

// Receive stores alerts and routes them into aggregation groups.
func (m *alertManager) Receive(alerts []Alert, now time.Time) {
	m.mu.Lock("Receive")
	defer m.mu.Unlock()

	for _, a := range alerts {
		if !validAlert(a) {
			continue
		}

		fp := promql.Fingerprint(a.Labels)
		if _, known := m.alerts[fp]; !known && len(m.alerts) >= maxStoredAlerts {
			continue
		}

		m.alerts[fp] = a
		m.dispatchLocked(fp, a, now)
	}
}

func (m *alertManager) dispatchLocked(fp string, a Alert, now time.Time) {
	for _, r := range matchRoutes(m.cfg.root, a.Labels) {
		gl := groupLabels(r, a.Labels)
		key := groupID(r, gl)

		g, ok := m.groups[key]
		if !ok {
			g = &aggGroup{
				route: r, labels: gl, alerts: map[string]Alert{},
				state: map[int]*notifyState{}, nextFlush: now.Add(r.wait),
			}
			m.groups[key] = g
		}

		g.alerts[fp] = a
	}
}

func groupID(r *amRoute, gl promql.Labels) string {
	return strconv.Itoa(r.id) + "/" + promql.Fingerprint(gl)
}

func groupLabels(r *amRoute, l promql.Labels) promql.Labels {
	if r.groupAll {
		return l.Clone()
	}

	out := promql.Labels{}

	for _, n := range r.groupBy {
		if v, ok := l[n]; ok {
			out[n] = v
		}
	}

	return out
}

// matchRoutes walks the tree depth-first; the first matching child stops the walk unless it sets continue.
func matchRoutes(r *amRoute, l promql.Labels) []*amRoute {
	if !promql.MatchesAll(l, r.matchers) {
		return nil
	}

	var out []*amRoute

	for _, c := range r.children {
		sub := matchRoutes(c, l)
		out = append(out, sub...)

		if len(sub) > 0 && !c.cont {
			return out
		}
	}

	if len(out) == 0 {
		return []*amRoute{r}
	}

	return out
}

// pendingNotify is a notification ready to deliver; done reports the outcome back to the dispatcher.
type pendingNotify struct {
	data     notifData
	done     func(err error)
	receiver *amReceiver
	sns      amSNS
}

// Flush returns the notifications due at now.
func (m *alertManager) Flush(now time.Time) []pendingNotify {
	m.mu.Lock("Flush")
	defer m.mu.Unlock()

	var out []pendingNotify

	for key, g := range m.groups {
		if now.Before(g.nextFlush) {
			continue
		}

		g.nextFlush = now.Add(g.route.interval)
		out = append(out, m.flushGroupLocked(key, g, now)...)
	}

	m.gcLocked(now)

	return out
}

func (m *alertManager) flushGroupLocked(key string, g *aggGroup, now time.Time) []pendingNotify {
	firing, resolved := m.splitLocked(g, now)

	var out []pendingNotify

	rc := m.cfg.receivers[g.route.receiver]
	if rc != nil {
		for i, s := range rc.sns {
			if p, ok := m.decideLocked(g, rc, i, s, firing, resolved, now); ok {
				out = append(out, p)
			}
		}
	}

	if len(out) == 0 {
		m.dropResolvedLocked(key, g, resolved)

		return nil
	}

	m.chainCleanup(key, g, resolved, out)

	return out
}

func (m *alertManager) chainCleanup(key string, g *aggGroup, resolved []Alert, out []pendingNotify) {
	left := len(out)

	for i := range out {
		inner := out[i].done

		out[i].done = func(err error) {
			inner(err)

			m.mu.Lock("done")
			defer m.mu.Unlock()

			if left--; left == 0 {
				m.dropResolvedLocked(key, g, resolved)
			}
		}
	}
}

func (m *alertManager) dropResolvedLocked(key string, g *aggGroup, resolved []Alert) {
	for _, a := range resolved {
		delete(g.alerts, promql.Fingerprint(a.Labels))
	}

	if len(g.alerts) == 0 {
		delete(m.groups, key)
	}
}

func (m *alertManager) splitLocked(g *aggGroup, now time.Time) ([]Alert, []Alert) {
	var firing, resolved []Alert

	for _, a := range g.alerts {
		switch {
		case a.resolved(now):
			resolved = append(resolved, a)
		case !m.inhibitedLocked(a, now):
			firing = append(firing, a)
		}
	}

	sortAlerts(firing)
	sortAlerts(resolved)

	return firing, resolved
}

func sortAlerts(a []Alert) {
	slices.SortFunc(a, func(x, y Alert) int {
		return strings.Compare(promql.Fingerprint(x.Labels), promql.Fingerprint(y.Labels))
	})
}

func (m *alertManager) inhibitedLocked(target Alert, now time.Time) bool {
	for _, rule := range m.cfg.inhibit {
		if !promql.MatchesAll(target.Labels, rule.target) {
			continue
		}

		for _, src := range m.alerts {
			if src.resolved(now) || !promql.MatchesAll(src.Labels, rule.source) {
				continue
			}

			if equalOn(rule.equal, src.Labels, target.Labels) &&
				promql.Fingerprint(src.Labels) != promql.Fingerprint(target.Labels) {
				return true
			}
		}
	}

	return false
}

func equalOn(names []string, a, b promql.Labels) bool {
	for _, n := range names {
		if a[n] != b[n] {
			return false
		}
	}

	return true
}

func (m *alertManager) decideLocked(
	g *aggGroup, rc *amReceiver, idx int, s amSNS, firing, resolved []Alert, now time.Time,
) (pendingNotify, bool) {
	st := g.state[idx]
	if st == nil {
		st = &notifyState{firing: map[string]bool{}}
	}

	curFiring := make(map[string]bool, len(firing))
	newFiring := false

	for _, a := range firing {
		fp := promql.Fingerprint(a.Labels)
		curFiring[fp] = true

		if !st.firing[fp] {
			newFiring = true
		}
	}

	resolvedNews := s.sendResolved && slices.ContainsFunc(resolved, func(a Alert) bool {
		return st.firing[promql.Fingerprint(a.Labels)]
	})

	repeat := len(firing) > 0 && !st.at.IsZero() && now.Sub(st.at) >= g.route.repeat

	if !newFiring && !resolvedNews && !repeat {
		return pendingNotify{}, false
	}

	shown := firing
	if s.sendResolved {
		shown = slices.Concat(firing, resolved)
	}

	return pendingNotify{
		receiver: rc, sns: s, data: newNotifData(rc.name, g, shown, now),
		done: func(err error) {
			if err != nil {
				return
			}

			m.mu.Lock("record")
			defer m.mu.Unlock()

			g.state[idx] = &notifyState{at: now, firing: curFiring}
		},
	}, true
}

// gcLocked drops resolved alerts that no group holds any more.
func (m *alertManager) gcLocked(now time.Time) {
	held := map[string]bool{}

	for _, g := range m.groups {
		for fp := range g.alerts {
			held[fp] = true
		}
	}

	for fp, a := range m.alerts {
		if a.resolved(now) && !held[fp] {
			delete(m.alerts, fp)
		}
	}
}

// List returns the stored alerts sorted by fingerprint.
func (m *alertManager) List() []Alert {
	m.mu.RLock("List")
	defer m.mu.RUnlock()

	out := make([]Alert, 0, len(m.alerts))
	for _, a := range m.alerts {
		out = append(out, a)
	}

	sortAlerts(out)

	return out
}
