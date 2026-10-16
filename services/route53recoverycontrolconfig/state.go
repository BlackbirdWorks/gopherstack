package route53recoverycontrolconfig

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// ConflictError is a data-plane rejection naming the safety rule that blocked an update.
type ConflictError struct {
	RuleARN string
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

// StateView is the data-plane view of a routing control.
type StateView struct {
	ARN       string
	Name      string
	PanelARN  string
	PanelName string
	Owner     string
	State     string
}

// StateUpdate is one requested routing control state change.
type StateUpdate struct {
	ARN   string
	State string
}

func (b *InMemoryBackend) stateView(c *RoutingControl) StateView {
	v := StateView{ARN: c.ARN, Name: c.Name, PanelARN: c.PanelARN, Owner: b.accountID, State: c.State}
	if p, ok := b.panels.Get(c.PanelARN); ok {
		v.PanelName = p.Name
	}

	return v
}

func (b *InMemoryBackend) controlInCluster(arn, clusterID string) (*RoutingControl, error) {
	c, err := b.getControl(arn)
	if err != nil {
		return nil, err
	}

	if clusterID != "" && idOf(c.ClusterARN) != clusterID {
		return nil, restroute.NotFound("Routing control %s is not served by this cluster endpoint", arn)
	}

	return c, nil
}

// RoutingControlState returns one routing control's state; clusterID, when set, scopes it to that cluster.
func (b *InMemoryBackend) RoutingControlState(arn, clusterID string) (StateView, error) {
	b.mu.RLock("RoutingControlState")
	defer b.mu.RUnlock()

	c, err := b.controlInCluster(arn, clusterID)
	if err != nil {
		return StateView{}, err
	}

	return b.stateView(c), nil
}

// RoutingControlIsOn reports (on, found) for a routing control, for sibling services (Route 53 health checks).
func (b *InMemoryBackend) RoutingControlIsOn(arn string) (bool, bool) {
	b.mu.RLock("RoutingControlIsOn")
	defer b.mu.RUnlock()

	c, err := b.getControl(arn)
	if err != nil {
		return false, false
	}

	return c.State == stateOn, true
}

// ListRoutingControlStates lists routing control states, optionally by control panel and/or cluster.
func (b *InMemoryBackend) ListRoutingControlStates(
	panelArn, clusterID, token string,
	maxResults int,
) ([]StateView, string, error) {
	b.mu.RLock("ListRoutingControlStates")
	defer b.mu.RUnlock()

	if panelArn != "" {
		if _, err := b.getPanel(panelArn); err != nil {
			return nil, "", err
		}
	}

	var all []*RoutingControl

	for _, c := range b.controls.All() {
		if !b.alive(c.DeletedAt) || (panelArn != "" && c.PanelARN != panelArn) ||
			(clusterID != "" && idOf(c.ClusterARN) != clusterID) {
			continue
		}

		all = append(all, c)
	}

	pg, err := restroute.Paginate(sortedBy(all, func(c *RoutingControl) string { return c.Name + c.ARN }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]StateView, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, b.stateView(c))
	}

	return out, pg.Next, nil
}

// onCount counts routing controls that are On in the proposed state.
func (b *InMemoryBackend) onCount(controls []string, proposed map[string]string) int {
	n := 0

	for _, arn := range controls {
		state, ok := proposed[arn]
		if !ok {
			if c, found := b.controls.Get(arn); found {
				state = c.State
			}
		}

		if state == stateOn {
			n++
		}
	}

	return n
}

func (r *SafetyRule) satisfied(on int) bool {
	var ok bool

	switch r.RuleType {
	case ruleTypeAtLeast:
		ok = on >= int(r.Threshold)
	case ruleTypeAnd:
		ok = on == len(r.Controls)
	default:
		ok = on >= 1
	}

	return ok != r.Inverted
}

func (r *SafetyRule) describeConfig() string {
	inv := ""
	if r.Inverted {
		inv = ", inverted"
	}

	if r.RuleType == ruleTypeAtLeast {
		return fmt.Sprintf("ATLEAST %d of %d%s", r.Threshold, len(r.Controls), inv)
	}

	return fmt.Sprintf("%s of %d%s", r.RuleType, len(r.Controls), inv)
}

func (b *InMemoryBackend) checkRule(r *SafetyRule, changed map[string]string, now time.Time) error {
	governed := r.Controls
	if r.Kind == ruleGating {
		governed = r.Targets
	}

	if !slices.ContainsFunc(governed, func(a string) bool {
		_, ok := changed[a]

		return ok
	}) {
		return nil
	}

	if wait := time.Duration(r.WaitPeriodMs) * time.Millisecond; wait > 0 && now.Sub(r.LastChangeAt) < wait {
		return &ConflictError{RuleARN: r.ARN, Message: fmt.Sprintf(
			"Safety rule %q (%s) is in its %d ms wait period; retry after it elapses", r.Name, r.ARN, r.WaitPeriodMs)}
	}

	if r.satisfied(b.onCount(r.Controls, changed)) {
		return nil
	}

	return &ConflictError{RuleARN: r.ARN, Message: fmt.Sprintf(
		"Safety rule %q (%s) would be violated: the %s requirement (%s) is not met by the resulting routing control states",
		r.Name,
		r.ARN,
		r.Kind,
		r.describeConfig(),
	)}
}

// UpdateRoutingControlStates applies all updates atomically after evaluating every affected safety rule.
func (b *InMemoryBackend) UpdateRoutingControlStates(
	updates []StateUpdate,
	overrides []string,
	clusterID string,
) error {
	if len(updates) == 0 {
		return restroute.Validation("At least one routing control state entry is required")
	}

	b.mu.Lock("UpdateRoutingControlStates")
	defer b.mu.Unlock()

	override := map[string]bool{}

	for _, o := range overrides {
		if _, err := b.getRule(o); err != nil {
			return restroute.Validation("SafetyRulesToOverride contains %s, which is not a safety rule", o)
		}

		override[o] = true
	}

	controls := make([]*RoutingControl, 0, len(updates))
	changed := map[string]string{}
	panels := map[string]bool{}

	for _, u := range updates {
		if u.State != stateOn && u.State != stateOff {
			return restroute.Validation("RoutingControlState must be On or Off")
		}

		c, err := b.controlInCluster(u.ARN, clusterID)
		if err != nil {
			return err
		}

		if _, dup := changed[u.ARN]; dup {
			return restroute.Validation("Routing control %s appears more than once", u.ARN)
		}

		changed[u.ARN] = u.State
		controls = append(controls, c)
		panels[c.PanelARN] = true
	}

	effective := map[string]string{}

	for _, c := range controls {
		if c.State != changed[c.ARN] {
			effective[c.ARN] = changed[c.ARN]
		}
	}

	now := time.Now()
	touched := b.evaluateRules(panels, effective, override, now)

	if touched.err != nil {
		return touched.err
	}

	for _, c := range controls {
		c.State = changed[c.ARN]
	}

	for _, r := range touched.rules {
		r.LastChangeAt = now
	}

	return nil
}

type ruleEvaluation struct {
	err   error
	rules []*SafetyRule
}

func (b *InMemoryBackend) evaluateRules(
	panels map[string]bool,
	effective map[string]string,
	override map[string]bool,
	now time.Time,
) ruleEvaluation {
	var ev ruleEvaluation

	if len(effective) == 0 {
		return ev
	}

	for _, panel := range slices.Sorted(maps.Keys(panels)) {
		for _, r := range sortedBy(b.rulesIn(panel), func(r *SafetyRule) string { return r.ARN }) {
			if !r.touchedBy(effective) {
				continue
			}

			ev.rules = append(ev.rules, r)

			if override[r.ARN] {
				continue
			}

			if err := b.checkRule(r, effective, now); err != nil {
				return ruleEvaluation{err: err}
			}
		}
	}

	return ev
}

func (r *SafetyRule) touchedBy(changed map[string]string) bool {
	for a := range changed {
		if r.governs(a) {
			return true
		}
	}

	return false
}
