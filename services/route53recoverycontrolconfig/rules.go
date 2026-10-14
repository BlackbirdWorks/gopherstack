package route53recoverycontrolconfig

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// governs reports whether the rule references the routing control.
func (r *SafetyRule) governs(controlArn string) bool {
	return slices.Contains(r.Controls, controlArn) || slices.Contains(r.Targets, controlArn)
}

// ruleView is the wire form of a rule: exactly one of the two members is set.
func (b *InMemoryBackend) ruleView(r *SafetyRule) wireRule {
	status := b.statusOf(r.CreatedAt, r.DeletedAt)
	cfg := &wireRuleConfig{Inverted: r.Inverted, Threshold: r.Threshold, Type: r.RuleType}

	if r.Kind == ruleGating {
		return wireRule{Gating: &wireGatingRule{
			RuleConfig: cfg, ControlPanelArn: r.PanelARN, Name: r.Name, SafetyRuleArn: r.ARN, Status: status,
			Owner: b.accountID, GatingControls: slices.Clone(r.Controls), TargetControls: slices.Clone(r.Targets),
			WaitPeriodMs: r.WaitPeriodMs,
		}}
	}

	return wireRule{Assertion: &wireAssertionRule{
		RuleConfig: cfg, ControlPanelArn: r.PanelARN, Name: r.Name, SafetyRuleArn: r.ARN, Status: status,
		Owner: b.accountID, AssertedControls: slices.Clone(r.Controls), WaitPeriodMs: r.WaitPeriodMs,
	}}
}

// NewRuleSpec is the validated input of CreateSafetyRule.
type NewRuleSpec struct {
	Name         string
	PanelARN     string
	Kind         string
	RuleType     string
	Controls     []string
	Targets      []string
	Threshold    int32
	WaitPeriodMs int32
	Inverted     bool
}

func validateRuleConfig(spec *NewRuleSpec) error {
	switch spec.RuleType {
	case ruleTypeAtLeast:
		if spec.Threshold < 1 || int(spec.Threshold) > len(spec.Controls) {
			return restroute.Validation(
				"Threshold must be between 1 and the number of controls (%d) for an ATLEAST rule",
				len(spec.Controls),
			)
		}
	case ruleTypeAnd, ruleTypeOr:
	default:
		return restroute.Validation("RuleConfig.Type must be ATLEAST, AND or OR")
	}

	if spec.WaitPeriodMs < 0 {
		return restroute.Validation("WaitPeriodMs must not be negative")
	}

	return validName("safety rule", spec.Name)
}

func (b *InMemoryBackend) requireControlsIn(panelArn string, controls []string) error {
	seen := map[string]bool{}

	for _, c := range controls {
		if seen[c] {
			return restroute.Validation("Routing control %s is listed more than once", c)
		}

		seen[c] = true

		rc, err := b.getControl(c)
		if err != nil {
			return restroute.Validation("Routing control %s does not exist", c)
		}

		if rc.PanelARN != panelArn {
			return restroute.Validation("Routing control %s is not in control panel %s", c, panelArn)
		}
	}

	return nil
}

// CreateSafetyRule creates an assertion or gating rule.
func (b *InMemoryBackend) CreateSafetyRule(spec *NewRuleSpec, token string, tags map[string]string) (wireRule, error) {
	if len(spec.Controls) == 0 {
		return wireRule{}, restroute.Validation("A safety rule needs at least one routing control")
	}

	if spec.Kind == ruleGating && len(spec.Targets) == 0 {
		return wireRule{}, restroute.Validation("A gating rule needs at least one target control")
	}

	if err := validateRuleConfig(spec); err != nil {
		return wireRule{}, err
	}

	b.mu.Lock("CreateSafetyRule")
	defer b.mu.Unlock()

	if existing, ok := b.idempotent("rule", token); ok {
		if r, err := b.getRule(existing); err == nil {
			return b.ruleView(r), nil
		}
	}

	if _, err := b.getPanel(spec.PanelARN); err != nil {
		return wireRule{}, err
	}

	if err := b.requireControlsIn(spec.PanelARN, spec.Controls); err != nil {
		return wireRule{}, err
	}

	if err := b.requireControlsIn(spec.PanelARN, spec.Targets); err != nil {
		return wireRule{}, err
	}

	for _, t := range spec.Targets {
		if slices.Contains(spec.Controls, t) {
			return wireRule{}, restroute.Validation(
				"Routing control %s cannot be both a gating and a target control",
				t,
			)
		}
	}

	r := &SafetyRule{
		ARN: arn.BuildGlobal(
			arnService,
			"",
			b.accountID,
			"controlpanel/"+idOf(spec.PanelARN)+"/safetyrule/"+newID()[:12],
		),
		PanelARN:     spec.PanelARN,
		Name:         spec.Name,
		Kind:         spec.Kind,
		RuleType:     spec.RuleType,
		Controls:     slices.Clone(spec.Controls),
		Targets:      slices.Clone(spec.Targets),
		Threshold:    spec.Threshold,
		WaitPeriodMs: spec.WaitPeriodMs,
		Inverted:     spec.Inverted,
		CreatedAt:    time.Now().UTC(),
		Tags:         cloneTags(tags),
	}
	b.rules.Put(r)
	b.rememberToken("rule", token, r.ARN)

	return b.ruleView(r), nil
}

// DescribeSafetyRule returns one rule.
func (b *InMemoryBackend) DescribeSafetyRule(arn string) (wireRule, error) {
	b.mu.RLock("DescribeSafetyRule")
	defer b.mu.RUnlock()

	r, err := b.getRule(arn)
	if err != nil {
		return wireRule{}, err
	}

	return b.ruleView(r), nil
}

// ListSafetyRules lists the rules of a control panel.
func (b *InMemoryBackend) ListSafetyRules(panelArn, token string, maxResults int) ([]wireRule, string, error) {
	b.mu.RLock("ListSafetyRules")
	defer b.mu.RUnlock()

	if _, err := b.getPanel(panelArn); err != nil {
		return nil, "", err
	}

	pg, err := restroute.Paginate(sortedBy(b.rulesIn(panelArn), func(r *SafetyRule) string { return r.Name + r.ARN }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]wireRule, 0, len(pg.Data))
	for _, r := range pg.Data {
		out = append(out, b.ruleView(r))
	}

	return out, pg.Next, nil
}

// UpdateSafetyRule changes a rule's name and wait period; kind must match the stored rule.
func (b *InMemoryBackend) UpdateSafetyRule(kind, arn, name string, waitPeriodMs int32) (wireRule, error) {
	if waitPeriodMs < 0 {
		return wireRule{}, restroute.Validation("WaitPeriodMs must not be negative")
	}

	if err := validName("safety rule", name); err != nil {
		return wireRule{}, err
	}

	b.mu.Lock("UpdateSafetyRule")
	defer b.mu.Unlock()

	r, err := b.getRule(arn)
	if err != nil {
		return wireRule{}, err
	}

	if r.Kind != kind {
		return wireRule{}, restroute.Validation("Safety rule %s is a %s rule", arn, r.Kind)
	}

	r.Name = name
	r.WaitPeriodMs = waitPeriodMs

	return b.ruleView(r), nil
}

// DeleteSafetyRule deletes a rule.
func (b *InMemoryBackend) DeleteSafetyRule(arn string) error {
	b.mu.Lock("DeleteSafetyRule")
	defer b.mu.Unlock()

	r, err := b.getRule(arn)
	if err != nil {
		return err
	}

	b.markDeleted(&r.DeletedAt, func() { b.rules.Delete(arn) })

	return nil
}
