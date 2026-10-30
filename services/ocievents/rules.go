package ocievents

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type actionIn struct {
	IsEnabled   *bool   `json:"isEnabled"`
	Description *string `json:"description"`
	TopicID     *string `json:"topicId"`
	StreamID    *string `json:"streamId"`
	FunctionID  *string `json:"functionId"`
	ActionType  string  `json:"actionType"`
}

type actionsIn struct {
	Actions []actionIn `json:"actions"`
}

type createRuleIn struct {
	Tags
	IsEnabled     *bool      `json:"isEnabled"`
	Actions       *actionsIn `json:"actions"`
	DisplayName   string     `json:"displayName"`
	Description   string     `json:"description"`
	Condition     string     `json:"condition"`
	CompartmentID string     `json:"compartmentId"`
}

type updateRuleIn struct {
	Tags
	DisplayName *string    `json:"displayName"`
	Description *string    `json:"description"`
	IsEnabled   *bool      `json:"isEnabled"`
	Condition   *string    `json:"condition"`
	Actions     *actionsIn `json:"actions"`
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func str(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func stateFor(enabled bool) string {
	if enabled {
		return stateActive
	}

	return stateInactive
}

func validName(name string) *APIError {
	if name == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxDisplayName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

// buildAction validates one action and checks that its target exists in the wired service.
func (b *InMemoryBackend) buildAction(in actionIn) (*Action, *APIError) {
	if in.IsEnabled == nil {
		return nil, errMissing("every action requires isEnabled")
	}

	a := &Action{
		ID: newOCID("eventaction"), ActionType: strings.ToUpper(in.ActionType), IsEnabled: *in.IsEnabled,
		State: stateFor(*in.IsEnabled), Description: str(in.Description),
	}

	switch a.ActionType {
	case actionONS:
		a.TopicID = str(in.TopicID)
		if a.TopicID == "" {
			return nil, errMissing("topicId is required for an ONS action")
		}

		if b.notifier != nil && !b.notifier.TopicActive(a.TopicID) {
			return nil, errNotFound(a.TopicID)
		}
	case actionOSS:
		a.StreamID = str(in.StreamID)
		if a.StreamID == "" {
			return nil, errMissing("streamId is required for an OSS action")
		}

		if b.streamer != nil && !b.streamer.StreamActive(a.StreamID) {
			return nil, errNotFound(a.StreamID)
		}
	case actionFAAS:
		a.FunctionID = str(in.FunctionID)
		if a.FunctionID == "" {
			return nil, errMissing("functionId is required for a FAAS action")
		}

		if b.invoker != nil && !b.invoker.FunctionActive(a.FunctionID) {
			return nil, errNotFound(a.FunctionID)
		}
	default:
		return nil, errParam("actionType must be ONS, OSS or FAAS")
	}

	return a, nil
}

func (b *InMemoryBackend) buildActions(in *actionsIn) ([]*Action, *APIError) {
	if in == nil || len(in.Actions) == 0 {
		return nil, errMissing("a rule requires at least one action")
	}

	if len(in.Actions) > maxActions {
		return nil, errLimit("a rule can have at most 10 actions")
	}

	out := make([]*Action, 0, len(in.Actions))

	for _, ai := range in.Actions {
		a, err := b.buildAction(ai)
		if err != nil {
			return nil, err
		}

		out = append(out, a)
	}

	return out, nil
}

func (b *InMemoryBackend) liveRules() int {
	n := 0

	for _, r := range b.rules {
		if r.State != stateDeleted {
			n++
		}
	}

	return n
}

// CreateRule creates a rule.
func (b *InMemoryBackend) CreateRule(in createRuleIn) (RuleWire, string, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return RuleWire{}, "", err
	}

	if err := validName(in.DisplayName); err != nil {
		return RuleWire{}, "", err
	}

	if in.IsEnabled == nil {
		return RuleWire{}, "", errMissing("isEnabled is required")
	}

	if _, err := parseCondition(in.Condition); err != nil {
		return RuleWire{}, "", err
	}

	acts, err := b.buildActions(in.Actions)
	if err != nil {
		return RuleWire{}, "", err
	}

	if b.liveRules() >= maxRules {
		return RuleWire{}, "", errLimit("the tenancy already has the maximum of 50 rules")
	}

	r := &Rule{
		ID: newOCID("eventrule"), DisplayName: in.DisplayName, Description: in.Description, Condition: in.Condition,
		CompartmentID: in.CompartmentID, IsEnabled: *in.IsEnabled, State: stateFor(*in.IsEnabled), Actions: acts,
		TimeCreated: b.nowUTC(), Etag: newEtag(), Tags: in.Tags, Seq: b.nextSeq(),
	}
	b.rules[r.ID] = r

	return ruleView(r), r.Etag, nil
}

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

func (b *InMemoryBackend) rule(id string) (*Rule, *APIError) {
	r, ok := b.rules[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return r, nil
}

func (b *InMemoryBackend) activeRule(id string) (*Rule, *APIError) {
	r, err := b.rule(id)
	if err != nil {
		return nil, err
	}

	if r.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return r, nil
}

// GetRule returns one rule and its etag; a deleted rule is still reported with state DELETED.
func (b *InMemoryBackend) GetRule(id string) (RuleWire, string, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	r, err := b.rule(id)
	if err != nil {
		return RuleWire{}, "", err
	}

	return ruleView(r), r.Etag, nil
}

// ListRules lists the rules of a compartment; DELETED rules are hidden unless filtered for.
func (b *InMemoryBackend) ListRules(compartment, name, state string, o ListOpts) (page.Page[RuleSummaryWire], error) {
	if compartment == "" {
		return page.Page[RuleSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	var all []*Rule

	for _, r := range b.rules {
		stateOK := (state == "" && r.State != stateDeleted) || (state != "" && strings.EqualFold(r.State, state))
		if r.CompartmentID == compartment && stateOK && (name == "" || strs.Equal(r.DisplayName, name)) {
			all = append(all, r)
		}
	}

	pg := paginate(all, o)
	out := make([]RuleSummaryWire, 0, len(pg.Data))

	for _, r := range pg.Data {
		out = append(out, summaryView(r))
	}

	return page.Page[RuleSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateRule changes the mutable attributes of a rule.
func (b *InMemoryBackend) UpdateRule(id, ifMatch string, in updateRuleIn) (RuleWire, string, error) {
	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	r, err := b.activeRule(id)
	if err != nil {
		return RuleWire{}, "", err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return RuleWire{}, "", err
	}

	if err = b.applyUpdate(r, in); err != nil {
		return RuleWire{}, "", err
	}

	r.Etag = newEtag()

	return ruleView(r), r.Etag, nil
}

func (b *InMemoryBackend) applyUpdate(r *Rule, in updateRuleIn) *APIError {
	if in.DisplayName != nil {
		if err := validName(*in.DisplayName); err != nil {
			return err
		}
	}

	if in.Condition != nil {
		if _, err := parseCondition(*in.Condition); err != nil {
			return err
		}
	}

	var acts []*Action

	if in.Actions != nil {
		var err *APIError

		if acts, err = b.buildActions(in.Actions); err != nil {
			return err
		}
	}

	if in.DisplayName != nil {
		r.DisplayName = *in.DisplayName
	}

	if in.Description != nil {
		r.Description = *in.Description
	}

	if in.Condition != nil {
		r.Condition = *in.Condition
	}

	if in.IsEnabled != nil {
		r.IsEnabled, r.State = *in.IsEnabled, stateFor(*in.IsEnabled)
	}

	if acts != nil {
		r.Actions = acts
	}

	r.Tags.merge(in.Tags)

	return nil
}

// DeleteRule deletes a rule; it stays readable with state DELETED.
func (b *InMemoryBackend) DeleteRule(id, ifMatch string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	r, err := b.activeRule(id)
	if err != nil {
		return err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return err
	}

	r.State, r.IsEnabled = stateDeleted, false

	return nil
}

// ChangeRuleCompartment moves a rule.
func (b *InMemoryBackend) ChangeRuleCompartment(id, ifMatch string, in changeCompartmentIn) error {
	b.mu.Lock("ChangeRuleCompartment")
	defer b.mu.Unlock()

	r, err := b.activeRule(id)
	if err != nil {
		return err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	r.CompartmentID, r.Etag = in.CompartmentID, newEtag()

	return nil
}
