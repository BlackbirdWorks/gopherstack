package networkfirewall

import (
	"context"
	"slices"
	"time"
)

const (
	phasePreDNS   = "PRE_DNS"
	phasePreReq   = "PRE_REQ"
	phasePostResp = "POST_RES"
)

// ProxyRuleCondition is one match condition of a proxy rule.
type ProxyRuleCondition struct {
	ConditionKey      string   `json:"ConditionKey,omitempty"`
	ConditionOperator string   `json:"ConditionOperator,omitempty"`
	ConditionValues   []string `json:"ConditionValues,omitempty"`
}

// ProxyRule is a proxy rule.
type ProxyRule struct {
	Action        string               `json:"Action,omitempty"`
	Description   string               `json:"Description,omitempty"`
	ProxyRuleName string               `json:"ProxyRuleName"`
	Conditions    []ProxyRuleCondition `json:"Conditions,omitempty"`
}

// ProxyRulesByRequestPhase groups proxy rules by request phase.
type ProxyRulesByRequestPhase struct {
	PostRESPONSE []ProxyRule `json:"PostRESPONSE,omitempty"`
	PreDNS       []ProxyRule `json:"PreDNS,omitempty"`
	PreREQUEST   []ProxyRule `json:"PreREQUEST,omitempty"`
}

// CreateProxyRule is a proxy rule with an insert position.
type CreateProxyRule struct {
	InsertPosition *int32 `json:"InsertPosition,omitempty"`
	ProxyRule
}

// CreateProxyRulesByRequestPhase groups new proxy rules by request phase.
type CreateProxyRulesByRequestPhase struct {
	PostRESPONSE []CreateProxyRule `json:"PostRESPONSE,omitempty"`
	PreDNS       []CreateProxyRule `json:"PreDNS,omitempty"`
	PreREQUEST   []CreateProxyRule `json:"PreREQUEST,omitempty"`
}

// ProxyRuleGroup is the ProxyRuleGroup response shape.
type ProxyRuleGroup struct {
	CreateTime         *float64                  `json:"CreateTime,omitempty"`
	Rules              *ProxyRulesByRequestPhase `json:"Rules,omitempty"`
	Description        string                    `json:"Description,omitempty"`
	ProxyRuleGroupArn  string                    `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string                    `json:"ProxyRuleGroupName"`
	Tags               []Tag                     `json:"Tags,omitempty"`
}

// CreateProxyRuleGroupInput is the CreateProxyRuleGroup request.
type CreateProxyRuleGroupInput struct {
	Rules              *ProxyRulesByRequestPhase `json:"Rules"`
	ProxyRuleGroupName string                    `json:"ProxyRuleGroupName"`
	Description        string                    `json:"Description"`
	Tags               []Tag                     `json:"Tags"`
}

// ProxyRuleGroupOutput is the response of calls that return a rule group and token.
type ProxyRuleGroupOutput struct {
	UpdateToken    string         `json:"UpdateToken"`
	ProxyRuleGroup ProxyRuleGroup `json:"ProxyRuleGroup"`
}

// DescribeProxyRuleGroupInput is the DescribeProxyRuleGroup request.
type DescribeProxyRuleGroupInput struct {
	ProxyRuleGroupArn  string `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// DeleteProxyRuleGroupInput is the DeleteProxyRuleGroup request.
type DeleteProxyRuleGroupInput = DescribeProxyRuleGroupInput

// DeleteProxyRuleGroupOutput is the DeleteProxyRuleGroup response.
type DeleteProxyRuleGroupOutput struct {
	ProxyRuleGroupArn  string `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// ListProxyRuleGroupsInput is the ListProxyRuleGroups request.
type ListProxyRuleGroupsInput = ListProxiesInput

// ListProxyRuleGroupsOutput is the ListProxyRuleGroups response.
type ListProxyRuleGroupsOutput struct {
	NextToken       string             `json:"NextToken,omitempty"`
	ProxyRuleGroups []ResourceMetadata `json:"ProxyRuleGroups"`
}

// CreateProxyRulesInput is the CreateProxyRules request.
type CreateProxyRulesInput struct {
	Rules              *CreateProxyRulesByRequestPhase `json:"Rules"`
	ProxyRuleGroupArn  string                          `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string                          `json:"ProxyRuleGroupName"`
}

// DeleteProxyRulesInput is the DeleteProxyRules request.
type DeleteProxyRulesInput struct {
	ProxyRuleGroupArn  string   `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string   `json:"ProxyRuleGroupName"`
	Rules              []string `json:"Rules"`
}

// DeleteProxyRulesOutput is the DeleteProxyRules response.
type DeleteProxyRulesOutput struct {
	ProxyRuleGroup ProxyRuleGroup `json:"ProxyRuleGroup"`
}

// DescribeProxyRuleInput is the DescribeProxyRule request.
type DescribeProxyRuleInput struct {
	ProxyRuleGroupArn  string `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
	ProxyRuleName      string `json:"ProxyRuleName"`
}

// DescribeProxyRuleOutput is the DescribeProxyRule response.
type DescribeProxyRuleOutput struct {
	UpdateToken string    `json:"UpdateToken"`
	ProxyRule   ProxyRule `json:"ProxyRule"`
}

// UpdateProxyRuleInput is the UpdateProxyRule request.
type UpdateProxyRuleInput struct {
	ProxyRuleGroupArn  string               `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string               `json:"ProxyRuleGroupName"`
	ProxyRuleName      string               `json:"ProxyRuleName"`
	UpdateToken        string               `json:"UpdateToken"`
	Action             string               `json:"Action"`
	Description        *string              `json:"Description"`
	AddConditions      []ProxyRuleCondition `json:"AddConditions"`
	RemoveConditions   []ProxyRuleCondition `json:"RemoveConditions"`
}

// UpdateProxyRuleOutput is the UpdateProxyRule response.
type UpdateProxyRuleOutput struct {
	UpdateToken       string               `json:"UpdateToken"`
	ProxyRule         ProxyRule            `json:"ProxyRule"`
	RemovedConditions []ProxyRuleCondition `json:"RemovedConditions,omitempty"`
}

// ProxyRulePriority moves a rule within its phase.
type ProxyRulePriority struct {
	NewPosition   *int32 `json:"NewPosition,omitempty"`
	ProxyRuleName string `json:"ProxyRuleName"`
}

// UpdateProxyRulePrioritiesInput is the UpdateProxyRulePriorities request.
type UpdateProxyRulePrioritiesInput struct {
	ProxyRuleGroupArn     string              `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName    string              `json:"ProxyRuleGroupName"`
	RuleGroupRequestPhase string              `json:"RuleGroupRequestPhase"`
	UpdateToken           string              `json:"UpdateToken"`
	Rules                 []ProxyRulePriority `json:"Rules"`
}

// UpdateProxyRulePrioritiesOutput is the UpdateProxyRulePriorities response.
type UpdateProxyRulePrioritiesOutput struct {
	ProxyRuleGroupArn     string              `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName    string              `json:"ProxyRuleGroupName"`
	RuleGroupRequestPhase string              `json:"RuleGroupRequestPhase"`
	UpdateToken           string              `json:"UpdateToken"`
	Rules                 []ProxyRulePriority `json:"Rules"`
}

func phaseSlice(p *ProxyRulesByRequestPhase, phase string) (*[]ProxyRule, error) {
	switch phase {
	case phasePreDNS:
		return &p.PreDNS, nil
	case phasePreReq:
		return &p.PreREQUEST, nil
	case phasePostResp:
		return &p.PostRESPONSE, nil
	default:
		return nil, invalid("RuleGroupRequestPhase %q is not valid", phase)
	}
}

func allPhases(p *ProxyRulesByRequestPhase) []*[]ProxyRule {
	return []*[]ProxyRule{&p.PreDNS, &p.PreREQUEST, &p.PostRESPONSE}
}

func (p *ProxyRulesByRequestPhase) find(name string) (*[]ProxyRule, int) {
	for _, s := range allPhases(p) {
		if i := slices.IndexFunc(*s, func(r ProxyRule) bool { return r.ProxyRuleName == name }); i >= 0 {
			return s, i
		}
	}

	return nil, -1
}

func validateProxyRule(r *ProxyRule) error {
	if err := validateName("ProxyRuleName", r.ProxyRuleName); err != nil {
		return err
	}

	if r.Action == "" {
		return invalid("Action is required for proxy rule %s", r.ProxyRuleName)
	}

	if err := validatePhaseAction(r.Action); err != nil {
		return err
	}

	if err := validateDescription(r.Description); err != nil {
		return err
	}

	return validateConditions(r.Conditions)
}

func validateConditions(c []ProxyRuleCondition) error {
	for _, x := range c {
		if x.ConditionKey == "" || x.ConditionOperator == "" || len(x.ConditionValues) == 0 {
			return invalid("A proxy rule condition requires ConditionKey, ConditionOperator and ConditionValues")
		}
	}

	return nil
}

func (b *InMemoryBackend) proxyRuleGroupViewLocked(r *proxyRuleGroupRecord) ProxyRuleGroup {
	rules := r.Rules

	return ProxyRuleGroup{
		CreateTime:         epoch(r.CreateTime),
		Rules:              &rules,
		Description:        r.Description,
		ProxyRuleGroupArn:  r.ARN,
		ProxyRuleGroupName: r.Name,
		Tags:               tagList(r.Tags),
	}
}

func (b *InMemoryBackend) proxyRuleGroupOutputLocked(r *proxyRuleGroupRecord) *ProxyRuleGroupOutput {
	return &ProxyRuleGroupOutput{UpdateToken: r.UpdateToken, ProxyRuleGroup: b.proxyRuleGroupViewLocked(r)}
}

// CreateProxyRuleGroup creates a proxy rule group, optionally with initial rules.
func (b *InMemoryBackend) CreateProxyRuleGroup(
	_ context.Context, in *CreateProxyRuleGroupInput,
) (*ProxyRuleGroupOutput, error) {
	if err := validateName("ProxyRuleGroupName", in.ProxyRuleGroupName); err != nil {
		return nil, err
	}

	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	if err := validateTags(tagMap(in.Tags)); err != nil {
		return nil, err
	}

	var rules ProxyRulesByRequestPhase
	if in.Rules != nil {
		rules = cloneRules(in.Rules)
	}

	seen := map[string]bool{}

	for _, s := range allPhases(&rules) {
		for i := range *s {
			if err := validateProxyRule(&(*s)[i]); err != nil {
				return nil, err
			}

			if seen[(*s)[i].ProxyRuleName] {
				return nil, invalid("Proxy rule %s is specified more than once", (*s)[i].ProxyRuleName)
			}

			seen[(*s)[i].ProxyRuleName] = true
		}
	}

	b.mu.Lock("CreateProxyRuleGroup")
	defer b.mu.Unlock()

	groupARN := b.arnFor(kindProxyRuleGroup, in.ProxyRuleGroupName)
	if b.proxyRuleGroups.Has(groupARN) {
		return nil, invalid("A proxy rule group named %s already exists", in.ProxyRuleGroupName)
	}

	r := &proxyRuleGroupRecord{
		Name:        in.ProxyRuleGroupName,
		ARN:         groupARN,
		Description: in.Description,
		UpdateToken: newToken(),
		Rules:       rules,
		Tags:        tagMap(in.Tags),
		CreateTime:  time.Now(),
	}

	b.proxyRuleGroups.Put(r)

	return b.proxyRuleGroupOutputLocked(r), nil
}

func cloneRules(p *ProxyRulesByRequestPhase) ProxyRulesByRequestPhase {
	return ProxyRulesByRequestPhase{
		PostRESPONSE: slices.Clone(p.PostRESPONSE),
		PreDNS:       slices.Clone(p.PreDNS),
		PreREQUEST:   slices.Clone(p.PreREQUEST),
	}
}

// DescribeProxyRuleGroup returns a rule group and its update token.
func (b *InMemoryBackend) DescribeProxyRuleGroup(
	_ context.Context, in *DescribeProxyRuleGroupInput,
) (*ProxyRuleGroupOutput, error) {
	b.mu.RLock("DescribeProxyRuleGroup")
	defer b.mu.RUnlock()

	r, err := lookup(b, b.proxyRuleGroups, kindProxyRuleGroup, "proxy rule group",
		arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName})
	if err != nil {
		return nil, err
	}

	return b.proxyRuleGroupOutputLocked(r), nil
}

// ListProxyRuleGroups lists proxy rule groups.
func (b *InMemoryBackend) ListProxyRuleGroups(
	_ context.Context, in *ListProxyRuleGroupsInput,
) (*ListProxyRuleGroupsOutput, error) {
	b.mu.RLock("ListProxyRuleGroups")
	defer b.mu.RUnlock()

	data, next, err := listMetadata(b.proxyRuleGroups.All(),
		func(r *proxyRuleGroupRecord) ResourceMetadata { return ResourceMetadata{Arn: r.ARN, Name: r.Name} },
		in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListProxyRuleGroupsOutput{ProxyRuleGroups: data, NextToken: next}, nil
}

// DeleteProxyRuleGroup deletes a rule group no proxy configuration uses.
func (b *InMemoryBackend) DeleteProxyRuleGroup(
	_ context.Context, in *DeleteProxyRuleGroupInput,
) (*DeleteProxyRuleGroupOutput, error) {
	b.mu.Lock("DeleteProxyRuleGroup")
	defer b.mu.Unlock()

	r, err := lookup(b, b.proxyRuleGroups, kindProxyRuleGroup, "proxy rule group",
		arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName})
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(
		b.proxyConfigs.All(),
		func(c *proxyConfigRecord) bool { return slices.Contains(c.Groups, r.ARN) },
	) {
		return nil, invalidOp("The proxy rule group %s is attached to a proxy configuration", r.Name)
	}

	b.proxyRuleGroups.Delete(r.ARN)

	return &DeleteProxyRuleGroupOutput{ProxyRuleGroupArn: r.ARN, ProxyRuleGroupName: r.Name}, nil
}

// mutateRuleGroup applies change to a rule group under the write lock and rotates its token.
func mutateRuleGroup[T any](
	b *InMemoryBackend, op string, id arnOrName, token string, tokenRequired bool,
	change func(*proxyRuleGroupRecord) error, build func(*proxyRuleGroupRecord) *T,
) (*T, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	r, err := lookup(b, b.proxyRuleGroups, kindProxyRuleGroup, "proxy rule group", id)
	if err != nil {
		return nil, err
	}

	if tokenRequired {
		err = requireToken(r.UpdateToken, token)
	} else {
		err = checkToken(r.UpdateToken, token)
	}

	if err != nil {
		return nil, err
	}

	if err = change(r); err != nil {
		return nil, err
	}

	r.UpdateToken = newToken()

	return build(r), nil
}

// CreateProxyRules adds rules to a rule group at the requested positions.
func (b *InMemoryBackend) CreateProxyRules(
	_ context.Context,
	in *CreateProxyRulesInput,
) (*ProxyRuleGroupOutput, error) {
	if in.Rules == nil {
		return nil, invalid("Rules is required")
	}

	return mutateRuleGroup(b, "CreateProxyRules", arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName}, "", false,
		func(r *proxyRuleGroupRecord) error {
			next := cloneRules(&r.Rules)

			add := func(dst *[]ProxyRule, rules []CreateProxyRule) error {
				for i := range rules {
					if err := validateProxyRule(&rules[i].ProxyRule); err != nil {
						return err
					}

					if s, _ := next.find(rules[i].ProxyRuleName); s != nil {
						return invalid("Proxy rule %s already exists", rules[i].ProxyRuleName)
					}

					*dst = insertAt(*dst, rules[i].InsertPosition, rules[i].ProxyRule)
				}

				return nil
			}

			for _, step := range []struct {
				dst   *[]ProxyRule
				rules []CreateProxyRule
			}{
				{&next.PreDNS, in.Rules.PreDNS},
				{&next.PreREQUEST, in.Rules.PreREQUEST},
				{&next.PostRESPONSE, in.Rules.PostRESPONSE},
			} {
				if err := add(step.dst, step.rules); err != nil {
					return err
				}
			}

			r.Rules = next

			return nil
		}, b.proxyRuleGroupOutputLocked)
}

// DeleteProxyRules removes rules by name from a rule group.
func (b *InMemoryBackend) DeleteProxyRules(
	_ context.Context,
	in *DeleteProxyRulesInput,
) (*DeleteProxyRulesOutput, error) {
	if len(in.Rules) == 0 {
		return nil, invalid("Rules is required")
	}

	return mutateRuleGroup(b, "DeleteProxyRules", arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName}, "", false,
		func(r *proxyRuleGroupRecord) error {
			next := cloneRules(&r.Rules)

			for _, name := range in.Rules {
				s, i := next.find(name)
				if s == nil {
					return invalid("Proxy rule %s does not exist", name)
				}

				*s = slices.Delete(*s, i, i+1)
			}

			r.Rules = next

			return nil
		},
		func(r *proxyRuleGroupRecord) *DeleteProxyRulesOutput {
			return &DeleteProxyRulesOutput{ProxyRuleGroup: b.proxyRuleGroupViewLocked(r)}
		})
}

// DescribeProxyRule returns one rule of a rule group.
func (b *InMemoryBackend) DescribeProxyRule(
	_ context.Context, in *DescribeProxyRuleInput,
) (*DescribeProxyRuleOutput, error) {
	b.mu.RLock("DescribeProxyRule")
	defer b.mu.RUnlock()

	r, err := lookup(b, b.proxyRuleGroups, kindProxyRuleGroup, "proxy rule group",
		arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName})
	if err != nil {
		return nil, err
	}

	s, i := r.Rules.find(in.ProxyRuleName)
	if s == nil {
		return nil, notFound("proxy rule", in.ProxyRuleName)
	}

	return &DescribeProxyRuleOutput{ProxyRule: (*s)[i], UpdateToken: r.UpdateToken}, nil
}

func removeConditions(have, drop []ProxyRuleCondition) ([]ProxyRuleCondition, []ProxyRuleCondition) {
	var kept, removed []ProxyRuleCondition

	for _, c := range have {
		if slices.ContainsFunc(drop, func(d ProxyRuleCondition) bool {
			return d.ConditionKey == c.ConditionKey && d.ConditionOperator == c.ConditionOperator &&
				slices.Equal(d.ConditionValues, c.ConditionValues)
		}) {
			removed = append(removed, c)

			continue
		}

		kept = append(kept, c)
	}

	return kept, removed
}

// UpdateProxyRule changes a rule's action, description and conditions.
func (b *InMemoryBackend) UpdateProxyRule(_ context.Context, in *UpdateProxyRuleInput) (*UpdateProxyRuleOutput, error) {
	if in.ProxyRuleName == "" {
		return nil, invalid("ProxyRuleName is required")
	}

	if err := validatePhaseAction(in.Action); err != nil {
		return nil, err
	}

	if err := validateConditions(in.AddConditions); err != nil {
		return nil, err
	}

	var removed []ProxyRuleCondition

	return mutateRuleGroup(b, "UpdateProxyRule", arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName},
		in.UpdateToken, true,
		func(r *proxyRuleGroupRecord) error {
			next := cloneRules(&r.Rules)

			s, i := next.find(in.ProxyRuleName)
			if s == nil {
				return notFound("proxy rule", in.ProxyRuleName)
			}

			rule := (*s)[i]
			if in.Action != "" {
				rule.Action = in.Action
			}

			if in.Description != nil {
				if err := validateDescription(*in.Description); err != nil {
					return err
				}

				rule.Description = *in.Description
			}

			rule.Conditions, removed = removeConditions(rule.Conditions, in.RemoveConditions)
			rule.Conditions = append(rule.Conditions, in.AddConditions...)
			(*s)[i] = rule
			r.Rules = next

			return nil
		},
		func(r *proxyRuleGroupRecord) *UpdateProxyRuleOutput {
			s, i := r.Rules.find(in.ProxyRuleName)

			return &UpdateProxyRuleOutput{ProxyRule: (*s)[i], RemovedConditions: removed, UpdateToken: r.UpdateToken}
		})
}

// UpdateProxyRulePriorities moves rules within one request phase.
func (b *InMemoryBackend) UpdateProxyRulePriorities(
	_ context.Context, in *UpdateProxyRulePrioritiesInput,
) (*UpdateProxyRulePrioritiesOutput, error) {
	if len(in.Rules) == 0 {
		return nil, invalid("Rules is required")
	}

	return mutateRuleGroup(b, "UpdateProxyRulePriorities", arnOrName{in.ProxyRuleGroupArn, in.ProxyRuleGroupName},
		in.UpdateToken, true,
		func(r *proxyRuleGroupRecord) error {
			next := cloneRules(&r.Rules)

			phase, err := phaseSlice(&next, in.RuleGroupRequestPhase)
			if err != nil {
				return err
			}

			for _, p := range in.Rules {
				from := slices.IndexFunc(*phase, func(x ProxyRule) bool { return x.ProxyRuleName == p.ProxyRuleName })
				if from < 0 {
					return invalid(
						"Proxy rule %s does not exist in phase %s",
						p.ProxyRuleName,
						in.RuleGroupRequestPhase,
					)
				}

				*phase = moveTo(*phase, from, p.NewPosition)
			}

			r.Rules = next

			return nil
		},
		func(r *proxyRuleGroupRecord) *UpdateProxyRulePrioritiesOutput {
			phase, _ := phaseSlice(&r.Rules, in.RuleGroupRequestPhase)
			out := make([]ProxyRulePriority, 0, len(*phase))

			for i, x := range *phase {
				out = append(out, ProxyRulePriority{NewPosition: new(toInt32(i)), ProxyRuleName: x.ProxyRuleName})
			}

			return &UpdateProxyRulePrioritiesOutput{
				ProxyRuleGroupArn: r.ARN, ProxyRuleGroupName: r.Name,
				RuleGroupRequestPhase: in.RuleGroupRequestPhase, Rules: out, UpdateToken: r.UpdateToken,
			}
		})
}
