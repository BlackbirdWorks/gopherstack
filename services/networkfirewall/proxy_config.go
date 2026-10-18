package networkfirewall

import (
	"context"
	"slices"
	"time"
)

const (
	actionAllow = "ALLOW"
	actionDeny  = "DENY"
	actionAlert = "ALERT"
)

// ProxyConfigDefaultRulePhaseActions are the default actions per request phase.
type ProxyConfigDefaultRulePhaseActions struct {
	PostRESPONSE string `json:"PostRESPONSE,omitempty"`
	PreDNS       string `json:"PreDNS,omitempty"`
	PreREQUEST   string `json:"PreREQUEST,omitempty"`
}

// ProxyConfigRuleGroup is a rule group attached to a proxy configuration.
type ProxyConfigRuleGroup struct {
	Priority           *int32 `json:"Priority,omitempty"`
	ProxyRuleGroupArn  string `json:"ProxyRuleGroupArn"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// ProxyConfiguration is the ProxyConfiguration response shape.
type ProxyConfiguration struct {
	CreateTime              *float64                            `json:"CreateTime,omitempty"`
	DefaultRulePhaseActions *ProxyConfigDefaultRulePhaseActions `json:"DefaultRulePhaseActions,omitempty"`
	Description             string                              `json:"Description,omitempty"`
	ProxyConfigurationArn   string                              `json:"ProxyConfigurationArn"`
	ProxyConfigurationName  string                              `json:"ProxyConfigurationName"`
	RuleGroups              []ProxyConfigRuleGroup              `json:"RuleGroups"`
	Tags                    []Tag                               `json:"Tags,omitempty"`
}

// CreateProxyConfigurationInput is the CreateProxyConfiguration request.
type CreateProxyConfigurationInput struct {
	DefaultRulePhaseActions *ProxyConfigDefaultRulePhaseActions `json:"DefaultRulePhaseActions"`
	ProxyConfigurationName  string                              `json:"ProxyConfigurationName"`
	Description             string                              `json:"Description"`
	RuleGroupArns           []string                            `json:"RuleGroupArns"`
	RuleGroupNames          []string                            `json:"RuleGroupNames"`
	Tags                    []Tag                               `json:"Tags"`
}

// ProxyConfigurationOutput is the response of every call that returns a configuration and token.
type ProxyConfigurationOutput struct {
	UpdateToken        string             `json:"UpdateToken"`
	ProxyConfiguration ProxyConfiguration `json:"ProxyConfiguration"`
}

// DescribeProxyConfigurationInput is the DescribeProxyConfiguration request.
type DescribeProxyConfigurationInput struct {
	ProxyConfigurationArn  string `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string `json:"ProxyConfigurationName"`
}

// UpdateProxyConfigurationInput is the UpdateProxyConfiguration request.
type UpdateProxyConfigurationInput struct {
	DefaultRulePhaseActions *ProxyConfigDefaultRulePhaseActions `json:"DefaultRulePhaseActions"`
	ProxyConfigurationArn   string                              `json:"ProxyConfigurationArn"`
	ProxyConfigurationName  string                              `json:"ProxyConfigurationName"`
	UpdateToken             string                              `json:"UpdateToken"`
}

// DeleteProxyConfigurationInput is the DeleteProxyConfiguration request.
type DeleteProxyConfigurationInput = DescribeProxyConfigurationInput

// DeleteProxyConfigurationOutput is the DeleteProxyConfiguration response.
type DeleteProxyConfigurationOutput struct {
	ProxyConfigurationArn  string `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string `json:"ProxyConfigurationName"`
}

// ListProxyConfigurationsInput is the ListProxyConfigurations request.
type ListProxyConfigurationsInput = ListProxiesInput

// ListProxyConfigurationsOutput is the ListProxyConfigurations response.
type ListProxyConfigurationsOutput struct {
	NextToken           string             `json:"NextToken,omitempty"`
	ProxyConfigurations []ResourceMetadata `json:"ProxyConfigurations"`
}

// ProxyRuleGroupAttachment names a rule group to attach and where to put it.
type ProxyRuleGroupAttachment struct {
	InsertPosition     *int32 `json:"InsertPosition,omitempty"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// AttachRuleGroupsToProxyConfigurationInput is the AttachRuleGroupsToProxyConfiguration request.
type AttachRuleGroupsToProxyConfigurationInput struct {
	ProxyConfigurationArn  string                     `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string                     `json:"ProxyConfigurationName"`
	UpdateToken            string                     `json:"UpdateToken"`
	RuleGroups             []ProxyRuleGroupAttachment `json:"RuleGroups"`
}

// DetachRuleGroupsFromProxyConfigurationInput is the DetachRuleGroupsFromProxyConfiguration request.
type DetachRuleGroupsFromProxyConfigurationInput struct {
	ProxyConfigurationArn  string   `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string   `json:"ProxyConfigurationName"`
	UpdateToken            string   `json:"UpdateToken"`
	RuleGroupArns          []string `json:"RuleGroupArns"`
	RuleGroupNames         []string `json:"RuleGroupNames"`
}

// ProxyRuleGroupPriority moves a rule group within a proxy configuration.
type ProxyRuleGroupPriority struct {
	NewPosition        *int32 `json:"NewPosition,omitempty"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// ProxyRuleGroupPriorityResult is a rule group and its resulting priority.
type ProxyRuleGroupPriorityResult struct {
	Priority           *int32 `json:"Priority,omitempty"`
	ProxyRuleGroupName string `json:"ProxyRuleGroupName"`
}

// UpdateProxyRuleGroupPrioritiesInput is the UpdateProxyRuleGroupPriorities request.
type UpdateProxyRuleGroupPrioritiesInput struct {
	ProxyConfigurationArn  string                   `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string                   `json:"ProxyConfigurationName"`
	UpdateToken            string                   `json:"UpdateToken"`
	RuleGroups             []ProxyRuleGroupPriority `json:"RuleGroups"`
}

// UpdateProxyRuleGroupPrioritiesOutput is the UpdateProxyRuleGroupPriorities response.
type UpdateProxyRuleGroupPrioritiesOutput struct {
	UpdateToken     string                         `json:"UpdateToken"`
	ProxyRuleGroups []ProxyRuleGroupPriorityResult `json:"ProxyRuleGroups"`
}

func validatePhaseAction(a string) error {
	switch a {
	case "", actionAllow, actionDeny, actionAlert:
		return nil
	default:
		return invalid("Action %q is not valid", a)
	}
}

func validateDefaultActions(d *ProxyConfigDefaultRulePhaseActions) error {
	if d == nil {
		return invalid("DefaultRulePhaseActions is required")
	}

	for _, a := range []string{d.PostRESPONSE, d.PreDNS, d.PreREQUEST} {
		if err := validatePhaseAction(a); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) proxyConfigViewLocked(r *proxyConfigRecord) ProxyConfiguration {
	groups := make([]ProxyConfigRuleGroup, 0, len(r.Groups))

	for i, g := range r.Groups {
		name := ""
		if rg, ok := b.proxyRuleGroups.Get(g); ok {
			name = rg.Name
		}

		groups = append(groups, ProxyConfigRuleGroup{
			Priority: new(toInt32(i)), ProxyRuleGroupArn: g, ProxyRuleGroupName: name,
		})
	}

	return ProxyConfiguration{
		CreateTime:              epoch(r.CreateTime),
		DefaultRulePhaseActions: r.Defaults,
		Description:             r.Description,
		ProxyConfigurationArn:   r.ARN,
		ProxyConfigurationName:  r.Name,
		RuleGroups:              groups,
		Tags:                    tagList(r.Tags),
	}
}

func (b *InMemoryBackend) proxyConfigOutputLocked(r *proxyConfigRecord) *ProxyConfigurationOutput {
	return &ProxyConfigurationOutput{UpdateToken: r.UpdateToken, ProxyConfiguration: b.proxyConfigViewLocked(r)}
}

// resolveRuleGroupLocked returns the ARN of a proxy rule group named by ARN or name.
func (b *InMemoryBackend) resolveRuleGroupLocked(arnStr, name string) (string, error) {
	g, err := lookup(b, b.proxyRuleGroups, kindProxyRuleGroup, "proxy rule group", arnOrName{arnStr, name})
	if err != nil {
		return "", err
	}

	return g.ARN, nil
}

// CreateProxyConfiguration creates a proxy configuration.
func (b *InMemoryBackend) CreateProxyConfiguration(
	_ context.Context, in *CreateProxyConfigurationInput,
) (*ProxyConfigurationOutput, error) {
	if err := validateName("ProxyConfigurationName", in.ProxyConfigurationName); err != nil {
		return nil, err
	}

	if err := validateDefaultActions(in.DefaultRulePhaseActions); err != nil {
		return nil, err
	}

	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	if err := validateTags(tagMap(in.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateProxyConfiguration")
	defer b.mu.Unlock()

	cfgARN := b.arnFor(kindProxyConfig, in.ProxyConfigurationName)
	if b.proxyConfigs.Has(cfgARN) {
		return nil, invalid("A proxy configuration named %s already exists", in.ProxyConfigurationName)
	}

	var groups []string

	refs := make([]arnOrName, 0, len(in.RuleGroupArns)+len(in.RuleGroupNames))
	for _, a := range in.RuleGroupArns {
		refs = append(refs, arnOrName{arn: a})
	}

	for _, n := range in.RuleGroupNames {
		refs = append(refs, arnOrName{name: n})
	}

	for _, ref := range refs {
		g, err := b.resolveRuleGroupLocked(ref.arn, ref.name)
		if err != nil {
			return nil, err
		}

		if slices.Contains(groups, g) {
			return nil, invalid("Proxy rule group %s is specified more than once", g)
		}

		groups = append(groups, g)
	}

	r := &proxyConfigRecord{
		Name:        in.ProxyConfigurationName,
		ARN:         cfgARN,
		Description: in.Description,
		UpdateToken: newToken(),
		Defaults:    in.DefaultRulePhaseActions,
		Groups:      groups,
		Tags:        tagMap(in.Tags),
		CreateTime:  time.Now(),
	}

	b.proxyConfigs.Put(r)

	return b.proxyConfigOutputLocked(r), nil
}

// DescribeProxyConfiguration returns a proxy configuration and its update token.
func (b *InMemoryBackend) DescribeProxyConfiguration(
	_ context.Context, in *DescribeProxyConfigurationInput,
) (*ProxyConfigurationOutput, error) {
	b.mu.RLock("DescribeProxyConfiguration")
	defer b.mu.RUnlock()

	r, err := lookup(b, b.proxyConfigs, kindProxyConfig, "proxy configuration",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName})
	if err != nil {
		return nil, err
	}

	return b.proxyConfigOutputLocked(r), nil
}

// ListProxyConfigurations lists proxy configurations.
func (b *InMemoryBackend) ListProxyConfigurations(
	_ context.Context, in *ListProxyConfigurationsInput,
) (*ListProxyConfigurationsOutput, error) {
	b.mu.RLock("ListProxyConfigurations")
	defer b.mu.RUnlock()

	data, next, err := listMetadata(b.proxyConfigs.All(),
		func(r *proxyConfigRecord) ResourceMetadata { return ResourceMetadata{Arn: r.ARN, Name: r.Name} },
		in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListProxyConfigurationsOutput{ProxyConfigurations: data, NextToken: next}, nil
}

// mutateProxyConfig applies change to a configuration under the write lock after the token check.
func mutateProxyConfig[T any](
	b *InMemoryBackend, op string, id arnOrName, token string,
	change func(*proxyConfigRecord) error, build func(*proxyConfigRecord) *T,
) (*T, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	r, err := lookup(b, b.proxyConfigs, kindProxyConfig, "proxy configuration", id)
	if err != nil {
		return nil, err
	}

	if err = requireToken(r.UpdateToken, token); err != nil {
		return nil, err
	}

	if err = change(r); err != nil {
		return nil, err
	}

	r.UpdateToken = newToken()

	return build(r), nil
}

// UpdateProxyConfiguration replaces the default rule phase actions.
func (b *InMemoryBackend) UpdateProxyConfiguration(
	_ context.Context, in *UpdateProxyConfigurationInput,
) (*ProxyConfigurationOutput, error) {
	if err := validateDefaultActions(in.DefaultRulePhaseActions); err != nil {
		return nil, err
	}

	return b.configChange("UpdateProxyConfiguration", arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName},
		in.UpdateToken, func(r *proxyConfigRecord) error {
			r.Defaults = in.DefaultRulePhaseActions

			return nil
		})
}

func (b *InMemoryBackend) configChange(
	op string, id arnOrName, token string, change func(*proxyConfigRecord) error,
) (*ProxyConfigurationOutput, error) {
	return mutateProxyConfig(b, op, id, token, change, b.proxyConfigOutputLocked)
}

// AttachRuleGroupsToProxyConfiguration attaches rule groups at the requested positions.
func (b *InMemoryBackend) AttachRuleGroupsToProxyConfiguration(
	_ context.Context, in *AttachRuleGroupsToProxyConfigurationInput,
) (*ProxyConfigurationOutput, error) {
	if len(in.RuleGroups) == 0 {
		return nil, invalid("RuleGroups is required")
	}

	return b.configChange("AttachRuleGroupsToProxyConfiguration",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName}, in.UpdateToken,
		func(r *proxyConfigRecord) error {
			groups := slices.Clone(r.Groups)

			for _, a := range in.RuleGroups {
				g, err := b.resolveRuleGroupLocked("", a.ProxyRuleGroupName)
				if err != nil {
					return err
				}

				if slices.Contains(groups, g) {
					return invalid("Proxy rule group %s is already attached", a.ProxyRuleGroupName)
				}

				groups = insertAt(groups, a.InsertPosition, g)
			}

			r.Groups = groups

			return nil
		})
}

// DetachRuleGroupsFromProxyConfiguration detaches rule groups; the rest keep their order.
func (b *InMemoryBackend) DetachRuleGroupsFromProxyConfiguration(
	_ context.Context, in *DetachRuleGroupsFromProxyConfigurationInput,
) (*ProxyConfigurationOutput, error) {
	if len(in.RuleGroupArns)+len(in.RuleGroupNames) == 0 {
		return nil, invalid("RuleGroupArns or RuleGroupNames is required")
	}

	return b.configChange("DetachRuleGroupsFromProxyConfiguration",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName}, in.UpdateToken,
		func(r *proxyConfigRecord) error {
			groups := slices.Clone(r.Groups)

			detach := func(arnStr, name string) error {
				g, err := b.resolveRuleGroupLocked(arnStr, name)
				if err != nil {
					return err
				}

				if !slices.Contains(groups, g) {
					return invalid("Proxy rule group %s is not attached", g)
				}

				groups = slices.DeleteFunc(groups, func(x string) bool { return x == g })

				return nil
			}

			for _, a := range in.RuleGroupArns {
				if err := detach(a, ""); err != nil {
					return err
				}
			}

			for _, n := range in.RuleGroupNames {
				if err := detach("", n); err != nil {
					return err
				}
			}

			r.Groups = groups

			return nil
		})
}

// UpdateProxyRuleGroupPriorities moves attached rule groups to new positions.
func (b *InMemoryBackend) UpdateProxyRuleGroupPriorities(
	_ context.Context, in *UpdateProxyRuleGroupPrioritiesInput,
) (*UpdateProxyRuleGroupPrioritiesOutput, error) {
	if len(in.RuleGroups) == 0 {
		return nil, invalid("RuleGroups is required")
	}

	return mutateProxyConfig(b, "UpdateProxyRuleGroupPriorities",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName}, in.UpdateToken,
		func(r *proxyConfigRecord) error {
			groups := slices.Clone(r.Groups)

			for _, p := range in.RuleGroups {
				g, rerr := b.resolveRuleGroupLocked("", p.ProxyRuleGroupName)
				if rerr != nil {
					return rerr
				}

				from := slices.Index(groups, g)
				if from < 0 {
					return invalid("Proxy rule group %s is not attached", p.ProxyRuleGroupName)
				}

				groups = moveTo(groups, from, p.NewPosition)
			}

			r.Groups = groups

			return nil
		},
		func(r *proxyConfigRecord) *UpdateProxyRuleGroupPrioritiesOutput {
			cfg := b.proxyConfigViewLocked(r)
			out := make([]ProxyRuleGroupPriorityResult, 0, len(cfg.RuleGroups))

			for _, g := range cfg.RuleGroups {
				out = append(out, ProxyRuleGroupPriorityResult{
					Priority: g.Priority, ProxyRuleGroupName: g.ProxyRuleGroupName,
				})
			}

			return &UpdateProxyRuleGroupPrioritiesOutput{UpdateToken: r.UpdateToken, ProxyRuleGroups: out}
		})
}

// DeleteProxyConfiguration deletes a configuration no proxy uses.
func (b *InMemoryBackend) DeleteProxyConfiguration(
	_ context.Context, in *DeleteProxyConfigurationInput,
) (*DeleteProxyConfigurationOutput, error) {
	b.mu.Lock("DeleteProxyConfiguration")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepProxiesLocked(now)

	r, err := lookup(b, b.proxyConfigs, kindProxyConfig, "proxy configuration",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName})
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(b.proxies.All(), func(p *proxyRecord) bool { return p.ConfigARN == r.ARN }) {
		return nil, invalidOp("The proxy configuration %s is used by a proxy", r.Name)
	}

	b.proxyConfigs.Delete(r.ARN)

	return &DeleteProxyConfigurationOutput{ProxyConfigurationArn: r.ARN, ProxyConfigurationName: r.Name}, nil
}
