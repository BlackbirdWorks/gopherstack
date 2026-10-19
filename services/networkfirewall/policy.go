package networkfirewall

import (
	"context"
	"slices"
	"strings"
	"time"
)

// CreateFirewallPolicyInput is the CreateFirewallPolicy request.
type CreateFirewallPolicyInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	FirewallPolicy          *FirewallPolicy          `json:"FirewallPolicy"`
	FirewallPolicyName      string                   `json:"FirewallPolicyName"`
	Description             string                   `json:"Description"`
	Tags                    []Tag                    `json:"Tags"`
	DryRun                  bool                     `json:"DryRun"`
}

// FirewallPolicyOutput is the Create/Update FirewallPolicy response.
type FirewallPolicyOutput struct {
	UpdateToken            string                 `json:"UpdateToken"`
	FirewallPolicyResponse FirewallPolicyResponse `json:"FirewallPolicyResponse"`
}

// DescribeFirewallPolicyInput is the DescribeFirewallPolicy request.
type DescribeFirewallPolicyInput struct {
	FirewallPolicyArn  string `json:"FirewallPolicyArn"`
	FirewallPolicyName string `json:"FirewallPolicyName"`
}

// DescribeFirewallPolicyOutput is the DescribeFirewallPolicy response.
type DescribeFirewallPolicyOutput struct {
	FirewallPolicy         *FirewallPolicy        `json:"FirewallPolicy,omitempty"`
	UpdateToken            string                 `json:"UpdateToken"`
	FirewallPolicyResponse FirewallPolicyResponse `json:"FirewallPolicyResponse"`
}

// ListFirewallPoliciesInput is the ListFirewallPolicies request.
type ListFirewallPoliciesInput struct {
	MaxResults *int32 `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

// ListFirewallPoliciesOutput is the ListFirewallPolicies response.
type ListFirewallPoliciesOutput struct {
	NextToken        string             `json:"NextToken,omitempty"`
	FirewallPolicies []ResourceMetadata `json:"FirewallPolicies"`
}

// UpdateFirewallPolicyInput is the UpdateFirewallPolicy request.
type UpdateFirewallPolicyInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	FirewallPolicy          *FirewallPolicy          `json:"FirewallPolicy"`
	UpdateToken             string                   `json:"UpdateToken"`
	FirewallPolicyArn       string                   `json:"FirewallPolicyArn"`
	FirewallPolicyName      string                   `json:"FirewallPolicyName"`
	Description             string                   `json:"Description"`
	DryRun                  bool                     `json:"DryRun"`
}

// DeleteFirewallPolicyInput is the DeleteFirewallPolicy request.
type DeleteFirewallPolicyInput = DescribeFirewallPolicyInput

// DeleteFirewallPolicyOutput is the DeleteFirewallPolicy response.
type DeleteFirewallPolicyOutput struct {
	FirewallPolicyResponse FirewallPolicyResponse `json:"FirewallPolicyResponse"`
}

func (b *InMemoryBackend) validatePolicyLocked(p *FirewallPolicy) error {
	if p == nil {
		return invalid("FirewallPolicy is required")
	}

	if len(p.StatelessDefaultActions) == 0 || len(p.StatelessFragmentDefaultActions) == 0 {
		return invalid("StatelessDefaultActions and StatelessFragmentDefaultActions are required")
	}

	priorities := map[int32]bool{}

	for _, ref := range p.StatelessRuleGroupReferences {
		g, ok := b.ruleGroups.Get(ref.ResourceArn)
		if !ok || g.Type != ruleGroupStateless {
			return notFound("stateless rule group", ref.ResourceArn)
		}

		if priorities[ref.Priority] {
			return invalid("Stateless rule group priority %d is used more than once", ref.Priority)
		}

		priorities[ref.Priority] = true
	}

	for _, ref := range p.StatefulRuleGroupReferences {
		if g, ok := b.ruleGroups.Get(ref.ResourceArn); !ok || g.Type == ruleGroupStateless {
			return notFound("stateful rule group", ref.ResourceArn)
		}
	}

	if p.TLSInspectionConfigurationArn != "" && !b.tlsConfigs.Has(p.TLSInspectionConfigurationArn) {
		return notFound("TLS inspection configuration", p.TLSInspectionConfigurationArn)
	}

	return nil
}

func (b *InMemoryBackend) policyResponseLocked(r *policyRecord) FirewallPolicyResponse {
	var stateless, stateful, users int32

	for _, ref := range r.Policy.StatelessRuleGroupReferences {
		if g, ok := b.ruleGroups.Get(ref.ResourceArn); ok {
			stateless += g.Capacity
		}
	}

	for _, ref := range r.Policy.StatefulRuleGroupReferences {
		if g, ok := b.ruleGroups.Get(ref.ResourceArn); ok {
			stateful += g.Capacity
		}
	}

	now := time.Now()

	for _, fw := range b.firewalls.All() {
		if fw.PolicyARN == r.ARN && !fw.expired(now) {
			users++
		}
	}

	return FirewallPolicyResponse{
		FirewallPolicyArn:             r.ARN,
		FirewallPolicyID:              r.ID,
		FirewallPolicyName:            r.Name,
		Description:                   r.Description,
		FirewallPolicyStatus:          resourceStatusActive,
		ConsumedStatelessRuleCapacity: new(stateless),
		ConsumedStatefulRuleCapacity:  new(stateful),
		NumberOfAssociations:          new(users),
		LastModifiedTime:              epoch(r.LastModified),
		EncryptionConfiguration:       normalizeEncryption(r.Encryption),
		Tags:                          tagList(r.Tags),
	}
}

func (b *InMemoryBackend) findPolicyLocked(arnStr, name string) (*policyRecord, error) {
	if err := targetNeeded(arnStr, name); err != nil {
		return nil, err
	}

	key := arnStr
	if key == "" {
		key = b.arnFor(kindPolicy, name)
	}

	r, ok := b.policies.Get(key)
	if !ok {
		return nil, notFound("firewall policy", strings.TrimPrefix(key, "arn:"))
	}

	return r, nil
}

// CreateFirewallPolicy creates a firewall policy.
func (b *InMemoryBackend) CreateFirewallPolicy(
	_ context.Context, in *CreateFirewallPolicyInput,
) (*FirewallPolicyOutput, error) {
	if err := validateName("FirewallPolicyName", in.FirewallPolicyName); err != nil {
		return nil, err
	}

	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	if err := validateEncryption(in.EncryptionConfiguration); err != nil {
		return nil, err
	}

	if err := validateTags(tagMap(in.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFirewallPolicy")
	defer b.mu.Unlock()

	if err := b.validatePolicyLocked(in.FirewallPolicy); err != nil {
		return nil, err
	}

	policyARN := b.arnFor(kindPolicy, in.FirewallPolicyName)
	if b.policies.Has(policyARN) {
		return nil, invalid("A firewall policy named %s already exists", in.FirewallPolicyName)
	}

	r := &policyRecord{
		Name:         in.FirewallPolicyName,
		ID:           newResourceID(),
		ARN:          policyARN,
		Description:  in.Description,
		UpdateToken:  newToken(),
		Policy:       *in.FirewallPolicy,
		Encryption:   normalizeEncryption(in.EncryptionConfiguration),
		Tags:         tagMap(in.Tags),
		LastModified: time.Now(),
	}

	if !in.DryRun {
		b.policies.Put(r)
	}

	return &FirewallPolicyOutput{UpdateToken: r.UpdateToken, FirewallPolicyResponse: b.policyResponseLocked(r)}, nil
}

// DescribeFirewallPolicy returns a policy and its body.
func (b *InMemoryBackend) DescribeFirewallPolicy(
	_ context.Context, in *DescribeFirewallPolicyInput,
) (*DescribeFirewallPolicyOutput, error) {
	b.mu.RLock("DescribeFirewallPolicy")
	defer b.mu.RUnlock()

	r, err := b.findPolicyLocked(in.FirewallPolicyArn, in.FirewallPolicyName)
	if err != nil {
		return nil, err
	}

	body := r.Policy

	return &DescribeFirewallPolicyOutput{
		FirewallPolicy: &body, UpdateToken: r.UpdateToken, FirewallPolicyResponse: b.policyResponseLocked(r),
	}, nil
}

// ListFirewallPolicies lists policies by name.
func (b *InMemoryBackend) ListFirewallPolicies(
	_ context.Context, in *ListFirewallPoliciesInput,
) (*ListFirewallPoliciesOutput, error) {
	b.mu.RLock("ListFirewallPolicies")
	defer b.mu.RUnlock()

	all := make([]ResourceMetadata, 0, b.policies.Len())
	for _, r := range b.policies.All() {
		all = append(all, ResourceMetadata{Arn: r.ARN, Name: r.Name})
	}

	slices.SortFunc(all, func(a, c ResourceMetadata) int { return strings.Compare(a.Name, c.Name) })

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListFirewallPoliciesOutput{FirewallPolicies: pg.Data, NextToken: pg.Next}, nil
}

// UpdateFirewallPolicy replaces a policy body, guarded by the update token.
func (b *InMemoryBackend) UpdateFirewallPolicy(
	_ context.Context, in *UpdateFirewallPolicyInput,
) (*FirewallPolicyOutput, error) {
	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	if err := validateEncryption(in.EncryptionConfiguration); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFirewallPolicy")
	defer b.mu.Unlock()

	r, err := b.findPolicyLocked(in.FirewallPolicyArn, in.FirewallPolicyName)
	if err != nil {
		return nil, err
	}

	if err = requireToken(r.UpdateToken, in.UpdateToken); err != nil {
		return nil, err
	}

	if err = b.validatePolicyLocked(in.FirewallPolicy); err != nil {
		return nil, err
	}

	next := *r
	next.Description = in.Description
	next.Policy = *in.FirewallPolicy
	next.Encryption = normalizeEncryption(in.EncryptionConfiguration)
	next.UpdateToken = newToken()
	next.LastModified = time.Now()

	if in.DryRun {
		return &FirewallPolicyOutput{
			UpdateToken:            next.UpdateToken,
			FirewallPolicyResponse: b.policyResponseLocked(&next),
		}, nil
	}

	*r = next

	return &FirewallPolicyOutput{UpdateToken: r.UpdateToken, FirewallPolicyResponse: b.policyResponseLocked(r)}, nil
}

// DeleteFirewallPolicy deletes an unused policy.
func (b *InMemoryBackend) DeleteFirewallPolicy(
	_ context.Context, in *DeleteFirewallPolicyInput,
) (*DeleteFirewallPolicyOutput, error) {
	b.mu.Lock("DeleteFirewallPolicy")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	r, err := b.findPolicyLocked(in.FirewallPolicyArn, in.FirewallPolicyName)
	if err != nil {
		return nil, err
	}

	for _, fw := range b.firewalls.All() {
		if fw.PolicyARN == r.ARN {
			return nil, invalidOp("The firewall policy %s is in use by firewall %s", r.Name, fw.Name)
		}
	}

	resp := b.policyResponseLocked(r)
	resp.FirewallPolicyStatus = statusDeleting

	b.policies.Delete(r.ARN)
	b.resourcePolicies.Delete(r.ARN)

	return &DeleteFirewallPolicyOutput{FirewallPolicyResponse: resp}, nil
}
