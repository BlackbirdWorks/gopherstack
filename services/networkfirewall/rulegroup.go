package networkfirewall

import (
	"context"
	"slices"
	"strings"
	"time"
)

const maxRuleGroupCapacity = 30000

// CreateRuleGroupInput is the CreateRuleGroup request.
type CreateRuleGroupInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	RuleGroup               *RuleGroup               `json:"RuleGroup"`
	SourceMetadata          *SourceMetadata          `json:"SourceMetadata"`
	SummaryConfiguration    *SummaryConfiguration    `json:"SummaryConfiguration"`
	Capacity                *int32                   `json:"Capacity"`
	RuleGroupName           string                   `json:"RuleGroupName"`
	Type                    string                   `json:"Type"`
	Description             string                   `json:"Description"`
	Rules                   string                   `json:"Rules"`
	Tags                    []Tag                    `json:"Tags"`
	AnalyzeRuleGroup        bool                     `json:"AnalyzeRuleGroup"`
	DryRun                  bool                     `json:"DryRun"`
}

// RuleGroupOutput is the Create/Update RuleGroup response.
type RuleGroupOutput struct {
	UpdateToken       string            `json:"UpdateToken"`
	RuleGroupResponse RuleGroupResponse `json:"RuleGroupResponse"`
}

// DescribeRuleGroupInput is the DescribeRuleGroup request.
type DescribeRuleGroupInput struct {
	RuleGroupArn     string `json:"RuleGroupArn"`
	RuleGroupName    string `json:"RuleGroupName"`
	Type             string `json:"Type"`
	AnalyzeRuleGroup bool   `json:"AnalyzeRuleGroup"`
}

// DescribeRuleGroupOutput is the DescribeRuleGroup response.
type DescribeRuleGroupOutput struct {
	RuleGroup         *RuleGroup        `json:"RuleGroup,omitempty"`
	UpdateToken       string            `json:"UpdateToken"`
	RuleGroupResponse RuleGroupResponse `json:"RuleGroupResponse"`
}

// DescribeRuleGroupMetadataInput is the DescribeRuleGroupMetadata request.
type DescribeRuleGroupMetadataInput struct {
	RuleGroupArn  string `json:"RuleGroupArn"`
	RuleGroupName string `json:"RuleGroupName"`
	Type          string `json:"Type"`
}

// DescribeRuleGroupMetadataOutput is the DescribeRuleGroupMetadata response.
type DescribeRuleGroupMetadataOutput struct {
	Capacity            *int32               `json:"Capacity,omitempty"`
	LastModifiedTime    *float64             `json:"LastModifiedTime,omitempty"`
	StatefulRuleOptions *StatefulRuleOptions `json:"StatefulRuleOptions,omitempty"`
	RuleGroupArn        string               `json:"RuleGroupArn"`
	RuleGroupName       string               `json:"RuleGroupName"`
	Description         string               `json:"Description,omitempty"`
	Type                string               `json:"Type,omitempty"`
}

// ListRuleGroupsInput is the ListRuleGroups request.
type ListRuleGroupsInput struct {
	MaxResults         *int32 `json:"MaxResults"`
	ManagedType        string `json:"ManagedType"`
	NextToken          string `json:"NextToken"`
	Scope              string `json:"Scope"`
	SubscriptionStatus string `json:"SubscriptionStatus"`
	Type               string `json:"Type"`
}

// ListRuleGroupsOutput is the ListRuleGroups response.
type ListRuleGroupsOutput struct {
	NextToken  string             `json:"NextToken,omitempty"`
	RuleGroups []ResourceMetadata `json:"RuleGroups"`
}

// UpdateRuleGroupInput is the UpdateRuleGroup request.
type UpdateRuleGroupInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	RuleGroup               *RuleGroup               `json:"RuleGroup"`
	SourceMetadata          *SourceMetadata          `json:"SourceMetadata"`
	SummaryConfiguration    *SummaryConfiguration    `json:"SummaryConfiguration"`
	UpdateToken             string                   `json:"UpdateToken"`
	RuleGroupArn            string                   `json:"RuleGroupArn"`
	RuleGroupName           string                   `json:"RuleGroupName"`
	Type                    string                   `json:"Type"`
	Description             string                   `json:"Description"`
	Rules                   string                   `json:"Rules"`
	AnalyzeRuleGroup        bool                     `json:"AnalyzeRuleGroup"`
	DryRun                  bool                     `json:"DryRun"`
}

// DeleteRuleGroupInput is the DeleteRuleGroup request.
type DeleteRuleGroupInput = DescribeRuleGroupMetadataInput

// DeleteRuleGroupOutput is the DeleteRuleGroup response.
type DeleteRuleGroupOutput struct {
	RuleGroupResponse RuleGroupResponse `json:"RuleGroupResponse"`
}

func ruleGroupKind(groupType string) (string, error) {
	switch groupType {
	case ruleGroupStateless:
		return kindStateless, nil
	case ruleGroupStateful:
		return kindStateful, nil
	default:
		return "", invalid("Type must be STATELESS or STATEFUL")
	}
}

func countRuleLines(rules string) int32 {
	var n int32

	for line := range strings.SplitSeq(rules, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			n++
		}
	}

	return n
}

// consumedCapacity is one unit per rule, domain target or stateless rule.
func consumedCapacity(groupType string, g *RuleGroup) int32 {
	if groupType == ruleGroupStateless {
		if g.RulesSource.StatelessRulesAndCustomActions == nil {
			return 0
		}

		return toInt32(len(g.RulesSource.StatelessRulesAndCustomActions.StatelessRules))
	}

	n := int(countRuleLines(g.RulesSource.RulesString)) + len(g.RulesSource.StatefulRules)
	if l := g.RulesSource.RulesSourceList; l != nil {
		n += len(l.Targets)
	}

	return toInt32(n)
}

// buildRuleGroup merges the Rules shortcut into a RuleGroup and checks it matches the group type.
func buildRuleGroup(groupType string, g *RuleGroup, rules string) (RuleGroup, error) {
	if g == nil && rules == "" {
		return RuleGroup{}, invalid("Either RuleGroup or Rules must be specified")
	}

	if g != nil && rules != "" {
		return RuleGroup{}, invalid("RuleGroup and Rules cannot both be specified")
	}

	if g == nil {
		return RuleGroup{RulesSource: RulesSource{RulesString: rules}}, validateRuleGroupType(groupType, &RuleGroup{
			RulesSource: RulesSource{RulesString: rules},
		})
	}

	return *g, validateRuleGroupType(groupType, g)
}

func validateRuleGroupType(groupType string, g *RuleGroup) error {
	src := &g.RulesSource
	stateful := src.RulesString != "" || len(src.StatefulRules) > 0 || src.RulesSourceList != nil

	if groupType == ruleGroupStateless {
		if stateful || src.StatelessRulesAndCustomActions == nil {
			return invalid("A STATELESS rule group requires RulesSource.StatelessRulesAndCustomActions only")
		}

		seen := map[int32]bool{}

		for _, rule := range src.StatelessRulesAndCustomActions.StatelessRules {
			if seen[rule.Priority] {
				return invalid("Stateless rule priority %d is used more than once", rule.Priority)
			}

			seen[rule.Priority] = true
		}

		return nil
	}

	if src.StatelessRulesAndCustomActions != nil || !stateful {
		return invalid("A STATEFUL rule group requires RulesString, RulesSourceList or StatefulRules")
	}

	return nil
}

func (b *InMemoryBackend) ruleGroupResponseLocked(r *ruleGroupRecord) RuleGroupResponse {
	var users int32

	for _, p := range b.policies.All() {
		if policyReferences(&p.Policy, r.ARN) {
			users++
		}
	}

	return RuleGroupResponse{
		RuleGroupArn:            r.ARN,
		RuleGroupID:             r.ID,
		RuleGroupName:           r.Name,
		Description:             r.Description,
		Type:                    r.Type,
		Capacity:                new(r.Capacity),
		ConsumedCapacity:        new(consumedCapacity(r.Type, &r.RuleGroup)),
		NumberOfAssociations:    new(users),
		RuleGroupStatus:         resourceStatusActive,
		LastModifiedTime:        epoch(r.LastModified),
		EncryptionConfiguration: normalizeEncryption(r.Encryption),
		SourceMetadata:          r.Source,
		SummaryConfiguration:    r.Summary,
		Tags:                    tagList(r.Tags),
	}
}

func policyReferences(p *FirewallPolicy, groupARN string) bool {
	return slices.ContainsFunc(p.StatelessRuleGroupReferences, func(r StatelessRuleGroupReference) bool {
		return r.ResourceArn == groupARN
	}) || slices.ContainsFunc(p.StatefulRuleGroupReferences, func(r StatefulRuleGroupReference) bool {
		return r.ResourceArn == groupARN
	})
}

func (b *InMemoryBackend) findRuleGroupLocked(arnStr, name, groupType string) (*ruleGroupRecord, error) {
	if err := targetNeeded(arnStr, name); err != nil {
		return nil, err
	}

	key := arnStr
	if key == "" {
		kind, err := ruleGroupKind(groupType)
		if err != nil {
			return nil, err
		}

		key = b.arnFor(kind, name)
	}

	r, ok := b.ruleGroups.Get(key)
	if !ok {
		return nil, notFound("rule group", strings.TrimPrefix(key, "arn:"))
	}

	return r, nil
}

// CreateRuleGroup creates a stateless or stateful rule group.
func (b *InMemoryBackend) CreateRuleGroup(_ context.Context, in *CreateRuleGroupInput) (*RuleGroupOutput, error) {
	kind, err := ruleGroupKind(in.Type)
	if err != nil {
		return nil, err
	}

	if err = validateName("RuleGroupName", in.RuleGroupName); err != nil {
		return nil, err
	}

	if in.Capacity == nil || *in.Capacity < 1 || *in.Capacity > maxRuleGroupCapacity {
		return nil, invalid("Capacity must be between 1 and %d", maxRuleGroupCapacity)
	}

	body, err := buildRuleGroup(in.Type, in.RuleGroup, in.Rules)
	if err != nil {
		return nil, err
	}

	if used := consumedCapacity(in.Type, &body); used > *in.Capacity {
		return nil, invalid("The rule group needs capacity %d but Capacity is %d", used, *in.Capacity)
	}

	for _, check := range []error{
		validateDescription(in.Description), validateEncryption(in.EncryptionConfiguration),
		validateTags(tagMap(in.Tags)),
	} {
		if check != nil {
			return nil, check
		}
	}

	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	groupARN := b.arnFor(kind, in.RuleGroupName)
	if b.ruleGroups.Has(groupARN) {
		return nil, invalid("A %s rule group named %s already exists", in.Type, in.RuleGroupName)
	}

	r := &ruleGroupRecord{
		Name:         in.RuleGroupName,
		ID:           newResourceID(),
		ARN:          groupARN,
		Type:         in.Type,
		Description:  in.Description,
		UpdateToken:  newToken(),
		Capacity:     *in.Capacity,
		RuleGroup:    body,
		Encryption:   normalizeEncryption(in.EncryptionConfiguration),
		Source:       in.SourceMetadata,
		Summary:      in.SummaryConfiguration,
		Tags:         tagMap(in.Tags),
		LastModified: time.Now(),
	}

	if !in.DryRun {
		b.ruleGroups.Put(r)
	}

	return &RuleGroupOutput{UpdateToken: r.UpdateToken, RuleGroupResponse: b.ruleGroupResponseLocked(r)}, nil
}

// DescribeRuleGroup returns a rule group and its body.
func (b *InMemoryBackend) DescribeRuleGroup(
	_ context.Context, in *DescribeRuleGroupInput,
) (*DescribeRuleGroupOutput, error) {
	b.mu.RLock("DescribeRuleGroup")
	defer b.mu.RUnlock()

	r, err := b.findRuleGroupLocked(in.RuleGroupArn, in.RuleGroupName, in.Type)
	if err != nil {
		return nil, err
	}

	body := r.RuleGroup

	return &DescribeRuleGroupOutput{
		RuleGroup: &body, UpdateToken: r.UpdateToken, RuleGroupResponse: b.ruleGroupResponseLocked(r),
	}, nil
}

// DescribeRuleGroupMetadata returns summary information about a rule group.
func (b *InMemoryBackend) DescribeRuleGroupMetadata(
	_ context.Context, in *DescribeRuleGroupMetadataInput,
) (*DescribeRuleGroupMetadataOutput, error) {
	b.mu.RLock("DescribeRuleGroupMetadata")
	defer b.mu.RUnlock()

	r, err := b.findRuleGroupLocked(in.RuleGroupArn, in.RuleGroupName, in.Type)
	if err != nil {
		return nil, err
	}

	return &DescribeRuleGroupMetadataOutput{
		RuleGroupArn:        r.ARN,
		RuleGroupName:       r.Name,
		Description:         r.Description,
		Type:                r.Type,
		Capacity:            new(r.Capacity),
		LastModifiedTime:    epoch(r.LastModified),
		StatefulRuleOptions: r.RuleGroup.StatefulRuleOptions,
	}, nil
}

// ListRuleGroups lists account rule groups; no managed groups exist.
func (b *InMemoryBackend) ListRuleGroups(_ context.Context, in *ListRuleGroupsInput) (*ListRuleGroupsOutput, error) {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	all := []ResourceMetadata{}

	if in.Scope != "MANAGED" && in.ManagedType == "" {
		for _, r := range b.ruleGroups.All() {
			if in.Type == "" || r.Type == in.Type {
				all = append(all, ResourceMetadata{Arn: r.ARN, Name: r.Name})
			}
		}
	}

	slices.SortFunc(all, func(a, c ResourceMetadata) int { return strings.Compare(a.Name, c.Name) })

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListRuleGroupsOutput{RuleGroups: pg.Data, NextToken: pg.Next}, nil
}

// UpdateRuleGroup replaces a rule group body, guarded by the update token.
func (b *InMemoryBackend) UpdateRuleGroup(_ context.Context, in *UpdateRuleGroupInput) (*RuleGroupOutput, error) {
	for _, check := range []error{validateDescription(in.Description), validateEncryption(in.EncryptionConfiguration)} {
		if check != nil {
			return nil, check
		}
	}

	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	r, err := b.findRuleGroupLocked(in.RuleGroupArn, in.RuleGroupName, in.Type)
	if err != nil {
		return nil, err
	}

	if err = requireToken(r.UpdateToken, in.UpdateToken); err != nil {
		return nil, err
	}

	body, err := buildRuleGroup(r.Type, in.RuleGroup, in.Rules)
	if err != nil {
		return nil, err
	}

	if used := consumedCapacity(r.Type, &body); used > r.Capacity {
		return nil, invalid("The rule group needs capacity %d but Capacity is %d", used, r.Capacity)
	}

	next := *r
	next.Description = in.Description
	next.RuleGroup = body
	next.Encryption = normalizeEncryption(in.EncryptionConfiguration)
	next.Source = in.SourceMetadata
	next.Summary = in.SummaryConfiguration
	next.UpdateToken = newToken()
	next.LastModified = time.Now()

	if in.DryRun {
		return &RuleGroupOutput{UpdateToken: next.UpdateToken, RuleGroupResponse: b.ruleGroupResponseLocked(&next)}, nil
	}

	*r = next

	return &RuleGroupOutput{UpdateToken: r.UpdateToken, RuleGroupResponse: b.ruleGroupResponseLocked(r)}, nil
}

// DeleteRuleGroup deletes a rule group that no policy references.
func (b *InMemoryBackend) DeleteRuleGroup(_ context.Context, in *DeleteRuleGroupInput) (*DeleteRuleGroupOutput, error) {
	b.mu.Lock("DeleteRuleGroup")
	defer b.mu.Unlock()

	r, err := b.findRuleGroupLocked(in.RuleGroupArn, in.RuleGroupName, in.Type)
	if err != nil {
		return nil, err
	}

	for _, p := range b.policies.All() {
		if policyReferences(&p.Policy, r.ARN) {
			return nil, invalidOp("The rule group %s is in use by firewall policy %s", r.Name, p.Name)
		}
	}

	resp := b.ruleGroupResponseLocked(r)
	resp.RuleGroupStatus = statusDeleting

	b.ruleGroups.Delete(r.ARN)
	b.resourcePolicies.Delete(r.ARN)

	return &DeleteRuleGroupOutput{RuleGroupResponse: resp}, nil
}
