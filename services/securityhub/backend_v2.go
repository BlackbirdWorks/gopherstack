package securityhub

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// HubV2 represents the Security Hub V2 configuration.
type HubV2 struct {
	HubV2Arn  string `json:"HubV2Arn"`
	CreatedAt string `json:"CreatedAt"`
	UpdatedAt string `json:"UpdatedAt"`
}

// AggregatorV2 represents a Security Hub V2 cross-region aggregator.
type AggregatorV2 struct {
	AggregatorV2Arn   string   `json:"AggregatorV2Arn"`
	AggregationRegion string   `json:"AggregationRegion"`
	RegionLinkingMode string   `json:"RegionLinkingMode"`
	CreatedAt         string   `json:"CreatedAt"`
	UpdatedAt         string   `json:"UpdatedAt"`
	Regions           []string `json:"Regions"`
}

// AutomationRuleV2 represents a Security Hub V2 automation rule.
type AutomationRuleV2 struct {
	Identifier  string           `json:"Identifier"`
	RuleArn     string           `json:"RuleArn"`
	RuleName    string           `json:"RuleName"`
	RuleStatus  string           `json:"RuleStatus"`
	Description string           `json:"Description"`
	CreatedAt   string           `json:"CreatedAt"`
	UpdatedAt   string           `json:"UpdatedAt"`
	Criteria    map[string]any   `json:"Criteria"`
	Actions     []map[string]any `json:"Actions"`
	RuleOrder   float64          `json:"RuleOrder"`
	IsTerminal  bool             `json:"IsTerminal"`
}

// ConnectorV2 represents a Security Hub V2 connector.
type ConnectorV2 struct {
	Provider        map[string]any    `json:"Provider"`
	Tags            map[string]string `json:"Tags"`
	ConnectorId     string            `json:"ConnectorId"` //nolint:revive,staticcheck // existing issue.
	ConnectorArn    string            `json:"ConnectorArn"`
	Name            string            `json:"Name"`
	Description     string            `json:"Description"`
	CreatedAt       string            `json:"CreatedAt"`
	UpdatedAt       string            `json:"UpdatedAt"`
	ConnectorStatus string            `json:"ConnectorStatus"`
}

// TicketV2 represents a Security Hub V2 ticket configuration.
type TicketV2 struct {
	TicketConfigurationArn string `json:"TicketConfigurationArn"`
	CreatedAt              string `json:"CreatedAt"`
}

// RecommendedPolicyV2 represents a recommended IAM policy.
type RecommendedPolicyV2 struct {
	MetadataUid    string `json:"MetadataUid"` //nolint:revive,staticcheck // existing issue.
	Policy         string `json:"Policy"`
	GenerationTime string `json:"GenerationTime"`
}

// --- HubV2 ARN helpers ---

func (b *InMemoryBackend) hubV2ARN() string {
	return arn.Build("securityhub", b.region, b.accountID, "hub-v2/default")
}

func (b *InMemoryBackend) aggregatorV2ARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("aggregator-v2/%d", seq))
}

func (b *InMemoryBackend) automationRuleV2ARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("automation-rule-v2/%s", id))
}

func (b *InMemoryBackend) connectorV2ARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("connector-v2/%s", id))
}

func (b *InMemoryBackend) ticketV2ARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("ticket-v2/%d", seq))
}

// --- HubV2 backend methods ---

func (b *InMemoryBackend) EnableSecurityHubV2(tags map[string]string) error {
	b.mu.Lock("EnableSecurityHubV2")
	defer b.mu.Unlock()

	if b.hubV2Enabled {
		return ErrHubAlreadyExists
	}

	now := time.Now().UTC().Format(time.RFC3339)
	b.hubV2Enabled = true
	b.hubV2 = &HubV2{
		HubV2Arn:  b.hubV2ARN(),
		CreatedAt: now,
		UpdatedAt: now,
	}

	if len(tags) > 0 {
		b.tags[b.hubV2.HubV2Arn] = tags
	}

	return nil
}

func (b *InMemoryBackend) DisableSecurityHubV2() error {
	b.mu.Lock("DisableSecurityHubV2")
	defer b.mu.Unlock()

	if !b.hubV2Enabled {
		return ErrHubNotEnabled
	}

	b.hubV2Enabled = false
	b.hubV2 = nil

	return nil
}

func (b *InMemoryBackend) DescribeSecurityHubV2() (*HubV2, error) {
	b.mu.RLock("DescribeSecurityHubV2")
	defer b.mu.RUnlock()

	if !b.hubV2Enabled || b.hubV2 == nil {
		return nil, ErrHubNotEnabled
	}

	cp := *b.hubV2

	return &cp, nil
}

// --- AggregatorV2 backend methods ---

func (b *InMemoryBackend) CreateAggregatorV2(regionLinkingMode string, regions []string) (*AggregatorV2, error) {
	b.mu.Lock("CreateAggregatorV2")
	defer b.mu.Unlock()

	b.aggregatorV2Seq++
	arn := b.aggregatorV2ARN(b.aggregatorV2Seq)
	now := time.Now().UTC().Format(time.RFC3339)

	agg := &AggregatorV2{
		AggregatorV2Arn:   arn,
		AggregationRegion: b.region,
		RegionLinkingMode: regionLinkingMode,
		Regions:           regions,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.aggregatorsV2.Put(agg)

	return agg, nil
}

func (b *InMemoryBackend) GetAggregatorV2(arn string) (*AggregatorV2, error) {
	b.mu.RLock("GetAggregatorV2")
	defer b.mu.RUnlock()

	agg, ok := b.aggregatorsV2.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) ListAggregatorsV2(nextToken string, maxResults int) ([]*AggregatorV2, string) {
	b.mu.RLock("ListAggregatorsV2")
	defer b.mu.RUnlock()

	snap := b.aggregatorsV2.All()
	all := make([]*AggregatorV2, 0, len(snap))

	for _, agg := range snap {
		cp := *agg
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateAggregatorV2(arn, regionLinkingMode string, regions []string) (*AggregatorV2, error) {
	b.mu.Lock("UpdateAggregatorV2")
	defer b.mu.Unlock()

	agg, ok := b.aggregatorsV2.Get(arn)
	if !ok {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	agg.RegionLinkingMode = regionLinkingMode
	agg.Regions = regions
	agg.UpdatedAt = now

	cp := *agg

	return &cp, nil
}

func (b *InMemoryBackend) DeleteAggregatorV2(arn string) error {
	b.mu.Lock("DeleteAggregatorV2")
	defer b.mu.Unlock()

	if !b.aggregatorsV2.Delete(arn) {
		return ErrNotFound
	}

	return nil
}

// --- AutomationRuleV2 backend methods ---

func (b *InMemoryBackend) CreateAutomationRuleV2(
	ruleName, ruleStatus, description string,
	criteria map[string]any,
	actions []map[string]any,
	ruleOrder float64,
	isTerminal bool,
	tags map[string]string,
) (*AutomationRuleV2, error) {
	b.mu.Lock("CreateAutomationRuleV2")
	defer b.mu.Unlock()

	b.automationRuleV2Seq++
	id := fmt.Sprintf("rule-v2-%d", b.automationRuleV2Seq)
	arn := b.automationRuleV2ARN(id)
	now := time.Now().UTC().Format(time.RFC3339)

	rule := &AutomationRuleV2{
		Identifier:  id,
		RuleArn:     arn,
		RuleName:    ruleName,
		RuleStatus:  ruleStatus,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
		Criteria:    criteria,
		Actions:     actions,
		RuleOrder:   ruleOrder,
		IsTerminal:  isTerminal,
	}
	b.automationRulesV2.Put(rule)

	if len(tags) > 0 {
		b.tags[arn] = tags
	}

	return rule, nil
}

func (b *InMemoryBackend) GetAutomationRuleV2(identifier string) (*AutomationRuleV2, error) {
	b.mu.RLock("GetAutomationRuleV2")
	defer b.mu.RUnlock()

	rule, ok := b.automationRulesV2.Get(identifier)
	if !ok {
		for _, r := range b.automationRulesV2.All() {
			if r.RuleArn == identifier {
				cp := *r

				return &cp, nil
			}
		}

		return nil, ErrNotFound
	}

	cp := *rule

	return &cp, nil
}

func (b *InMemoryBackend) ListAutomationRulesV2(nextToken string, maxResults int) ([]*AutomationRuleV2, string) {
	b.mu.RLock("ListAutomationRulesV2")
	defer b.mu.RUnlock()

	snap := b.automationRulesV2.All()
	all := make([]*AutomationRuleV2, 0, len(snap))

	for _, rule := range snap {
		cp := *rule
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateAutomationRuleV2(
	identifier string,
	updates map[string]any,
) (*AutomationRuleV2, error) {
	b.mu.Lock("UpdateAutomationRuleV2")
	defer b.mu.Unlock()

	var target *AutomationRuleV2

	if rule, ok := b.automationRulesV2.Get(identifier); ok {
		target = rule
	} else {
		for _, r := range b.automationRulesV2.All() {
			if r.RuleArn == identifier {
				target = r

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if v, ok := updates["RuleName"].(string); ok {
		target.RuleName = v
	}

	if v, ok := updates["RuleStatus"].(string); ok {
		target.RuleStatus = v
	}

	if v, ok := updates["Description"].(string); ok {
		target.Description = v
	}

	if v, ok := updates["Criteria"].(map[string]any); ok {
		target.Criteria = v
	}

	// Actions decodes from JSON as []any (each element map[string]any), not
	// []map[string]any -- a direct type assertion to []map[string]any always
	// fails and silently drops every Actions update.
	if raw, ok := updates["Actions"].([]any); ok {
		actionMaps := make([]map[string]any, 0, len(raw))

		for _, a := range raw {
			if m, isMap := a.(map[string]any); isMap {
				actionMaps = append(actionMaps, m)
			}
		}

		target.Actions = actionMaps
	}

	if v, ok := updates["RuleOrder"].(float64); ok {
		target.RuleOrder = v
	}

	if v, ok := updates["IsTerminal"].(bool); ok {
		target.IsTerminal = v
	}

	target.UpdatedAt = now
	cp := *target

	return &cp, nil
}

func (b *InMemoryBackend) DeleteAutomationRuleV2(identifier string) error {
	b.mu.Lock("DeleteAutomationRuleV2")
	defer b.mu.Unlock()

	if b.automationRulesV2.Delete(identifier) {
		return nil
	}

	for _, r := range b.automationRulesV2.All() {
		if r.RuleArn == identifier {
			b.automationRulesV2.Delete(r.Identifier)

			return nil
		}
	}

	return ErrNotFound
}

// --- ConnectorV2 backend methods ---

func (b *InMemoryBackend) CreateConnectorV2(
	name, description string,
	provider map[string]any,
	tags map[string]string,
) (*ConnectorV2, error) {
	b.mu.Lock("CreateConnectorV2")
	defer b.mu.Unlock()

	b.connectorV2Seq++
	id := fmt.Sprintf("connector-v2-%d", b.connectorV2Seq)
	arn := b.connectorV2ARN(id)
	now := time.Now().UTC().Format(time.RFC3339)

	c := &ConnectorV2{
		ConnectorId:     id,
		ConnectorArn:    arn,
		Name:            name,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
		ConnectorStatus: "ACTIVE",
		Provider:        provider,
		Tags:            tags,
	}
	b.connectorsV2.Put(c)

	if len(tags) > 0 {
		b.tags[arn] = tags
	}

	return c, nil
}

func (b *InMemoryBackend) GetConnectorV2(connectorID string) (*ConnectorV2, error) {
	b.mu.RLock("GetConnectorV2")
	defer b.mu.RUnlock()

	c, ok := b.connectorsV2.Get(connectorID)
	if !ok {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				cp := *conn

				return &cp, nil
			}
		}

		return nil, ErrNotFound
	}

	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) ListConnectorsV2(nextToken string, maxResults int) ([]*ConnectorV2, string) {
	b.mu.RLock("ListConnectorsV2")
	defer b.mu.RUnlock()

	snap := b.connectorsV2.All()
	all := make([]*ConnectorV2, 0, len(snap))

	for _, c := range snap {
		cp := *c
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateConnectorV2(
	connectorID, name, description string,
	provider map[string]any,
) (*ConnectorV2, error) {
	b.mu.Lock("UpdateConnectorV2")
	defer b.mu.Unlock()

	var target *ConnectorV2

	if c, ok := b.connectorsV2.Get(connectorID); ok {
		target = c
	} else {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				target = conn

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if name != "" {
		target.Name = name
	}

	if description != "" {
		target.Description = description
	}

	if provider != nil {
		target.Provider = provider
	}

	target.UpdatedAt = now
	cp := *target

	return &cp, nil
}

func (b *InMemoryBackend) DeleteConnectorV2(connectorID string) error {
	b.mu.Lock("DeleteConnectorV2")
	defer b.mu.Unlock()

	if b.connectorsV2.Delete(connectorID) {
		return nil
	}

	for _, c := range b.connectorsV2.All() {
		if c.ConnectorArn == connectorID {
			b.connectorsV2.Delete(c.ConnectorId)

			return nil
		}
	}

	return ErrNotFound
}

func (b *InMemoryBackend) RegisterConnectorV2(connectorID string, provider map[string]any) (*ConnectorV2, error) {
	b.mu.Lock("RegisterConnectorV2")
	defer b.mu.Unlock()

	var target *ConnectorV2

	if c, ok := b.connectorsV2.Get(connectorID); ok {
		target = c
	} else {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				target = conn

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	target.ConnectorStatus = "REGISTERED"

	if provider != nil {
		target.Provider = provider
	}

	target.UpdatedAt = now
	cp := *target

	return &cp, nil
}

// --- TicketV2 backend methods ---

func (b *InMemoryBackend) CreateTicketV2(
	ticketConfig map[string]any, //nolint:revive // existing issue.
	tags map[string]string,
) (*TicketV2, error) {
	b.mu.Lock("CreateTicketV2")
	defer b.mu.Unlock()

	b.ticketV2Seq++
	arn := b.ticketV2ARN(b.ticketV2Seq)
	now := time.Now().UTC().Format(time.RFC3339)

	t := &TicketV2{
		TicketConfigurationArn: arn,
		CreatedAt:              now,
	}
	b.ticketsV2.Put(t)

	if len(tags) > 0 {
		b.tags[arn] = tags
	}

	return t, nil
}

// --- Findings V2 backend methods ---

func (b *InMemoryBackend) GetFindingsV2(
	filters map[string]any,
	sortCriteria []map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	// Delegate to existing GetFindings – V2 uses same store
	return b.GetFindings(filters, sortCriteria, nextToken, maxResults)
}

func (b *InMemoryBackend) BatchUpdateFindingsV2(
	findingIdentifiers []map[string]any,
	updates map[string]any,
) ([]map[string]any, []map[string]any) {
	return b.BatchUpdateFindings(findingIdentifiers, updates)
}

func (b *InMemoryBackend) GetFindingStatisticsV2(groupByAttributes []string) []map[string]any {
	b.mu.RLock("GetFindingStatisticsV2")
	defer b.mu.RUnlock()

	type key struct{ attr, val string }
	counts := make(map[key]int)

	for _, finding := range b.findings {
		for _, attr := range groupByAttributes {
			val := ""
			if v, ok := finding[attr]; ok {
				val = fmt.Sprintf("%v", v)
			}

			counts[key{attr, val}]++
		}
	}

	var result []map[string]any

	seen := make(map[string]map[string]any)

	for k, count := range counts {
		if existing, ok := seen[k.attr]; ok {
			existing["Count"] = existing["Count"].(int) + count //nolint:errcheck // existing issue.
		} else {
			entry := map[string]any{
				"GroupByAttribute": k.attr, //nolint:goconst // existing issue.
				"GroupByValue":     k.val,
				"Count":            count, //nolint:goconst // existing issue.
			}
			seen[k.attr] = entry
			result = append(result, entry)
		}
	}

	return result
}

func (b *InMemoryBackend) GetFindingsTrendsV2(
	groupByAttribute string,
	startTime, endTime string,
) []map[string]any {
	return []map[string]any{
		{
			"GroupByAttribute": groupByAttribute,
			"DateRanges": []map[string]any{
				{
					"DateRange": map[string]any{
						"StartDate": startTime,
						"EndDate":   endTime,
					},
					"Count": len(b.findings),
				},
			},
		},
	}
}

// --- Resources V2 backend methods ---

func (b *InMemoryBackend) GetResourcesV2(
	filters map[string]any, //nolint:revive // existing issue.
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetResourcesV2")
	defer b.mu.RUnlock()

	// Derive resource list from findings
	resourceMap := make(map[string]map[string]any)

	for _, finding := range b.findings {
		if resources, ok := finding["Resources"].([]any); ok {
			for _, r := range resources {
				if res, ok := r.(map[string]any); ok { //nolint:govet // existing issue.
					if id, ok := res["Id"].(string); ok && id != "" { //nolint:govet // existing issue.
						resourceMap[id] = res
					}
				}
			}
		}
	}

	var all []map[string]any //nolint:prealloc // existing issue.

	for _, r := range resourceMap {
		cp := make(map[string]any)
		maps.Copy(cp, r)

		all = append(all, cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) GetResourcesStatisticsV2(groupByAttributes []string) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	type key struct{ attr, val string }
	counts := make(map[key]int)

	for _, r := range resources {
		for _, attr := range groupByAttributes {
			val := ""
			if v, ok := r[attr]; ok {
				val = fmt.Sprintf("%v", v)
			}

			counts[key{attr, val}]++
		}
	}

	var result []map[string]any //nolint:prealloc // existing issue.

	for k, count := range counts {
		result = append(result, map[string]any{
			"GroupByAttribute": k.attr,
			"GroupByValue":     k.val,
			"Count":            count,
		})
	}

	return result
}

func (b *InMemoryBackend) GetResourcesTrendsV2(
	groupByAttribute string,
	startTime, endTime string,
) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	return []map[string]any{
		{
			"GroupByAttribute": groupByAttribute,
			"DateRanges": []map[string]any{
				{
					"DateRange": map[string]any{
						"StartDate": startTime,
						"EndDate":   endTime,
					},
					"Count": len(resources),
				},
			},
		},
	}
}

// --- DescribeProductsV2 ---

func (b *InMemoryBackend) DescribeProductsV2(nextToken string, maxResults int) ([]*Product, string) {
	// V2 reuses same product catalog as V1
	return b.DescribeProducts("", nextToken, maxResults)
}

// --- RecommendedPolicyV2 ---

func (b *InMemoryBackend) GenerateRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error) {
	b.mu.Lock("GenerateRecommendedPolicyV2")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	rec := &RecommendedPolicyV2{
		MetadataUid:    metadataUID,
		Policy:         `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"securityhub:*","Resource":"*"}]}`,
		GenerationTime: now,
	}
	b.recommendedPoliciesV2.Put(rec)

	return rec, nil
}

func (b *InMemoryBackend) GetRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error) {
	b.mu.RLock("GetRecommendedPolicyV2")
	defer b.mu.RUnlock()

	rec, ok := b.recommendedPoliciesV2.Get(metadataUID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *rec

	return &cp, nil
}
