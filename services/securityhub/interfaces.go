package securityhub

// StorageBackend is the interface for SecurityHub storage operations.
type StorageBackend interface {
	// Hub management
	EnableHub(enableDefaultStandards bool, tags map[string]string) error
	DisableHub() error
	DescribeHub() (*Hub, error)
	UpdateHubConfiguration(autoEnableControls *bool, autoEnableStandards *string, controlFindingGenerator *string) error

	// Findings
	ImportFindings(findings []map[string]any) (int, int, []map[string]any)
	GetFindings(
		filters map[string]any,
		sortCriteria []map[string]any,
		nextToken string,
		maxResults int,
	) ([]map[string]any, string)
	UpdateFindings(filters map[string]any, note map[string]any, recordState string) error
	BatchUpdateFindings(
		findingIdentifiers []map[string]any,
		updates map[string]any,
	) ([]map[string]any, []map[string]any)
	GetFindingHistory(
		findingIdentifier map[string]any,
		startTime, endTime string,
		nextToken string,
		maxResults int,
	) ([]map[string]any, string)

	// Insights
	CreateInsight(name, groupByAttribute string, filters map[string]any) (string, error)
	GetInsights(insightArns []string, nextToken string, maxResults int) ([]*Insight, string, error)
	UpdateInsight(insightArn, name, groupByAttribute string, filters map[string]any) error
	DeleteInsight(insightArn string) (string, error)
	GetInsightResults(insightArn string) (*InsightResults, error)

	// Standards
	BatchEnableStandards(requests []map[string]any) ([]*StandardsSubscription, []map[string]any)
	BatchDisableStandards(subscriptionArns []string) ([]*StandardsSubscription, []map[string]any)
	GetEnabledStandards(subscriptionArns []string, nextToken string, maxResults int) ([]*StandardsSubscription, string)
	DescribeStandards(nextToken string, maxResults int) ([]*Standard, string)
	DescribeStandardsControls(subscriptionArn, nextToken string, maxResults int) ([]*StandardsControl, string)
	UpdateStandardsControl(controlArn, controlStatus, disabledReason string) error
	ListStandardsControlAssociations(
		securityControlID, nextToken string,
		maxResults int,
	) ([]*StandardsControlAssociation, string)
	BatchGetStandardsControlAssociations(requests []map[string]any) ([]*StandardsControlAssociation, []map[string]any)
	BatchUpdateStandardsControlAssociations(updates []map[string]any) ([]map[string]any, error)

	// Action Targets
	CreateActionTarget(name, description, id string) (string, error)
	DescribeActionTargets(actionTargetArns []string, nextToken string, maxResults int) ([]*ActionTarget, string)
	UpdateActionTarget(actionTargetArn, name, description string) error
	DeleteActionTarget(actionTargetArn string) (string, error)

	// Products
	DescribeProducts(productArn, nextToken string, maxResults int) ([]*Product, string)
	EnableImportFindingsForProduct(productArn string) (string, error)
	DisableImportFindingsForProduct(productSubscriptionArn string) error
	ListEnabledProductsForImport(nextToken string, maxResults int) ([]string, string)

	// Security Controls
	GetSecurityControlDefinition(securityControlID string) (*SecurityControlDefinition, error)
	ListSecurityControlDefinitions(
		standardsArn, nextToken string,
		maxResults int,
	) ([]*SecurityControlDefinition, string)
	BatchGetSecurityControls(securityControlIDs []string) ([]*SecurityControl, []map[string]any)
	UpdateSecurityControl(securityControlID string, parameters map[string]any, lastUpdateReason string) error

	// Automation Rules
	CreateAutomationRule(rule map[string]any) (string, string)
	ListAutomationRules(nextToken string, maxResults int) ([]*AutomationRuleMetadata, string)
	BatchGetAutomationRules(automationRulesArns []string) ([]*AutomationRule, []map[string]any)
	BatchDeleteAutomationRules(automationRulesArns []string) ([]string, []map[string]any)
	BatchUpdateAutomationRules(updates []map[string]any) ([]string, []map[string]any)

	// Tags
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	// Metadata
	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
