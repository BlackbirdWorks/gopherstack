package securityhub

import "context"

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

	// Members
	CreateMembers(accounts []map[string]any) ([]*Member, []map[string]any)
	DeleteMembers(accountIDs []string) ([]string, []map[string]any)
	GetMembers(accountIDs []string) ([]*Member, []map[string]any)
	InviteMembers(accountIDs []string) []map[string]any
	ListMembers(onlyAssociated bool, nextToken string, maxResults int) ([]*Member, string)
	DisassociateMembers(accountIDs []string) error

	// Invitations / Admin
	AcceptAdministratorInvitation(administratorID, invitationID string) error
	AcceptInvitation(masterID, invitationID string) error
	DeclineInvitations(accountIDs []string) ([]map[string]any, []map[string]any)
	DeleteInvitations(accountIDs []string) ([]map[string]any, []map[string]any)
	GetInvitationsCount() int
	ListInvitations(nextToken string, maxResults int) ([]*Invitation, string)
	GetAdministratorAccount() (*AdminAccount, error)
	GetMasterAccount() (*AdminAccount, error)
	DisassociateFromAdministratorAccount() error
	DisassociateFromMasterAccount() error

	// Organization
	DescribeOrganizationConfiguration() *OrgConfig
	UpdateOrganizationConfiguration(autoEnable bool, autoEnableStandards string, orgConfigType string) error
	EnableOrganizationAdminAccount(accountID string) error
	DisableOrganizationAdminAccount(accountID string) error
	ListOrganizationAdminAccounts(nextToken string, maxResults int) ([]*OrgAdminAccount, string)

	// Finding Aggregator
	CreateFindingAggregator(regionLinkingMode string, regions []string) (*FindingAggregator, error)
	GetFindingAggregator(arn string) (*FindingAggregator, error)
	ListFindingAggregators(nextToken string, maxResults int) ([]*FindingAggregator, string)
	UpdateFindingAggregator(arn, regionLinkingMode string, regions []string) (*FindingAggregator, error)
	DeleteFindingAggregator(arn string) error

	// Configuration Policy
	CreateConfigurationPolicy(
		name, description string,
		policy map[string]any,
		tags map[string]string,
	) (*ConfigurationPolicy, error)
	GetConfigurationPolicy(identifier string) (*ConfigurationPolicy, error)
	UpdateConfigurationPolicy(identifier, name, description string, policy map[string]any) (*ConfigurationPolicy, error)
	DeleteConfigurationPolicy(identifier string) error
	ListConfigurationPolicies(nextToken string, maxResults int) ([]*ConfigurationPolicy, string)
	StartConfigurationPolicyAssociation(
		configPolicyIdentifier, targetID, targetType string,
	) (*ConfigurationPolicyAssociation, error)
	StartConfigurationPolicyDisassociation(configPolicyIdentifier, targetID, targetType string) error
	GetConfigurationPolicyAssociation(targetID, targetType string) (*ConfigurationPolicyAssociation, error)
	ListConfigurationPolicyAssociations(
		filterPolicyID, filterType, nextToken string,
		maxResults int,
	) ([]*ConfigurationPolicyAssociation, string)
	BatchGetConfigurationPolicyAssociations(
		requests []map[string]any,
	) ([]*ConfigurationPolicyAssociation, []map[string]any)

	// Hub V2
	EnableSecurityHubV2(tags map[string]string) error
	DisableSecurityHubV2() error
	DescribeSecurityHubV2() (*HubV2, error)
	EnableSecurityHubFeatureV2(featureName string) error
	DisableSecurityHubFeatureV2(featureName string) error

	// CSPM Connectors (third-party cloud provider connectors, distinct from
	// Connectors V2's ticketing-system connectors)
	CreateConnector(name, description string, provider map[string]any, tags map[string]string) (*CspmConnector, error)
	GetConnector(connectorID string) (*CspmConnector, error)
	UpdateConnector(connectorID, description string, provider map[string]any) (*CspmConnector, error)
	DeleteConnector(connectorID string) (string, error)
	ListConnectors(
		connectorStatus, enablementStatus, providerName, nextToken string,
		maxResults int,
	) ([]*CspmConnector, string)

	// Aggregator V2
	CreateAggregatorV2(regionLinkingMode string, regions []string) (*AggregatorV2, error)
	GetAggregatorV2(arn string) (*AggregatorV2, error)
	ListAggregatorsV2(nextToken string, maxResults int) ([]*AggregatorV2, string)
	UpdateAggregatorV2(arn, regionLinkingMode string, regions []string) (*AggregatorV2, error)
	DeleteAggregatorV2(arn string) error

	// Automation Rules V2
	CreateAutomationRuleV2(
		ruleName, ruleStatus, description string,
		criteria map[string]any,
		actions []map[string]any,
		ruleOrder float64,
		tags map[string]string,
	) (*AutomationRuleV2, error)
	GetAutomationRuleV2(identifier string) (*AutomationRuleV2, error)
	ListAutomationRulesV2(nextToken string, maxResults int) ([]*AutomationRuleV2, string)
	UpdateAutomationRuleV2(identifier string, updates map[string]any) (*AutomationRuleV2, error)
	DeleteAutomationRuleV2(identifier string) error

	// Connectors V2
	CreateConnectorV2(name, description string, provider map[string]any, tags map[string]string) (*ConnectorV2, error)
	GetConnectorV2(connectorID string) (*ConnectorV2, error)
	ListConnectorsV2(nextToken string, maxResults int) ([]*ConnectorV2, string)
	UpdateConnectorV2(connectorID, name, description string, provider map[string]any) (*ConnectorV2, error)
	DeleteConnectorV2(connectorID string) error
	RegisterConnectorV2(authCode, authState string) (*ConnectorV2, error)

	// Tickets V2
	CreateTicketV2(connectorID, findingMetadataUID, mode string) (*TicketV2, error)

	// Findings V2
	GetFindingsV2(
		filters map[string]any,
		sortCriteria []map[string]any,
		nextToken string,
		maxResults int,
	) ([]map[string]any, string)
	BatchUpdateFindingsV2(
		findingIdentifiers []map[string]any,
		metadataUids []string,
		updates map[string]any,
	) ([]map[string]any, []map[string]any)
	GetFindingStatisticsV2(groupByFields []string) []map[string]any
	GetFindingsTrendsV2(startTime, endTime string) []map[string]any

	// Resources V2
	GetResourcesV2(filters map[string]any, nextToken string, maxResults int) ([]map[string]any, string)
	GetResourcesStatisticsV2(groupByFields []string) []map[string]any
	GetResourcesTrendsV2(startTime, endTime string) []map[string]any

	// Products V2
	DescribeProductsV2(nextToken string, maxResults int) ([]*Product, string)

	// Recommended Policy V2
	GenerateRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error)
	GetRecommendedPolicyV2(metadataUID string) (*RecommendedPolicyV2, error)

	// Metadata
	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
