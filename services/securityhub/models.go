package securityhub

// Hub represents the SecurityHub configuration.
type Hub struct {
	HubArn                  string `json:"HubArn"`
	SubscribedAt            string `json:"SubscribedAt"`
	AutoEnableStandards     string `json:"AutoEnableStandards"`
	ControlFindingGenerator string `json:"ControlFindingGenerator"`
	AutoEnableControls      bool   `json:"AutoEnableControls"`
}

// Insight represents a SecurityHub insight.
type Insight struct {
	Filters          map[string]any `json:"Filters"`
	InsightArn       string         `json:"InsightArn"`
	Name             string         `json:"Name"`
	GroupByAttribute string         `json:"GroupByAttribute"`
}

// InsightResults holds GetInsightResults response data.
type InsightResults struct {
	InsightArn       string
	GroupByAttribute string
	ResultValues     []map[string]any
}

// StandardsSubscription represents an enabled standard.
type StandardsSubscription struct {
	StandardsInput           map[string]string `json:"StandardsInput"`
	StatusReason             map[string]any    `json:"StatusReason"`
	StandardsSubscriptionArn string            `json:"StandardsSubscriptionArn"`
	StandardsArn             string            `json:"StandardsArn"`
	StandardsStatus          string            `json:"StandardsStatus"`
}

// Standard represents an available standard.
type Standard struct {
	StandardsManagedBy map[string]any
	StandardsArn       string
	Name               string
	Description        string
	EnabledByDefault   bool
}

// StandardsControl represents a control in an enabled standard.
type StandardsControl struct {
	StandardsControlArn    string   `json:"StandardsControlArn"`
	ControlStatus          string   `json:"ControlStatus"`
	DisabledReason         string   `json:"DisabledReason"`
	ControlStatusUpdatedAt string   `json:"ControlStatusUpdatedAt"`
	ControlID              string   `json:"ControlId"`
	Title                  string   `json:"Title"`
	Description            string   `json:"Description"`
	RemediationURL         string   `json:"RemediationUrl"`
	SeverityRating         string   `json:"SeverityRating"`
	RelatedRequirements    []string `json:"RelatedRequirements"`
}

// StandardsControlAssociation represents association between security control and standard.
type StandardsControlAssociation struct {
	SecurityControlID           string
	StandardsArn                string
	AssociationStatus           string
	RelatedRequirements         []string
	UpdatedAt                   string
	UpdatedReason               string
	StandardsControlTitle       string
	StandardsControlDescription string
	StandardsControlArns        []string
}

// ActionTarget represents a custom action target.
type ActionTarget struct {
	ActionTargetArn string `json:"ActionTargetArn"`
	Name            string `json:"Name"`
	Description     string `json:"Description"`
}

// Product represents an integration product.
type Product struct {
	ProductArn                        string
	ProductName                       string
	CompanyName                       string
	Description                       string
	MarketplaceURL                    string
	ActivationURL                     string
	ProductSubscriptionResourcePolicy string
	Categories                        []string
	IntegrationTypes                  []string
}

// SecurityControlDefinition represents a security control definition.
type SecurityControlDefinition struct {
	ParameterDefinitions      map[string]any
	SecurityControlID         string
	Title                     string
	Description               string
	RemediationURL            string
	SeverityRating            string
	CurrentRegionAvailability string
	CustomizableProperties    []string
}

// SecurityControl represents the current state of a security control.
type SecurityControl struct {
	SecurityControlID     string
	SecurityControlArn    string
	Title                 string
	Description           string
	RemediationURL        string
	SeverityRating        string
	SecurityControlStatus string
	UpdateStatus          string
	Parameters            map[string]any
	LastUpdateReason      string
}

// AutomationRuleMetadata is the summary view returned by ListAutomationRules.
type AutomationRuleMetadata struct {
	RuleArn     string
	RuleStatus  string
	RuleName    string
	Description string
	CreatedAt   string
	UpdatedAt   string
	CreatedBy   string
	RuleOrder   int32
	IsTerminal  bool
}

// AutomationRule is the full rule returned by BatchGetAutomationRules.
type AutomationRule struct {
	Criteria    map[string]any   `json:"Criteria"`
	RuleArn     string           `json:"RuleArn"`
	RuleStatus  string           `json:"RuleStatus"`
	RuleName    string           `json:"RuleName"`
	Description string           `json:"Description"`
	CreatedAt   string           `json:"CreatedAt"`
	UpdatedAt   string           `json:"UpdatedAt"`
	CreatedBy   string           `json:"CreatedBy"`
	Actions     []map[string]any `json:"Actions"`
	RuleOrder   int32            `json:"RuleOrder"`
	IsTerminal  bool             `json:"IsTerminal"`
}

// HubV2 represents the Security Hub V2 configuration.
type HubV2 struct {
	Features  map[string]*HubV2Feature `json:"Features"`
	HubV2Arn  string                   `json:"HubV2Arn"`
	CreatedAt string                   `json:"CreatedAt"`
	UpdatedAt string                   `json:"UpdatedAt"`
}

// HubV2Feature represents one opt-in Security Hub V2 feature's enablement
// status (see EnableSecurityHubFeatureV2/DisableSecurityHubFeatureV2),
// mirroring the real SDK's types.FeatureDetail{FeatureStatus, UpdatedAt}.
type HubV2Feature struct {
	FeatureStatus string `json:"FeatureStatus"`
	UpdatedAt     string `json:"UpdatedAt"`
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

// AutomationRuleV2 represents a Security Hub V2 automation rule. Identifier
// is the store key (also the URL path segment); on the wire it is "RuleId"
// (securityhub@v1.75.4 types.go:905-935), not "Identifier" -- see
// automationRuleV2ToResponse. Unlike the V1 AutomationRule, the real V2
// shape has no IsTerminal member at all.
type AutomationRuleV2 struct {
	Identifier  string           `json:"RuleId"`
	RuleArn     string           `json:"RuleArn"`
	RuleName    string           `json:"RuleName"`
	RuleStatus  string           `json:"RuleStatus"`
	Description string           `json:"Description"`
	CreatedAt   string           `json:"CreatedAt"`
	UpdatedAt   string           `json:"UpdatedAt"`
	Criteria    map[string]any   `json:"Criteria"`
	Actions     []map[string]any `json:"Actions"`
	RuleOrder   float64          `json:"RuleOrder"`
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

// CspmConnector represents a Security Hub CSPM connector to a third-party
// cloud provider (currently Azure only, per the real API's
// CspmProviderConfiguration/CspmProviderDetail unions -- see connectors.go).
// Provider holds the raw tagged-union wire value (e.g.
// {"Azure": {AWSConfigConnectorArn, AzureRegions, ScopeConfiguration}})
// exactly as sent/returned on the wire, so Get/List responses can echo it
// back without re-deriving the union shape.
type CspmConnector struct {
	Provider         map[string]any    `json:"Provider"`
	Tags             map[string]string `json:"Tags"`
	ConnectorId      string            `json:"ConnectorId"` //nolint:revive,staticcheck // existing issue.
	ConnectorArn     string            `json:"ConnectorArn"`
	Name             string            `json:"Name"`
	Description      string            `json:"Description"`
	CreatedAt        string            `json:"CreatedAt"`
	LastUpdatedAt    string            `json:"LastUpdatedAt"`
	CreatedBy        string            `json:"CreatedBy"`
	EnablementStatus string            `json:"EnablementStatus"`
	// ConnectorStatus/HealthMessage/HealthCheckedAt make up the connector's
	// health check (types.CspmHealthCheck) -- kept as flat fields here rather
	// than a nested struct since nothing else needs to address them as a unit.
	ConnectorStatus string `json:"ConnectorStatus"`
	HealthMessage   string `json:"HealthMessage"`
	HealthCheckedAt string `json:"HealthCheckedAt"`
	ProviderName    string `json:"ProviderName"`
}

// TicketV2 represents a Security Hub V2 ticket linking a third-party ITSM
// ticket to a finding. TicketSrcUrl is left permanently empty: the real
// field carries a URL into the caller's ITSM system, which this backend
// never integrates with, so there is no real state to populate it from.
type TicketV2 struct {
	//nolint:revive,staticcheck // matches the AWS wire field name, like ConnectorId below.
	TicketId string `json:"TicketId"`
	//nolint:revive,staticcheck // matches the AWS wire field name.
	TicketSrcUrl string `json:"TicketSrcUrl"`
	//nolint:revive,staticcheck // existing pattern in this file.
	ConnectorId string `json:"ConnectorId"`
	//nolint:revive,staticcheck // matches the AWS wire field name.
	FindingMetadataUid string `json:"FindingMetadataUid"`
	Mode               string `json:"Mode"`
	CreatedAt          string `json:"CreatedAt"`
}

// RecommendedPolicyV2 represents a recommended IAM policy.
type RecommendedPolicyV2 struct {
	MetadataUid    string `json:"MetadataUid"` //nolint:revive,staticcheck // existing issue.
	Policy         string `json:"Policy"`
	GenerationTime string `json:"GenerationTime"`
}

// Member represents a Security Hub member account.
type Member struct {
	AccountId       string `json:"AccountId"`       //nolint:revive,staticcheck // existing issue.
	AdministratorId string `json:"AdministratorId"` //nolint:revive,staticcheck // existing issue.
	MasterId        string `json:"MasterId"`        // deprecated alias //nolint:revive,staticcheck // existing issue.
	Email           string `json:"Email"`
	MemberStatus    string `json:"MemberStatus"`
	InvitedAt       string `json:"InvitedAt"`
	UpdatedAt       string `json:"UpdatedAt"`
}

// Invitation represents a pending invitation.
type Invitation struct {
	AccountId    string `json:"AccountId"`    //nolint:revive,staticcheck // existing issue.
	InvitationId string `json:"InvitationId"` //nolint:revive,staticcheck // existing issue.
	InvitedAt    string `json:"InvitedAt"`
	MemberStatus string `json:"MemberStatus"`
}

// AdminAccount represents the administrator account relationship. It wires
// the real securityhub@v1.75.4 types.Invitation shape (GetAdministratorAccountOutput.
// Administrator / GetMasterAccountOutput.Master are both *types.Invitation),
// whose status member is "MemberStatus" -- the same real field the sibling
// Invitation model (used by ListInvitations) already names correctly.
type AdminAccount struct {
	AccountId    string `json:"AccountId"`    //nolint:revive,staticcheck // existing issue.
	InvitationId string `json:"InvitationId"` //nolint:revive,staticcheck // existing issue.
	InvitedAt    string `json:"InvitedAt"`
	MemberStatus string `json:"MemberStatus"`
}

// OrgConfig represents the organization configuration.
type OrgConfig struct {
	AutoEnableStandards           string `json:"AutoEnableStandards"`
	OrganizationConfigurationType string `json:"OrganizationConfigurationType"`
	AutoEnable                    bool   `json:"AutoEnable"`
	MemberAccountLimitReached     bool   `json:"MemberAccountLimitReached"`
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	Status    string `json:"Status"`
}

// ConfigurationPolicy represents a Security Hub central configuration policy.
type ConfigurationPolicy struct {
	ConfigurationPolicy map[string]any    `json:"ConfigurationPolicy"`
	Tags                map[string]string `json:"Tags"`
	Arn                 string            `json:"Arn"`
	Id                  string            `json:"Id"` //nolint:revive,staticcheck // existing issue.
	Name                string            `json:"Name"`
	Description         string            `json:"Description"`
	CreatedAt           string            `json:"CreatedAt"`
	UpdatedAt           string            `json:"UpdatedAt"`
}

// ConfigurationPolicyAssociation represents an association between a policy and a target.
type ConfigurationPolicyAssociation struct {
	ConfigurationPolicyId    string `json:"ConfigurationPolicyId"` //nolint:revive,staticcheck // existing issue.
	TargetId                 string `json:"TargetId"`              //nolint:revive,staticcheck // existing issue.
	TargetType               string `json:"TargetType"`
	AssociationType          string `json:"AssociationType"`
	UpdatedAt                string `json:"UpdatedAt"`
	AssociationStatus        string `json:"AssociationStatus"`
	AssociationStatusMessage string `json:"AssociationStatusMessage"`
}

// FindingAggregator represents a Security Hub finding aggregator for cross-region aggregation.
type FindingAggregator struct {
	FindingAggregatorArn     string   `json:"FindingAggregatorArn"`
	FindingAggregationRegion string   `json:"FindingAggregationRegion"`
	RegionLinkingMode        string   `json:"RegionLinkingMode"`
	Regions                  []string `json:"Regions"`
}
