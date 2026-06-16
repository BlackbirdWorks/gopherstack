package securityhub

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	keyErrorCode    = "ErrorCode"
	keyErrorMessage = "ErrorMessage"

	errCodeInvalidInput = "InvalidInput"

	keyStandardsArn      = "StandardsArn"
	keySecurityControlID = "SecurityControlId"
	keyRuleArn           = "RuleArn"

	statusEnabled       = "ENABLED"
	statusAvailable     = "AVAILABLE"
	companyAWS          = "AWS"
	intTypeSendFindings = "SEND_FINDINGS_TO_SECURITY_HUB"

	msgRuleNotFound = "Rule not found"

	maxStandardsResults = 25
	maxDefaultResults   = 100
)

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

var (
	ErrHubNotEnabled    = errors.New("SecurityHub is not enabled")
	ErrHubAlreadyExists = errors.New("SecurityHub is already enabled")
	ErrNotFound         = errors.New("not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrAlreadyExists    = errors.New("resource already exists")
)

// knownStandards defines the built-in standards available in SecurityHub.
var knownStandards = []Standard{ //nolint:gochecknoglobals // read-only lookup data
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
		Name:         "AWS Foundational Security Best Practices v1.0.0",
		Description: "The AWS Foundational Security Best Practices standard is a set of controls that detect " +
			"when your deployed accounts and resources deviate from security best practices.",
		EnabledByDefault: true,
	},
	{
		StandardsArn: "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0",
		Name:         "CIS AWS Foundations Benchmark v1.2.0",
		Description: "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.2.0 is a set of " +
			"security configuration best practices for AWS.",
		EnabledByDefault: true,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/cis-aws-foundations-benchmark/v/1.4.0",
		Name:         "CIS AWS Foundations Benchmark v1.4.0",
		Description: "The Center for Internet Security (CIS) AWS Foundations Benchmark v1.4.0 is a set of " +
			"security configuration best practices for AWS.",
		EnabledByDefault: false,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/pci-dss/v/3.2.1",
		Name:         "PCI DSS v3.2.1",
		Description: "The Payment Card Industry Data Security Standard (PCI DSS) is a proprietary " +
			"information security standard.",
		EnabledByDefault: false,
	},
	{
		StandardsArn: "arn:aws:securityhub:us-east-1::standards/nist-800-53/v/5.0.0",
		Name:         "NIST Special Publication 800-53 Revision 5",
		Description: "The NIST Special Publication 800-53 Revision 5 standard provides a catalog of " +
			"security and privacy controls.",
		EnabledByDefault: false,
	},
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	configPolicyAssocs     map[string]*ConfigurationPolicyAssociation
	orgConfig              *OrgConfig
	tags                   map[string]map[string]string
	automationRules        map[string]*AutomationRule
	hub                    *Hub
	findings               map[string]map[string]any
	insights               map[string]*Insight
	controlParams          map[string]map[string]any
	productSubscriptions   map[string]string
	mu                     *lockmetrics.RWMutex
	actionTargets          map[string]*ActionTarget
	members                map[string]*Member
	invitations            map[string]*Invitation
	adminAccount           *AdminAccount
	configPolicies         map[string]*ConfigurationPolicy
	orgAdminAccounts       map[string]string
	recommendedPoliciesV2  map[string]*RecommendedPolicyV2
	findingAggregators     map[string]*FindingAggregator
	controlOverrides       map[string]*StandardsControl
	ticketsV2              map[string]*TicketV2
	connectorsV2           map[string]*ConnectorV2
	standardsSubscriptions map[string]*StandardsSubscription
	hubV2                  *HubV2
	aggregatorsV2          map[string]*AggregatorV2
	automationRulesV2      map[string]*AutomationRuleV2
	region                 string
	accountID              string
	aggregatorV2Seq        int
	connectorV2Seq         int
	automationRuleV2Seq    int
	configPolicySeq        int
	memberSeq              int
	findingAggregatorSeq   int
	ticketV2Seq            int
	standardsSeq           int
	actionTargetSeq        int
	insightSeq             int
	automationRuleSeq      int
	hubEnabled             bool
	hubV2Enabled           bool
}

// NewInMemoryBackend creates a new in-memory backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                     lockmetrics.New("securityhub"),
		accountID:              accountID,
		region:                 region,
		findings:               make(map[string]map[string]any),
		insights:               make(map[string]*Insight),
		standardsSubscriptions: make(map[string]*StandardsSubscription),
		controlOverrides:       make(map[string]*StandardsControl),
		actionTargets:          make(map[string]*ActionTarget),
		productSubscriptions:   make(map[string]string),
		controlParams:          make(map[string]map[string]any),
		automationRules:        make(map[string]*AutomationRule),
		tags:                   make(map[string]map[string]string),
		// Members / Invitations / Admin
		members:          make(map[string]*Member),
		invitations:      make(map[string]*Invitation),
		orgAdminAccounts: make(map[string]string),
		// Finding Aggregator
		findingAggregators: make(map[string]*FindingAggregator),
		// Configuration Policy
		configPolicies:     make(map[string]*ConfigurationPolicy),
		configPolicyAssocs: make(map[string]*ConfigurationPolicyAssociation),
		// V2 resources
		aggregatorsV2:         make(map[string]*AggregatorV2),
		automationRulesV2:     make(map[string]*AutomationRuleV2),
		connectorsV2:          make(map[string]*ConnectorV2),
		ticketsV2:             make(map[string]*TicketV2),
		recommendedPoliciesV2: make(map[string]*RecommendedPolicyV2),
	}
}

func (b *InMemoryBackend) AccountID() string { return b.accountID }
func (b *InMemoryBackend) Region() string    { return b.region }

func (b *InMemoryBackend) hubARN() string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:hub/default", b.region, b.accountID)
}

func (b *InMemoryBackend) subscriptionARN(seq int) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:subscription/%d", b.region, b.accountID, seq)
}

// unused: keep compiler happy
var _ = (*InMemoryBackend).subscriptionARN

func (b *InMemoryBackend) insightARN(seq int) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:insight/%s/%d", b.region, b.accountID, b.accountID, seq)
}

func (b *InMemoryBackend) actionTargetARN(id string) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:action/custom/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) automationRuleARN(seq int) string {
	return fmt.Sprintf("arn:aws:securityhub:%s:%s:automation-rule/%d", b.region, b.accountID, seq)
}

func (b *InMemoryBackend) securityControlARN(id string) string {
	return fmt.Sprintf("arn:aws:securityhub:%s::security-control/%s", b.region, id)
}

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.hubEnabled = false
	b.hub = nil
	b.findings = make(map[string]map[string]any)
	b.insights = make(map[string]*Insight)
	b.insightSeq = 0
	b.standardsSubscriptions = make(map[string]*StandardsSubscription)
	b.standardsSeq = 0
	b.controlOverrides = make(map[string]*StandardsControl)
	b.actionTargets = make(map[string]*ActionTarget)
	b.actionTargetSeq = 0
	b.productSubscriptions = make(map[string]string)
	b.controlParams = make(map[string]map[string]any)
	b.automationRules = make(map[string]*AutomationRule)
	b.automationRuleSeq = 0
	b.tags = make(map[string]map[string]string)
	// Members / Invitations / Admin
	b.members = make(map[string]*Member)
	b.invitations = make(map[string]*Invitation)
	b.adminAccount = nil
	b.orgConfig = nil
	b.orgAdminAccounts = make(map[string]string)
	b.memberSeq = 0
	// Finding Aggregator
	b.findingAggregators = make(map[string]*FindingAggregator)
	b.findingAggregatorSeq = 0
	// Configuration Policy
	b.configPolicies = make(map[string]*ConfigurationPolicy)
	b.configPolicyAssocs = make(map[string]*ConfigurationPolicyAssociation)
	b.configPolicySeq = 0
	// V2
	b.hubV2Enabled = false
	b.hubV2 = nil
	b.aggregatorsV2 = make(map[string]*AggregatorV2)
	b.aggregatorV2Seq = 0
	b.automationRulesV2 = make(map[string]*AutomationRuleV2)
	b.automationRuleV2Seq = 0
	b.connectorsV2 = make(map[string]*ConnectorV2)
	b.connectorV2Seq = 0
	b.ticketsV2 = make(map[string]*TicketV2)
	b.ticketV2Seq = 0
	b.recommendedPoliciesV2 = make(map[string]*RecommendedPolicyV2)
}

// persistence structs for Snapshot/Restore.
type snapshot struct {
	ProductSubscriptions   map[string]string                 `json:"productSubscriptions"`
	Hub                    *Hub                              `json:"hub"`
	Findings               map[string]map[string]any         `json:"findings"`
	Insights               map[string]*Insight               `json:"insights"`
	Tags                   map[string]map[string]string      `json:"tags"`
	StandardsSubscriptions map[string]*StandardsSubscription `json:"standardsSubscriptions"`
	AutomationRules        map[string]*AutomationRule        `json:"automationRules"`
	ControlOverrides       map[string]*StandardsControl      `json:"controlOverrides"`
	ActionTargets          map[string]*ActionTarget          `json:"actionTargets"`
	ControlParams          map[string]map[string]any         `json:"controlParams"`
	// Members / Invitations / Admin
	Members          map[string]*Member     `json:"members"`
	Invitations      map[string]*Invitation `json:"invitations"`
	AdminAccount     *AdminAccount          `json:"adminAccount"`
	OrgConfig        *OrgConfig             `json:"orgConfig"`
	OrgAdminAccounts map[string]string      `json:"orgAdminAccounts"`
	// Finding Aggregator
	FindingAggregators map[string]*FindingAggregator `json:"findingAggregators"`
	// Configuration Policy
	ConfigPolicies     map[string]*ConfigurationPolicy            `json:"configPolicies"`
	ConfigPolicyAssocs map[string]*ConfigurationPolicyAssociation `json:"configPolicyAssocs"`
	// V2
	HubV2                 *HubV2                          `json:"hubV2"`
	AggregatorsV2         map[string]*AggregatorV2        `json:"aggregatorsV2"`
	AutomationRulesV2     map[string]*AutomationRuleV2    `json:"automationRulesV2"`
	ConnectorsV2          map[string]*ConnectorV2         `json:"connectorsV2"`
	TicketsV2             map[string]*TicketV2            `json:"ticketsV2"`
	RecommendedPoliciesV2 map[string]*RecommendedPolicyV2 `json:"recommendedPoliciesV2"`
	StandardsSeq          int                             `json:"standardsSeq"`
	ActionTargetSeq       int                             `json:"actionTargetSeq"`
	AutomationRuleSeq     int                             `json:"automationRuleSeq"`
	InsightSeq            int                             `json:"insightSeq"`
	MemberSeq             int                             `json:"memberSeq"`
	FindingAggregatorSeq  int                             `json:"findingAggregatorSeq"`
	ConfigPolicySeq       int                             `json:"configPolicySeq"`
	AggregatorV2Seq       int                             `json:"aggregatorV2Seq"`
	AutomationRuleV2Seq   int                             `json:"automationRuleV2Seq"`
	ConnectorV2Seq        int                             `json:"connectorV2Seq"`
	TicketV2Seq           int                             `json:"ticketV2Seq"`
	HubEnabled            bool                            `json:"hubEnabled"`
	HubV2Enabled          bool                            `json:"hubV2Enabled"`
}

func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := snapshot{
		HubEnabled:             b.hubEnabled,
		Hub:                    b.hub,
		Findings:               b.findings,
		Insights:               b.insights,
		InsightSeq:             b.insightSeq,
		StandardsSubscriptions: b.standardsSubscriptions,
		StandardsSeq:           b.standardsSeq,
		ControlOverrides:       b.controlOverrides,
		ActionTargets:          b.actionTargets,
		ActionTargetSeq:        b.actionTargetSeq,
		ProductSubscriptions:   b.productSubscriptions,
		ControlParams:          b.controlParams,
		AutomationRules:        b.automationRules,
		AutomationRuleSeq:      b.automationRuleSeq,
		Tags:                   b.tags,
		// Members / Invitations / Admin
		Members:          b.members,
		Invitations:      b.invitations,
		AdminAccount:     b.adminAccount,
		OrgConfig:        b.orgConfig,
		OrgAdminAccounts: b.orgAdminAccounts,
		MemberSeq:        b.memberSeq,
		// Finding Aggregator
		FindingAggregators:   b.findingAggregators,
		FindingAggregatorSeq: b.findingAggregatorSeq,
		// Configuration Policy
		ConfigPolicies:     b.configPolicies,
		ConfigPolicyAssocs: b.configPolicyAssocs,
		ConfigPolicySeq:    b.configPolicySeq,
		// V2
		HubV2Enabled:          b.hubV2Enabled,
		HubV2:                 b.hubV2,
		AggregatorsV2:         b.aggregatorsV2,
		AggregatorV2Seq:       b.aggregatorV2Seq,
		AutomationRulesV2:     b.automationRulesV2,
		AutomationRuleV2Seq:   b.automationRuleV2Seq,
		ConnectorsV2:          b.connectorsV2,
		ConnectorV2Seq:        b.connectorV2Seq,
		TicketsV2:             b.ticketsV2,
		TicketV2Seq:           b.ticketV2Seq,
		RecommendedPoliciesV2: b.recommendedPoliciesV2,
	}

	data, _ := json.Marshal(snap)

	return data
}

func (b *InMemoryBackend) Restore(data []byte) error { //nolint:funlen // existing issue.
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.hubEnabled = snap.HubEnabled
	b.hub = snap.Hub
	b.findings = snap.Findings
	b.insights = snap.Insights
	b.insightSeq = snap.InsightSeq
	b.standardsSubscriptions = snap.StandardsSubscriptions
	b.standardsSeq = snap.StandardsSeq
	b.controlOverrides = snap.ControlOverrides
	b.actionTargets = snap.ActionTargets
	b.actionTargetSeq = snap.ActionTargetSeq
	b.productSubscriptions = snap.ProductSubscriptions
	b.controlParams = snap.ControlParams
	b.automationRules = snap.AutomationRules
	b.automationRuleSeq = snap.AutomationRuleSeq
	b.tags = snap.Tags
	// Members / Invitations / Admin
	if snap.Members != nil {
		b.members = snap.Members
	}

	if snap.Invitations != nil {
		b.invitations = snap.Invitations
	}

	b.adminAccount = snap.AdminAccount
	b.orgConfig = snap.OrgConfig

	if snap.OrgAdminAccounts != nil {
		b.orgAdminAccounts = snap.OrgAdminAccounts
	}

	b.memberSeq = snap.MemberSeq
	// Finding Aggregator
	if snap.FindingAggregators != nil {
		b.findingAggregators = snap.FindingAggregators
	}

	b.findingAggregatorSeq = snap.FindingAggregatorSeq
	// Configuration Policy
	if snap.ConfigPolicies != nil {
		b.configPolicies = snap.ConfigPolicies
	}

	if snap.ConfigPolicyAssocs != nil {
		b.configPolicyAssocs = snap.ConfigPolicyAssocs
	}

	b.configPolicySeq = snap.ConfigPolicySeq
	// V2
	b.hubV2Enabled = snap.HubV2Enabled
	b.hubV2 = snap.HubV2

	if snap.AggregatorsV2 != nil {
		b.aggregatorsV2 = snap.AggregatorsV2
	}

	b.aggregatorV2Seq = snap.AggregatorV2Seq

	if snap.AutomationRulesV2 != nil {
		b.automationRulesV2 = snap.AutomationRulesV2
	}

	b.automationRuleV2Seq = snap.AutomationRuleV2Seq

	if snap.ConnectorsV2 != nil {
		b.connectorsV2 = snap.ConnectorsV2
	}

	b.connectorV2Seq = snap.ConnectorV2Seq

	if snap.TicketsV2 != nil {
		b.ticketsV2 = snap.TicketsV2
	}

	b.ticketV2Seq = snap.TicketV2Seq

	if snap.RecommendedPoliciesV2 != nil {
		b.recommendedPoliciesV2 = snap.RecommendedPoliciesV2
	}

	return nil
}

// --- Hub Management ---

func (b *InMemoryBackend) EnableHub(enableDefaultStandards bool, tags map[string]string) error {
	b.mu.Lock("EnableHub")
	defer b.mu.Unlock()

	if b.hubEnabled {
		return ErrHubAlreadyExists
	}

	b.hubEnabled = true
	now := time.Now().UTC().Format(time.RFC3339)
	b.hub = &Hub{
		HubArn:                  b.hubARN(),
		SubscribedAt:            now,
		AutoEnableControls:      true,
		AutoEnableStandards:     "DEFAULT",
		ControlFindingGenerator: "SECURITY_CONTROL",
	}

	if len(tags) > 0 {
		b.tags[b.hub.HubArn] = tags
	}

	if enableDefaultStandards {
		for i, std := range knownStandards {
			if std.EnabledByDefault {
				b.standardsSeq++
				subArn := fmt.Sprintf(
					"arn:aws:securityhub:%s:%s:subscription/%s/v/1.0.0",
					b.region,
					b.accountID,
					fmt.Sprintf("default-%d", i),
				)
				b.standardsSubscriptions[subArn] = &StandardsSubscription{
					StandardsSubscriptionArn: subArn,
					StandardsArn:             std.StandardsArn,
					StandardsInput:           map[string]string{},
					StandardsStatus:          "READY",
				}
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) DisableHub() error {
	b.mu.Lock("DisableHub")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	b.hubEnabled = false
	b.hub = nil

	return nil
}

func (b *InMemoryBackend) DescribeHub() (*Hub, error) {
	b.mu.RLock("DescribeHub")
	defer b.mu.RUnlock()

	if !b.hubEnabled || b.hub == nil {
		return nil, ErrHubNotEnabled
	}

	cp := *b.hub

	return &cp, nil
}

func (b *InMemoryBackend) UpdateHubConfiguration(
	autoEnableControls *bool,
	autoEnableStandards *string,
	controlFindingGenerator *string,
) error {
	b.mu.Lock("UpdateHubConfiguration")
	defer b.mu.Unlock()

	if !b.hubEnabled || b.hub == nil {
		return ErrHubNotEnabled
	}

	if autoEnableControls != nil {
		b.hub.AutoEnableControls = *autoEnableControls
	}

	if autoEnableStandards != nil {
		b.hub.AutoEnableStandards = *autoEnableStandards
	}

	if controlFindingGenerator != nil {
		b.hub.ControlFindingGenerator = *controlFindingGenerator
	}

	return nil
}

// --- Findings ---

func findingKey(productArn, id string) string {
	return productArn + "|" + id
}

// validateASFFRequiredFields checks that a finding has the mandatory ASFF fields.
// AWS rejects findings missing any of these fields in BatchImportFindings.
// Returns a non-empty error message if validation fails.
func validateASFFRequiredFields(f map[string]any, productArn, id string) string {
	if productArn == "" {
		return "ProductArn is required"
	}

	if id == "" {
		return "Id is required"
	}

	if v, _ := f["AwsAccountId"].(string); v == "" {
		return "AwsAccountId is required"
	}

	if v, _ := f["GeneratorId"].(string); v == "" {
		return "GeneratorId is required"
	}

	if v, _ := f["Title"].(string); v == "" {
		return "Title is required"
	}

	if v, _ := f["Description"].(string); v == "" {
		return "Description is required"
	}

	// At least one of CreatedAt/UpdatedAt/FirstObservedAt/LastObservedAt must be present.
	hasTimestamp := false
	for _, k := range []string{keyCreatedAt, keyUpdatedAt, "FirstObservedAt", "LastObservedAt"} {
		if v, _ := f[k].(string); v != "" {
			hasTimestamp = true

			break
		}
	}

	if !hasTimestamp {
		return "At least one of CreatedAt, UpdatedAt, FirstObservedAt, or LastObservedAt is required"
	}

	// Resources must be a non-empty array.
	resources, _ := f["Resources"].([]any)
	if len(resources) == 0 {
		return "Resources must contain at least one entry"
	}

	return ""
}

func (b *InMemoryBackend) ImportFindings(findings []map[string]any) (int, int, []map[string]any) {
	b.mu.Lock("ImportFindings")
	defer b.mu.Unlock()

	successCount := 0
	failedCount := 0
	var failedFindings []map[string]any

	for _, f := range findings {
		productArn, _ := f["ProductArn"].(string)
		id, _ := f["Id"].(string)

		if msg := validateASFFRequiredFields(f, productArn, id); msg != "" {
			failedCount++
			failedFindings = append(failedFindings, map[string]any{
				"Id":            id,
				"ProductArn":    productArn, //nolint:goconst // existing issue.
				keyErrorCode:    "InvalidInputException",
				keyErrorMessage: msg,
			})

			continue
		}

		key := findingKey(productArn, id)

		// Copy the finding
		stored := make(map[string]any, len(f))
		maps.Copy(stored, f)

		b.findings[key] = stored
		successCount++
	}

	return successCount, failedCount, failedFindings
}

func (b *InMemoryBackend) GetFindings(
	filters map[string]any,
	_ []map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	var results []map[string]any

	for _, f := range b.findings {
		if matchesFindingFilters(f, filters) {
			results = append(results, f)
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []map[string]any{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

// matchesFindingFilters is a simplified filter check.
// AWS SecurityHub finding filters are complex; we support a basic subset.
func matchesFindingFilters(finding, filters map[string]any) bool {
	if len(filters) == 0 {
		return true
	}

	fID, _ := finding["Id"].(string)
	if !matchesStringFilter(fID, filters["Id"]) {
		return false
	}

	fArn, _ := finding["ProductArn"].(string)

	return matchesStringFilter(fArn, filters["ProductArn"])
}

// matchesStringFilter checks a single string field value against a SecurityHub filter value.
func matchesStringFilter(fieldVal string, filterVal any) bool {
	items, ok := filterVal.([]any)
	if !ok {
		return true
	}

	for _, item := range items {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		val, _ := m["Value"].(string)
		comp, _ := m["Comparison"].(string)

		if comp == "NOT_EQUALS" {
			if fieldVal == val {
				return false
			}
		} else if fieldVal != val {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) UpdateFindings(filters map[string]any, note map[string]any, recordState string) error {
	b.mu.Lock("UpdateFindings")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	for key, f := range b.findings {
		if matchesFindingFilters(f, filters) {
			if note != nil {
				f["Note"] = note
			}

			if recordState != "" {
				f["RecordState"] = recordState
			}

			b.findings[key] = f
		}
	}

	return nil
}

func (b *InMemoryBackend) BatchUpdateFindings(
	findingIdentifiers []map[string]any,
	updates map[string]any,
) ([]map[string]any, []map[string]any) {
	b.mu.Lock("BatchUpdateFindings")
	defer b.mu.Unlock()

	var processedFindings []map[string]any
	var unprocessedFindings []map[string]any

	for _, ident := range findingIdentifiers {
		productArn, _ := ident["ProductArn"].(string)
		id, _ := ident["Id"].(string)
		key := findingKey(productArn, id)

		f, exists := b.findings[key]
		if !exists {
			unprocessedFindings = append(unprocessedFindings, map[string]any{
				"FindingIdentifier": ident,
				keyErrorCode:        errCodeInvalidInput,
				keyErrorMessage:     "Finding not found",
			})

			continue
		}

		// Apply updates
		maps.Copy(f, updates)

		b.findings[key] = f
		processedFindings = append(processedFindings, ident)
	}

	if processedFindings == nil {
		processedFindings = []map[string]any{}
	}

	if unprocessedFindings == nil {
		unprocessedFindings = []map[string]any{}
	}

	return processedFindings, unprocessedFindings
}

func (b *InMemoryBackend) GetFindingHistory(
	_ map[string]any,
	_, _ string,
	_ string,
	_ int,
) ([]map[string]any, string) {
	b.mu.RLock("GetFindingHistory")
	defer b.mu.RUnlock()

	// Return empty history since we don't track history
	return []map[string]any{}, ""
}

// --- Insights ---

func (b *InMemoryBackend) CreateInsight(name, groupByAttribute string, filters map[string]any) (string, error) {
	b.mu.Lock("CreateInsight")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	b.insightSeq++
	arn := b.insightARN(b.insightSeq)

	b.insights[arn] = &Insight{
		InsightArn:       arn,
		Name:             name,
		GroupByAttribute: groupByAttribute,
		Filters:          filters,
	}

	return arn, nil
}

func (b *InMemoryBackend) GetInsights(
	insightArns []string,
	nextToken string,
	maxResults int,
) ([]*Insight, string, error) {
	b.mu.RLock("GetInsights")
	defer b.mu.RUnlock()

	if !b.hubEnabled {
		return nil, "", ErrHubNotEnabled
	}

	var results []*Insight

	if len(insightArns) > 0 {
		for _, arn := range insightArns {
			if insight, ok := b.insights[arn]; ok {
				results = append(results, insight)
			}
		}
	} else {
		for _, insight := range b.insights {
			results = append(results, insight)
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*Insight{}, "", nil
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut, nil
}

func (b *InMemoryBackend) UpdateInsight(insightArn, name, groupByAttribute string, filters map[string]any) error {
	b.mu.Lock("UpdateInsight")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	insight, ok := b.insights[insightArn]
	if !ok {
		return fmt.Errorf("%w: insight %s", ErrNotFound, insightArn)
	}

	if name != "" {
		insight.Name = name
	}

	if groupByAttribute != "" {
		insight.GroupByAttribute = groupByAttribute
	}

	if filters != nil {
		insight.Filters = filters
	}

	return nil
}

func (b *InMemoryBackend) DeleteInsight(insightArn string) (string, error) {
	b.mu.Lock("DeleteInsight")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	if _, ok := b.insights[insightArn]; !ok {
		return "", fmt.Errorf("%w: insight %s", ErrNotFound, insightArn)
	}

	delete(b.insights, insightArn)

	return insightArn, nil
}

func (b *InMemoryBackend) GetInsightResults(insightArn string) (*InsightResults, error) {
	b.mu.RLock("GetInsightResults")
	defer b.mu.RUnlock()

	if !b.hubEnabled {
		return nil, ErrHubNotEnabled
	}

	insight, ok := b.insights[insightArn]
	if !ok {
		return nil, fmt.Errorf("%w: insight %s", ErrNotFound, insightArn)
	}

	// Return empty results (no real aggregation in mock)
	return &InsightResults{
		InsightArn:       insightArn,
		GroupByAttribute: insight.GroupByAttribute,
		ResultValues:     []map[string]any{},
	}, nil
}

// --- Standards ---

func (b *InMemoryBackend) BatchEnableStandards(requests []map[string]any) ([]*StandardsSubscription, []map[string]any) {
	b.mu.Lock("BatchEnableStandards")
	defer b.mu.Unlock()

	var subscriptions []*StandardsSubscription
	var failures []map[string]any

	for _, req := range requests {
		standardsArn, _ := req[keyStandardsArn].(string)

		if standardsArn == "" {
			failures = append(failures, map[string]any{
				keyStandardsArn: standardsArn,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: "StandardsArn is required",
			})

			continue
		}

		b.standardsSeq++
		subArn := fmt.Sprintf("arn:aws:securityhub:%s:%s:subscription/%d", b.region, b.accountID, b.standardsSeq)

		sub := &StandardsSubscription{
			StandardsSubscriptionArn: subArn,
			StandardsArn:             standardsArn,
			StandardsInput:           map[string]string{},
			StandardsStatus:          "PENDING",
		}

		if input, ok := req["StandardsInput"].(map[string]any); ok {
			for k, v := range input {
				if sv, isStr := v.(string); isStr {
					sub.StandardsInput[k] = sv
				}
			}
		}

		b.standardsSubscriptions[subArn] = sub
		subscriptions = append(subscriptions, sub)
	}

	if subscriptions == nil {
		subscriptions = []*StandardsSubscription{}
	}

	if failures == nil {
		failures = []map[string]any{}
	}

	return subscriptions, failures
}

func (b *InMemoryBackend) BatchDisableStandards(
	subscriptionArns []string,
) ([]*StandardsSubscription, []map[string]any) {
	b.mu.Lock("BatchDisableStandards")
	defer b.mu.Unlock()

	var subscriptions []*StandardsSubscription
	var failures []map[string]any

	for _, arn := range subscriptionArns {
		sub, ok := b.standardsSubscriptions[arn]
		if !ok {
			failures = append(failures, map[string]any{
				"StandardsSubscriptionArn": arn,
				keyErrorCode:               errCodeInvalidInput,
				keyErrorMessage:            "StandardsSubscription not found",
			})

			continue
		}

		sub.StandardsStatus = "INCOMPLETE"
		subscriptions = append(subscriptions, sub)
		delete(b.standardsSubscriptions, arn)
	}

	if subscriptions == nil {
		subscriptions = []*StandardsSubscription{}
	}

	if failures == nil {
		failures = []map[string]any{}
	}

	return subscriptions, failures
}

func (b *InMemoryBackend) GetEnabledStandards(
	subscriptionArns []string,
	nextToken string,
	maxResults int,
) ([]*StandardsSubscription, string) {
	b.mu.RLock("GetEnabledStandards")
	defer b.mu.RUnlock()
	results := filterOrAll(subscriptionArns, b.standardsSubscriptions)

	return paginateSlice(results, nextToken, maxResults, maxStandardsResults)
}

func (b *InMemoryBackend) DescribeStandards(nextToken string, maxResults int) ([]*Standard, string) {
	b.mu.RLock("DescribeStandards")
	defer b.mu.RUnlock()

	results := make([]*Standard, len(knownStandards))
	for i := range knownStandards {
		cp := knownStandards[i]
		results[i] = &cp
	}

	if maxResults <= 0 || maxResults > 25 {
		maxResults = 25
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*Standard{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) DescribeStandardsControls(
	subscriptionArn, nextToken string,
	maxResults int,
) ([]*StandardsControl, string) {
	b.mu.RLock("DescribeStandardsControls")
	defer b.mu.RUnlock()

	if _, ok := b.standardsSubscriptions[subscriptionArn]; !ok {
		return []*StandardsControl{}, ""
	}

	// Return a small set of mock controls
	controls := defaultControls(subscriptionArn)

	// Apply overrides
	for i, c := range controls {
		if override, ok := b.controlOverrides[c.StandardsControlArn]; ok {
			controls[i] = override
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(controls) {
		return []*StandardsControl{}, ""
	}

	end := start + maxResults
	end = min(end, len(controls))

	page := controls[start:end]
	nextOut := ""

	if end < len(controls) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

// defaultControls returns a minimal set of controls for a subscription.
func defaultControls(subscriptionArn string) []*StandardsControl {
	prefix := subscriptionArn + "/control"

	return []*StandardsControl{
		{
			StandardsControlArn:    prefix + "/1",
			ControlStatus:          statusEnabled,
			ControlID:              "1",
			Title:                  "Avoid the use of the root user",
			Description:            "The root user has unrestricted access to all resources in the AWS account.",
			RemediationURL:         "https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html",
			SeverityRating:         "CRITICAL",
			RelatedRequirements:    []string{"CIS AWS Foundations 1.1"},
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		},
		{
			StandardsControlArn:    prefix + "/2",
			ControlStatus:          statusEnabled,
			ControlID:              "2",
			Title:                  "Ensure MFA is enabled for all IAM users with console password",
			Description:            "Multi-Factor Authentication (MFA) adds an extra layer of protection.",
			RemediationURL:         "https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html",
			SeverityRating:         "MEDIUM",
			RelatedRequirements:    []string{"CIS AWS Foundations 1.2"},
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}
}

func (b *InMemoryBackend) UpdateStandardsControl(controlArn, controlStatus, disabledReason string) error {
	b.mu.Lock("UpdateStandardsControl")
	defer b.mu.Unlock()

	ctrl, exists := b.controlOverrides[controlArn]
	if !exists {
		// Create from defaults
		ctrl = &StandardsControl{
			StandardsControlArn:    controlArn,
			ControlStatus:          statusEnabled,
			ControlStatusUpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
	}

	ctrl.ControlStatus = controlStatus
	ctrl.DisabledReason = disabledReason
	ctrl.ControlStatusUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.controlOverrides[controlArn] = ctrl

	return nil
}

func (b *InMemoryBackend) ListStandardsControlAssociations(
	securityControlID, nextToken string,
	maxResults int,
) ([]*StandardsControlAssociation, string) {
	b.mu.RLock("ListStandardsControlAssociations")
	defer b.mu.RUnlock()

	results := make([]*StandardsControlAssociation, 0, len(knownStandards))

	for _, std := range knownStandards {
		assoc := &StandardsControlAssociation{
			SecurityControlID: securityControlID,
			StandardsArn:      std.StandardsArn,
			AssociationStatus: statusEnabled,
			UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		}
		results = append(results, assoc)
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*StandardsControlAssociation{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) BatchGetStandardsControlAssociations(
	requests []map[string]any,
) ([]*StandardsControlAssociation, []map[string]any) {
	b.mu.RLock("BatchGetStandardsControlAssociations")
	defer b.mu.RUnlock()

	var associations []*StandardsControlAssociation
	var unprocessed []map[string]any

	for _, req := range requests {
		secCtlID, _ := req[keySecurityControlID].(string)
		stdArn, _ := req[keyStandardsArn].(string)

		if secCtlID == "" || stdArn == "" {
			unprocessed = append(unprocessed, req)

			continue
		}

		assoc := &StandardsControlAssociation{
			SecurityControlID: secCtlID,
			StandardsArn:      stdArn,
			AssociationStatus: statusEnabled,
			UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		}
		associations = append(associations, assoc)
	}

	if associations == nil {
		associations = []*StandardsControlAssociation{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return associations, unprocessed
}

func (b *InMemoryBackend) BatchUpdateStandardsControlAssociations(updates []map[string]any) ([]map[string]any, error) {
	b.mu.Lock("BatchUpdateStandardsControlAssociations")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, u := range updates {
		if _, hasCtl := u[keySecurityControlID]; !hasCtl {
			unprocessed = append(unprocessed, u)
		}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return unprocessed, nil
}

// --- Action Targets ---

func (b *InMemoryBackend) CreateActionTarget(name, description, id string) (string, error) {
	b.mu.Lock("CreateActionTarget")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	arn := b.actionTargetARN(id)

	if _, exists := b.actionTargets[arn]; exists {
		return "", fmt.Errorf("%w: %s", ErrAlreadyExists, arn)
	}

	b.actionTargets[arn] = &ActionTarget{
		ActionTargetArn: arn,
		Name:            name,
		Description:     description,
	}

	return arn, nil
}

func (b *InMemoryBackend) DescribeActionTargets(
	actionTargetArns []string,
	nextToken string,
	maxResults int,
) ([]*ActionTarget, string) {
	b.mu.RLock("DescribeActionTargets")
	defer b.mu.RUnlock()
	results := filterOrAll(actionTargetArns, b.actionTargets)

	return paginateSlice(results, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateActionTarget(actionTargetArn, name, description string) error {
	b.mu.Lock("UpdateActionTarget")
	defer b.mu.Unlock()

	at, ok := b.actionTargets[actionTargetArn]
	if !ok {
		return fmt.Errorf("%w: action target %s", ErrNotFound, actionTargetArn)
	}

	if name != "" {
		at.Name = name
	}

	if description != "" {
		at.Description = description
	}

	return nil
}

func (b *InMemoryBackend) DeleteActionTarget(actionTargetArn string) (string, error) {
	b.mu.Lock("DeleteActionTarget")
	defer b.mu.Unlock()

	if _, ok := b.actionTargets[actionTargetArn]; !ok {
		return "", fmt.Errorf("%w: action target %s", ErrNotFound, actionTargetArn)
	}

	delete(b.actionTargets, actionTargetArn)

	return actionTargetArn, nil
}

// --- Products ---

var knownProducts = []Product{ //nolint:gochecknoglobals // read-only lookup data
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		ProductName:      "GuardDuty",
		CompanyName:      companyAWS,
		Description:      "Amazon GuardDuty is a threat detection service.",
		Categories:       []string{"Software and Configuration Checks"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/guardduty/",
		ActivationURL:    "https://console.aws.amazon.com/guardduty/",
	},
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/inspector",
		ProductName:      "Inspector",
		CompanyName:      companyAWS,
		Description:      "Amazon Inspector is an automated security assessment service.",
		Categories:       []string{"Software and Configuration Checks"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/inspector/",
		ActivationURL:    "https://console.aws.amazon.com/inspector/",
	},
	{
		ProductArn:       "arn:aws:securityhub:us-east-1::product/aws/macie",
		ProductName:      "Macie",
		CompanyName:      companyAWS,
		Description:      "Amazon Macie is a data security service.",
		Categories:       []string{"Sensitive Data Identifications"},
		IntegrationTypes: []string{intTypeSendFindings},
		MarketplaceURL:   "https://aws.amazon.com/macie/",
		ActivationURL:    "https://console.aws.amazon.com/macie/",
	},
}

func (b *InMemoryBackend) DescribeProducts(productArn, nextToken string, maxResults int) ([]*Product, string) {
	b.mu.RLock("DescribeProducts")
	defer b.mu.RUnlock()

	var results []*Product

	for i := range knownProducts {
		p := knownProducts[i]
		if productArn == "" || p.ProductArn == productArn {
			results = append(results, &p)
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*Product{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) EnableImportFindingsForProduct(productArn string) (string, error) {
	b.mu.Lock("EnableImportFindingsForProduct")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return "", ErrHubNotEnabled
	}

	// Check if already enabled
	for subArn, pArn := range b.productSubscriptions {
		if pArn == productArn {
			return subArn, nil
		}
	}

	subArn := fmt.Sprintf("arn:aws:securityhub:%s:%s:product-subscription/%s", b.region, b.accountID, productArn)
	b.productSubscriptions[subArn] = productArn

	return subArn, nil
}

func (b *InMemoryBackend) DisableImportFindingsForProduct(productSubscriptionArn string) error {
	b.mu.Lock("DisableImportFindingsForProduct")
	defer b.mu.Unlock()

	if _, ok := b.productSubscriptions[productSubscriptionArn]; !ok {
		return fmt.Errorf("%w: product subscription %s", ErrNotFound, productSubscriptionArn)
	}

	delete(b.productSubscriptions, productSubscriptionArn)

	return nil
}

func (b *InMemoryBackend) ListEnabledProductsForImport(nextToken string, maxResults int) ([]string, string) {
	b.mu.RLock("ListEnabledProductsForImport")
	defer b.mu.RUnlock()

	results := make([]string, 0, len(b.productSubscriptions))
	for subArn := range b.productSubscriptions {
		results = append(results, subArn)
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []string{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

// --- Security Controls ---

var knownSecurityControls = []SecurityControlDefinition{ //nolint:gochecknoglobals // read-only lookup data
	{
		SecurityControlID:         "CloudTrail.1",
		Title:                     "CloudTrail should be enabled and configured with at least one multi-Region trail",
		Description:               "This control checks that there is at least one multi-region AWS CloudTrail trail.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/cloudtrail-controls.html",
		SeverityRating:            "HIGH",
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
	{
		SecurityControlID: "IAM.1",
		Title:             "IAM policies should not allow full administrative privileges",
		Description: "This control checks whether the default version of IAM policies have " +
			"administrator access.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/iam-controls.html",
		SeverityRating:            "HIGH",
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
	{
		SecurityControlID:         "S3.1",
		Title:                     "S3 Block Public Access setting should be enabled",
		Description:               "This control checks whether the S3 block public access setting is enabled.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/s3-controls.html",
		SeverityRating:            "MEDIUM",
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
}

func (b *InMemoryBackend) GetSecurityControlDefinition(securityControlID string) (*SecurityControlDefinition, error) {
	b.mu.RLock("GetSecurityControlDefinition")
	defer b.mu.RUnlock()

	for i := range knownSecurityControls {
		if knownSecurityControls[i].SecurityControlID == securityControlID {
			cp := knownSecurityControls[i]

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: security control %s", ErrNotFound, securityControlID)
}

func (b *InMemoryBackend) ListSecurityControlDefinitions(
	_, nextToken string,
	maxResults int,
) ([]*SecurityControlDefinition, string) {
	b.mu.RLock("ListSecurityControlDefinitions")
	defer b.mu.RUnlock()

	results := make([]*SecurityControlDefinition, len(knownSecurityControls))
	for i := range knownSecurityControls {
		cp := knownSecurityControls[i]
		results[i] = &cp
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*SecurityControlDefinition{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) BatchGetSecurityControls(securityControlIDs []string) ([]*SecurityControl, []map[string]any) {
	b.mu.RLock("BatchGetSecurityControls")
	defer b.mu.RUnlock()

	var controls []*SecurityControl
	var unprocessed []map[string]any

	for _, id := range securityControlIDs {
		var def *SecurityControlDefinition

		for i := range knownSecurityControls {
			if knownSecurityControls[i].SecurityControlID == id {
				cp := knownSecurityControls[i]
				def = &cp

				break
			}
		}

		if def == nil {
			unprocessed = append(unprocessed, map[string]any{
				keySecurityControlID: id,
				keyErrorCode:         errCodeInvalidInput,
				keyErrorMessage:      "Security control not found",
			})

			continue
		}

		params := b.controlParams[id]
		if params == nil {
			params = map[string]any{}
		}

		controls = append(controls, &SecurityControl{
			SecurityControlID:     def.SecurityControlID,
			SecurityControlArn:    b.securityControlARN(def.SecurityControlID),
			Title:                 def.Title,
			Description:           def.Description,
			RemediationURL:        def.RemediationURL,
			SeverityRating:        def.SeverityRating,
			SecurityControlStatus: statusEnabled,
			UpdateStatus:          "READY",
			Parameters:            params,
		})
	}

	if controls == nil {
		controls = []*SecurityControl{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return controls, unprocessed
}

func (b *InMemoryBackend) UpdateSecurityControl(
	securityControlID string,
	parameters map[string]any,
	_ string,
) error {
	b.mu.Lock("UpdateSecurityControl")
	defer b.mu.Unlock()

	b.controlParams[securityControlID] = parameters

	return nil
}

// --- Automation Rules ---

func (b *InMemoryBackend) CreateAutomationRule(rule map[string]any) (string, string) {
	b.mu.Lock("CreateAutomationRule")
	defer b.mu.Unlock()

	b.automationRuleSeq++
	ruleArn := b.automationRuleARN(b.automationRuleSeq)
	now := time.Now().UTC().Format(time.RFC3339)

	criteria, _ := rule["Criteria"].(map[string]any)
	actions, _ := rule["Actions"].([]any)

	var actionMaps []map[string]any

	for _, a := range actions {
		if m, ok := a.(map[string]any); ok {
			actionMaps = append(actionMaps, m)
		}
	}

	ruleName, _ := rule["RuleName"].(string)
	desc, _ := rule["Description"].(string)
	ruleStatus, _ := rule["RuleStatus"].(string)
	isTerminal, _ := rule["IsTerminal"].(bool)

	ruleOrder := int32(0)
	if ro, ok := rule["RuleOrder"].(float64); ok {
		ruleOrder = int32(ro)
	}

	if ruleStatus == "" {
		ruleStatus = statusEnabled
	}

	b.automationRules[ruleArn] = &AutomationRule{
		RuleArn:     ruleArn,
		RuleStatus:  ruleStatus,
		RuleOrder:   ruleOrder,
		RuleName:    ruleName,
		Description: desc,
		IsTerminal:  isTerminal,
		CreatedAt:   now,
		UpdatedAt:   now,
		CreatedBy:   b.accountID,
		Criteria:    criteria,
		Actions:     actionMaps,
	}

	return ruleArn, now
}

func (b *InMemoryBackend) ListAutomationRules(nextToken string, maxResults int) ([]*AutomationRuleMetadata, string) {
	b.mu.RLock("ListAutomationRules")
	defer b.mu.RUnlock()

	results := make([]*AutomationRuleMetadata, 0, len(b.automationRules))

	for _, rule := range b.automationRules {
		results = append(results, &AutomationRuleMetadata{
			RuleArn:     rule.RuleArn,
			RuleStatus:  rule.RuleStatus,
			RuleOrder:   rule.RuleOrder,
			RuleName:    rule.RuleName,
			Description: rule.Description,
			IsTerminal:  rule.IsTerminal,
			CreatedAt:   rule.CreatedAt,
			UpdatedAt:   rule.UpdatedAt,
			CreatedBy:   rule.CreatedBy,
		})
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*AutomationRuleMetadata{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) BatchGetAutomationRules(automationRulesArns []string) ([]*AutomationRule, []map[string]any) {
	b.mu.RLock("BatchGetAutomationRules")
	defer b.mu.RUnlock()

	var rules []*AutomationRule
	var unprocessed []map[string]any

	for _, arn := range automationRulesArns {
		rule, ok := b.automationRules[arn]
		if !ok {
			unprocessed = append(unprocessed, map[string]any{
				keyRuleArn:      arn,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: msgRuleNotFound,
			})

			continue
		}

		rules = append(rules, rule)
	}

	if rules == nil {
		rules = []*AutomationRule{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return rules, unprocessed
}

func (b *InMemoryBackend) BatchDeleteAutomationRules(automationRulesArns []string) ([]string, []map[string]any) {
	b.mu.Lock("BatchDeleteAutomationRules")
	defer b.mu.Unlock()

	var deleted []string
	var unprocessed []map[string]any

	for _, arn := range automationRulesArns {
		if _, ok := b.automationRules[arn]; !ok {
			unprocessed = append(unprocessed, map[string]any{
				keyRuleArn:      arn,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: msgRuleNotFound,
			})

			continue
		}

		delete(b.automationRules, arn)
		deleted = append(deleted, arn)
	}

	if deleted == nil {
		deleted = []string{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return deleted, unprocessed
}

func (b *InMemoryBackend) BatchUpdateAutomationRules(updates []map[string]any) ([]string, []map[string]any) {
	b.mu.Lock("BatchUpdateAutomationRules")
	defer b.mu.Unlock()

	var processed []string
	var unprocessed []map[string]any

	for _, u := range updates {
		arn, _ := u[keyRuleArn].(string)

		rule, ok := b.automationRules[arn]
		if !ok {
			unprocessed = append(unprocessed, map[string]any{
				keyRuleArn:      arn,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: msgRuleNotFound,
			})

			continue
		}

		if name, hasName := u["RuleName"].(string); hasName {
			rule.RuleName = name
		}

		if desc, hasDesc := u["Description"].(string); hasDesc {
			rule.Description = desc
		}

		if status, hasStatus := u["RuleStatus"].(string); hasStatus {
			rule.RuleStatus = status
		}

		if order, hasOrder := u["RuleOrder"].(float64); hasOrder {
			rule.RuleOrder = int32(order)
		}

		if terminal, hasTerminal := u["IsTerminal"].(bool); hasTerminal {
			rule.IsTerminal = terminal
		}

		rule.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		processed = append(processed, arn)
	}

	if processed == nil {
		processed = []string{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return processed, unprocessed
}

// --- Tags ---

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)

	if existing, ok := b.tags[resourceArn]; ok {
		maps.Copy(result, existing)
	}

	return result, nil
}

// --- Generic pagination helpers ---

// filterOrAll returns values from m for the given arns, or all values if arns is empty.
func filterOrAll[T any](arns []string, m map[string]T) []T {
	var results []T
	if len(arns) > 0 {
		for _, arn := range arns {
			if v, ok := m[arn]; ok {
				results = append(results, v)
			}
		}
	} else {
		for _, v := range m {
			results = append(results, v)
		}
	}

	return results
}

// paginateSlice applies token-based pagination, capping maxResults at maxCap.
func paginateSlice[T any](results []T, nextToken string, maxResults, maxCap int) ([]T, string) {
	if maxResults <= 0 || maxResults > maxCap {
		maxResults = maxCap
	}
	start := decodeToken(nextToken)
	if start >= len(results) {
		return []T{}, ""
	}
	end := start + maxResults
	end = min(end, len(results))
	nextOut := ""
	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return results[start:end], nextOut
}

// --- Token helpers ---

func encodeToken(offset int) string {
	return strconv.Itoa(offset)
}

func decodeToken(token string) int {
	if token == "" {
		return 0
	}

	var offset int

	if _, err := fmt.Sscanf(token, "%d", &offset); err != nil {
		return 0
	}

	return offset
}
