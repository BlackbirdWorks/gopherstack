package securityhub

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	securityHubServiceName = "securityhub"
	matchPriority          = service.PriorityPathVersioned

	pathAccounts     = "/accounts"
	pathAssociations = "/associations"

	keyMessage                = "Message"
	keyInsightArn             = "InsightArn"
	keyName                   = "Name"
	keyDescription            = "Description"
	keyTitle                  = "Title"
	keyRemediationURL         = "RemediationUrl"
	keySeverityRating         = "SeverityRating"
	keyUpdatedAt              = "UpdatedAt"
	keyActionTargetArn        = "ActionTargetArn"
	keyCreatedAt              = "CreatedAt"
	keyStandardsSubscriptions = "StandardsSubscriptions"
	keyUnprocessedAutoRules   = "UnprocessedAutomationRules"
	keyMaxResults             = "MaxResults"

	msgHubNotEnabled   = "SecurityHub is not enabled"
	msgInsightNotFound = "Insight not found"

	// Operation names (returned by ExtractOperation).
	opEnableSecurityHub    = "EnableSecurityHub"
	opDisableSecurityHub   = "DisableSecurityHub"
	opDescribeHub          = "DescribeHub"
	opUpdateSecurityHubCfg = "UpdateSecurityHubConfiguration"

	opGetFindings         = "GetFindings"
	opBatchImportFindings = "BatchImportFindings"
	opBatchUpdateFindings = "BatchUpdateFindings"
	opUpdateFindings      = "UpdateFindings"
	opGetFindingHistory   = "GetFindingHistory"

	opCreateInsight     = "CreateInsight"
	opGetInsights       = "GetInsights"
	opGetInsightResults = "GetInsightResults"
	opUpdateInsight     = "UpdateInsight"
	opDeleteInsight     = "DeleteInsight"

	opBatchEnableStandards  = "BatchEnableStandards"
	opBatchDisableStandards = "BatchDisableStandards"
	opGetEnabledStandards   = "GetEnabledStandards"
	opDescribeStandards     = "DescribeStandards"
	opDescribeStdControls   = "DescribeStandardsControls"
	opUpdateStdControl      = "UpdateStandardsControl"

	opListStdCtlAssocs        = "ListStandardsControlAssociations"
	opBatchGetStdCtlAssocs    = "BatchGetStandardsControlAssociations"
	opBatchUpdateStdCtlAssocs = "BatchUpdateStandardsControlAssociations"

	opCreateActionTarget    = "CreateActionTarget"
	opDescribeActionTargets = "DescribeActionTargets"
	opUpdateActionTarget    = "UpdateActionTarget"
	opDeleteActionTarget    = "DeleteActionTarget"

	opDescribeProducts                = "DescribeProducts"
	opListEnabledProductsForImport    = "ListEnabledProductsForImport"
	opEnableImportFindingsForProduct  = "EnableImportFindingsForProduct"
	opDisableImportFindingsForProduct = "DisableImportFindingsForProduct"

	opGetSecurityControlDefinition   = "GetSecurityControlDefinition"
	opListSecurityControlDefinitions = "ListSecurityControlDefinitions"
	opBatchGetSecurityControls       = "BatchGetSecurityControls"
	opUpdateSecurityControl          = "UpdateSecurityControl"

	opListAutomationRules     = "ListAutomationRules"
	opCreateAutomationRule    = "CreateAutomationRule"
	opBatchGetAutomationRules = "BatchGetAutomationRules"
	opBatchDeleteAutoRules    = "BatchDeleteAutomationRules"
	opBatchUpdateAutoRules    = "BatchUpdateAutomationRules"

	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"

	// Members.
	opCreateMembers       = "CreateMembers"
	opDeleteMembers       = "DeleteMembers"
	opGetMembers          = "GetMembers"
	opInviteMembers       = "InviteMembers"
	opListMembers         = "ListMembers"
	opDisassociateMembers = "DisassociateMembers"

	// Invitations / Admin.
	opAcceptAdministratorInvitation        = "AcceptAdministratorInvitation"
	opAcceptInvitation                     = "AcceptInvitation"
	opDeclineInvitations                   = "DeclineInvitations"
	opDeleteInvitations                    = "DeleteInvitations"
	opGetInvitationsCount                  = "GetInvitationsCount"
	opListInvitations                      = "ListInvitations"
	opGetAdministratorAccount              = "GetAdministratorAccount"
	opGetMasterAccount                     = "GetMasterAccount"
	opDisassociateFromAdministratorAccount = "DisassociateFromAdministratorAccount"
	opDisassociateFromMasterAccount        = "DisassociateFromMasterAccount"

	// Organization.
	opDescribeOrganizationConfiguration = "DescribeOrganizationConfiguration"
	opUpdateOrganizationConfiguration   = "UpdateOrganizationConfiguration"
	opEnableOrganizationAdminAccount    = "EnableOrganizationAdminAccount"
	opDisableOrganizationAdminAccount   = "DisableOrganizationAdminAccount"
	opListOrganizationAdminAccounts     = "ListOrganizationAdminAccounts"

	// Finding Aggregator.
	opCreateFindingAggregator = "CreateFindingAggregator"
	opGetFindingAggregator    = "GetFindingAggregator"
	opListFindingAggregators  = "ListFindingAggregators"
	opUpdateFindingAggregator = "UpdateFindingAggregator"
	opDeleteFindingAggregator = "DeleteFindingAggregator"

	// Configuration Policy.
	opCreateConfigurationPolicy               = "CreateConfigurationPolicy"
	opGetConfigurationPolicy                  = "GetConfigurationPolicy"
	opUpdateConfigurationPolicy               = "UpdateConfigurationPolicy"
	opDeleteConfigurationPolicy               = "DeleteConfigurationPolicy"
	opListConfigurationPolicies               = "ListConfigurationPolicies"
	opGetConfigurationPolicyAssociation       = "GetConfigurationPolicyAssociation"
	opListConfigurationPolicyAssociations     = "ListConfigurationPolicyAssociations"
	opStartConfigurationPolicyAssociation     = "StartConfigurationPolicyAssociation"
	opStartConfigurationPolicyDisassociation  = "StartConfigurationPolicyDisassociation"
	opBatchGetConfigurationPolicyAssociations = "BatchGetConfigurationPolicyAssociations"

	// Hub V2.
	opEnableSecurityHubV2   = "EnableSecurityHubV2"
	opDisableSecurityHubV2  = "DisableSecurityHubV2"
	opDescribeSecurityHubV2 = "DescribeSecurityHubV2"

	// Aggregator V2.
	opCreateAggregatorV2 = "CreateAggregatorV2"
	opGetAggregatorV2    = "GetAggregatorV2"
	opListAggregatorsV2  = "ListAggregatorsV2"
	opUpdateAggregatorV2 = "UpdateAggregatorV2"
	opDeleteAggregatorV2 = "DeleteAggregatorV2"

	// Automation Rules V2.
	opCreateAutomationRuleV2 = "CreateAutomationRuleV2"
	opGetAutomationRuleV2    = "GetAutomationRuleV2"
	opListAutomationRulesV2  = "ListAutomationRulesV2"
	opUpdateAutomationRuleV2 = "UpdateAutomationRuleV2"
	opDeleteAutomationRuleV2 = "DeleteAutomationRuleV2"

	// Connectors V2.
	opCreateConnectorV2   = "CreateConnectorV2"
	opGetConnectorV2      = "GetConnectorV2"
	opListConnectorsV2    = "ListConnectorsV2"
	opUpdateConnectorV2   = "UpdateConnectorV2"
	opDeleteConnectorV2   = "DeleteConnectorV2"
	opRegisterConnectorV2 = "RegisterConnectorV2"

	// Tickets V2.
	opCreateTicketV2 = "CreateTicketV2"

	// Findings V2.
	opGetFindingsV2          = "GetFindingsV2"
	opBatchUpdateFindingsV2  = "BatchUpdateFindingsV2"
	opGetFindingStatisticsV2 = "GetFindingStatisticsV2"
	opGetFindingsTrendsV2    = "GetFindingsTrendsV2"

	// Resources V2.
	opGetResourcesV2           = "GetResourcesV2"
	opGetResourcesStatisticsV2 = "GetResourcesStatisticsV2"
	opGetResourcesTrendsV2     = "GetResourcesTrendsV2"

	// Products V2.
	opDescribeProductsV2 = "DescribeProductsV2"

	// Recommended Policy V2.
	opGenerateRecommendedPolicyV2 = "GenerateRecommendedPolicyV2"
	opGetRecommendedPolicyV2      = "GetRecommendedPolicyV2"

	opUnknown = "Unknown"
)

// Handler handles SecurityHub HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "SecurityHub" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen // existing issue.
	return []string{
		opEnableSecurityHub,
		opDisableSecurityHub,
		opDescribeHub,
		opUpdateSecurityHubCfg,
		opGetFindings,
		opBatchImportFindings,
		opBatchUpdateFindings,
		opUpdateFindings,
		opGetFindingHistory,
		opCreateInsight,
		opGetInsights,
		opGetInsightResults,
		opUpdateInsight,
		opDeleteInsight,
		opBatchEnableStandards,
		opBatchDisableStandards,
		opGetEnabledStandards,
		opDescribeStandards,
		opDescribeStdControls,
		opUpdateStdControl,
		opListStdCtlAssocs,
		opBatchGetStdCtlAssocs,
		opBatchUpdateStdCtlAssocs,
		opCreateActionTarget,
		opDescribeActionTargets,
		opUpdateActionTarget,
		opDeleteActionTarget,
		opDescribeProducts,
		opListEnabledProductsForImport,
		opEnableImportFindingsForProduct,
		opDisableImportFindingsForProduct,
		opGetSecurityControlDefinition,
		opListSecurityControlDefinitions,
		opBatchGetSecurityControls,
		opUpdateSecurityControl,
		opListAutomationRules,
		opCreateAutomationRule,
		opBatchGetAutomationRules,
		opBatchDeleteAutoRules,
		opBatchUpdateAutoRules,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		// Members
		opCreateMembers,
		opDeleteMembers,
		opGetMembers,
		opInviteMembers,
		opListMembers,
		opDisassociateMembers,
		// Invitations / Admin
		opAcceptAdministratorInvitation,
		opAcceptInvitation,
		opDeclineInvitations,
		opDeleteInvitations,
		opGetInvitationsCount,
		opListInvitations,
		opGetAdministratorAccount,
		opGetMasterAccount,
		opDisassociateFromAdministratorAccount,
		opDisassociateFromMasterAccount,
		// Organization
		opDescribeOrganizationConfiguration,
		opUpdateOrganizationConfiguration,
		opEnableOrganizationAdminAccount,
		opDisableOrganizationAdminAccount,
		opListOrganizationAdminAccounts,
		// Finding Aggregator
		opCreateFindingAggregator,
		opGetFindingAggregator,
		opListFindingAggregators,
		opUpdateFindingAggregator,
		opDeleteFindingAggregator,
		// Configuration Policy
		opCreateConfigurationPolicy,
		opGetConfigurationPolicy,
		opUpdateConfigurationPolicy,
		opDeleteConfigurationPolicy,
		opListConfigurationPolicies,
		opGetConfigurationPolicyAssociation,
		opListConfigurationPolicyAssociations,
		opStartConfigurationPolicyAssociation,
		opStartConfigurationPolicyDisassociation,
		opBatchGetConfigurationPolicyAssociations,
		// Hub V2
		opEnableSecurityHubV2,
		opDisableSecurityHubV2,
		opDescribeSecurityHubV2,
		// Aggregator V2
		opCreateAggregatorV2,
		opGetAggregatorV2,
		opListAggregatorsV2,
		opUpdateAggregatorV2,
		opDeleteAggregatorV2,
		// Automation Rules V2
		opCreateAutomationRuleV2,
		opGetAutomationRuleV2,
		opListAutomationRulesV2,
		opUpdateAutomationRuleV2,
		opDeleteAutomationRuleV2,
		// Connectors V2
		opCreateConnectorV2,
		opGetConnectorV2,
		opListConnectorsV2,
		opUpdateConnectorV2,
		opDeleteConnectorV2,
		opRegisterConnectorV2,
		// Tickets V2
		opCreateTicketV2,
		// Findings V2
		opGetFindingsV2,
		opBatchUpdateFindingsV2,
		opGetFindingStatisticsV2,
		opGetFindingsTrendsV2,
		// Resources V2
		opGetResourcesV2,
		opGetResourcesStatisticsV2,
		opGetResourcesTrendsV2,
		// Products V2
		opDescribeProductsV2,
		// Recommended Policy V2
		opGenerateRecommendedPolicyV2,
		opGetRecommendedPolicyV2,
	}
}

// ChaosServiceName returns the service name for chaos engineering.
func (h *Handler) ChaosServiceName() string { return securityHubServiceName }

// ChaosOperations returns the operations for chaos engineering.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions for chaos engineering.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches SecurityHub requests by path.
// For /findings and /tags paths, uses the Authorization header to disambiguate
// from other services (e.g. Macie2) that share those path prefixes.
func (h *Handler) RouteMatcher() service.Matcher { //nolint:gocyclo,cyclop // existing issue.
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		// Unambiguous SecurityHub-only paths
		if strings.HasPrefix(path, "/actionTargets") ||
			strings.HasPrefix(path, "/insights") ||
			strings.HasPrefix(path, "/productSubscriptions") ||
			strings.HasPrefix(path, "/standards") ||
			strings.HasPrefix(path, "/associations") ||
			strings.HasPrefix(path, "/securityControl") ||
			strings.HasPrefix(path, "/automationrules") ||
			strings.HasPrefix(path, "/findingHistory") ||
			strings.HasPrefix(path, "/hubv2") ||
			strings.HasPrefix(path, "/members") ||
			strings.HasPrefix(path, "/invitations") ||
			strings.HasPrefix(path, "/administrator") ||
			strings.HasPrefix(path, "/master") ||
			strings.HasPrefix(path, "/organization") ||
			strings.HasPrefix(path, "/findingAggregator") ||
			strings.HasPrefix(path, "/configurationPolicy") ||
			strings.HasPrefix(path, "/configurationPolicyAssociation") ||
			strings.HasPrefix(path, "/aggregatorv2") ||
			strings.HasPrefix(path, "/automationrulesv2") ||
			strings.HasPrefix(path, "/connectorsv2") ||
			strings.HasPrefix(path, "/ticketsv2") ||
			strings.HasPrefix(path, "/findingsv2") ||
			strings.HasPrefix(path, "/findingsTrendsv2") ||
			strings.HasPrefix(path, "/resourcesv2") ||
			strings.HasPrefix(path, "/resourcesTrendsv2") ||
			strings.HasPrefix(path, "/productsV2") ||
			strings.HasPrefix(path, "/recommendedPolicyV2") ||
			path == pathAccounts ||
			strings.HasPrefix(path, "/products") {
			return true
		}

		// /accounts with any method
		if strings.HasPrefix(path, pathAccounts) {
			return true
		}

		// /findings — disambiguate using Authorization signing service
		if strings.HasPrefix(path, "/findings") {
			return isSecurityHubRequest(c)
		}

		// /tags/{ARN} — match SecurityHub ARNs
		if after, ok := strings.CutPrefix(path, "/tags/"); ok {
			return strings.Contains(after, ":"+securityHubServiceName+":")
		}

		return false
	}
}

// isSecurityHubRequest checks the Authorization header for the securityhub signing service.
func isSecurityHubRequest(c *echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")

	return strings.Contains(auth, "/"+securityHubServiceName+"/")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

// handleREST dispatches to the appropriate handler function.
func (h *Handler) handleREST(c *echo.Context) error { //nolint:funlen // existing issue.
	op, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: "invalid JSON body",
			})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	handlers := map[string]func() error{
		// Hub
		opEnableSecurityHub:    func() error { return h.handleEnableHub(c, body) },
		opDisableSecurityHub:   func() error { return h.handleDisableHub(c) },
		opDescribeHub:          func() error { return h.handleDescribeHub(c) },
		opUpdateSecurityHubCfg: func() error { return h.handleUpdateHubConfig(c, body) },
		// Findings
		opGetFindings:         func() error { return h.handleGetFindings(c, body) },
		opBatchImportFindings: func() error { return h.handleBatchImportFindings(c, body) },
		opBatchUpdateFindings: func() error { return h.handleBatchUpdateFindings(c, body) },
		opUpdateFindings:      func() error { return h.handleUpdateFindings(c, body) },
		opGetFindingHistory:   func() error { return h.handleGetFindingHistory(c, body) },
		// Insights
		opCreateInsight:     func() error { return h.handleCreateInsight(c, body) },
		opGetInsights:       func() error { return h.handleGetInsights(c, body) },
		opGetInsightResults: func() error { return h.handleGetInsightResults(c, resource) },
		opUpdateInsight:     func() error { return h.handleUpdateInsight(c, resource, body) },
		opDeleteInsight:     func() error { return h.handleDeleteInsight(c, resource) },
		// Standards
		opBatchEnableStandards:    func() error { return h.handleBatchEnableStandards(c, body) },
		opBatchDisableStandards:   func() error { return h.handleBatchDisableStandards(c, body) },
		opGetEnabledStandards:     func() error { return h.handleGetEnabledStandards(c, body) },
		opDescribeStandards:       func() error { return h.handleDescribeStandards(c) },
		opDescribeStdControls:     func() error { return h.handleDescribeStandardsControls(c, resource) },
		opUpdateStdControl:        func() error { return h.handleUpdateStandardsControl(c, resource, body) },
		opListStdCtlAssocs:        func() error { return h.handleListStandardsControlAssociations(c) },
		opBatchGetStdCtlAssocs:    func() error { return h.handleBatchGetStdCtlAssociations(c, body) },
		opBatchUpdateStdCtlAssocs: func() error { return h.handleBatchUpdateStdCtlAssociations(c, body) },
		// Action Targets
		opCreateActionTarget:    func() error { return h.handleCreateActionTarget(c, body) },
		opDescribeActionTargets: func() error { return h.handleDescribeActionTargets(c, body) },
		opUpdateActionTarget:    func() error { return h.handleUpdateActionTarget(c, resource, body) },
		opDeleteActionTarget:    func() error { return h.handleDeleteActionTarget(c, resource) },
		// Products
		opDescribeProducts:                func() error { return h.handleDescribeProducts(c) },
		opListEnabledProductsForImport:    func() error { return h.handleListEnabledProductsForImport(c) },
		opEnableImportFindingsForProduct:  func() error { return h.handleEnableImportFindingsForProduct(c, body) },
		opDisableImportFindingsForProduct: func() error { return h.handleDisableImportFindingsForProduct(c, resource) },
		// Security Controls
		opGetSecurityControlDefinition:   func() error { return h.handleGetSecurityControlDefinition(c) },
		opListSecurityControlDefinitions: func() error { return h.handleListSecurityControlDefinitions(c) },
		opBatchGetSecurityControls:       func() error { return h.handleBatchGetSecurityControls(c, body) },
		opUpdateSecurityControl:          func() error { return h.handleUpdateSecurityControl(c, body) },
		// Automation Rules
		opListAutomationRules:     func() error { return h.handleListAutomationRules(c) },
		opCreateAutomationRule:    func() error { return h.handleCreateAutomationRule(c, body) },
		opBatchGetAutomationRules: func() error { return h.handleBatchGetAutomationRules(c, body) },
		opBatchDeleteAutoRules:    func() error { return h.handleBatchDeleteAutomationRules(c, body) },
		opBatchUpdateAutoRules:    func() error { return h.handleBatchUpdateAutomationRules(c, body) },
		// Tags
		opListTagsForResource: func() error { return h.handleListTagsForResource(c, resource) },
		opTagResource:         func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:       func() error { return h.handleUntagResource(c, resource) },
		// Members
		opCreateMembers:       func() error { return h.handleCreateMembers(c, body) },
		opDeleteMembers:       func() error { return h.handleDeleteMembers(c, body) },
		opGetMembers:          func() error { return h.handleGetMembers(c, body) },
		opInviteMembers:       func() error { return h.handleInviteMembers(c, body) },
		opListMembers:         func() error { return h.handleListMembers(c) },
		opDisassociateMembers: func() error { return h.handleDisassociateMembers(c, body) },
		// Invitations / Admin
		opAcceptAdministratorInvitation:        func() error { return h.handleAcceptAdministratorInvitation(c, body) },
		opAcceptInvitation:                     func() error { return h.handleAcceptInvitation(c, body) },
		opDeclineInvitations:                   func() error { return h.handleDeclineInvitations(c, body) },
		opDeleteInvitations:                    func() error { return h.handleDeleteInvitations(c, body) },
		opGetInvitationsCount:                  func() error { return h.handleGetInvitationsCount(c) },
		opListInvitations:                      func() error { return h.handleListInvitations(c) },
		opGetAdministratorAccount:              func() error { return h.handleGetAdministratorAccount(c) },
		opGetMasterAccount:                     func() error { return h.handleGetMasterAccount(c) },
		opDisassociateFromAdministratorAccount: func() error { return h.handleDisassociateFromAdministratorAccount(c) },
		opDisassociateFromMasterAccount:        func() error { return h.handleDisassociateFromMasterAccount(c) },
		// Organization
		opDescribeOrganizationConfiguration: func() error { return h.handleDescribeOrganizationConfiguration(c) },
		opUpdateOrganizationConfiguration:   func() error { return h.handleUpdateOrganizationConfiguration(c, body) },
		opEnableOrganizationAdminAccount:    func() error { return h.handleEnableOrganizationAdminAccount(c, body) },
		opDisableOrganizationAdminAccount:   func() error { return h.handleDisableOrganizationAdminAccount(c, body) },
		opListOrganizationAdminAccounts:     func() error { return h.handleListOrganizationAdminAccounts(c) },
		// Finding Aggregator
		opCreateFindingAggregator: func() error { return h.handleCreateFindingAggregator(c, body) },
		opGetFindingAggregator:    func() error { return h.handleGetFindingAggregator(c, resource) },
		opListFindingAggregators:  func() error { return h.handleListFindingAggregators(c) },
		opUpdateFindingAggregator: func() error { return h.handleUpdateFindingAggregator(c, body) },
		opDeleteFindingAggregator: func() error { return h.handleDeleteFindingAggregator(c, resource) },
		// Configuration Policy
		opCreateConfigurationPolicy:               func() error { return h.handleCreateConfigurationPolicy(c, body) },
		opGetConfigurationPolicy:                  func() error { return h.handleGetConfigurationPolicy(c, resource) },
		opUpdateConfigurationPolicy:               func() error { return h.handleUpdateConfigurationPolicy(c, resource, body) }, //nolint:lll // existing issue.
		opDeleteConfigurationPolicy:               func() error { return h.handleDeleteConfigurationPolicy(c, resource) },
		opListConfigurationPolicies:               func() error { return h.handleListConfigurationPolicies(c) },
		opGetConfigurationPolicyAssociation:       func() error { return h.handleGetConfigurationPolicyAssociation(c, body) },
		opListConfigurationPolicyAssociations:     func() error { return h.handleListConfigurationPolicyAssociations(c, body) },     //nolint:lll // existing issue.
		opStartConfigurationPolicyAssociation:     func() error { return h.handleStartConfigurationPolicyAssociation(c, body) },     //nolint:lll // existing issue.
		opStartConfigurationPolicyDisassociation:  func() error { return h.handleStartConfigurationPolicyDisassociation(c, body) },  //nolint:lll // existing issue.
		opBatchGetConfigurationPolicyAssociations: func() error { return h.handleBatchGetConfigurationPolicyAssociations(c, body) }, //nolint:lll // existing issue.
		// Hub V2
		opEnableSecurityHubV2:   func() error { return h.handleEnableSecurityHubV2(c, body) },
		opDisableSecurityHubV2:  func() error { return h.handleDisableSecurityHubV2(c) },
		opDescribeSecurityHubV2: func() error { return h.handleDescribeSecurityHubV2(c) },
		// Aggregator V2
		opCreateAggregatorV2: func() error { return h.handleCreateAggregatorV2(c, body) },
		opGetAggregatorV2:    func() error { return h.handleGetAggregatorV2(c, resource) },
		opListAggregatorsV2:  func() error { return h.handleListAggregatorsV2(c) },
		opUpdateAggregatorV2: func() error { return h.handleUpdateAggregatorV2(c, resource, body) },
		opDeleteAggregatorV2: func() error { return h.handleDeleteAggregatorV2(c, resource) },
		// Automation Rules V2
		opCreateAutomationRuleV2: func() error { return h.handleCreateAutomationRuleV2(c, body) },
		opGetAutomationRuleV2:    func() error { return h.handleGetAutomationRuleV2(c, resource) },
		opListAutomationRulesV2:  func() error { return h.handleListAutomationRulesV2(c) },
		opUpdateAutomationRuleV2: func() error { return h.handleUpdateAutomationRuleV2(c, resource, body) },
		opDeleteAutomationRuleV2: func() error { return h.handleDeleteAutomationRuleV2(c, resource) },
		// Connectors V2
		opCreateConnectorV2:   func() error { return h.handleCreateConnectorV2(c, body) },
		opGetConnectorV2:      func() error { return h.handleGetConnectorV2(c, resource) },
		opListConnectorsV2:    func() error { return h.handleListConnectorsV2(c) },
		opUpdateConnectorV2:   func() error { return h.handleUpdateConnectorV2(c, resource, body) },
		opDeleteConnectorV2:   func() error { return h.handleDeleteConnectorV2(c, resource) },
		opRegisterConnectorV2: func() error { return h.handleRegisterConnectorV2(c, body) },
		// Tickets V2
		opCreateTicketV2: func() error { return h.handleCreateTicketV2(c, body) },
		// Findings V2
		opGetFindingsV2:          func() error { return h.handleGetFindingsV2(c, body) },
		opBatchUpdateFindingsV2:  func() error { return h.handleBatchUpdateFindingsV2(c, body) },
		opGetFindingStatisticsV2: func() error { return h.handleGetFindingStatisticsV2(c, body) },
		opGetFindingsTrendsV2:    func() error { return h.handleGetFindingsTrendsV2(c, body) },
		// Resources V2
		opGetResourcesV2:           func() error { return h.handleGetResourcesV2(c, body) },
		opGetResourcesStatisticsV2: func() error { return h.handleGetResourcesStatisticsV2(c, body) },
		opGetResourcesTrendsV2:     func() error { return h.handleGetResourcesTrendsV2(c, body) },
		// Products V2
		opDescribeProductsV2: func() error { return h.handleDescribeProductsV2(c) },
		// Recommended Policy V2
		opGenerateRecommendedPolicyV2: func() error { return h.handleGenerateRecommendedPolicyV2(c, resource, body) },
		opGetRecommendedPolicyV2:      func() error { return h.handleGetRecommendedPolicyV2(c, resource) },
	}

	if fn, ok := handlers[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{
		keyMessage: "unknown operation",
	})
}

// classifyPath maps (method, path) → (operation, resource).
func classifyPath(method, path string) (string, string) { //nolint:gocyclo,cyclop,funlen // existing issue.
	switch {
	case path == pathAccounts || strings.HasPrefix(path, pathAccounts+"/"):
		return classifyHubPath(method, path)
	case strings.HasPrefix(path, "/hubv2"):
		return classifyHubV2Path(method, path)
	case strings.HasPrefix(path, "/findingHistory") ||
		(strings.HasPrefix(path, "/findings") && !strings.HasPrefix(path, "/findingsv2") && !strings.HasPrefix(path, "/findingsTrendsv2") && !strings.HasPrefix(path, "/findingAggregator")): //nolint:lll // existing issue.
		return classifyFindingsPath(method, path)
	case strings.HasPrefix(path, "/findingsv2") || strings.HasPrefix(path, "/findingsTrendsv2"):
		return classifyFindingsV2Path(method, path)
	case strings.HasPrefix(path, "/resourcesv2") || strings.HasPrefix(path, "/resourcesTrendsv2"):
		return classifyResourcesV2Path(method, path)
	case strings.HasPrefix(path, "/insights"):
		return classifyInsightsPath(method, path)
	case strings.HasPrefix(path, "/standards") ||
		path == pathAssociations ||
		strings.HasPrefix(path, pathAssociations+"/"):
		return classifyStandardsPath(method, path)
	case strings.HasPrefix(path, "/actionTargets"):
		return classifyActionTargetsPath(method, path)
	case strings.HasPrefix(path, "/productsV2"):
		return classifyProductsV2Path(method, path)
	case strings.HasPrefix(path, "/products") || strings.HasPrefix(path, "/productSubscriptions"):
		return classifyProductsPath(method, path)
	case strings.HasPrefix(path, "/securityControl"):
		return classifySecurityControlsPath(method, path)
	case strings.HasPrefix(path, "/automationrulesv2"):
		return classifyAutomationRulesV2Path(method, path)
	case strings.HasPrefix(path, "/automationrules"):
		return classifyAutomationRulesPath(method, path)
	case strings.HasPrefix(path, "/members"):
		return classifyMembersPath(method, path)
	case strings.HasPrefix(path, "/invitations"):
		return classifyInvitationsPath(method, path)
	case strings.HasPrefix(path, "/administrator"):
		return classifyAdministratorPath(method, path)
	case strings.HasPrefix(path, "/master"):
		return classifyMasterPath(method, path)
	case strings.HasPrefix(path, "/organization"):
		return classifyOrganizationPath(method, path)
	case strings.HasPrefix(path, "/findingAggregator"):
		return classifyFindingAggregatorPath(method, path)
	case strings.HasPrefix(path, "/configurationPolicyAssociation"):
		return classifyConfigPolicyAssocPath(method, path)
	case strings.HasPrefix(path, "/configurationPolicy"):
		return classifyConfigPolicyPath(method, path)
	case strings.HasPrefix(path, "/aggregatorv2"):
		return classifyAggregatorV2Path(method, path)
	case strings.HasPrefix(path, "/connectorsv2"):
		return classifyConnectorsV2Path(method, path)
	case strings.HasPrefix(path, "/ticketsv2"):
		return classifyTicketsV2Path(method, path)
	case strings.HasPrefix(path, "/recommendedPolicyV2"):
		return classifyRecommendedPolicyV2Path(method, path)
	case strings.HasPrefix(path, "/tags/"):
		return classifyTagsPath(method, path)
	}

	return opUnknown, ""
}

func classifyHubPath(method, _ string) (string, string) {
	switch method {
	case http.MethodPost:
		return opEnableSecurityHub, ""
	case http.MethodDelete:
		return opDisableSecurityHub, ""
	case http.MethodGet:
		return opDescribeHub, ""
	case http.MethodPatch:
		return opUpdateSecurityHubCfg, ""
	}

	return opUnknown, ""
}

func classifyFindingsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findings":
		return opGetFindings, ""
	case method == http.MethodPost && path == "/findings/import":
		return opBatchImportFindings, ""
	case method == http.MethodPatch && path == "/findings/batchupdate":
		return opBatchUpdateFindings, ""
	case method == http.MethodPatch && path == "/findings":
		return opUpdateFindings, ""
	case method == http.MethodPost && path == "/findingHistory/get":
		return opGetFindingHistory, ""
	}

	return opUnknown, ""
}

func classifyInsightsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/insights":
		return opCreateInsight, ""
	case method == http.MethodPost && path == "/insights/get":
		return opGetInsights, ""
	case strings.HasPrefix(path, "/insights/results/") && method == http.MethodGet:
		return opGetInsightResults, strings.TrimPrefix(path, "/insights/results/")
	case strings.HasPrefix(path, "/insights/") && method == http.MethodPatch:
		return opUpdateInsight, strings.TrimPrefix(path, "/insights/")
	case strings.HasPrefix(path, "/insights/") && method == http.MethodDelete:
		return opDeleteInsight, strings.TrimPrefix(path, "/insights/")
	}

	return opUnknown, ""
}

func classifyStandardsPath(method, path string) (string, string) {
	if strings.HasPrefix(path, pathAssociations) {
		return classifyAssociationsPath(method, path)
	}

	switch {
	case method == http.MethodPost && path == "/standards/register":
		return opBatchEnableStandards, ""
	case method == http.MethodPost && path == "/standards/deregister":
		return opBatchDisableStandards, ""
	case method == http.MethodPost && path == "/standards/get":
		return opGetEnabledStandards, ""
	case method == http.MethodGet && path == "/standards":
		return opDescribeStandards, ""
	case strings.HasPrefix(path, "/standards/controls/") && method == http.MethodGet:
		return opDescribeStdControls, strings.TrimPrefix(path, "/standards/controls/")
	case strings.HasPrefix(path, "/standards/control/") && method == http.MethodPatch:
		return opUpdateStdControl, strings.TrimPrefix(path, "/standards/control/")
	}

	return opUnknown, ""
}

func classifyAssociationsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == pathAssociations:
		return opListStdCtlAssocs, ""
	case method == http.MethodPost && path == pathAssociations+"/batchGet":
		return opBatchGetStdCtlAssocs, ""
	case method == http.MethodPatch && path == pathAssociations:
		return opBatchUpdateStdCtlAssocs, ""
	}

	return opUnknown, ""
}

func classifyActionTargetsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/actionTargets":
		return opCreateActionTarget, ""
	case method == http.MethodPost && path == "/actionTargets/get":
		return opDescribeActionTargets, ""
	case strings.HasPrefix(path, "/actionTargets/") && method == http.MethodPatch:
		return opUpdateActionTarget, strings.TrimPrefix(path, "/actionTargets/")
	case strings.HasPrefix(path, "/actionTargets/") && method == http.MethodDelete:
		return opDeleteActionTarget, strings.TrimPrefix(path, "/actionTargets/")
	}

	return opUnknown, ""
}

func classifyProductsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/products":
		return opDescribeProducts, ""
	case method == http.MethodGet && path == "/productSubscriptions":
		return opListEnabledProductsForImport, ""
	case method == http.MethodPost && path == "/productSubscriptions":
		return opEnableImportFindingsForProduct, ""
	case strings.HasPrefix(path, "/productSubscriptions/") && method == http.MethodDelete:
		return opDisableImportFindingsForProduct, strings.TrimPrefix(path, "/productSubscriptions/")
	}

	return opUnknown, ""
}

func classifySecurityControlsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/securityControl/definition":
		return opGetSecurityControlDefinition, ""
	case method == http.MethodGet && path == "/securityControls/definitions":
		return opListSecurityControlDefinitions, ""
	case method == http.MethodPost && path == "/securityControls/batchGet":
		return opBatchGetSecurityControls, ""
	case method == http.MethodPatch && path == "/securityControl/update":
		return opUpdateSecurityControl, ""
	}

	return opUnknown, ""
}

func classifyAutomationRulesPath(method, path string) (string, string) {
	switch {
	case method == http.MethodGet && path == "/automationrules/list":
		return opListAutomationRules, ""
	case method == http.MethodPost && path == "/automationrules/create":
		return opCreateAutomationRule, ""
	case method == http.MethodPost && path == "/automationrules/get":
		return opBatchGetAutomationRules, ""
	case method == http.MethodPost && path == "/automationrules/delete":
		return opBatchDeleteAutoRules, ""
	case method == http.MethodPatch && path == "/automationrules/update":
		return opBatchUpdateAutoRules, ""
	}

	return opUnknown, ""
}

func classifyTagsPath(method, path string) (string, string) {
	resource := strings.TrimPrefix(path, "/tags/")

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resource
	case http.MethodPost:
		return opTagResource, resource
	case http.MethodDelete:
		return opUntagResource, resource
	}

	return opUnknown, ""
}

// --- Hub handlers ---

func (h *Handler) handleEnableHub(c *echo.Context, body map[string]any) error {
	enableDefault := true

	if v, ok := body["EnableDefaultStandards"].(bool); ok {
		enableDefault = v
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.EnableHub(enableDefault, tags); err != nil {
		if errors.Is(err, ErrHubAlreadyExists) {
			return c.JSON(http.StatusConflict, map[string]any{
				keyMessage: "SecurityHub is already enabled",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableHub(c *echo.Context) error {
	if err := h.Backend.DisableHub(); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeHub(c *echo.Context) error {
	hub, err := h.Backend.DescribeHub()
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: "SecurityHub is not subscribed",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"HubArn":                  hub.HubArn,
		"SubscribedAt":            hub.SubscribedAt,
		"AutoEnableControls":      hub.AutoEnableControls,
		"AutoEnableStandards":     hub.AutoEnableStandards,
		"ControlFindingGenerator": hub.ControlFindingGenerator,
	})
}

func (h *Handler) handleUpdateHubConfig(c *echo.Context, body map[string]any) error {
	var autoEnableControls *bool

	if v, ok := body["AutoEnableControls"].(bool); ok {
		autoEnableControls = &v
	}

	var autoEnableStandards *string

	if v, ok := body["AutoEnableStandards"].(string); ok {
		autoEnableStandards = &v
	}

	var controlFindingGenerator *string

	if v, ok := body["ControlFindingGenerator"].(string); ok {
		controlFindingGenerator = &v
	}

	err := h.Backend.UpdateHubConfiguration(autoEnableControls, autoEnableStandards, controlFindingGenerator)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Finding handlers ---

func (h *Handler) handleGetFindings(c *echo.Context, body map[string]any) error {
	filters, _ := body["Filters"].(map[string]any)
	sortCriteria, _ := body["SortCriteria"].([]any)
	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	var sortMaps []map[string]any

	for _, s := range sortCriteria {
		if m, ok := s.(map[string]any); ok {
			sortMaps = append(sortMaps, m)
		}
	}

	findings, nextOut := h.Backend.GetFindings(filters, sortMaps, nextToken, maxResults)

	resp := map[string]any{"Findings": findings}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchImportFindings(c *echo.Context, body map[string]any) error {
	rawFindings, _ := body["Findings"].([]any)

	var findings []map[string]any

	for _, f := range rawFindings {
		if m, ok := f.(map[string]any); ok {
			findings = append(findings, m)
		}
	}

	successCount, failedCount, failedFindings := h.Backend.ImportFindings(findings)

	return c.JSON(http.StatusOK, map[string]any{
		"SuccessCount":   successCount,
		"FailedCount":    failedCount,
		"FailedFindings": failedFindings,
	})
}

func (h *Handler) handleBatchUpdateFindings(c *echo.Context, body map[string]any) error {
	rawIdents, _ := body["FindingIdentifiers"].([]any)

	var identifiers []map[string]any

	for _, i := range rawIdents {
		if m, ok := i.(map[string]any); ok {
			identifiers = append(identifiers, m)
		}
	}

	// Collect updates (all body fields except FindingIdentifiers)
	updates := make(map[string]any)

	for k, v := range body {
		if k != "FindingIdentifiers" {
			updates[k] = v
		}
	}

	processed, unprocessed := h.Backend.BatchUpdateFindings(identifiers, updates)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedFindings":   processed,
		"UnprocessedFindings": unprocessed,
	})
}

func (h *Handler) handleUpdateFindings(c *echo.Context, body map[string]any) error {
	filters, _ := body["Filters"].(map[string]any)
	note, _ := body["Note"].(map[string]any)
	recordState, _ := body["RecordState"].(string)

	if err := h.Backend.UpdateFindings(filters, note, recordState); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetFindingHistory(c *echo.Context, body map[string]any) error {
	ident, _ := body["FindingIdentifier"].(map[string]any)
	startTime, _ := body["StartTime"].(string)
	endTime, _ := body["EndTime"].(string)
	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	records, nextOut := h.Backend.GetFindingHistory(ident, startTime, endTime, nextToken, maxResults)

	resp := map[string]any{"Records": records}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Insight handlers ---

func (h *Handler) handleCreateInsight(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	filters, _ := body["Filters"].(map[string]any)

	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "Name is required",
		})
	}

	if groupByAttribute == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: "GroupByAttribute is required",
		})
	}

	arn, err := h.Backend.CreateInsight(name, groupByAttribute, filters)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyInsightArn: arn})
}

func (h *Handler) handleGetInsights(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["InsightArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	insights, nextOut, err := h.Backend.GetInsights(arns, nextToken, maxResults)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	items := make([]map[string]any, len(insights))

	for i, ins := range insights {
		items[i] = map[string]any{
			keyInsightArn:      ins.InsightArn,
			keyName:            ins.Name,
			"GroupByAttribute": ins.GroupByAttribute, //nolint:goconst // existing issue.
			"Filters":          ins.Filters,
		}
	}

	resp := map[string]any{"Insights": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetInsightResults(c *echo.Context, insightArn string) error {
	results, err := h.Backend.GetInsightResults(insightArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{
				keyMessage: msgInsightNotFound,
			})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"InsightResults": map[string]any{
			keyInsightArn:      results.InsightArn,
			"GroupByAttribute": results.GroupByAttribute,
			"ResultValues":     results.ResultValues,
		},
	})
}

func (h *Handler) handleUpdateInsight(c *echo.Context, insightArn string, body map[string]any) error {
	name, _ := body["Name"].(string)
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	filters, _ := body["Filters"].(map[string]any)

	if err := h.Backend.UpdateInsight(insightArn, name, groupByAttribute, filters); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgInsightNotFound})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteInsight(c *echo.Context, insightArn string) error {
	deletedArn, err := h.Backend.DeleteInsight(insightArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgInsightNotFound})
		}

		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyInsightArn: deletedArn})
}

// --- Standards handlers ---

func (h *Handler) handleBatchEnableStandards(c *echo.Context, body map[string]any) error {
	rawRequests, _ := body["StandardsSubscriptionRequests"].([]any)

	var requests []map[string]any

	for _, r := range rawRequests {
		if m, ok := r.(map[string]any); ok {
			requests = append(requests, m)
		}
	}

	subscriptions, _ := h.Backend.BatchEnableStandards(requests)
	items := standardsSubscriptionsToMaps(subscriptions)

	return c.JSON(http.StatusOK, map[string]any{
		keyStandardsSubscriptions: items,
	})
}

func (h *Handler) handleBatchDisableStandards(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["StandardsSubscriptionArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	subscriptions, _ := h.Backend.BatchDisableStandards(arns)
	items := standardsSubscriptionsToMaps(subscriptions)

	return c.JSON(http.StatusOK, map[string]any{
		keyStandardsSubscriptions: items,
	})
}

func (h *Handler) handleGetEnabledStandards(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["StandardsSubscriptionArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	subscriptions, nextOut := h.Backend.GetEnabledStandards(arns, nextToken, maxResults)
	items := standardsSubscriptionsToMaps(subscriptions)

	resp := map[string]any{keyStandardsSubscriptions: items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func standardsSubscriptionsToMaps(subs []*StandardsSubscription) []map[string]any {
	items := make([]map[string]any, len(subs))

	for i, s := range subs {
		items[i] = map[string]any{
			"StandardsSubscriptionArn": s.StandardsSubscriptionArn,
			keyStandardsArn:            s.StandardsArn,
			"StandardsInput":           s.StandardsInput,
			"StandardsStatus":          s.StandardsStatus,
		}
	}

	return items
}

func (h *Handler) handleDescribeStandards(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	standards, nextOut := h.Backend.DescribeStandards(nextToken, maxResults)
	items := make([]map[string]any, len(standards))

	for i, s := range standards {
		items[i] = map[string]any{
			keyStandardsArn:    s.StandardsArn,
			keyName:            s.Name,
			keyDescription:     s.Description,
			"EnabledByDefault": s.EnabledByDefault,
		}
	}

	resp := map[string]any{"Standards": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeStandardsControls(c *echo.Context, subscriptionArn string) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	controls, nextOut := h.Backend.DescribeStandardsControls(subscriptionArn, nextToken, maxResults)
	items := make([]map[string]any, len(controls))

	for i, ctrl := range controls {
		items[i] = map[string]any{
			"StandardsControlArn":    ctrl.StandardsControlArn,
			"ControlStatus":          ctrl.ControlStatus,
			"DisabledReason":         ctrl.DisabledReason,
			"ControlStatusUpdatedAt": ctrl.ControlStatusUpdatedAt,
			"ControlId":              ctrl.ControlID,
			keyTitle:                 ctrl.Title,
			keyDescription:           ctrl.Description,
			keyRemediationURL:        ctrl.RemediationURL,
			keySeverityRating:        ctrl.SeverityRating,
			"RelatedRequirements":    ctrl.RelatedRequirements,
		}
	}

	resp := map[string]any{"Controls": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateStandardsControl(c *echo.Context, controlArn string, body map[string]any) error {
	controlStatus, _ := body["ControlStatus"].(string)
	disabledReason, _ := body["DisabledReason"].(string)

	if err := h.Backend.UpdateStandardsControl(controlArn, controlStatus, disabledReason); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListStandardsControlAssociations(c *echo.Context) error {
	secCtlID := c.QueryParam("SecurityControlId")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	assocs, nextOut := h.Backend.ListStandardsControlAssociations(secCtlID, nextToken, maxResults)
	items := standardsControlAssociationsToMaps(assocs)

	resp := map[string]any{"StandardsControlAssociationSummaries": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchGetStdCtlAssociations(c *echo.Context, body map[string]any) error {
	rawRequests, _ := body["StandardsControlAssociationIds"].([]any)

	var requests []map[string]any

	for _, r := range rawRequests {
		if m, ok := r.(map[string]any); ok {
			requests = append(requests, m)
		}
	}

	assocs, unprocessed := h.Backend.BatchGetStandardsControlAssociations(requests)
	items := standardsControlAssociationsToMaps(assocs)

	return c.JSON(http.StatusOK, map[string]any{
		"StandardsControlAssociationDetails": items,
		"UnprocessedAssociations":            unprocessed,
	})
}

func (h *Handler) handleBatchUpdateStdCtlAssociations(c *echo.Context, body map[string]any) error {
	rawUpdates, _ := body["StandardsControlAssociationUpdates"].([]any)

	var updates []map[string]any

	for _, u := range rawUpdates {
		if m, ok := u.(map[string]any); ok {
			updates = append(updates, m)
		}
	}

	unprocessed, err := h.Backend.BatchUpdateStandardsControlAssociations(updates)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"UnprocessedAssociationUpdates": unprocessed,
	})
}

func standardsControlAssociationsToMaps(assocs []*StandardsControlAssociation) []map[string]any {
	items := make([]map[string]any, len(assocs))

	for i, a := range assocs {
		items[i] = map[string]any{
			keySecurityControlID: a.SecurityControlID,
			keyStandardsArn:      a.StandardsArn,
			"AssociationStatus":  a.AssociationStatus,
			keyUpdatedAt:         a.UpdatedAt,
		}
	}

	return items
}

// --- Action Target handlers ---

func (h *Handler) handleCreateActionTarget(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)
	id, _ := body["Id"].(string)

	arn, err := h.Backend.CreateActionTarget(name, description, id)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusConflict, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyActionTargetArn: arn})
}

func (h *Handler) handleDescribeActionTargets(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["ActionTargetArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	targets, nextOut := h.Backend.DescribeActionTargets(arns, nextToken, maxResults)
	items := make([]map[string]any, len(targets))

	for i, t := range targets {
		items[i] = map[string]any{
			keyActionTargetArn: t.ActionTargetArn,
			keyName:            t.Name,
			keyDescription:     t.Description,
		}
	}

	resp := map[string]any{"ActionTargets": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateActionTarget(c *echo.Context, actionTargetArn string, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	if err := h.Backend.UpdateActionTarget(actionTargetArn, name, description); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "ActionTarget not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteActionTarget(c *echo.Context, actionTargetArn string) error {
	deletedArn, err := h.Backend.DeleteActionTarget(actionTargetArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "ActionTarget not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyActionTargetArn: deletedArn})
}

// --- Product handlers ---

func (h *Handler) handleDescribeProducts(c *echo.Context) error {
	productArn := c.QueryParam("ProductArn")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	products, nextOut := h.Backend.DescribeProducts(productArn, nextToken, maxResults)
	items := make([]map[string]any, len(products))

	for i, p := range products {
		items[i] = map[string]any{
			"ProductArn":       p.ProductArn, //nolint:goconst // existing issue.
			"ProductName":      p.ProductName,
			"CompanyName":      p.CompanyName,
			keyDescription:     p.Description,
			"Categories":       p.Categories,
			"IntegrationTypes": p.IntegrationTypes,
			"MarketplaceUrl":   p.MarketplaceURL,
			"ActivationUrl":    p.ActivationURL,
		}
	}

	resp := map[string]any{"Products": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListEnabledProductsForImport(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	arns, nextOut := h.Backend.ListEnabledProductsForImport(nextToken, maxResults)

	resp := map[string]any{"ProductSubscriptions": arns}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleEnableImportFindingsForProduct(c *echo.Context, body map[string]any) error {
	productArn, _ := body["ProductArn"].(string)

	subArn, err := h.Backend.EnableImportFindingsForProduct(productArn)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"ProductSubscriptionArn": subArn})
}

func (h *Handler) handleDisableImportFindingsForProduct(c *echo.Context, productSubscriptionArn string) error {
	if err := h.Backend.DisableImportFindingsForProduct(productSubscriptionArn); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Product subscription not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Security Control handlers ---

func (h *Handler) handleGetSecurityControlDefinition(c *echo.Context) error {
	secCtlID := c.QueryParam("SecurityControlId")

	def, err := h.Backend.GetSecurityControlDefinition(secCtlID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "Security control not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityControlDefinition": securityControlDefinitionToMap(def),
	})
}

func (h *Handler) handleListSecurityControlDefinitions(c *echo.Context) error {
	standardsArn := c.QueryParam("StandardsArn")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	defs, nextOut := h.Backend.ListSecurityControlDefinitions(standardsArn, nextToken, maxResults)
	items := make([]map[string]any, len(defs))

	for i, d := range defs {
		items[i] = securityControlDefinitionToMap(d)
	}

	resp := map[string]any{"SecurityControlDefinitions": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func securityControlDefinitionToMap(d *SecurityControlDefinition) map[string]any {
	return map[string]any{
		keySecurityControlID:        d.SecurityControlID,
		keyTitle:                    d.Title,
		keyDescription:              d.Description,
		keyRemediationURL:           d.RemediationURL,
		keySeverityRating:           d.SeverityRating,
		"CurrentRegionAvailability": d.CurrentRegionAvailability,
		"CustomizableProperties":    d.CustomizableProperties,
		"ParameterDefinitions":      d.ParameterDefinitions,
	}
}

func (h *Handler) handleBatchGetSecurityControls(c *echo.Context, body map[string]any) error {
	rawIDs, _ := body["SecurityControlIds"].([]any)

	var ids []string

	for _, id := range rawIDs {
		if s, ok := id.(string); ok {
			ids = append(ids, s)
		}
	}

	controls, unprocessed := h.Backend.BatchGetSecurityControls(ids)
	items := make([]map[string]any, len(controls))

	for i, ctrl := range controls {
		items[i] = map[string]any{
			keySecurityControlID:    ctrl.SecurityControlID,
			"SecurityControlArn":    ctrl.SecurityControlArn,
			keyTitle:                ctrl.Title,
			keyDescription:          ctrl.Description,
			keyRemediationURL:       ctrl.RemediationURL,
			keySeverityRating:       ctrl.SeverityRating,
			"SecurityControlStatus": ctrl.SecurityControlStatus,
			"UpdateStatus":          ctrl.UpdateStatus,
			"Parameters":            ctrl.Parameters,
			"LastUpdateReason":      ctrl.LastUpdateReason,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityControls": items,
		"UnprocessedIds":   unprocessed,
	})
}

func (h *Handler) handleUpdateSecurityControl(c *echo.Context, body map[string]any) error {
	secCtlID, _ := body["SecurityControlId"].(string)
	parameters, _ := body["Parameters"].(map[string]any)
	lastUpdateReason, _ := body["LastUpdateReason"].(string)

	if err := h.Backend.UpdateSecurityControl(secCtlID, parameters, lastUpdateReason); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Automation Rule handlers ---

func (h *Handler) handleListAutomationRules(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	rules, nextOut := h.Backend.ListAutomationRules(nextToken, maxResults)
	items := make([]map[string]any, len(rules))

	for i, r := range rules {
		items[i] = map[string]any{
			keyRuleArn:     r.RuleArn,
			"RuleStatus":   r.RuleStatus, //nolint:goconst // existing issue.
			"RuleOrder":    r.RuleOrder,  //nolint:goconst // existing issue.
			"RuleName":     r.RuleName,   //nolint:goconst // existing issue.
			keyDescription: r.Description,
			"IsTerminal":   r.IsTerminal, //nolint:goconst // existing issue.
			keyCreatedAt:   r.CreatedAt,
			keyUpdatedAt:   r.UpdatedAt,
			"CreatedBy":    r.CreatedBy,
		}
	}

	resp := map[string]any{"AutomationRulesMetadata": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateAutomationRule(c *echo.Context, body map[string]any) error {
	ruleArn, createdAt := h.Backend.CreateAutomationRule(body)

	return c.JSON(http.StatusOK, map[string]any{
		keyRuleArn:   ruleArn,
		keyCreatedAt: createdAt,
	})
}

func (h *Handler) handleBatchGetAutomationRules(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["AutomationRulesArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	rules, unprocessed := h.Backend.BatchGetAutomationRules(arns)
	items := make([]map[string]any, len(rules))

	for i, r := range rules {
		items[i] = map[string]any{
			keyRuleArn:     r.RuleArn,
			"RuleStatus":   r.RuleStatus,
			"RuleOrder":    r.RuleOrder,
			"RuleName":     r.RuleName,
			keyDescription: r.Description,
			"IsTerminal":   r.IsTerminal,
			keyCreatedAt:   r.CreatedAt,
			keyUpdatedAt:   r.UpdatedAt,
			"CreatedBy":    r.CreatedBy,
			"Criteria":     r.Criteria,
			"Actions":      r.Actions,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Rules":                 items,
		keyUnprocessedAutoRules: unprocessed,
	})
}

func (h *Handler) handleBatchDeleteAutomationRules(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["AutomationRulesArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	deleted, unprocessed := h.Backend.BatchDeleteAutomationRules(arns)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedAutomationRules": deleted,
		keyUnprocessedAutoRules:    unprocessed,
	})
}

func (h *Handler) handleBatchUpdateAutomationRules(c *echo.Context, body map[string]any) error {
	rawUpdates, _ := body["UpdateAutomationRulesRequestItems"].([]any)

	var updates []map[string]any

	for _, u := range rawUpdates {
		if m, ok := u.(map[string]any); ok {
			updates = append(updates, m)
		}
	}

	processed, unprocessed := h.Backend.BatchUpdateAutomationRules(updates)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedAutomationRules": processed,
		keyUnprocessedAutoRules:    unprocessed,
	})
}

// --- Tag handlers ---

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body map[string]any) error {
	tags := make(map[string]string)

	if t, ok := body["Tags"].(map[string]any); ok {
		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.TagResource(resourceArn, tags); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	rawKeys := c.QueryParams()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, rawKeys); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Helpers ---

func intFromBody(body map[string]any) int {
	if v, ok := body[keyMaxResults].(float64); ok {
		return int(v)
	}

	return 0
}

func queryInt(c *echo.Context) int {
	v, err := strconv.Atoi(c.QueryParam(keyMaxResults))
	if err != nil {
		return 0
	}

	return v
}
