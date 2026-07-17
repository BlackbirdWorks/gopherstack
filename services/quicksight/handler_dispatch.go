package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) dispatch(c *echo.Context) error {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)
	switch {
	case isNamespaceOp(op):
		return h.dispatchNamespace(c, op)
	case isGroupOp(op):
		return h.dispatchGroup(c, op)
	case isUserOp(op):
		return h.dispatchUser(c, op)
	case isDataSourceOp(op):
		return h.dispatchDataSource(c, op)
	case isDataSetOp(op):
		return h.dispatchDataSet(c, op)
	case isDashboardOp(op):
		return h.dispatchDashboard(c, op)
	case isAnalysisOp(op):
		return h.dispatchAnalysis(c, op)
	case isTagOp(op):
		return h.dispatchTag(c, op)
	case isFolderOp(op):
		return h.dispatchFolder(c, op)
	case isTemplateOp(op):
		return h.dispatchTemplate(c, op)
	case isThemeOp(op):
		return h.dispatchTheme(c, op)
	case isTopicFamilyOp(op):
		return h.dispatchTopicFamily(c, op)
	case op != opUnknown:
		return h.dispatchNew(c, op)
	default:
		return writeError(
			c,
			http.StatusNotImplemented,
			"UnsupportedOperationException",
			fmt.Sprintf("operation %q not implemented", op),
		)
	}
}

// isTopicFamilyOp reports whether op is one of the Topic, VPC Connection, IAM
// Policy Assignment, Brand, Custom Permissions/Role/User-permission, OAuth app,
// Identity Propagation, Asset Bundle/Dashboard Snapshot job, DataSet Refresh
// Schedule/Properties, or Embed URL operations. These otherwise-unrelated
// families are grouped behind a single dispatch() case (routed on to
// dispatchTopicFamily) purely to keep dispatch's cyclomatic complexity in budget.
func isTopicFamilyOp(op string) bool {
	return isTopicOp(op) || isVPCConnectionOp(op) || isIAMPolicyAssignmentOp(op) ||
		isBrandOp(op) || isCustomPermOp(op) || isOAuthOp(op) || isIdentityPropOp(op) ||
		isAssetBundleOp(op) || isRefreshScheduleOp(op) || isEmbedURLOp(op) ||
		isResourceSearchOp(op) || op == opListFoldersForResource || isAccountCustomPermOp(op) ||
		isFinalStubOp(op)
}

// isFinalStubOp reports whether op is one of the Action Connector, Automation
// Job, Flow, or namespace Self-Upgrade operations — the last Appendix-A
// canned-stub families to gain real backend implementations. Grouped behind
// one predicate/dispatch pair purely to keep isTopicFamilyOp/
// dispatchTopicFamily's complexity in budget.
func isFinalStubOp(op string) bool {
	return isActionConnectorOp(op) || isAutomationJobOp(op) || isFlowOp(op) || isSelfUpgradeOp(op)
}

func (h *Handler) dispatchFinalStub(c *echo.Context, op string) error {
	switch {
	case isActionConnectorOp(op):
		return h.dispatchActionConnector(c, op)
	case isAutomationJobOp(op):
		return h.dispatchAutomationJob(c, op)
	case isFlowOp(op):
		return h.dispatchFlow(c, op)
	case isSelfUpgradeOp(op):
		return h.dispatchSelfUpgrade(c, op)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

func (h *Handler) dispatchTopicFamily(c *echo.Context, op string) error {
	switch {
	case isTopicOp(op):
		return h.dispatchTopic(c, op)
	case isVPCConnectionOp(op):
		return h.dispatchVPCConnection(c, op)
	case isIAMPolicyAssignmentOp(op):
		return h.dispatchIAMPolicyAssignment(c, op)
	case isBrandOp(op):
		return h.dispatchBrand(c, op)
	case isCustomPermOp(op):
		return h.dispatchCustomPerm(c, op)
	case isOAuthOp(op):
		return h.dispatchOAuth(c, op)
	case isIdentityPropOp(op):
		return h.dispatchIdentityProp(c, op)
	case isAssetBundleOp(op):
		return h.dispatchAssetBundle(c, op)
	case isRefreshScheduleOp(op):
		return h.dispatchRefreshSchedule(c, op)
	case isEmbedURLOp(op):
		return h.dispatchEmbedURL(c, op)
	case isResourceSearchOp(op):
		return h.dispatchResourceSearch(c, op)
	case op == opListFoldersForResource:
		return h.handleListFoldersForResource(c)
	case isAccountCustomPermOp(op):
		return h.dispatchAccountCustomPerm(c, op)
	case isFinalStubOp(op):
		return h.dispatchFinalStub(c, op)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// appendixHandlerFn is a function that builds the response body for one Appendix-A op.
type appendixHandlerFn func(resID, subID string) map[string]any

// dispatchNew handles all Appendix-A operations that are not dispatched by the
// legacy type-specific helpers (namespace/group/user/datasource/dataset/dashboard/analysis/tag).
// Stateful resource families (folders/templates/themes/vpc-connections/brands) are
// backed by real state too, but dispatch() routes those op names to their own
// dispatchFolder/dispatchTemplate/dispatchTheme/dispatchVPCConnection/dispatchBrand
// helpers before dispatchNew is ever reached, so they are not repeated here.
// Account/config-cluster operations are likewise real (backed by InMemoryBackend)
// and are routed to dispatchAccountConfig. GetIdentityContext and PredictQAResults
// are likewise real but (unlike the canned ops) need the request body, so they are
// special-cased here too, ahead of the canned appendixOps table.
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
	if isAccountConfigOp(op) {
		return h.dispatchAccountConfig(c, op)
	}
	if op == opGetIdentityContext {
		return h.handleGetIdentityContext(c)
	}
	if op == opPredictQAResults {
		return h.handlePredictQAResults(c)
	}

	segs := pathSegsFromCtx(c)
	resID := seg(segs, segResID)
	subID := seg(segs, segSubResID)

	if fn, ok := h.appendixOps[op]; ok {
		return writeJSON(c, http.StatusOK, fn(resID, subID))
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException",
		"operation not implemented: "+op)
}

// buildAppendixOps returns the full op→handler map for all Appendix-A operations.
// It contains no branches, so cyclomatic complexity is 1.
func buildAppendixOps() map[string]appendixHandlerFn {
	reqID := func(extra map[string]any) map[string]any {
		extra[keyRequestID] = newReqID()

		return extra
	}
	simple := func() map[string]any { return reqID(map[string]any{}) }
	noContent := func(_, _ string) map[string]any { return simple() }

	return map[string]appendixHandlerFn{
		// ---- Folders ----
		// CreateFolder, DescribeFolder, UpdateFolder, DeleteFolder, ListFolders,
		// SearchFolders, folder memberships, and folder permissions are real
		// (backed by InMemoryBackend) and routed via dispatchFolder in
		// handler_folders.go, not through this canned table.
		// ListFoldersForResource is real (backed by InMemoryBackend) and routed via
		// dispatchTopicFamily -> handleListFoldersForResource, not through this
		// canned table.

		// ---- Templates ----
		// CreateTemplate, DescribeTemplate, DescribeTemplateDefinition, UpdateTemplate,
		// DeleteTemplate, ListTemplates, ListTemplateVersions, template permissions,
		// and template aliases are real (backed by InMemoryBackend) and routed via
		// dispatchTemplate in handler_templates.go, not through this canned table.

		// ---- Themes ----
		// CreateTheme, DescribeTheme, UpdateTheme, DeleteTheme, ListThemes,
		// ListThemeVersions, theme permissions, and theme aliases are real (backed
		// by InMemoryBackend) and routed via dispatchTheme in handler_themes.go,
		// not through this canned table.

		// ---- Topics ----
		// CreateTopic, DescribeTopic, UpdateTopic, DeleteTopic, ListTopics, topic
		// permissions, topic refresh/refresh-schedules, and reviewed answers are
		// real (backed by InMemoryBackend) and routed via dispatchTopic in
		// handler_topics.go, not through this canned table. SearchTopics is real
		// (backed by InMemoryBackend) and routed via dispatchResourceSearch below,
		// not through this canned table.

		// ---- VPC Connections ----
		// CreateVPCConnection, DescribeVPCConnection, UpdateVPCConnection,
		// DeleteVPCConnection, and ListVPCConnections are real (backed by
		// InMemoryBackend) and routed via dispatchVPCConnection in
		// handler_vpcconnections.go, not through this canned table.

		// ---- IAM Policy Assignments ----
		// CreateIAMPolicyAssignment, DescribeIAMPolicyAssignment,
		// UpdateIAMPolicyAssignment, DeleteIAMPolicyAssignment,
		// ListIAMPolicyAssignments, and ListIAMPolicyAssignmentsForUser are real
		// (backed by InMemoryBackend) and routed via dispatchIAMPolicyAssignment
		// in handler_iampolicyassignments.go, not through this canned table.

		// ---- Custom Permissions ----
		// CreateCustomPermissions, DescribeCustomPermissions, UpdateCustomPermissions,
		// DeleteCustomPermissions, and ListCustomPermissions are real (backed by
		// InMemoryBackend) and routed via dispatchCustomPerm in
		// handler_custompermissions.go, not through this canned table.

		// ---- Role Memberships / Role & User Custom Permission ----
		// CreateRoleMembership, DeleteRoleMembership, ListRoleMemberships,
		// DescribeRoleCustomPermission, UpdateRoleCustomPermission,
		// DeleteRoleCustomPermission, UpdateUserCustomPermission, and
		// DeleteUserCustomPermission are real (backed by InMemoryBackend) and routed
		// via dispatchCustomPerm in handler_custompermissions.go, not through this
		// canned table.

		// ---- Dashboard Extras ----
		// DescribeDashboardDefinition, DescribeDashboardPermissions,
		// UpdateDashboardPermissions, UpdateDashboardPublishedVersion, and
		// UpdateDashboardLinks are real (backed by InMemoryBackend) and routed via
		// dispatchDashboard in handler.go, not through this canned table.
		// StartDashboardSnapshotJob, StartDashboardSnapshotJobSchedule,
		// DescribeDashboardSnapshotJob, and DescribeDashboardSnapshotJobResult are
		// real (backed by InMemoryBackend) and routed via dispatchDashboardSnapshot
		// in handler_assetbundle.go, not through this canned table.
		// GetDashboardEmbedUrl is real (backed by InMemoryBackend) and routed via
		// dispatchEmbedURL in handler_embedurl.go, not through this canned table.
		// DescribeDashboardsQAConfiguration and UpdateDashboardsQAConfiguration are
		// real (backed by InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Analysis Extras ----
		// DescribeAnalysisDefinition, DescribeAnalysisPermissions, and
		// UpdateAnalysisPermissions are real (backed by InMemoryBackend) and routed
		// via dispatchAnalysis in handler.go, not through this canned table.

		// ---- Data Set Extras ----
		// DescribeDataSetPermissions and UpdateDataSetPermissions are real (backed by
		// InMemoryBackend) and routed via dispatchDataSet in handler.go, not through
		// this canned table.
		// CreateRefreshSchedule, DescribeRefreshSchedule, UpdateRefreshSchedule,
		// DeleteRefreshSchedule, ListRefreshSchedules, PutDataSetRefreshProperties,
		// DescribeDataSetRefreshProperties, and DeleteDataSetRefreshProperties are
		// real (backed by InMemoryBackend) and routed via dispatchRefreshSchedule in
		// handler_refreshschedule.go, not through this canned table.

		// ---- Data Source Extras ----
		// DescribeDataSourcePermissions and UpdateDataSourcePermissions are real
		// (backed by InMemoryBackend) and routed via dispatchDataSource in
		// handler.go, not through this canned table.

		// ---- Brands ----
		// CreateBrand, DescribeBrand, UpdateBrand, DeleteBrand, ListBrands,
		// DescribeBrandAssignment, UpdateBrandAssignment, DeleteBrandAssignment,
		// DescribeBrandPublishedVersion, and UpdateBrandPublishedVersion are real
		// (backed by InMemoryBackend) and routed via dispatchBrand in
		// handler_brands.go, not through this canned table.

		// ---- OAuth Client Apps ----
		// CreateOAuthClientApplication, DescribeOAuthClientApplication,
		// UpdateOAuthClientApplication, DeleteOAuthClientApplication, and
		// ListOAuthClientApplications are real (backed by InMemoryBackend) and routed
		// via dispatchOAuth in handler_oauth.go, not through this canned table.

		// ---- Action Connectors ----
		// CreateActionConnector, DescribeActionConnector, UpdateActionConnector,
		// DeleteActionConnector, ListActionConnectors, SearchActionConnectors,
		// DescribeActionConnectorPermissions, and UpdateActionConnectorPermissions
		// are real (backed by InMemoryBackend) and routed via dispatchActionConnector
		// in handler_actionconnector.go, not through this canned table.

		// ---- Identity Propagation ----
		// ListIdentityPropagationConfigs, UpdateIdentityPropagationConfig, and
		// DeleteIdentityPropagationConfig are real (backed by InMemoryBackend) and
		// routed via dispatchIdentityProp in handler_identitypropagation.go, not
		// through this canned table.

		// ---- Asset Bundle ----
		// StartAssetBundleExportJob, DescribeAssetBundleExportJob,
		// ListAssetBundleExportJobs, StartAssetBundleImportJob,
		// DescribeAssetBundleImportJob, and ListAssetBundleImportJobs are real
		// (backed by InMemoryBackend) and routed via dispatchAssetBundle in
		// handler_assetbundle.go, not through this canned table.

		// ---- Automation ----
		// StartAutomationJob and DescribeAutomationJob are real (backed by
		// InMemoryBackend) and routed via dispatchAutomationJob in
		// handler_automation.go, not through this canned table.

		// ---- Account Customization ----
		// CreateAccountCustomization, DescribeAccountCustomization,
		// UpdateAccountCustomization, and DeleteAccountCustomization are real
		// (backed by InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Account Custom Permission ----
		// DescribeAccountCustomPermission, UpdateAccountCustomPermission, and
		// DeleteAccountCustomPermission are real (backed by InMemoryBackend) and
		// routed via dispatchTopicFamily -> dispatchAccountCustomPerm, not through
		// this canned table.

		// ---- Account Settings ----
		// DescribeAccountSettings and UpdateAccountSettings are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Account Subscription ----
		// CreateAccountSubscription, DescribeAccountSubscription, and
		// DeleteAccountSubscription are real (backed by InMemoryBackend) and
		// routed via dispatchAccountConfig in handler_account.go, not through
		// this canned table.

		// ---- IP Restriction ----
		// DescribeIpRestriction and UpdateIpRestriction are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Key Registration ----
		// DescribeKeyRegistration and UpdateKeyRegistration are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Public Sharing ----
		// UpdatePublicSharingSettings is real (backed by InMemoryBackend) and
		// routed via dispatchAccountConfig in handler_account.go, not through
		// this canned table.

		// ---- Q Personalization ----
		// DescribeQPersonalizationConfiguration and
		// UpdateQPersonalizationConfiguration are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- Q Search Config ----
		// DescribeQuickSightQSearchConfiguration and
		// UpdateQuickSightQSearchConfiguration are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- SPICE Capacity ----
		// UpdateSPICECapacityConfiguration is real (backed by InMemoryBackend) and
		// routed via dispatchAccountConfig in handler_account.go, not through this
		// canned table.

		// ---- Default QBiz ----
		// DescribeDefaultQBusinessApplication, UpdateDefaultQBusinessApplication,
		// and DeleteDefaultQBusinessApplication are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- App Token Grant ----
		// UpdateApplicationWithTokenExchangeGrant is a genuinely void-result op:
		// its real output (UpdateApplicationWithTokenExchangeGrantOutput) carries
		// only the RequestId/Status envelope, and the SDK defines no corresponding
		// Describe/Get op to make the grant observable — there is no state for
		// this backend to track beyond acknowledging the request, so the canned
		// envelope-only response below is the correct (not fabricated) shape.
		opUpdateAppTokenGrant: noContent,

		// ---- Identity Context ----
		// GetIdentityContext is real (mints an opaque identity-context token) and
		// routed via dispatchNew's op==opGetIdentityContext special-case in
		// dispatchNew, not through this canned table.

		// ---- Predict QA ----
		// PredictQAResults is real (grounds its answer in the account's actual
		// Topics, backed by InMemoryBackend) and routed via dispatchNew's
		// op==opPredictQAResults special-case, not through this canned table.

		// ---- Embed URLs ----
		// GenerateEmbedUrlForAnonymousUser, GenerateEmbedUrlForRegisteredUser,
		// GenerateEmbedUrlForRegisteredUserWithIdentity, GetDashboardEmbedUrl, and
		// GetSessionEmbedUrl are real (backed by InMemoryBackend) and routed via
		// dispatchEmbedURL in handler_embedurl.go, not through this canned table.

		// ---- Search ----
		// SearchAnalyses, SearchDashboards, SearchDataSets, SearchDataSources, and
		// SearchTopics are real (backed by InMemoryBackend) and routed via
		// dispatchTopicFamily -> dispatchResourceSearch, not through this canned
		// table.

		// ---- Flows ----
		// ListFlows, SearchFlows, GetFlowMetadata, GetFlowPermissions, and
		// UpdateFlowPermissions are real (backed by InMemoryBackend) and routed via
		// dispatchFlow in handler_flow.go, not through this canned table.

		// ---- Namespace Self-Upgrade ----
		// DescribeSelfUpgradeConfiguration, UpdateSelfUpgradeConfiguration,
		// ListSelfUpgrades, and UpdateSelfUpgrade are real (backed by
		// InMemoryBackend) and routed via dispatchSelfUpgrade in
		// handler_selfupgrade.go, not through this canned table.
	}
}

// ---- Search{Analyses,Dashboards,DataSets,DataSources,Topics} ----

// isResourceSearchOp reports whether op is one of the Search{Analyses,
// Dashboards,DataSets,DataSources,Topics} operations (SearchFolders is handled
// separately, by dispatchFolder).
func isResourceSearchOp(op string) bool {
	switch op {
	case opSearchAnalyses, opSearchDashboards, opSearchDataSets, opSearchDataSources, opSearchTopics:
		return true
	}

	return false
}

func (h *Handler) dispatchResourceSearch(c *echo.Context, op string) error {
	switch op {
	case opSearchAnalyses:
		return h.handleSearchAnalyses(c)
	case opSearchDashboards:
		return h.handleSearchDashboards(c)
	case opSearchDataSets:
		return h.handleSearchDataSets(c)
	case opSearchDataSources:
		return h.handleSearchDataSources(c)
	case opSearchTopics:
		return h.handleSearchTopics(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}
