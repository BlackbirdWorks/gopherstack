package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// appendixHandlerFn is a function that builds the response body for one Appendix-A op.
type appendixHandlerFn func(resID, subID string) map[string]any

// buildStatefulAppendixOps returns the op→handler map for Appendix-A operations
// backed by real backend state (Create persists, Describe/List reflect state,
// Update mutates, Delete removes). It is a flat map literal, so its cyclomatic
// complexity is 1.
func (h *Handler) buildStatefulAppendixOps() map[string]echo.HandlerFunc {
	return map[string]echo.HandlerFunc{
		// ---- Folders ----
		opCreateFolder:   h.handleCreateFolder,
		opDescribeFolder: h.handleDescribeFolder,
		opUpdateFolder:   h.handleUpdateFolder,
		opDeleteFolder:   h.handleDeleteFolder,
		opListFolders:    h.handleListFolders,
		// ---- Templates ----
		opCreateTemplate:   h.handleCreateTemplate,
		opDescribeTemplate: h.handleDescribeTemplate,
		opUpdateTemplate:   h.handleUpdateTemplate,
		opDeleteTemplate:   h.handleDeleteTemplate,
		opListTemplates:    h.handleListTemplates,
		// ---- Themes ----
		opCreateTheme:   h.handleCreateTheme,
		opDescribeTheme: h.handleDescribeTheme,
		opUpdateTheme:   h.handleUpdateTheme,
		opDeleteTheme:   h.handleDeleteTheme,
		opListThemes:    h.handleListThemes,
		// ---- VPC Connections ----
		opCreateVPCConnection:   h.handleCreateVPCConnection,
		opDescribeVPCConnection: h.handleDescribeVPCConnection,
		opUpdateVPCConnection:   h.handleUpdateVPCConnection,
		opDeleteVPCConnection:   h.handleDeleteVPCConnection,
		opListVPCConnections:    h.handleListVPCConnections,
		// ---- Brands ----
		opCreateBrand:   h.handleCreateBrand,
		opDescribeBrand: h.handleDescribeBrand,
		opUpdateBrand:   h.handleUpdateBrand,
		opDeleteBrand:   h.handleDeleteBrand,
		opListBrands:    h.handleListBrands,
	}
}

// dispatchNew handles all Appendix-A operations that are not dispatched by the
// legacy type-specific helpers (namespace/group/user/datasource/dataset/dashboard/analysis/tag).
// Stateful resource families (folders/templates/themes/vpc-connections/brands) are
// served from real backend state via statefulOps. Account/config-cluster operations
// are likewise real (backed by InMemoryBackend) and are routed to dispatchAccountConfig.
// GetIdentityContext and PredictQAResults are likewise real but (unlike the canned
// ops) need the request body, so they are special-cased here too, ahead of the
// canned appendixOps table.
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
	if fn, ok := h.statefulOps[op]; ok {
		return fn(c)
	}
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

// classifyAccountSubscriptionPaths routes /account/{accountId} paths.
func classifyAccountSubscriptionPaths(method string, segs []string, _ int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch method {
	case http.MethodPost:
		return opCreateAccountSubscription, accountID
	case http.MethodGet:
		return opDescribeAccountSubscription, accountID
	case http.MethodDelete:
		return opDeleteAccountSubscription, accountID
	}

	return opUnknown, ""
}

// classifyNamespaceSingularPaths routes /accounts/{id}/namespace/{ns}/... paths.
func classifyNamespaceSingularPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsSubResID {
		return opUnknown, ""
	}

	ns := seg(segs, segResID)
	sub := seg(segs, segSubRes)
	subID := seg(segs, segSubResID)

	if sub == pathSegIAMPolicyAssignments {
		switch method { //nolint:gocritic // existing issue.
		case http.MethodDelete:
			return opDeleteIAMPolicyAssignment, subID
		}
	}

	_ = ns

	return opUnknown, ""
}

// classifyFolderPaths routes /accounts/{id}/folders/... paths.
func classifyFolderPaths( //nolint:gocognit,cyclop // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method { //nolint:gocritic // existing issue.
		case http.MethodGet:
			return opListFolders, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateFolder, id
		case http.MethodGet:
			return opDescribeFolder, id
		case http.MethodPut:
			return opUpdateFolder, id
		case http.MethodDelete:
			return opDeleteFolder, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeFolderPermissions, id
			case http.MethodPut:
				return opUpdateFolderPermissions, id
			}
		case pathSegResolvedPerms:
			if method == http.MethodGet {
				return opDescribeFolderResolvedPerms, id
			}
		case pathSegMembers:
			if method == http.MethodGet {
				return opListFolderMembers, id
			}
		}
	case nSegsSubSubRes:
		if seg(segs, segSubRes) == pathSegMembers {
			id := seg(segs, segResID)
			switch method {
			case http.MethodPut:
				return opCreateFolderMembership, id
			case http.MethodDelete:
				return opDeleteFolderMembership, id
			}
		}
	}

	return opUnknown, ""
}

// classifyTemplatePaths routes /accounts/{id}/templates/... paths.
func classifyTemplatePaths( //nolint:gocognit,cyclop // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListTemplates, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateTemplate, id
		case http.MethodGet:
			return opDescribeTemplate, id
		case http.MethodPut:
			return opUpdateTemplate, id
		case http.MethodDelete:
			return opDeleteTemplate, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegDefinition:
			if method == http.MethodGet {
				return opDescribeTemplateDefinition, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeTemplatePerms, id
			case http.MethodPut:
				return opUpdateTemplatePerms, id
			}
		case pathSegAliases:
			if method == http.MethodGet {
				return opListTemplateAliases, id
			}
		case pathSegVersions:
			if method == http.MethodGet {
				return opListTemplateVersions, id
			}
		}
	case nSegsSubResID:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		alias := seg(segs, segSubResID)
		if sub == pathSegAliases {
			switch method {
			case http.MethodPost:
				return opCreateTemplateAlias, alias
			case http.MethodGet:
				return opDescribeTemplateAlias, alias
			case http.MethodPut:
				return opUpdateTemplateAlias, alias
			case http.MethodDelete:
				return opDeleteTemplateAlias, id
			}
		}
	}

	return opUnknown, ""
}

// classifyThemePaths routes /accounts/{id}/themes/... paths.
func classifyThemePaths( //nolint:gocognit,cyclop // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListThemes, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateTheme, id
		case http.MethodGet:
			return opDescribeTheme, id
		case http.MethodPut:
			return opUpdateTheme, id
		case http.MethodDelete:
			return opDeleteTheme, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeThemePerms, id
			case http.MethodPut:
				return opUpdateThemePerms, id
			}
		case pathSegAliases:
			if method == http.MethodGet {
				return opListThemeAliases, id
			}
		case pathSegVersions:
			if method == http.MethodGet {
				return opListThemeVersions, id
			}
		}
	case nSegsSubResID:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		alias := seg(segs, segSubResID)
		if sub == pathSegAliases {
			switch method {
			case http.MethodPost:
				return opCreateThemeAlias, alias
			case http.MethodGet:
				return opDescribeThemeAlias, alias
			case http.MethodPut:
				return opUpdateThemeAlias, alias
			case http.MethodDelete:
				return opDeleteThemeAlias, id
			}
		}
	}

	return opUnknown, ""
}

// classifyTopicPaths routes /accounts/{id}/topics/... paths.
func classifyTopicPaths( //nolint:gocognit,cyclop,funlen // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateTopic, accountID
		case http.MethodGet:
			return opListTopics, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeTopic, id
		case http.MethodPut:
			return opUpdateTopic, id
		case http.MethodDelete:
			return opDeleteTopic, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeTopicPerms, id
			case http.MethodPut:
				return opUpdateTopicPerms, id
			}
		case pathSegSchedules:
			if method == http.MethodPost {
				return opCreateTopicRefreshSchedule, id
			}
			if method == http.MethodGet {
				return opListTopicRefreshSchedules, id
			}
		case pathSegReviewedAnswers:
			if method == http.MethodGet {
				return opListTopicReviewedAnswers, id
			}
		case pathSegBatchCreateReviewed:
			if method == http.MethodPost {
				return opBatchCreateTopicAnswers, id
			}
		case pathSegBatchDeleteReviewed:
			if method == http.MethodPost {
				return opBatchDeleteTopicAnswers, id
			}
		}
	case nSegsSubResID:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		subID := seg(segs, segSubResID)
		switch sub {
		case pathSegSchedules:
			switch method {
			case http.MethodGet:
				return opDescribeTopicRefreshSchedule, id
			case http.MethodPut:
				return opUpdateTopicRefreshSchedule, id
			case http.MethodDelete:
				return opDeleteTopicRefreshSchedule, id
			}
		case pathSegRefresh:
			_ = subID
			if method == http.MethodGet {
				return opDescribeTopicRefresh, id
			}
		}
	}

	return opUnknown, ""
}

// classifyVPCConnectionPaths routes /accounts/{id}/vpc-connections/... paths.
func classifyVPCConnectionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateVPCConnection, accountID
		case http.MethodGet:
			return opListVPCConnections, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeVPCConnection, id
		case http.MethodPut:
			return opUpdateVPCConnection, id
		case http.MethodDelete:
			return opDeleteVPCConnection, id
		}
	}

	return opUnknown, ""
}

// classifyActionConnectorPaths routes /accounts/{id}/action-connectors/... paths.
func classifyActionConnectorPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateActionConnector, accountID
		case http.MethodGet:
			return opListActionConnectors, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeActionConnector, id
		case http.MethodPut:
			return opUpdateActionConnector, id
		case http.MethodDelete:
			return opDeleteActionConnector, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeActionConnectorPerms, id
			case http.MethodPut:
				return opUpdateActionConnectorPerms, id
			}
		case pathSegSearch:
			if method == http.MethodPost {
				return opSearchActionConnectors, accountID
			}
		}
	}

	return opUnknown, ""
}

// classifyBrandPaths routes /accounts/{id}/brands/... paths.
func classifyBrandPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListBrands, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateBrand, id
		case http.MethodGet:
			return opDescribeBrand, id
		case http.MethodPut:
			return opUpdateBrand, id
		case http.MethodDelete:
			return opDeleteBrand, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		if sub == pathSegPublishedVersion {
			switch method {
			case http.MethodGet:
				return opDescribeBrandPublishedVer, id
			case http.MethodPut:
				return opUpdateBrandPublishedVer, id
			}
		}
	}

	return opUnknown, ""
}

// classifyBrandAssignmentPaths routes /accounts/{id}/brandassignments/... paths.
func classifyBrandAssignmentPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeBrandAssignment, accountID
		case http.MethodPut:
			return opUpdateBrandAssignment, accountID
		case http.MethodDelete:
			return opDeleteBrandAssignment, accountID
		}
	}

	return opUnknown, ""
}

// classifyCustomPermissionsPaths routes /accounts/{id}/custom-permissions/... paths.
func classifyCustomPermissionsPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateCustomPermissions, accountID
		case http.MethodGet:
			return opListCustomPermissions, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeCustomPermissions, id
		case http.MethodPut:
			return opUpdateCustomPermissions, id
		case http.MethodDelete:
			return opDeleteCustomPermissions, id
		}
	}

	return opUnknown, ""
}

// classifyOAuthAppPaths routes /accounts/{id}/oauth-client-applications/... paths.
func classifyOAuthAppPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateOAuthClientApp, accountID
		case http.MethodGet:
			return opListOAuthClientApps, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeOAuthClientApp, id
		case http.MethodPut:
			return opUpdateOAuthClientApp, id
		case http.MethodDelete:
			return opDeleteOAuthClientApp, id
		}
	}

	return opUnknown, ""
}

// classifyIdentityPropagationPaths routes /accounts/{id}/identity-propagation-config/... paths.
func classifyIdentityPropagationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListIdentityPropagationConfigs, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPut:
			return opUpdateIdentityPropagationConfig, id
		case http.MethodDelete:
			return opDeleteIdentityPropagationConfig, id
		}
	}

	return opUnknown, ""
}

// classifyAssetBundleExportPaths routes /accounts/{id}/asset-bundle-export-jobs/... paths.
func classifyAssetBundleExportPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opStartAssetBundleExportJob, accountID
		case http.MethodGet:
			return opListAssetBundleExportJobs, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		if method == http.MethodGet {
			return opDescribeAssetBundleExportJob, id
		}
	}

	return opUnknown, ""
}

// classifyAssetBundleImportPaths routes /accounts/{id}/asset-bundle-import-jobs/... paths.
func classifyAssetBundleImportPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opStartAssetBundleImportJob, accountID
		case http.MethodGet:
			return opListAssetBundleImportJobs, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		if method == http.MethodGet {
			return opDescribeAssetBundleImportJob, id
		}
	}

	return opUnknown, ""
}

// classifyAutomationPaths routes
// /accounts/{id}/automation-groups/{groupId}/automations/{automationId}/jobs[/{jobId}]
// paths (StartAutomationJob is the 7-segment POST; DescribeAutomationJob is
// the 8-segment GET with a trailing JobId).
func classifyAutomationPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsSubSubRes || seg(segs, segSubSubRes) != pathSegJobs {
		return opUnknown, ""
	}

	automationID := seg(segs, segSubResID)

	switch method {
	case http.MethodPost:
		if n == nSegsSubSubRes {
			return opStartAutomationJob, automationID
		}
	case http.MethodGet:
		if n == nSegsSubSubResID {
			return opDescribeAutomationJob, automationID
		}
	}

	return opUnknown, ""
}

// classifyFlowPaths routes /accounts/{id}/flows/... paths. Per the real
// QuickSight API, SearchFlows lives at /flows/searchFlows (POST) and
// GetFlowMetadata at /flows/{FlowId}/metadata (GET); there is no
// /flows/{FlowId} (bare) endpoint.
func classifyFlowPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		if method == http.MethodGet {
			return opListFlows, accountID
		}
	case nSegsAccountResID:
		if method == http.MethodPost && seg(segs, segResID) == pathSegSearchFlows {
			return opSearchFlows, accountID
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegMetadata:
			if method == http.MethodGet {
				return opGetFlowMetadata, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opGetFlowPermissions, id
			case http.MethodPut:
				return opUpdateFlowPerms, id
			}
		}
	}

	return opUnknown, ""
}

// classifyResourceFoldersPaths routes /accounts/{id}/resource/{resARN}/folders paths.
func classifyResourceFoldersPaths(method string, segs []string, n int) (string, string) {
	if n == nSegsSubRes && seg(segs, segSubRes) == pathSegFolders && method == http.MethodGet {
		return opListFoldersForResource, seg(segs, segResID)
	}

	return opUnknown, ""
}

// classifyCustomizationPaths routes /accounts/{id}/customizations/... paths.
func classifyCustomizationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodPost:
			return opCreateAccountCustomization, accountID
		case http.MethodGet:
			return opDescribeAccountCustomization, accountID
		case http.MethodPut:
			return opUpdateAccountCustomization, accountID
		case http.MethodDelete:
			return opDeleteAccountCustomization, accountID
		}
	}

	return opUnknown, ""
}

// classifyAccountCustomPermissionPaths routes /accounts/{id}/custom-permission/... paths.
func classifyAccountCustomPermissionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeAccountCustomPerm, accountID
		case http.MethodPut:
			return opUpdateAccountCustomPerm, accountID
		case http.MethodDelete:
			return opDeleteAccountCustomPerm, accountID
		}
	}

	return opUnknown, ""
}

// classifyAccountSettingsPaths routes /accounts/{id}/settings/... paths.
func classifyAccountSettingsPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeAccountSettings, accountID
		case http.MethodPut:
			return opUpdateAccountSettings, accountID
		}
	}

	return opUnknown, ""
}

// classifyDashboardsQAPaths routes /accounts/{id}/dashboards-qa-configuration/... paths.
func classifyDashboardsQAPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeDashboardsQAConfiguration, accountID
		case http.MethodPut:
			return opUpdateDashboardsQAConfiguration, accountID
		}
	}

	return opUnknown, ""
}

// classifyDefaultQBizPaths routes /accounts/{id}/default-qbusiness-application/... paths.
func classifyDefaultQBizPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeDefaultQBiz, accountID
		case http.MethodPut:
			return opUpdateDefaultQBiz, accountID
		case http.MethodDelete:
			return opDeleteDefaultQBiz, accountID
		}
	}

	return opUnknown, ""
}

// classifyIPRestrictionPaths routes /accounts/{id}/ip-restriction/... paths.
func classifyIPRestrictionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeIpRestriction, accountID
		case http.MethodPost:
			return opUpdateIpRestriction, accountID
		}
	}

	return opUnknown, ""
}

// classifyKeyRegistrationPaths routes /accounts/{id}/key-registration/... paths.
func classifyKeyRegistrationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeKeyRegistration, accountID
		case http.MethodPost:
			return opUpdateKeyRegistration, accountID
		}
	}

	return opUnknown, ""
}

// classifyQPersonalizationPaths routes /accounts/{id}/q-personalization-configuration/... paths.
func classifyQPersonalizationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeQPersonalization, accountID
		case http.MethodPut:
			return opUpdateQPersonalization, accountID
		}
	}

	return opUnknown, ""
}

// classifyQSearchConfigPaths routes /accounts/{id}/quicksight-q-search-configuration/... paths.
func classifyQSearchConfigPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	if n == nSegsAccountRes {
		switch method {
		case http.MethodGet:
			return opDescribeQSearchConfig, accountID
		case http.MethodPut:
			return opUpdateQSearchConfig, accountID
		}
	}

	return opUnknown, ""
}

// classifyEmbedURLPaths routes /accounts/{id}/embed-url/... paths.
func classifyEmbedURLPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsAccountResID {
		return opUnknown, ""
	}

	accountID := seg(segs, segAccountID)
	subType := seg(segs, segResID)

	if method != http.MethodPost {
		return opUnknown, ""
	}

	switch subType {
	case "anonymous-user":
		return opGenerateEmbedForAnonUser, accountID
	case "registered-user":
		return opGenerateEmbedForRegUser, accountID
	case "registered-user-with-identity":
		return opGenerateEmbedForRegUserIdentity, accountID
	}

	return opUnknown, ""
}

// ---- Identity Context ----

// identityFromUserIdentifier extracts the (kind, value) pair from a
// UserIdentifier request field, a smithy union serialized as exactly one of
// {"Email":..}, {"UserArn":..}, {"UserName":..}. Returns ("", "") if none of
// those keys are present.
func identityFromUserIdentifier(m map[string]any) (string, string) {
	for _, kind := range []string{identityKindEmail, identityKindUserArn, identityKindUserName} {
		if v, ok := m[kind].(string); ok && v != "" {
			return kind, v
		}
	}

	return "", ""
}

// handleGetIdentityContext mints an identity-context token for a QuickSight
// user. Real GetIdentityContextOutput returns the token under Context (an
// STS-style identity token to pass as AssumeRole's ContextAssertion), not a
// fabricated field.
func (h *Handler) handleGetIdentityContext(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	userIdentifier, _ := body["UserIdentifier"].(map[string]any)
	kind, value := identityFromUserIdentifier(userIdentifier)

	token, err := h.Backend.GenerateIdentityContext(
		accountID, strField(body, "Namespace"), kind, value, strField(body, "ContextRegion"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Context":    token,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Predict QA ----

// qaResultToMap converts a QAResult into its PredictQAResultsOutput wire
// representation. Real AWS nests the generated-answer fields under a
// GeneratedAnswer object; the DASHBOARD_VISUAL result type (not produced by
// this emulator, since it cannot render a visual for a natural-language
// query) is intentionally not modeled here.
func qaResultToMap(r *QAResult) map[string]any {
	m := map[string]any{"ResultType": r.ResultType}

	if r.ResultType == qaResultTypeGeneratedAnswer {
		m["GeneratedAnswer"] = map[string]any{
			"AnswerId":     r.AnswerID,
			"AnswerStatus": r.AnswerStatus,
			"QuestionId":   r.QuestionID,
			"QuestionText": r.QuestionText,
			"TopicId":      r.TopicID,
			"TopicName":    r.TopicName,
		}
	}

	return m
}

// handlePredictQAResults answers a natural-language query, grounding the
// answer in the account's real Topics rather than fabricating a response.
func (h *Handler) handlePredictQAResults(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	result, err := h.Backend.PredictQAResults(accountID, strField(body, "QueryText"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PrimaryResult": qaResultToMap(result),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
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

func (h *Handler) handleSearchAnalyses(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	analyses, next, err := h.Backend.SearchAnalyses(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(analyses))
	for _, a := range analyses {
		items = append(items, analysisToMap(a))
	}

	resp := map[string]any{
		keyAnalysisSummaryList: items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchDashboards(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dashboards, next, err := h.Backend.SearchDashboards(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		items = append(items, dashboardToMap(d))
	}

	resp := map[string]any{
		keyDashboardSummaryList: items,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchDataSets(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dataSets, next, err := h.Backend.SearchDataSets(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dataSets))
	for _, ds := range dataSets {
		items = append(items, dataSetToMap(ds))
	}

	resp := map[string]any{
		keyDataSetSummaries: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchDataSources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dataSources, next, err := h.Backend.SearchDataSources(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dataSources))
	for _, ds := range dataSources {
		items = append(items, dataSourceToMap(ds))
	}

	resp := map[string]any{
		keyDataSources: items,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// handleSearchTopics searches the account's topics. Real SearchTopicsOutput
// returns the matches under TopicSummaryList (distinct from ListTopics'
// TopicsSummaries key).
func (h *Handler) handleSearchTopics(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	topics, next, err := h.Backend.SearchTopics(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		items = append(items, topicSummaryToMap(t))
	}

	resp := map[string]any{
		"TopicSummaryList": items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- ListFoldersForResource ----

func (h *Handler) handleListFoldersForResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	resourceArn := seg(segs, segResID)

	folderArns, next, err := h.Backend.ListFoldersForResource(
		accountID, resourceArn, maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		"Folders":    folderArns,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Account Custom Permission ----

// isAccountCustomPermOp reports whether op is one of the account-level
// Describe/Update/DeleteAccountCustomPermission operations (applying a named
// custom permissions profile to an entire account), as distinct from
// isCustomPermOp's CustomPermissions-profile CRUD family.
func isAccountCustomPermOp(op string) bool {
	switch op {
	case opDescribeAccountCustomPerm, opUpdateAccountCustomPerm, opDeleteAccountCustomPerm:
		return true
	}

	return false
}

func (h *Handler) dispatchAccountCustomPerm(c *echo.Context, op string) error {
	switch op {
	case opDescribeAccountCustomPerm:
		return h.handleDescribeAccountCustomPerm(c)
	case opUpdateAccountCustomPerm:
		return h.handleUpdateAccountCustomPerm(c)
	case opDeleteAccountCustomPerm:
		return h.handleDeleteAccountCustomPerm(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleDescribeAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	name, err := h.Backend.DescribeAccountCustomPermission(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"CustomPermissionsName": name,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	})
}

func (h *Handler) handleUpdateAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if updateErr := h.Backend.UpdateAccountCustomPermission(
		accountID, strField(body, "CustomPermissionsName"),
	); updateErr != nil {
		return httpErr(c, updateErr)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteAccountCustomPerm(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	if err := h.Backend.DeleteAccountCustomPermission(accountID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
