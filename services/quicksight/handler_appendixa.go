package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// appendixHandlerFn is a function that builds the response body for one Appendix-A op.
type appendixHandlerFn func(resID, subID string) map[string]any

// dispatchNew handles all Appendix-A operations that are not dispatched by the
// legacy type-specific helpers (namespace/group/user/datasource/dataset/dashboard/analysis/tag).
// Account/config-cluster operations are real (backed by InMemoryBackend) and are
// routed to dispatchAccountConfig here, ahead of the canned appendixOps table.
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
	if isAccountConfigOp(op) {
		return h.dispatchAccountConfig(c, op)
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
		extra[keyRequestID] = reqIDPlaceholder

		return extra
	}
	simple := func() map[string]any { return reqID(map[string]any{}) }
	withID := func(key string) appendixHandlerFn {
		return func(resID, _ string) map[string]any { return reqID(map[string]any{key: resID}) }
	}
	withList := func(key string) appendixHandlerFn {
		return func(_, _ string) map[string]any { return reqID(map[string]any{key: []any{}}) }
	}
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
		// handler_topics.go, not through this canned table.
		opSearchTopics: withList("TopicsSummaries"),

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
		// DescribeDashboardDefinition, DescribeDashboardPermissions, and
		// UpdateDashboardPermissions are real (backed by InMemoryBackend) and routed
		// via dispatchDashboard in handler.go, not through this canned table.
		opUpdateDashboardPublishedVersion: withID("DashboardId"),
		opUpdateDashboardLinks:            withID("DashboardId"),
		// StartDashboardSnapshotJob, DescribeDashboardSnapshotJob, and
		// DescribeDashboardSnapshotJobResult are real (backed by InMemoryBackend) and
		// routed via dispatchDashboardSnapshot in handler_assetbundle.go, not through
		// this canned table.
		opStartDashboardSnapshotJobSchedule: withID("DashboardId"),
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
		opCreateActionConnector:        noContent,
		opDescribeActionConnector:      noContent,
		opUpdateActionConnector:        noContent,
		opDeleteActionConnector:        noContent,
		opListActionConnectors:         noContent,
		opSearchActionConnectors:       noContent,
		opDescribeActionConnectorPerms: noContent,
		opUpdateActionConnectorPerms:   noContent,

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
		opStartAutomationJob:    noContent,
		opDescribeAutomationJob: noContent,

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
		opUpdateSPICECapacity: noContent,

		// ---- Default QBiz ----
		// DescribeDefaultQBusinessApplication, UpdateDefaultQBusinessApplication,
		// and DeleteDefaultQBusinessApplication are real (backed by
		// InMemoryBackend) and routed via dispatchAccountConfig in
		// handler_account.go, not through this canned table.

		// ---- App Token Grant ----
		opUpdateAppTokenGrant: noContent,

		// ---- Identity Context ----
		opGetIdentityContext: func(_, _ string) map[string]any {
			return reqID(map[string]any{"IdentityContextDomains": []any{}})
		},

		// ---- Predict QA ----
		opPredictQAResults: noContent,

		// ---- Embed URLs ----
		// GenerateEmbedUrlForAnonymousUser, GenerateEmbedUrlForRegisteredUser,
		// GenerateEmbedUrlForRegisteredUserWithIdentity, GetDashboardEmbedUrl, and
		// GetSessionEmbedUrl are real (backed by InMemoryBackend) and routed via
		// dispatchEmbedURL in handler_embedurl.go, not through this canned table.

		// ---- Search ----
		// SearchAnalyses, SearchDashboards, SearchDataSets, and SearchDataSources are
		// real (backed by InMemoryBackend) and routed via dispatchTopicFamily ->
		// dispatchResourceSearch, not through this canned table.

		// ---- Flows ----
		opListFlows:          noContent,
		opSearchFlows:        noContent,
		opGetFlowMetadata:    noContent,
		opGetFlowPermissions: noContent,
		opUpdateFlowPerms:    noContent,

		// ---- Namespace Self-Upgrade ----
		opDescribeSelfUpgradeConfig: noContent,
		opUpdateSelfUpgradeConfig:   noContent,
		opListSelfUpgrades:          noContent,
		opUpdateSelfUpgrade:         noContent,
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

// classifyAutomationPaths routes /accounts/{id}/automation-groups/... paths.
func classifyAutomationPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	_ = accountID
	if n >= nSegsSubRes && seg(segs, segSubRes) == pathSegJobs {
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opStartAutomationJob, id
		case http.MethodGet:
			return opDescribeAutomationJob, id
		}
	}

	return opUnknown, ""
}

// classifyFlowPaths routes /accounts/{id}/flows/... paths.
func classifyFlowPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodGet:
			return opListFlows, accountID
		case http.MethodPost:
			return opSearchFlows, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		if method == http.MethodGet {
			return opGetFlowMetadata, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		if sub == pathSegPermissions {
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

// ---- Search{Analyses,Dashboards,DataSets,DataSources} ----

// isResourceSearchOp reports whether op is one of the Search{Analyses,
// Dashboards,DataSets,DataSources} operations (SearchFolders and SearchTopics
// are handled separately, by dispatchFolder and the canned table respectively).
func isResourceSearchOp(op string) bool {
	switch op {
	case opSearchAnalyses, opSearchDashboards, opSearchDataSets, opSearchDataSources:
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
