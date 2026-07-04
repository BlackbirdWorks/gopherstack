package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// appendixHandlerFn is a function that builds the response body for one Appendix-A op.
type appendixHandlerFn func(resID, subID string) map[string]any

// dispatchNew handles all Appendix-A operations that are not dispatched by the
// legacy type-specific helpers (namespace/group/user/datasource/dataset/dashboard/analysis/tag).
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
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
func buildAppendixOps() map[string]appendixHandlerFn { //nolint:funlen // existing issue.
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
	withNested := func(outerKey, innerKey string) appendixHandlerFn {
		return func(resID, _ string) map[string]any {
			return reqID(map[string]any{outerKey: map[string]any{innerKey: resID}})
		}
	}
	withIDAndPerms := func(key string) appendixHandlerFn {
		return func(resID, _ string) map[string]any {
			return reqID(map[string]any{key: resID, "Permissions": []any{}})
		}
	}
	withAlias := func(outerKey string) appendixHandlerFn {
		return func(_, subID string) map[string]any {
			return reqID(map[string]any{outerKey: map[string]any{"AliasName": subID}})
		}
	}
	withSubID := func(key string) appendixHandlerFn {
		return func(_, subID string) map[string]any { return reqID(map[string]any{key: subID}) }
	}
	noContent := func(_, _ string) map[string]any { return simple() }
	embedURL := func(_, _ string) map[string]any {
		return reqID(map[string]any{"EmbedUrl": "https://embed.example.com"})
	}

	return map[string]appendixHandlerFn{
		// ---- Folders ----
		// CreateFolder, DescribeFolder, UpdateFolder, DeleteFolder, ListFolders,
		// SearchFolders, folder memberships, and folder permissions are real
		// (backed by InMemoryBackend) and routed via dispatchFolder in
		// handler_folders.go, not through this canned table.
		opListFoldersForResource: withList("Folders"),

		// ---- Templates ----
		opCreateTemplate:             withID("TemplateId"),
		opDescribeTemplate:           withNested("Template", "TemplateId"),
		opUpdateTemplate:             withID("TemplateId"),
		opDeleteTemplate:             withID("TemplateId"),
		opListTemplates:              withList("TemplateSummaryList"),
		opListTemplateVersions:       withList("TemplateVersionSummaryList"),
		opDescribeTemplateDefinition: withID("TemplateId"),
		opDescribeTemplatePerms:      withIDAndPerms("TemplateId"),
		opUpdateTemplatePerms:        withIDAndPerms("TemplateId"),
		opCreateTemplateAlias:        withAlias("TemplateAlias"),
		opDescribeTemplateAlias:      withAlias("TemplateAlias"),
		opUpdateTemplateAlias:        withAlias("TemplateAlias"),
		opDeleteTemplateAlias:        withID("TemplateId"),
		opListTemplateAliases:        withList("TemplateAliasList"),

		// ---- Themes ----
		opCreateTheme:        withID("ThemeId"),
		opDescribeTheme:      withNested("Theme", "ThemeId"),
		opUpdateTheme:        withID("ThemeId"),
		opDeleteTheme:        withID("ThemeId"),
		opListThemes:         withList("ThemeSummaryList"),
		opListThemeVersions:  withList("ThemeVersionSummaryList"),
		opDescribeThemePerms: withIDAndPerms("ThemeId"),
		opUpdateThemePerms:   withIDAndPerms("ThemeId"),
		opCreateThemeAlias:   withAlias("ThemeAlias"),
		opDescribeThemeAlias: withAlias("ThemeAlias"),
		opUpdateThemeAlias:   withAlias("ThemeAlias"),
		opDeleteThemeAlias:   withID("ThemeId"),
		opListThemeAliases:   withList("ThemeAliasList"),

		// ---- Topics ----
		opCreateTopic: func(_, _ string) map[string]any {
			return reqID(map[string]any{"TopicId": "new-topic"})
		},
		opDescribeTopic:                withID("TopicId"),
		opUpdateTopic:                  withID("TopicId"),
		opDeleteTopic:                  withID("TopicId"),
		opListTopics:                   withList("TopicsSummaries"),
		opSearchTopics:                 withList("TopicsSummaries"),
		opDescribeTopicPerms:           withIDAndPerms("TopicId"),
		opUpdateTopicPerms:             withIDAndPerms("TopicId"),
		opDescribeTopicRefresh:         withID("TopicId"),
		opCreateTopicRefreshSchedule:   withID("TopicId"),
		opDescribeTopicRefreshSchedule: withID("TopicId"),
		opUpdateTopicRefreshSchedule:   withID("TopicId"),
		opDeleteTopicRefreshSchedule:   withID("TopicId"),
		opListTopicRefreshSchedules:    withList("RefreshSchedules"),
		opBatchCreateTopicAnswers:      withID("TopicId"),
		opBatchDeleteTopicAnswers:      withID("TopicId"),
		opListTopicReviewedAnswers:     withID("TopicId"),

		// ---- VPC Connections ----
		opCreateVPCConnection: func(_, _ string) map[string]any {
			return reqID(map[string]any{"VPCConnectionId": "new-vpc"})
		},
		opDescribeVPCConnection: func(resID, _ string) map[string]any {
			return reqID(map[string]any{"VPCConnection": map[string]any{"VPCConnectionId": resID}})
		},
		opUpdateVPCConnection: withID("VPCConnectionId"),
		opDeleteVPCConnection: withID("VPCConnectionId"),
		opListVPCConnections:  withList("VPCConnectionSummaries"),

		// ---- IAM Policy Assignments ----
		opCreateIAMPolicyAssignment: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AssignmentName": "new-assign"})
		},
		opDescribeIAMPolicyAssignment: func(_, subID string) map[string]any {
			return reqID(map[string]any{"IAMPolicyAssignment": map[string]any{"AssignmentName": subID}})
		},
		opUpdateIAMPolicyAssignment:       withSubID("AssignmentName"),
		opDeleteIAMPolicyAssignment:       withSubID("AssignmentName"),
		opListIAMPolicyAssignments:        withList("IAMPolicyAssignments"),
		opListIAMPolicyAssignmentsForUser: withList("IAMPolicyAssignments"),

		// ---- Custom Permissions ----
		opCreateCustomPermissions: func(_, _ string) map[string]any {
			return reqID(map[string]any{
				keyArn: "arn:aws:quicksight:us-east-1:000000000000:custom-permissions/new",
			})
		},
		opDescribeCustomPermissions: func(_, _ string) map[string]any {
			return reqID(map[string]any{"CustomPermissions": map[string]any{keyArn: "arn"}})
		},
		opUpdateCustomPermissions: func(resID, _ string) map[string]any {
			return reqID(map[string]any{
				keyArn: "arn:aws:quicksight:us-east-1:000000000000:custom-permissions/" + resID,
			})
		},
		opDeleteCustomPermissions: noContent,
		opListCustomPermissions:   withList("CustomPermissionsList"),

		// ---- Role Memberships ----
		opCreateRoleMembership: noContent,
		opDeleteRoleMembership: noContent,
		opListRoleMemberships:  withList("MembersList"),
		opGetRoleCustomPermission: func(_, _ string) map[string]any {
			return reqID(map[string]any{"CustomPermissionsName": "perm"})
		},
		opUpdateRoleCustomPermission: noContent,
		opDeleteRoleCustomPermission: noContent,

		// ---- User Custom Permission ----
		opUpdateUserCustomPermission: noContent,
		opDeleteUserCustomPermission: noContent,

		// ---- Dashboard Extras ----
		opDescribeDashboardDefinition:     withID("DashboardId"),
		opDescribeDashboardPerms:          withIDAndPerms("DashboardId"),
		opUpdateDashboardPerms:            withIDAndPerms("DashboardId"),
		opUpdateDashboardPublishedVersion: withID("DashboardId"),
		opUpdateDashboardLinks:            withID("DashboardId"),
		opStartDashboardSnapshotJob: func(_, _ string) map[string]any {
			return reqID(map[string]any{"SnapshotJobId": "snap1"})
		},
		opDescribeDashboardSnapshotJob: func(_, _ string) map[string]any {
			return reqID(map[string]any{"SnapshotJob": map[string]any{}})
		},
		opDescribeDashboardSnapshotJobResult: func(_, _ string) map[string]any {
			return reqID(map[string]any{"JobResult": map[string]any{}})
		},
		opStartDashboardSnapshotJobSchedule: withID("DashboardId"),
		opGetDashboardEmbedUrl:              embedURL,
		opDescribeDashboardsQAConfiguration: func(_, _ string) map[string]any {
			return reqID(map[string]any{"DashboardsQAStatus": "ENABLED"}) //nolint:goconst // existing issue.
		},
		opUpdateDashboardsQAConfiguration: noContent,

		// ---- Analysis Extras ----
		opDescribeAnalysisDefinition: withID("AnalysisId"),
		opDescribeAnalysisPerms:      withIDAndPerms("AnalysisId"),
		opUpdateAnalysisPerms:        withIDAndPerms("AnalysisId"),

		// ---- Data Set Extras ----
		opDescribeDataSetPerms: withIDAndPerms("DataSetId"),
		opUpdateDataSetPerms:   withIDAndPerms("DataSetId"),
		opCreateRefreshSchedule: func(_, _ string) map[string]any {
			return reqID(map[string]any{"ScheduleId": "sched1"})
		},
		opDescribeRefreshSchedule: func(_, _ string) map[string]any {
			return reqID(map[string]any{"RefreshSchedule": map[string]any{}})
		},
		opUpdateRefreshSchedule:       withSubID("ScheduleId"),
		opDeleteRefreshSchedule:       noContent,
		opListRefreshSchedules:        withList("RefreshSchedules"),
		opPutDataSetRefreshProperties: noContent,
		opDescribeDataSetRefreshProps: func(_, _ string) map[string]any {
			return reqID(map[string]any{"DataSetRefreshProperties": map[string]any{}})
		},
		opDeleteDataSetRefreshProps: noContent,

		// ---- Data Source Extras ----
		opDescribeDataSourcePerms: withIDAndPerms("DataSourceId"),
		opUpdateDataSourcePerms:   withIDAndPerms("DataSourceId"),

		// ---- Brands ----
		opCreateBrand:   withID("BrandId"),
		opDescribeBrand: withNested("Brand", "BrandId"),
		opUpdateBrand:   withID("BrandId"),
		opDeleteBrand:   withID("BrandId"),
		opListBrands:    withList("Brands"),
		opDescribeBrandAssignment: func(_, _ string) map[string]any {
			return reqID(map[string]any{"BrandAssignment": map[string]any{}})
		},
		opUpdateBrandAssignment: func(_, _ string) map[string]any {
			return reqID(map[string]any{"BrandAssignment": map[string]any{}})
		},
		opDeleteBrandAssignment: noContent,
		opDescribeBrandPublishedVer: func(_, _ string) map[string]any {
			return reqID(map[string]any{"BrandDefinition": map[string]any{}})
		},
		opUpdateBrandPublishedVer: noContent,

		// ---- OAuth Client Apps ----
		opCreateOAuthClientApp: func(_, _ string) map[string]any {
			return reqID(map[string]any{"OAuthClientApplication": map[string]any{}}) //nolint:goconst // existing issue.
		},
		opDescribeOAuthClientApp: func(_, _ string) map[string]any {
			return reqID(map[string]any{"OAuthClientApplication": map[string]any{}})
		},
		opUpdateOAuthClientApp: func(_, _ string) map[string]any {
			return reqID(map[string]any{"OAuthClientApplication": map[string]any{}})
		},
		opDeleteOAuthClientApp: noContent,
		opListOAuthClientApps:  withList("OAuthClientApplications"),

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
		opListIdentityPropagationConfigs:  noContent,
		opUpdateIdentityPropagationConfig: noContent,
		opDeleteIdentityPropagationConfig: noContent,

		// ---- Asset Bundle ----
		opStartAssetBundleExportJob:    noContent,
		opDescribeAssetBundleExportJob: noContent,
		opListAssetBundleExportJobs:    noContent,
		opStartAssetBundleImportJob:    noContent,
		opDescribeAssetBundleImportJob: noContent,
		opListAssetBundleImportJobs:    noContent,

		// ---- Automation ----
		opStartAutomationJob:    noContent,
		opDescribeAutomationJob: noContent,

		// ---- Account Customization ----
		opCreateAccountCustomization: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AccountCustomization": map[string]any{}}) //nolint:goconst // existing issue.
		},
		opDescribeAccountCustomization: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AccountCustomization": map[string]any{}})
		},
		opUpdateAccountCustomization: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AccountCustomization": map[string]any{}})
		},
		opDeleteAccountCustomization: noContent,

		// ---- Account Custom Permission ----
		opDescribeAccountCustomPerm: noContent,
		opUpdateAccountCustomPerm:   noContent,
		opDeleteAccountCustomPerm:   noContent,

		// ---- Account Settings ----
		opDescribeAccountSettings: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AccountSettings": map[string]any{}})
		},
		opUpdateAccountSettings: noContent,

		// ---- Account Subscription ----
		opCreateAccountSubscription: func(_, _ string) map[string]any {
			return reqID(map[string]any{"SignupResponse": map[string]any{}})
		},
		opDescribeAccountSubscription: func(_, _ string) map[string]any {
			return reqID(map[string]any{"AccountInfo": map[string]any{}})
		},
		opDeleteAccountSubscription: noContent,

		// ---- IP Restriction ----
		opDescribeIpRestriction: func(_, _ string) map[string]any {
			return reqID(map[string]any{"IpRestrictionRuleMap": map[string]any{}})
		},
		opUpdateIpRestriction: noContent,

		// ---- Key Registration ----
		opDescribeKeyRegistration: func(_, _ string) map[string]any {
			return reqID(map[string]any{"RegisteredCustomerManagedKeys": []any{}})
		},
		opUpdateKeyRegistration: noContent,

		// ---- Public Sharing ----
		opUpdatePublicSharingSettings: noContent,

		// ---- Q Personalization ----
		opDescribeQPersonalization: func(_, _ string) map[string]any {
			return reqID(map[string]any{"PersonalizationMode": "ENABLED"})
		},
		opUpdateQPersonalization: noContent,

		// ---- Q Search Config ----
		opDescribeQSearchConfig: func(_, _ string) map[string]any {
			return reqID(map[string]any{"QSearchStatus": "ENABLED"})
		},
		opUpdateQSearchConfig: noContent,

		// ---- SPICE Capacity ----
		opUpdateSPICECapacity: noContent,

		// ---- Default QBiz ----
		opDescribeDefaultQBiz: func(_, _ string) map[string]any {
			return reqID(map[string]any{"DefaultQBusinessApplication": map[string]any{}})
		},
		opUpdateDefaultQBiz: noContent,
		opDeleteDefaultQBiz: noContent,

		// ---- App Token Grant ----
		opUpdateAppTokenGrant: noContent,

		// ---- Identity Context ----
		opGetIdentityContext: func(_, _ string) map[string]any {
			return reqID(map[string]any{"IdentityContextDomains": []any{}})
		},

		// ---- Predict QA ----
		opPredictQAResults: noContent,

		// ---- Embed URLs ----
		opGenerateEmbedForAnonUser:        embedURL,
		opGenerateEmbedForRegUser:         embedURL,
		opGenerateEmbedForRegUserIdentity: embedURL,
		opGetSessionEmbedUrl:              embedURL,

		// ---- Search ----
		opSearchAnalyses:    withList("AnalysisSummaryList"),
		opSearchDashboards:  withList("DashboardSummaryList"),
		opSearchDataSets:    withList("DataSetSummaries"),
		opSearchDataSources: withList("DataSources"),

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
