package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchNew handles all Appendix-A operations that are not dispatched by the
// legacy type-specific helpers (namespace/group/user/datasource/dataset/dashboard/analysis/tag).
//
//nolint:cyclop,gocyclo // large operation router — many branches are unavoidable
func (h *Handler) dispatchNew(c *echo.Context, op string) error {
	segs := pathSegsFromCtx(c)
	resID := seg(segs, segResID)
	subID := seg(segs, segSubResID)

	switch op {
	// ---- Folders ----
	case opCreateFolder:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeFolder:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"Folder": map[string]any{"FolderId": resID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateFolder:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteFolder:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderId": resID, keyRequestID: reqIDPlaceholder})
	case opListFolders:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderSummaryList": []any{}, keyRequestID: reqIDPlaceholder})
	case opSearchFolders:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderSummaryList": []any{}, keyRequestID: reqIDPlaceholder})
	case opDescribeFolderPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"FolderId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateFolderPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"FolderId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeFolderResolvedPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"FolderId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opCreateFolderMembership:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"FolderMember": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteFolderMembership:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListFolderMembers:
		return writeJSON(c, http.StatusOK, map[string]any{"FolderMemberList": []any{}, keyRequestID: reqIDPlaceholder})
	case opListFoldersForResource:
		return writeJSON(c, http.StatusOK, map[string]any{"Folders": []any{}, keyRequestID: reqIDPlaceholder})

	// ---- Templates ----
	case opCreateTemplate:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeTemplate:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"Template": map[string]any{"TemplateId": resID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateTemplate:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteTemplate:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateId": resID, keyRequestID: reqIDPlaceholder})
	case opListTemplates:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateSummaryList": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opListTemplateVersions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateVersionSummaryList": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeTemplateDefinition:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeTemplatePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateTemplatePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opCreateTemplateAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeTemplateAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateTemplateAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TemplateAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteTemplateAlias:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateId": resID, keyRequestID: reqIDPlaceholder})
	case opListTemplateAliases:
		return writeJSON(c, http.StatusOK, map[string]any{"TemplateAliasList": []any{}, keyRequestID: reqIDPlaceholder})

	// ---- Themes ----
	case opCreateTheme:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeTheme:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"Theme": map[string]any{"ThemeId": resID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateTheme:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteTheme:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeId": resID, keyRequestID: reqIDPlaceholder})
	case opListThemes:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeSummaryList": []any{}, keyRequestID: reqIDPlaceholder})
	case opListThemeVersions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeVersionSummaryList": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeThemePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateThemePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opCreateThemeAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeThemeAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateThemeAlias:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"ThemeAlias": map[string]any{"AliasName": subID}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteThemeAlias:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeId": resID, keyRequestID: reqIDPlaceholder})
	case opListThemeAliases:
		return writeJSON(c, http.StatusOK, map[string]any{"ThemeAliasList": []any{}, keyRequestID: reqIDPlaceholder})

	// ---- Topics ----
	case opCreateTopic:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": "new-topic", keyRequestID: reqIDPlaceholder})
	case opDescribeTopic:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opUpdateTopic:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteTopic:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opListTopics:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicsSummaries": []any{}, keyRequestID: reqIDPlaceholder})
	case opSearchTopics:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicsSummaries": []any{}, keyRequestID: reqIDPlaceholder})
	case opDescribeTopicPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TopicId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateTopicPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"TopicId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeTopicRefresh:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opCreateTopicRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeTopicRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opUpdateTopicRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteTopicRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opListTopicRefreshSchedules:
		return writeJSON(c, http.StatusOK, map[string]any{"RefreshSchedules": []any{}, keyRequestID: reqIDPlaceholder})
	case opBatchCreateTopicAnswers:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opBatchDeleteTopicAnswers:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})
	case opListTopicReviewedAnswers:
		return writeJSON(c, http.StatusOK, map[string]any{"TopicId": resID, keyRequestID: reqIDPlaceholder})

	// ---- VPC Connections ----
	case opCreateVPCConnection:
		return writeJSON(c, http.StatusOK, map[string]any{"VPCConnectionId": "new-vpc", keyRequestID: reqIDPlaceholder})
	case opDescribeVPCConnection:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"VPCConnection": map[string]any{"VPCConnectionId": resID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateVPCConnection:
		return writeJSON(c, http.StatusOK, map[string]any{"VPCConnectionId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteVPCConnection:
		return writeJSON(c, http.StatusOK, map[string]any{"VPCConnectionId": resID, keyRequestID: reqIDPlaceholder})
	case opListVPCConnections:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"VPCConnectionSummaries": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- IAM Policy Assignments ----
	case opCreateIAMPolicyAssignment:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AssignmentName": "new-assign", keyRequestID: reqIDPlaceholder},
		)
	case opDescribeIAMPolicyAssignment:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{
				"IAMPolicyAssignment": map[string]any{"AssignmentName": subID},
				keyRequestID:          reqIDPlaceholder,
			},
		)
	case opUpdateIAMPolicyAssignment:
		return writeJSON(c, http.StatusOK, map[string]any{"AssignmentName": subID, keyRequestID: reqIDPlaceholder})
	case opDeleteIAMPolicyAssignment:
		return writeJSON(c, http.StatusOK, map[string]any{"AssignmentName": subID, keyRequestID: reqIDPlaceholder})
	case opListIAMPolicyAssignments:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"IAMPolicyAssignments": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opListIAMPolicyAssignmentsForUser:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"IAMPolicyAssignments": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Custom Permissions ----
	case opCreateCustomPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{
				"Arn":        "arn:aws:quicksight:us-east-1:000000000000:custom-permissions/new",
				keyRequestID: reqIDPlaceholder,
			},
		)
	case opDescribeCustomPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"CustomPermissions": map[string]any{"Arn": "arn"}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateCustomPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{
				"Arn":        "arn:aws:quicksight:us-east-1:000000000000:custom-permissions/" + resID,
				keyRequestID: reqIDPlaceholder,
			},
		)
	case opDeleteCustomPermissions:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListCustomPermissions:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"CustomPermissionsList": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Role Memberships ----
	case opCreateRoleMembership:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteRoleMembership:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListRoleMemberships:
		return writeJSON(c, http.StatusOK, map[string]any{"MembersList": []any{}, keyRequestID: reqIDPlaceholder})
	case opGetRoleCustomPermission:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"CustomPermissionsName": "perm", keyRequestID: reqIDPlaceholder},
		)
	case opUpdateRoleCustomPermission:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteRoleCustomPermission:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- User Custom Permission ----
	case opUpdateUserCustomPermission:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteUserCustomPermission:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Dashboard Extras ----
	case opDescribeDashboardDefinition:
		return writeJSON(c, http.StatusOK, map[string]any{"DashboardId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeDashboardPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DashboardId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDashboardPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DashboardId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDashboardPublishedVersion:
		return writeJSON(c, http.StatusOK, map[string]any{"DashboardId": resID, keyRequestID: reqIDPlaceholder})
	case opUpdateDashboardLinks:
		return writeJSON(c, http.StatusOK, map[string]any{"DashboardId": resID, keyRequestID: reqIDPlaceholder})
	case opStartDashboardSnapshotJob:
		return writeJSON(c, http.StatusOK, map[string]any{"SnapshotJobId": "snap1", keyRequestID: reqIDPlaceholder})
	case opDescribeDashboardSnapshotJob:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"SnapshotJob": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeDashboardSnapshotJobResult:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"JobResult": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opStartDashboardSnapshotJobSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"DashboardId": resID, keyRequestID: reqIDPlaceholder})
	case opGetDashboardEmbedUrl:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"EmbedUrl": "https://embed.example.com", keyRequestID: reqIDPlaceholder},
		)
	case opDescribeDashboardsQAConfiguration:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DashboardsQAStatus": "ENABLED", keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDashboardsQAConfiguration:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Analysis Extras ----
	case opDescribeAnalysisDefinition:
		return writeJSON(c, http.StatusOK, map[string]any{"AnalysisId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeAnalysisPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AnalysisId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateAnalysisPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AnalysisId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Data Set Extras ----
	case opDescribeDataSetPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DataSetId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDataSetPerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DataSetId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opCreateRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"ScheduleId": "sched1", keyRequestID: reqIDPlaceholder})
	case opDescribeRefreshSchedule:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"RefreshSchedule": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{"ScheduleId": subID, keyRequestID: reqIDPlaceholder})
	case opDeleteRefreshSchedule:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListRefreshSchedules:
		return writeJSON(c, http.StatusOK, map[string]any{"RefreshSchedules": []any{}, keyRequestID: reqIDPlaceholder})
	case opPutDataSetRefreshProperties:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeDataSetRefreshProps:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DataSetRefreshProperties": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteDataSetRefreshProps:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Data Source Extras ----
	case opDescribeDataSourcePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DataSourceId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDataSourcePerms:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DataSourceId": resID, "Permissions": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Brands ----
	case opCreateBrand:
		return writeJSON(c, http.StatusOK, map[string]any{"BrandId": resID, keyRequestID: reqIDPlaceholder})
	case opDescribeBrand:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"Brand": map[string]any{"BrandId": resID}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateBrand:
		return writeJSON(c, http.StatusOK, map[string]any{"BrandId": resID, keyRequestID: reqIDPlaceholder})
	case opDeleteBrand:
		return writeJSON(c, http.StatusOK, map[string]any{"BrandId": resID, keyRequestID: reqIDPlaceholder})
	case opListBrands:
		return writeJSON(c, http.StatusOK, map[string]any{"Brands": []any{}, keyRequestID: reqIDPlaceholder})
	case opDescribeBrandAssignment:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"BrandAssignment": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateBrandAssignment:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"BrandAssignment": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteBrandAssignment:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeBrandPublishedVer:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"BrandDefinition": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateBrandPublishedVer:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- OAuth Client Apps ----
	case opCreateOAuthClientApp:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"OAuthClientApplication": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeOAuthClientApp:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"OAuthClientApplication": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateOAuthClientApp:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"OAuthClientApplication": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteOAuthClientApp:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListOAuthClientApps:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"OAuthClientApplications": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Action Connectors ----
	case opCreateActionConnector:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeActionConnector:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateActionConnector:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteActionConnector:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListActionConnectors:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opSearchActionConnectors:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeActionConnectorPerms:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateActionConnectorPerms:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Identity Propagation ----
	case opListIdentityPropagationConfigs:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateIdentityPropagationConfig:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteIdentityPropagationConfig:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Asset Bundle ----
	case opStartAssetBundleExportJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeAssetBundleExportJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListAssetBundleExportJobs:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opStartAssetBundleImportJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeAssetBundleImportJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListAssetBundleImportJobs:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Automation ----
	case opStartAutomationJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDescribeAutomationJob:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Account Customization ----
	case opCreateAccountCustomization:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AccountCustomization": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeAccountCustomization:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AccountCustomization": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateAccountCustomization:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AccountCustomization": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteAccountCustomization:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Account Custom Permission ----
	case opDescribeAccountCustomPerm:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateAccountCustomPerm:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteAccountCustomPerm:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Account Settings ----
	case opDescribeAccountSettings:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AccountSettings": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateAccountSettings:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Account Subscription ----
	case opCreateAccountSubscription:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"SignupResponse": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDescribeAccountSubscription:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AccountInfo": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opDeleteAccountSubscription:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- IP Restriction ----
	case opDescribeIpRestriction:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"IpRestrictionRuleMap": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateIpRestriction:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Key Registration ----
	case opDescribeKeyRegistration:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"RegisteredCustomerManagedKeys": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateKeyRegistration:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Public Sharing ----
	case opUpdatePublicSharingSettings:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Q Personalization ----
	case opDescribeQPersonalization:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"PersonalizationMode": "ENABLED", keyRequestID: reqIDPlaceholder},
		)
	case opUpdateQPersonalization:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Q Search Config ----
	case opDescribeQSearchConfig:
		return writeJSON(c, http.StatusOK, map[string]any{"QSearchStatus": "ENABLED", keyRequestID: reqIDPlaceholder})
	case opUpdateQSearchConfig:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- SPICE Capacity ----
	case opUpdateSPICECapacity:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Default QBiz ----
	case opDescribeDefaultQBiz:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DefaultQBusinessApplication": map[string]any{}, keyRequestID: reqIDPlaceholder},
		)
	case opUpdateDefaultQBiz:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opDeleteDefaultQBiz:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- App Token Grant ----
	case opUpdateAppTokenGrant:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Identity Context ----
	case opGetIdentityContext:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"IdentityContextDomains": []any{}, keyRequestID: reqIDPlaceholder},
		)

	// ---- Predict QA ----
	case opPredictQAResults:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Embed URLs ----
	case opGenerateEmbedForAnonUser:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"EmbedUrl": "https://embed.example.com", keyRequestID: reqIDPlaceholder},
		)
	case opGenerateEmbedForRegUser:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"EmbedUrl": "https://embed.example.com", keyRequestID: reqIDPlaceholder},
		)
	case opGenerateEmbedForRegUserIdentity:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"EmbedUrl": "https://embed.example.com", keyRequestID: reqIDPlaceholder},
		)
	case opGetSessionEmbedUrl:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"EmbedUrl": "https://embed.example.com", keyRequestID: reqIDPlaceholder},
		)

	// ---- Search ----
	case opSearchAnalyses:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"AnalysisSummaryList": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opSearchDashboards:
		return writeJSON(
			c,
			http.StatusOK,
			map[string]any{"DashboardSummaryList": []any{}, keyRequestID: reqIDPlaceholder},
		)
	case opSearchDataSets:
		return writeJSON(c, http.StatusOK, map[string]any{"DataSetSummaries": []any{}, keyRequestID: reqIDPlaceholder})
	case opSearchDataSources:
		return writeJSON(c, http.StatusOK, map[string]any{"DataSources": []any{}, keyRequestID: reqIDPlaceholder})

	// ---- Flows ----
	case opListFlows:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opSearchFlows:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opGetFlowMetadata:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opGetFlowPermissions:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateFlowPerms:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})

	// ---- Namespace Self-Upgrade ----
	case opDescribeSelfUpgradeConfig:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateSelfUpgradeConfig:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opListSelfUpgrades:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	case opUpdateSelfUpgrade:
		return writeJSON(c, http.StatusOK, map[string]any{keyRequestID: reqIDPlaceholder})
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException",
		"operation not implemented: "+op)
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
		switch method {
		case http.MethodDelete:
			return opDeleteIAMPolicyAssignment, subID
		}
	}

	_ = ns

	return opUnknown, ""
}

// classifyFolderPaths routes /accounts/{id}/folders/... paths.
func classifyFolderPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
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
		// /accounts/{id}/folders/{fid}/members/{type}/{mid}
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
func classifyTemplatePaths(method string, segs []string, n int) (string, string) {
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
func classifyThemePaths(method string, segs []string, n int) (string, string) {
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
func classifyTopicPaths(method string, segs []string, n int) (string, string) {
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

// classifyEmbedUrlPaths routes /accounts/{id}/embed-url/... paths.
func classifyEmbedUrlPaths(method string, segs []string, n int) (string, string) {
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
