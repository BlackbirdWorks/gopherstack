package quicksight

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	quicksightServiceName   = "QuickSight"
	quicksightSigningName   = "quicksight"
	quicksightPathPrefix    = "/accounts/"
	quicksightTagPrefix     = "/resources/"
	quicksightMatchPriority = service.PriorityPathVersioned + 1

	opUnknown = "Unknown"

	// operation name constants.
	opCreateNamespace         = "CreateNamespace"
	opDescribeNamespace       = "DescribeNamespace"
	opDeleteNamespace         = "DeleteNamespace"
	opListNamespaces          = "ListNamespaces"
	opCreateGroup             = "CreateGroup"
	opDescribeGroup           = "DescribeGroup"
	opUpdateGroup             = "UpdateGroup"
	opDeleteGroup             = "DeleteGroup"
	opListGroups              = "ListGroups"
	opSearchGroups            = "SearchGroups"
	opCreateGroupMembership   = "CreateGroupMembership"
	opDescribeGroupMembership = "DescribeGroupMembership"
	opDeleteGroupMembership   = "DeleteGroupMembership"
	opListGroupMemberships    = "ListGroupMemberships"
	opRegisterUser            = "RegisterUser"
	opDescribeUser            = "DescribeUser"
	opUpdateUser              = "UpdateUser"
	opDeleteUser              = "DeleteUser"
	opDeleteUserByPrincipalID = "DeleteUserByPrincipalId"
	opListUsers               = "ListUsers"
	opListUserGroups          = "ListUserGroups"
	opCreateDataSource        = "CreateDataSource"
	opDescribeDataSource      = "DescribeDataSource"
	opUpdateDataSource        = "UpdateDataSource"
	opDeleteDataSource        = "DeleteDataSource"
	opListDataSources         = "ListDataSources"
	opCreateDataSet           = "CreateDataSet"
	opDescribeDataSet         = "DescribeDataSet"
	opUpdateDataSet           = "UpdateDataSet"
	opDeleteDataSet           = "DeleteDataSet"
	opListDataSets            = "ListDataSets"
	opCreateIngestion         = "CreateIngestion"
	opDescribeIngestion       = "DescribeIngestion"
	opCancelIngestion         = "CancelIngestion"
	opListIngestions          = "ListIngestions"
	opCreateDashboard         = "CreateDashboard"
	opDescribeDashboard       = "DescribeDashboard"
	opUpdateDashboard         = "UpdateDashboard"
	opDeleteDashboard         = "DeleteDashboard"
	opListDashboards          = "ListDashboards"
	opListDashboardVersions   = "ListDashboardVersions"
	opCreateAnalysis          = "CreateAnalysis"
	opDescribeAnalysis        = "DescribeAnalysis"
	opUpdateAnalysis          = "UpdateAnalysis"
	opDeleteAnalysis          = "DeleteAnalysis"
	opListAnalyses            = "ListAnalyses"
	opRestoreAnalysis         = "RestoreAnalysis"
	opTagResource             = "TagResource"
	opUntagResource           = "UntagResource"
	opListTagsForResource     = "ListTagsForResource"

	// folder ops.
	opCreateFolder                = "CreateFolder"
	opDescribeFolder              = "DescribeFolder"
	opUpdateFolder                = "UpdateFolder"
	opDeleteFolder                = "DeleteFolder"
	opListFolders                 = "ListFolders"
	opSearchFolders               = "SearchFolders"
	opCreateFolderMembership      = "CreateFolderMembership"
	opDeleteFolderMembership      = "DeleteFolderMembership"
	opListFolderMembers           = "ListFolderMembers"
	opListFoldersForResource      = "ListFoldersForResource"
	opDescribeFolderPermissions   = "DescribeFolderPermissions"
	opDescribeFolderResolvedPerms = "DescribeFolderResolvedPermissions"
	opUpdateFolderPermissions     = "UpdateFolderPermissions"

	// template ops.
	opCreateTemplate             = "CreateTemplate"
	opDescribeTemplate           = "DescribeTemplate"
	opUpdateTemplate             = "UpdateTemplate"
	opDeleteTemplate             = "DeleteTemplate"
	opListTemplates              = "ListTemplates"
	opListTemplateVersions       = "ListTemplateVersions"
	opDescribeTemplateDefinition = "DescribeTemplateDefinition"
	opDescribeTemplatePerms      = "DescribeTemplatePermissions"
	opUpdateTemplatePerms        = "UpdateTemplatePermissions"
	opCreateTemplateAlias        = "CreateTemplateAlias"
	opDescribeTemplateAlias      = "DescribeTemplateAlias"
	opUpdateTemplateAlias        = "UpdateTemplateAlias"
	opDeleteTemplateAlias        = "DeleteTemplateAlias"
	opListTemplateAliases        = "ListTemplateAliases"

	// theme ops.
	opCreateTheme        = "CreateTheme"
	opDescribeTheme      = "DescribeTheme"
	opUpdateTheme        = "UpdateTheme"
	opDeleteTheme        = "DeleteTheme"
	opListThemes         = "ListThemes"
	opListThemeVersions  = "ListThemeVersions"
	opDescribeThemePerms = "DescribeThemePermissions"
	opUpdateThemePerms   = "UpdateThemePermissions"
	opCreateThemeAlias   = "CreateThemeAlias"
	opDescribeThemeAlias = "DescribeThemeAlias"
	opUpdateThemeAlias   = "UpdateThemeAlias"
	opDeleteThemeAlias   = "DeleteThemeAlias"
	opListThemeAliases   = "ListThemeAliases"

	// topic ops.
	opCreateTopic                  = "CreateTopic"
	opDescribeTopic                = "DescribeTopic"
	opUpdateTopic                  = "UpdateTopic"
	opDeleteTopic                  = "DeleteTopic"
	opListTopics                   = "ListTopics"
	opSearchTopics                 = "SearchTopics"
	opDescribeTopicPerms           = "DescribeTopicPermissions"
	opUpdateTopicPerms             = "UpdateTopicPermissions"
	opCreateTopicRefreshSchedule   = "CreateTopicRefreshSchedule"
	opDescribeTopicRefreshSchedule = "DescribeTopicRefreshSchedule"
	opUpdateTopicRefreshSchedule   = "UpdateTopicRefreshSchedule"
	opDeleteTopicRefreshSchedule   = "DeleteTopicRefreshSchedule"
	opListTopicRefreshSchedules    = "ListTopicRefreshSchedules"
	opDescribeTopicRefresh         = "DescribeTopicRefresh"
	opBatchCreateTopicAnswers      = "BatchCreateTopicReviewedAnswer"
	opBatchDeleteTopicAnswers      = "BatchDeleteTopicReviewedAnswer"
	opListTopicReviewedAnswers     = "ListTopicReviewedAnswers"

	// VPC connection ops.
	opCreateVPCConnection   = "CreateVPCConnection"
	opDescribeVPCConnection = "DescribeVPCConnection"
	opUpdateVPCConnection   = "UpdateVPCConnection"
	opDeleteVPCConnection   = "DeleteVPCConnection"
	opListVPCConnections    = "ListVPCConnections"

	// IAM policy assignment ops.
	opCreateIAMPolicyAssignment       = "CreateIAMPolicyAssignment"
	opDescribeIAMPolicyAssignment     = "DescribeIAMPolicyAssignment"
	opUpdateIAMPolicyAssignment       = "UpdateIAMPolicyAssignment"
	opDeleteIAMPolicyAssignment       = "DeleteIAMPolicyAssignment"
	opListIAMPolicyAssignments        = "ListIAMPolicyAssignments"
	opListIAMPolicyAssignmentsForUser = "ListIAMPolicyAssignmentsForUser"

	// custom permissions ops.
	opCreateCustomPermissions   = "CreateCustomPermissions"
	opDescribeCustomPermissions = "DescribeCustomPermissions"
	opUpdateCustomPermissions   = "UpdateCustomPermissions"
	opDeleteCustomPermissions   = "DeleteCustomPermissions"
	opListCustomPermissions     = "ListCustomPermissions"

	// role membership ops.
	opCreateRoleMembership       = "CreateRoleMembership"
	opDeleteRoleMembership       = "DeleteRoleMembership"
	opListRoleMemberships        = "ListRoleMemberships"
	opGetRoleCustomPermission    = "DescribeRoleCustomPermission"
	opUpdateRoleCustomPermission = "UpdateRoleCustomPermission"
	opDeleteRoleCustomPermission = "DeleteRoleCustomPermission"

	// user custom permission ops.
	opUpdateUserCustomPermission = "UpdateUserCustomPermission"
	opDeleteUserCustomPermission = "DeleteUserCustomPermission"

	// dashboard extra ops.
	opDescribeDashboardDefinition        = "DescribeDashboardDefinition"
	opDescribeDashboardPerms             = "DescribeDashboardPermissions"
	opUpdateDashboardPerms               = "UpdateDashboardPermissions"
	opUpdateDashboardPublishedVersion    = "UpdateDashboardPublishedVersion"
	opUpdateDashboardLinks               = "UpdateDashboardLinks"
	opStartDashboardSnapshotJob          = "StartDashboardSnapshotJob"
	opDescribeDashboardSnapshotJob       = "DescribeDashboardSnapshotJob"
	opDescribeDashboardSnapshotJobResult = "DescribeDashboardSnapshotJobResult"
	opStartDashboardSnapshotJobSchedule  = "StartDashboardSnapshotJobSchedule"
	opGetDashboardEmbedUrl               = "GetDashboardEmbedUrl" //nolint:revive,staticcheck // existing issue.
	opDescribeDashboardsQAConfiguration  = "DescribeDashboardsQAConfiguration"
	opUpdateDashboardsQAConfiguration    = "UpdateDashboardsQAConfiguration"

	// analysis extra ops.
	opDescribeAnalysisDefinition = "DescribeAnalysisDefinition"
	opDescribeAnalysisPerms      = "DescribeAnalysisPermissions"
	opUpdateAnalysisPerms        = "UpdateAnalysisPermissions"

	// data-set extra ops.
	opDescribeDataSetPerms        = "DescribeDataSetPermissions"
	opUpdateDataSetPerms          = "UpdateDataSetPermissions"
	opCreateRefreshSchedule       = "CreateRefreshSchedule"
	opDescribeRefreshSchedule     = "DescribeRefreshSchedule"
	opUpdateRefreshSchedule       = "UpdateRefreshSchedule"
	opDeleteRefreshSchedule       = "DeleteRefreshSchedule"
	opListRefreshSchedules        = "ListRefreshSchedules"
	opPutDataSetRefreshProperties = "PutDataSetRefreshProperties"
	opDescribeDataSetRefreshProps = "DescribeDataSetRefreshProperties"
	opDeleteDataSetRefreshProps   = "DeleteDataSetRefreshProperties"

	// data-source extra ops.
	opDescribeDataSourcePerms = "DescribeDataSourcePermissions"
	opUpdateDataSourcePerms   = "UpdateDataSourcePermissions"

	// brand ops.
	opCreateBrand               = "CreateBrand"
	opDescribeBrand             = "DescribeBrand"
	opUpdateBrand               = "UpdateBrand"
	opDeleteBrand               = "DeleteBrand"
	opListBrands                = "ListBrands"
	opDescribeBrandAssignment   = "DescribeBrandAssignment"
	opUpdateBrandAssignment     = "UpdateBrandAssignment"
	opDeleteBrandAssignment     = "DeleteBrandAssignment"
	opDescribeBrandPublishedVer = "DescribeBrandPublishedVersion"
	opUpdateBrandPublishedVer   = "UpdateBrandPublishedVersion"

	// OAuth app ops.
	opCreateOAuthClientApp   = "CreateOAuthClientApplication"
	opDescribeOAuthClientApp = "DescribeOAuthClientApplication"
	opUpdateOAuthClientApp   = "UpdateOAuthClientApplication"
	opDeleteOAuthClientApp   = "DeleteOAuthClientApplication"
	opListOAuthClientApps    = "ListOAuthClientApplications"

	// action connector ops.
	opCreateActionConnector        = "CreateActionConnector"
	opDescribeActionConnector      = "DescribeActionConnector"
	opUpdateActionConnector        = "UpdateActionConnector"
	opDeleteActionConnector        = "DeleteActionConnector"
	opListActionConnectors         = "ListActionConnectors"
	opSearchActionConnectors       = "SearchActionConnectors"
	opDescribeActionConnectorPerms = "DescribeActionConnectorPermissions"
	opUpdateActionConnectorPerms   = "UpdateActionConnectorPermissions"

	// identity propagation ops.
	opListIdentityPropagationConfigs  = "ListIdentityPropagationConfigs"
	opUpdateIdentityPropagationConfig = "UpdateIdentityPropagationConfig"
	opDeleteIdentityPropagationConfig = "DeleteIdentityPropagationConfig"

	// asset bundle ops.
	opStartAssetBundleExportJob    = "StartAssetBundleExportJob"
	opDescribeAssetBundleExportJob = "DescribeAssetBundleExportJob"
	opListAssetBundleExportJobs    = "ListAssetBundleExportJobs"
	opStartAssetBundleImportJob    = "StartAssetBundleImportJob"
	opDescribeAssetBundleImportJob = "DescribeAssetBundleImportJob"
	opListAssetBundleImportJobs    = "ListAssetBundleImportJobs"

	// automation ops.
	opStartAutomationJob    = "StartAutomationJob"
	opDescribeAutomationJob = "DescribeAutomationJob"

	// account-level ops.
	opCreateAccountCustomization   = "CreateAccountCustomization"
	opDescribeAccountCustomization = "DescribeAccountCustomization"
	opUpdateAccountCustomization   = "UpdateAccountCustomization"
	opDeleteAccountCustomization   = "DeleteAccountCustomization"
	opDescribeAccountCustomPerm    = "DescribeAccountCustomPermission"
	opUpdateAccountCustomPerm      = "UpdateAccountCustomPermission"
	opDeleteAccountCustomPerm      = "DeleteAccountCustomPermission"
	opDescribeAccountSettings      = "DescribeAccountSettings"
	opUpdateAccountSettings        = "UpdateAccountSettings"
	opCreateAccountSubscription    = "CreateAccountSubscription"
	opDescribeAccountSubscription  = "DescribeAccountSubscription"
	opDeleteAccountSubscription    = "DeleteAccountSubscription"
	opDescribeIpRestriction        = "DescribeIpRestriction" //nolint:revive,staticcheck // existing issue.
	opUpdateIpRestriction          = "UpdateIpRestriction"   //nolint:revive // existing issue.
	opDescribeKeyRegistration      = "DescribeKeyRegistration"
	opUpdateKeyRegistration        = "UpdateKeyRegistration"
	opUpdatePublicSharingSettings  = "UpdatePublicSharingSettings"
	opDescribeQPersonalization     = "DescribeQPersonalizationConfiguration"
	opUpdateQPersonalization       = "UpdateQPersonalizationConfiguration"
	opDescribeQSearchConfig        = "DescribeQuickSightQSearchConfiguration"
	opUpdateQSearchConfig          = "UpdateQuickSightQSearchConfiguration"
	opUpdateSPICECapacity          = "UpdateSPICECapacityConfiguration"
	opDescribeDefaultQBiz          = "DescribeDefaultQBusinessApplication"
	opUpdateDefaultQBiz            = "UpdateDefaultQBusinessApplication"
	opDeleteDefaultQBiz            = "DeleteDefaultQBusinessApplication"
	opUpdateAppTokenGrant          = "UpdateApplicationWithTokenExchangeGrant" //nolint:gosec // existing issue.
	opGetIdentityContext           = "GetIdentityContext"
	opPredictQAResults             = "PredictQAResults"

	// embed ops.
	opGenerateEmbedForAnonUser        = "GenerateEmbedUrlForAnonymousUser"
	opGenerateEmbedForRegUser         = "GenerateEmbedUrlForRegisteredUser"
	opGenerateEmbedForRegUserIdentity = "GenerateEmbedUrlForRegisteredUserWithIdentity"
	opGetSessionEmbedUrl              = "GetSessionEmbedUrl" //nolint:revive,staticcheck // existing issue.

	// search ops.
	opSearchAnalyses    = "SearchAnalyses"
	opSearchDashboards  = "SearchDashboards"
	opSearchDataSets    = "SearchDataSets"
	opSearchDataSources = "SearchDataSources"

	// flow ops.
	opListFlows          = "ListFlows"
	opSearchFlows        = "SearchFlows"
	opGetFlowMetadata    = "GetFlowMetadata"
	opGetFlowPermissions = "GetFlowPermissions"
	opUpdateFlowPerms    = "UpdateFlowPermissions"

	// namespace self-upgrade ops.
	opDescribeSelfUpgradeConfig = "DescribeSelfUpgradeConfiguration"
	opUpdateSelfUpgradeConfig   = "UpdateSelfUpgradeConfiguration"
	opListSelfUpgrades          = "ListSelfUpgrades"
	opUpdateSelfUpgrade         = "UpdateSelfUpgrade"

	// path segment indices.
	segAccountID   = 1
	segResource    = 2
	segResID       = 3
	segSubRes      = 4
	segSubResID    = 5
	segSubSubRes   = 6
	segSubSubResID = 7

	// segment count constants.
	nSegsAccountRoot  = 2
	nSegsAccountRes   = 3
	nSegsAccountResID = 4
	nSegsSubRes       = 5
	nSegsSubResID     = 6
	nSegsSubSubRes    = 7
	nSegsSubSubResID  = 8

	// JSON response keys.
	keyRequestID            = "RequestId"
	keyStatus               = "Status"
	keyUpdateStatus         = "UpdateStatus"
	keyNextToken            = "NextToken"
	keyGroup                = "Group"
	keyGroupList            = "GroupList"
	keyUser                 = "User"
	keyUserList             = "UserList"
	keyDataSource           = "DataSource"
	keyDataSources          = "DataSources"
	keyDataSourceID         = "DataSourceId"
	keyDataSet              = "DataSet"
	keyDataSetSummaries     = "DataSetSummaries"
	keyDataSetID            = "DataSetId"
	keyIngestion            = "Ingestion"
	keyIngestions           = "Ingestions"
	keyIngestionID          = "IngestionId"
	keyIngestionStatus      = "IngestionStatus"
	keyDashboard            = "Dashboard"
	keyDashboardSummaryList = "DashboardSummaryList"
	keyDashboardID          = "DashboardId"
	keyDashboardArn         = "DashboardArn"
	keyAnalysis             = "Analysis"
	keyAnalysisSummaryList  = "AnalysisSummaryList"
	keyAnalysisID           = "AnalysisId"
	keyCreatedTime          = "CreatedTime"
	keyLastUpdatedTime      = "LastUpdatedTime"
	keyArn                  = "Arn"
	keyName                 = "Name"
	keyCapacityRegion       = "CapacityRegion"
	keyCreationStatus       = "CreationStatus"
	keyIdentityStore        = "IdentityStore"
	keyNamespace            = "Namespace"
	keyMemberName           = "MemberName"

	// request ID placeholder.
	reqIDPlaceholder = "request-id"

	// path segment names.
	pathSegAccounts       = "accounts"
	pathSegNamespaces     = "namespaces"
	pathSegGroups         = "groups"
	pathSegMembers        = "members"
	pathSegUsers          = "users"
	pathSegDataSources    = "data-sources"
	pathSegDataSets       = "data-sets"
	pathSegIngestions     = "ingestions"
	pathSegDashboards     = "dashboards"
	pathSegAnalyses       = "analyses"
	pathSegVersions       = "versions"
	pathSegSearch         = "search"
	pathSegRestore        = "restore"
	pathSegResources      = "resources"
	pathSegTagsSuffix     = "tags"
	pathSegGroupsSearch   = "groups-search"
	pathSegUserPrincipals = "user-principals"

	// new path segment names.
	pathSegFolders              = "folders"
	pathSegTemplates            = "templates"
	pathSegThemes               = "themes"
	pathSegTopics               = "topics"
	pathSegVPCConnections       = "vpc-connections"
	pathSegActionConnectors     = "action-connectors"
	pathSegBrands               = "brands"
	pathSegBrandAssignments     = "brandassignments"
	pathSegCustomPermissions    = "custom-permissions"
	pathSegCustomPermission     = "custom-permission"
	pathSegCustomizations       = "customizations"
	pathSegSettings             = "settings"
	pathSegOAuthApps            = "oauth-client-applications"
	pathSegIAMPolicyAssignments = "iam-policy-assignments"
	pathSegRoles                = "roles"
	pathSegIdentityPropagation  = "identity-propagation-config"
	pathSegAssetBundleExport    = "asset-bundle-export-jobs"
	pathSegAssetBundleImport    = "asset-bundle-import-jobs"
	pathSegAutomationGroups     = "automation-groups"
	pathSegRefreshSchedules     = "refresh-schedules"
	pathSegRefreshProperties    = "refresh-properties"
	pathSegPermissions          = "permissions"
	pathSegDefinition           = "definition"
	pathSegAliases              = "aliases"
	pathSegSnapshotJobs         = "snapshot-jobs"
	pathSegLinkedEntities       = "linked-entities"
	pathSegSchedules            = "schedules"
	pathSegEmbedUrl             = "embed-url" //nolint:revive,staticcheck // existing issue.
	pathSegIPRestriction        = "ip-restriction"
	pathSegKeyRegistration      = "key-registration"
	pathSegPublicSharing        = "public-sharing-settings"
	pathSegQPersonalization     = "q-personalization-configuration"
	pathSegQSearchConfig        = "quicksight-q-search-configuration"
	pathSegSPICECapacity        = "spice-capacity-configuration"
	pathSegDashboardsQACfg      = "dashboards-qa-configuration"
	pathSegDefaultQBiz          = "default-qbusiness-application"
	pathSegSessionEmbedUrl      = "session-embed-url" //nolint:revive,staticcheck // existing issue.
	pathSegFlows                = "flows"
	pathSegIdentityContext      = "identity-context"
	pathSegResource2            = "resource"
	pathSegAppTokenGrant        = "application-with-token-exchange-grant"
	pathSegV2                   = "v2"
	pathSegPublishedVersion     = "publishedversion"
	pathSegAccountSingular      = "account"
	pathSegNamespaceSingular    = "namespace"
	pathSegReviewedAnswers      = "reviewed-answers"
	pathSegBatchCreateReviewed  = "batch-create-reviewed-answers"
	pathSegBatchDeleteReviewed  = "batch-delete-reviewed-answers"
	pathSegSearchFlows          = "searchFlows"
	pathSegQA                   = "qa"
	pathSegPredict              = "predict"
	pathSegSelfUpgradeCfg       = "self-upgrade-configuration"
	pathSegSelfUpgradeReqs      = "self-upgrade-requests"
	pathSegUpdateSelfUpgrade    = "update-self-upgrade-request"
	pathSegAutomations          = "automations"
	pathSegJobs                 = "jobs"
	pathSegResolvedPerms        = "resolved-permissions"
	pathSegExport               = "export"
	pathSegImport               = "import"
	pathSegMetadata             = "metadata"
	pathSegRefresh              = "refresh"
	pathSegResult               = "result"

	// error codes.
	errInvalidParam = "InvalidParameterValueException"
	errInvalidBody  = "invalid request body"

	// queryValueTrue is the string form of a "true" boolean query parameter
	// (forceDeleteWithoutRecovery, includeInputPayload, includeOutputPayload, ...).
	queryValueTrue = "true"
)

// newReqID returns a unique AWS-style request ID for each response.
func newReqID() string { return uuid.NewString() }

// Handler is the Echo HTTP handler for QuickSight operations.
type Handler struct {
	Backend     StorageBackend
	appendixOps map[string]appendixHandlerFn
	accountID   string
	region      string
}

// NewHandler creates a new QuickSight handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{
		Backend:     b,
		appendixOps: buildAppendixOps(),
		accountID:   b.AccountID(),
		region:      b.Region(),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return quicksightServiceName }

// Reset clears the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns routing priority (above SecurityHub which also uses /accounts/).
func (h *Handler) MatchPriority() int { return quicksightMatchPriority }

// RouteMatcher returns a function that matches QuickSight requests.
// QuickSight shares /accounts/ prefix with SecurityHub; we disambiguate via
// the Authorization header signing service name ("quicksight").
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if strings.HasPrefix(path, quicksightPathPrefix) || strings.HasPrefix(path, quicksightTagPrefix) {
			return isQuickSightRequest(c)
		}

		return false
	}
}

func isQuickSightRequest(c *echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")

	return strings.Contains(auth, "/"+quicksightSigningName+"/")
}

// ExtractOperation extracts the QuickSight operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the primary resource ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyRequest(c.Request().Method, c.Request().URL.Path)

	return resource
}

// GetSupportedOperations returns the list of implemented QuickSight operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen // existing issue.
	return []string{
		opCreateNamespace,
		opDescribeNamespace,
		opDeleteNamespace,
		opListNamespaces,
		opCreateGroup,
		opDescribeGroup,
		opUpdateGroup,
		opDeleteGroup,
		opListGroups,
		opSearchGroups,
		opCreateGroupMembership,
		opDescribeGroupMembership,
		opDeleteGroupMembership,
		opListGroupMemberships,
		opRegisterUser,
		opDescribeUser,
		opUpdateUser,
		opDeleteUser,
		opDeleteUserByPrincipalID,
		opListUsers,
		opListUserGroups,
		opCreateDataSource,
		opDescribeDataSource,
		opUpdateDataSource,
		opDeleteDataSource,
		opListDataSources,
		opCreateDataSet,
		opDescribeDataSet,
		opUpdateDataSet,
		opDeleteDataSet,
		opListDataSets,
		opCreateIngestion,
		opDescribeIngestion,
		opCancelIngestion,
		opListIngestions,
		opCreateDashboard,
		opDescribeDashboard,
		opUpdateDashboard,
		opDeleteDashboard,
		opListDashboards,
		opListDashboardVersions,
		opCreateAnalysis,
		opDescribeAnalysis,
		opUpdateAnalysis,
		opDeleteAnalysis,
		opListAnalyses,
		opRestoreAnalysis,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// folder ops
		opCreateFolder,
		opDescribeFolder,
		opUpdateFolder,
		opDeleteFolder,
		opListFolders,
		opSearchFolders,
		opCreateFolderMembership,
		opDeleteFolderMembership,
		opListFolderMembers,
		opListFoldersForResource,
		opDescribeFolderPermissions,
		opDescribeFolderResolvedPerms,
		opUpdateFolderPermissions,
		// template ops
		opCreateTemplate,
		opDescribeTemplate,
		opUpdateTemplate,
		opDeleteTemplate,
		opListTemplates,
		opListTemplateVersions,
		opDescribeTemplateDefinition,
		opDescribeTemplatePerms,
		opUpdateTemplatePerms,
		opCreateTemplateAlias,
		opDescribeTemplateAlias,
		opUpdateTemplateAlias,
		opDeleteTemplateAlias,
		opListTemplateAliases,
		// theme ops
		opCreateTheme,
		opDescribeTheme,
		opUpdateTheme,
		opDeleteTheme,
		opListThemes,
		opListThemeVersions,
		opDescribeThemePerms,
		opUpdateThemePerms,
		opCreateThemeAlias,
		opDescribeThemeAlias,
		opUpdateThemeAlias,
		opDeleteThemeAlias,
		opListThemeAliases,
		// topic ops
		opCreateTopic,
		opDescribeTopic,
		opUpdateTopic,
		opDeleteTopic,
		opListTopics,
		opSearchTopics,
		opDescribeTopicPerms,
		opUpdateTopicPerms,
		opCreateTopicRefreshSchedule,
		opDescribeTopicRefreshSchedule,
		opUpdateTopicRefreshSchedule,
		opDeleteTopicRefreshSchedule,
		opListTopicRefreshSchedules,
		opDescribeTopicRefresh,
		opBatchCreateTopicAnswers,
		opBatchDeleteTopicAnswers,
		opListTopicReviewedAnswers,
		// VPC connection ops
		opCreateVPCConnection,
		opDescribeVPCConnection,
		opUpdateVPCConnection,
		opDeleteVPCConnection,
		opListVPCConnections,
		// IAM policy assignment ops
		opCreateIAMPolicyAssignment,
		opDescribeIAMPolicyAssignment,
		opUpdateIAMPolicyAssignment,
		opDeleteIAMPolicyAssignment,
		opListIAMPolicyAssignments,
		opListIAMPolicyAssignmentsForUser,
		// custom permissions ops
		opCreateCustomPermissions,
		opDescribeCustomPermissions,
		opUpdateCustomPermissions,
		opDeleteCustomPermissions,
		opListCustomPermissions,
		// role ops
		opCreateRoleMembership,
		opDeleteRoleMembership,
		opListRoleMemberships,
		opGetRoleCustomPermission,
		opUpdateRoleCustomPermission,
		opDeleteRoleCustomPermission,
		// user custom permission ops
		opUpdateUserCustomPermission,
		opDeleteUserCustomPermission,
		// dashboard extra ops
		opDescribeDashboardDefinition,
		opDescribeDashboardPerms,
		opUpdateDashboardPerms,
		opUpdateDashboardPublishedVersion,
		opUpdateDashboardLinks,
		opStartDashboardSnapshotJob,
		opDescribeDashboardSnapshotJob,
		opDescribeDashboardSnapshotJobResult,
		opStartDashboardSnapshotJobSchedule,
		opGetDashboardEmbedUrl,
		opDescribeDashboardsQAConfiguration,
		opUpdateDashboardsQAConfiguration,
		// analysis extra ops
		opDescribeAnalysisDefinition,
		opDescribeAnalysisPerms,
		opUpdateAnalysisPerms,
		// data-set extra ops
		opDescribeDataSetPerms,
		opUpdateDataSetPerms,
		opCreateRefreshSchedule,
		opDescribeRefreshSchedule,
		opUpdateRefreshSchedule,
		opDeleteRefreshSchedule,
		opListRefreshSchedules,
		opPutDataSetRefreshProperties,
		opDescribeDataSetRefreshProps,
		opDeleteDataSetRefreshProps,
		// data-source extra ops
		opDescribeDataSourcePerms,
		opUpdateDataSourcePerms,
		// brand ops
		opCreateBrand,
		opDescribeBrand,
		opUpdateBrand,
		opDeleteBrand,
		opListBrands,
		opDescribeBrandAssignment,
		opUpdateBrandAssignment,
		opDeleteBrandAssignment,
		opDescribeBrandPublishedVer,
		opUpdateBrandPublishedVer,
		// OAuth app ops
		opCreateOAuthClientApp,
		opDescribeOAuthClientApp,
		opUpdateOAuthClientApp,
		opDeleteOAuthClientApp,
		opListOAuthClientApps,
		// action connector ops
		opCreateActionConnector,
		opDescribeActionConnector,
		opUpdateActionConnector,
		opDeleteActionConnector,
		opListActionConnectors,
		opSearchActionConnectors,
		opDescribeActionConnectorPerms,
		opUpdateActionConnectorPerms,
		// identity propagation ops
		opListIdentityPropagationConfigs,
		opUpdateIdentityPropagationConfig,
		opDeleteIdentityPropagationConfig,
		// asset bundle ops
		opStartAssetBundleExportJob,
		opDescribeAssetBundleExportJob,
		opListAssetBundleExportJobs,
		opStartAssetBundleImportJob,
		opDescribeAssetBundleImportJob,
		opListAssetBundleImportJobs,
		// automation ops
		opStartAutomationJob,
		opDescribeAutomationJob,
		// account-level ops
		opCreateAccountCustomization,
		opDescribeAccountCustomization,
		opUpdateAccountCustomization,
		opDeleteAccountCustomization,
		opDescribeAccountCustomPerm,
		opUpdateAccountCustomPerm,
		opDeleteAccountCustomPerm,
		opDescribeAccountSettings,
		opUpdateAccountSettings,
		opCreateAccountSubscription,
		opDescribeAccountSubscription,
		opDeleteAccountSubscription,
		opDescribeIpRestriction,
		opUpdateIpRestriction,
		opDescribeKeyRegistration,
		opUpdateKeyRegistration,
		opUpdatePublicSharingSettings,
		opDescribeQPersonalization,
		opUpdateQPersonalization,
		opDescribeQSearchConfig,
		opUpdateQSearchConfig,
		opUpdateSPICECapacity,
		opDescribeDefaultQBiz,
		opUpdateDefaultQBiz,
		opDeleteDefaultQBiz,
		opUpdateAppTokenGrant,
		opGetIdentityContext,
		opPredictQAResults,
		// embed ops
		opGenerateEmbedForAnonUser,
		opGenerateEmbedForRegUser,
		opGenerateEmbedForRegUserIdentity,
		opGetSessionEmbedUrl,
		// search ops
		opSearchAnalyses,
		opSearchDashboards,
		opSearchDataSets,
		opSearchDataSources,
		// flow ops
		opListFlows,
		opSearchFlows,
		opGetFlowMetadata,
		opGetFlowPermissions,
		opUpdateFlowPerms,
		// namespace self-upgrade ops
		opDescribeSelfUpgradeConfig,
		opUpdateSelfUpgradeConfig,
		opListSelfUpgrades,
		opUpdateSelfUpgrade,
	}
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.dispatch(c)
	}
}

// Register registers routes on the Echo server.
func (h *Handler) Register(_ context.Context, _ *echo.Echo) error { return nil }

// ChaosServiceName returns the lowercase AWS service name.
func (h *Handler) ChaosServiceName() string { return quicksightSigningName }

// ChaosOperations returns operations eligible for fault injection.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler serves.
func (h *Handler) ChaosRegions() []string { return []string{h.region} }
