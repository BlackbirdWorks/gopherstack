package quicksight

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	quicksightServiceName = "QuickSight"
	quicksightSigningName = "quicksight"
	quicksightPathPrefix  = "/accounts/"
	// quicksightAccountSubscriptionPrefix covers CreateAccountSubscription,
	// DescribeAccountSubscription, DeleteAccountSubscription, GetAccountSettings and
	// UpdateAccountSettings, the only QuickSight operations minted under the singular
	// "/account/{AwsAccountId}" instead of "/accounts/..." (confirmed against
	// aws-sdk-go-v2/service/quicksight's serializers.go SplitURI calls). Safe to match
	// broadly here since RouteMatcher still requires the Authorization header to name
	// "quicksight" below; the unrelated Account Management service ("account" signing
	// name) additionally requires POST plus an exact fixed-path match, so there is no
	// collision.
	quicksightAccountSubscriptionPrefix = "/account/"
	quicksightTagPrefix                 = "/resources/"
	// quicksightV1PathPrefix covers the KnowledgeBase and Space families,
	// the only QuickSight operations minted under "/v1/accounts/..."
	// instead of the usual "/accounts/..." (see classifyRequest's v1-strip
	// comment in handler_paths.go). Safe to match broadly here since
	// RouteMatcher still requires the Authorization header to name
	// "quicksight" below, same disambiguation as quicksightPathPrefix.
	quicksightV1PathPrefix  = "/v1/accounts/"
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

	// topic V2 ops (Q topics -- see topics_v2.go's doc comment for how these
	// relate to the topic ops above).
	opCreateTopicV2        = "CreateTopicV2"
	opDescribeTopicV2      = "DescribeTopicV2"
	opUpdateTopicV2        = "UpdateTopicV2"
	opDeleteTopicV2        = "DeleteTopicV2"
	opListTopicsV2         = "ListTopicsV2"
	opSearchTopicsV2       = "SearchTopicsV2"
	opDescribeTopicPermsV2 = "DescribeTopicPermissionsV2"
	opUpdateTopicPermsV2   = "UpdateTopicPermissionsV2"

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
	opCreateFlow         = "CreateFlow"
	opDescribeFlow       = "DescribeFlow"
	opUpdateFlow         = "UpdateFlow"
	opDeleteFlow         = "DeleteFlow"

	// agent ops.
	opCreateAgent        = "CreateAgent"
	opDescribeAgent      = "DescribeAgent"
	opUpdateAgent        = "UpdateAgent"
	opDeleteAgent        = "DeleteAgent"
	opListAgents         = "ListAgents"
	opSearchAgents       = "SearchAgents"
	opDescribeAgentPerms = "DescribeAgentPermissions"
	opUpdateAgentPerms   = "UpdateAgentPermissions"

	// knowledge base ops.
	opCreateKnowledgeBase        = "CreateKnowledgeBase"
	opDescribeKnowledgeBase      = "DescribeKnowledgeBase"
	opUpdateKnowledgeBase        = "UpdateKnowledgeBase"
	opDeleteKnowledgeBase        = "DeleteKnowledgeBase"
	opBatchDeleteKnowledgeBase   = "BatchDeleteKnowledgeBase"
	opListKnowledgeBases         = "ListKnowledgeBases"
	opSearchKnowledgeBases       = "SearchKnowledgeBases"
	opDescribeKnowledgeBasePerms = "DescribeKnowledgeBasePermissions"
	opUpdateKnowledgeBasePerms   = "UpdateKnowledgeBasePermissions"

	// space ops.
	opCreateSpace          = "CreateSpace"
	opDescribeSpace        = "DescribeSpace"
	opUpdateSpace          = "UpdateSpace"
	opDeleteSpace          = "DeleteSpace"
	opListSpaces           = "ListSpaces"
	opSearchSpaces         = "SearchSpaces"
	opDescribeSpacePerms   = "DescribeSpacePermissions"
	opUpdateSpacePerms     = "UpdateSpacePermissions"
	opListSpaceResources   = "ListSpaceResources"
	opUpdateSpaceResources = "UpdateSpaceResources"

	// user index capacity.
	opListUsersIndexCapacity = "ListUsersIndexCapacity"

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
	pathSegTopicsV2             = "topicsV2"
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
	pathSegV1                   = "v1"
	pathSegAgents               = "agents"
	pathSegKnowledgeBases       = "knowledge-bases"
	pathSegSpaces               = "spaces"
	pathSegQuickIndex           = "quick-index"
	pathSegUserCapacity         = "user-capacity"
	pathSegBatchDelete          = "batch-delete"

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
		if strings.HasPrefix(path, quicksightPathPrefix) || strings.HasPrefix(path, quicksightTagPrefix) ||
			strings.HasPrefix(path, quicksightV1PathPrefix) ||
			strings.HasPrefix(path, quicksightAccountSubscriptionPrefix) {
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
func (h *Handler) GetSupportedOperations() []string {
	groups := [][]string{
		namespaceAndGroupOps(),
		userOps(),
		dataSourceOps(),
		dataSetOps(),
		ingestionOps(),
		dashboardOps(),
		analysisOps(),
		tagOps(),
		folderOps(),
		templateOps(),
		themeOps(),
		topicOps(),
		topicV2Ops(),
		vpcConnectionOps(),
		iamPolicyAssignmentOps(),
		customPermissionsOps(),
		roleOps(),
		userCustomPermissionOps(),
		dashboardExtraOps(),
		analysisExtraOps(),
		dataSetExtraOps(),
		dataSourceExtraOps(),
		brandOps(),
		oauthAppOps(),
		actionConnectorOps(),
		identityPropagationOps(),
		assetBundleOps(),
		automationOps(),
		accountLevelOps(),
		embedOps(),
		searchOps(),
		flowOps(),
		selfUpgradeOps(),
		agentOps(),
		knowledgeBaseOps(),
		spaceOps(),
		userIndexCapacityOps(),
	}

	var ops []string
	for _, g := range groups {
		ops = append(ops, g...)
	}

	return ops
}

func namespaceAndGroupOps() []string {
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
	}
}

func userOps() []string {
	return []string{
		opRegisterUser,
		opDescribeUser,
		opUpdateUser,
		opDeleteUser,
		opDeleteUserByPrincipalID,
		opListUsers,
		opListUserGroups,
	}
}

func dataSourceOps() []string {
	return []string{
		opCreateDataSource,
		opDescribeDataSource,
		opUpdateDataSource,
		opDeleteDataSource,
		opListDataSources,
	}
}

func dataSetOps() []string {
	return []string{
		opCreateDataSet,
		opDescribeDataSet,
		opUpdateDataSet,
		opDeleteDataSet,
		opListDataSets,
	}
}

func ingestionOps() []string {
	return []string{
		opCreateIngestion,
		opDescribeIngestion,
		opCancelIngestion,
		opListIngestions,
	}
}

func dashboardOps() []string {
	return []string{
		opCreateDashboard,
		opDescribeDashboard,
		opUpdateDashboard,
		opDeleteDashboard,
		opListDashboards,
		opListDashboardVersions,
	}
}

func analysisOps() []string {
	return []string{
		opCreateAnalysis,
		opDescribeAnalysis,
		opUpdateAnalysis,
		opDeleteAnalysis,
		opListAnalyses,
		opRestoreAnalysis,
	}
}

func tagOps() []string {
	return []string{
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

func folderOps() []string {
	return []string{
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
	}
}

func templateOps() []string {
	return []string{
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
	}
}

func themeOps() []string {
	return []string{
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
	}
}

func topicOps() []string {
	return []string{
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
	}
}

func topicV2Ops() []string {
	return []string{
		opCreateTopicV2,
		opDescribeTopicV2,
		opUpdateTopicV2,
		opDeleteTopicV2,
		opListTopicsV2,
		opSearchTopicsV2,
		opDescribeTopicPermsV2,
		opUpdateTopicPermsV2,
	}
}

func vpcConnectionOps() []string {
	return []string{
		opCreateVPCConnection,
		opDescribeVPCConnection,
		opUpdateVPCConnection,
		opDeleteVPCConnection,
		opListVPCConnections,
	}
}

func iamPolicyAssignmentOps() []string {
	return []string{
		opCreateIAMPolicyAssignment,
		opDescribeIAMPolicyAssignment,
		opUpdateIAMPolicyAssignment,
		opDeleteIAMPolicyAssignment,
		opListIAMPolicyAssignments,
		opListIAMPolicyAssignmentsForUser,
	}
}

func customPermissionsOps() []string {
	return []string{
		opCreateCustomPermissions,
		opDescribeCustomPermissions,
		opUpdateCustomPermissions,
		opDeleteCustomPermissions,
		opListCustomPermissions,
	}
}

func roleOps() []string {
	return []string{
		opCreateRoleMembership,
		opDeleteRoleMembership,
		opListRoleMemberships,
		opGetRoleCustomPermission,
		opUpdateRoleCustomPermission,
		opDeleteRoleCustomPermission,
	}
}

func userCustomPermissionOps() []string {
	return []string{
		opUpdateUserCustomPermission,
		opDeleteUserCustomPermission,
	}
}

func dashboardExtraOps() []string {
	return []string{
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
	}
}

func analysisExtraOps() []string {
	return []string{
		opDescribeAnalysisDefinition,
		opDescribeAnalysisPerms,
		opUpdateAnalysisPerms,
	}
}

func dataSetExtraOps() []string {
	return []string{
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
	}
}

func dataSourceExtraOps() []string {
	return []string{
		opDescribeDataSourcePerms,
		opUpdateDataSourcePerms,
	}
}

func brandOps() []string {
	return []string{
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
	}
}

func oauthAppOps() []string {
	return []string{
		opCreateOAuthClientApp,
		opDescribeOAuthClientApp,
		opUpdateOAuthClientApp,
		opDeleteOAuthClientApp,
		opListOAuthClientApps,
	}
}

func actionConnectorOps() []string {
	return []string{
		opCreateActionConnector,
		opDescribeActionConnector,
		opUpdateActionConnector,
		opDeleteActionConnector,
		opListActionConnectors,
		opSearchActionConnectors,
		opDescribeActionConnectorPerms,
		opUpdateActionConnectorPerms,
	}
}

func identityPropagationOps() []string {
	return []string{
		opListIdentityPropagationConfigs,
		opUpdateIdentityPropagationConfig,
		opDeleteIdentityPropagationConfig,
	}
}

func assetBundleOps() []string {
	return []string{
		opStartAssetBundleExportJob,
		opDescribeAssetBundleExportJob,
		opListAssetBundleExportJobs,
		opStartAssetBundleImportJob,
		opDescribeAssetBundleImportJob,
		opListAssetBundleImportJobs,
	}
}

func automationOps() []string {
	return []string{
		opStartAutomationJob,
		opDescribeAutomationJob,
	}
}

func accountLevelOps() []string {
	return []string{
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
	}
}

func embedOps() []string {
	return []string{
		opGenerateEmbedForAnonUser,
		opGenerateEmbedForRegUser,
		opGenerateEmbedForRegUserIdentity,
		opGetSessionEmbedUrl,
	}
}

func searchOps() []string {
	return []string{
		opSearchAnalyses,
		opSearchDashboards,
		opSearchDataSets,
		opSearchDataSources,
	}
}

func flowOps() []string {
	return []string{
		opListFlows,
		opSearchFlows,
		opGetFlowMetadata,
		opGetFlowPermissions,
		opUpdateFlowPerms,
		opCreateFlow,
		opDescribeFlow,
		opUpdateFlow,
		opDeleteFlow,
	}
}

func selfUpgradeOps() []string {
	return []string{
		opDescribeSelfUpgradeConfig,
		opUpdateSelfUpgradeConfig,
		opListSelfUpgrades,
		opUpdateSelfUpgrade,
	}
}

func agentOps() []string {
	return []string{
		opCreateAgent,
		opDescribeAgent,
		opUpdateAgent,
		opDeleteAgent,
		opListAgents,
		opSearchAgents,
		opDescribeAgentPerms,
		opUpdateAgentPerms,
	}
}

func knowledgeBaseOps() []string {
	return []string{
		opCreateKnowledgeBase,
		opDescribeKnowledgeBase,
		opUpdateKnowledgeBase,
		opDeleteKnowledgeBase,
		opBatchDeleteKnowledgeBase,
		opListKnowledgeBases,
		opSearchKnowledgeBases,
		opDescribeKnowledgeBasePerms,
		opUpdateKnowledgeBasePerms,
	}
}

func spaceOps() []string {
	return []string{
		opCreateSpace,
		opDescribeSpace,
		opUpdateSpace,
		opDeleteSpace,
		opListSpaces,
		opSearchSpaces,
		opDescribeSpacePerms,
		opUpdateSpacePerms,
		opListSpaceResources,
		opUpdateSpaceResources,
	}
}

func userIndexCapacityOps() []string {
	return []string{opListUsersIndexCapacity}
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
