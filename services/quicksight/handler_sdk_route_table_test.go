package quicksight_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// sdkRouteCases is the authoritative method+path for every real QuickSight
// operation, extracted from quicksight@v1.123.1 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op. No two
// ops in this table share the same (method, path-with-params-stripped)
// pair, so unlike s3/lambda no entry needed a required dynamic query/header
// member to disambiguate it from a sibling.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{
			"BatchCreateTopicReviewedAnswer",
			"POST",
			"/accounts/PLACEHOLDER/topics/PLACEHOLDER/batch-create-reviewed-answers",
		},
		{"BatchDeleteKnowledgeBase", "POST", "/v1/accounts/PLACEHOLDER/knowledge-bases/batch-delete"},
		{
			"BatchDeleteTopicReviewedAnswer",
			"POST",
			"/accounts/PLACEHOLDER/topics/PLACEHOLDER/batch-delete-reviewed-answers",
		},
		{"CancelIngestion", "DELETE", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/ingestions/PLACEHOLDER"},
		{"CreateAccountCustomization", "POST", "/accounts/PLACEHOLDER/customizations"},
		{"CreateAccountSubscription", "POST", "/account/PLACEHOLDER"},
		{"CreateActionConnector", "POST", "/accounts/PLACEHOLDER/action-connectors"},
		{"CreateAgent", "POST", "/accounts/PLACEHOLDER/agents"},
		{"CreateAnalysis", "POST", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER"},
		{"CreateBrand", "POST", "/accounts/PLACEHOLDER/brands/PLACEHOLDER"},
		{"CreateCustomPermissions", "POST", "/accounts/PLACEHOLDER/custom-permissions"},
		{"CreateDashboard", "POST", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER"},
		{"CreateDataSet", "POST", "/accounts/PLACEHOLDER/data-sets"},
		{"CreateDataSource", "POST", "/accounts/PLACEHOLDER/data-sources"},
		{"CreateFlow", "POST", "/accounts/PLACEHOLDER/flows"},
		{"CreateFolder", "POST", "/accounts/PLACEHOLDER/folders/PLACEHOLDER"},
		{"CreateFolderMembership", "PUT", "/accounts/PLACEHOLDER/folders/PLACEHOLDER/members/PLACEHOLDER/PLACEHOLDER"},
		{"CreateGroup", "POST", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups"},
		{
			"CreateGroupMembership",
			"PUT",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER/members/PLACEHOLDER",
		},
		{"CreateIAMPolicyAssignment", "POST", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/iam-policy-assignments"},
		{"CreateIngestion", "PUT", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/ingestions/PLACEHOLDER"},
		{"CreateKnowledgeBase", "POST", "/v1/accounts/PLACEHOLDER/knowledge-bases"},
		{"CreateNamespace", "POST", "/accounts/PLACEHOLDER"},
		{"CreateOAuthClientApplication", "POST", "/accounts/PLACEHOLDER/oauth-client-applications"},
		{"CreateRefreshSchedule", "POST", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-schedules"},
		{
			"CreateRoleMembership",
			"POST",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/members/PLACEHOLDER",
		},
		{"CreateSpace", "POST", "/v1/accounts/PLACEHOLDER/spaces"},
		{"CreateTemplate", "POST", "/accounts/PLACEHOLDER/templates/PLACEHOLDER"},
		{"CreateTemplateAlias", "POST", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"CreateTheme", "POST", "/accounts/PLACEHOLDER/themes/PLACEHOLDER"},
		{"CreateThemeAlias", "POST", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"CreateTopic", "POST", "/accounts/PLACEHOLDER/topics"},
		{"CreateTopicRefreshSchedule", "POST", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/schedules"},
		{"CreateTopicV2", "POST", "/accounts/PLACEHOLDER/topicsV2"},
		{"CreateVPCConnection", "POST", "/accounts/PLACEHOLDER/vpc-connections"},
		{"DeleteAccountCustomPermission", "DELETE", "/accounts/PLACEHOLDER/custom-permission"},
		{"DeleteAccountCustomization", "DELETE", "/accounts/PLACEHOLDER/customizations"},
		{"DeleteAccountSubscription", "DELETE", "/account/PLACEHOLDER"},
		{"DeleteActionConnector", "DELETE", "/accounts/PLACEHOLDER/action-connectors/PLACEHOLDER"},
		{"DeleteAgent", "DELETE", "/accounts/PLACEHOLDER/agents/PLACEHOLDER"},
		{"DeleteAnalysis", "DELETE", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER"},
		{"DeleteBrand", "DELETE", "/accounts/PLACEHOLDER/brands/PLACEHOLDER"},
		{"DeleteBrandAssignment", "DELETE", "/accounts/PLACEHOLDER/brandassignments"},
		{"DeleteCustomPermissions", "DELETE", "/accounts/PLACEHOLDER/custom-permissions/PLACEHOLDER"},
		{"DeleteDashboard", "DELETE", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER"},
		{"DeleteDataSet", "DELETE", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER"},
		{"DeleteDataSetRefreshProperties", "DELETE", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-properties"},
		{"DeleteDataSource", "DELETE", "/accounts/PLACEHOLDER/data-sources/PLACEHOLDER"},
		{"DeleteDefaultQBusinessApplication", "DELETE", "/accounts/PLACEHOLDER/default-qbusiness-application"},
		{"DeleteFlow", "DELETE", "/accounts/PLACEHOLDER/flows/PLACEHOLDER"},
		{"DeleteFolder", "DELETE", "/accounts/PLACEHOLDER/folders/PLACEHOLDER"},
		{
			"DeleteFolderMembership",
			"DELETE",
			"/accounts/PLACEHOLDER/folders/PLACEHOLDER/members/PLACEHOLDER/PLACEHOLDER",
		},
		{"DeleteGroup", "DELETE", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER"},
		{
			"DeleteGroupMembership",
			"DELETE",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER/members/PLACEHOLDER",
		},
		{
			"DeleteIAMPolicyAssignment",
			"DELETE",
			"/accounts/PLACEHOLDER/namespace/PLACEHOLDER/iam-policy-assignments/PLACEHOLDER",
		},
		{"DeleteIdentityPropagationConfig", "DELETE", "/accounts/PLACEHOLDER/identity-propagation-config/PLACEHOLDER"},
		{"DeleteKnowledgeBase", "DELETE", "/v1/accounts/PLACEHOLDER/knowledge-bases/PLACEHOLDER"},
		{"DeleteNamespace", "DELETE", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER"},
		{"DeleteOAuthClientApplication", "DELETE", "/accounts/PLACEHOLDER/oauth-client-applications/PLACEHOLDER"},
		{
			"DeleteRefreshSchedule",
			"DELETE",
			"/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-schedules/PLACEHOLDER",
		},
		{
			"DeleteRoleCustomPermission",
			"DELETE",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/custom-permission",
		},
		{
			"DeleteRoleMembership",
			"DELETE",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/members/PLACEHOLDER",
		},
		{"DeleteSpace", "DELETE", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER"},
		{"DeleteTemplate", "DELETE", "/accounts/PLACEHOLDER/templates/PLACEHOLDER"},
		{"DeleteTemplateAlias", "DELETE", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DeleteTheme", "DELETE", "/accounts/PLACEHOLDER/themes/PLACEHOLDER"},
		{"DeleteThemeAlias", "DELETE", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DeleteTopic", "DELETE", "/accounts/PLACEHOLDER/topics/PLACEHOLDER"},
		{"DeleteTopicRefreshSchedule", "DELETE", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/schedules/PLACEHOLDER"},
		{"DeleteTopicV2", "DELETE", "/accounts/PLACEHOLDER/topicsV2/PLACEHOLDER"},
		{"DeleteUser", "DELETE", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER"},
		{
			"DeleteUserByPrincipalId",
			"DELETE",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/user-principals/PLACEHOLDER",
		},
		{
			"DeleteUserCustomPermission",
			"DELETE",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER/custom-permission",
		},
		{"DeleteVPCConnection", "DELETE", "/accounts/PLACEHOLDER/vpc-connections/PLACEHOLDER"},
		{"DescribeAccountCustomPermission", "GET", "/accounts/PLACEHOLDER/custom-permission"},
		{"DescribeAccountCustomization", "GET", "/accounts/PLACEHOLDER/customizations"},
		{"DescribeAccountSettings", "GET", "/accounts/PLACEHOLDER/settings"},
		{"DescribeAccountSubscription", "GET", "/account/PLACEHOLDER"},
		{"DescribeActionConnector", "GET", "/accounts/PLACEHOLDER/action-connectors/PLACEHOLDER"},
		{
			"DescribeActionConnectorPermissions",
			"GET",
			"/accounts/PLACEHOLDER/action-connectors/PLACEHOLDER/permissions",
		},
		{"DescribeAgent", "GET", "/accounts/PLACEHOLDER/agents/PLACEHOLDER"},
		{"DescribeAgentPermissions", "GET", "/accounts/PLACEHOLDER/agents/PLACEHOLDER/permissions"},
		{"DescribeAnalysis", "GET", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER"},
		{"DescribeAnalysisDefinition", "GET", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER/definition"},
		{"DescribeAnalysisPermissions", "GET", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER/permissions"},
		{"DescribeAssetBundleExportJob", "GET", "/accounts/PLACEHOLDER/asset-bundle-export-jobs/PLACEHOLDER"},
		{"DescribeAssetBundleImportJob", "GET", "/accounts/PLACEHOLDER/asset-bundle-import-jobs/PLACEHOLDER"},
		{
			"DescribeAutomationJob",
			"GET",
			"/accounts/PLACEHOLDER/automation-groups/PLACEHOLDER/automations/PLACEHOLDER/jobs/PLACEHOLDER",
		},
		{"DescribeBrand", "GET", "/accounts/PLACEHOLDER/brands/PLACEHOLDER"},
		{"DescribeBrandAssignment", "GET", "/accounts/PLACEHOLDER/brandassignments"},
		{"DescribeBrandPublishedVersion", "GET", "/accounts/PLACEHOLDER/brands/PLACEHOLDER/publishedversion"},
		{"DescribeCustomPermissions", "GET", "/accounts/PLACEHOLDER/custom-permissions/PLACEHOLDER"},
		{"DescribeDashboard", "GET", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER"},
		{"DescribeDashboardDefinition", "GET", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/definition"},
		{"DescribeDashboardPermissions", "GET", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/permissions"},
		{
			"DescribeDashboardSnapshotJob",
			"GET",
			"/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/snapshot-jobs/PLACEHOLDER",
		},
		{
			"DescribeDashboardSnapshotJobResult",
			"GET",
			"/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/snapshot-jobs/PLACEHOLDER/result",
		},
		{"DescribeDashboardsQAConfiguration", "GET", "/accounts/PLACEHOLDER/dashboards-qa-configuration"},
		{"DescribeDataSet", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER"},
		{"DescribeDataSetPermissions", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/permissions"},
		{"DescribeDataSetRefreshProperties", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-properties"},
		{"DescribeDataSource", "GET", "/accounts/PLACEHOLDER/data-sources/PLACEHOLDER"},
		{"DescribeDataSourcePermissions", "GET", "/accounts/PLACEHOLDER/data-sources/PLACEHOLDER/permissions"},
		{"DescribeDefaultQBusinessApplication", "GET", "/accounts/PLACEHOLDER/default-qbusiness-application"},
		{"DescribeFlow", "GET", "/accounts/PLACEHOLDER/flows/PLACEHOLDER"},
		{"DescribeFolder", "GET", "/accounts/PLACEHOLDER/folders/PLACEHOLDER"},
		{"DescribeFolderPermissions", "GET", "/accounts/PLACEHOLDER/folders/PLACEHOLDER/permissions"},
		{"DescribeFolderResolvedPermissions", "GET", "/accounts/PLACEHOLDER/folders/PLACEHOLDER/resolved-permissions"},
		{"DescribeGroup", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER"},
		{
			"DescribeGroupMembership",
			"GET",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER/members/PLACEHOLDER",
		},
		{
			"DescribeIAMPolicyAssignment",
			"GET",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/iam-policy-assignments/PLACEHOLDER",
		},
		{"DescribeIngestion", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/ingestions/PLACEHOLDER"},
		{"DescribeIpRestriction", "GET", "/accounts/PLACEHOLDER/ip-restriction"},
		{"DescribeKeyRegistration", "GET", "/accounts/PLACEHOLDER/key-registration"},
		{"DescribeKnowledgeBase", "GET", "/v1/accounts/PLACEHOLDER/knowledge-bases/PLACEHOLDER"},
		{"DescribeKnowledgeBasePermissions", "GET", "/v1/accounts/PLACEHOLDER/knowledge-bases/PLACEHOLDER/permissions"},
		{"DescribeNamespace", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER"},
		{"DescribeOAuthClientApplication", "GET", "/accounts/PLACEHOLDER/oauth-client-applications/PLACEHOLDER"},
		{"DescribeQPersonalizationConfiguration", "GET", "/accounts/PLACEHOLDER/q-personalization-configuration"},
		{"DescribeQuickSightQSearchConfiguration", "GET", "/accounts/PLACEHOLDER/quicksight-q-search-configuration"},
		{"DescribeRefreshSchedule", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-schedules/PLACEHOLDER"},
		{
			"DescribeRoleCustomPermission",
			"GET",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/custom-permission",
		},
		{
			"DescribeSelfUpgradeConfiguration",
			"GET",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/self-upgrade-configuration",
		},
		{"DescribeSpace", "GET", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER"},
		{"DescribeSpacePermissions", "GET", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER/permissions"},
		{"DescribeTemplate", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER"},
		{"DescribeTemplateAlias", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DescribeTemplateDefinition", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/definition"},
		{"DescribeTemplatePermissions", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/permissions"},
		{"DescribeTheme", "GET", "/accounts/PLACEHOLDER/themes/PLACEHOLDER"},
		{"DescribeThemeAlias", "GET", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DescribeThemePermissions", "GET", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/permissions"},
		{"DescribeTopic", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER"},
		{"DescribeTopicPermissions", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/permissions"},
		{"DescribeTopicPermissionsV2", "GET", "/accounts/PLACEHOLDER/topicsV2/PLACEHOLDER/permissions"},
		{"DescribeTopicRefresh", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/refresh/PLACEHOLDER"},
		{"DescribeTopicRefreshSchedule", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/schedules/PLACEHOLDER"},
		{"DescribeTopicV2", "GET", "/accounts/PLACEHOLDER/topicsV2/PLACEHOLDER"},
		{"DescribeUser", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER"},
		{"DescribeVPCConnection", "GET", "/accounts/PLACEHOLDER/vpc-connections/PLACEHOLDER"},
		{"GenerateEmbedUrlForAnonymousUser", "POST", "/accounts/PLACEHOLDER/embed-url/anonymous-user"},
		{"GenerateEmbedUrlForRegisteredUser", "POST", "/accounts/PLACEHOLDER/embed-url/registered-user"},
		{
			"GenerateEmbedUrlForRegisteredUserWithIdentity",
			"POST",
			"/accounts/PLACEHOLDER/embed-url/registered-user-with-identity",
		},
		{"GetDashboardEmbedUrl", "GET", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/embed-url"},
		{"GetFlowMetadata", "GET", "/accounts/PLACEHOLDER/flows/PLACEHOLDER/metadata"},
		{"GetFlowPermissions", "GET", "/accounts/PLACEHOLDER/flows/PLACEHOLDER/permissions"},
		{"GetIdentityContext", "POST", "/accounts/PLACEHOLDER/identity-context"},
		{"GetSessionEmbedUrl", "GET", "/accounts/PLACEHOLDER/session-embed-url"},
		{"ListActionConnectors", "GET", "/accounts/PLACEHOLDER/action-connectors"},
		{"ListAgents", "GET", "/accounts/PLACEHOLDER/agents"},
		{"ListAnalyses", "GET", "/accounts/PLACEHOLDER/analyses"},
		{"ListAssetBundleExportJobs", "GET", "/accounts/PLACEHOLDER/asset-bundle-export-jobs"},
		{"ListAssetBundleImportJobs", "GET", "/accounts/PLACEHOLDER/asset-bundle-import-jobs"},
		{"ListBrands", "GET", "/accounts/PLACEHOLDER/brands"},
		{"ListCustomPermissions", "GET", "/accounts/PLACEHOLDER/custom-permissions"},
		{"ListDashboardVersions", "GET", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/versions"},
		{"ListDashboards", "GET", "/accounts/PLACEHOLDER/dashboards"},
		{"ListDataSets", "GET", "/accounts/PLACEHOLDER/data-sets"},
		{"ListDataSources", "GET", "/accounts/PLACEHOLDER/data-sources"},
		{"ListFlows", "GET", "/accounts/PLACEHOLDER/flows"},
		{"ListFolderMembers", "GET", "/accounts/PLACEHOLDER/folders/PLACEHOLDER/members"},
		{"ListFolders", "GET", "/accounts/PLACEHOLDER/folders"},
		{"ListFoldersForResource", "GET", "/accounts/PLACEHOLDER/resource/PLACEHOLDER/folders"},
		{"ListGroupMemberships", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER/members"},
		{"ListGroups", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups"},
		{"ListIAMPolicyAssignments", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/v2/iam-policy-assignments"},
		{
			"ListIAMPolicyAssignmentsForUser",
			"GET",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER/iam-policy-assignments",
		},
		{"ListIdentityPropagationConfigs", "GET", "/accounts/PLACEHOLDER/identity-propagation-config"},
		{"ListIngestions", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/ingestions"},
		{"ListKnowledgeBases", "GET", "/v1/accounts/PLACEHOLDER/knowledge-bases"},
		{"ListNamespaces", "GET", "/accounts/PLACEHOLDER/namespaces"},
		{"ListOAuthClientApplications", "GET", "/accounts/PLACEHOLDER/oauth-client-applications"},
		{"ListRefreshSchedules", "GET", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-schedules"},
		{"ListRoleMemberships", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/members"},
		{"ListSelfUpgrades", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/self-upgrade-requests"},
		{"ListSpaceResources", "GET", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER/resources"},
		{"ListSpaces", "GET", "/v1/accounts/PLACEHOLDER/spaces"},
		{"ListTagsForResource", "GET", "/resources/PLACEHOLDER/tags"},
		{"ListTemplateAliases", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/aliases"},
		{"ListTemplateVersions", "GET", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/versions"},
		{"ListTemplates", "GET", "/accounts/PLACEHOLDER/templates"},
		{"ListThemeAliases", "GET", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/aliases"},
		{"ListThemeVersions", "GET", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/versions"},
		{"ListThemes", "GET", "/accounts/PLACEHOLDER/themes"},
		{"ListTopicRefreshSchedules", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/schedules"},
		{"ListTopicReviewedAnswers", "GET", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/reviewed-answers"},
		{"ListTopics", "GET", "/accounts/PLACEHOLDER/topics"},
		{"ListTopicsV2", "GET", "/accounts/PLACEHOLDER/topicsV2"},
		{"ListUserGroups", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER/groups"},
		{"ListUsers", "GET", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users"},
		{"ListUsersIndexCapacity", "POST", "/accounts/PLACEHOLDER/quick-index/user-capacity"},
		{"ListVPCConnections", "GET", "/accounts/PLACEHOLDER/vpc-connections"},
		{"PredictQAResults", "POST", "/accounts/PLACEHOLDER/qa/predict"},
		{"PutDataSetRefreshProperties", "PUT", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-properties"},
		{"RegisterUser", "POST", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users"},
		{"RestoreAnalysis", "POST", "/accounts/PLACEHOLDER/restore/analyses/PLACEHOLDER"},
		{"SearchActionConnectors", "POST", "/accounts/PLACEHOLDER/search/action-connectors"},
		{"SearchAgents", "POST", "/accounts/PLACEHOLDER/search/agents"},
		{"SearchAnalyses", "POST", "/accounts/PLACEHOLDER/search/analyses"},
		{"SearchDashboards", "POST", "/accounts/PLACEHOLDER/search/dashboards"},
		{"SearchDataSets", "POST", "/accounts/PLACEHOLDER/search/data-sets"},
		{"SearchDataSources", "POST", "/accounts/PLACEHOLDER/search/data-sources"},
		{"SearchFlows", "POST", "/accounts/PLACEHOLDER/flows/searchFlows"},
		{"SearchFolders", "POST", "/accounts/PLACEHOLDER/search/folders"},
		{"SearchGroups", "POST", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups-search"},
		{"SearchKnowledgeBases", "POST", "/v1/accounts/PLACEHOLDER/search/knowledge-bases"},
		{"SearchSpaces", "POST", "/v1/accounts/PLACEHOLDER/search/spaces"},
		{"SearchTopics", "POST", "/accounts/PLACEHOLDER/search/topics"},
		{"SearchTopicsV2", "POST", "/accounts/PLACEHOLDER/search/topicsV2"},
		{"StartAssetBundleExportJob", "POST", "/accounts/PLACEHOLDER/asset-bundle-export-jobs/export"},
		{"StartAssetBundleImportJob", "POST", "/accounts/PLACEHOLDER/asset-bundle-import-jobs/import"},
		{
			"StartAutomationJob",
			"POST",
			"/accounts/PLACEHOLDER/automation-groups/PLACEHOLDER/automations/PLACEHOLDER/jobs",
		},
		{"StartDashboardSnapshotJob", "POST", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/snapshot-jobs"},
		{
			"StartDashboardSnapshotJobSchedule",
			"POST",
			"/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/schedules/PLACEHOLDER",
		},
		{"TagResource", "POST", "/resources/PLACEHOLDER/tags"},
		{"UntagResource", "DELETE", "/resources/PLACEHOLDER/tags"},
		{"UpdateAccountCustomPermission", "PUT", "/accounts/PLACEHOLDER/custom-permission"},
		{"UpdateAccountCustomization", "PUT", "/accounts/PLACEHOLDER/customizations"},
		{"UpdateAccountSettings", "PUT", "/accounts/PLACEHOLDER/settings"},
		{"UpdateActionConnector", "PUT", "/accounts/PLACEHOLDER/action-connectors/PLACEHOLDER"},
		{"UpdateActionConnectorPermissions", "POST", "/accounts/PLACEHOLDER/action-connectors/PLACEHOLDER/permissions"},
		{"UpdateAgent", "PUT", "/accounts/PLACEHOLDER/agents/PLACEHOLDER"},
		{"UpdateAgentPermissions", "PUT", "/accounts/PLACEHOLDER/agents/PLACEHOLDER/permissions"},
		{"UpdateAnalysis", "PUT", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER"},
		{"UpdateAnalysisPermissions", "PUT", "/accounts/PLACEHOLDER/analyses/PLACEHOLDER/permissions"},
		{
			"UpdateApplicationWithTokenExchangeGrant",
			"PUT",
			"/accounts/PLACEHOLDER/application-with-token-exchange-grant",
		},
		{"UpdateBrand", "PUT", "/accounts/PLACEHOLDER/brands/PLACEHOLDER"},
		{"UpdateBrandAssignment", "PUT", "/accounts/PLACEHOLDER/brandassignments"},
		{"UpdateBrandPublishedVersion", "PUT", "/accounts/PLACEHOLDER/brands/PLACEHOLDER/publishedversion"},
		{"UpdateCustomPermissions", "PUT", "/accounts/PLACEHOLDER/custom-permissions/PLACEHOLDER"},
		{"UpdateDashboard", "PUT", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER"},
		{"UpdateDashboardLinks", "PUT", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/linked-entities"},
		{"UpdateDashboardPermissions", "PUT", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/permissions"},
		{"UpdateDashboardPublishedVersion", "PUT", "/accounts/PLACEHOLDER/dashboards/PLACEHOLDER/versions/PLACEHOLDER"},
		{"UpdateDashboardsQAConfiguration", "PUT", "/accounts/PLACEHOLDER/dashboards-qa-configuration"},
		{"UpdateDataSet", "PUT", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER"},
		{"UpdateDataSetPermissions", "POST", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/permissions"},
		{"UpdateDataSource", "PUT", "/accounts/PLACEHOLDER/data-sources/PLACEHOLDER"},
		{"UpdateDataSourcePermissions", "POST", "/accounts/PLACEHOLDER/data-sources/PLACEHOLDER/permissions"},
		{"UpdateDefaultQBusinessApplication", "PUT", "/accounts/PLACEHOLDER/default-qbusiness-application"},
		{"UpdateFlow", "PUT", "/accounts/PLACEHOLDER/flows/PLACEHOLDER"},
		{"UpdateFlowPermissions", "PUT", "/accounts/PLACEHOLDER/flows/PLACEHOLDER/permissions"},
		{"UpdateFolder", "PUT", "/accounts/PLACEHOLDER/folders/PLACEHOLDER"},
		{"UpdateFolderPermissions", "PUT", "/accounts/PLACEHOLDER/folders/PLACEHOLDER/permissions"},
		{"UpdateGroup", "PUT", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/groups/PLACEHOLDER"},
		{
			"UpdateIAMPolicyAssignment",
			"PUT",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/iam-policy-assignments/PLACEHOLDER",
		},
		{"UpdateIdentityPropagationConfig", "POST", "/accounts/PLACEHOLDER/identity-propagation-config/PLACEHOLDER"},
		{"UpdateIpRestriction", "POST", "/accounts/PLACEHOLDER/ip-restriction"},
		{"UpdateKeyRegistration", "POST", "/accounts/PLACEHOLDER/key-registration"},
		{"UpdateKnowledgeBase", "POST", "/v1/accounts/PLACEHOLDER/knowledge-bases/PLACEHOLDER"},
		{"UpdateKnowledgeBasePermissions", "POST", "/v1/accounts/PLACEHOLDER/knowledge-bases/PLACEHOLDER/permissions"},
		{"UpdateOAuthClientApplication", "PUT", "/accounts/PLACEHOLDER/oauth-client-applications/PLACEHOLDER"},
		{"UpdatePublicSharingSettings", "PUT", "/accounts/PLACEHOLDER/public-sharing-settings"},
		{"UpdateQPersonalizationConfiguration", "PUT", "/accounts/PLACEHOLDER/q-personalization-configuration"},
		{"UpdateQuickSightQSearchConfiguration", "PUT", "/accounts/PLACEHOLDER/quicksight-q-search-configuration"},
		{"UpdateRefreshSchedule", "PUT", "/accounts/PLACEHOLDER/data-sets/PLACEHOLDER/refresh-schedules"},
		{
			"UpdateRoleCustomPermission",
			"PUT",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/roles/PLACEHOLDER/custom-permission",
		},
		{"UpdateSPICECapacityConfiguration", "POST", "/accounts/PLACEHOLDER/spice-capacity-configuration"},
		{"UpdateSelfUpgrade", "POST", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/update-self-upgrade-request"},
		{
			"UpdateSelfUpgradeConfiguration",
			"PUT",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/self-upgrade-configuration",
		},
		{"UpdateSpace", "PUT", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER"},
		{"UpdateSpacePermissions", "PUT", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER/permissions"},
		{"UpdateSpaceResources", "PUT", "/v1/accounts/PLACEHOLDER/spaces/PLACEHOLDER/resources"},
		{"UpdateTemplate", "PUT", "/accounts/PLACEHOLDER/templates/PLACEHOLDER"},
		{"UpdateTemplateAlias", "PUT", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"UpdateTemplatePermissions", "PUT", "/accounts/PLACEHOLDER/templates/PLACEHOLDER/permissions"},
		{"UpdateTheme", "PUT", "/accounts/PLACEHOLDER/themes/PLACEHOLDER"},
		{"UpdateThemeAlias", "PUT", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"UpdateThemePermissions", "PUT", "/accounts/PLACEHOLDER/themes/PLACEHOLDER/permissions"},
		{"UpdateTopic", "PUT", "/accounts/PLACEHOLDER/topics/PLACEHOLDER"},
		{"UpdateTopicPermissions", "PUT", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/permissions"},
		{"UpdateTopicPermissionsV2", "PUT", "/accounts/PLACEHOLDER/topicsV2/PLACEHOLDER/permissions"},
		{"UpdateTopicRefreshSchedule", "PUT", "/accounts/PLACEHOLDER/topics/PLACEHOLDER/schedules/PLACEHOLDER"},
		{"UpdateTopicV2", "PUT", "/accounts/PLACEHOLDER/topicsV2/PLACEHOLDER"},
		{"UpdateUser", "PUT", "/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER"},
		{
			"UpdateUserCustomPermission",
			"PUT",
			"/accounts/PLACEHOLDER/namespaces/PLACEHOLDER/users/PLACEHOLDER/custom-permission",
		},
		{"UpdateVPCConnection", "PUT", "/accounts/PLACEHOLDER/vpc-connections/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real QuickSight op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the "UnsupportedOperationException" errType that dispatch()'s default case
// emits (handler_dispatch.go) -- guarding against an op name that resolves
// correctly but has no matching case anywhere in the dispatch tree
// (gopherstack-ey26 class), not just an ExtractOperation mismatch.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := quicksight.NewHandler(quicksight.NewInMemoryBackend("123456789012", "us-east-1"))

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnsupportedOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
