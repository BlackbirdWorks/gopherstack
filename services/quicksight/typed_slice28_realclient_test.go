package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qsdocument "github.com/aws/aws-sdk-go-v2/service/quicksight/document"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestSlice28_QuickSight_RealClient covers quicksight's remaining
// typed-client-uncovered op families (gopherstack-n3zi slice 28): TopicV2,
// custom permissions (account/role/user), role memberships, IAM policy
// assignments, identity propagation, self-upgrade configuration, brands,
// Q Business/Dashboards-QA/QSearch config, and the Search* family.
func TestSlice28_QuickSight_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testFlowLifecycleRealClient, "flow_lifecycle"},
		{testAccountCustomizationRealClient, "account_customization"},
		{testAccountCustomPermissionRealClient, "account_custom_permission"},
		{testIAMPolicyAssignmentRealClient, "iam_policy_assignment"},
		{testRoleAndUserCustomPermissionRealClient, "role_and_user_custom_permission"},
		{testIdentityPropagationRealClient, "identity_propagation"},
		{testSelfUpgradeRealClient, "self_upgrade"},
		{testBrandsExtraRealClient, "brands_extra"},
		{testDefaultQBizRealClient, "default_qbiz"},
		{testQConfigRealClient, "q_config"},
		{testActionConnectorExtraRealClient, "action_connector_extra"},
		{testAgentExtraRealClient, "agent_extra"},
		{testSpaceExtraRealClient, "space_extra"},
		{testKnowledgeBaseExtraRealClient, "knowledge_base_extra"},
		{testAssetBundleImportRealClient, "asset_bundle_import"},
		{testDashboardSnapshotExtraRealClient, "dashboard_snapshot_extra"},
		{testTopicV2RealClient, "topic_v2"},
		{testTopicPermissionsAndSearchRealClient, "topic_permissions_and_search"},
		{testTopicRefreshRealClient, "topic_refresh"},
		{testTopicReviewedAnswersRealClient, "topic_reviewed_answers"},
		{testSearchDataSetsAndSourcesRealClient, "search_datasets_and_sources"},
		{testAppTokenGrantRealClient, "app_token_grant"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testFlowLifecycleRealClient covers CreateFlow, DescribeFlow, UpdateFlow,
// ListFlows, DeleteFlow.
func testFlowLifecycleRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateFlow(t.Context(), &quicksightsdk.CreateFlowInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Name:         aws.String("slice28-flow"),
		FlowDefinition: qsdocument.NewLazyDocument(map[string]any{
			"steps": []any{},
		}),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.FlowId))
	flowID := aws.ToString(created.FlowId)

	described, err := client.DescribeFlow(t.Context(), &quicksightsdk.DescribeFlowInput{
		AwsAccountId: aws.String(qsTestAccountID), FlowId: aws.String(flowID),
		PublishState: types.FlowPublishStatePublished,
	})
	require.NoError(t, err)
	require.NotNil(t, described.Flow)
	assert.Equal(t, "slice28-flow", aws.ToString(described.Flow.Name))

	_, err = client.UpdateFlow(t.Context(), &quicksightsdk.UpdateFlowInput{
		AwsAccountId: aws.String(qsTestAccountID),
		FlowId:       aws.String(flowID),
		Name:         aws.String("slice28-flow-renamed"),
		FlowDefinition: qsdocument.NewLazyDocument(map[string]any{
			"steps": []any{},
		}),
	})
	require.NoError(t, err)

	listed, err := client.ListFlows(t.Context(), &quicksightsdk.ListFlowsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.FlowSummaryList, 1)
	assert.Equal(t, "slice28-flow-renamed", aws.ToString(listed.FlowSummaryList[0].Name))

	_, err = client.DeleteFlow(t.Context(), &quicksightsdk.DeleteFlowInput{
		AwsAccountId: aws.String(qsTestAccountID), FlowId: aws.String(flowID),
	})
	require.NoError(t, err)
}

// testAccountCustomizationRealClient covers CreateAccountCustomization,
// UpdateAccountCustomization, DeleteAccountCustomization.
func testAccountCustomizationRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateAccountCustomization(t.Context(), &quicksightsdk.CreateAccountCustomizationInput{
		AwsAccountId:         aws.String(qsTestAccountID),
		AccountCustomization: &types.AccountCustomization{DefaultTheme: aws.String("theme-1")},
	})
	require.NoError(t, err)

	updated, err := client.UpdateAccountCustomization(t.Context(), &quicksightsdk.UpdateAccountCustomizationInput{
		AwsAccountId:         aws.String(qsTestAccountID),
		AccountCustomization: &types.AccountCustomization{DefaultTheme: aws.String("theme-2")},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.AccountCustomization)
	assert.Equal(t, "theme-2", aws.ToString(updated.AccountCustomization.DefaultTheme))

	_, err = client.DeleteAccountCustomization(t.Context(), &quicksightsdk.DeleteAccountCustomizationInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
}

// testAccountCustomPermissionRealClient covers DescribeAccountCustomPermission,
// UpdateAccountCustomPermission, DeleteAccountCustomPermission.
func testAccountCustomPermissionRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateCustomPermissions(t.Context(), &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("acp-1"),
		Capabilities:          &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
	})
	require.NoError(t, err)

	_, err = client.UpdateAccountCustomPermission(t.Context(), &quicksightsdk.UpdateAccountCustomPermissionInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("acp-1"),
	})
	require.NoError(t, err)

	described, err := client.DescribeAccountCustomPermission(
		t.Context(), &quicksightsdk.DescribeAccountCustomPermissionInput{
			AwsAccountId: aws.String(qsTestAccountID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "acp-1", aws.ToString(described.CustomPermissionsName))

	_, err = client.DeleteAccountCustomPermission(t.Context(), &quicksightsdk.DeleteAccountCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
}

// testIAMPolicyAssignmentRealClient covers CreateIAMPolicyAssignment,
// DescribeIAMPolicyAssignment, UpdateIAMPolicyAssignment,
// ListIAMPolicyAssignments, ListIAMPolicyAssignmentsForUser,
// DeleteIAMPolicyAssignment.
func testIAMPolicyAssignmentRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateIAMPolicyAssignment(t.Context(), &quicksightsdk.CreateIAMPolicyAssignmentInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		Namespace:        aws.String("default"),
		AssignmentName:   aws.String("ipa-1"),
		AssignmentStatus: types.AssignmentStatusEnabled,
		PolicyArn:        aws.String("arn:aws:iam::000000000000:policy/qs-policy"),
		Identities:       map[string][]string{"USER": {"ipa-user-1"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "ipa-1", aws.ToString(created.AssignmentName))

	described, err := client.DescribeIAMPolicyAssignment(t.Context(), &quicksightsdk.DescribeIAMPolicyAssignmentInput{
		AwsAccountId:   aws.String(qsTestAccountID),
		Namespace:      aws.String("default"),
		AssignmentName: aws.String("ipa-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.IAMPolicyAssignment)
	assert.Equal(t, types.AssignmentStatusEnabled, described.IAMPolicyAssignment.AssignmentStatus)

	_, err = client.UpdateIAMPolicyAssignment(t.Context(), &quicksightsdk.UpdateIAMPolicyAssignmentInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		Namespace:        aws.String("default"),
		AssignmentName:   aws.String("ipa-1"),
		AssignmentStatus: types.AssignmentStatusDisabled,
	})
	require.NoError(t, err)

	listed, err := client.ListIAMPolicyAssignments(t.Context(), &quicksightsdk.ListIAMPolicyAssignmentsInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
	})
	require.NoError(t, err)
	require.Len(t, listed.IAMPolicyAssignments, 1)
	assert.Equal(t, types.AssignmentStatusDisabled, listed.IAMPolicyAssignments[0].AssignmentStatus)

	forUser, err := client.ListIAMPolicyAssignmentsForUser(
		t.Context(), &quicksightsdk.ListIAMPolicyAssignmentsForUserInput{
			AwsAccountId: aws.String(
				qsTestAccountID,
			), Namespace: aws.String("default"), UserName: aws.String("ipa-user-1"),
		},
	)
	require.NoError(t, err)
	// Disabled by the update above -- ListIAMPolicyAssignmentsForUser only
	// surfaces ENABLED assignments (real API semantics).
	assert.Empty(t, forUser.ActiveAssignments)

	_, err = client.DeleteIAMPolicyAssignment(t.Context(), &quicksightsdk.DeleteIAMPolicyAssignmentInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), Namespace: aws.String("default"), AssignmentName: aws.String("ipa-1"),
	})
	require.NoError(t, err)
}

// testRoleAndUserCustomPermissionRealClient covers CreateRoleMembership,
// DeleteRoleMembership, ListRoleMemberships, DescribeRoleCustomPermission,
// UpdateRoleCustomPermission, DeleteRoleCustomPermission,
// UpdateUserCustomPermission, DeleteUserCustomPermission.
func testRoleAndUserCustomPermissionRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateCustomPermissions(t.Context(), &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("rup-1"),
		Capabilities:          &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
	})
	require.NoError(t, err)

	_, err = client.CreateRoleMembership(t.Context(), &quicksightsdk.CreateRoleMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Namespace:    aws.String("default"),
		Role:         types.RoleAuthor,
		MemberName:   aws.String("role-member-1"),
	})
	require.NoError(t, err)

	listed, err := client.ListRoleMemberships(t.Context(), &quicksightsdk.ListRoleMembershipsInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"), Role: types.RoleAuthor,
	})
	require.NoError(t, err)
	assert.Contains(t, listed.MembersList, "role-member-1")

	_, err = client.DeleteRoleMembership(t.Context(), &quicksightsdk.DeleteRoleMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		Role: types.RoleAuthor, MemberName: aws.String("role-member-1"),
	})
	require.NoError(t, err)

	_, err = client.UpdateRoleCustomPermission(t.Context(), &quicksightsdk.UpdateRoleCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		Role: types.RoleAuthor, CustomPermissionsName: aws.String("rup-1"),
	})
	require.NoError(t, err)

	described, err := client.DescribeRoleCustomPermission(t.Context(), &quicksightsdk.DescribeRoleCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"), Role: types.RoleAuthor,
	})
	require.NoError(t, err)
	assert.Equal(t, "rup-1", aws.ToString(described.CustomPermissionsName))

	_, err = client.DeleteRoleCustomPermission(t.Context(), &quicksightsdk.DeleteRoleCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"), Role: types.RoleAuthor,
	})
	require.NoError(t, err)

	_, err = client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Namespace:    aws.String("default"),
		UserName:     aws.String("rup-user-1"),
		Email:        aws.String("rup-user-1@example.com"),
		IdentityType: types.IdentityTypeQuicksight,
		UserRole:     types.UserRoleAuthor,
	})
	require.NoError(t, err)

	_, err = client.UpdateUserCustomPermission(t.Context(), &quicksightsdk.UpdateUserCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		UserName: aws.String("rup-user-1"), CustomPermissionsName: aws.String("rup-1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteUserCustomPermission(t.Context(), &quicksightsdk.DeleteUserCustomPermissionInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"), UserName: aws.String("rup-user-1"),
	})
	require.NoError(t, err)
}

// testIdentityPropagationRealClient covers UpdateIdentityPropagationConfig,
// ListIdentityPropagationConfigs, DeleteIdentityPropagationConfig.
func testIdentityPropagationRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.UpdateIdentityPropagationConfig(t.Context(), &quicksightsdk.UpdateIdentityPropagationConfigInput{
		AwsAccountId:      aws.String(qsTestAccountID),
		Service:           types.ServiceTypeAthena,
		AuthorizedTargets: []string{"arn:aws:sts::000000000000:assumed-role/qs-role/session"},
	})
	require.NoError(t, err)

	listed, err := client.ListIdentityPropagationConfigs(
		t.Context(), &quicksightsdk.ListIdentityPropagationConfigsInput{
			AwsAccountId: aws.String(qsTestAccountID),
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.Services, 1)
	assert.Equal(t, types.ServiceTypeAthena, listed.Services[0].Service)

	_, err = client.DeleteIdentityPropagationConfig(t.Context(), &quicksightsdk.DeleteIdentityPropagationConfigInput{
		AwsAccountId: aws.String(qsTestAccountID), Service: types.ServiceTypeAthena,
	})
	require.NoError(t, err)
}

// testSelfUpgradeRealClient covers DescribeSelfUpgradeConfiguration,
// UpdateSelfUpgradeConfiguration, ListSelfUpgrades, UpdateSelfUpgrade.
// There is no CreateSelfUpgradeRequest API (requests originate from the
// console); backend state is seeded via the export-only SeedSelfUpgradeRequest
// helper, mirroring this package's own documented convention for this family.
func testSelfUpgradeRealClient(t *testing.T) {
	t.Helper()

	backend := quicksight.NewInMemoryBackend(qsTestAccountID, rtQSTestRegion)
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)

	quicksight.SeedSelfUpgradeRequest(backend, qsTestAccountID, "default", &quicksight.SelfUpgradeRequestDetail{
		UpgradeRequestID: "sur-1",
		OriginalRole:     "READER",
		RequestedRole:    "AUTHOR",
		RequestStatus:    "PENDING",
		CreationTime:     1700000000,
	})

	described, err := client.DescribeSelfUpgradeConfiguration(
		t.Context(), &quicksightsdk.DescribeSelfUpgradeConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, described.SelfUpgradeConfiguration)
	assert.NotEmpty(t, described.SelfUpgradeConfiguration.SelfUpgradeStatus)

	_, err = client.UpdateSelfUpgradeConfiguration(
		t.Context(), &quicksightsdk.UpdateSelfUpgradeConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
			SelfUpgradeStatus: types.SelfUpgradeStatusAutoApproval,
		},
	)
	require.NoError(t, err)

	listed, err := client.ListSelfUpgrades(t.Context(), &quicksightsdk.ListSelfUpgradesInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
	})
	require.NoError(t, err)
	require.Len(t, listed.SelfUpgradeRequestDetails, 1)
	assert.Equal(t, "sur-1", aws.ToString(listed.SelfUpgradeRequestDetails[0].UpgradeRequestId))

	updated, err := client.UpdateSelfUpgrade(t.Context(), &quicksightsdk.UpdateSelfUpgradeInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		Action: types.SelfUpgradeAdminActionApprove, UpgradeRequestId: aws.String("sur-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.SelfUpgradeRequestDetail)
	assert.Equal(t, types.SelfUpgradeRequestStatusApproved, updated.SelfUpgradeRequestDetail.RequestStatus)
}

// testBrandsExtraRealClient covers DescribeBrandPublishedVersion,
// UpdateBrandPublishedVersion, DescribeBrandAssignment, UpdateBrandAssignment,
// DeleteBrandAssignment.
func testBrandsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateBrand(t.Context(), &quicksightsdk.CreateBrandInput{
		AwsAccountId:    aws.String(qsTestAccountID),
		BrandId:         aws.String("brand-extra-1"),
		BrandDefinition: &types.BrandDefinition{BrandName: aws.String("brand-extra")},
	})
	require.NoError(t, err)
	brandArn := aws.ToString(created.BrandDetail.Arn)

	_, err = client.UpdateBrandPublishedVersion(t.Context(), &quicksightsdk.UpdateBrandPublishedVersionInput{
		AwsAccountId: aws.String(qsTestAccountID), BrandId: aws.String("brand-extra-1"), VersionId: aws.String("1"),
	})
	require.NoError(t, err)

	published, err := client.DescribeBrandPublishedVersion(
		t.Context(), &quicksightsdk.DescribeBrandPublishedVersionInput{
			AwsAccountId: aws.String(qsTestAccountID), BrandId: aws.String("brand-extra-1"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, published.BrandDefinition)
	assert.Equal(t, "brand-extra", aws.ToString(published.BrandDefinition.BrandName))

	_, err = client.UpdateBrandAssignment(t.Context(), &quicksightsdk.UpdateBrandAssignmentInput{
		AwsAccountId: aws.String(qsTestAccountID), BrandArn: aws.String(brandArn),
	})
	require.NoError(t, err)

	assigned, err := client.DescribeBrandAssignment(t.Context(), &quicksightsdk.DescribeBrandAssignmentInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	assert.Equal(t, brandArn, aws.ToString(assigned.BrandArn))

	_, err = client.DeleteBrandAssignment(t.Context(), &quicksightsdk.DeleteBrandAssignmentInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
}

// testDefaultQBizRealClient covers UpdateDefaultQBusinessApplication,
// DescribeDefaultQBusinessApplication, DeleteDefaultQBusinessApplication.
func testDefaultQBizRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.UpdateDefaultQBusinessApplication(
		t.Context(), &quicksightsdk.UpdateDefaultQBusinessApplicationInput{
			AwsAccountId:  aws.String(qsTestAccountID),
			Namespace:     aws.String("default"),
			ApplicationId: aws.String("qbiz-app-1"),
		},
	)
	require.NoError(t, err)

	described, err := client.DescribeDefaultQBusinessApplication(
		t.Context(), &quicksightsdk.DescribeDefaultQBusinessApplicationInput{
			AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "qbiz-app-1", aws.ToString(described.ApplicationId))

	_, err = client.DeleteDefaultQBusinessApplication(
		t.Context(), &quicksightsdk.DeleteDefaultQBusinessApplicationInput{
			AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		},
	)
	require.NoError(t, err)
}

// testQConfigRealClient covers DescribeQuickSightQSearchConfiguration,
// UpdateQuickSightQSearchConfiguration, DescribeDashboardsQAConfiguration,
// UpdateDashboardsQAConfiguration.
func testQConfigRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.UpdateQuickSightQSearchConfiguration(
		t.Context(), &quicksightsdk.UpdateQuickSightQSearchConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID), QSearchStatus: types.QSearchStatusEnabled,
		},
	)
	require.NoError(t, err)

	qsearch, err := client.DescribeQuickSightQSearchConfiguration(
		t.Context(), &quicksightsdk.DescribeQuickSightQSearchConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.QSearchStatusEnabled, qsearch.QSearchStatus)

	_, err = client.UpdateDashboardsQAConfiguration(
		t.Context(), &quicksightsdk.UpdateDashboardsQAConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID), DashboardsQAStatus: types.DashboardsQAStatusEnabled,
		},
	)
	require.NoError(t, err)

	dashQA, err := client.DescribeDashboardsQAConfiguration(
		t.Context(), &quicksightsdk.DescribeDashboardsQAConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DashboardsQAStatusEnabled, dashQA.DashboardsQAStatus)
}

// testActionConnectorExtraRealClient covers UpdateActionConnector,
// DescribeActionConnectorPermissions, UpdateActionConnectorPermissions.
func testActionConnectorExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateActionConnector(t.Context(), &quicksightsdk.CreateActionConnectorInput{
		AwsAccountId:      aws.String(qsTestAccountID),
		ActionConnectorId: aws.String("ac-extra-1"),
		Name:              aws.String("ac-extra"),
		Type:              types.ActionConnectorTypeGenericHttp,
		AuthenticationConfig: &types.AuthConfig{
			AuthenticationType: types.ConnectionAuthTypeNone,
			AuthenticationMetadata: &types.AuthenticationMetadataMemberNoneConnectionMetadata{
				Value: types.NoneConnectionMetadata{BaseEndpoint: aws.String("https://example.com")},
			},
		},
	})
	require.NoError(t, err)

	updated, err := client.UpdateActionConnector(t.Context(), &quicksightsdk.UpdateActionConnectorInput{
		AwsAccountId: aws.String(qsTestAccountID), ActionConnectorId: aws.String("ac-extra-1"),
		Name: aws.String("ac-extra-renamed"),
		AuthenticationConfig: &types.AuthConfig{
			AuthenticationType: types.ConnectionAuthTypeNone,
			AuthenticationMetadata: &types.AuthenticationMetadataMemberNoneConnectionMetadata{
				Value: types.NoneConnectionMetadata{BaseEndpoint: aws.String("https://example.com")},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "ac-extra-1", aws.ToString(updated.ActionConnectorId))

	perms, err := client.DescribeActionConnectorPermissions(
		t.Context(), &quicksightsdk.DescribeActionConnectorPermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), ActionConnectorId: aws.String("ac-extra-1"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateActionConnectorPermissions(
		t.Context(), &quicksightsdk.UpdateActionConnectorPermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), ActionConnectorId: aws.String("ac-extra-1"),
			GrantPermissions: []types.ResourcePermission{
				{
					Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/ac-owner"),
					Actions:   []string{"quicksight:DescribeActionConnector"},
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)
}

// testAgentExtraRealClient covers UpdateAgent, DescribeAgentPermissions,
// UpdateAgentPermissions, SearchAgents.
func testAgentExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateAgent(t.Context(), &quicksightsdk.CreateAgentInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), AgentId: aws.String("agent-extra-1"), Name: aws.String("agent-extra"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateAgent(t.Context(), &quicksightsdk.UpdateAgentInput{
		AwsAccountId: aws.String(qsTestAccountID), AgentId: aws.String("agent-extra-1"),
		Name: aws.String("agent-extra-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "agent-extra-1", aws.ToString(updated.AgentId))

	perms, err := client.DescribeAgentPermissions(t.Context(), &quicksightsdk.DescribeAgentPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), AgentId: aws.String("agent-extra-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateAgentPermissions(t.Context(), &quicksightsdk.UpdateAgentPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), AgentId: aws.String("agent-extra-1"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/agent-owner"),
				Actions:   []string{"quicksight:DescribeAgent"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)

	found, err := client.SearchAgents(t.Context(), &quicksightsdk.SearchAgentsInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Filters:      []types.AgentSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, found.AgentSummaries)
}

// testSpaceExtraRealClient covers DescribeSpacePermissions,
// UpdateSpacePermissions, UpdateSpaceResources.
func testSpaceExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateSpace(t.Context(), &quicksightsdk.CreateSpaceInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), SpaceId: aws.String("space-extra-1"), Name: aws.String("space-extra"),
	})
	require.NoError(t, err)

	// A real resource ARN this backend already tracks, to attach via
	// UpdateSpaceResources (arnExists validates it, mirroring UpdateAgent's
	// association validation).
	topic, err := client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("space-extra-topic"),
		Topic: &types.TopicDetails{Name: aws.String("space-extra-topic")},
	})
	require.NoError(t, err)
	topicArn := aws.ToString(topic.Arn)

	perms, err := client.DescribeSpacePermissions(t.Context(), &quicksightsdk.DescribeSpacePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-extra-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateSpacePermissions(t.Context(), &quicksightsdk.UpdateSpacePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-extra-1"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/space-owner"),
				Actions:   []string{"quicksight:DescribeSpace"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)

	updatedRes, err := client.UpdateSpaceResources(t.Context(), &quicksightsdk.UpdateSpaceResourcesInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-extra-1"),
		AddResources: []types.SpaceResourceOperation{
			{
				ResourceType:    types.SpaceQuickSightResourceTypeTopic,
				ResourceDetails: &types.SpaceQuickSightResourceDetailsMemberResourceArn{Value: topicArn},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, updatedRes.FailedResourceOperations)
}

// testKnowledgeBaseExtraRealClient covers BatchDeleteKnowledgeBase,
// DescribeKnowledgeBasePermissions, UpdateKnowledgeBasePermissions,
// UpdateKnowledgeBase, SearchKnowledgeBases.
func testKnowledgeBaseExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	kbConfig := func() *types.KnowledgeBaseConfiguration {
		return &types.KnowledgeBaseConfiguration{
			TemplateConfiguration: &types.KbTemplateConfiguration{
				Template: qsdocument.NewLazyDocument(map[string]any{
					"type": "S3V2",
					"connectionConfiguration": map[string]any{
						"bucketName":           "test-bucket",
						"bucketOwnerAccountId": qsTestAccountID,
					},
				}),
			},
		}
	}

	_, err := client.CreateKnowledgeBase(t.Context(), &quicksightsdk.CreateKnowledgeBaseInput{
		AwsAccountId:               aws.String(qsTestAccountID),
		KnowledgeBaseId:            aws.String("kb-extra-1"),
		Name:                       aws.String("kb-extra"),
		DataSourceArn:              aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/kb-extra-ds"),
		KnowledgeBaseConfiguration: kbConfig(),
	})
	require.NoError(t, err)

	updated, err := client.UpdateKnowledgeBase(t.Context(), &quicksightsdk.UpdateKnowledgeBaseInput{
		AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-extra-1"),
		Name: aws.String("kb-extra-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "kb-extra-1", aws.ToString(updated.KnowledgeBaseId))

	perms, err := client.DescribeKnowledgeBasePermissions(
		t.Context(), &quicksightsdk.DescribeKnowledgeBasePermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-extra-1"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateKnowledgeBasePermissions(
		t.Context(), &quicksightsdk.UpdateKnowledgeBasePermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-extra-1"),
			GrantPermissions: []types.ResourcePermission{
				{
					Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/kb-owner"),
					Actions:   []string{"quicksight:DescribeKnowledgeBase"},
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)

	found, err := client.SearchKnowledgeBases(t.Context(), &quicksightsdk.SearchKnowledgeBasesInput{
		AwsAccountId: aws.String(qsTestAccountID), Filters: []types.KnowledgeBaseSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, found.KnowledgeBaseSummaries)

	deleted, err := client.BatchDeleteKnowledgeBase(t.Context(), &quicksightsdk.BatchDeleteKnowledgeBaseInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		KnowledgeBaseIds: []string{"kb-extra-1", "kb-extra-does-not-exist"},
	})
	require.NoError(t, err)
	require.Len(t, deleted.Deleted, 1)
	assert.Equal(t, "kb-extra-1", aws.ToString(deleted.Deleted[0].KnowledgeBaseId))
	require.Len(t, deleted.Errors, 1)
	assert.Equal(t, "kb-extra-does-not-exist", aws.ToString(deleted.Errors[0].KnowledgeBaseId))
}

// testAssetBundleImportRealClient covers StartAssetBundleImportJob,
// DescribeAssetBundleImportJob, ListAssetBundleImportJobs.
func testAssetBundleImportRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	started, err := client.StartAssetBundleImportJob(t.Context(), &quicksightsdk.StartAssetBundleImportJobInput{
		AwsAccountId:           aws.String(qsTestAccountID),
		AssetBundleImportJobId: aws.String("import-job-1"),
		AssetBundleImportSource: &types.AssetBundleImportSource{
			S3Uri: aws.String("s3://qs-bundles/import-job-1.qs"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "import-job-1", aws.ToString(started.AssetBundleImportJobId))

	described, err := client.DescribeAssetBundleImportJob(t.Context(), &quicksightsdk.DescribeAssetBundleImportJobInput{
		AwsAccountId: aws.String(qsTestAccountID), AssetBundleImportJobId: aws.String("import-job-1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, described.JobStatus)

	listed, err := client.ListAssetBundleImportJobs(t.Context(), &quicksightsdk.ListAssetBundleImportJobsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.AssetBundleImportJobSummaryList, 1)
}

// testDashboardSnapshotExtraRealClient covers DescribeDashboardSnapshotJobResult
// and StartDashboardSnapshotJobSchedule.
func testDashboardSnapshotExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String("snap-extra-dataset"),
		Name:         aws.String("snap-extra-dataset"),
		ImportMode:   types.DataSetImportModeSpice,
		PhysicalTableMap: map[string]types.PhysicalTable{
			"pt1": &types.PhysicalTableMemberRelationalTable{Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/snap-extra-ds"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns:  []types.InputColumn{{Name: aws.String("col1"), Type: types.InputColumnDataTypeString}},
			}},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DashboardId:  aws.String("snap-extra-dashboard"),
		Name:         aws.String("snap-extra-dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	started, err := client.StartDashboardSnapshotJob(t.Context(), &quicksightsdk.StartDashboardSnapshotJobInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("snap-extra-dashboard"),
		SnapshotJobId: aws.String("snap-extra-job"),
		SnapshotConfiguration: &types.SnapshotConfiguration{
			FileGroups: []types.SnapshotFileGroup{{
				Files: []types.SnapshotFile{{
					SheetSelections: []types.SnapshotFileSheetSelection{{
						SheetId:        aws.String("sheet-1"),
						SelectionScope: types.SnapshotFileSheetSelectionScopeAllVisuals,
					}},
					FormatType: types.SnapshotFileFormatTypeCsv,
				}},
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-extra-job", aws.ToString(started.SnapshotJobId))

	result, err := client.DescribeDashboardSnapshotJobResult(
		t.Context(), &quicksightsdk.DescribeDashboardSnapshotJobResultInput{
			AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("snap-extra-dashboard"),
			SnapshotJobId: aws.String("snap-extra-job"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.SnapshotJobStatusCompleted, result.JobStatus)
	require.NotNil(t, result.Result)

	_, err = client.StartDashboardSnapshotJobSchedule(
		t.Context(), &quicksightsdk.StartDashboardSnapshotJobScheduleInput{
			AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("snap-extra-dashboard"),
			ScheduleId: aws.String("snap-extra-schedule"),
		},
	)
	require.NoError(t, err)
}

// testTopicV2RealClient covers CreateTopicV2, DescribeTopicV2, UpdateTopicV2,
// ListTopicsV2, SearchTopicsV2, DescribeTopicPermissionsV2,
// UpdateTopicPermissionsV2, DeleteTopicV2.
func testTopicV2RealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateTopicV2(t.Context(), &quicksightsdk.CreateTopicV2Input{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("topicv2-1"),
		Topic: &types.TopicV2Details{
			Name: aws.String("topicv2-1-name"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "topicv2-1", aws.ToString(created.TopicId))

	described, err := client.DescribeTopicV2(t.Context(), &quicksightsdk.DescribeTopicV2Input{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topicv2-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Topic)
	assert.Equal(t, "topicv2-1-name", aws.ToString(described.Topic.Name))

	_, err = client.UpdateTopicV2(t.Context(), &quicksightsdk.UpdateTopicV2Input{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topicv2-1"),
		Topic: &types.TopicV2Details{Name: aws.String("topicv2-1-renamed")},
	})
	require.NoError(t, err)

	listed, err := client.ListTopicsV2(t.Context(), &quicksightsdk.ListTopicsV2Input{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listed.TopicSummaryList)

	found, err := client.SearchTopicsV2(t.Context(), &quicksightsdk.SearchTopicsV2Input{
		AwsAccountId: aws.String(qsTestAccountID), Filters: []types.TopicSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, found.TopicSummaryList)

	perms, err := client.DescribeTopicPermissionsV2(t.Context(), &quicksightsdk.DescribeTopicPermissionsV2Input{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topicv2-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateTopicPermissionsV2(t.Context(), &quicksightsdk.UpdateTopicPermissionsV2Input{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topicv2-1"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/topicv2-owner"),
				Actions:   []string{"quicksight:DescribeTopic"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)

	_, err = client.DeleteTopicV2(t.Context(), &quicksightsdk.DeleteTopicV2Input{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topicv2-1"),
	})
	require.NoError(t, err)
}

// testTopicPermissionsAndSearchRealClient covers DescribeTopicPermissions,
// UpdateTopicPermissions, SearchTopics, PredictQAResults.
func testTopicPermissionsAndSearchRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-perms-1"),
		Topic: &types.TopicDetails{Name: aws.String("SalesTopic")},
	})
	require.NoError(t, err)

	perms, err := client.DescribeTopicPermissions(t.Context(), &quicksightsdk.DescribeTopicPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-perms-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, perms.Permissions)

	updatedPerms, err := client.UpdateTopicPermissions(t.Context(), &quicksightsdk.UpdateTopicPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-perms-1"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String("arn:aws:quicksight:us-east-1:000000000000:user/default/topic-owner"),
				Actions:   []string{"quicksight:DescribeTopic"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, updatedPerms.Permissions, 1)

	found, err := client.SearchTopics(t.Context(), &quicksightsdk.SearchTopicsInput{
		AwsAccountId: aws.String(qsTestAccountID), Filters: []types.TopicSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, found.TopicSummaryList)

	qa, err := client.PredictQAResults(t.Context(), &quicksightsdk.PredictQAResultsInput{
		AwsAccountId: aws.String(qsTestAccountID), QueryText: aws.String("how is SalesTopic performing"),
	})
	require.NoError(t, err)
	require.NotNil(t, qa.PrimaryResult)
	assert.Equal(t, types.QAResultTypeGeneratedAnswer, qa.PrimaryResult.ResultType)
	require.NotNil(t, qa.PrimaryResult.GeneratedAnswer)
	assert.Equal(t, "topic-perms-1", aws.ToString(qa.PrimaryResult.GeneratedAnswer.TopicId))
}

// testTopicRefreshRealClient covers DescribeTopicRefresh,
// CreateTopicRefreshSchedule, DescribeTopicRefreshSchedule,
// UpdateTopicRefreshSchedule, ListTopicRefreshSchedules,
// DeleteTopicRefreshSchedule.
func testTopicRefreshRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-refresh-1"),
		Topic: &types.TopicDetails{Name: aws.String("RefreshTopic")},
	})
	require.NoError(t, err)

	refresh, err := client.DescribeTopicRefresh(t.Context(), &quicksightsdk.DescribeTopicRefreshInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), TopicId: aws.String("topic-refresh-1"), RefreshId: aws.String("ref-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, refresh.RefreshDetails)
	assert.Equal(t, "ref-1", aws.ToString(refresh.RefreshDetails.RefreshId))

	// CreateTopicRefreshScheduleInput carries no DatasetId member at all --
	// only DatasetArn/DatasetName (quicksight@v1.129.0
	// api_op_CreateTopicRefreshSchedule.go:35-38); the handler derives the
	// dataset key from this ARN's suffix.
	datasetArn := "arn:aws:quicksight:us-east-1:000000000000:dataset/refresh-ds-1"
	created, err := client.CreateTopicRefreshSchedule(t.Context(), &quicksightsdk.CreateTopicRefreshScheduleInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), TopicId: aws.String("topic-refresh-1"), DatasetArn: aws.String(datasetArn),
		RefreshSchedule: &types.TopicRefreshSchedule{
			IsEnabled:         aws.Bool(true),
			TopicScheduleType: types.TopicScheduleTypeDaily,
			RepeatAt:          aws.String("06:00"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, datasetArn, aws.ToString(created.DatasetArn))

	described, err := client.DescribeTopicRefreshSchedule(t.Context(), &quicksightsdk.DescribeTopicRefreshScheduleInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), TopicId: aws.String("topic-refresh-1"), DatasetId: aws.String("refresh-ds-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.RefreshSchedule)
	assert.True(t, aws.ToBool(described.RefreshSchedule.IsEnabled))
	assert.Equal(t, types.TopicScheduleTypeDaily, described.RefreshSchedule.TopicScheduleType)
	assert.Equal(t, "06:00", aws.ToString(described.RefreshSchedule.RepeatAt))

	_, err = client.UpdateTopicRefreshSchedule(t.Context(), &quicksightsdk.UpdateTopicRefreshScheduleInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), TopicId: aws.String("topic-refresh-1"), DatasetId: aws.String("refresh-ds-1"),
		RefreshSchedule: &types.TopicRefreshSchedule{IsEnabled: aws.Bool(false)},
	})
	require.NoError(t, err)

	listed, err := client.ListTopicRefreshSchedules(t.Context(), &quicksightsdk.ListTopicRefreshSchedulesInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-refresh-1"),
	})
	require.NoError(t, err)
	require.Len(t, listed.RefreshSchedules, 1)
	require.NotNil(t, listed.RefreshSchedules[0].RefreshSchedule)
	assert.False(t, aws.ToBool(listed.RefreshSchedules[0].RefreshSchedule.IsEnabled))
	// TopicScheduleType untouched by the partial update.
	assert.Equal(t, types.TopicScheduleTypeDaily, listed.RefreshSchedules[0].RefreshSchedule.TopicScheduleType)

	_, err = client.DeleteTopicRefreshSchedule(t.Context(), &quicksightsdk.DeleteTopicRefreshScheduleInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), TopicId: aws.String("topic-refresh-1"), DatasetId: aws.String("refresh-ds-1"),
	})
	require.NoError(t, err)
}

// testTopicReviewedAnswersRealClient covers BatchCreateTopicReviewedAnswer,
// BatchDeleteTopicReviewedAnswer, ListTopicReviewedAnswers.
func testTopicReviewedAnswersRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-answers-1"),
		Topic: &types.TopicDetails{Name: aws.String("AnswersTopic")},
	})
	require.NoError(t, err)

	created, err := client.BatchCreateTopicReviewedAnswer(
		t.Context(), &quicksightsdk.BatchCreateTopicReviewedAnswerInput{
			AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-answers-1"),
			Answers: []types.CreateTopicReviewedAnswer{
				{
					AnswerId:   aws.String("ans-1"),
					DatasetArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:dataset/ans-ds"),
					Question:   aws.String("How many orders?"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, created.SucceededAnswers, 1)
	assert.Equal(t, "ans-1", aws.ToString(created.SucceededAnswers[0].AnswerId))
	assert.Empty(t, created.InvalidAnswers)

	listed, err := client.ListTopicReviewedAnswers(t.Context(), &quicksightsdk.ListTopicReviewedAnswersInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-answers-1"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Answers, 1)

	deleted, err := client.BatchDeleteTopicReviewedAnswer(
		t.Context(), &quicksightsdk.BatchDeleteTopicReviewedAnswerInput{
			AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-answers-1"),
			AnswerIds: []string{"ans-1", "ans-does-not-exist"},
		},
	)
	require.NoError(t, err)
	require.Len(t, deleted.SucceededAnswers, 1)
	assert.Equal(t, "ans-1", aws.ToString(deleted.SucceededAnswers[0].AnswerId))
	require.Len(t, deleted.InvalidAnswers, 1)
	assert.Equal(t, "ans-does-not-exist", aws.ToString(deleted.InvalidAnswers[0].AnswerId))
	assert.Equal(t, types.ReviewedAnswerErrorCodeMissingAnswer, deleted.InvalidAnswers[0].Error)
}

// testSearchDataSetsAndSourcesRealClient covers SearchDataSets and
// SearchDataSources.
func testSearchDataSetsAndSourcesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("search-ds-1"),
		Name: aws.String("search-ds-1"), Type: types.DataSourceTypeAthena,
	})
	require.NoError(t, err)

	_, err = client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("search-dset-1"),
		Name: aws.String("search-dset-1"), ImportMode: types.DataSetImportModeSpice,
		PhysicalTableMap: map[string]types.PhysicalTable{
			"pt1": &types.PhysicalTableMemberRelationalTable{Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/search-ds-1"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns:  []types.InputColumn{{Name: aws.String("col1"), Type: types.InputColumnDataTypeString}},
			}},
		},
	})
	require.NoError(t, err)

	foundSets, err := client.SearchDataSets(t.Context(), &quicksightsdk.SearchDataSetsInput{
		AwsAccountId: aws.String(qsTestAccountID), Filters: []types.DataSetSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, foundSets.DataSetSummaries)

	foundSources, err := client.SearchDataSources(t.Context(), &quicksightsdk.SearchDataSourcesInput{
		AwsAccountId: aws.String(qsTestAccountID), Filters: []types.DataSourceSearchFilter{},
	})
	require.NoError(t, err)
	require.NotEmpty(t, foundSources.DataSourceSummaries)
	assert.Equal(t, "search-ds-1", aws.ToString(foundSources.DataSourceSummaries[0].DataSourceId))
}

// testAppTokenGrantRealClient covers UpdateApplicationWithTokenExchangeGrant,
// a genuinely void-result op: its real output carries only the
// RequestId/Status envelope and there is no corresponding Describe/Get op to
// make the grant observable (see handler_dispatch.go's dispatch-table
// comment for this op).
func testAppTokenGrantRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	// Namespace is required by the client's own local validation
	// (validators.go's validateOpUpdateApplicationWithTokenExchangeGrantInput)
	// even though this op has no document serializer at all -- Namespace is
	// never actually placed on the wire (quicksight@v1.129.0 serializers.go's
	// UpdateApplicationWithTokenExchangeGrant path has no query/body binding
	// for it), so its value here is arbitrary.
	_, err := client.UpdateApplicationWithTokenExchangeGrant(
		t.Context(), &quicksightsdk.UpdateApplicationWithTokenExchangeGrantInput{
			AwsAccountId: aws.String(qsTestAccountID),
			Namespace:    aws.String("default"),
		},
	)
	require.NoError(t, err)
}
