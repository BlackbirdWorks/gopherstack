package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestSlice3_QuickSight_RealClient covers quicksight's highest-priority
// typed-client-uncovered op families (gopherstack-n3zi slice 3): data
// sources, data sets, analyses, dashboards, templates, themes, folders,
// users, groups, namespaces, ingestions, permissions, tags and embedding.
// Each subtest creates real state through the typed aws-sdk-go-v2 client
// and asserts decoded response values.
func TestSlice3_QuickSight_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testNamespacesRealClient, "namespaces"},
		{testGroupMembershipsRealClient, "group_memberships"},
		{testUsersRealClient, "users"},
		{testDataSourcesRealClient, "data_sources"},
		{testDataSetsExtraRealClient, "data_sets_extra"},
		{testIngestionsRealClient, "ingestions"},
		{testAnalysesExtraRealClient, "analyses_extra"},
		{testDashboardsExtraRealClient, "dashboards_extra"},
		{testTemplatesExtraRealClient, "templates_extra"},
		{testThemesRealClient, "themes"},
		{testFoldersExtraRealClient, "folders_extra"},
		{testTagsRealClient, "tags"},
		{testEmbeddingRealClient, "embedding"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

const qsTestAccountID = "000000000000"

func newQuickSightTestClient(t *testing.T) *quicksightsdk.Client {
	t.Helper()

	backend := quicksight.NewInMemoryBackend(qsTestAccountID, rtQSTestRegion)
	h := quicksight.NewHandler(backend)

	return newTestQuickSightClient(t, h)
}

// minimalDataSetIdentifierDeclarations satisfies the client-side
// "DataSetIdentifierDeclarations is required" check shared by Dashboard,
// Analysis and (via DataSetConfigurations) Template definitions -- only
// nil-ness is validated (validators.go's validateDashboardVersionDefinition/
// validateAnalysisDefinition), so an empty-but-non-nil slice is sufficient.
func minimalDataSetIdentifierDeclarations() []types.DataSetIdentifierDeclaration {
	return []types.DataSetIdentifierDeclaration{}
}

// testNamespacesRealClient covers CreateNamespace, DescribeNamespace,
// ListNamespaces and DeleteNamespace.
func testNamespacesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateNamespace(t.Context(), &quicksightsdk.CreateNamespaceInput{
		AwsAccountId:  aws.String(qsTestAccountID),
		Namespace:     aws.String("slice3-ns"),
		IdentityStore: types.IdentityStoreQuicksight,
	})
	require.NoError(t, err)
	assert.Equal(t, types.IdentityStoreQuicksight, created.IdentityStore)
	assert.NotEmpty(t, aws.ToString(created.Arn))

	described, err := client.DescribeNamespace(t.Context(), &quicksightsdk.DescribeNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("slice3-ns"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Namespace)
	assert.Equal(t, "slice3-ns", aws.ToString(described.Namespace.Name))

	listed, err := client.ListNamespaces(t.Context(), &quicksightsdk.ListNamespacesInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	found := false

	for _, ns := range listed.Namespaces {
		if aws.ToString(ns.Name) == "slice3-ns" {
			found = true
		}
	}

	assert.True(t, found, "created namespace must appear in ListNamespaces")

	_, err = client.DeleteNamespace(t.Context(), &quicksightsdk.DeleteNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("slice3-ns"),
	})
	require.NoError(t, err)

	_, err = client.DescribeNamespace(t.Context(), &quicksightsdk.DescribeNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("slice3-ns"),
	})
	require.Error(t, err)
}

// testGroupMembershipsRealClient covers CreateGroupMembership,
// DescribeGroupMembership, ListGroupMemberships, DeleteGroupMembership and
// UpdateGroup.
func testGroupMembershipsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateNamespace(t.Context(), &quicksightsdk.CreateNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		IdentityStore: types.IdentityStoreQuicksight,
	})
	require.NoError(t, err)

	_, err = client.CreateGroup(t.Context(), &quicksightsdk.CreateGroupInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"), Description: aws.String("original"),
	})
	require.NoError(t, err)

	registered, err := client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		Email: aws.String("gm-member@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("gm-member"),
	})
	require.NoError(t, err)
	userName := aws.ToString(registered.User.UserName)

	_, err = client.CreateGroupMembership(t.Context(), &quicksightsdk.CreateGroupMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"), MemberName: aws.String(userName),
	})
	require.NoError(t, err)

	described, err := client.DescribeGroupMembership(t.Context(), &quicksightsdk.DescribeGroupMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"), MemberName: aws.String(userName),
	})
	require.NoError(t, err)
	require.NotNil(t, described.GroupMember)
	assert.Equal(t, userName, aws.ToString(described.GroupMember.MemberName))

	listed, err := client.ListGroupMemberships(t.Context(), &quicksightsdk.ListGroupMembershipsInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"),
	})
	require.NoError(t, err)
	require.Len(t, listed.GroupMemberList, 1)
	assert.Equal(t, userName, aws.ToString(listed.GroupMemberList[0].MemberName))

	updated, err := client.UpdateGroup(t.Context(), &quicksightsdk.UpdateGroupInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"), Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updated.Group.Description))

	_, err = client.DeleteGroupMembership(t.Context(), &quicksightsdk.DeleteGroupMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"), MemberName: aws.String(userName),
	})
	require.NoError(t, err)

	listed2, err := client.ListGroupMemberships(t.Context(), &quicksightsdk.ListGroupMembershipsInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("gm-ns"),
		GroupName: aws.String("engineers"),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.GroupMemberList)
}

// testUsersRealClient covers RegisterUser, DescribeUser, UpdateUser,
// ListUsers, ListUserGroups, DeleteUser and DeleteUserByPrincipalId.
func testUsersRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateNamespace(t.Context(), &quicksightsdk.CreateNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		IdentityStore: types.IdentityStoreQuicksight,
	})
	require.NoError(t, err)

	registered, err := client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		Email: aws.String("slice3-user@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("slice3-user"),
	})
	require.NoError(t, err)
	require.NotNil(t, registered.User)
	userName := aws.ToString(registered.User.UserName)
	principalID := aws.ToString(registered.User.PrincipalId)
	assert.Equal(t, types.UserRoleReader, registered.User.Role)

	described, err := client.DescribeUser(t.Context(), &quicksightsdk.DescribeUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice3-user@example.com", aws.ToString(described.User.Email))

	updated, err := client.UpdateUser(t.Context(), &quicksightsdk.UpdateUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String(userName), Email: aws.String("updated@example.com"), Role: types.UserRoleAuthor,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated@example.com", aws.ToString(updated.User.Email))
	assert.Equal(t, types.UserRoleAuthor, updated.User.Role)

	listed, err := client.ListUsers(t.Context(), &quicksightsdk.ListUsersInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
	})
	require.NoError(t, err)
	found := false

	for _, u := range listed.UserList {
		if aws.ToString(u.UserName) == userName {
			found = true
		}
	}

	assert.True(t, found, "registered user must appear in ListUsers")

	groups, err := client.ListUserGroups(t.Context(), &quicksightsdk.ListUserGroupsInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Empty(t, groups.GroupList)

	_, err = client.DeleteUser(t.Context(), &quicksightsdk.DeleteUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	_, err = client.DescribeUser(t.Context(), &quicksightsdk.DescribeUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String(userName),
	})
	require.Error(t, err)

	registered2, err := client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		Email: aws.String("slice3-user2@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("slice3-user2"),
	})
	require.NoError(t, err)
	_ = principalID

	_, err = client.DeleteUserByPrincipalId(t.Context(), &quicksightsdk.DeleteUserByPrincipalIdInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		PrincipalId: registered2.User.PrincipalId,
	})
	require.NoError(t, err)

	_, err = client.DescribeUser(t.Context(), &quicksightsdk.DescribeUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("users-ns"),
		UserName: aws.String("slice3-user2"),
	})
	require.Error(t, err)
}

// testDataSourcesRealClient covers CreateDataSource, DescribeDataSource,
// UpdateDataSource, ListDataSources, DescribeDataSourcePermissions,
// UpdateDataSourcePermissions and DeleteDataSource.
func testDataSourcesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
		Name: aws.String("Slice3 DataSource"), Type: types.DataSourceTypeAthena,
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeDataSource"},
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.Arn))

	described, err := client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.DataSource)
	assert.Equal(t, types.DataSourceTypeAthena, described.DataSource.Type)
	assert.Equal(t, "Slice3 DataSource", aws.ToString(described.DataSource.Name))

	updated, err := client.UpdateDataSource(t.Context(), &quicksightsdk.UpdateDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
		Name: aws.String("Renamed DataSource"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updated.Arn))

	describedAfterUpdate, err := client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Renamed DataSource", aws.ToString(describedAfterUpdate.DataSource.Name))

	listed, err := client.ListDataSources(t.Context(), &quicksightsdk.ListDataSourcesInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.DataSources, 1)
	assert.Equal(t, "slice3-ds", aws.ToString(listed.DataSources[0].DataSourceId))

	perms, err := client.DescribeDataSourcePermissions(t.Context(), &quicksightsdk.DescribeDataSourcePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)
	assert.Contains(t, perms.Permissions[0].Actions, "quicksight:DescribeDataSource")

	_, err = client.UpdateDataSourcePermissions(t.Context(), &quicksightsdk.UpdateDataSourcePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:UpdateDataSource"},
			},
		},
	})
	require.NoError(t, err)

	permsAfterGrant, err := client.DescribeDataSourcePermissions(
		t.Context(), &quicksightsdk.DescribeDataSourcePermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
		})
	require.NoError(t, err)
	assert.Len(t, permsAfterGrant.Permissions, 2)

	_, err = client.DeleteDataSource(t.Context(), &quicksightsdk.DeleteDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDataSource(t.Context(), &quicksightsdk.DescribeDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-ds"),
	})
	require.Error(t, err)
}

// testDataSetsExtraRealClient covers DeleteDataSet, DescribeDataSetPermissions,
// UpdateDataSetPermissions, ListDataSets, PutDataSetRefreshProperties,
// DescribeDataSetRefreshProperties and DeleteDataSetRefreshProperties.
func testDataSetsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	physicalTableMap := map[string]types.PhysicalTable{
		"pt1": &types.PhysicalTableMemberRelationalTable{
			Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/src1"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns: []types.InputColumn{
					{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger},
				},
			},
		},
	}

	_, err := client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		Name: aws.String("Slice3 DataSet"), ImportMode: types.DataSetImportModeDirectQuery,
		PhysicalTableMap: physicalTableMap,
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeDataSet"},
			},
		},
	})
	require.NoError(t, err)

	perms, err := client.DescribeDataSetPermissions(t.Context(), &quicksightsdk.DescribeDataSetPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)

	_, err = client.UpdateDataSetPermissions(t.Context(), &quicksightsdk.UpdateDataSetPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:DescribeDataSet"},
			},
		},
	})
	require.NoError(t, err)

	permsAfterGrant, err := client.DescribeDataSetPermissions(
		t.Context(), &quicksightsdk.DescribeDataSetPermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		})
	require.NoError(t, err)
	assert.Len(t, permsAfterGrant.Permissions, 2)

	listed, err := client.ListDataSets(t.Context(), &quicksightsdk.ListDataSetsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.DataSetSummaries, 1)
	assert.Equal(t, "slice3-dset", aws.ToString(listed.DataSetSummaries[0].DataSetId))

	_, err = client.PutDataSetRefreshProperties(t.Context(), &quicksightsdk.PutDataSetRefreshPropertiesInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		DataSetRefreshProperties: &types.DataSetRefreshProperties{
			RefreshConfiguration: &types.RefreshConfiguration{
				IncrementalRefresh: &types.IncrementalRefresh{
					LookbackWindow: &types.LookbackWindow{
						ColumnName: aws.String("updated_at"),
						Size:       aws.Int64(1),
						SizeUnit:   types.LookbackWindowSizeUnitDay,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeDataSetRefreshProperties(
		t.Context(), &quicksightsdk.DescribeDataSetRefreshPropertiesInput{
			AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		})
	require.NoError(t, err)
	require.NotNil(t, described.DataSetRefreshProperties)
	require.NotNil(t, described.DataSetRefreshProperties.RefreshConfiguration)
	require.NotNil(t, described.DataSetRefreshProperties.RefreshConfiguration.IncrementalRefresh)
	assert.Equal(t, "updated_at",
		aws.ToString(
			described.DataSetRefreshProperties.RefreshConfiguration.IncrementalRefresh.LookbackWindow.ColumnName,
		))

	_, err = client.DeleteDataSetRefreshProperties(
		t.Context(), &quicksightsdk.DeleteDataSetRefreshPropertiesInput{
			AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
		})
	require.NoError(t, err)

	_, err = client.DeleteDataSet(t.Context(), &quicksightsdk.DeleteDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDataSet(t.Context(), &quicksightsdk.DescribeDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-dset"),
	})
	require.Error(t, err)
}

// testIngestionsRealClient covers CreateIngestion, DescribeIngestion,
// ListIngestions and CancelIngestion.
func testIngestionsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	physicalTableMap := map[string]types.PhysicalTable{
		"pt1": &types.PhysicalTableMemberRelationalTable{
			Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/src1"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns: []types.InputColumn{
					{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger},
				},
			},
		},
	}

	_, err := client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-ing-dset"),
		Name: aws.String("Slice3 Ingestion DataSet"), ImportMode: types.DataSetImportModeDirectQuery,
		PhysicalTableMap: physicalTableMap,
	})
	require.NoError(t, err)

	created, err := client.CreateIngestion(t.Context(), &quicksightsdk.CreateIngestionInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-ing-dset"),
		IngestionId: aws.String("slice3-ingestion-1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.Arn))

	described, err := client.DescribeIngestion(t.Context(), &quicksightsdk.DescribeIngestionInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-ing-dset"),
		IngestionId: aws.String("slice3-ingestion-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Ingestion)
	assert.Equal(t, "slice3-ingestion-1", aws.ToString(described.Ingestion.IngestionId))

	listed, err := client.ListIngestions(t.Context(), &quicksightsdk.ListIngestionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-ing-dset"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Ingestions, 1)

	_, err = client.CancelIngestion(t.Context(), &quicksightsdk.CancelIngestionInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("slice3-ing-dset"),
		IngestionId: aws.String("slice3-ingestion-1"),
	})
	require.NoError(t, err)
}

// testAnalysesExtraRealClient covers DeleteAnalysis, DescribeAnalysisPermissions,
// UpdateAnalysisPermissions, ListAnalyses, RestoreAnalysis and SearchAnalyses.
func testAnalysesExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateAnalysis(t.Context(), &quicksightsdk.CreateAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-analysis"),
		Name: aws.String("Slice3 Analysis"),
		Definition: &types.AnalysisDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeAnalysis"},
			},
		},
	})
	require.NoError(t, err)

	perms, err := client.DescribeAnalysisPermissions(t.Context(), &quicksightsdk.DescribeAnalysisPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-analysis"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)

	_, err = client.UpdateAnalysisPermissions(t.Context(), &quicksightsdk.UpdateAnalysisPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-analysis"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:DescribeAnalysis"},
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListAnalyses(t.Context(), &quicksightsdk.ListAnalysesInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	found := false

	for _, a := range listed.AnalysisSummaryList {
		if aws.ToString(a.AnalysisId) == "slice3-analysis" {
			found = true
		}
	}

	assert.True(t, found)

	searched, err := client.SearchAnalyses(t.Context(), &quicksightsdk.SearchAnalysesInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Filters:      []types.AnalysisSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotNil(t, searched.AnalysisSummaryList)

	_, err = client.DeleteAnalysis(t.Context(), &quicksightsdk.DeleteAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-analysis"),
	})
	require.NoError(t, err)

	restored, err := client.RestoreAnalysis(t.Context(), &quicksightsdk.RestoreAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-analysis"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice3-analysis", aws.ToString(restored.AnalysisId))
}

// testDashboardsExtraRealClient covers DescribeDashboardDefinition,
// ListDashboards, ListDashboardVersions, SearchDashboards,
// UpdateDashboardPublishedVersion, UpdateDashboardLinks and
// UpdatePublicSharingSettings.
func testDashboardsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateAnalysis(t.Context(), &quicksightsdk.CreateAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("slice3-dash-src-analysis"),
		Name: aws.String("Link Source Analysis"),
		Definition: &types.AnalysisDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	_, err = client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-dashboard"),
		Name: aws.String("Slice3 Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	def, err := client.DescribeDashboardDefinition(t.Context(), &quicksightsdk.DescribeDashboardDefinitionInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-dashboard"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Slice3 Dashboard", aws.ToString(def.Name))
	require.NotNil(t, def.Definition)

	listed, err := client.ListDashboards(t.Context(), &quicksightsdk.ListDashboardsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	found := false

	for _, d := range listed.DashboardSummaryList {
		if aws.ToString(d.DashboardId) == "slice3-dashboard" {
			found = true
		}
	}

	assert.True(t, found)

	versions, err := client.ListDashboardVersions(t.Context(), &quicksightsdk.ListDashboardVersionsInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-dashboard"),
	})
	require.NoError(t, err)
	require.Len(t, versions.DashboardVersionSummaryList, 1)

	searched, err := client.SearchDashboards(t.Context(), &quicksightsdk.SearchDashboardsInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Filters:      []types.DashboardSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotNil(t, searched.DashboardSummaryList)

	updatedVersion, err := client.UpdateDashboardPublishedVersion(
		t.Context(), &quicksightsdk.UpdateDashboardPublishedVersionInput{
			AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-dashboard"),
			VersionNumber: aws.Int64(1),
		})
	require.NoError(t, err)
	assert.Equal(t, "slice3-dashboard", aws.ToString(updatedVersion.DashboardId))

	analysisArn := "arn:aws:quicksight:us-east-1:000000000000:analysis/slice3-dash-src-analysis"

	linked, err := client.UpdateDashboardLinks(t.Context(), &quicksightsdk.UpdateDashboardLinksInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-dashboard"),
		LinkEntities: []string{analysisArn},
	})
	require.NoError(t, err)
	assert.Contains(t, linked.LinkEntities, analysisArn)

	_, err = client.UpdatePublicSharingSettings(t.Context(), &quicksightsdk.UpdatePublicSharingSettingsInput{
		AwsAccountId: aws.String(qsTestAccountID), PublicSharingEnabled: true,
	})
	require.NoError(t, err)
}

// testTemplatesExtraRealClient covers DescribeTemplateDefinition,
// DescribeTemplatePermissions, UpdateTemplatePermissions, ListTemplates,
// CreateTemplateAlias, DescribeTemplateAlias, UpdateTemplateAlias,
// DeleteTemplateAlias and ListTemplateAliases.
func testTemplatesExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateTemplate(t.Context(), &quicksightsdk.CreateTemplateInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		Name: aws.String("Slice3 Template"),
		Definition: &types.TemplateVersionDefinition{
			DataSetConfigurations: []types.DataSetConfiguration{},
		},
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeTemplate"},
			},
		},
	})
	require.NoError(t, err)

	def, err := client.DescribeTemplateDefinition(t.Context(), &quicksightsdk.DescribeTemplateDefinitionInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Slice3 Template", aws.ToString(def.Name))

	perms, err := client.DescribeTemplatePermissions(t.Context(), &quicksightsdk.DescribeTemplatePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)

	_, err = client.UpdateTemplatePermissions(t.Context(), &quicksightsdk.UpdateTemplatePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:DescribeTemplate"},
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTemplates(t.Context(), &quicksightsdk.ListTemplatesInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	found := false

	for _, tmpl := range listed.TemplateSummaryList {
		if aws.ToString(tmpl.TemplateId) == "slice3-template" {
			found = true
		}
	}

	assert.True(t, found)

	alias, err := client.CreateTemplateAlias(t.Context(), &quicksightsdk.CreateTemplateAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		AliasName: aws.String("PROD"), TemplateVersionNumber: aws.Int64(1),
	})
	require.NoError(t, err)
	require.NotNil(t, alias.TemplateAlias)
	assert.Equal(t, "PROD", aws.ToString(alias.TemplateAlias.AliasName))

	describedAlias, err := client.DescribeTemplateAlias(t.Context(), &quicksightsdk.DescribeTemplateAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		AliasName: aws.String("PROD"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, aws.ToInt64(describedAlias.TemplateAlias.TemplateVersionNumber))

	_, err = client.UpdateTemplate(t.Context(), &quicksightsdk.UpdateTemplateInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		Name: aws.String("Slice3 Template v2"),
		Definition: &types.TemplateVersionDefinition{
			DataSetConfigurations: []types.DataSetConfiguration{},
		},
	})
	require.NoError(t, err)

	updatedAlias, err := client.UpdateTemplateAlias(t.Context(), &quicksightsdk.UpdateTemplateAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		AliasName: aws.String("PROD"), TemplateVersionNumber: aws.Int64(2),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, aws.ToInt64(updatedAlias.TemplateAlias.TemplateVersionNumber))

	// 2, not 1: CreateTemplate implicitly creates the "$LATEST" alias
	// alongside the explicit "PROD" one created above.
	listedAliases, err := client.ListTemplateAliases(t.Context(), &quicksightsdk.ListTemplateAliasesInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
	})
	require.NoError(t, err)
	require.Len(t, listedAliases.TemplateAliasList, 2)

	_, err = client.DeleteTemplateAlias(t.Context(), &quicksightsdk.DeleteTemplateAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
		AliasName: aws.String("PROD"),
	})
	require.NoError(t, err)

	listedAliases2, err := client.ListTemplateAliases(t.Context(), &quicksightsdk.ListTemplateAliasesInput{
		AwsAccountId: aws.String(qsTestAccountID), TemplateId: aws.String("slice3-template"),
	})
	require.NoError(t, err)
	require.Len(t, listedAliases2.TemplateAliasList, 1)
	assert.Equal(t, "$LATEST", aws.ToString(listedAliases2.TemplateAliasList[0].AliasName))
}

// testThemesRealClient covers CreateTheme, DescribeTheme, UpdateTheme,
// DeleteTheme, DescribeThemePermissions, UpdateThemePermissions,
// CreateThemeAlias, DescribeThemeAlias, UpdateThemeAlias, DeleteThemeAlias,
// ListThemeAliases and ListThemeVersions.
func testThemesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateTheme(t.Context(), &quicksightsdk.CreateThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		Name: aws.String("Slice3 Theme"), BaseThemeId: aws.String("SEASIDE"),
		Configuration: &types.ThemeConfiguration{},
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeTheme"},
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.ThemeId))

	described, err := client.DescribeTheme(t.Context(), &quicksightsdk.DescribeThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Theme)
	assert.Equal(t, "Slice3 Theme", aws.ToString(described.Theme.Name))

	_, err = client.UpdateTheme(t.Context(), &quicksightsdk.UpdateThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		Name: aws.String("Slice3 Theme v2"), BaseThemeId: aws.String("SEASIDE"),
		Configuration: &types.ThemeConfiguration{},
	})
	require.NoError(t, err)

	describedAfterUpdate, err := client.DescribeTheme(t.Context(), &quicksightsdk.DescribeThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Slice3 Theme v2", aws.ToString(describedAfterUpdate.Theme.Name))

	versions, err := client.ListThemeVersions(t.Context(), &quicksightsdk.ListThemeVersionsInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	require.Len(t, versions.ThemeVersionSummaryList, 2)

	perms, err := client.DescribeThemePermissions(t.Context(), &quicksightsdk.DescribeThemePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)

	updatedPerms, err := client.UpdateThemePermissions(t.Context(), &quicksightsdk.UpdateThemePermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:DescribeTheme"},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, updatedPerms.Permissions, 2)

	alias, err := client.CreateThemeAlias(t.Context(), &quicksightsdk.CreateThemeAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		AliasName: aws.String("PROD"), ThemeVersionNumber: aws.Int64(1),
	})
	require.NoError(t, err)
	require.NotNil(t, alias.ThemeAlias)

	describedAlias, err := client.DescribeThemeAlias(t.Context(), &quicksightsdk.DescribeThemeAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		AliasName: aws.String("PROD"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, aws.ToInt64(describedAlias.ThemeAlias.ThemeVersionNumber))

	updatedAlias, err := client.UpdateThemeAlias(t.Context(), &quicksightsdk.UpdateThemeAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		AliasName: aws.String("PROD"), ThemeVersionNumber: aws.Int64(2),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, aws.ToInt64(updatedAlias.ThemeAlias.ThemeVersionNumber))

	// 2, not 1: CreateTheme implicitly creates the "$LATEST" alias alongside
	// the explicit "PROD" one created above.
	listedAliases, err := client.ListThemeAliases(t.Context(), &quicksightsdk.ListThemeAliasesInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	require.Len(t, listedAliases.ThemeAliasList, 2)

	_, err = client.DeleteThemeAlias(t.Context(), &quicksightsdk.DeleteThemeAliasInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
		AliasName: aws.String("PROD"),
	})
	require.NoError(t, err)

	listedAliases2, err := client.ListThemeAliases(t.Context(), &quicksightsdk.ListThemeAliasesInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)
	require.Len(t, listedAliases2.ThemeAliasList, 1)
	assert.Equal(t, "$LATEST", aws.ToString(listedAliases2.ThemeAliasList[0].AliasName))

	_, err = client.DeleteTheme(t.Context(), &quicksightsdk.DeleteThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.NoError(t, err)

	_, err = client.DescribeTheme(t.Context(), &quicksightsdk.DescribeThemeInput{
		AwsAccountId: aws.String(qsTestAccountID), ThemeId: aws.String("slice3-theme"),
	})
	require.Error(t, err)
}

// testFoldersExtraRealClient covers UpdateFolder, DescribeFolderPermissions,
// UpdateFolderPermissions, DescribeFolderResolvedPermissions,
// DeleteFolderMembership, ListFoldersForResource and SearchFolders.
func testFoldersExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateFolder(t.Context(), &quicksightsdk.CreateFolderInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		Name: aws.String("Slice3 Folder"), FolderType: types.FolderTypeShared,
		Permissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/owner",
				),
				Actions: []string{"quicksight:DescribeFolder"},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateFolder(t.Context(), &quicksightsdk.UpdateFolderInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		Name: aws.String("Slice3 Folder Renamed"),
	})
	require.NoError(t, err)

	described, err := client.DescribeFolder(t.Context(), &quicksightsdk.DescribeFolderInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Slice3 Folder Renamed", aws.ToString(described.Folder.Name))

	perms, err := client.DescribeFolderPermissions(t.Context(), &quicksightsdk.DescribeFolderPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
	})
	require.NoError(t, err)
	require.Len(t, perms.Permissions, 1)

	updatedPerms, err := client.UpdateFolderPermissions(t.Context(), &quicksightsdk.UpdateFolderPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		GrantPermissions: []types.ResourcePermission{
			{
				Principal: aws.String(
					"arn:aws:quicksight:us-east-1:000000000000:user/default/second-owner",
				),
				Actions: []string{"quicksight:DescribeFolder"},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, updatedPerms.Permissions, 2)

	resolved, err := client.DescribeFolderResolvedPermissions(
		t.Context(), &quicksightsdk.DescribeFolderResolvedPermissionsInput{
			AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		})
	require.NoError(t, err)
	assert.NotEmpty(t, resolved.Permissions)

	_, err = client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-folder-dashboard"),
		Name: aws.String("Folder Member Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	dashboardArn := "arn:aws:quicksight:us-east-1:000000000000:dashboard/slice3-folder-dashboard"

	_, err = client.CreateFolderMembership(t.Context(), &quicksightsdk.CreateFolderMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		MemberId: aws.String("slice3-folder-dashboard"), MemberType: types.MemberTypeDashboard,
	})
	require.NoError(t, err)

	foldersForResource, err := client.ListFoldersForResource(t.Context(), &quicksightsdk.ListFoldersForResourceInput{
		AwsAccountId: aws.String(qsTestAccountID), ResourceArn: aws.String(dashboardArn),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, foldersForResource.Folders)

	_, err = client.DeleteFolderMembership(t.Context(), &quicksightsdk.DeleteFolderMembershipInput{
		AwsAccountId: aws.String(qsTestAccountID), FolderId: aws.String("slice3-folder"),
		MemberId: aws.String("slice3-folder-dashboard"), MemberType: types.MemberTypeDashboard,
	})
	require.NoError(t, err)

	searched, err := client.SearchFolders(t.Context(), &quicksightsdk.SearchFoldersInput{
		AwsAccountId: aws.String(qsTestAccountID),
		Filters:      []types.FolderSearchFilter{},
	})
	require.NoError(t, err)
	assert.NotNil(t, searched.FolderSummaryList)
}

// testTagsRealClient covers TagResource, UntagResource and ListTagsForResource.
func testTagsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateDataSource(t.Context(), &quicksightsdk.CreateDataSourceInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSourceId: aws.String("slice3-tags-ds"),
		Name: aws.String("Slice3 Tags DataSource"), Type: types.DataSourceTypeAthena,
	})
	require.NoError(t, err)
	resourceArn := aws.ToString(created.Arn)

	_, err = client.TagResource(t.Context(), &quicksightsdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(t.Context(), &quicksightsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listed.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(listed.Tags[0].Value))

	_, err = client.UntagResource(t.Context(), &quicksightsdk.UntagResourceInput{
		ResourceArn: aws.String(resourceArn), TagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listed2, err := client.ListTagsForResource(t.Context(), &quicksightsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	assert.Empty(t, listed2.Tags)
}

// testEmbeddingRealClient covers GenerateEmbedUrlForAnonymousUser,
// GenerateEmbedUrlForRegisteredUser, GenerateEmbedUrlForRegisteredUserWithIdentity,
// GetDashboardEmbedUrl, GetSessionEmbedUrl and GetIdentityContext.
func testEmbeddingRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-embed-dashboard"),
		Name: aws.String("Embed Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)
	dashboardArn := "arn:aws:quicksight:us-east-1:000000000000:dashboard/slice3-embed-dashboard"

	anon, err := client.GenerateEmbedUrlForAnonymousUser(
		t.Context(), &quicksightsdk.GenerateEmbedUrlForAnonymousUserInput{
			AwsAccountId:           aws.String(qsTestAccountID),
			Namespace:              aws.String("default"),
			AuthorizedResourceArns: []string{dashboardArn},
			ExperienceConfiguration: &types.AnonymousUserEmbeddingExperienceConfiguration{
				Dashboard: &types.AnonymousUserDashboardEmbeddingConfiguration{
					InitialDashboardId: aws.String("slice3-embed-dashboard"),
				},
			},
		})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(anon.EmbedUrl))
	assert.NotEmpty(t, aws.ToString(anon.AnonymousUserArn))

	registered, err := client.RegisterUser(t.Context(), &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("default"),
		Email: aws.String("embed-user@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("embed-user"),
	})
	require.NoError(t, err)
	userArn := aws.ToString(registered.User.Arn)

	reg, err := client.GenerateEmbedUrlForRegisteredUser(
		t.Context(), &quicksightsdk.GenerateEmbedUrlForRegisteredUserInput{
			AwsAccountId: aws.String(qsTestAccountID), UserArn: aws.String(userArn),
			ExperienceConfiguration: &types.RegisteredUserEmbeddingExperienceConfiguration{
				Dashboard: &types.RegisteredUserDashboardEmbeddingConfiguration{
					InitialDashboardId: aws.String("slice3-embed-dashboard"),
				},
			},
		})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(reg.EmbedUrl))

	withIdentity, err := client.GenerateEmbedUrlForRegisteredUserWithIdentity(
		t.Context(), &quicksightsdk.GenerateEmbedUrlForRegisteredUserWithIdentityInput{
			AwsAccountId: aws.String(qsTestAccountID),
			ExperienceConfiguration: &types.RegisteredUserEmbeddingExperienceConfiguration{
				Dashboard: &types.RegisteredUserDashboardEmbeddingConfiguration{
					InitialDashboardId: aws.String("slice3-embed-dashboard"),
				},
			},
		})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(withIdentity.EmbedUrl))

	dashboardEmbed, err := client.GetDashboardEmbedUrl(t.Context(), &quicksightsdk.GetDashboardEmbedUrlInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("slice3-embed-dashboard"),
		IdentityType: types.EmbeddingIdentityTypeQuicksight,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(dashboardEmbed.EmbedUrl))

	session, err := client.GetSessionEmbedUrl(t.Context(), &quicksightsdk.GetSessionEmbedUrlInput{
		AwsAccountId: aws.String(qsTestAccountID), EntryPoint: aws.String("/start"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(session.EmbedUrl))

	identityCtx, err := client.GetIdentityContext(t.Context(), &quicksightsdk.GetIdentityContextInput{
		AwsAccountId:   aws.String(qsTestAccountID),
		UserIdentifier: &types.UserIdentifierMemberUserArn{Value: userArn},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(identityCtx.Context))
}
