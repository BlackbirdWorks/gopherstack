package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qsdocument "github.com/aws/aws-sdk-go-v2/service/quicksight/document"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ConnectionsAndSpaces covers quicksight's highest-priority
// remaining typed-client-uncovered op families (gopherstack-n3zi):
// VPC connections, custom permissions, OAuth apps, asset bundle export
// jobs, refresh schedules, topics, brands, spaces, agents, knowledge
// bases, action connectors, dashboard snapshot jobs, automation jobs, IP
// restriction, Q personalization and SPICE capacity.
func TestRealClient_ConnectionsAndSpaces(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testVPCConnectionsRealClient, "vpc_connections"},
		{testCustomPermissionsRealClient, "custom_permissions"},
		{testOAuthAppsRealClient, "oauth_apps"},
		{testAssetBundleExportRealClient, "asset_bundle_export"},
		{testRefreshSchedulesRealClient, "refresh_schedules"},
		{testTopicsRealClient, "topics"},
		{testBrandsRealClient, "brands"},
		{testSpacesRealClient, "spaces"},
		{testAgentsRealClient, "agents"},
		{testKnowledgeBasesRealClient, "knowledge_bases"},
		{testActionConnectorsRealClient, "action_connectors"},
		{testDashboardSnapshotJobRealClient, "dashboard_snapshot_job"},
		{testAutomationJobRealClient, "automation_job"},
		{testAccountSettingsExtraRealClient, "account_settings_extra"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testVPCConnectionsRealClient covers CreateVPCConnection,
// DescribeVPCConnection, UpdateVPCConnection, ListVPCConnections,
// DeleteVPCConnection.
func testVPCConnectionsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateVPCConnection(t.Context(), &quicksightsdk.CreateVPCConnectionInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		VPCConnectionId:  aws.String("vpcc-1"),
		Name:             aws.String("test-vpc-conn"),
		SubnetIds:        []string{"subnet-1", "subnet-2"},
		SecurityGroupIds: []string{"sg-1"},
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/qs-vpc"),
	})
	require.NoError(t, err)
	assert.Equal(t, "vpcc-1", aws.ToString(created.VPCConnectionId))

	described, err := client.DescribeVPCConnection(t.Context(), &quicksightsdk.DescribeVPCConnectionInput{
		AwsAccountId: aws.String(qsTestAccountID), VPCConnectionId: aws.String("vpcc-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.VPCConnection)
	assert.Equal(t, "test-vpc-conn", aws.ToString(described.VPCConnection.Name))
	// SubnetIds is a Create/Update request field only -- real
	// DescribeVPCConnectionOutput's VPCConnection type has no SubnetIds member at
	// all (types.go:24545-24583); it surfaces subnets only indirectly via
	// NetworkInterfaces[].SubnetId, which this backend doesn't model.
	assert.ElementsMatch(t, []string{"sg-1"}, described.VPCConnection.SecurityGroupIds)

	updated, err := client.UpdateVPCConnection(t.Context(), &quicksightsdk.UpdateVPCConnectionInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		VPCConnectionId:  aws.String("vpcc-1"),
		Name:             aws.String("renamed-vpc-conn"),
		SubnetIds:        []string{"subnet-1"},
		SecurityGroupIds: []string{"sg-1"},
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/qs-vpc"),
	})
	require.NoError(t, err)
	assert.Equal(t, "vpcc-1", aws.ToString(updated.VPCConnectionId))

	listed, err := client.ListVPCConnections(t.Context(), &quicksightsdk.ListVPCConnectionsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.VPCConnectionSummaries, 1)
	assert.Equal(t, "renamed-vpc-conn", aws.ToString(listed.VPCConnectionSummaries[0].Name))

	_, err = client.DeleteVPCConnection(t.Context(), &quicksightsdk.DeleteVPCConnectionInput{
		AwsAccountId: aws.String(qsTestAccountID), VPCConnectionId: aws.String("vpcc-1"),
	})
	require.NoError(t, err)
}

// testCustomPermissionsRealClient covers CreateCustomPermissions,
// DescribeCustomPermissions, UpdateCustomPermissions,
// ListCustomPermissions, DeleteCustomPermissions.
func testCustomPermissionsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateCustomPermissions(t.Context(), &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("cp-1"),
		Capabilities: &types.Capabilities{
			ExportToCsv: types.CapabilityStateDeny,
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(created.Arn))

	described, err := client.DescribeCustomPermissions(t.Context(), &quicksightsdk.DescribeCustomPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), CustomPermissionsName: aws.String("cp-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.CustomPermissions)
	assert.Equal(t, types.CapabilityStateDeny, described.CustomPermissions.Capabilities.ExportToCsv)

	_, err = client.UpdateCustomPermissions(t.Context(), &quicksightsdk.UpdateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("cp-1"),
		Capabilities: &types.Capabilities{
			ExportToCsv:  types.CapabilityStateDeny,
			PrintReports: types.CapabilityStateDeny,
		},
	})
	require.NoError(t, err)

	listed, err := client.ListCustomPermissions(t.Context(), &quicksightsdk.ListCustomPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.CustomPermissionsList, 1)

	_, err = client.DeleteCustomPermissions(t.Context(), &quicksightsdk.DeleteCustomPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), CustomPermissionsName: aws.String("cp-1"),
	})
	require.NoError(t, err)
}

// testOAuthAppsRealClient covers CreateOAuthClientApplication,
// DescribeOAuthClientApplication, UpdateOAuthClientApplication,
// ListOAuthClientApplications, DeleteOAuthClientApplication.
func testOAuthAppsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateOAuthClientApplication(t.Context(), &quicksightsdk.CreateOAuthClientApplicationInput{
		AwsAccountId:                  aws.String(qsTestAccountID),
		OAuthClientApplicationId:      aws.String("oauth-1"),
		Name:                          aws.String("test-oauth-app"),
		ClientId:                      aws.String("client-1"),
		ClientSecret:                  aws.String("test-secret"),
		OAuthClientAuthenticationType: types.OAuthClientAuthenticationTypeToken,
		OAuthTokenEndpointUrl:         aws.String("https://example.com/oauth/token"),
	})
	require.NoError(t, err)
	assert.Equal(t, "oauth-1", aws.ToString(created.OAuthClientApplicationId))

	described, err := client.DescribeOAuthClientApplication(
		t.Context(), &quicksightsdk.DescribeOAuthClientApplicationInput{
			AwsAccountId: aws.String(qsTestAccountID), OAuthClientApplicationId: aws.String("oauth-1"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, described.OAuthClientApplication)
	assert.Equal(t, "test-oauth-app", aws.ToString(described.OAuthClientApplication.Name))

	_, err = client.UpdateOAuthClientApplication(t.Context(), &quicksightsdk.UpdateOAuthClientApplicationInput{
		AwsAccountId:             aws.String(qsTestAccountID),
		OAuthClientApplicationId: aws.String("oauth-1"),
		Name:                     aws.String("renamed-oauth-app"),
	})
	require.NoError(t, err)

	listed, err := client.ListOAuthClientApplications(t.Context(), &quicksightsdk.ListOAuthClientApplicationsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.OAuthClientApplications, 1)

	_, err = client.DeleteOAuthClientApplication(t.Context(), &quicksightsdk.DeleteOAuthClientApplicationInput{
		AwsAccountId: aws.String(qsTestAccountID), OAuthClientApplicationId: aws.String("oauth-1"),
	})
	require.NoError(t, err)
}

// testAssetBundleExportRealClient covers StartAssetBundleExportJob,
// DescribeAssetBundleExportJob, ListAssetBundleExportJobs.
func testAssetBundleExportRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	started, err := client.StartAssetBundleExportJob(t.Context(), &quicksightsdk.StartAssetBundleExportJobInput{
		AwsAccountId:           aws.String(qsTestAccountID),
		AssetBundleExportJobId: aws.String("job-1"),
		ResourceArns:           []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/d-1"},
		ExportFormat:           types.AssetBundleExportFormatQuicksightJson,
	})
	require.NoError(t, err)
	assert.Equal(t, "job-1", aws.ToString(started.AssetBundleExportJobId))

	described, err := client.DescribeAssetBundleExportJob(t.Context(), &quicksightsdk.DescribeAssetBundleExportJobInput{
		AwsAccountId: aws.String(qsTestAccountID), AssetBundleExportJobId: aws.String("job-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AssetBundleExportFormatQuicksightJson, described.ExportFormat)

	listed, err := client.ListAssetBundleExportJobs(t.Context(), &quicksightsdk.ListAssetBundleExportJobsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.AssetBundleExportJobSummaryList, 1)
}

// testRefreshSchedulesRealClient covers CreateRefreshSchedule,
// DescribeRefreshSchedule, UpdateRefreshSchedule, ListRefreshSchedules,
// DeleteRefreshSchedule -- built on top of a real data source + data set,
// like the data_sources/data_sets_extra setup in realclient_datasets_and_dashboards_test.go.
func testRefreshSchedulesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	datasetID := "rs-dataset-1"
	_, err := client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String(datasetID),
		Name:         aws.String("rs-dataset"),
		ImportMode:   types.DataSetImportModeSpice,
		PhysicalTableMap: map[string]types.PhysicalTable{
			"pt1": &types.PhysicalTableMemberRelationalTable{Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/rs-datasource"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns:  []types.InputColumn{{Name: aws.String("col1"), Type: types.InputColumnDataTypeString}},
			}},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateRefreshSchedule(t.Context(), &quicksightsdk.CreateRefreshScheduleInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String(datasetID),
		Schedule: &types.RefreshSchedule{
			ScheduleId:  aws.String("sched-1"),
			RefreshType: types.IngestionTypeFullRefresh,
			ScheduleFrequency: &types.RefreshFrequency{
				Interval: types.RefreshIntervalDaily,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "sched-1", aws.ToString(created.ScheduleId))

	described, err := client.DescribeRefreshSchedule(t.Context(), &quicksightsdk.DescribeRefreshScheduleInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String(datasetID), ScheduleId: aws.String("sched-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.RefreshSchedule)
	assert.Equal(t, types.IngestionTypeFullRefresh, described.RefreshSchedule.RefreshType)

	_, err = client.UpdateRefreshSchedule(t.Context(), &quicksightsdk.UpdateRefreshScheduleInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String(datasetID),
		Schedule: &types.RefreshSchedule{
			ScheduleId:  aws.String("sched-1"),
			RefreshType: types.IngestionTypeIncrementalRefresh,
			ScheduleFrequency: &types.RefreshFrequency{
				Interval: types.RefreshIntervalHourly,
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListRefreshSchedules(t.Context(), &quicksightsdk.ListRefreshSchedulesInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String(datasetID),
	})
	require.NoError(t, err)
	require.Len(t, listed.RefreshSchedules, 1)
	assert.Equal(t, types.IngestionTypeIncrementalRefresh, listed.RefreshSchedules[0].RefreshType)

	_, err = client.DeleteRefreshSchedule(t.Context(), &quicksightsdk.DeleteRefreshScheduleInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String(datasetID), ScheduleId: aws.String("sched-1"),
	})
	require.NoError(t, err)
}

// testTopicsRealClient covers CreateTopic, DescribeTopic, UpdateTopic,
// ListTopics, DeleteTopic.
func testTopicsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateTopic(t.Context(), &quicksightsdk.CreateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("topic-1"),
		Topic:        &types.TopicDetails{Name: aws.String("test-topic")},
	})
	require.NoError(t, err)
	assert.Equal(t, "topic-1", aws.ToString(created.TopicId))

	described, err := client.DescribeTopic(t.Context(), &quicksightsdk.DescribeTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Topic)
	assert.Equal(t, "test-topic", aws.ToString(described.Topic.Name))

	_, err = client.UpdateTopic(t.Context(), &quicksightsdk.UpdateTopicInput{
		AwsAccountId: aws.String(qsTestAccountID),
		TopicId:      aws.String("topic-1"),
		Topic:        &types.TopicDetails{Name: aws.String("renamed-topic")},
	})
	require.NoError(t, err)

	listed, err := client.ListTopics(t.Context(), &quicksightsdk.ListTopicsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.TopicsSummaries, 1)

	_, err = client.DeleteTopic(t.Context(), &quicksightsdk.DeleteTopicInput{
		AwsAccountId: aws.String(qsTestAccountID), TopicId: aws.String("topic-1"),
	})
	require.NoError(t, err)
}

// testBrandsRealClient covers CreateBrand, DescribeBrand, UpdateBrand,
// ListBrands, DeleteBrand.
func testBrandsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateBrand(t.Context(), &quicksightsdk.CreateBrandInput{
		AwsAccountId:    aws.String(qsTestAccountID),
		BrandId:         aws.String("brand-1"),
		BrandDefinition: &types.BrandDefinition{BrandName: aws.String("test-brand")},
	})
	require.NoError(t, err)
	require.NotNil(t, created.BrandDetail)
	assert.Equal(t, "brand-1", aws.ToString(created.BrandDetail.BrandId))

	described, err := client.DescribeBrand(t.Context(), &quicksightsdk.DescribeBrandInput{
		AwsAccountId: aws.String(qsTestAccountID), BrandId: aws.String("brand-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.BrandDefinition)
	assert.Equal(t, "test-brand", aws.ToString(described.BrandDefinition.BrandName))

	_, err = client.UpdateBrand(t.Context(), &quicksightsdk.UpdateBrandInput{
		AwsAccountId:    aws.String(qsTestAccountID),
		BrandId:         aws.String("brand-1"),
		BrandDefinition: &types.BrandDefinition{BrandName: aws.String("renamed-brand")},
	})
	require.NoError(t, err)

	listed, err := client.ListBrands(t.Context(), &quicksightsdk.ListBrandsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.Brands, 1)

	_, err = client.DeleteBrand(t.Context(), &quicksightsdk.DeleteBrandInput{
		AwsAccountId: aws.String(qsTestAccountID), BrandId: aws.String("brand-1"),
	})
	require.NoError(t, err)
}

// testSpacesRealClient covers CreateSpace, DescribeSpace, UpdateSpace,
// ListSpaceResources, DeleteSpace.
func testSpacesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateSpace(t.Context(), &quicksightsdk.CreateSpaceInput{
		AwsAccountId: aws.String(qsTestAccountID),
		SpaceId:      aws.String("space-1"),
		Name:         aws.String("test-space"),
		Description:  aws.String("a space"),
	})
	require.NoError(t, err)
	assert.Equal(t, "space-1", aws.ToString(created.SpaceId))

	described, err := client.DescribeSpace(t.Context(), &quicksightsdk.DescribeSpaceInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Space)
	assert.Equal(t, "test-space", aws.ToString(described.Space.Name))

	_, err = client.UpdateSpace(t.Context(), &quicksightsdk.UpdateSpaceInput{
		AwsAccountId: aws.String(qsTestAccountID),
		SpaceId:      aws.String("space-1"),
		Name:         aws.String("renamed-space"),
	})
	require.NoError(t, err)

	resources, err := client.ListSpaceResources(t.Context(), &quicksightsdk.ListSpaceResourcesInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, resources.SpaceResources)

	_, err = client.DeleteSpace(t.Context(), &quicksightsdk.DeleteSpaceInput{
		AwsAccountId: aws.String(qsTestAccountID), SpaceId: aws.String("space-1"),
	})
	require.NoError(t, err)
}

// testAgentsRealClient covers CreateAgent, DescribeAgent, ListAgents,
// DeleteAgent.
func testAgentsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateAgent(t.Context(), &quicksightsdk.CreateAgentInput{
		AwsAccountId: aws.String(qsTestAccountID),
		AgentId:      aws.String("agent-1"),
		Name:         aws.String("test-agent"),
	})
	require.NoError(t, err)
	assert.Equal(t, "agent-1", aws.ToString(created.AgentId))

	described, err := client.DescribeAgent(t.Context(), &quicksightsdk.DescribeAgentInput{
		AwsAccountId: aws.String(qsTestAccountID), AgentId: aws.String("agent-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Agent)
	assert.Equal(t, "test-agent", aws.ToString(described.Agent.Name))

	listed, err := client.ListAgents(t.Context(), &quicksightsdk.ListAgentsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.AgentSummaries, 1)

	_, err = client.DeleteAgent(t.Context(), &quicksightsdk.DeleteAgentInput{
		AwsAccountId: aws.String(qsTestAccountID), AgentId: aws.String("agent-1"),
	})
	require.NoError(t, err)
}

// testKnowledgeBasesRealClient covers CreateKnowledgeBase,
// DescribeKnowledgeBase, ListKnowledgeBases, DeleteKnowledgeBase.
func testKnowledgeBasesRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateKnowledgeBase(t.Context(), &quicksightsdk.CreateKnowledgeBaseInput{
		AwsAccountId:    aws.String(qsTestAccountID),
		KnowledgeBaseId: aws.String("kb-1"),
		Name:            aws.String("test-kb"),
		DataSourceArn:   aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/ds-1"),
		KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
			TemplateConfiguration: &types.KbTemplateConfiguration{
				Template: qsdocument.NewLazyDocument(map[string]any{
					"type": "S3V2",
					"connectionConfiguration": map[string]any{
						"bucketName":           "test-bucket",
						"bucketOwnerAccountId": qsTestAccountID,
					},
				}),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "kb-1", aws.ToString(created.KnowledgeBaseId))

	described, err := client.DescribeKnowledgeBase(t.Context(), &quicksightsdk.DescribeKnowledgeBaseInput{
		AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.KnowledgeBase)
	assert.Equal(t, "test-kb", aws.ToString(described.KnowledgeBase.Name))

	listed, err := client.ListKnowledgeBases(t.Context(), &quicksightsdk.ListKnowledgeBasesInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.KnowledgeBaseSummaries, 1)

	_, err = client.DeleteKnowledgeBase(t.Context(), &quicksightsdk.DeleteKnowledgeBaseInput{
		AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-1"),
	})
	require.NoError(t, err)
}

// testActionConnectorsRealClient covers CreateActionConnector,
// DescribeActionConnector, ListActionConnectors, DeleteActionConnector.
func testActionConnectorsRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	created, err := client.CreateActionConnector(t.Context(), &quicksightsdk.CreateActionConnectorInput{
		AwsAccountId:      aws.String(qsTestAccountID),
		ActionConnectorId: aws.String("ac-1"),
		Name:              aws.String("test-action-connector"),
		Type:              types.ActionConnectorTypeGenericHttp,
		AuthenticationConfig: &types.AuthConfig{
			AuthenticationType: types.ConnectionAuthTypeNone,
			AuthenticationMetadata: &types.AuthenticationMetadataMemberNoneConnectionMetadata{
				Value: types.NoneConnectionMetadata{BaseEndpoint: aws.String("https://example.com")},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "ac-1", aws.ToString(created.ActionConnectorId))

	described, err := client.DescribeActionConnector(t.Context(), &quicksightsdk.DescribeActionConnectorInput{
		AwsAccountId: aws.String(qsTestAccountID), ActionConnectorId: aws.String("ac-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.ActionConnector)
	assert.Equal(t, "test-action-connector", aws.ToString(described.ActionConnector.Name))

	listed, err := client.ListActionConnectors(t.Context(), &quicksightsdk.ListActionConnectorsInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listed.ActionConnectorSummaries, 1)

	_, err = client.DeleteActionConnector(t.Context(), &quicksightsdk.DeleteActionConnectorInput{
		AwsAccountId: aws.String(qsTestAccountID), ActionConnectorId: aws.String("ac-1"),
	})
	require.NoError(t, err)
}

// testDashboardSnapshotJobRealClient covers StartDashboardSnapshotJob and
// DescribeDashboardSnapshotJob, built on top of a real dashboard (same
// setup realclient_datasets_and_dashboards_test.go's dashboards_extra case already established a pattern
// for).
func testDashboardSnapshotJobRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	datasetID := "snap-dataset-1"
	_, err := client.CreateDataSet(t.Context(), &quicksightsdk.CreateDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DataSetId:    aws.String(datasetID),
		Name:         aws.String("snap-dataset"),
		ImportMode:   types.DataSetImportModeSpice,
		PhysicalTableMap: map[string]types.PhysicalTable{
			"pt1": &types.PhysicalTableMemberRelationalTable{Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/snap-datasource"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns:  []types.InputColumn{{Name: aws.String("col1"), Type: types.InputColumnDataTypeString}},
			}},
		},
	})
	require.NoError(t, err)

	dashboardID := "snap-dashboard-1"
	_, err = client.CreateDashboard(t.Context(), &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID),
		DashboardId:  aws.String(dashboardID),
		Name:         aws.String("snap-dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	started, err := client.StartDashboardSnapshotJob(t.Context(), &quicksightsdk.StartDashboardSnapshotJobInput{
		AwsAccountId:  aws.String(qsTestAccountID),
		DashboardId:   aws.String(dashboardID),
		SnapshotJobId: aws.String("snap-job-1"),
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
	assert.Equal(t, "snap-job-1", aws.ToString(started.SnapshotJobId))

	described, err := client.DescribeDashboardSnapshotJob(t.Context(), &quicksightsdk.DescribeDashboardSnapshotJobInput{
		AwsAccountId: aws.String(
			qsTestAccountID,
		), DashboardId: aws.String(dashboardID), SnapshotJobId: aws.String("snap-job-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, dashboardID, aws.ToString(described.DashboardId))
}

// testAutomationJobRealClient covers StartAutomationJob and
// DescribeAutomationJob.
func testAutomationJobRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	started, err := client.StartAutomationJob(t.Context(), &quicksightsdk.StartAutomationJobInput{
		AwsAccountId:      aws.String(qsTestAccountID),
		AutomationGroupId: aws.String("ag-1"),
		AutomationId:      aws.String("auto-1"),
		InputPayload:      aws.String(`{"key":"value"}`),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(started.JobId))

	described, err := client.DescribeAutomationJob(t.Context(), &quicksightsdk.DescribeAutomationJobInput{
		AwsAccountId:      aws.String(qsTestAccountID),
		AutomationGroupId: aws.String("ag-1"),
		AutomationId:      aws.String("auto-1"),
		JobId:             started.JobId,
	})
	require.NoError(t, err)
	require.NotEmpty(t, described.JobStatus)
}

// testAccountSettingsExtraRealClient covers UpdateIpRestriction,
// DescribeIpRestriction, UpdateQPersonalizationConfiguration,
// DescribeQPersonalizationConfiguration, UpdateSPICECapacityConfiguration.
func testAccountSettingsExtraRealClient(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)

	_, err := client.UpdateIpRestriction(t.Context(), &quicksightsdk.UpdateIpRestrictionInput{
		AwsAccountId:         aws.String(qsTestAccountID),
		Enabled:              aws.Bool(true),
		IpRestrictionRuleMap: map[string]string{"10.0.0.0/8": "internal"},
	})
	require.NoError(t, err)

	described, err := client.DescribeIpRestriction(t.Context(), &quicksightsdk.DescribeIpRestrictionInput{
		AwsAccountId: aws.String(qsTestAccountID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(described.Enabled))
	assert.Equal(t, "internal", described.IpRestrictionRuleMap["10.0.0.0/8"])

	_, err = client.UpdateQPersonalizationConfiguration(
		t.Context(), &quicksightsdk.UpdateQPersonalizationConfigurationInput{
			AwsAccountId:        aws.String(qsTestAccountID),
			PersonalizationMode: types.PersonalizationModeEnabled,
		},
	)
	require.NoError(t, err)

	descPersonalization, err := client.DescribeQPersonalizationConfiguration(
		t.Context(), &quicksightsdk.DescribeQPersonalizationConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.PersonalizationModeEnabled, descPersonalization.PersonalizationMode)

	_, err = client.UpdateSPICECapacityConfiguration(
		t.Context(), &quicksightsdk.UpdateSPICECapacityConfigurationInput{
			AwsAccountId: aws.String(qsTestAccountID),
			PurchaseMode: types.PurchaseModeManual,
		},
	)
	require.NoError(t, err)
}
