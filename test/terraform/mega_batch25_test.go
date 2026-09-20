package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsvc "github.com/aws/aws-sdk-go-v2/service/quicksight"
	quicksighttypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch25 provisions QuickSight (namespace, group +
// membership, role membership, user, data source, data set, ingestion,
// refresh schedule, analysis, template + alias, dashboard, theme, folder +
// membership, IAM policy assignment, VPC connection) resources via Terraform
// and verifies each through its own SDK client's Describe path.
//
// aws_quicksight_account_settings is deliberately not covered: the provider
// (v5.100.0, internal/service/quicksight/account_settings.go) fails
// client-side before any request reaches the emulator --
// "operation error QuickSight: UpdateAccountSettings, serialization failed:
// serialization failed: input member AwsAccountId must not be empty" --
// even with aws_account_id set explicitly in config. The schema's
// Optional+Computed aws_account_id (schema/stdattributes.go
// AWSAccountIDAttribute) never reaches the SDK input's AwsAccountId field at
// Create time; a provider-side defect, not an emulator gap.
func TestTerraform_MegaBatch25(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-25",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch25QuickSight(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch25QuickSight(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createQuickSightClient(t)
	accountID := "000000000000"

	nsOut, err := client.DescribeNamespace(ctx, &quicksightsvc.DescribeNamespaceInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("mega-batch-25-namespace"),
	})
	require.NoError(t, err, "DescribeNamespace should succeed")
	require.NotNil(t, nsOut.Namespace)

	groupOut, err := client.DescribeGroup(ctx, &quicksightsvc.DescribeGroupInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		GroupName:    aws.String("mega-batch-25-group"),
	})
	require.NoError(t, err, "DescribeGroup should succeed")
	assert.Equal(t, "mega-batch-25-group", aws.ToString(groupOut.Group.GroupName))

	userOut, err := client.DescribeUser(ctx, &quicksightsvc.DescribeUserInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		UserName:     aws.String("mega-batch-25-user"),
	})
	require.NoError(t, err, "DescribeUser should succeed")
	assert.Equal(t, "mega-batch-25-user@example.com", aws.ToString(userOut.User.Email))

	gmOut, err := client.DescribeGroupMembership(ctx, &quicksightsvc.DescribeGroupMembershipInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		GroupName:    aws.String("mega-batch-25-group"),
		MemberName:   aws.String("mega-batch-25-user"),
	})
	require.NoError(t, err, "DescribeGroupMembership should succeed")
	assert.Equal(t, "mega-batch-25-user", aws.ToString(gmOut.GroupMember.MemberName))

	rmOut, err := client.ListRoleMemberships(ctx, &quicksightsvc.ListRoleMembershipsInput{
		AwsAccountId: aws.String(accountID),
		Namespace:    aws.String("default"),
		Role:         quicksighttypes.RoleReader,
	})
	require.NoError(t, err, "ListRoleMemberships should succeed")
	assert.Contains(t, rmOut.MembersList, "mega-batch-25-group")

	dsrcOut, err := client.DescribeDataSource(ctx, &quicksightsvc.DescribeDataSourceInput{
		AwsAccountId: aws.String(accountID),
		DataSourceId: aws.String("mega-batch-25-data-source"),
	})
	require.NoError(t, err, "DescribeDataSource should succeed")
	assert.Equal(t, quicksighttypes.DataSourceTypeS3, dsrcOut.DataSource.Type)

	dsetOut, err := client.DescribeDataSet(ctx, &quicksightsvc.DescribeDataSetInput{
		AwsAccountId: aws.String(accountID),
		DataSetId:    aws.String("mega-batch-25-data-set"),
	})
	require.NoError(t, err, "DescribeDataSet should succeed")
	assert.Equal(t, quicksighttypes.DataSetImportModeSpice, dsetOut.DataSet.ImportMode)

	ingOut, err := client.DescribeIngestion(ctx, &quicksightsvc.DescribeIngestionInput{
		AwsAccountId: aws.String(accountID),
		DataSetId:    aws.String("mega-batch-25-data-set"),
		IngestionId:  aws.String("mega-batch-25-ingestion"),
	})
	require.NoError(t, err, "DescribeIngestion should succeed")
	require.NotNil(t, ingOut.Ingestion)

	refOut, err := client.DescribeRefreshSchedule(ctx, &quicksightsvc.DescribeRefreshScheduleInput{
		AwsAccountId: aws.String(accountID),
		DataSetId:    aws.String("mega-batch-25-data-set"),
		ScheduleId:   aws.String("mega-batch-25-refresh-schedule"),
	})
	require.NoError(t, err, "DescribeRefreshSchedule should succeed")
	require.NotNil(t, refOut.RefreshSchedule)
	assert.Equal(t, quicksighttypes.IngestionTypeFullRefresh, refOut.RefreshSchedule.RefreshType)

	anaOut, err := client.DescribeAnalysis(ctx, &quicksightsvc.DescribeAnalysisInput{
		AwsAccountId: aws.String(accountID),
		AnalysisId:   aws.String("mega-batch-25-analysis"),
	})
	require.NoError(t, err, "DescribeAnalysis should succeed")
	assert.Equal(t, "mega-batch-25-analysis", aws.ToString(anaOut.Analysis.Name))

	tmplOut, err := client.DescribeTemplate(ctx, &quicksightsvc.DescribeTemplateInput{
		AwsAccountId: aws.String(accountID),
		TemplateId:   aws.String("mega-batch-25-template"),
	})
	require.NoError(t, err, "DescribeTemplate should succeed")
	assert.Equal(t, "mega-batch-25-template", aws.ToString(tmplOut.Template.Name))

	tmplAliasOut, err := client.DescribeTemplateAlias(ctx, &quicksightsvc.DescribeTemplateAliasInput{
		AwsAccountId: aws.String(accountID),
		TemplateId:   aws.String("mega-batch-25-template"),
		AliasName:    aws.String("mega-batch-25-alias"),
	})
	require.NoError(t, err, "DescribeTemplateAlias should succeed")
	assert.Equal(t, "mega-batch-25-alias", aws.ToString(tmplAliasOut.TemplateAlias.AliasName))

	dashOut, err := client.DescribeDashboard(ctx, &quicksightsvc.DescribeDashboardInput{
		AwsAccountId: aws.String(accountID),
		DashboardId:  aws.String("mega-batch-25-dashboard"),
	})
	require.NoError(t, err, "DescribeDashboard should succeed")
	assert.Equal(t, "mega-batch-25-dashboard", aws.ToString(dashOut.Dashboard.Name))

	themeOut, err := client.DescribeTheme(ctx, &quicksightsvc.DescribeThemeInput{
		AwsAccountId: aws.String(accountID),
		ThemeId:      aws.String("mega-batch-25-theme"),
	})
	require.NoError(t, err, "DescribeTheme should succeed")
	assert.Equal(t, "mega-batch-25-theme", aws.ToString(themeOut.Theme.Name))

	folderOut, err := client.DescribeFolder(ctx, &quicksightsvc.DescribeFolderInput{
		AwsAccountId: aws.String(accountID),
		FolderId:     aws.String("mega-batch-25-folder"),
	})
	require.NoError(t, err, "DescribeFolder should succeed")
	assert.Equal(t, "mega-batch-25-folder", aws.ToString(folderOut.Folder.Name))

	folderPermOut, err := client.DescribeFolderPermissions(ctx, &quicksightsvc.DescribeFolderPermissionsInput{
		AwsAccountId: aws.String(accountID),
		FolderId:     aws.String("mega-batch-25-folder"),
	})
	require.NoError(t, err, "DescribeFolderPermissions should succeed")
	assert.Equal(t, "mega-batch-25-folder", aws.ToString(folderPermOut.FolderId))

	iamAssignOut, err := client.DescribeIAMPolicyAssignment(ctx, &quicksightsvc.DescribeIAMPolicyAssignmentInput{
		AwsAccountId:   aws.String(accountID),
		Namespace:      aws.String("default"),
		AssignmentName: aws.String("mega-batch-25-iam-policy-assignment"),
	})
	require.NoError(t, err, "DescribeIAMPolicyAssignment should succeed")
	require.NotNil(t, iamAssignOut.IAMPolicyAssignment)
	assert.Equal(t, quicksighttypes.AssignmentStatusEnabled, iamAssignOut.IAMPolicyAssignment.AssignmentStatus)

	vpcConnOut, err := client.DescribeVPCConnection(ctx, &quicksightsvc.DescribeVPCConnectionInput{
		AwsAccountId:    aws.String(accountID),
		VPCConnectionId: aws.String("mega-batch-25-vpc-connection"),
	})
	require.NoError(t, err, "DescribeVPCConnection should succeed")
	assert.Equal(t, "mega-batch-25-vpc-connection", aws.ToString(vpcConnOut.VPCConnection.Name))
}
