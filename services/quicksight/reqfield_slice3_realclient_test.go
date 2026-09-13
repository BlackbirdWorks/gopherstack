package quicksight_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReqFieldSlice3_QuickSight proves, against the real typed
// aws-sdk-go-v2 quicksight client, that the 7 tier-1 findings this pass
// classified as DROPPED PARAMETER (gopherstack-xhu2t slice 3) are now read
// and actually applied. quicksight is not the generic
// `jsonOp(h.Backend.<Op>)` shape, but it has its own analogous blind spot:
// most handlers decode into a `map[string]any` body and pull fields out with
// helpers (strField/mapField/queryParam) rather than a named Go struct field
// -- reqfielddiff can't see that read either. 25 of this service's 37
// tier-1 findings turned out to already be handled that way (see PARITY.md
// dated section); this file only proves the 7 that were genuinely dropped.
// The remaining 5 are MISSING FEATURE, recorded under PARITY.md
// items_still_open, not tested here.
func TestReqFieldSlice3_QuickSight(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testReqField3QSRegisterUserCustomPermissionsName, "register_user_custom_permissions_name"},
		{testReqField3QSUpdateUserCustomPermissionsName, "update_user_custom_permissions_name"},
		{testReqField3QSDeleteAnalysisRecoveryWindow, "delete_analysis_recovery_window_in_days"},
		{testReqField3QSUpdateCustomPermissionsGovernance, "update_custom_permissions_governance"},
		{testReqField3QSDashboardPublishOptions, "create_and_update_dashboard_publish_options"},
		{testReqField3QSGetDashboardEmbedURLNamespace, "get_dashboard_embed_url_namespace"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testReqField3QSRegisterUserCustomPermissionsName(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &quicksightsdk.CreateNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-a"),
		IdentityStore: types.IdentityStoreQuicksight,
	})
	require.NoError(t, err)

	_, err = client.CreateCustomPermissions(ctx, &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("reqfield3-cp"),
		Capabilities:          &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
	})
	require.NoError(t, err)

	registered, err := client.RegisterUser(ctx, &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-a"),
		Email: aws.String("reqfield3-user@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("reqfield3-user"),
		CustomPermissionsName: aws.String("reqfield3-cp"),
	})
	require.NoError(t, err)
	assert.Equal(t, "reqfield3-cp", aws.ToString(registered.User.CustomPermissionsName))

	described, err := client.DescribeUser(ctx, &quicksightsdk.DescribeUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-a"),
		UserName: aws.String("reqfield3-user"),
	})
	require.NoError(t, err)
	assert.Equal(t, "reqfield3-cp", aws.ToString(described.User.CustomPermissionsName))
}

func testReqField3QSUpdateUserCustomPermissionsName(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &quicksightsdk.CreateNamespaceInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-b"),
		IdentityStore: types.IdentityStoreQuicksight,
	})
	require.NoError(t, err)

	_, err = client.CreateCustomPermissions(ctx, &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("reqfield3-cp-2"),
		Capabilities:          &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
	})
	require.NoError(t, err)

	_, err = client.RegisterUser(ctx, &quicksightsdk.RegisterUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-b"),
		Email: aws.String("reqfield3-user2@example.com"), IdentityType: types.IdentityTypeQuicksight,
		UserRole: types.UserRoleReader, UserName: aws.String("reqfield3-user2"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateUser(ctx, &quicksightsdk.UpdateUserInput{
		AwsAccountId: aws.String(qsTestAccountID), Namespace: aws.String("reqfield3-ns-b"),
		UserName: aws.String("reqfield3-user2"), Email: aws.String("reqfield3-user2@example.com"),
		Role:                  types.UserRoleReader,
		CustomPermissionsName: aws.String("reqfield3-cp-2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "reqfield3-cp-2", aws.ToString(updated.User.CustomPermissionsName))
}

func testReqField3QSDeleteAnalysisRecoveryWindow(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateAnalysis(ctx, &quicksightsdk.CreateAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("reqfield3-analysis"),
		Name: aws.String("ReqField3 Analysis"),
		Definition: &types.AnalysisDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	before := time.Now().UTC()

	deletedShort, err := client.DeleteAnalysis(ctx, &quicksightsdk.DeleteAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("reqfield3-analysis"),
		RecoveryWindowInDays: aws.Int64(7),
	})
	require.NoError(t, err)
	require.NotNil(t, deletedShort.DeletionTime)
	shortWindow := deletedShort.DeletionTime.Sub(before)
	assert.InDelta(t, 7*24*time.Hour, shortWindow, float64(time.Hour))

	_, err = client.CreateAnalysis(ctx, &quicksightsdk.CreateAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("reqfield3-analysis-default"),
		Name: aws.String("ReqField3 Analysis Default"),
		Definition: &types.AnalysisDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	before2 := time.Now().UTC()

	deletedDefault, err := client.DeleteAnalysis(ctx, &quicksightsdk.DeleteAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("reqfield3-analysis-default"),
	})
	require.NoError(t, err)
	require.NotNil(t, deletedDefault.DeletionTime)
	defaultWindow := deletedDefault.DeletionTime.Sub(before2)
	assert.InDelta(t, 30*24*time.Hour, defaultWindow, float64(time.Hour))
}

func testReqField3QSUpdateCustomPermissionsGovernance(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateCustomPermissions(ctx, &quicksightsdk.CreateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("reqfield3-governance-cp"),
		Capabilities:          &types.Capabilities{ExportToCsv: types.CapabilityStateDeny},
	})
	require.NoError(t, err)

	_, err = client.UpdateCustomPermissions(ctx, &quicksightsdk.UpdateCustomPermissionsInput{
		AwsAccountId:          aws.String(qsTestAccountID),
		CustomPermissionsName: aws.String("reqfield3-governance-cp"),
		Governance: &types.Governance{
			DefaultCategoryEffects: map[string]types.DefaultCategoryEffect{
				"ACCESS_AND_AUTHORING": types.DefaultCategoryEffectDenyByDefault,
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeCustomPermissions(ctx, &quicksightsdk.DescribeCustomPermissionsInput{
		AwsAccountId: aws.String(qsTestAccountID), CustomPermissionsName: aws.String("reqfield3-governance-cp"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.CustomPermissions.Governance)
	assert.Equal(
		t,
		types.DefaultCategoryEffectDenyByDefault,
		described.CustomPermissions.Governance.DefaultCategoryEffects["ACCESS_AND_AUTHORING"],
	)
}

func testReqField3QSDashboardPublishOptions(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateDashboard(ctx, &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-dashboard"),
		Name: aws.String("ReqField3 Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
		DashboardPublishOptions: &types.DashboardPublishOptions{
			AdHocFilteringOption: &types.AdHocFilteringOption{
				AvailabilityStatus: types.DashboardBehaviorDisabled,
			},
		},
	})
	require.NoError(t, err)

	def, err := client.DescribeDashboardDefinition(ctx, &quicksightsdk.DescribeDashboardDefinitionInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-dashboard"),
	})
	require.NoError(t, err)
	require.NotNil(t, def.DashboardPublishOptions)
	require.NotNil(t, def.DashboardPublishOptions.AdHocFilteringOption)
	assert.Equal(
		t,
		types.DashboardBehaviorDisabled,
		def.DashboardPublishOptions.AdHocFilteringOption.AvailabilityStatus,
	)

	_, err = client.UpdateDashboard(ctx, &quicksightsdk.UpdateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-dashboard"),
		Name: aws.String("ReqField3 Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
		DashboardPublishOptions: &types.DashboardPublishOptions{
			AdHocFilteringOption: &types.AdHocFilteringOption{
				AvailabilityStatus: types.DashboardBehaviorEnabled,
			},
		},
	})
	require.NoError(t, err)

	def2, err := client.DescribeDashboardDefinition(ctx, &quicksightsdk.DescribeDashboardDefinitionInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-dashboard"),
	})
	require.NoError(t, err)
	require.NotNil(t, def2.DashboardPublishOptions)
	require.NotNil(t, def2.DashboardPublishOptions.AdHocFilteringOption)
	assert.Equal(
		t,
		types.DashboardBehaviorEnabled,
		def2.DashboardPublishOptions.AdHocFilteringOption.AvailabilityStatus,
	)
}

func testReqField3QSGetDashboardEmbedURLNamespace(t *testing.T) {
	t.Helper()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateAnalysis(ctx, &quicksightsdk.CreateAnalysisInput{
		AwsAccountId: aws.String(qsTestAccountID), AnalysisId: aws.String("reqfield3-embed-src"),
		Name: aws.String("ReqField3 Embed Source"),
		Definition: &types.AnalysisDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	_, err = client.CreateDashboard(ctx, &quicksightsdk.CreateDashboardInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-embed-dashboard"),
		Name: aws.String("ReqField3 Embed Dashboard"),
		Definition: &types.DashboardVersionDefinition{
			DataSetIdentifierDeclarations: minimalDataSetIdentifierDeclarations(),
		},
	})
	require.NoError(t, err)

	// A real (default) namespace succeeds.
	_, err = client.GetDashboardEmbedUrl(ctx, &quicksightsdk.GetDashboardEmbedUrlInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-embed-dashboard"),
		IdentityType: types.EmbeddingIdentityTypeQuicksight,
		Namespace:    aws.String("default"),
	})
	require.NoError(t, err)

	// An unknown namespace now errors instead of silently succeeding --
	// Namespace was previously decoded nowhere.
	_, err = client.GetDashboardEmbedUrl(ctx, &quicksightsdk.GetDashboardEmbedUrlInput{
		AwsAccountId: aws.String(qsTestAccountID), DashboardId: aws.String("reqfield3-embed-dashboard"),
		IdentityType: types.EmbeddingIdentityTypeQuicksight,
		Namespace:    aws.String("no-such-namespace"),
	})
	require.Error(t, err)
}
