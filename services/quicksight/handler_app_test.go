package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

const appTestAccountID = "000000000000"

const (
	aliceArn = "arn:aws:quicksight:us-east-1:000000000000:user/default/alice"
	bobArn   = "arn:aws:quicksight:us-east-1:000000000000:user/default/bob"
)

func newAppTestClient(t *testing.T) (*quicksight.InMemoryBackend, *quicksightsdk.Client) {
	t.Helper()

	backend := quicksight.NewInMemoryBackend(appTestAccountID, rtQSTestRegion)
	h := quicksight.NewHandler(backend)

	return backend, newTestQuickSightClient(t, h)
}

// TestRealClient_AppLifecycle proves the Q App family (DescribeApp/
// DeleteApp/ListApps/SearchApps/DescribeAppPermissions/UpdateAppPermissions)
// against the real aws-sdk-go-v2 client. Real AWS has no CreateApp -- apps
// are console-only -- so this backend is seeded via AddAppInternal.
func TestRealClient_AppLifecycle(t *testing.T) {
	t.Parallel()

	backend, client := newAppTestClient(t)
	ctx := t.Context()

	seeded := backend.AddAppInternal(quicksight.App{
		AppID: "app-1",
		Name:  "Trip Planner",
		Permissions: []quicksight.ResourcePermission{
			{
				Principal: aliceArn,
				Actions:   []string{"quicksight:DescribeApp"},
			},
		},
	})
	require.NotEmpty(t, seeded.Arn)

	descOut, err := client.DescribeApp(ctx, &quicksightsdk.DescribeAppInput{
		AwsAccountId: aws.String(appTestAccountID),
		AppId:        aws.String("app-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.App)
	assert.Equal(t, "app-1", aws.ToString(descOut.App.AppId))
	assert.Equal(t, "Trip Planner", aws.ToString(descOut.App.Name))
	assert.Equal(t, types.AppVisibilityPrivate, descOut.App.Visibility)
	assert.Equal(t, seeded.Arn, aws.ToString(descOut.App.Arn))

	listOut, err := client.ListApps(ctx, &quicksightsdk.ListAppsInput{
		AwsAccountId: aws.String(appTestAccountID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AppSummaryList, 1)
	assert.Equal(t, "app-1", aws.ToString(listOut.AppSummaryList[0].AppId))

	searchOut, err := client.SearchApps(ctx, &quicksightsdk.SearchAppsInput{
		AwsAccountId: aws.String(appTestAccountID),
		Filters: []types.SearchAppsFilter{
			{
				Name:     types.SearchAppsFilterNameAppName,
				Operator: types.FilterOperatorStringEquals,
				Value:    aws.String("Trip Planner"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, searchOut.AppSummaryList, 1)

	// A non-matching filter value excludes the record, proving the filter
	// actually filters rather than always returning every app.
	noMatchOut, err := client.SearchApps(ctx, &quicksightsdk.SearchAppsInput{
		AwsAccountId: aws.String(appTestAccountID),
		Filters: []types.SearchAppsFilter{
			{
				Name:     types.SearchAppsFilterNameAppName,
				Operator: types.FilterOperatorStringEquals,
				Value:    aws.String("Nonexistent App"),
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, noMatchOut.AppSummaryList)

	permsOut, err := client.DescribeAppPermissions(ctx, &quicksightsdk.DescribeAppPermissionsInput{
		AwsAccountId: aws.String(appTestAccountID),
		AppId:        aws.String("app-1"),
	})
	require.NoError(t, err)
	require.Len(t, permsOut.Permissions, 1)
	assert.Equal(t, aliceArn, aws.ToString(permsOut.Permissions[0].Principal))

	updateOut, err := client.UpdateAppPermissions(ctx, &quicksightsdk.UpdateAppPermissionsInput{
		AwsAccountId: aws.String(appTestAccountID),
		AppId:        aws.String("app-1"),
		GrantPermissions: []types.ResourcePermission{
			{Principal: aws.String(bobArn), Actions: []string{"quicksight:DescribeApp"}},
		},
		RevokePermissions: []types.ResourcePermission{
			{Principal: aws.String(aliceArn), Actions: []string{"quicksight:DescribeApp"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, updateOut.Permissions, 1)
	assert.Equal(t, bobArn, aws.ToString(updateOut.Permissions[0].Principal))

	_, err = client.DeleteApp(ctx, &quicksightsdk.DeleteAppInput{
		AwsAccountId: aws.String(appTestAccountID),
		AppId:        aws.String("app-1"),
	})
	require.NoError(t, err)

	_, err = client.DescribeApp(ctx, &quicksightsdk.DescribeAppInput{
		AwsAccountId: aws.String(appTestAccountID),
		AppId:        aws.String("app-1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
