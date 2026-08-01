package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/require"
)

// TestCreateApp_Validation exercises CreateApp's required-field validation.
func TestCreateApp_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *resiliencehubsdk.CreateAppInput
		name  string
	}{
		{name: "missing name", input: &resiliencehubsdk.CreateAppInput{}},
		{name: "nonexistent policy arn", input: &resiliencehubsdk.CreateAppInput{
			Name: aws.String(
				"app",
			), PolicyArn: aws.String("arn:aws:resiliencehub:us-east-1:000000000000:resiliency-policy/nope"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			_, err := client.CreateApp(t.Context(), tt.input)
			require.Error(t, err)
		})
	}
}

// TestDeleteApp_RejectsWhileAssessmentInProgress verifies the real
// FK-integrity check: DeleteApp is rejected (ConflictException) while a
// Pending/InProgress assessment exists, unless ForceDelete is set.
func TestDeleteApp_RejectsWhileAssessmentInProgress(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("app")})
	require.NoError(t, err)

	_, err = client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
		AppArn: appOut.App.AppArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("a1"),
	})
	require.NoError(t, err)

	_, err = client.DeleteApp(ctx, &resiliencehubsdk.DeleteAppInput{AppArn: appOut.App.AppArn})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)

	_, err = client.DeleteApp(
		ctx,
		&resiliencehubsdk.DeleteAppInput{AppArn: appOut.App.AppArn, ForceDelete: aws.Bool(true)},
	)
	require.NoError(t, err)
}

// TestUpdateApp_ClearResiliencyPolicyArn verifies ClearResiliencyPolicyArn is
// handled distinctly from an omitted PolicyArn -- a wire trap this service
// shares with several others (nil means "leave alone"; the explicit bool
// flag means "clear regardless").
func TestUpdateApp_ClearResiliencyPolicyArn(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	policyOut, err := client.CreateResiliencyPolicy(ctx, &resiliencehubsdk.CreateResiliencyPolicyInput{
		PolicyName: aws.String("p1"), Tier: types.ResiliencyPolicyTierCritical,
		Policy: map[string]types.FailurePolicy{
			"Software": {RtoInSecs: 60, RpoInSecs: 60},
			"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
			"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
			"Region":   {RtoInSecs: 60, RpoInSecs: 60},
		},
	})
	require.NoError(t, err)

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{
		Name: aws.String("app"), PolicyArn: policyOut.Policy.PolicyArn,
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(policyOut.Policy.PolicyArn), aws.ToString(appOut.App.PolicyArn))

	// Omitting PolicyArn on update leaves the binding alone.
	updated, err := client.UpdateApp(ctx, &resiliencehubsdk.UpdateAppInput{
		AppArn: appOut.App.AppArn, Description: aws.String("d1"),
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(policyOut.Policy.PolicyArn), aws.ToString(updated.App.PolicyArn))

	// Explicit ClearResiliencyPolicyArn=true clears it.
	cleared, err := client.UpdateApp(ctx, &resiliencehubsdk.UpdateAppInput{
		AppArn: appOut.App.AppArn, ClearResiliencyPolicyArn: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(cleared.App.PolicyArn))
}

// TestListApps_SingleFilter verifies ListApps' name filter matches only the
// app it names.
func TestListApps_SingleFilter(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("app-a")})
	require.NoError(t, err)

	_, err = client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("app-b")})
	require.NoError(t, err)

	listed, err := client.ListApps(ctx, &resiliencehubsdk.ListAppsInput{Name: aws.String("app-a")})
	require.NoError(t, err)
	require.Len(t, listed.AppSummaries, 1)
	require.Equal(t, "app-a", aws.ToString(listed.AppSummaries[0].Name))
}
