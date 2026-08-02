package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

// TestPersistence_SnapshotRestore verifies that an App, a ResiliencyPolicy
// (with its binding), and their tags survive a Snapshot/Restore round-trip
// into a fresh backend.
func TestPersistence_SnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := resiliencehub.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := resiliencehub.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{
		Name: aws.String("persisted-app"),
		Tags: map[string]string{"team": "sre"},
	})
	require.NoError(t, err)

	policyOut, err := client.CreateResiliencyPolicy(ctx, &resiliencehubsdk.CreateResiliencyPolicyInput{
		PolicyName: aws.String("persisted-policy"),
		Tier:       types.ResiliencyPolicyTierCritical,
		Policy: map[string]types.FailurePolicy{
			"Software": {RtoInSecs: 60, RpoInSecs: 60},
			"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
			"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
			"Region":   {RtoInSecs: 60, RpoInSecs: 60},
		},
	})
	require.NoError(t, err)

	data := backend.Snapshot(ctx)
	require.NotEmpty(t, data)

	restored := resiliencehub.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(restored.Close)
	require.NoError(t, restored.Restore(ctx, data))

	restoredApp, err := restored.GetApp(aws.ToString(appOut.App.AppArn))
	require.NoError(t, err)
	require.Equal(t, "persisted-app", restoredApp.Name)

	tagVal, ok := restoredApp.Tags.Get("team")
	require.True(t, ok)
	require.Equal(t, "sre", tagVal)

	restoredPolicy, err := restored.DescribeResiliencyPolicy(aws.ToString(policyOut.Policy.PolicyArn))
	require.NoError(t, err)
	require.Equal(t, "persisted-policy", restoredPolicy.Name)
	require.Len(t, restoredPolicy.Policy, 4)
}
