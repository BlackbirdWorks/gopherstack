package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/outposts"
)

const ledgerTestAccountID = "111111111111"

// setupOutpostWithCapacity creates a Site/Outpost and configures
// instanceType capacity on its seeded Asset via StartCapacityTask,
// returning the Outpost's ARN and asset ID.
func setupOutpostWithCapacity(
	t *testing.T, client *outpostssdk.Client, instanceType string, count int32,
) (string, string) {
	t.Helper()

	siteID := createTestSite(t, client)
	created := createTestOutpost(t, client, siteID)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
		OutpostIdentifier: created.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, assets.Assets, 1)

	waitForCapacityTaskCompletion(t, client, created.OutpostId, assets.Assets[0].AssetId, instanceType, count)

	return aws.ToString(created.OutpostArn), aws.ToString(assets.Assets[0].AssetId)
}

func TestConsumeCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		configuredType string
		requestType    string
		requestIDs     []string
		configuredQty  int32
	}{
		{
			name: "consumes within available capacity", configuredType: "m5.xlarge", configuredQty: 3,
			requestType: "m5.xlarge", requestIDs: []string{"i-1", "i-2"},
		},
		{
			name: "consumes exactly all available capacity", configuredType: "m5.xlarge", configuredQty: 2,
			requestType: "m5.xlarge", requestIDs: []string{"i-1", "i-2"},
		},
		{
			name: "rejects exceeding available capacity", configuredType: "m5.xlarge", configuredQty: 1,
			requestType: "m5.xlarge", requestIDs: []string{"i-1", "i-2"},
			wantErr: outposts.ErrInsufficientOutpostCapacity,
		},
		{
			name: "rejects an instance type with no configured capacity", configuredType: "m5.xlarge", configuredQty: 5,
			requestType: "c5.large", requestIDs: []string{"i-1"}, wantErr: outposts.ErrInsufficientOutpostCapacity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newTestHandlerAndClient(t)
			outpostArn, _ := setupOutpostWithCapacity(t, client, tt.configuredType, tt.configuredQty)

			err := h.Backend.ConsumeCapacity(outpostArn, tt.requestType, ledgerTestAccountID, tt.requestIDs)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestConsumeCapacity_OutpostNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerAndClient(t)

	err := h.Backend.ConsumeCapacity(
		"arn:aws:outposts:us-east-1:111111111111:outpost/op-doesnotexist00",
		"m5.xlarge", ledgerTestAccountID, []string{"i-1"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, outposts.ErrOutpostNotFound)
}

func TestConsumeCapacity_ZeroInstanceIDsIsNoop(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	outpostArn, _ := setupOutpostWithCapacity(t, client, "m5.xlarge", 2)

	err := h.Backend.ConsumeCapacity(outpostArn, "m5.xlarge", ledgerTestAccountID, nil)
	require.NoError(t, err)
}

// TestConsumeCapacityThenReleaseCapacity is a sequential lifecycle (consume
// -> observe the ledger drop and ListAssetInstances populate -> release ->
// observe both reverse), not a permutation table.
func TestConsumeCapacityThenReleaseCapacity(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	outpostArn, assetID := setupOutpostWithCapacity(t, client, "m5.xlarge", 2)

	err := h.Backend.ConsumeCapacity(outpostArn, "m5.xlarge", ledgerTestAccountID, []string{"i-1", "i-2"})
	require.NoError(t, err)

	// Capacity is fully depleted: GetOutpostInstanceTypes no longer lists it.
	instTypes, err := client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: aws.String(outpostArn),
	})
	require.NoError(t, err)
	assert.Empty(t, instTypes.InstanceTypes, "fully-consumed capacity must not appear as currently configured")

	// A third instance is now rejected.
	err = h.Backend.ConsumeCapacity(outpostArn, "m5.xlarge", ledgerTestAccountID, []string{"i-3"})
	require.ErrorIs(t, err, outposts.ErrInsufficientOutpostCapacity)

	// ListAssetInstances reflects both real running instances.
	listed, err := client.ListAssetInstances(t.Context(), &outpostssdk.ListAssetInstancesInput{
		OutpostIdentifier: aws.String(outpostArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.AssetInstances, 2)

	for _, ai := range listed.AssetInstances {
		assert.Equal(t, ledgerTestAccountID, aws.ToString(ai.AccountId))
		assert.Equal(t, assetID, aws.ToString(ai.AssetId))
		assert.Equal(t, "m5.xlarge", aws.ToString(ai.InstanceType))
		assert.Equal(t, types.AWSServiceNameEc2, ai.AwsServiceName)
	}

	// Release one instance's capacity back.
	h.Backend.ReleaseCapacity("i-1")

	instTypes, err = client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
		OutpostId: aws.String(outpostArn),
	})
	require.NoError(t, err)
	require.Len(t, instTypes.InstanceTypes, 1, "released capacity makes the instance type configured again")
	assert.Equal(t, "m5.xlarge", aws.ToString(instTypes.InstanceTypes[0].InstanceType))

	listed, err = client.ListAssetInstances(t.Context(), &outpostssdk.ListAssetInstancesInput{
		OutpostIdentifier: aws.String(outpostArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.AssetInstances, 1, "the released instance is no longer listed as running")
	assert.Equal(t, "i-2", aws.ToString(listed.AssetInstances[0].InstanceId))

	// The now-freed unit can be consumed again.
	err = h.Backend.ConsumeCapacity(outpostArn, "m5.xlarge", ledgerTestAccountID, []string{"i-3"})
	require.NoError(t, err)
}

func TestReleaseCapacity_UnknownInstanceIsNoop(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerAndClient(t)

	require.NotPanics(t, func() {
		h.Backend.ReleaseCapacity("i-neverlaunched")
	})
}

func TestListAssetInstances_Filters(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	outpostArn, assetID := setupOutpostWithCapacity(t, client, "m5.xlarge", 1)

	err := h.Backend.ConsumeCapacity(outpostArn, "m5.xlarge", ledgerTestAccountID, []string{"i-match"})
	require.NoError(t, err)

	tests := []struct {
		input *outpostssdk.ListAssetInstancesInput
		name  string
		want  int
	}{
		{
			name:  "no filters returns the instance",
			input: &outpostssdk.ListAssetInstancesInput{OutpostIdentifier: aws.String(outpostArn)},
			want:  1,
		},
		{
			name: "matching account filter returns the instance",
			input: &outpostssdk.ListAssetInstancesInput{
				OutpostIdentifier: aws.String(outpostArn), AccountIdFilter: []string{ledgerTestAccountID},
			},
			want: 1,
		},
		{
			name: "non-matching account filter excludes it",
			input: &outpostssdk.ListAssetInstancesInput{
				OutpostIdentifier: aws.String(outpostArn), AccountIdFilter: []string{"999999999999"},
			},
			want: 0,
		},
		{
			name: "matching asset filter returns the instance",
			input: &outpostssdk.ListAssetInstancesInput{
				OutpostIdentifier: aws.String(outpostArn), AssetIdFilter: []string{assetID},
			},
			want: 1,
		},
		{
			name: "non-matching instance type filter excludes it",
			input: &outpostssdk.ListAssetInstancesInput{
				OutpostIdentifier: aws.String(outpostArn), InstanceTypeFilter: []string{"c5.large"},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := client.ListAssetInstances(t.Context(), tt.input)
			require.NoError(t, listErr)
			assert.Len(t, out.AssetInstances, tt.want)
		})
	}
}
