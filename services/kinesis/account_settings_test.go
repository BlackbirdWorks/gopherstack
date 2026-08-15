package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestUpdateAccountSettings_RoundTrip drives UpdateAccountSettings /
// DescribeAccountSettings through the real aws-sdk-go-v2 client. Its real
// Input has exactly one member, MinimumThroughputBillingCommitment
// (kinesis@v1.46.4 api_op_UpdateAccountSettings.go:42-51) -- gopherstack used
// to decode a wholly fabricated shape (ShardLimit/OnDemandStreamCount/
// OnDemandStreamCountLimit, none of which are real members) and would have
// silently ignored this request under the old code (gopherstack-nbg8).
func TestUpdateAccountSettings_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	before, err := client.DescribeAccountSettings(t.Context(), &kinesissdk.DescribeAccountSettingsInput{})
	require.NoError(t, err)
	require.NotNil(t, before.MinimumThroughputBillingCommitment)
	assert.Equal(
		t, kinesissdktypes.MinimumThroughputBillingCommitmentOutputStatusDisabled,
		before.MinimumThroughputBillingCommitment.Status,
	)

	upd, err := client.UpdateAccountSettings(t.Context(), &kinesissdk.UpdateAccountSettingsInput{
		MinimumThroughputBillingCommitment: &kinesissdktypes.MinimumThroughputBillingCommitmentInput{
			Status: kinesissdktypes.MinimumThroughputBillingCommitmentInputStatusEnabled,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, upd.MinimumThroughputBillingCommitment)
	assert.Equal(
		t, kinesissdktypes.MinimumThroughputBillingCommitmentOutputStatusEnabled,
		upd.MinimumThroughputBillingCommitment.Status,
	)
	require.NotNil(t, upd.MinimumThroughputBillingCommitment.StartedAt)
	assert.False(t, upd.MinimumThroughputBillingCommitment.StartedAt.IsZero())

	after, err := client.DescribeAccountSettings(t.Context(), &kinesissdk.DescribeAccountSettingsInput{})
	require.NoError(t, err)
	require.NotNil(t, after.MinimumThroughputBillingCommitment)
	assert.Equal(
		t, kinesissdktypes.MinimumThroughputBillingCommitmentOutputStatusEnabled,
		after.MinimumThroughputBillingCommitment.Status,
	)
	assert.Equal(
		t,
		*upd.MinimumThroughputBillingCommitment.StartedAt,
		*after.MinimumThroughputBillingCommitment.StartedAt,
	)

	// Disabling clears the running commitment and stamps EndedAt.
	dis, err := client.UpdateAccountSettings(t.Context(), &kinesissdk.UpdateAccountSettingsInput{
		MinimumThroughputBillingCommitment: &kinesissdktypes.MinimumThroughputBillingCommitmentInput{
			Status: kinesissdktypes.MinimumThroughputBillingCommitmentInputStatusDisabled,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dis.MinimumThroughputBillingCommitment)
	assert.Equal(
		t, kinesissdktypes.MinimumThroughputBillingCommitmentOutputStatusDisabled,
		dis.MinimumThroughputBillingCommitment.Status,
	)
	require.NotNil(t, dis.MinimumThroughputBillingCommitment.EndedAt)
}

// TestUpdateAccountSettings_MissingCommitmentRejected verifies the required
// member is enforced server-side too (a raw/non-SDK client could still omit it).
func TestUpdateAccountSettings_MissingCommitmentRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLimits_DynamicOpenShardCount verifies dynamic shard count.
func TestDescribeLimits_DynamicOpenShardCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var before struct {
		OpenShardCount int `json:"OpenShardCount"`
		ShardLimit     int `json:"ShardLimit"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &before))
	assert.Equal(t, 500, before.ShardLimit)

	// Create a stream with 3 shards.
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "limit-stream", "ShardCount": 3})

	rec = doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var after struct {
		OpenShardCount int `json:"OpenShardCount"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
	assert.Equal(t, before.OpenShardCount+3, after.OpenShardCount)
}

// TestDescribeLimits verifies the DescribeLimits operation returns all four
// required members (kinesis@v1.46.4 api_op_DescribeLimits.go:34-51):
// ShardLimit/OpenShardCount/OnDemandStreamCount/OnDemandStreamCountLimit.
// OnDemandStreamCount and OnDemandStreamCountLimit were previously dropped
// entirely -- a real client would decode zero values for both, not the
// backend's actual state (gopherstack-nbg8: found while auditing the wire
// shape of the sibling account-settings ops).
func TestDescribeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ShardLimit               int `json:"ShardLimit"`
		OpenShardCount           int `json:"OpenShardCount"`
		OnDemandStreamCount      int `json:"OnDemandStreamCount"`
		OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 500, resp.ShardLimit)
	assert.Equal(t, 0, resp.OpenShardCount)
	assert.Equal(t, 0, resp.OnDemandStreamCount)
	assert.Positive(t, resp.OnDemandStreamCountLimit)
}

// TestDescribeLimits_OnDemandStreamCount_RoundTrip drives DescribeLimits
// through the real SDK client and proves OnDemandStreamCount reflects actual
// ON_DEMAND streams (fails against the pre-fix handler, which always sent 0
// regardless of state -- the field was entirely absent from the response).
func TestDescribeLimits_OnDemandStreamCount_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String("od-describelimits"),
		StreamModeDetails: &kinesissdktypes.StreamModeDetails{
			StreamMode: kinesissdktypes.StreamModeOnDemand,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeLimits(t.Context(), &kinesissdk.DescribeLimitsInput{})
	require.NoError(t, err)
	require.NotNil(t, out.OnDemandStreamCount)
	assert.Equal(t, int32(1), *out.OnDemandStreamCount)
	require.NotNil(t, out.OnDemandStreamCountLimit)
	assert.Positive(t, *out.OnDemandStreamCountLimit)
}

// TestDescribeAccountSettings verifies the DescribeAccountSettings operation
// returns its real (and only) member, MinimumThroughputBillingCommitment
// (kinesis@v1.46.4 api_op_DescribeAccountSettings.go:34-45).
func TestDescribeAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MinimumThroughputBillingCommitment struct {
			Status               string  `json:"Status"`
			EarliestAllowedEndAt float64 `json:"EarliestAllowedEndAt"`
			EndedAt              float64 `json:"EndedAt"`
			StartedAt            float64 `json:"StartedAt"`
		} `json:"MinimumThroughputBillingCommitment"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DISABLED", resp.MinimumThroughputBillingCommitment.Status)
	assert.Zero(t, resp.MinimumThroughputBillingCommitment.StartedAt)
	assert.Zero(t, resp.MinimumThroughputBillingCommitment.EndedAt)
	assert.Zero(t, resp.MinimumThroughputBillingCommitment.EarliestAllowedEndAt)
}

// TestCountOnDemandStreams verifies the backend's per-region ON_DEMAND stream
// count, which backs DescribeLimits' OnDemandStreamCount member.
func TestCountOnDemandStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		provisionedCount  int
		onDemandCount     int
		wantOnDemandCount int
	}{
		{name: "no_on_demand", provisionedCount: 2, onDemandCount: 0, wantOnDemandCount: 0},
		{name: "one_on_demand", provisionedCount: 1, onDemandCount: 1, wantOnDemandCount: 1},
		{name: "two_on_demand", provisionedCount: 0, onDemandCount: 2, wantOnDemandCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			for i := range tt.provisionedCount {
				require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
					StreamName: "prov-acct-" + tt.name + "-" + string(rune('a'+i)),
					ShardCount: 1,
					StreamMode: kinesis.StreamModeProvisioned,
				}))
			}
			for i := range tt.onDemandCount {
				require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
					StreamName: "od-acct-" + tt.name + "-" + string(rune('a'+i)),
					ShardCount: 1,
					StreamMode: kinesis.StreamModeOnDemand,
				}))
			}

			assert.Equal(t, tt.wantOnDemandCount, b.CountOnDemandStreams(context.Background()))
		})
	}
}

func TestHandleDescribeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		OpenShardCount int `json:"OpenShardCount"`
		ShardLimit     int `json:"ShardLimit"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.OpenShardCount)
	assert.Equal(t, 500, resp.ShardLimit)
}

// TestDescribeLimits_OnDemandStreamCount_ViaHandler exercises
// SetOnDemandStreamCountLimit (the Go-level replacement for the fabricated
// UpdateAccountSettings.OnDemandStreamCountLimit field) and DescribeLimits
// together.
func TestDescribeLimits_OnDemandStreamCount_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	b.SetOnDemandStreamCountLimit(5)

	// Create 2 ON_DEMAND streams.
	for i := range 2 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("acct-od-stream-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}))
	}

	assert.Equal(t, 2, b.CountOnDemandStreams(context.Background()))
	assert.Equal(t, 5, b.OnDemandStreamCountLimit(context.Background()))
}

func TestOnDemandLimit_DefaultLimitIsPositive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	assert.Positive(t, b.OnDemandStreamCountLimit(context.Background()), "default ON_DEMAND limit should be positive")
}
