package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKinesis_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"ShardLevelMetrics": []string{"IncomingBytes"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

// TestDescribeLimits verifies the DescribeLimits operation returns account shard limits.
func TestDescribeLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeLimits", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ShardLimit     int `json:"ShardLimit"`
		OpenShardCount int `json:"OpenShardCount"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 500, resp.ShardLimit)
	assert.Equal(t, 0, resp.OpenShardCount)
}

// TestDescribeAccountSettings verifies the DescribeAccountSettings operation.
func TestDescribeAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ShardLimit               int `json:"ShardLimit"`
		OnDemandStreamCount      int `json:"OnDemandStreamCount"`
		OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 500, resp.ShardLimit)
	assert.Equal(t, 0, resp.OnDemandStreamCount)
	assert.Positive(t, resp.OnDemandStreamCountLimit)
}

func TestDescribeAccountSettings_OnDemandCount(t *testing.T) {
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

			out, err := b.DescribeAccountSettings(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tt.wantOnDemandCount, out.OnDemandStreamCount)
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

func TestDescribeAccountSettings_OnDemandCount_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 5,
	}))

	// Create 2 ON_DEMAND streams.
	for i := range 2 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("acct-od-stream-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}))
	}

	out, err := b.DescribeAccountSettings(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, out.OnDemandStreamCount)
	assert.Equal(t, 5, out.OnDemandStreamCountLimit)
}

func TestOnDemandLimit_DefaultLimitIsPositive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	out, err := b.DescribeAccountSettings(context.Background())
	require.NoError(t, err)
	assert.Positive(t, out.OnDemandStreamCountLimit, "default ON_DEMAND limit should be positive")
}
