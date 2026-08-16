package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestUpdateShardCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		initialShards    int
		targetShards     int
		wantCode         int
		wantCurrentCount int
		wantTargetCount  int
	}{
		{
			name:             "scale_up",
			initialShards:    2,
			targetShards:     4,
			wantCode:         http.StatusOK,
			wantCurrentCount: 2,
			wantTargetCount:  4,
		},
		{
			name:             "scale_down",
			initialShards:    4,
			targetShards:     2,
			wantCode:         http.StatusOK,
			wantCurrentCount: 4,
			wantTargetCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamName := "reshard-stream-" + tt.name

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": streamName,
				"ShardCount": tt.initialShards,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "UpdateShardCount", map[string]any{
				"StreamName":       streamName,
				"TargetShardCount": tt.targetShards,
				"ScalingType":      "UNIFORM_SCALING",
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				StreamName        string `json:"StreamName"`
				CurrentShardCount int    `json:"CurrentShardCount"`
				TargetShardCount  int    `json:"TargetShardCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, streamName, resp.StreamName)
			assert.Equal(t, tt.wantCurrentCount, resp.CurrentShardCount)
			assert.Equal(t, tt.wantTargetCount, resp.TargetShardCount)

			// Verify new shard count via ListShards.
			rec = doRequest(t, h, "ListShards", map[string]any{"StreamName": streamName})
			require.Equal(t, http.StatusOK, rec.Code)

			var shardsResp struct {
				Shards []any `json:"Shards"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &shardsResp))
			assert.Len(t, shardsResp.Shards, tt.targetShards)
		})
	}
}

func TestUpdateShardCountErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "stream_not_found",
			body:     map[string]any{"StreamName": "no-such-stream", "TargetShardCount": 2},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_target",
			body:     map[string]any{"StreamName": "x", "TargetShardCount": 0},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unsupported_scaling_type",
			body:     map[string]any{"StreamName": "x", "TargetShardCount": 2, "ScalingType": "RANDOM_SCALING"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateShardCount", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestUpdateShardCount_OldShardsMarkedClosed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-shardcount-closed",
		ShardCount: 2,
	}))

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-shardcount-closed"},
	)
	require.NoError(t, err)
	require.Len(t, out.Shards, 2)

	// Scale up to 4.
	_, err = b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-shardcount-closed",
		TargetShardCount: 4,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// DescribeStream must include old closed shards + new open ones.
	out2, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-shardcount-closed"},
	)
	require.NoError(t, err)

	openCount := 0
	closedCount := 0
	for _, s := range out2.Shards {
		if s.Closed {
			closedCount++
		} else {
			openCount++
		}
	}

	assert.Equal(t, 4, openCount, "should have 4 new open shards")
	assert.Equal(t, 2, closedCount, "old 2 shards should be marked closed")
	assert.Len(t, out2.Shards, 6, "total 6 shards (2 closed + 4 open)")
}

func TestUpdateShardCount_ListShardsOnlyReturnsOpenShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-listshard-stream",
		ShardCount: 2,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-listshard-stream",
		TargetShardCount: 3,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// ListShards default = open shards only.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "update-listshard-stream"})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 3, "ListShards should return only the 3 new open shards")
}

func TestUpdateShardCount_CurrentCountIsOpenShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-currentcount-stream",
		ShardCount: 4,
	}))

	out, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-currentcount-stream",
		TargetShardCount: 2,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// CurrentShardCount should reflect the 4 open shards before the operation.
	assert.Equal(t, 4, out.CurrentShardCount)
	assert.Equal(t, 2, out.TargetShardCount)
}

func TestUpdateShardCount_UniqueShardIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-uniqueids-stream",
		ShardCount: 2,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-uniqueids-stream",
		TargetShardCount: 3,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// Scale again (3 -> 2 stays within the AWS 50%-200% per-call window).
	_, err = b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-uniqueids-stream",
		TargetShardCount: 2,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-uniqueids-stream"},
	)
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for _, s := range out.Shards {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard ID %q", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}
}

func TestUpdateShardCount_ViaHandler_OpenShardsOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "handler-update-shard",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "UpdateShardCount", map[string]any{
		"StreamName":       "handler-update-shard",
		"TargetShardCount": 4,
		"ScalingType":      "UNIFORM_SCALING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp struct {
		CurrentShardCount int `json:"CurrentShardCount"`
		TargetShardCount  int `json:"TargetShardCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, 2, updateResp.CurrentShardCount)
	assert.Equal(t, 4, updateResp.TargetShardCount)

	// ListShards returns only open shards → should see 4 new open shards.
	rec = doRequest(t, h, "ListShards", map[string]any{"StreamName": "handler-update-shard"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Shards []any `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Shards, 4)
}

func TestUpdateShardCount_SecondScaleStillWorks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "double-scale-stream",
		ShardCount: 2,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "double-scale-stream",
		TargetShardCount: 4,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	out2, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "double-scale-stream",
		TargetShardCount: 2,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)
	assert.Equal(t, 4, out2.CurrentShardCount, "current count after first scale is 4 open shards")
	assert.Equal(t, 2, out2.TargetShardCount)
}

func TestUpdateShardCount_LargeScale(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "large-scale-stream",
		ShardCount: 5,
	}))

	out, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "large-scale-stream",
		TargetShardCount: 10,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)
	assert.Equal(t, 5, out.CurrentShardCount)
	assert.Equal(t, 10, out.TargetShardCount)

	// Verify 10 open shards via ListShards.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "large-scale-stream"})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 10)
}
