package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestRefinement2_CreateStream_DefaultsToProvisioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		wantMode   string
		shardCount int
	}{
		{name: "no_mode_specified", streamName: "prov-stream-1", shardCount: 1, wantMode: "PROVISIONED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": tt.shardCount,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp struct {
				StreamDescription struct {
					StreamModeDetails *struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			require.NotNil(t, resp.StreamDescription.StreamModeDetails)
			assert.Equal(t, tt.wantMode, resp.StreamDescription.StreamModeDetails.StreamMode)
		})
	}
}

func TestRefinement2_CreateStream_WithOnDemand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		streamMode string
		wantMode   string
		shardCount int
	}{
		{name: "on_demand", streamName: "od-stream-1", shardCount: 1, streamMode: "ON_DEMAND", wantMode: "ON_DEMAND"},
		{
			name:       "provisioned_explicit",
			streamName: "prov-stream-2",
			shardCount: 2,
			streamMode: "PROVISIONED",
			wantMode:   "PROVISIONED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": tt.shardCount,
				"StreamModeDetails": map[string]any{
					"StreamMode": tt.streamMode,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp struct {
				StreamDescription struct {
					StreamModeDetails *struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			require.NotNil(t, resp.StreamDescription.StreamModeDetails)
			assert.Equal(t, tt.wantMode, resp.StreamDescription.StreamModeDetails.StreamMode)
		})
	}
}

func TestRefinement2_CreateStream_WithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]any
		name       string
		streamName string
		tagKey     string
		tagValue   string
	}{
		{
			name:       "with_tags",
			streamName: "tagged-stream-1",
			tags:       map[string]any{"env": "prod", "team": "platform"},
			tagKey:     "env",
			tagValue:   "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
				"Tags":       tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)
			var descResp struct {
				StreamDescription struct {
					StreamARN string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))

			rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": descResp.StreamDescription.StreamARN,
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagsResp))

			found := false
			for _, kv := range tagsResp.Tags {
				if kv.Key == tt.tagKey {
					assert.Equal(t, tt.tagValue, kv.Value)
					found = true
				}
			}
			assert.True(t, found, "expected tag key %q not found", tt.tagKey)
		})
	}
}

func TestRefinement2_UpdateStreamMode_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newMode  string
		wantMode string
	}{
		{name: "to_on_demand", newMode: "ON_DEMAND", wantMode: "ON_DEMAND"},
		{name: "to_provisioned", newMode: "PROVISIONED", wantMode: "PROVISIONED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamName := "mode-stream-" + tt.name

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
			require.Equal(t, http.StatusOK, rec2.Code)
			var descResp struct {
				StreamDescription struct {
					StreamARN string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))

			rec3 := doRequest(t, h, "UpdateStreamMode", map[string]any{
				"StreamARN": descResp.StreamDescription.StreamARN,
				"StreamModeDetails": map[string]any{
					"StreamMode": tt.newMode,
				},
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			rec4 := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
			require.Equal(t, http.StatusOK, rec4.Code)
			var verifyResp struct {
				StreamDescription struct {
					StreamModeDetails *struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &verifyResp))
			require.NotNil(t, verifyResp.StreamDescription.StreamModeDetails)
			assert.Equal(t, tt.wantMode, verifyResp.StreamDescription.StreamModeDetails.StreamMode)
		})
	}
}

func TestRefinement2_UpdateStreamMode_InvalidMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{name: "invalid_mode", mode: "INVALID", wantErr: true},
		{name: "empty_mode", mode: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "inv-mode-stream",
				ShardCount: 1,
			}))

			err := b.UpdateStreamMode(context.Background(), &kinesis.UpdateStreamModeInput{
				StreamARN:         "arn:aws:kinesis:us-east-1:123456789012:stream/inv-mode-stream",
				StreamModeDetails: kinesis.StreamModeDetails{StreamMode: tt.mode},
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement2_UpdateStreamMode_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		streamARN string
		wantErr   bool
	}{
		{
			name:      "nonexistent_stream",
			streamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/no-such-stream",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			err := b.UpdateStreamMode(context.Background(), &kinesis.UpdateStreamModeInput{
				StreamARN:         tt.streamARN,
				StreamModeDetails: kinesis.StreamModeDetails{StreamMode: kinesis.StreamModeOnDemand},
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement2_DescribeStream_StreamCreationTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "has_timestamp", streamName: "ts-stream-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			before := time.Now().Unix()
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp struct {
				StreamDescription struct {
					StreamCreationTimestamp float64 `json:"StreamCreationTimestamp"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			assert.GreaterOrEqual(t, int64(resp.StreamDescription.StreamCreationTimestamp), before)
		})
	}
}

func TestRefinement2_DescribeStream_StreamModeDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		wantMode   string
	}{
		{name: "default_provisioned", streamName: "mode-details-stream-1", wantMode: "PROVISIONED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp struct {
				StreamDescription struct {
					StreamModeDetails *struct {
						StreamMode string `json:"StreamMode"`
					} `json:"StreamModeDetails"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			require.NotNil(t, resp.StreamDescription.StreamModeDetails)
			assert.Equal(t, tt.wantMode, resp.StreamDescription.StreamModeDetails.StreamMode)
		})
	}
}

func TestRefinement2_DescribeStreamSummary_OpenShardCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		streamName     string
		initialShards  int
		wantOpenShards int
		doMerge        bool
	}{
		{
			name:           "no_merge",
			streamName:     "summary-stream-1",
			initialShards:  2,
			doMerge:        false,
			wantOpenShards: 2,
		},
		{
			name:           "after_merge",
			streamName:     "summary-stream-2",
			initialShards:  2,
			doMerge:        true,
			wantOpenShards: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": tt.initialShards,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.doMerge {
				rec2 := doRequest(t, h, "ListShards", map[string]any{
					"StreamName": tt.streamName,
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				var shardsResp struct {
					Shards []struct {
						ShardID string `json:"ShardId"`
					} `json:"Shards"`
				}
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &shardsResp))
				require.Len(t, shardsResp.Shards, 2)

				rec3 := doRequest(t, h, "MergeShards", map[string]any{
					"StreamName":           tt.streamName,
					"ShardToMerge":         shardsResp.Shards[0].ShardID,
					"AdjacentShardToMerge": shardsResp.Shards[1].ShardID,
				})
				require.Equal(t, http.StatusOK, rec3.Code)
			}

			rec4 := doRequest(t, h, "DescribeStreamSummary", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec4.Code)

			var summaryResp struct {
				StreamDescriptionSummary struct {
					OpenShardCount int `json:"OpenShardCount"`
				} `json:"StreamDescriptionSummary"`
			}
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &summaryResp))
			assert.Equal(t, tt.wantOpenShards, summaryResp.StreamDescriptionSummary.OpenShardCount)
		})
	}
}

func TestRefinement2_DescribeStream_EncryptionTypeDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		streamName     string
		wantEncryption string
	}{
		{name: "defaults_to_none", streamName: "enc-default-stream-1", wantEncryption: "NONE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp struct {
				StreamDescription struct {
					EncryptionType string `json:"EncryptionType"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantEncryption, resp.StreamDescription.EncryptionType)
		})
	}
}

func TestRefinement2_MergeShards_KeepsClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		shardCount      int
		wantTotalShards int
		wantOpenShards  int
	}{
		{
			name:            "after_merge",
			streamName:      "merge-closed-stream-1",
			shardCount:      2,
			wantTotalShards: 3,
			wantOpenShards:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: tt.shardCount,
			}))

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)
			require.Len(t, out.Shards, 2)

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         out.Shards[0].ShardID,
				AdjacentShardToMerge: out.Shards[1].ShardID,
			}))

			out2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			assert.Len(t, out2.Shards, tt.wantTotalShards)

			openCount := 0
			for _, s := range out2.Shards {
				if !s.Closed {
					openCount++
				}
			}
			assert.Equal(t, tt.wantOpenShards, openCount)
		})
	}
}

func TestRefinement2_SplitShard_KeepsClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		wantTotalShards int
		wantOpenShards  int
	}{
		{
			name:            "after_split",
			streamName:      "split-closed-stream-1",
			wantTotalShards: 3,
			wantOpenShards:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 1,
			}))

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)
			require.Len(t, out.Shards, 1)

			newHashKey := "170141183460469231731687303715884105727"

			require.NoError(t, b.SplitShard(context.Background(), &kinesis.SplitShardInput{
				StreamName:         tt.streamName,
				ShardToSplit:       out.Shards[0].ShardID,
				NewStartingHashKey: newHashKey,
			}))

			out2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			assert.Len(t, out2.Shards, tt.wantTotalShards)

			openCount := 0
			for _, s := range out2.Shards {
				if !s.Closed {
					openCount++
				}
			}
			assert.Equal(t, tt.wantOpenShards, openCount)
		})
	}
}

func TestRefinement2_CountOpenShards_ExcludesClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		shardCount int
		wantCount  int
		doMerge    bool
	}{
		{name: "no_merge", streamName: "count-stream-1", shardCount: 2, doMerge: false, wantCount: 2},
		{name: "after_merge", streamName: "count-stream-2", shardCount: 2, doMerge: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: tt.shardCount,
			}))

			if tt.doMerge {
				out, err := b.DescribeStream(
					context.Background(),
					&kinesis.DescribeStreamInput{StreamName: tt.streamName},
				)
				require.NoError(t, err)
				require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
					StreamName:           tt.streamName,
					ShardToMerge:         out.Shards[0].ShardID,
					AdjacentShardToMerge: out.Shards[1].ShardID,
				}))
			}

			assert.Equal(t, tt.wantCount, b.CountOpenShards(context.Background()))
		})
	}
}

func TestRefinement2_DescribeAccountSettings_OnDemandCount(t *testing.T) {
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

func TestRefinement2_PutRecord_ExplicitHashKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		explicitHashKey string
		wantShard       string
		shardCount      int
	}{
		{
			name:            "routes_to_shard0",
			shardCount:      2,
			explicitHashKey: "0",
			wantShard:       "shardId-000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "ehk-stream-" + tt.name,
				ShardCount: tt.shardCount,
			}))

			out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:      "ehk-stream-" + tt.name,
				PartitionKey:    "some-key",
				ExplicitHashKey: tt.explicitHashKey,
				Data:            []byte("data"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantShard, out.ShardID)
		})
	}
}

func TestRefinement2_ListShards_ExclusiveStartShardId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		exclusiveStartShardID string
		shardCount            int
		wantCount             int
	}{
		{name: "no_filter", shardCount: 3, exclusiveStartShardID: "", wantCount: 3},
		{name: "skip_first", shardCount: 3, exclusiveStartShardID: "shardId-000000000000", wantCount: 2},
		{name: "skip_first_two", shardCount: 3, exclusiveStartShardID: "shardId-000000000001", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "list-shards-excl-" + tt.name,
				ShardCount: tt.shardCount,
			}))

			out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
				StreamName:            "list-shards-excl-" + tt.name,
				ExclusiveStartShardID: tt.exclusiveStartShardID,
			})
			require.NoError(t, err)
			assert.Len(t, out.Shards, tt.wantCount)
		})
	}
}

func TestRefinement2_ListShards_IncludesClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		shardCount      int
		wantTotalShards int
	}{
		{name: "after_merge_shows_all", streamName: "list-closed-stream-1", shardCount: 2, wantTotalShards: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: tt.shardCount,
			}))

			ds, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         ds.Shards[0].ShardID,
				AdjacentShardToMerge: ds.Shards[1].ShardID,
			}))

			// Use FROM_TRIM_HORIZON filter to retrieve all shards including closed ones.
			out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
				StreamName:  tt.streamName,
				ShardFilter: "FROM_TRIM_HORIZON",
			})
			require.NoError(t, err)
			assert.Len(t, out.Shards, tt.wantTotalShards)

			// Without a filter, only open shards are returned (matching AWS default behavior).
			openOut, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: tt.streamName})
			require.NoError(t, err)
			assert.Len(t, openOut.Shards, 1, "expected only the 1 open (merged) shard without filter")
		})
	}
}

func TestRefinement2_ShardDescription_ParentShardId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "merged_shard_has_parents", streamName: "parent-shard-stream-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 2,
			}))

			ds, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			shard0ID := ds.Shards[0].ShardID
			shard1ID := ds.Shards[1].ShardID

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         shard0ID,
				AdjacentShardToMerge: shard1ID,
			}))

			ds2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			var mergedShard *kinesis.ShardDescription
			for i := range ds2.Shards {
				if !ds2.Shards[i].Closed {
					mergedShard = &ds2.Shards[i]

					break
				}
			}
			require.NotNil(t, mergedShard)
			assert.Equal(t, shard0ID, mergedShard.ParentShardID)
			assert.Equal(t, shard1ID, mergedShard.AdjacentParentShardID)
		})
	}
}

func TestRefinement2_NextSeq_Serialized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "seq_preserved_after_snapshot_restore", streamName: "seq-stream-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 1,
			}))

			out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:   tt.streamName,
				PartitionKey: "key",
				Data:         []byte("data"),
			})
			require.NoError(t, err)
			firstSeq := out.SequenceNumber

			snapshot := b.Snapshot()
			require.NotNil(t, snapshot)

			b2 := kinesis.NewInMemoryBackend()
			require.NoError(t, b2.Restore(snapshot))

			out2, err := b2.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:   tt.streamName,
				PartitionKey: "key2",
				Data:         []byte("data2"),
			})
			require.NoError(t, err)
			assert.NotEqual(t, firstSeq, out2.SequenceNumber)
		})
	}
}

func TestRefinement2_AddStreamInternal_DefaultsStreamMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		streamMode string
		wantMode   string
	}{
		{
			name:       "empty_mode_defaults_to_provisioned",
			streamName: "internal-stream-1",
			streamMode: "",
			wantMode:   "PROVISIONED",
		},
		{name: "on_demand_preserved", streamName: "internal-stream-2", streamMode: "ON_DEMAND", wantMode: "ON_DEMAND"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()

			b.AddStreamInternal(&kinesis.Stream{
				Name:       tt.streamName,
				ARN:        "arn:aws:kinesis:us-east-1:123456789012:stream/" + tt.streamName,
				Status:     "ACTIVE",
				StreamMode: tt.streamMode,
			})

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, out.StreamMode)
		})
	}
}

func TestRefinement2_ListTagsForResource_UsesHandlerTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]any
		name       string
		streamName string
		tagKey     string
		tagValue   string
	}{
		{
			name:       "tags_from_create",
			streamName: "ltfr-stream-1",
			tags:       map[string]any{"project": "gopherstack"},
			tagKey:     "project",
			tagValue:   "gopherstack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
				"Tags":       tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			var descResp struct {
				StreamDescription struct {
					StreamARN string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))

			rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": descResp.StreamDescription.StreamARN,
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagsResp))

			found := false
			for _, kv := range tagsResp.Tags {
				if kv.Key == tt.tagKey {
					assert.Equal(t, tt.tagValue, kv.Value)
					found = true
				}
			}
			assert.True(t, found, "expected tag key %q not found", tt.tagKey)
		})
	}
}

func TestRefinement2_GetSupportedOperations_IncludesUpdateStreamMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "update_stream_mode_present", wantOp: "UpdateStreamMode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.GetSupportedOperations()
			assert.Contains(t, ops, tt.wantOp)
		})
	}
}
