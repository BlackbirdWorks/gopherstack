package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestCreateStream_ShardCount_Validation asserts ValidationException for ShardCount <= 0 on PROVISIONED streams.
func TestCreateStream_ShardCount_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamMode string
		wantType   string
		shardCount int
		wantCode   int
	}{
		{
			name:       "valid_provisioned_shard_1",
			shardCount: 1,
			wantCode:   http.StatusOK,
		},
		{
			name:       "zero_shard_count_provisioned_rejected",
			shardCount: 0,
			wantCode:   http.StatusBadRequest,
			wantType:   "InvalidArgumentException",
		},
		{
			name:       "negative_shard_count_rejected",
			shardCount: -1,
			wantCode:   http.StatusBadRequest,
			wantType:   "InvalidArgumentException",
		},
		{
			name:       "on_demand_zero_shard_count_allowed",
			shardCount: 0,
			streamMode: "ON_DEMAND",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"StreamName": "shard-val-stream-" + tt.name,
				"ShardCount": tt.shardCount,
			}
			if tt.streamMode != "" {
				body["StreamModeDetails"] = map[string]any{"StreamMode": tt.streamMode}
			}

			rec := doRequest(t, h, "CreateStream", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}

// AWS behaviour: for ON_DEMAND streams ShardCount is ignored — capacity is
// managed automatically. A caller may omit it or pass 0.
func TestCreateStream_OnDemand_ShardCountZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantMode       string
		wantStatus     int
		wantShardCount int // 0 = not checked
	}{
		{
			name: "on_demand_no_shard_count",
			body: map[string]any{
				"StreamName":        "od-no-sc-1",
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus: http.StatusOK,
			wantMode:   "ON_DEMAND",
			// AWS allocates 4 shards to a freshly created on-demand stream;
			// ShardCount is not honored for ON_DEMAND streams.
			wantShardCount: 4,
		},
		{
			name: "on_demand_shard_count_zero",
			body: map[string]any{
				"StreamName":        "od-sc-zero",
				"ShardCount":        0,
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus:     http.StatusOK,
			wantMode:       "ON_DEMAND",
			wantShardCount: 4,
		},
		{
			name: "on_demand_explicit_shard_count_ignored",
			body: map[string]any{
				"StreamName":        "od-explicit-sc",
				"ShardCount":        17,
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus: http.StatusOK,
			wantMode:   "ON_DEMAND",
			// A caller-supplied ShardCount is ignored for ON_DEMAND streams —
			// AWS always starts a new on-demand stream at 4 shards.
			wantShardCount: 4,
		},
		{
			name: "provisioned_still_requires_shard_count",
			body: map[string]any{
				"StreamName": "prov-no-sc",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantMode != "" {
				descRec := doRequest(t, h, "DescribeStream", map[string]any{
					"StreamName": tt.body["StreamName"],
				})
				require.Equal(t, http.StatusOK, descRec.Code)

				var resp struct {
					StreamDescription struct {
						StreamModeDetails *struct {
							StreamMode string `json:"StreamMode"`
						} `json:"StreamModeDetails"`
						Shards []struct {
							ShardID string `json:"ShardId"`
						} `json:"Shards"`
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
				require.NotNil(t, resp.StreamDescription.StreamModeDetails)
				assert.Equal(t, tt.wantMode, resp.StreamDescription.StreamModeDetails.StreamMode)

				if tt.wantShardCount > 0 {
					assert.Len(t, resp.StreamDescription.Shards, tt.wantShardCount)
				}
			}
		})
	}
}

// AWS behaviour: DeleteStream accepts either StreamName or StreamARN to identify the stream.
func TestDeleteStream_ByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useARN     bool
		wantStatus int
	}{
		{
			name:       "by_name",
			useARN:     false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "by_arn",
			useARN:     true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamName := "delete-by-" + tt.name
			doRequest(t, h, "CreateStream", map[string]any{"StreamName": streamName, "ShardCount": 1})

			b := h.Backend.(*kinesis.InMemoryBackend)
			desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: streamName})
			require.NoError(t, err)

			var deleteBody map[string]any
			if tt.useARN {
				deleteBody = map[string]any{"StreamARN": desc.StreamARN}
			} else {
				deleteBody = map[string]any{"StreamName": streamName}
			}

			rec := doRequest(t, h, "DeleteStream", deleteBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify stream is gone.
			descRec := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
			assert.Equal(t, http.StatusBadRequest, descRec.Code)
		})
	}
}

// TestValidStreamNames verifies accepted stream name patterns.
func TestValidStreamNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "simple", streamName: "my-stream"},
		{name: "with_underscore", streamName: "my_stream"},
		{name: "with_dot", streamName: "my.stream"},
		{name: "alphanumeric", streamName: "Stream123"},
		{name: "single_char", streamName: "s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestKinesisBackend_ListStreamsLimit(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	for i := range 5 {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("limit-stream-%d", i),
		}))
	}

	out, err := bk.ListStreams(context.Background(), &kinesis.ListStreamsInput{Limit: 3})
	require.NoError(t, err)
	assert.Len(t, out.StreamNames, 3)
	assert.True(t, out.HasMoreStreams)
}

// TestListStreams_Sorted verifies that stream names are returned in alphabetical order.
func TestListStreams_Sorted(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: name}))
	}

	out, err := bk.ListStreams(context.Background(), &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, out.StreamNames)
}

// TestListStreams_Pagination verifies cursor-based pagination using both
// ExclusiveStartStreamName and the opaque NextToken. AWS returns names in
// alphabetical order and sets NextToken to the last returned name when
// HasMoreStreams is true; passing it back as ExclusiveStartStreamName must
// resume past that name.
func TestListStreams_Pagination(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: n}))
	}

	tests := []struct {
		input     *kinesis.ListStreamsInput
		name      string
		wantToken string
		want      []string
		wantMore  bool
	}{
		{
			name:      "first_page",
			input:     &kinesis.ListStreamsInput{Limit: 2},
			want:      []string{"a", "b"},
			wantMore:  true,
			wantToken: "b",
		},
		{
			name:      "second_page_via_next_token",
			input:     &kinesis.ListStreamsInput{Limit: 2, NextToken: "b"},
			want:      []string{"c", "d"},
			wantMore:  true,
			wantToken: "d",
		},
		{
			name:     "third_page_exclusive_start",
			input:    &kinesis.ListStreamsInput{Limit: 5, ExclusiveStartStreamName: "d"},
			want:     []string{"e"},
			wantMore: false,
		},
		{
			name:     "exclusive_start_skips_match",
			input:    &kinesis.ListStreamsInput{Limit: 10, ExclusiveStartStreamName: "a"},
			want:     []string{"b", "c", "d", "e"},
			wantMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := bk.ListStreams(context.Background(), tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.StreamNames)
			assert.Equal(t, tt.wantMore, out.HasMoreStreams)
			assert.Equal(t, tt.wantToken, out.NextToken)
		})
	}
}

// TestDeleteStream_ClosesTags verifies that deleting a stream does not leak the
// stream-level tags Prometheus metric registry.
func TestDeleteStream_ClosesTags(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "tagged-stream"}))

	// Delete should not panic (Close is safe to call).
	require.NoError(t, bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "tagged-stream"}))

	// Recreating with the same name should succeed (Tags registry released).
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "tagged-stream"}))
}

func TestCreateStream_DefaultsToProvisioned(t *testing.T) {
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

func TestCreateStream_WithOnDemand(t *testing.T) {
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

func TestCreateStream_WithTags(t *testing.T) {
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

func TestStreamLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateStream
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "my-stream",
		"ShardCount": 2,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListStreams
	rec = doRequest(t, h, "ListStreams", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		StreamNames []string `json:"StreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Contains(t, listResp.StreamNames, "my-stream")

	// DescribeStream
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "my-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamName   string `json:"StreamName"`
			StreamStatus string `json:"StreamStatus"`
			Shards       []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-stream", descResp.StreamDescription.StreamName)
	assert.Equal(t, "ACTIVE", descResp.StreamDescription.StreamStatus)
	assert.Len(t, descResp.StreamDescription.Shards, 2)

	// DescribeStreamSummary
	rec = doRequest(t, h, "DescribeStreamSummary", map[string]any{
		"StreamName": "my-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var sumResp struct {
		StreamDescriptionSummary struct {
			StreamName     string `json:"StreamName"`
			OpenShardCount int    `json:"OpenShardCount"`
		} `json:"StreamDescriptionSummary"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sumResp))
	assert.Equal(t, "my-stream", sumResp.StreamDescriptionSummary.StreamName)
	assert.Equal(t, 2, sumResp.StreamDescriptionSummary.OpenShardCount)

	// DeleteStream
	rec = doRequest(t, h, "DeleteStream", map[string]any{
		"StreamName": "my-stream",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "my-stream",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateStreamAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "dup-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "dup-stream",
		"ShardCount": 1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUseException", errResp.Type)
}

func TestCreateStreamRegionOverride(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "regional-stream-2",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleListStreamsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Fresh handler has no streams; JSON result should have empty array, not nil
	rec := doRequest(t, h, "ListStreams", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamNames []string `json:"StreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp.StreamNames)
	assert.Empty(t, resp.StreamNames)
}

func TestCreateStream_OnDemandLimitEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	// Set a tight limit of 2 ON_DEMAND streams.
	b.SetOnDemandStreamCountLimit(2)

	// Create 2 ON_DEMAND streams (should succeed).
	for i := range 2 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("od-limit-stream-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}))
	}

	// Third ON_DEMAND stream should fail with LimitExceededException.
	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-limit-stream-overflow",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	})
	require.Error(t, err)
}

func TestCreateStream_OnDemandLimit_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	b.SetOnDemandStreamCountLimit(1)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "od-handler-1",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "od-handler-2",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceededException", errResp.Type)
}

func TestCreateStream_ProvisionedNotAffectedByOnDemandLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	b.SetOnDemandStreamCountLimit(1)

	// Fill the ON_DEMAND quota.
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-quota-stream",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Provisioned stream should still be createable.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "prov-no-limit",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "PROVISIONED"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateStream_OnDemandLimit_DeleteFreesSlot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	b.SetOnDemandStreamCountLimit(1)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Limit reached.
	require.Error(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream-2",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Delete the first stream to free the slot.
	require.NoError(t, b.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "od-del-stream"}))

	// Now the second stream should succeed.
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream-2",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))
}

func TestCreateStream_OnDemandLimit_AtBoundary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	b.SetOnDemandStreamCountLimit(3)

	for i := range 3 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("od-boundary-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}), "should succeed for streams 0..2")
	}

	// The 4th should fail.
	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-boundary-overflow",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	})
	require.Error(t, err)
}
