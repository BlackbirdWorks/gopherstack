package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWS allows callers to identify a stream by ARN instead of name.
func TestDescribeStream_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "arn-describe-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "arn-describe-stream"})
	require.NoError(t, err)

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{"StreamName": "arn-describe-stream"},
		},
		{
			name: "by_arn",
			body: map[string]any{"StreamARN": desc.StreamARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeStream", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				StreamDescription struct {
					StreamName string `json:"StreamName"`
					StreamARN  string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "arn-describe-stream", resp.StreamDescription.StreamName)
			assert.Equal(t, desc.StreamARN, resp.StreamDescription.StreamARN)
		})
	}
}

func TestDescribeStreamSummary_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "arn-summary-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "arn-summary-stream"})
	require.NoError(t, err)

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{"StreamName": "arn-summary-stream"},
		},
		{
			name: "by_arn",
			body: map[string]any{"StreamARN": desc.StreamARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeStreamSummary", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				StreamDescriptionSummary struct {
					StreamName string `json:"StreamName"`
					StreamARN  string `json:"StreamARN"`
				} `json:"StreamDescriptionSummary"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "arn-summary-stream", resp.StreamDescriptionSummary.StreamName)
			assert.Equal(t, desc.StreamARN, resp.StreamDescriptionSummary.StreamARN)
		})
	}
}

// TestDescribeStream_EncryptionFields verifies encryption fields in describe response.
func TestDescribeStream_EncryptionFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "enc-describe-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Start encryption.
	rec = doRequest(t, h, "StartStreamEncryption", map[string]any{
		"StreamName":     "enc-describe-stream",
		"EncryptionType": "KMS",
		"KeyId":          "my-key-id",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStream should return encryption info.
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "enc-describe-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamDescription struct {
			EncryptionType string `json:"EncryptionType"`
			KeyID          string `json:"KeyId"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "KMS", resp.StreamDescription.EncryptionType)
	assert.Equal(t, "my-key-id", resp.StreamDescription.KeyID)
}

// TestDescribeStreamSummary_EncryptionFields verifies encryption in summary.
func TestDescribeStreamSummary_EncryptionFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "enc-summary-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "StartStreamEncryption", map[string]any{
		"StreamName":     "enc-summary-stream",
		"EncryptionType": "KMS",
		"KeyId":          "summary-key-id",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStreamSummary", map[string]any{
		"StreamName": "enc-summary-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamDescriptionSummary struct {
			EncryptionType string `json:"EncryptionType"`
			KeyID          string `json:"KeyId"`
		} `json:"StreamDescriptionSummary"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "KMS", resp.StreamDescriptionSummary.EncryptionType)
	assert.Equal(t, "summary-key-id", resp.StreamDescriptionSummary.KeyID)
}

func TestDescribeStreamSummary_OpenShardCountAndConsumerCount(t *testing.T) {
	t.Parallel()

	h := kinesis.NewHandler(kinesis.NewInMemoryBackend())

	rec := doParityRequest(t, h, "CreateStream",
		map[string]any{"StreamName": "summary-test", "ShardCount": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doParityRequest(t, h, "DescribeStream",
		map[string]any{"StreamName": "summary-test"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.NewDecoder(strings.NewReader(descRec.Body.String())).Decode(&descResp))

	regRec := doParityRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    descResp.StreamDescription.StreamARN,
		"ConsumerName": "c1",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	sumRec := doParityRequest(t, h, "DescribeStreamSummary",
		map[string]any{"StreamName": "summary-test"})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var sumResp struct {
		StreamDescriptionSummary struct {
			StreamStatus   string `json:"StreamStatus"`
			OpenShardCount int    `json:"OpenShardCount"`
			ConsumerCount  int    `json:"ConsumerCount"`
		} `json:"StreamDescriptionSummary"`
	}

	require.NoError(t, json.NewDecoder(strings.NewReader(sumRec.Body.String())).Decode(&sumResp))

	assert.Equal(t, 3, sumResp.StreamDescriptionSummary.OpenShardCount)
	assert.Equal(t, 1, sumResp.StreamDescriptionSummary.ConsumerCount)
	assert.Equal(t, "ACTIVE", sumResp.StreamDescriptionSummary.StreamStatus)
}

func TestDescribeStream_StreamCreationTimestamp(t *testing.T) {
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

func TestDescribeStream_StreamModeDetails(t *testing.T) {
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

func TestDescribeStreamSummary_OpenShardCount_AfterMerge(t *testing.T) {
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

func TestDescribeStream_EncryptionTypeDefault(t *testing.T) {
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

func TestDescribeStream_IncludesClosedShardsAfterMerge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-closed-merge",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-merge"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	s0 := descResp.StreamDescription.Shards[0].ShardID
	s1 := descResp.StreamDescription.Shards[1].ShardID

	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "describe-closed-merge",
		"ShardToMerge":         s0,
		"AdjacentShardToMerge": s1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// After merge: 2 closed parents + 1 open merged = 3 total.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-merge"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 3)
}

func TestDescribeStream_IncludesClosedShardsAfterSplit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-closed-split",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-split"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	shardID := descResp.StreamDescription.Shards[0].ShardID

	const splitKey = "170141183460469231731687303715884105728"
	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "describe-closed-split",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": splitKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// After split: 1 closed parent + 2 open children = 3 total.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-split"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 3)
}

func TestDescribeStream_OpenShardNoEndingSequenceNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-open-shard",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-open-shard"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)
	// Open shards have an empty EndingSequenceNumber.
	assert.Empty(t, descResp.StreamDescription.Shards[0].SequenceNumberRange.EndingSequenceNumber)
}

// TestDescribeStream_OpenShardWithRecordsNoEndingSeq verifies that an
// OPEN shard that already holds records still reports no EndingSequenceNumber.
// In real AWS, EndingSequenceNumber is reported only for CLOSED shards — a
// KCL-style consumer treats its presence as the signal a shard is closed and it
// should advance to the child shards. Reporting it on an open-but-populated shard
// would make a consumer prematurely abandon a live shard.
func TestDescribeStream_OpenShardWithRecordsNoEndingSeq(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "open-shard-with-records",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Write several records into the single (still open) shard.
	for i := range 3 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "open-shard-with-records",
			"PartitionKey": fmt.Sprintf("pk-%d", i),
			"Data":         []byte("payload"),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					StartingSequenceNumber string `json:"StartingSequenceNumber"`
					EndingSequenceNumber   string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "open-shard-with-records"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)

	shard := descResp.StreamDescription.Shards[0]
	// A populated open shard still has a starting sequence number...
	assert.NotEmpty(t, shard.SequenceNumberRange.StartingSequenceNumber)
	// ...but must NOT report an ending sequence number while it remains open.
	assert.Empty(t, shard.SequenceNumberRange.EndingSequenceNumber,
		"open shard with records must not report EndingSequenceNumber")
}

func TestDescribeStream_UpdateShardCount_IncludesOldClosedShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-usc-stream",
		"ShardCount": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "UpdateShardCount", map[string]any{
		"StreamName":       "describe-usc-stream",
		"TargetShardCount": 2,
		"ScalingType":      "UNIFORM_SCALING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStream should include both closed (3 old) and open (2 new) shards.
	var descResp struct {
		StreamDescription struct {
			Shards []any `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-usc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 5, "3 closed + 2 open = 5")
}

// TestDescribeStream_ShardPagination verifies that DescribeStream
// paginates its Shards list using Limit/ExclusiveStartShardId/HasMoreShards,
// matching the AWS contract (default page size 100, resumable pagination).
// A stream accumulates CLOSED shards forever across reshards, so a
// long-lived, heavily-resharded stream can exceed a single page — previously
// DescribeStream always returned every shard in one response and hardcoded
// HasMoreShards to false.
func TestDescribeStream_ShardPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		exclusiveStartShardID string
		limit                 int
		shardCount            int
		wantLen               int
		wantHasMore           bool
	}{
		{
			name:        "default_limit_under_page",
			shardCount:  5,
			wantLen:     5,
			wantHasMore: false,
		},
		{
			name:        "explicit_limit_truncates",
			shardCount:  5,
			limit:       2,
			wantLen:     2,
			wantHasMore: true,
		},
		{
			name:                  "exclusive_start_resumes_after_page",
			shardCount:            5,
			limit:                 2,
			exclusiveStartShardID: "shardId-000000000001", // page 1 returned shards 0,1
			wantLen:               2,                      // shards 2,3
			wantHasMore:           true,
		},
		{
			name:                  "exclusive_start_reaches_end",
			shardCount:            5,
			limit:                 2,
			exclusiveStartShardID: "shardId-000000000003", // shards 4 remains
			wantLen:               1,
			wantHasMore:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "paginated-stream",
				ShardCount: tt.shardCount,
			}))

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{
				StreamName:            "paginated-stream",
				Limit:                 tt.limit,
				ExclusiveStartShardID: tt.exclusiveStartShardID,
			})
			require.NoError(t, err)
			assert.Len(t, out.Shards, tt.wantLen)
			assert.Equal(t, tt.wantHasMore, out.HasMoreShards)
		})
	}
}

func TestDescribeStream_ClosedShardHasEndingSequenceNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "closing-seqnum-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put a record so the shard has a non-trivial sequence range.
	doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   "closing-seqnum-stream",
		"PartitionKey": "pk",
		"Data":         []byte("data"),
	})

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "closing-seqnum-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Split the shard to close it.
	const splitKey = "170141183460469231731687303715884105728"
	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "closing-seqnum-stream",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": splitKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Re-describe: closed shard should have a non-empty EndingSequenceNumber.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "closing-seqnum-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 3)

	closedCount := 0
	for _, s := range descResp.StreamDescription.Shards {
		if s.SequenceNumberRange.EndingSequenceNumber != "" {
			closedCount++
		}
	}
	assert.Equal(t, 1, closedCount, "exactly one shard should be closed with a non-empty ending seq")
}
