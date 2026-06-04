package kinesis_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// ---------------------------------------------------------------------------
// Fix 1: CreateStream ON_DEMAND accepts ShardCount=0 (or omitted)
//
// AWS behaviour: for ON_DEMAND streams ShardCount is ignored — capacity is
// managed automatically.  Previously the handler rejected ShardCount<=0
// before inspecting the stream mode, so ON_DEMAND callers that omit
// ShardCount received InvalidArgumentException.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_CreateStream_OnDemand_ShardCountZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantMode   string
		wantStatus int
	}{
		{
			name: "on_demand_no_shard_count",
			body: map[string]any{
				"StreamName":        "od-no-sc-1",
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus: http.StatusOK,
			wantMode:   "ON_DEMAND",
		},
		{
			name: "on_demand_shard_count_zero",
			body: map[string]any{
				"StreamName":        "od-sc-zero",
				"ShardCount":        0,
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus: http.StatusOK,
			wantMode:   "ON_DEMAND",
		},
		{
			name: "on_demand_explicit_shard_count",
			body: map[string]any{
				"StreamName":        "od-explicit-sc",
				"ShardCount":        4,
				"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
			},
			wantStatus: http.StatusOK,
			wantMode:   "ON_DEMAND",
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
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
				require.NotNil(t, resp.StreamDescription.StreamModeDetails)
				assert.Equal(t, tt.wantMode, resp.StreamDescription.StreamModeDetails.StreamMode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 2: DescribeStream and DescribeStreamSummary accept StreamARN
//
// AWS allows callers to identify a stream by ARN instead of name.
// Previously only StreamName was parsed, so ARN-only requests silently
// returned ResourceNotFoundException.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_DescribeStream_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "arn-describe-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "arn-describe-stream"})
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

func TestAccuracyBatch2_DescribeStreamSummary_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "arn-summary-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "arn-summary-stream"})
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

// ---------------------------------------------------------------------------
// Fix 3: ListShards rejects NextToken when StreamName is also provided
//
// AWS ValidationException: "NextToken and StreamName/ExclusiveStartShardId
// are mutually exclusive parameters."
// Previously the handler silently used both, which could produce incorrect
// results or an unexpected ResourceNotFoundException on the stream lookup.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_ListShards_NextTokenStreamNameMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "mutex-shards-stream", "ShardCount": 2})

	// First page to get a valid NextToken.
	rec := doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "mutex-shards-stream",
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.NotEmpty(t, page1.NextToken, "expected NextToken for pagination")

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "next_token_with_stream_name",
			body: map[string]any{
				"NextToken":  page1.NextToken,
				"StreamName": "mutex-shards-stream",
			},
		},
		{
			name: "next_token_with_exclusive_start_shard_id",
			body: map[string]any{
				"NextToken":             page1.NextToken,
				"StreamName":            "mutex-shards-stream",
				"ExclusiveStartShardId": "shardId-000000000000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec2 := doRequest(t, h, "ListShards", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec2.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
			assert.Equal(t, "ValidationException", errResp.Type)
		})
	}
}

func TestAccuracyBatch2_ListShards_NextTokenAlone_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "next-token-only-stream", "ShardCount": 3})

	// Get NextToken from first page.
	rec := doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "next-token-only-stream",
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken"`
		Shards    []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.NotEmpty(t, page1.NextToken)

	// Use NextToken alone (no StreamName) — should succeed because the token embeds stream context.
	rec2 := doRequest(t, h, "ListShards", map[string]any{
		"NextToken":  page1.NextToken,
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	assert.NotEmpty(t, page2.Shards)
}

// ---------------------------------------------------------------------------
// Fix 4: DeleteStream accepts StreamARN
//
// AWS behaviour: DeleteStream accepts either StreamName or StreamARN to
// identify the stream. Previously the handler only parsed StreamName, so
// ARN-only callers received ResourceNotFoundException.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_DeleteStream_ByARN(t *testing.T) {
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
			desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
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

// ---------------------------------------------------------------------------
// Fix 5: PutRecord and PutRecords accept StreamARN
//
// AWS behaviour: PutRecord and PutRecords accept either StreamName or
// StreamARN. Previously the handler only parsed StreamName, so ARN-only
// callers received ResourceNotFoundException.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_PutRecord_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "put-record-arn-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "put-record-arn-stream"})
	require.NoError(t, err)

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{
				"StreamName":   "put-record-arn-stream",
				"PartitionKey": "pk1",
				"Data":         []byte("hello"),
			},
		},
		{
			name: "by_arn",
			body: map[string]any{
				"StreamARN":    desc.StreamARN,
				"PartitionKey": "pk2",
				"Data":         []byte("world"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "PutRecord", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ShardId        string `json:"ShardId"`
				SequenceNumber string `json:"SequenceNumber"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ShardId)
			assert.NotEmpty(t, resp.SequenceNumber)
		})
	}
}

func TestAccuracyBatch2_PutRecords_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "put-records-arn-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "put-records-arn-stream"})
	require.NoError(t, err)

	records := []map[string]any{
		{"PartitionKey": "pk1", "Data": []byte("r1")},
		{"PartitionKey": "pk2", "Data": []byte("r2")},
	}

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{"StreamName": "put-records-arn-stream", "Records": records},
		},
		{
			name: "by_arn",
			body: map[string]any{"StreamARN": desc.StreamARN, "Records": records},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "PutRecords", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				FailedRecordCount int `json:"FailedRecordCount"`
				Records           []struct {
					ShardId        string `json:"ShardId"`
					SequenceNumber string `json:"SequenceNumber"`
				} `json:"Records"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, 0, resp.FailedRecordCount)
			assert.Len(t, resp.Records, 2)
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 6: GetShardIterator accepts StreamARN
//
// AWS behaviour: GetShardIterator accepts either StreamName or StreamARN.
// Previously the handler only parsed StreamName, so ARN-only callers
// received ResourceNotFoundException.
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_GetShardIterator_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "gsi-arn-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "gsi-arn-stream"})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Shards)
	shardID := desc.Shards[0].ShardID

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{
				"StreamName":        "gsi-arn-stream",
				"ShardId":           shardID,
				"ShardIteratorType": "TRIM_HORIZON",
			},
		},
		{
			name: "by_arn",
			body: map[string]any{
				"StreamARN":         desc.StreamARN,
				"ShardId":           shardID,
				"ShardIteratorType": "TRIM_HORIZON",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "GetShardIterator", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ShardIterator string `json:"ShardIterator"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ShardIterator)
		})
	}
}
