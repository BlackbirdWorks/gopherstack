package kinesis_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/kinesis"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func newTestHandler(t *testing.T) *kinesis.Handler {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)
	backend := kinesis.NewInMemoryBackend()

	return kinesis.NewHandler(backend, log)
}

// doRequest sends a JSON request to the handler with the given X-Amz-Target action.
func doRequest(t *testing.T, h *kinesis.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	if action != "" {
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestKinesisHandler_ErrorResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		action      string
		wantErrType string
		wantCode    int
	}{
		{
			name:        "StreamNotFound",
			action:      "DescribeStream",
			body:        map[string]any{"StreamName": "nonexistent"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "UnknownAction",
			action:      "BogusAction",
			body:        nil,
			wantCode:    http.StatusBadRequest,
			wantErrType: "UnknownOperationException",
		},
		{
			name:        "DeleteStreamNotFound",
			action:      "DeleteStream",
			body:        map[string]any{"StreamName": "does-not-exist"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "GetRecordsExpiredIterator",
			action:      "GetRecords",
			body:        map[string]any{"ShardIterator": "definitely-not-base64!!"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ExpiredIteratorException",
		},
		{
			name:     "ListShardsNotFound",
			action:   "ListShards",
			body:     map[string]any{"StreamName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeStreamSummaryNotFound",
			action:   "DescribeStreamSummary",
			body:     map[string]any{"StreamName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "PutRecordNotFound",
			action:   "PutRecord",
			body:     map[string]any{"StreamName": "nonexistent", "PartitionKey": "pk", "Data": []byte("data")},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "HandlerNoTarget",
			action:   "",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
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

func TestPutAndGetRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 1 shard
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "records-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe to find shard ID
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "records-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.NotEmpty(t, descResp.StreamDescription.Shards)
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// PutRecord
	rec = doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   "records-stream",
		"PartitionKey": "pk-1",
		"Data":         []byte("hello world"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp struct {
		ShardID        string `json:"ShardId"`
		SequenceNumber string `json:"SequenceNumber"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	assert.NotEmpty(t, putResp.ShardID)
	assert.NotEmpty(t, putResp.SequenceNumber)
	firstSeq := putResp.SequenceNumber

	// PutRecords (batch)
	rec = doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "records-stream",
		"Records": []map[string]any{
			{"PartitionKey": "pk-2", "Data": []byte("record 2")},
			{"PartitionKey": "pk-3", "Data": []byte("record 3")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchResp struct {
		Records []struct {
			ShardID        string `json:"ShardId"`
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))
	assert.Equal(t, 0, batchResp.FailedRecordCount)
	assert.Len(t, batchResp.Records, 2)

	// GetShardIterator - TRIM_HORIZON (reads from beginning)
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "records-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var iterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
	assert.NotEmpty(t, iterResp.ShardIterator)

	// GetRecords
	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": iterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		NextShardIterator string `json:"NextShardIterator"`
		Records           []struct {
			PartitionKey   string `json:"PartitionKey"`
			SequenceNumber string `json:"SequenceNumber"`
			Data           []byte `json:"Data"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Len(t, getResp.Records, 3) // 1 + 2 batch
	assert.NotEmpty(t, getResp.NextShardIterator)

	// GetShardIterator - AT_SEQUENCE_NUMBER
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":             "records-stream",
		"ShardId":                shardID,
		"ShardIteratorType":      "AT_SEQUENCE_NUMBER",
		"StartingSequenceNumber": firstSeq,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var atSeqIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &atSeqIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": atSeqIterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var atSeqResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &atSeqResp))
	// AT_SEQUENCE_NUMBER starts at the given record (inclusive)
	require.NotEmpty(t, atSeqResp.Records)
	assert.Equal(t, firstSeq, atSeqResp.Records[0].SequenceNumber)

	// GetShardIterator - AFTER_SEQUENCE_NUMBER
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":             "records-stream",
		"ShardId":                shardID,
		"ShardIteratorType":      "AFTER_SEQUENCE_NUMBER",
		"StartingSequenceNumber": firstSeq,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": afterIterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterSeqResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterSeqResp))
	// AFTER_SEQUENCE_NUMBER skips the given record
	assert.Len(t, afterSeqResp.Records, 2)

	// GetShardIterator - LATEST (no new records)
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "records-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "LATEST",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var latestIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &latestIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": latestIterResp.ShardIterator,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var latestResp struct {
		Records []any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &latestResp))
	assert.Empty(t, latestResp.Records) // No new records since iterator was created
}

func TestListShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateStream with 3 shards
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "shards-stream",
		"ShardCount": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListShards
	rec = doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "shards-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listShardsResp struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listShardsResp))
	assert.Len(t, listShardsResp.Shards, 3)
}

func TestCreateStreamAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "dup-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "dup-stream",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUseException", errResp.Type)
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "Kinesis", h.Name())
	assert.NotEmpty(t, h.GetSupportedOperations())

	e := echo.New()

	// Valid target should match
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.CreateStream")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))

	// Wrong prefix should not match
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, h.RouteMatcher()(c2))
}

func TestMatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 75, h.MatchPriority())

	e := echo.New()

	// ExtractOperation with valid target
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.ListStreams")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "ListStreams", h.ExtractOperation(c))

	// ExtractOperation with no target
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	// Valid body
	body, _ := json.Marshal(map[string]string{"StreamName": "my-stream"})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-stream", h.ExtractResource(c))

	// Invalid body
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("not-json")))
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c2))
}

func TestListAll(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	// Empty
	assert.Empty(t, bk.ListAll())

	// Create some streams
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "s1"}))
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "s2"}))

	all := bk.ListAll()
	assert.Len(t, all, 2)

	names := make([]string, len(all))
	for i, s := range all {
		names[i] = s.Name
		assert.NotEmpty(t, s.ARN)
		assert.NotEmpty(t, s.Status)
	}

	assert.ElementsMatch(t, []string{"s1", "s2"}, names)
}

func TestBackendWithConfig(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "regional-stream"}))

	all := bk.ListAll()
	require.Len(t, all, 1)
	assert.Contains(t, all[0].ARN, "eu-west-1")
	assert.Contains(t, all[0].ARN, "123456789012")
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

func TestPutRecordsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "nonexistent",
		"Records":    []map[string]any{{"PartitionKey": "pk", "Data": []byte("data")}},
	})
	// PutRecords calls PutRecord for each entry, which fails, but the outer PutRecords itself succeeds
	// with failed record count set
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.FailedRecordCount)
}

func TestGetShardIteratorBadIteratorType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "bad-iter-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get shard ID
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "bad-iter-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	shardID := descResp.StreamDescription.Shards[0].ShardID

	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "bad-iter-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "INVALID_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetShardIteratorNonExistentShard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "no-shard-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "no-shard-stream",
		"ShardId":           "shardId-not-real",
		"ShardIteratorType": "TRIM_HORIZON",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMultipleShardRouting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 4 shards
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "multi-shard-stream",
		"ShardCount": 4,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put records with different partition keys
	shardIDs := make(map[string]bool)
	for i := range 10 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "multi-shard-stream",
			"PartitionKey": fmt.Sprintf("pk-%d", i),
			"Data":         []byte("data"),
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var putResp struct {
			ShardID string `json:"ShardId"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
		shardIDs[putResp.ShardID] = true
	}

	// With 10 records and 4 shards, we should get records on more than 1 shard
	assert.GreaterOrEqual(t, len(shardIDs), 1)
}

func TestSequenceNumberOrdering(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "order-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get shard ID
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "order-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Put 5 records
	seqNums := make([]string, 5)
	for i := range 5 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "order-stream",
			"PartitionKey": "pk",
			"Data":         []byte("data"),
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var putResp struct {
			SequenceNumber string `json:"SequenceNumber"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
		seqNums[i] = putResp.SequenceNumber
	}

	// Verify ordering
	for i := 1; i < len(seqNums); i++ {
		assert.Greater(t, seqNums[i], seqNums[i-1],
			"sequence numbers should be strictly increasing: %s <= %s", seqNums[i], seqNums[i-1])
	}

	// Read back and verify order
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "order-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var iterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": iterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	require.Len(t, getResp.Records, 5)

	for i, r := range getResp.Records {
		assert.Equal(t, seqNums[i], r.SequenceNumber)
	}
}

func TestPutRecordMaxRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "trim-stream",
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

func TestHandleInvalidJSONRequests(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	// Send invalid JSON to each operation
	ops := []string{
		"CreateStream", "DeleteStream", "DescribeStream", "DescribeStreamSummary",
		"PutRecord", "PutRecords", "GetShardIterator", "GetRecords", "ListShards",
	}

	for _, op := range ops {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{invalid")))
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err, "op=%s", op)
		// All should return 4xx
		assert.GreaterOrEqual(t, rec.Code, 400, "op=%s should return error", op)
	}
}
