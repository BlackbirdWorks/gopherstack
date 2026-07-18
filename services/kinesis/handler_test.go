package kinesis_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) *kinesis.Handler {
	t.Helper()

	backend := kinesis.NewInMemoryBackend()

	return kinesis.NewHandler(backend)
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

func newParityBackend(t *testing.T) *kinesis.InMemoryBackend {
	t.Helper()

	return kinesis.NewInMemoryBackend()
}

func createParityStream(t *testing.T, b *kinesis.InMemoryBackend, name string, shards int) {
	t.Helper()

	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: name,
		ShardCount: shards,
	})
	require.NoError(t, err)
}

// shardHashRange returns the [start, end] big.Int hash range for shard i in an
// n-shard stream — mirrors the CreateStream shard-layout calculation exactly.
func shardHashRange(i, n int) (*big.Int, *big.Int) {
	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), 128),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(n)),
	)

	start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))

	var end *big.Int
	if i == n-1 {
		end = maxHashKey
	} else {
		end = new(big.Int).Sub(
			new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
			big.NewInt(1),
		)
	}

	return start, end
}

// expectedShardIndex returns which shard (0-based) pk lands on in an n-shard stream
// using MD5 routing, matching the AWS Kinesis contract.
func expectedShardIndex(pk string, n int) int {
	sum := md5.Sum([]byte(pk))
	h := new(big.Int).SetBytes(sum[:])

	for i := range n {
		start, end := shardHashRange(i, n)
		if h.Cmp(start) >= 0 && h.Cmp(end) <= 0 {
			return i
		}
	}

	return 0
}

// doParityRequest sends a JSON action request through the handler.
func doParityRequest(t *testing.T, h *kinesis.Handler, action string, body any) *httptest.ResponseRecorder {
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
	req.Header.Set("X-Amz-Target", "Kinesis_20131202."+action)

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

// TestHandlerReset verifies Handler.Reset() clears both backend and tags.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "hr-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	h.Reset()

	b := h.Backend.(*kinesis.InMemoryBackend)
	assert.Equal(t, 0, b.StreamCount())
}

// TestHandlerOpsPreBuilt verifies the dispatch table is pre-built.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.HandlerOpsLen())
}

// TestReset verifies that Reset() clears all backend state.
func TestReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "reset-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	b := h.Backend.(*kinesis.InMemoryBackend)
	assert.Equal(t, 1, b.StreamCount())

	b.Reset()

	assert.Equal(t, 0, b.StreamCount())
}

// TestGetSupportedOperations verifies GetSupportedOperations lists every
// supported Kinesis action. Consolidates three previously-overlapping checks
// (all-ops, UpdateStreamMode presence, and the "new ops" subset) into one
// table-driven test covering the union of all previously-asserted operations.
func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	wantOps := []string{
		"CreateStream", "DeleteStream", "DescribeStream", "DescribeStreamSummary",
		"ListStreams", "PutRecord", "PutRecords", "GetShardIterator", "GetRecords",
		"ListShards", "AddTagsToStream", "RemoveTagsFromStream", "ListTagsForStream",
		"IncreaseStreamRetentionPeriod", "DecreaseStreamRetentionPeriod",
		"RegisterStreamConsumer", "DescribeStreamConsumer", "ListStreamConsumers",
		"DeregisterStreamConsumer", "SubscribeToShard", "UpdateShardCount",
		"EnableEnhancedMonitoring", "DisableEnhancedMonitoring",
		"DescribeLimits", "DescribeAccountSettings",
		"MergeShards", "SplitShard",
		"StartStreamEncryption", "StopStreamEncryption",
		"DeleteResourcePolicy", "GetResourcePolicy", "PutResourcePolicy",
		"ListTagsForResource", "UpdateStreamMode",
	}

	for _, op := range wantOps {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, ops, op, "expected %q to be in GetSupportedOperations()", op)
		})
	}
}
