package kinesis_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

const kinesisPrefix = "Kinesis_20131202."

// cborKinesisRequest builds a POST with CBOR Content-Type and X-Amz-Target.
func cborKinesisRequest(t *testing.T, action string, body cbor.Value) *http.Request {
	t.Helper()

	var raw []byte
	if body != nil {
		raw = cbor.Encode(body)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", service.ContentTypeCBOR)
	req.Header.Set("X-Amz-Target", kinesisPrefix+action)

	return req
}

// serveCBOR invokes the Kinesis handler and returns the recorder.
func serveCBOR(t *testing.T, h *kinesis.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// decodeCBORKinesisResponse decodes a CBOR response body to cbor.Map.
func decodeCBORKinesisResponse(t *testing.T, rr *httptest.ResponseRecorder) cbor.Map {
	t.Helper()
	require.Equal(t, service.ContentTypeCBOR, rr.Header().Get("Content-Type"))

	val, err := cbor.Decode(rr.Body.Bytes())
	require.NoError(t, err)

	m, ok := val.(cbor.Map)
	require.True(t, ok, "expected cbor.Map, got %T", val)

	return m
}

// TestKinesisCBOR_PutRecord verifies PutRecord round-trip over the CBOR protocol.
func TestKinesisCBOR_PutRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		partKey string
		data    []byte
	}{
		{
			name:    "small binary payload",
			data:    []byte("hello kinesis"),
			partKey: "pk1",
		},
		{
			name:    "binary payload with null bytes",
			data:    []byte{0x00, 0x01, 0x02, 0xFF},
			partKey: "pk2",
		},
		{
			name:    "empty payload",
			data:    []byte{},
			partKey: "pk3",
		},
		{
			name:    "larger payload",
			data:    bytes.Repeat([]byte{0xAB}, 1024),
			partKey: "pk4",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createKinesisStream(t, h, "CborStream")

			putBody := cbor.Map{
				"StreamName":   cbor.String("CborStream"),
				"Data":         cbor.Slice(tc.data),
				"PartitionKey": cbor.String(tc.partKey),
			}

			req := cborKinesisRequest(t, "PutRecord", putBody)
			rr := serveCBOR(t, h, req)
			assert.Equal(t, http.StatusOK, rr.Code, "PutRecord: %s", rr.Body.String())
			assert.Equal(t, service.ContentTypeCBOR, rr.Header().Get("Content-Type"))

			resp := decodeCBORKinesisResponse(t, rr)

			_, hasSeq := resp["SequenceNumber"]
			assert.True(t, hasSeq, "response must contain SequenceNumber")
			_, hasShard := resp["ShardId"]
			assert.True(t, hasShard, "response must contain ShardId")
		})
	}
}

// TestKinesisCBOR_PutRecords verifies PutRecords over the CBOR protocol.
func TestKinesisCBOR_PutRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "CborMultiStream")

	records := cbor.List{
		cbor.Map{
			"Data":         cbor.Slice([]byte("record-1")),
			"PartitionKey": cbor.String("p1"),
		},
		cbor.Map{
			"Data":         cbor.Slice([]byte("record-2")),
			"PartitionKey": cbor.String("p2"),
		},
		cbor.Map{
			"Data":         cbor.Slice([]byte("record-3")),
			"PartitionKey": cbor.String("p3"),
		},
	}

	putBody := cbor.Map{
		"StreamName": cbor.String("CborMultiStream"),
		"Records":    records,
	}

	req := cborKinesisRequest(t, "PutRecords", putBody)
	rr := serveCBOR(t, h, req)
	assert.Equal(t, http.StatusOK, rr.Code, "PutRecords: %s", rr.Body.String())

	resp := decodeCBORKinesisResponse(t, rr)

	recList, ok := resp["Records"].(cbor.List)
	require.True(t, ok, "response must contain Records list")
	assert.Len(t, recList, 3)
}

// TestKinesisCBOR_GetRecords verifies that GetRecords returns Data as CBOR Slice.
func TestKinesisCBOR_GetRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "CborGetStream")

	payload := []byte("cbor-test-data")

	// Put a record via CBOR.
	putBody := cbor.Map{
		"StreamName":   cbor.String("CborGetStream"),
		"Data":         cbor.Slice(payload),
		"PartitionKey": cbor.String("p1"),
	}
	putReq := cborKinesisRequest(t, "PutRecord", putBody)
	putRR := serveCBOR(t, h, putReq)
	require.Equal(t, http.StatusOK, putRR.Code)

	// Get shard iterator via JSON.
	iterBody := map[string]any{
		"StreamName":        "CborGetStream",
		"ShardId":           "shardId-000000000000",
		"ShardIteratorType": "TRIM_HORIZON",
	}
	iterBytes, _ := json.Marshal(iterBody)
	iterReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(iterBytes))
	iterReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	iterReq.Header.Set("X-Amz-Target", kinesisPrefix+"GetShardIterator")

	e := echo.New()
	iterRec := httptest.NewRecorder()
	iterC := e.NewContext(iterReq, iterRec)
	require.NoError(t, h.Handler()(iterC))
	require.Equal(t, http.StatusOK, iterRec.Code)

	var iterResp map[string]any
	require.NoError(t, json.Unmarshal(iterRec.Body.Bytes(), &iterResp))
	shardIter, ok := iterResp["ShardIterator"].(string)
	require.True(t, ok, "must have ShardIterator")

	// GetRecords via CBOR.
	getBody := cbor.Map{
		"ShardIterator": cbor.String(shardIter),
		"Limit":         cbor.Uint(10),
	}
	getReq := cborKinesisRequest(t, "GetRecords", getBody)
	getRR := serveCBOR(t, h, getReq)
	require.Equal(t, http.StatusOK, getRR.Code)

	resp := decodeCBORKinesisResponse(t, getRR)

	recList, ok := resp["Records"].(cbor.List)
	require.True(t, ok, "response must contain Records")
	require.NotEmpty(t, recList, "should have at least one record")

	firstRec, ok := recList[0].(cbor.Map)
	require.True(t, ok)

	dataVal, ok := firstRec["Data"]
	require.True(t, ok, "record must have Data field")

	sl, ok := dataVal.(cbor.Slice)
	require.True(t, ok, "Data must be cbor.Slice over CBOR protocol, got %T", dataVal)
	assert.Equal(t, payload, []byte(sl))
}

// TestKinesisCBOR_DataRoundTrip tests that binary written as base64 via JSON is
// returned as cbor.Slice over the CBOR protocol.
func TestKinesisCBOR_DataRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "CborB64Stream")

	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
	b64Data := base64.StdEncoding.EncodeToString(rawData)

	// Write via JSON with base64 Data.
	jsonPut := map[string]any{
		"StreamName":   "CborB64Stream",
		"Data":         b64Data,
		"PartitionKey": "pk1",
	}
	b, _ := json.Marshal(jsonPut)
	putReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	putReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	putReq.Header.Set("X-Amz-Target", kinesisPrefix+"PutRecord")

	e := echo.New()
	putRec := httptest.NewRecorder()
	putC := e.NewContext(putReq, putRec)
	require.NoError(t, h.Handler()(putC))
	require.Equal(t, http.StatusOK, putRec.Code)

	// Get shard iterator.
	iterReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(mustMarshalBytes(t, map[string]any{
		"StreamName":        "CborB64Stream",
		"ShardId":           "shardId-000000000000",
		"ShardIteratorType": "TRIM_HORIZON",
	})))
	iterReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	iterReq.Header.Set("X-Amz-Target", kinesisPrefix+"GetShardIterator")
	iterRec := httptest.NewRecorder()
	iterC := e.NewContext(iterReq, iterRec)
	require.NoError(t, h.Handler()(iterC))
	var iterResp map[string]any
	require.NoError(t, json.Unmarshal(iterRec.Body.Bytes(), &iterResp))
	shardIter := iterResp["ShardIterator"].(string)

	// Read via CBOR.
	getReq := cborKinesisRequest(t, "GetRecords", cbor.Map{
		"ShardIterator": cbor.String(shardIter),
	})
	getRR := serveCBOR(t, h, getReq)
	require.Equal(t, http.StatusOK, getRR.Code)

	resp := decodeCBORKinesisResponse(t, getRR)
	recs := resp["Records"].(cbor.List)
	require.NotEmpty(t, recs)

	rec0 := recs[0].(cbor.Map)
	dataVal := rec0["Data"]

	sl, ok := dataVal.(cbor.Slice)
	require.True(t, ok, "Data must be cbor.Slice over CBOR protocol, got %T", dataVal)
	assert.Equal(t, rawData, []byte(sl))
}

// TestKinesisCBOR_InvalidBody verifies 400 for a malformed CBOR request body.
func TestKinesisCBOR_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte{0xFF, 0xFF}))
	req.Header.Set("Content-Type", service.ContentTypeCBOR)
	req.Header.Set("X-Amz-Target", kinesisPrefix+"PutRecord")

	rr := serveCBOR(t, h, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// TestKinesisCBOR_CreateStream verifies CreateStream over CBOR.
func TestKinesisCBOR_CreateStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := cbor.Map{
		"StreamName": cbor.String("CborNewStream"),
		"ShardCount": cbor.Uint(1),
	}

	req := cborKinesisRequest(t, "CreateStream", body)
	rr := serveCBOR(t, h, req)
	assert.Equal(t, http.StatusOK, rr.Code, "CreateStream: %s", rr.Body.String())
	assert.Equal(t, service.ContentTypeCBOR, rr.Header().Get("Content-Type"))
}

// TestKinesisCBOR_ListStreams verifies ListStreams over CBOR.
func TestKinesisCBOR_ListStreams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "ListStream1")
	createKinesisStream(t, h, "ListStream2")

	req := cborKinesisRequest(t, "ListStreams", cbor.Map{})
	rr := serveCBOR(t, h, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	resp := decodeCBORKinesisResponse(t, rr)
	names, ok := resp["StreamNames"].(cbor.List)
	require.True(t, ok, "response must contain StreamNames")
	assert.GreaterOrEqual(t, len(names), 2)
}

// TestKinesisCBOR_JSONAndCBORCoexist verifies CBOR and JSON writes share the same stream.
func TestKinesisCBOR_JSONAndCBORCoexist(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "CoexistStream")

	// Write via CBOR.
	cborPayload := []byte("cbor-record")
	putCBOR := cbor.Map{
		"StreamName":   cbor.String("CoexistStream"),
		"Data":         cbor.Slice(cborPayload),
		"PartitionKey": cbor.String("pk1"),
	}
	req1 := cborKinesisRequest(t, "PutRecord", putCBOR)
	rr1 := serveCBOR(t, h, req1)
	require.Equal(t, http.StatusOK, rr1.Code)

	// Write via JSON.
	jsonPayload := []byte("json-record")
	b64 := base64.StdEncoding.EncodeToString(jsonPayload)
	putJSON := map[string]any{
		"StreamName":   "CoexistStream",
		"Data":         b64,
		"PartitionKey": "pk2",
	}
	rawJSON, _ := json.Marshal(putJSON)
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(rawJSON))
	req2.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req2.Header.Set("X-Amz-Target", kinesisPrefix+"PutRecord")
	e := echo.New()
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	require.NoError(t, h.Handler()(c2))
	require.Equal(t, http.StatusOK, rec2.Code)

	// Read via JSON to confirm both records are present.
	iterReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(mustMarshalBytes(t, map[string]any{
		"StreamName":        "CoexistStream",
		"ShardId":           "shardId-000000000000",
		"ShardIteratorType": "TRIM_HORIZON",
	})))
	iterReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	iterReq.Header.Set("X-Amz-Target", kinesisPrefix+"GetShardIterator")
	iterRec := httptest.NewRecorder()
	iterC := e.NewContext(iterReq, iterRec)
	require.NoError(t, h.Handler()(iterC))
	var iterResp map[string]any
	require.NoError(t, json.Unmarshal(iterRec.Body.Bytes(), &iterResp))
	shardIter := iterResp["ShardIterator"].(string)

	getReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(mustMarshalBytes(t, map[string]any{
		"ShardIterator": shardIter,
		"Limit":         10,
	})))
	getReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	getReq.Header.Set("X-Amz-Target", kinesisPrefix+"GetRecords")
	getRec := httptest.NewRecorder()
	getC := e.NewContext(getReq, getRec)
	require.NoError(t, h.Handler()(getC))
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	recs := getResp["Records"].([]any)
	assert.GreaterOrEqual(t, len(recs), 2, "both records must be visible via JSON")
}

func mustMarshalBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return b
}
