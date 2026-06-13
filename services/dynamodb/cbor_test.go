package dynamodb_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// cborRequest builds an HTTP POST with X-Amz-Target and a CBOR-encoded body.
func cborRequest(t *testing.T, target string, body cbor.Value) *http.Request {
	t.Helper()

	var raw []byte
	if body != nil {
		raw = cbor.Encode(body)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", service.ContentTypeCBOR)
	req.Header.Set("X-Amz-Target", target)

	return req
}

// decodeCBORResponse decodes the response body (CBOR) into a cbor.Map.
func decodeCBORResponse(t *testing.T, rr *httptest.ResponseRecorder) cbor.Map {
	t.Helper()
	require.Equal(t, service.ContentTypeCBOR, rr.Header().Get("Content-Type"))

	val, err := cbor.Decode(rr.Body.Bytes())
	require.NoError(t, err, "cbor.Decode response")

	m, ok := val.(cbor.Map)
	require.True(t, ok, "expected cbor.Map response, got %T", val)

	return m
}

// setupCBORTable creates a table via the JSON path (no CBOR needed for setup).
func setupCBORTable(t *testing.T, handler *dynamodb.DynamoDBHandler, tableName string) {
	t.Helper()

	const pkAttr = "pk"

	body := mustMarshal(t, map[string]any{
		"TableName": tableName,
		"KeySchema": []map[string]any{
			{"AttributeName": pkAttr, "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": pkAttr, "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rr := httptest.NewRecorder()

	err := serveEchoHandler(handler.Handler(), rr, req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rr.Code, "CreateTable: %s", rr.Body.String())
}

// TestDynamoDBCBOR_PutItemGetItem verifies a PutItem→GetItem round-trip over the CBOR protocol.
func TestDynamoDBCBOR_PutItemGetItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		item       cbor.Map
		key        cbor.Map
		checkField string
		checkType  string
	}{
		{
			name: "string attribute",
			item: cbor.Map{
				"pk":   cbor.Map{"S": cbor.String("row1")},
				"name": cbor.Map{"S": cbor.String("Alice")},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row1")},
			},
			checkField: "name",
			checkType:  "S",
		},
		{
			name: "number attribute",
			item: cbor.Map{
				"pk":    cbor.Map{"S": cbor.String("row2")},
				"count": cbor.Map{"N": cbor.String("42")},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row2")},
			},
			checkField: "count",
			checkType:  "N",
		},
		{
			name: "bool attribute",
			item: cbor.Map{
				"pk":     cbor.Map{"S": cbor.String("row3")},
				"active": cbor.Map{"BOOL": cbor.Bool(true)},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row3")},
			},
			checkField: "active",
			checkType:  "BOOL",
		},
		{
			name: "null attribute",
			item: cbor.Map{
				"pk":    cbor.Map{"S": cbor.String("row4")},
				"empty": cbor.Map{"NULL": cbor.Bool(true)},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row4")},
			},
			checkField: "empty",
			checkType:  "NULL",
		},
		{
			name: "string set attribute",
			item: cbor.Map{
				"pk":   cbor.Map{"S": cbor.String("row5")},
				"tags": cbor.Map{"SS": cbor.List{cbor.String("a"), cbor.String("b")}},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row5")},
			},
			checkField: "tags",
			checkType:  "SS",
		},
		{
			name: "map attribute",
			item: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row6")},
				"meta": cbor.Map{"M": cbor.Map{
					"k": cbor.Map{"S": cbor.String("v")},
				}},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row6")},
			},
			checkField: "meta",
			checkType:  "M",
		},
		{
			name: "list attribute",
			item: cbor.Map{
				"pk":   cbor.Map{"S": cbor.String("row7")},
				"list": cbor.Map{"L": cbor.List{cbor.Map{"S": cbor.String("x")}}},
			},
			key: cbor.Map{
				"pk": cbor.Map{"S": cbor.String("row7")},
			},
			checkField: "list",
			checkType:  "L",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)
			setupCBORTable(t, handler, "CBORTest")

			// PutItem via CBOR
			putBody := cbor.Map{
				"TableName": cbor.String("CBORTest"),
				"Item":      tc.item,
			}
			putReq := cborRequest(t, "DynamoDB_20120810.PutItem", putBody)
			putRR := httptest.NewRecorder()
			require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
			assert.Equal(t, http.StatusOK, putRR.Code, "PutItem status: %s", putRR.Body.String())
			assert.Equal(t, service.ContentTypeCBOR, putRR.Header().Get("Content-Type"))

			// GetItem via CBOR
			getBody := cbor.Map{
				"TableName": cbor.String("CBORTest"),
				"Key":       tc.key,
			}
			getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
			getRR := httptest.NewRecorder()
			require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
			assert.Equal(t, http.StatusOK, getRR.Code, "GetItem status: %s", getRR.Body.String())

			resp := decodeCBORResponse(t, getRR)

			item, ok := resp["Item"]
			require.True(t, ok, "response must contain Item")

			itemMap, ok := item.(cbor.Map)
			require.True(t, ok, "Item must be cbor.Map")

			field, ok := itemMap[tc.checkField]
			require.True(t, ok, "Item must contain field %q", tc.checkField)

			attrMap, ok := field.(cbor.Map)
			require.True(t, ok, "field %q must be a cbor.Map AttributeValue", tc.checkField)

			_, ok = attrMap[tc.checkType]
			assert.True(t, ok,
				"attribute %q must have type key %q, got keys %v",
				tc.checkField, tc.checkType, cborMapKeys(attrMap))
		})
	}
}

// TestDynamoDBCBOR_BinaryAttribute verifies that binary (B) attribute values are
// encoded as CBOR byte strings (major type 2) in both directions.
func TestDynamoDBCBOR_BinaryAttribute(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "CBORBin")

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	// PutItem with a binary attribute encoded as CBOR byte string.
	putBody := cbor.Map{
		"TableName": cbor.String("CBORBin"),
		"Item": cbor.Map{
			"pk":   cbor.Map{"S": cbor.String("bin1")},
			"data": cbor.Map{"B": cbor.Slice(payload)},
		},
	}

	putReq := cborRequest(t, "DynamoDB_20120810.PutItem", putBody)
	putRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
	assert.Equal(t, http.StatusOK, putRR.Code, "PutItem: %s", putRR.Body.String())

	// GetItem and verify the binary attribute comes back as CBOR Slice.
	getBody := cbor.Map{
		"TableName": cbor.String("CBORBin"),
		"Key": cbor.Map{
			"pk": cbor.Map{"S": cbor.String("bin1")},
		},
	}

	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	require.Equal(t, http.StatusOK, getRR.Code, "GetItem: %s", getRR.Body.String())

	resp := decodeCBORResponse(t, getRR)

	item := resp["Item"].(cbor.Map)
	dataAttr := item["data"].(cbor.Map)
	bVal, ok := dataAttr["B"]
	require.True(t, ok, "B key must be present in data attribute")

	sl, ok := bVal.(cbor.Slice)
	require.True(t, ok, "B value must be cbor.Slice, got %T", bVal)
	assert.Equal(t, payload, []byte(sl), "binary payload must survive the round-trip")
}

// TestDynamoDBCBOR_BinarySetAttribute verifies BS (binary set) attribute encoding.
func TestDynamoDBCBOR_BinarySetAttribute(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "CBORBinSet")

	payloads := [][]byte{
		{0x01, 0x02},
		{0x03, 0x04},
	}

	bsList := make(cbor.List, len(payloads))
	for i, p := range payloads {
		bsList[i] = cbor.Slice(p)
	}

	putBody := cbor.Map{
		"TableName": cbor.String("CBORBinSet"),
		"Item": cbor.Map{
			"pk": cbor.Map{"S": cbor.String("bset1")},
			"bs": cbor.Map{"BS": bsList},
		},
	}

	putReq := cborRequest(t, "DynamoDB_20120810.PutItem", putBody)
	putRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
	assert.Equal(t, http.StatusOK, putRR.Code)

	getBody := cbor.Map{
		"TableName": cbor.String("CBORBinSet"),
		"Key":       cbor.Map{"pk": cbor.Map{"S": cbor.String("bset1")}},
	}

	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	require.Equal(t, http.StatusOK, getRR.Code)

	resp := decodeCBORResponse(t, getRR)
	item := resp["Item"].(cbor.Map)
	bsAttr := item["bs"].(cbor.Map)

	bsVal, ok := bsAttr["BS"]
	require.True(t, ok, "BS key must be present")

	bsList2, ok := bsVal.(cbor.List)
	require.True(t, ok, "BS value must be cbor.List")
	assert.Len(t, bsList2, len(payloads))

	for _, el := range bsList2 {
		_, isSlice := el.(cbor.Slice)
		assert.True(t, isSlice, "each BS element must be cbor.Slice, got %T", el)
	}
}

// TestDynamoDBCBOR_MissingItem verifies GetItem for a non-existent item over CBOR.
func TestDynamoDBCBOR_MissingItem(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "CBORMiss")

	getBody := cbor.Map{
		"TableName": cbor.String("CBORMiss"),
		"Key":       cbor.Map{"pk": cbor.Map{"S": cbor.String("nope")}},
	}

	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	assert.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, service.ContentTypeCBOR, getRR.Header().Get("Content-Type"))

	resp := decodeCBORResponse(t, getRR)
	_, hasItem := resp["Item"]
	assert.False(t, hasItem, "missing item should not have Item in response")
}

// TestDynamoDBCBOR_InvalidBody verifies that a malformed CBOR body yields 400.
func TestDynamoDBCBOR_InvalidBody(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte{0xFF, 0xFF}))
	req.Header.Set("Content-Type", service.ContentTypeCBOR)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	rr := httptest.NewRecorder()

	require.NoError(t, serveEchoHandler(handler.Handler(), rr, req))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// TestDynamoDBCBOR_TableNotFound verifies CBOR error response when table is missing.
func TestDynamoDBCBOR_TableNotFound(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	getBody := cbor.Map{
		"TableName": cbor.String("NoSuchTable"),
		"Key":       cbor.Map{"pk": cbor.Map{"S": cbor.String("x")}},
	}

	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	assert.Equal(t, http.StatusBadRequest, getRR.Code)
}

// TestDynamoDBCBOR_CRC32Header verifies that CBOR responses include X-Amz-Crc32.
func TestDynamoDBCBOR_CRC32Header(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "CBORCRC")

	putBody := cbor.Map{
		"TableName": cbor.String("CBORCRC"),
		"Item":      cbor.Map{"pk": cbor.Map{"S": cbor.String("c1")}},
	}
	putReq := cborRequest(t, "DynamoDB_20120810.PutItem", putBody)
	putRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
	assert.Equal(t, http.StatusOK, putRR.Code)
	assert.NotEmpty(t, putRR.Header().Get("X-Amz-Crc32"), "X-Amz-Crc32 must be set for CBOR responses")
}

// TestDynamoDBCBOR_JSONAndCBORCoexist verifies that JSON and CBOR requests can
// share the same backend state — CBOR is purely a wire protocol.
func TestDynamoDBCBOR_JSONAndCBORCoexist(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "CoexistTable")

	// Write via JSON.
	jsonPut := mustMarshal(t, map[string]any{
		"TableName": "CoexistTable",
		"Item": map[string]any{
			"pk":    map[string]any{"S": "jkey"},
			"value": map[string]any{"S": "from-json"},
		},
	})
	putReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(jsonPut))
	putReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	putReq.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	putRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
	require.Equal(t, http.StatusOK, putRR.Code)

	// Read via CBOR.
	getBody := cbor.Map{
		"TableName": cbor.String("CoexistTable"),
		"Key":       cbor.Map{"pk": cbor.Map{"S": cbor.String("jkey")}},
	}
	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	require.Equal(t, http.StatusOK, getRR.Code)

	resp := decodeCBORResponse(t, getRR)
	item := resp["Item"].(cbor.Map)
	valAttr := item["value"].(cbor.Map)
	s, ok := valAttr["S"].(cbor.String)
	require.True(t, ok)
	assert.Equal(t, "from-json", string(s))

	// Write via CBOR, read via JSON.
	putBody2 := cbor.Map{
		"TableName": cbor.String("CoexistTable"),
		"Item": cbor.Map{
			"pk":    cbor.Map{"S": cbor.String("ckey")},
			"value": cbor.Map{"S": cbor.String("from-cbor")},
		},
	}
	putReq2 := cborRequest(t, "DynamoDB_20120810.PutItem", putBody2)
	putRR2 := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR2, putReq2))
	require.Equal(t, http.StatusOK, putRR2.Code)

	jsonGet := mustMarshal(t, map[string]any{
		"TableName": "CoexistTable",
		"Key":       map[string]any{"pk": map[string]any{"S": "ckey"}},
	})
	getReq2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(jsonGet))
	getReq2.Header.Set("Content-Type", "application/x-amz-json-1.0")
	getReq2.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	getRR2 := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR2, getReq2))
	require.Equal(t, http.StatusOK, getRR2.Code)

	var jsonResp map[string]any
	require.NoError(t, json.Unmarshal(getRR2.Body.Bytes(), &jsonResp))
	itemJSON := jsonResp["Item"].(map[string]any)
	valueJSON := itemJSON["value"].(map[string]any)
	assert.Equal(t, "from-cbor", valueJSON["S"])
}

// TestDynamoDBCBOR_Base64RoundTrip tests that binary data written as base64 JSON
// and read via CBOR produces a CBOR Slice (not a String).
func TestDynamoDBCBOR_Base64RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	setupCBORTable(t, handler, "B64Table")

	rawBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	b64 := base64.StdEncoding.EncodeToString(rawBytes)

	// Write via JSON with base64-encoded binary.
	jsonPut := mustMarshal(t, map[string]any{
		"TableName": "B64Table",
		"Item": map[string]any{
			"pk":  map[string]any{"S": "b64key"},
			"bin": map[string]any{"B": b64},
		},
	})
	putReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(jsonPut))
	putReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	putReq.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	putRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), putRR, putReq))
	require.Equal(t, http.StatusOK, putRR.Code)

	// Read via CBOR and check B is cbor.Slice.
	getBody := cbor.Map{
		"TableName": cbor.String("B64Table"),
		"Key":       cbor.Map{"pk": cbor.Map{"S": cbor.String("b64key")}},
	}
	getReq := cborRequest(t, "DynamoDB_20120810.GetItem", getBody)
	getRR := httptest.NewRecorder()
	require.NoError(t, serveEchoHandler(handler.Handler(), getRR, getReq))
	require.Equal(t, http.StatusOK, getRR.Code)

	resp := decodeCBORResponse(t, getRR)
	item := resp["Item"].(cbor.Map)
	binAttr := item["bin"].(cbor.Map)
	bVal, ok := binAttr["B"]
	require.True(t, ok)

	sl, ok := bVal.(cbor.Slice)
	require.True(t, ok, "B must be cbor.Slice when CBOR protocol is used, got %T", bVal)
	assert.Equal(t, rawBytes, []byte(sl))
}

// cborMapKeys returns the keys of a cbor.Map for diagnostic output.
func cborMapKeys(m cbor.Map) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
