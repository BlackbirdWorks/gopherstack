package service_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// roundTrip encodes v as CBOR, decodes to JSON, and returns the JSON.
func roundTripToJSON(t *testing.T, v cbor.Value) []byte {
	t.Helper()

	cborBytes := cbor.Encode(v)

	jsonBytes, err := service.CBORToJSON(cborBytes)
	if err != nil {
		t.Fatalf("CBORToJSON error: %v", err)
	}

	return jsonBytes
}

func TestCBORToJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   cbor.Value
		wantKey string
		wantVal interface{}
	}{
		{
			name:    "string value",
			input:   cbor.Map{"key": cbor.String("hello")},
			wantKey: "key",
			wantVal: "hello",
		},
		{
			name:    "uint value",
			input:   cbor.Map{"n": cbor.Uint(42)},
			wantKey: "n",
			wantVal: float64(42),
		},
		{
			name:    "negative int",
			input:   cbor.Map{"n": cbor.NegInt(7)},
			wantKey: "n",
			wantVal: float64(-7),
		},
		{
			name:    "bool true",
			input:   cbor.Map{"b": cbor.Bool(true)},
			wantKey: "b",
			wantVal: true,
		},
		{
			name:    "bool false",
			input:   cbor.Map{"b": cbor.Bool(false)},
			wantKey: "b",
			wantVal: false,
		},
		{
			name:    "nil value",
			input:   cbor.Map{"n": (*cbor.Nil)(nil)},
			wantKey: "n",
			wantVal: nil,
		},
		{
			name:    "byte string base64-encoded",
			input:   cbor.Map{"B": cbor.Slice([]byte("Hello"))},
			wantKey: "B",
			wantVal: base64.StdEncoding.EncodeToString([]byte("Hello")),
		},
		{
			name:  "nested map",
			input: cbor.Map{"Item": cbor.Map{"pk": cbor.Map{"S": cbor.String("row1")}}},
		},
		{
			name: "list of strings",
			input: cbor.Map{"SS": cbor.List{
				cbor.String("a"),
				cbor.String("b"),
			}},
		},
		{
			name:    "float64",
			input:   cbor.Map{"f": cbor.Float64(3.14)},
			wantKey: "f",
			wantVal: 3.14,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			jsonBytes := roundTripToJSON(t, tc.input)

			if tc.wantKey == "" {
				return
			}

			var got map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &got); err != nil {
				t.Fatalf("unmarshal result: %v", err)
			}

			gotVal, ok := got[tc.wantKey]
			if !ok {
				t.Fatalf("key %q not found in result %s", tc.wantKey, jsonBytes)
			}

			if tc.wantVal == nil {
				if gotVal != nil {
					t.Errorf("got %v, want nil", gotVal)
				}
				return
			}

			if gotVal != tc.wantVal {
				t.Errorf("got %v (%T), want %v (%T)", gotVal, gotVal, tc.wantVal, tc.wantVal)
			}
		})
	}
}

func TestJSONToCBOR(t *testing.T) {
	t.Parallel()

	binaryKeys := map[string]bool{"B": true, "BS": true, "Data": true}

	tests := []struct {
		name      string
		jsonInput string
		binaryKey string
		wantSlice bool
		wantStr   string
	}{
		{
			name:      "plain string stays string",
			jsonInput: `{"S": "hello"}`,
			binaryKey: "S",
			wantStr:   "hello",
		},
		{
			name:      "B field decoded to byte string",
			jsonInput: `{"B": "SGVsbG8="}`,
			binaryKey: "B",
			wantSlice: true,
		},
		{
			name:      "Data field decoded to byte string",
			jsonInput: `{"Data": "SGVsbG8="}`,
			binaryKey: "Data",
			wantSlice: true,
		},
		{
			name:      "non-binary key string stays string",
			jsonInput: `{"N": "123"}`,
			binaryKey: "N",
			wantStr:   "123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cborBytes, err := service.JSONToCBOR([]byte(tc.jsonInput), binaryKeys)
			if err != nil {
				t.Fatalf("JSONToCBOR error: %v", err)
			}

			val, err := cbor.Decode(cborBytes)
			if err != nil {
				t.Fatalf("cbor.Decode error: %v", err)
			}

			m, ok := val.(cbor.Map)
			if !ok {
				t.Fatalf("expected cbor.Map, got %T", val)
			}

			inner, ok := m[tc.binaryKey]
			if !ok {
				t.Fatalf("key %q not found", tc.binaryKey)
			}

			if tc.wantSlice {
				if _, isSlice := inner.(cbor.Slice); !isSlice {
					t.Errorf("expected cbor.Slice for key %q, got %T", tc.binaryKey, inner)
				}
				return
			}

			s, isStr := inner.(cbor.String)
			if !isStr {
				t.Errorf("expected cbor.String for key %q, got %T", tc.binaryKey, inner)
				return
			}

			if string(s) != tc.wantStr {
				t.Errorf("got string %q, want %q", string(s), tc.wantStr)
			}
		})
	}
}

func TestCBORRoundTrip(t *testing.T) {
	t.Parallel()

	binaryKeys := map[string]bool{"B": true, "BS": true}

	tests := []struct {
		name      string
		jsonInput string
	}{
		{
			name:      "simple string attribute",
			jsonInput: `{"TableName":"t","Item":{"pk":{"S":"hello"}}}`,
		},
		{
			name:      "number attribute",
			jsonInput: `{"TableName":"t","Item":{"count":{"N":"42"}}}`,
		},
		{
			name:      "bool attribute",
			jsonInput: `{"TableName":"t","Item":{"active":{"BOOL":true}}}`,
		},
		{
			name:      "null attribute",
			jsonInput: `{"TableName":"t","Item":{"empty":{"NULL":true}}}`,
		},
		{
			name:      "string set",
			jsonInput: `{"TableName":"t","Item":{"tags":{"SS":["a","b","c"]}}}`,
		},
		{
			name:      "map attribute",
			jsonInput: `{"TableName":"t","Item":{"meta":{"M":{"key":{"S":"val"}}}}}`,
		},
		{
			name:      "list attribute",
			jsonInput: `{"TableName":"t","Item":{"list":{"L":[{"S":"x"},{"N":"1"}]}}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// JSON → CBOR → JSON
			cborBytes, err := service.JSONToCBOR([]byte(tc.jsonInput), binaryKeys)
			if err != nil {
				t.Fatalf("JSONToCBOR: %v", err)
			}

			jsonOut, err := service.CBORToJSON(cborBytes)
			if err != nil {
				t.Fatalf("CBORToJSON: %v", err)
			}

			var orig, got map[string]interface{}
			if err := json.Unmarshal([]byte(tc.jsonInput), &orig); err != nil {
				t.Fatalf("unmarshal orig: %v", err)
			}
			if err := json.Unmarshal(jsonOut, &got); err != nil {
				t.Fatalf("unmarshal got: %v", err)
			}

			origJSON, _ := json.Marshal(orig)
			gotJSON, _ := json.Marshal(got)

			if string(origJSON) != string(gotJSON) {
				t.Errorf("round-trip mismatch:\n  orig: %s\n   got: %s", origJSON, gotJSON)
			}
		})
	}
}

func TestCBORBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	binaryKeys := map[string]bool{"B": true, "BS": true}

	payload := []byte{0x01, 0x02, 0x03, 0xFF}
	b64 := base64.StdEncoding.EncodeToString(payload)

	// Build CBOR with a byte string for the "B" key.
	original := cbor.Map{
		"Item": cbor.Map{
			"data": cbor.Map{
				"B": cbor.Slice(payload),
			},
		},
	}

	cborBytes := cbor.Encode(original)

	// CBOR → JSON
	jsonBytes, err := service.CBORToJSON(cborBytes)
	if err != nil {
		t.Fatalf("CBORToJSON: %v", err)
	}

	var jsonMap map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &jsonMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	item := jsonMap["Item"].(map[string]interface{})
	dataAttr := item["data"].(map[string]interface{})
	gotB64 := dataAttr["B"].(string)

	if gotB64 != b64 {
		t.Errorf("CBOR→JSON binary: got base64 %q, want %q", gotB64, b64)
	}

	// JSON → CBOR (round-trip back)
	cborOut, err := service.JSONToCBOR(jsonBytes, binaryKeys)
	if err != nil {
		t.Fatalf("JSONToCBOR: %v", err)
	}

	val, err := cbor.Decode(cborOut)
	if err != nil {
		t.Fatalf("cbor.Decode: %v", err)
	}

	m := val.(cbor.Map)
	itemMap := m["Item"].(cbor.Map)
	dataMap := itemMap["data"].(cbor.Map)
	bVal := dataMap["B"]

	sl, ok := bVal.(cbor.Slice)
	if !ok {
		t.Fatalf("expected cbor.Slice for B, got %T", bVal)
	}

	if string(sl) != string(payload) {
		t.Errorf("binary round-trip: got %v, want %v", []byte(sl), payload)
	}
}

func TestIsCBORRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		contentType string
		want        bool
	}{
		{"application/x-amz-cbor-1.1", true},
		{"application/x-amz-json-1.0", false},
		{"application/x-amz-json-1.1", false},
		{"", false},
		{"application/x-amz-cbor-1.1; charset=utf-8", true},
	}

	for _, tc := range tests {
		t.Run(tc.contentType, func(t *testing.T) {
			t.Parallel()

			req, _ := http.NewRequest("POST", "/", nil) //nolint:noctx
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}

			if got := service.IsCBORRequest(req); got != tc.want {
				t.Errorf("IsCBORRequest(%q) = %v, want %v", tc.contentType, got, tc.want)
			}
		})
	}
}
