package service_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"

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

	type args struct {
		input cbor.Value
	}
	type wants struct {
		val any
		key string
	}

	tests := []struct {
		args  args
		wants wants
		name  string
	}{
		{
			name:  "string value",
			args:  args{input: cbor.Map{"key": cbor.String("hello")}},
			wants: wants{key: "key", val: "hello"},
		},
		{
			name:  "uint value",
			args:  args{input: cbor.Map{"n": cbor.Uint(42)}},
			wants: wants{key: "n", val: float64(42)},
		},
		{
			name:  "negative int",
			args:  args{input: cbor.Map{"n": cbor.NegInt(7)}},
			wants: wants{key: "n", val: float64(-7)},
		},
		{
			name:  "bool true",
			args:  args{input: cbor.Map{"b": cbor.Bool(true)}},
			wants: wants{key: "b", val: true},
		},
		{
			name:  "bool false",
			args:  args{input: cbor.Map{"b": cbor.Bool(false)}},
			wants: wants{key: "b", val: false},
		},
		{
			name:  "nil value",
			args:  args{input: cbor.Map{"n": (*cbor.Nil)(nil)}},
			wants: wants{key: "n", val: nil},
		},
		{
			name:  "byte string base64-encoded",
			args:  args{input: cbor.Map{"B": cbor.Slice([]byte("Hello"))}},
			wants: wants{key: "B", val: base64.StdEncoding.EncodeToString([]byte("Hello"))},
		},
		{
			name:  "nested map",
			args:  args{input: cbor.Map{"Item": cbor.Map{"pk": cbor.Map{"S": cbor.String("row1")}}}},
			wants: wants{},
		},
		{
			name: "list of strings",
			args: args{input: cbor.Map{"SS": cbor.List{
				cbor.String("a"),
				cbor.String("b"),
			}}},
			wants: wants{},
		},
		{
			name:  "float64",
			args:  args{input: cbor.Map{"f": cbor.Float64(3.14)}},
			wants: wants{key: "f", val: 3.14},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			jsonBytes := roundTripToJSON(t, tc.args.input)

			if tc.wants.key == "" {
				return
			}

			var got map[string]any
			if err := json.Unmarshal(jsonBytes, &got); err != nil {
				t.Fatalf("unmarshal result: %v", err)
			}

			gotVal, ok := got[tc.wants.key]
			if !ok {
				t.Fatalf("key %q not found in result %s", tc.wants.key, jsonBytes)
			}

			if tc.wants.val == nil {
				if gotVal != nil {
					t.Errorf("got %v, want nil", gotVal)
				}

				return
			}

			if gotVal != tc.wants.val {
				t.Errorf("got %v (%T), want %v (%T)", gotVal, gotVal, tc.wants.val, tc.wants.val)
			}
		})
	}
}

func TestJSONToCBOR(t *testing.T) {
	t.Parallel()

	binaryKeys := map[string]bool{"B": true, "BS": true, "Data": true}

	type args struct {
		jsonInput string
		binaryKey string
	}
	type wants struct {
		str   string
		slice bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "plain string stays string",
			args:  args{jsonInput: `{"S": "hello"}`, binaryKey: "S"},
			wants: wants{str: "hello"},
		},
		{
			name:  "B field decoded to byte string",
			args:  args{jsonInput: `{"B": "SGVsbG8="}`, binaryKey: "B"},
			wants: wants{slice: true},
		},
		{
			name:  "Data field decoded to byte string",
			args:  args{jsonInput: `{"Data": "SGVsbG8="}`, binaryKey: "Data"},
			wants: wants{slice: true},
		},
		{
			name:  "non-binary key string stays string",
			args:  args{jsonInput: `{"N": "123"}`, binaryKey: "N"},
			wants: wants{str: "123"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cborBytes, err := service.JSONToCBOR([]byte(tc.args.jsonInput), binaryKeys)
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

			inner, ok := m[tc.args.binaryKey]
			if !ok {
				t.Fatalf("key %q not found", tc.args.binaryKey)
			}

			if tc.wants.slice {
				if _, isSlice := inner.(cbor.Slice); !isSlice {
					t.Errorf("expected cbor.Slice for key %q, got %T", tc.args.binaryKey, inner)
				}

				return
			}

			s, isStr := inner.(cbor.String)
			if !isStr {
				t.Errorf("expected cbor.String for key %q, got %T", tc.args.binaryKey, inner)

				return
			}

			if string(s) != tc.wants.str {
				t.Errorf("got string %q, want %q", string(s), tc.wants.str)
			}
		})
	}
}

func TestCBORRoundTrip(t *testing.T) {
	t.Parallel()

	binaryKeys := map[string]bool{"B": true, "BS": true}

	type args struct {
		jsonInput string
	}
	type wants struct {
		jsonInput string
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "simple string attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"pk":{"S":"hello"}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"pk":{"S":"hello"}}}`},
		},
		{
			name:  "number attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"count":{"N":"42"}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"count":{"N":"42"}}}`},
		},
		{
			name:  "bool attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"active":{"BOOL":true}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"active":{"BOOL":true}}}`},
		},
		{
			name:  "null attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"empty":{"NULL":true}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"empty":{"NULL":true}}}`},
		},
		{
			name:  "string set",
			args:  args{jsonInput: `{"TableName":"t","Item":{"tags":{"SS":["a","b","c"]}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"tags":{"SS":["a","b","c"]}}}`},
		},
		{
			name:  "map attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"meta":{"M":{"key":{"S":"val"}}}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"meta":{"M":{"key":{"S":"val"}}}}}`},
		},
		{
			name:  "list attribute",
			args:  args{jsonInput: `{"TableName":"t","Item":{"list":{"L":[{"S":"x"},{"N":"1"}]}}}`},
			wants: wants{jsonInput: `{"TableName":"t","Item":{"list":{"L":[{"S":"x"},{"N":"1"}]}}}`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// JSON → CBOR → JSON
			cborBytes, err := service.JSONToCBOR([]byte(tc.args.jsonInput), binaryKeys)
			if err != nil {
				t.Fatalf("JSONToCBOR: %v", err)
			}

			jsonOut, err := service.CBORToJSON(cborBytes)
			if err != nil {
				t.Fatalf("CBORToJSON: %v", err)
			}

			var orig, got map[string]any
			if e := json.Unmarshal([]byte(tc.args.jsonInput), &orig); e != nil {
				t.Fatalf("unmarshal orig: %v", e)
			}
			if e := json.Unmarshal(jsonOut, &got); e != nil {
				t.Fatalf("unmarshal got: %v", e)
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

	type args struct {
		payload []byte
	}
	type wants struct {
		payload []byte
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "binary round trip",
			args: args{
				payload: []byte{0x01, 0x02, 0x03, 0xFF},
			},
			wants: wants{
				payload: []byte{0x01, 0x02, 0x03, 0xFF},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b64 := base64.StdEncoding.EncodeToString(tc.args.payload)

			// Build CBOR with a byte string for the "B" key.
			original := cbor.Map{
				"Item": cbor.Map{
					"data": cbor.Map{
						"B": cbor.Slice(tc.args.payload),
					},
				},
			}

			cborBytes := cbor.Encode(original)

			// CBOR → JSON
			jsonBytes, err := service.CBORToJSON(cborBytes)
			if err != nil {
				t.Fatalf("CBORToJSON: %v", err)
			}

			var jsonMap map[string]any
			if err = json.Unmarshal(jsonBytes, &jsonMap); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			item := jsonMap["Item"].(map[string]any)
			dataAttr := item["data"].(map[string]any)
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

			if string(sl) != string(tc.wants.payload) {
				t.Errorf("binary round-trip: got %v, want %v", []byte(sl), tc.wants.payload)
			}
		})
	}
}

func TestIsCBORRequest(t *testing.T) {
	t.Parallel()

	type args struct {
		contentType string
	}
	type wants struct {
		isCBOR bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "valid cbor",
			args:  args{contentType: "application/x-amz-cbor-1.1"},
			wants: wants{isCBOR: true},
		},
		{
			name:  "json 1.0",
			args:  args{contentType: "application/x-amz-json-1.0"},
			wants: wants{isCBOR: false},
		},
		{
			name:  "json 1.1",
			args:  args{contentType: "application/x-amz-json-1.1"},
			wants: wants{isCBOR: false},
		},
		{
			name:  "empty",
			args:  args{contentType: ""},
			wants: wants{isCBOR: false},
		},
		{
			name:  "with charset",
			args:  args{contentType: "application/x-amz-cbor-1.1; charset=utf-8"},
			wants: wants{isCBOR: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, _ := http.NewRequest(http.MethodPost, "/", nil)
			if tc.args.contentType != "" {
				req.Header.Set("Content-Type", tc.args.contentType)
			}

			if got := service.IsCBORRequest(req); got != tc.wants.isCBOR {
				t.Errorf("IsCBORRequest(%q) = %v, want %v", tc.args.contentType, got, tc.wants.isCBOR)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// rpc-v2-cbor (Smithy RPCv2 CBOR) helpers, shared by AppStream and
// CloudWatch -- see services/appstream/rpcv2cbor.go and
// services/cloudwatch/rpcv2cbor.go, the two rpc-v2-cbor services in this
// codebase. This protocol is distinct from the AWS CBOR-1.1 protocol tested
// above: it dispatches on URL path prefix rather than Content-Type, and
// carries the operation name in the path rather than an X-Amz-Target header.
// ---------------------------------------------------------------------------

const testRPCv2CBORServicePath = "/service/TestService/operation/"

func TestIsRPCv2CBORRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{
			name:   "POST under service path matches",
			method: http.MethodPost,
			path:   testRPCv2CBORServicePath + "SomeOperation",
			want:   true,
		},
		{
			name:   "GET under service path does not match",
			method: http.MethodGet,
			path:   testRPCv2CBORServicePath + "SomeOperation",
			want:   false,
		},
		{
			name:   "POST outside service path does not match",
			method: http.MethodPost,
			path:   "/some/other/path",
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, nil)

			if got := service.IsRPCv2CBORRequest(req, testRPCv2CBORServicePath); got != tc.want {
				t.Errorf("IsRPCv2CBORRequest(%s %q) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestExtractRPCv2CBOROperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "strips service path prefix",
			path: testRPCv2CBORServicePath + "PutMetricData",
			want: "PutMetricData",
		},
		{
			name: "path without prefix is unchanged",
			path: "PutMetricData",
			want: "PutMetricData",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := service.ExtractRPCv2CBOROperation(tc.path, testRPCv2CBORServicePath); got != tc.want {
				t.Errorf("ExtractRPCv2CBOROperation(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestWriteRPCv2CBORResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body cbor.Map
		name string
	}{
		{name: "empty map", body: cbor.Map{}},
		{name: "map with a string field", body: cbor.Map{"Name": cbor.String("value")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			if err := service.WriteRPCv2CBORResponse(c, tc.body); err != nil {
				t.Fatalf("WriteRPCv2CBORResponse: %v", err)
			}

			if got := rec.Code; got != http.StatusOK {
				t.Errorf("status = %d, want %d", got, http.StatusOK)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/cbor" {
				t.Errorf("Content-Type = %q, want application/cbor", got)
			}
			if got := rec.Header().Get("Smithy-Protocol"); got != "rpc-v2-cbor" {
				t.Errorf("Smithy-Protocol = %q, want rpc-v2-cbor", got)
			}

			decoded, err := cbor.Decode(rec.Body.Bytes())
			if err != nil {
				t.Fatalf("cbor.Decode: %v", err)
			}
			m, ok := decoded.(cbor.Map)
			if !ok {
				t.Fatalf("expected cbor.Map, got %T", decoded)
			}
			if len(m) != len(tc.body) {
				t.Errorf("decoded body has %d keys, want %d", len(m), len(tc.body))
			}
		})
	}
}

// TestWriteRPCv2CBORError is the regression test for gopherstack-7fyf: the
// generated rpc-v2-cbor SDK deserializer (getProtocolErrorInfo) resolves the
// exception name from an "__type" key in the decoded CBOR body, not from the
// X-Amzn-Errortype header -- the header alone (CloudWatch's behavior before
// this fix) leaves errors.As/errors.Is against typed exceptions unable to
// match on the client, even though the header and status code look correct.
func TestWriteRPCv2CBORError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    string
		message string
		status  int
	}{
		{
			name:    "ResourceNotFoundException",
			status:  http.StatusBadRequest,
			code:    "ResourceNotFoundException",
			message: "no such alarm",
		},
		{
			name:    "InternalFailure",
			status:  http.StatusInternalServerError,
			code:    "InternalFailure",
			message: "boom",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			if err := service.WriteRPCv2CBORError(c, tc.status, tc.code, tc.message); err != nil {
				t.Fatalf("WriteRPCv2CBORError: %v", err)
			}

			if got := rec.Code; got != tc.status {
				t.Errorf("status = %d, want %d", got, tc.status)
			}
			if got := rec.Header().Get("X-Amzn-Errortype"); got != tc.code {
				t.Errorf("X-Amzn-Errortype header = %q, want %q", got, tc.code)
			}

			decoded, err := cbor.Decode(rec.Body.Bytes())
			if err != nil {
				t.Fatalf("cbor.Decode: %v", err)
			}
			m, ok := decoded.(cbor.Map)
			if !ok {
				t.Fatalf("expected cbor.Map, got %T", decoded)
			}

			typeVal, ok := m["__type"].(cbor.String)
			if !ok {
				t.Fatalf("CBOR error body missing __type key (body: %+v)", m)
			}
			if string(typeVal) != tc.code {
				t.Errorf("__type = %q, want %q", string(typeVal), tc.code)
			}

			msgVal, ok := m["message"].(cbor.String)
			if !ok {
				t.Fatalf("CBOR error body missing message key (body: %+v)", m)
			}
			if string(msgVal) != tc.message {
				t.Errorf("message = %q, want %q", string(msgVal), tc.message)
			}
		})
	}
}
