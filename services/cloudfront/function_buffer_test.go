package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFunction(t *testing.T, mult int, runtime, code, event string) *types.TestResult {
	t.Helper()

	be, client := newRealClientBackendAndClient(t)
	be.SetFunctionComputeMultiplier(mult)

	etag := createFn(t, client, "f", runtime, code)

	out, err := client.TestFunction(t.Context(), &cfsdk.TestFunctionInput{
		Name: aws.String("f"), IfMatch: aws.String(etag), Stage: types.FunctionStageDevelopment,
		EventObject: []byte(event),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TestResult)

	return out.TestResult
}

func TestTestFunction_BufferModule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		runtime string
		expr    string
		want    string
		wantErr string
	}{
		{name: "utf8_to_hex", expr: `Buffer.from('héllo').toString('hex')`, want: "68c3a96c6c6f"},
		{name: "hex_to_utf8", expr: `Buffer.from('68656c6c6f', 'hex').toString('utf8')`, want: "hello"},
		{name: "base64_roundtrip", expr: `Buffer.from('hello?>', 'utf8').toString('base64')`, want: "aGVsbG8/Pg=="},
		{name: "base64_decode", expr: `Buffer.from('aGVsbG8/Pg==', 'base64').toString()`, want: "hello?>"},
		{name: "base64url_encode", expr: `Buffer.from('hello?>').toString('base64url')`, want: "aGVsbG8_Pg"},
		{name: "base64url_decode", expr: `Buffer.from('aGVsbG8_Pg', 'base64url').toString()`, want: "hello?>"},
		{name: "from_array", expr: `Buffer.from([104, 105]).toString()`, want: "hi"},
		{name: "alloc_zero", expr: `Buffer.alloc(3).toString('hex')`, want: "000000"},
		{name: "alloc_fill_number", expr: `Buffer.alloc(3, 1).toString('hex')`, want: "010101"},
		{name: "alloc_fill_string", expr: `Buffer.alloc(5, 'ab').toString()`, want: "ababa"},
		{name: "byte_length", expr: `String(Buffer.byteLength('héllo'))`, want: "6"},
		{name: "concat", expr: `Buffer.concat([Buffer.from('ab'), Buffer.from('cd')]).toString()`, want: "abcd"},
		{name: "compare_less", expr: `String(Buffer.compare(Buffer.from('a'), Buffer.from('b')))`, want: "-1"},
		{name: "compare_equal", expr: `String(Buffer.from('a').compare(Buffer.from('a')))`, want: "0"},
		{name: "equals", expr: `String(Buffer.from('a').equals(Buffer.from('a')))`, want: "true"},
		{name: "not_equals", expr: `String(Buffer.from('a').equals(Buffer.from('b')))`, want: "false"},
		{name: "slice", expr: `Buffer.from('abcdef').slice(1, 4).toString()`, want: "bcd"},
		{name: "slice_negative", expr: `Buffer.from('abcdef').slice(-2).toString()`, want: "ef"},
		{name: "subarray", expr: `Buffer.from('abcdef').subarray(2).toString()`, want: "cdef"},
		{name: "to_string_range", expr: `Buffer.from('abcdef').toString('utf8', 1, 3)`, want: "bc"},
		{name: "is_buffer", expr: `String(Buffer.isBuffer(Buffer.from('a')) && !Buffer.isBuffer('a'))`, want: "true"},
		{name: "bad_encoding", expr: `Buffer.from('a').toString('ucs2')`, wantErr: "Unknown encoding"},
		{name: "bad_from", expr: `Buffer.from(1)`, wantErr: "first argument"},
		{name: "not_in_js1", runtime: "cloudfront-js-1.0", expr: `typeof Buffer`, want: "undefined"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := tt.runtime
			if rt == "" {
				rt = "cloudfront-js-2.0"
			}

			code := `function handler(e) { e.request.uri = ` + tt.expr + `; return e.request; }`
			res := testFunction(t, 0, rt, code, reqEvent)

			if tt.wantErr != "" {
				assert.Contains(t, aws.ToString(res.FunctionErrorMessage), tt.wantErr)

				return
			}

			assert.Empty(t, aws.ToString(res.FunctionErrorMessage))
			assert.Contains(t, aws.ToString(res.FunctionOutput), `"uri":"`+tt.want+`"`)
		})
	}
}

const bufHandler = ` function handler(e) { e.request.uri = b.Buffer.from('a').toString('hex'); return e.request; }`

func TestTestFunction_BufferRequire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
	}{
		{"require", `const b = require('buffer');` + bufHandler},
		{"import_default", `import b from 'buffer';` + bufHandler},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := testFunction(t, 0, "cloudfront-js-2.0", tt.code, reqEvent)
			assert.Empty(t, aws.ToString(res.FunctionErrorMessage))
			assert.Contains(t, aws.ToString(res.FunctionOutput), `"uri":"61"`)
		})
	}
}

func TestTestFunction_ComputeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     string
		wantErr  string
		wantUtil string
		mult     int
	}{
		{
			name: "strict_limit_kills_spinning_function", mult: 1,
			code:    `function handler(e) { while (true) {} }`,
			wantErr: "maximum allowed compute time", wantUtil: "100",
		},
		{
			name: "lenient_default_lets_short_loop_finish", mult: 0,
			code: `function handler(e) { var n = 0; for (var i = 0; i < 1000; i++) { n += i; } return e.request; }`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := testFunction(t, tt.mult, "cloudfront-js-2.0", tt.code, reqEvent)

			if tt.wantErr == "" {
				assert.Empty(t, aws.ToString(res.FunctionErrorMessage))

				return
			}

			assert.Contains(t, aws.ToString(res.FunctionErrorMessage), tt.wantErr)
			assert.Equal(t, tt.wantUtil, aws.ToString(res.ComputeUtilization))
		})
	}
}

func TestTestFunction_KVSMeta(t *testing.T) {
	t.Parallel()

	be, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	kvs, err := client.CreateKeyValueStore(ctx, &cfsdk.CreateKeyValueStoreInput{Name: aws.String("store")})
	require.NoError(t, err)

	id := aws.ToString(kvs.KeyValueStore.Id)

	for _, k := range []string{"a", "b", "c"} {
		_, err = be.PutKVSValue(id, k, "v", "")
		require.NoError(t, err)
	}

	code := `import cf from 'cloudfront';
async function handler(e) {
  const m = await cf.kvs().meta();
  e.request.uri = [m.keyCount, typeof m.creationDateTime, typeof m.lastUpdatedDateTime,
    Date.parse(m.lastUpdatedDateTime) >= Date.parse(m.creationDateTime)].join('|');
  return e.request;
}`
	etag := createFn(t, client, "mf", "cloudfront-js-2.0", code, aws.ToString(kvs.KeyValueStore.ARN))

	out, err := client.TestFunction(ctx, &cfsdk.TestFunctionInput{
		Name: aws.String("mf"), IfMatch: aws.String(etag), Stage: types.FunctionStageDevelopment,
		EventObject: []byte(reqEvent),
	})
	require.NoError(t, err)

	assert.Empty(t, aws.ToString(out.TestResult.FunctionErrorMessage))
	assert.Contains(t, aws.ToString(out.TestResult.FunctionOutput), `"uri":"3|string|string|true"`)
}
