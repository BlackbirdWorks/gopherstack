package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const reqEvent = `{"version":"1.0","context":{"eventType":"viewer-request"},` +
	`"request":{"method":"GET","uri":"/a","querystring":{"x":{"value":"1"}},` +
	`"headers":{"host":{"value":"d.example.com"}},"cookies":{}}}`

const respEvent = `{"version":"1.0","context":{"eventType":"viewer-response"},` +
	`"request":{"method":"GET","uri":"/a"},` +
	`"response":{"statusCode":200,"statusDescription":"OK","headers":{},"cookies":{}}}`

func createFn(t *testing.T, client *cfsdk.Client, name, runtime, code string, kvs ...string) string {
	t.Helper()

	cfg := &types.FunctionConfig{Comment: aws.String("c"), Runtime: types.FunctionRuntime(runtime)}
	if len(kvs) > 0 {
		cfg.KeyValueStoreAssociations = &types.KeyValueStoreAssociations{
			Quantity: aws.Int32(int32(len(kvs))),
		}

		for _, k := range kvs {
			cfg.KeyValueStoreAssociations.Items = append(cfg.KeyValueStoreAssociations.Items,
				types.KeyValueStoreAssociation{KeyValueStoreARN: aws.String(k)})
		}
	}

	out, err := client.CreateFunction(t.Context(), &cfsdk.CreateFunctionInput{
		Name: aws.String(name), FunctionConfig: cfg, FunctionCode: []byte(code),
	})
	require.NoError(t, err)

	return aws.ToString(out.ETag)
}

func TestTestFunction_Execution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		runtime    string
		code       string
		event      string
		wantOutput string
		wantErr    string
		wantLogs   []string
	}{
		{
			name: "js1_rewrite_uri", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:       `function handler(event) { var r = event.request; r.uri = '/b' + r.uri; return r; }`,
			wantOutput: `"uri":"/b/a"`,
		},
		{
			name:    "js2_arrow_and_const",
			runtime: "cloudfront-js-2.0",
			event:   reqEvent,
			code: `const h = (e) => { const r = e.request; r.headers['x-n'] = {value: 'v'}; return r; };
function handler(e) { return h(e); }`,
			wantOutput: `"x-n":{"value":"v"}`,
		},
		{
			name:       "viewer_response_header",
			runtime:    "cloudfront-js-1.0",
			event:      respEvent,
			code:       `function handler(event) { var r = event.response; r.headers['x-frame'] = {value: 'DENY'}; return r; }`,
			wantOutput: `"x-frame":{"value":"DENY"}`,
		},
		{
			name: "generated_response", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:       `function handler(event) { return {statusCode: 403, statusDescription: 'Forbidden'}; }`,
			wantOutput: `"statusCode":403`,
		},
		{
			name: "console_logs", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:     `function handler(event) { console.log('hi', 1); console.log({a: 1}); return event.request; }`,
			wantLogs: []string{"hi 1", `{"a":1}`}, wantOutput: `"uri":"/a"`,
		},
		{
			name: "crypto_sha256", runtime: "cloudfront-js-1.0", event: reqEvent,
			code: `var crypto = require('crypto');
function handler(event) {
  event.request.uri = crypto.createHash('sha256').update('a').digest('hex');
  return event.request;
}`,
			wantOutput: `ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb`,
		},
		{
			name: "querystring_module", runtime: "cloudfront-js-1.0", event: reqEvent,
			code: `var qs = require('querystring');
function handler(event) { event.request.uri = qs.stringify(qs.parse('a=1&b=2')); return event.request; }`,
			wantOutput: `a=1&b=2`,
		},
		{
			name: "async_handler_js2", runtime: "cloudfront-js-2.0", event: reqEvent,
			code:       `async function handler(event) { return event.request; }`,
			wantOutput: `"uri":"/a"`,
		},
		{
			name: "cloudfront_module_not_in_js1", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `var cf = require('cloudfront'); function handler(e) { return e.request; }`,
			wantErr: "Cannot find module",
		},
		{
			name: "throws", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `function handler(event) { throw new Error('boom'); }`,
			wantErr: "boom",
		},
		{
			name: "no_return", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `function handler(event) { }`,
			wantErr: "invalid value",
		},
		{
			name: "no_handler", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `var x = 1;`,
			wantErr: "handler is not defined",
		},
		{
			name: "escape_require_fs", runtime: "cloudfront-js-2.0", event: reqEvent,
			code:    `function handler(e) { require('fs'); return e.request; }`,
			wantErr: "Cannot find module 'fs'",
		},
		{
			name: "escape_process", runtime: "cloudfront-js-2.0", event: reqEvent,
			code:    `function handler(e) { return {x: process.env}; }`,
			wantErr: "process is not defined",
		},
		{
			name: "escape_import_fs", runtime: "cloudfront-js-2.0", event: reqEvent,
			code:    `import fs from 'fs'; function handler(e) { return e.request; }`,
			wantErr: "unsupported module",
		},
		{
			name: "infinite_loop_times_out", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `function handler(e) { while (true) {} }`,
			wantErr: "maximum allowed compute time",
		},
		{
			name: "deep_recursion_bounded", runtime: "cloudfront-js-1.0", event: reqEvent,
			code:    `function f(n) { return f(n + 1); } function handler(e) { f(0); return e.request; }`,
			wantErr: "Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			etag := createFn(t, client, "f", tt.runtime, tt.code)

			out, err := client.TestFunction(t.Context(), &cfsdk.TestFunctionInput{
				Name: aws.String("f"), IfMatch: aws.String(etag), Stage: types.FunctionStageDevelopment,
				EventObject: []byte(tt.event),
			})
			require.NoError(t, err)

			res := out.TestResult
			require.NotNil(t, res)
			assert.NotEmpty(t, aws.ToString(res.ComputeUtilization))
			assert.Equal(t, "f", aws.ToString(res.FunctionSummary.Name))

			if tt.wantErr != "" {
				assert.Contains(t, aws.ToString(res.FunctionErrorMessage), tt.wantErr)
				assert.Empty(t, aws.ToString(res.FunctionOutput))

				return
			}

			assert.Empty(t, aws.ToString(res.FunctionErrorMessage))
			assert.Contains(t, aws.ToString(res.FunctionOutput), tt.wantOutput)

			if tt.wantLogs != nil {
				assert.Equal(t, tt.wantLogs, res.FunctionExecutionLogs)
			}
		})
	}
}

func TestTestFunction_KeyValueStore(t *testing.T) {
	t.Parallel()

	be := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(be.Close)

	client := newTestCloudFrontClient(t, cloudfront.NewHandler(be))
	ctx := t.Context()

	kvs, err := client.CreateKeyValueStore(ctx, &cfsdk.CreateKeyValueStoreInput{Name: aws.String("store")})
	require.NoError(t, err)

	kvsARN := aws.ToString(kvs.KeyValueStore.ARN)
	kvsID := aws.ToString(kvs.KeyValueStore.Id)

	_, err = be.PutKVSValue(kvsID, "color", `{"c":"red"}`, "")
	require.NoError(t, err)

	code := `import cf from 'cloudfront';
const kv = cf.kvs();
async function handler(event) {
  const req = event.request;
  req.uri = '/' + await kv.get('color');
  const j = await kv.get('color', {format: 'json'});
  req.headers['x-c'] = {value: j.c};
  req.headers['x-e'] = {value: String(await kv.exists('nope'))};
  try { await kv.get('nope'); } catch (e) { req.headers['x-m'] = {value: 'missing'}; }
  return req;
}`
	etag := createFn(t, client, "kvf", "cloudfront-js-2.0", code, kvsARN)

	out, err := client.TestFunction(ctx, &cfsdk.TestFunctionInput{
		Name: aws.String("kvf"), IfMatch: aws.String(etag), Stage: types.FunctionStageDevelopment,
		EventObject: []byte(reqEvent),
	})
	require.NoError(t, err)

	res := out.TestResult
	assert.Empty(t, aws.ToString(res.FunctionErrorMessage))
	assert.Contains(t, aws.ToString(res.FunctionOutput), `"x-c":{"value":"red"}`)
	assert.Contains(t, aws.ToString(res.FunctionOutput), `"x-e":{"value":"false"}`)
	assert.Contains(t, aws.ToString(res.FunctionOutput), `"x-m":{"value":"missing"}`)

	desc, err := client.DescribeFunction(ctx, &cfsdk.DescribeFunctionInput{Name: aws.String("kvf")})
	require.NoError(t, err)
	require.NotNil(t, desc.FunctionSummary.FunctionConfig.KeyValueStoreAssociations)
	assert.Equal(t, kvsARN, aws.ToString(
		desc.FunctionSummary.FunctionConfig.KeyValueStoreAssociations.Items[0].KeyValueStoreARN))
}

func TestTestFunction_Stage(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	etag := createFn(t, client, "sf", "cloudfront-js-1.0", `function handler(e) { return e.request; }`)

	_, err := client.TestFunction(t.Context(), &cfsdk.TestFunctionInput{
		Name: aws.String("sf"), IfMatch: aws.String(etag), Stage: types.FunctionStageLive,
		EventObject: []byte(reqEvent),
	})
	require.Error(t, err, "unpublished function has no LIVE stage")
}
