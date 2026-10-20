package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUtilExtra_VTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name:     "to_s3_object_json",
			template: `$util.dynamodb.toS3ObjectJson("k", "b", "r")`,
			want:     `{"S":"{\"s3\":{\"key\":\"k\",\"bucket\":\"b\",\"region\":\"r\"}}"}`,
		},
		{
			name:     "to_s3_object_version",
			template: `$util.toJson($util.dynamodb.toS3Object("k", "b", "r", "v1"))`,
			want:     `{"S":"{\"s3\":{\"key\":\"k\",\"bucket\":\"b\",\"region\":\"r\",\"version\":\"v1\"}}"}`,
		},
		{
			name: "from_s3_object_json",
			template: `#set($o = $util.dynamodb.fromS3ObjectJson('{"s3":{"key":"k","bucket":"b","region":"r"}}'))` +
				`$o.bucket/$o.key/$o.region`,
			want: `b/k/r`,
		},
		{
			name:     "normalize_nfkc",
			template: `$util.str.normalize("ﬁ", "nfkc")`,
			want:     "fi",
		},
		{
			name:     "xml_to_json_string",
			template: `$util.xml.toJsonString("<a><b>1</b><c>x</c><c>y</c></a>")`,
			want:     `{"a":{"b":"1","c":["x","y"]}}`,
		},
		{
			name:     "xml_to_map",
			template: `#set($m = $util.xml.toMap("<p><n>7</n></p>"))$m.p.n`,
			want:     "7",
		},
		{
			name:     "copy_headers_drops_disallowed",
			template: `$util.toJson($util.http.copyHeaders({"x-a": "1", "Host": "h", "content-length": "3"}))`,
			want:     `{"x-a":"1"}`,
		},
		{
			name:     "subscription_filter_and",
			template: `$util.toJson($util.transform.toSubscriptionFilter({"severity": {"le": 3}, "id": {"eq": "1"}}))`,
			want: `{"filterGroup":[{"filters":[{"fieldName":"id","operator":"eq","value":"1"},` +
				`{"fieldName":"severity","operator":"le","value":3}]}]}`,
		},
		{
			name:     "subscription_filter_or",
			template: `$util.toJson($util.transform.toSubscriptionFilter({"or": [{"a": {"eq": 1}}, {"b": {"ne": 2}}]}))`,
			want: `{"filterGroup":[{"filters":[{"fieldName":"a","operator":"eq","value":1}]},` +
				`{"filters":[{"fieldName":"b","operator":"ne","value":2}]}]}`,
		},
		{
			name:     "subscription_filter_ignored_field",
			template: `$util.toJson($util.transform.toSubscriptionFilter({"a": {"eq": 1}, "skip": {"eq": 2}}, ["skip"]))`,
			want:     `{"filterGroup":[{"filters":[{"fieldName":"a","operator":"eq","value":1}]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := newTestBackend().EvaluateMappingTemplate(tt.template, "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, out)
		})
	}
}

func TestUtilExtra_VTLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
	}{
		{"s3_object_missing_args", `$util.dynamodb.toS3Object("k")`},
		{"normalize_bad_form", `$util.str.normalize("a", "xx")`},
		{"xml_invalid", `$util.xml.toMap("<a>")`},
		{"filter_bad_operator", `$util.transform.toSubscriptionFilter({"a": {"zz": 1}})`},
		{"from_s3_not_object", `$util.dynamodb.fromS3ObjectJson("{}")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newTestBackend().EvaluateMappingTemplate(tt.template, "")
			require.Error(t, err)
		})
	}
}

func TestUtilExtra_JS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"s3_object",
			`util.dynamodb.toS3Object('k', 'b', 'r')`,
			`{"S":"{\"s3\":{\"key\":\"k\",\"bucket\":\"b\",\"region\":\"r\"}}"}`,
		},
		{"normalize", `util.str.normalize('ﬁ', 'nfkc')`, `"fi"`},
		{"xml", `util.xml.toMap('<a><b>1</b></a>')`, `{"a":{"b":"1"}}`},
		{"xml_json", `util.xml.toJsonString('<a><b>1</b></a>')`, `"{\"a\":{\"b\":\"1\"}}"`},
		{"copy_headers", `util.http.copyHeaders({'x-a': '1', Authorization: 'z'})`, `{"x-a":"1"}`},
		{
			"subscription_filter",
			`util.transform.toSubscriptionFilter({a: {in: [1, 2]}})`,
			`{"filterGroup":[{"filters":[{"fieldName":"a","operator":"in","value":[1,2]}]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code := jsUtilsImport + `export function request(ctx) { return ` + tt.body + `; }`
			out, err := newTestBackend().EvaluateCode(code, "", "request", "APPSYNC_JS")
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, out)
		})
	}
}

func TestEvaluateCode_ConsoleLogsAndSandbox(t *testing.T) {
	t.Parallel()

	t.Run("logs_captured", func(t *testing.T) {
		t.Parallel()

		code := `export function request(ctx) { console.log('a', 1); console.error({k: 'v'}); return ctx.args; }`
		_, logs, err := newTestBackend().EvaluateCodeLogs(code, `{"arguments":{"x":1}}`, "request", "APPSYNC_JS")
		require.NoError(t, err)
		assert.Equal(t, []string{"a 1", `{"k":"v"}`}, logs)
	})

	escapes := []struct{ name, body, wantErr string }{
		{"require", `require('fs')`, "require is not defined"},
		{"process", `process.env`, "process is not defined"},
		{"loop", `(function () { while (true) {} })()`, "timed out"},
	}

	for _, tt := range escapes {
		t.Run("escape_"+tt.name, func(t *testing.T) {
			t.Parallel()

			code := `export function request(ctx) { return ` + tt.body + `; }`
			_, err := newTestBackend().EvaluateCode(code, "", "request", "APPSYNC_JS")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	t.Run("import_unknown_module", func(t *testing.T) {
		t.Parallel()

		code := `import fs from 'fs'; export function request() { return 1; }`
		_, err := newTestBackend().EvaluateCode(code, "", "request", "APPSYNC_JS")
		require.Error(t, err)
	})
}
