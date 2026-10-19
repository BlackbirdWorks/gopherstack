package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const jsUtilsImport = `import { util, runtime } from '@aws-appsync/utils'; `

func TestEvaluateCode_Engine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     string
		ctx      string
		function string
		want     string
	}{
		{
			name: "control flow and bindings",
			code: `export function request(ctx) {
  const out = [];
  for (let i = 0; i < ctx.args.n; i++) { if (i % 2 === 0) { out.push(i * 2); } }
  const sum = out.reduce((a, b) => a + b, 0);
  return { out, sum, label: ` + "`n=${ctx.args.n}`" + ` , ...{extra: true} };
}`,
			ctx:  `{"arguments":{"n":5}}`,
			want: `{"out":[0,4,8],"sum":12,"label":"n=5","extra":true}`,
		},
		{
			name: "string concat",
			code: `export function request(ctx) { return 'id-' + ctx.args.id + '!'; }`,
			ctx:  `{"arguments":{"id":"x"}}`,
			want: `"id-x!"`,
		},
		{
			name: "get helper",
			code: `import * as ddb from '@aws-appsync/utils/dynamodb';
export function request(ctx) { return ddb.get({ key: { id: ctx.args.id } }); }`,
			ctx:  `{"arguments":{"id":"1"}}`,
			want: `{"operation":"GetItem","key":{"id":{"S":"1"}}}`,
		},
		{
			name: "put helper with condition",
			code: `import { put } from '@aws-appsync/utils/dynamodb';
export function request(ctx) {
  return put({ key: { id: 'a' }, item: { id: 'a', n: 1 }, condition: { id: { attributeExists: false } } });
}`,
			want: `{"operation":"PutItem","key":{"id":{"S":"a"}},"attributeValues":{"id":{"S":"a"},"n":{"N":"1"}},` +
				`"condition":{"expression":"(attribute_not_exists(#id))","expressionNames":{"#id":"id"},` +
				`"expressionValues":{}}}`,
		},
		{
			name: "update helper operations",
			code: `import { update, operations as ops } from '@aws-appsync/utils/dynamodb';
export function request(ctx) {
  return update({ key: { id: 'a' }, update: { title: 'T', count: ops.increment(2), old: ops.remove() } });
}`,
			want: `{"operation":"UpdateItem","key":{"id":{"S":"a"}},"update":{"expression":` +
				`"SET #f0 = :v0, #f1 = #f1 + :v1 REMOVE #f2","expressionNames":{"#f0":"title","#f1":"count",` +
				`"#f2":"old"},"expressionValues":{":v0":{"S":"T"},":v1":{"N":"2"}}}}`,
		},
		{
			name: "query helper",
			code: `import { query } from '@aws-appsync/utils/dynamodb';
export function request(ctx) { return query({ query: { pk: { eq: 'p' }, sk: { beginsWith: 'x' } }, limit: 5 }); }`,
			want: `{"operation":"Query","query":{"expression":"((#pk = :pk_eq) AND (begins_with(#sk, :sk_beginsWith)))",` +
				`"expressionNames":{"#pk":"pk","#sk":"sk"},"expressionValues":{":pk_eq":{"S":"p"},` +
				`":sk_beginsWith":{"S":"x"}}},"limit":5}`,
		},
		{
			name: "util time and dynamodb",
			code: jsUtilsImport + `export function request(ctx) {
  return [util.time.epochMilliSecondsToISO8601(1700000000000), util.dynamodb.toDynamoDB(3)];
}`,
			want: `["2023-11-14T22:13:20.000Z",{"N":"3"}]`,
		},
		{
			name: "import alias",
			code: `import { util as u } from '@aws-appsync/utils';
export function request(ctx) { return u.str.toUpper('abc'); }`,
			want: `"ABC"`,
		},
		{
			name: "early return",
			code: jsUtilsImport + `export function request(ctx) { runtime.earlyReturn({ done: true }); return 1; }`,
			want: `{"done":true}`,
		},
		{
			name: "response handler",
			code: `export function response(ctx) { return ctx.result.items.map(i => i.n); }`,
			ctx:  `{"result":{"items":[{"n":1},{"n":2}]}}`, function: "response",
			want: `[1,2]`,
		},
		{
			name: "stash read",
			code: `export function request(ctx) { ctx.stash.x = 1; return ctx.stash; }`,
			want: `{"x":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := newTestBackend().EvaluateCode(tt.code, tt.ctx, tt.function, "APPSYNC_JS")
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, out)
		})
	}
}

func TestEvaluateCode_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		code    string
		wantMsg string
	}{
		{
			name:    "util error",
			code:    jsUtilsImport + `export function request(ctx) { util.error('nope', 'MyErr'); }`,
			wantMsg: "nope",
		},
		{name: "syntax error", code: `export function request(ctx) { return ; ;; }}`, wantMsg: "Unexpected"},
		{
			name:    "unsupported module",
			code:    `import fs from 'fs'; export function request() {}`,
			wantMsg: "unsupported module",
		},
		{
			name:    "namespace import without as",
			code:    `import * from '@aws-appsync/utils'; export function request() {}`,
			wantMsg: "unsupported import form",
		},
		{name: "default export", code: `export default function () {}`, wantMsg: "default exports"},
		{name: "missing handler", code: `export function response(ctx) { return 1; }`, wantMsg: "no request handler"},
		{
			name:    "infinite loop is interrupted",
			code:    `export function request(ctx) { while (true) {} }`,
			wantMsg: "timed out",
		},
		{name: "no host access", code: `export function request(ctx) { return require('fs'); }`, wantMsg: "require"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newTestBackend().EvaluateCode(tt.code, "", "", "APPSYNC_JS")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}
