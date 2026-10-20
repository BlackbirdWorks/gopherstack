package appsync_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const runtimeSchema = `
type Todo { id: ID! title: String stamp: String }
type Query {
  getTodo(id: ID!): Todo
  external(name: String): Int
  viaLambda(name: String): String
  calc(n: Int): Int
  cached: String
  warned: String
  beforeShort: String
  headered: String
}
type Mutation { createTodo(id: ID!, title: String): Todo }
`

const (
	jsImportUtil = `import { util, runtime } from '@aws-appsync/utils';
`
	pipelineBeforeJS = jsImportUtil + `
export function request(ctx) {
  ctx.stash.stamp = util.time.epochMilliSecondsToISO8601(1700000000000);
  ctx.stash.id = ctx.args.id;
  return { seed: 1 };
}
export function response(ctx) { return ctx.prev.result; }`
	validateFnJS = jsImportUtil + `
export function request(ctx) {
  if (!ctx.args.title) { util.error('title required', 'ValidationError', { field: 'title' }); }
  return { payload: ctx.args.title.trim() + ':' + ctx.prev.result.seed };
}
export function response(ctx) { ctx.stash.title = ctx.result; return ctx.result; }`
	putFnVTL = `{"operation":"PutItem","key":{"id":$util.dynamodb.toDynamoDBJson($ctx.stash.id)},` +
		`"attributeValues":{"title":$util.dynamodb.toDynamoDBJson($ctx.stash.title),` +
		`"stamp":$util.dynamodb.toDynamoDBJson($ctx.stash.stamp)}}`
)

type runtimeEnv struct {
	handler *appsync.Handler
	api     *appsync.GraphqlAPI
	lambda  *mockLambdaInvoker
	ddb     *idKeyedDDB
	http    *httpSeen
	key     string
}

type httpSeen struct {
	method string
	path   string
	query  string
	header string
	mu     sync.Mutex
}

func newRuntimeEnv(t *testing.T) *runtimeEnv {
	t.Helper()

	seen := &httpSeen{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.mu.Lock()
		seen.method, seen.path, seen.query, seen.header = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Test")
		seen.mu.Unlock()

		_, _ = w.Write([]byte(`{"v":42}`))
	}))
	t.Cleanup(srv.Close)

	h, b := newTestHandler()
	env := &runtimeEnv{
		handler: h, lambda: &mockLambdaInvoker{payload: []byte(`{"greeting":"hi bob"}`)},
		ddb: &idKeyedDDB{}, http: seen,
	}
	b.SetLambdaInvoker(env.lambda)
	b.SetDynamoDBBackend(env.ddb)

	api, err := b.CreateGraphqlAPI("rt", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "", 0)
	require.NoError(t, err)

	env.api, env.key = api, key.ID

	_, err = b.StartSchemaCreation(api.APIID, runtimeSchema)
	require.NoError(t, err)

	for _, ds := range []*appsync.DataSource{
		{Name: "None", Type: appsync.DataSourceTypeNone},
		{
			Name: "Ddb", Type: appsync.DataSourceTypeDynamoDB,
			DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "todos"},
		},
		{
			Name: "Lam", Type: appsync.DataSourceTypeLambda,
			LambdaConfig: &appsync.LambdaDataSourceConfig{
				LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
			},
		},
		{Name: "Web", Type: appsync.DataSourceTypeHTTP, HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: srv.URL}},
	} {
		_, err = b.CreateDataSource(api.APIID, ds)
		require.NoError(t, err)
	}

	env.addResolvers(t, b)

	return env
}

func (e *runtimeEnv) addResolvers(t *testing.T, b *appsync.InMemoryBackend) {
	t.Helper()

	mkFn := func(f *appsync.Function) string {
		fn, err := b.CreateFunction(e.api.APIID, f)
		require.NoError(t, err)

		return fn.FunctionID
	}

	js := &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"}
	validate := mkFn(&appsync.Function{Name: "validate", DataSourceName: "None", Runtime: js, Code: validateFnJS})
	put := mkFn(&appsync.Function{
		Name: "put", DataSourceName: "Ddb", RequestMappingTemplate: putFnVTL,
		ResponseMappingTemplate: `$util.toJson($ctx.result)`,
	})

	for typ, rs := range map[string][]*appsync.Resolver{
		"Query": {
			{
				FieldName: "getTodo", DataSourceName: "Ddb",
				RequestMappingTemplate:  `{"operation":"GetItem","key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)}}`,
				ResponseMappingTemplate: `$util.toJson($ctx.result)`,
			},
			{
				FieldName: "external", DataSourceName: "Web",
				RequestMappingTemplate: `{"version":"2018-05-29","method":"POST","resourcePath":"/thing",` +
					`"params":{"query":{"q":"$ctx.args.name"},"headers":{"X-Test":"yes"},"body":{"a":1}}}`,
				ResponseMappingTemplate: "#if($ctx.result.statusCode == 200)\n$util.parseJson($ctx.result.body).v\n" +
					"#else\n$util.error(\"http failed\")\n#end",
			},
			{
				FieldName: "viaLambda", DataSourceName: "Lam", Runtime: js,
				Code: jsImportUtil + `export function request(ctx) {
  return { operation: 'Invoke', payload: { name: ctx.args.name } };
}
export function response(ctx) { return ctx.result.greeting; }`,
			},
			{
				FieldName: "calc", DataSourceName: "None",
				RequestMappingTemplate:  `{"payload":{"v":$ctx.args.n}}`,
				ResponseMappingTemplate: `#set($r = $ctx.result.v * 2)$r`,
			},
			{
				FieldName: "cached", DataSourceName: "None", Runtime: js,
				Code: jsImportUtil + `export function request(ctx) { runtime.earlyReturn('from-cache'); }
export function response(ctx) { util.error('response must not run'); }`,
			},
			{
				FieldName: "headered", DataSourceName: "None",
				RequestMappingTemplate: `$util.http.addResponseHeader("X-Custom", "v1")` +
					`$util.http.addResponseHeaders({"X-Other": "v2"}){"payload":"ok"}`,
				ResponseMappingTemplate: `$util.toJson($ctx.result)`,
			},
			{
				FieldName: "warned", DataSourceName: "None",
				RequestMappingTemplate:  `{"payload":"ok"}`,
				ResponseMappingTemplate: `$util.appendError("heads up", "Warning")$util.toJson($ctx.result)`,
			},
			{
				FieldName: "beforeShort", Kind: "PIPELINE", PipelineConfig: []string{validate},
				RequestMappingTemplate:  `#return("short-circuit")`,
				ResponseMappingTemplate: `$util.toJson("after:$ctx.prev.result")`,
			},
		},
		"Mutation": {{
			FieldName: "createTodo", Kind: "PIPELINE", Runtime: js, PipelineConfig: []string{validate, put},
			Code: pipelineBeforeJS,
		}},
	} {
		for _, r := range rs {
			_, err := b.CreateResolver(e.api.APIID, typ, r)
			require.NoError(t, err)
		}
	}
}

type gqlReply struct {
	Data   map[string]any `json:"data"`
	Errors []struct {
		Data      any    `json:"data"`
		Message   string `json:"message"`
		ErrorType string `json:"errorType"`
		Path      []any  `json:"path"`
	} `json:"errors"`
}

func (e *runtimeEnv) exec(t *testing.T, query string) gqlReply {
	t.Helper()

	rec := doRequestWithHeaders(t, e.handler, "/v1/apis/"+e.api.APIID+"/graphql",
		map[string]any{"query": query}, map[string]string{"X-Api-Key": e.key})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out gqlReply
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())

	return out
}

func TestResolverRuntime_EndToEnd(t *testing.T) {
	t.Parallel()

	env := newRuntimeEnv(t)

	created := env.exec(t, `mutation { createTodo(id: "t1", title: "  write tests ") { id title stamp } }`)
	require.Empty(t, created.Errors)
	assert.Equal(t, map[string]any{
		"id": "t1", "title": "write tests:1", "stamp": "2023-11-14T22:13:20.000Z",
	}, created.Data["createTodo"], "stash must flow JS before -> JS fn -> VTL fn, prev.result seeds fn1")

	got := env.exec(t, `query { getTodo(id: "t1") { id title } }`)
	require.Empty(t, got.Errors)
	assert.Equal(t, map[string]any{"id": "t1", "title": "write tests:1"}, got.Data["getTodo"])
}

func TestResolverRuntime_DataSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  any
		check func(t *testing.T, env *runtimeEnv)
		name  string
		query string
		field string
	}{
		{
			name: "http", query: `query { external(name: "z") }`, field: "external", want: float64(42),
			check: func(t *testing.T, env *runtimeEnv) {
				t.Helper()
				env.http.mu.Lock()
				defer env.http.mu.Unlock()
				assert.Equal(t, "POST", env.http.method)
				assert.Equal(t, "/thing", env.http.path)
				assert.Equal(t, "q=z", env.http.query)
				assert.Equal(t, "yes", env.http.header)
			},
		},
		{
			name:  "lambda invoke payload",
			query: `query { viaLambda(name: "bob") }`,
			field: "viaLambda",
			want:  "hi bob",
			check: func(t *testing.T, env *runtimeEnv) {
				t.Helper()
				require.Len(t, env.lambda.calls, 1)
				assert.JSONEq(t, `{"name":"bob"}`, string(env.lambda.calls[0].payload))
			},
		},
		{name: "none vtl arithmetic", query: `query { calc(n: 21) }`, field: "calc", want: float64(42)},
		{name: "early return skips response", query: `query { cached }`, field: "cached", want: "from-cache"},
		{
			name: "pipeline before short circuit", query: `query { beforeShort }`, field: "beforeShort",
			want: "after:short-circuit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newRuntimeEnv(t)
			got := env.exec(t, tt.query)
			require.Empty(t, got.Errors)
			assert.Equal(t, tt.want, got.Data[tt.field])

			if tt.check != nil {
				tt.check(t, env)
			}
		})
	}
}

func TestResolverRuntime_Errors(t *testing.T) {
	t.Parallel()

	env := newRuntimeEnv(t)

	failed := env.exec(t, `mutation { createTodo(id: "t2", title: "") { id } }`)
	require.Len(t, failed.Errors, 1)
	assert.Nil(t, failed.Data["createTodo"])
	assert.Equal(t, "title required", failed.Errors[0].Message)
	assert.Equal(t, "ValidationError", failed.Errors[0].ErrorType)
	assert.Equal(t, map[string]any{"field": "title"}, failed.Errors[0].Data)
	assert.Equal(t, []any{"createTodo"}, failed.Errors[0].Path)

	warned := env.exec(t, `query { warned }`)
	assert.Equal(t, "ok", warned.Data["warned"], "appendError keeps the field value")
	require.Len(t, warned.Errors, 1)
	assert.Equal(t, "Warning", warned.Errors[0].ErrorType)
}

func TestResolverRuntime_ResponseHeaders(t *testing.T) {
	t.Parallel()

	env := newRuntimeEnv(t)
	rec := doRequestWithHeaders(t, env.handler, "/v1/apis/"+env.api.APIID+"/graphql",
		map[string]any{"query": `query { headered }`}, map[string]string{"X-Api-Key": env.key})

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "v1", rec.Header().Get("X-Custom"))
	assert.Equal(t, "v2", rec.Header().Get("X-Other"))
	assert.Contains(t, rec.Body.String(), `"headered":"ok"`)
}
