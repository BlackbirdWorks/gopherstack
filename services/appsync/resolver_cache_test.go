package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const cacheSchema = `
type Query { hello(id: ID): String other(id: ID): String }
type Mutation { touch(id: ID): String }
`

type cacheFixture struct {
	h      *appsync.Handler
	b      *appsync.InMemoryBackend
	lambda *mockLambdaInvoker
	apiID  string
	key    string
}

func newCacheFixture(t *testing.T, behavior string, ttl int64, hello *appsync.CachingConfig) *cacheFixture {
	t.Helper()

	h, b := newTestHandler()
	f := &cacheFixture{h: h, b: b, lambda: &mockLambdaInvoker{payload: []byte(`"hi"`)}}
	b.SetLambdaInvoker(f.lambda)

	api, err := b.CreateGraphqlAPI("c", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "", 0)
	require.NoError(t, err)

	f.apiID, f.key = api.APIID, key.ID

	_, err = b.StartSchemaCreation(api.APIID, cacheSchema)
	require.NoError(t, err)

	for _, ds := range []*appsync.DataSource{
		{Name: "None", Type: appsync.DataSourceTypeNone},
		{
			Name: "Lam", Type: appsync.DataSourceTypeLambda,
			LambdaConfig: &appsync.LambdaDataSourceConfig{
				LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
			},
		},
	} {
		_, err = b.CreateDataSource(api.APIID, ds)
		require.NoError(t, err)
	}

	lam := func(field string, cc *appsync.CachingConfig) *appsync.Resolver {
		return &appsync.Resolver{
			FieldName: field, DataSourceName: "Lam", CachingConfig: cc,
			RequestMappingTemplate:  `{"operation":"Invoke","payload":{"id":"$ctx.args.id"}}`,
			ResponseMappingTemplate: `$util.toJson($ctx.result)`,
		}
	}

	for _, r := range []*appsync.Resolver{lam("hello", hello), lam("other", nil)} {
		_, err = b.CreateResolver(api.APIID, "Query", r)
		require.NoError(t, err)
	}

	_, err = b.CreateResolver(api.APIID, "Mutation", &appsync.Resolver{
		FieldName: "touch", DataSourceName: "None",
		RequestMappingTemplate: `{"payload":"ok"}`,
		ResponseMappingTemplate: `$extensions.evictFromApiCache("Query", "hello", ` +
			`{"ctx.arguments.id": $ctx.args.id})$util.toJson($ctx.result)`,
	})
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{Type: "SMALL", TTL: ttl, APICachingBehavior: behavior})
	require.NoError(t, err)

	return f
}

func (f *cacheFixture) query(t *testing.T, q string) string {
	t.Helper()

	rec := doRequestWithHeaders(t, f.h, "/v1/apis/"+f.apiID+"/graphql",
		map[string]any{"query": q}, map[string]string{"X-Api-Key": f.key})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out gqlReply
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Empty(t, out.Errors, rec.Body.String())

	raw, err := json.Marshal(out.Data)
	require.NoError(t, err)

	return string(raw)
}

// AppSync developer guide, "Caching and compression": FULL_REQUEST_CACHING
// caches query responses by arguments and identity for the cache TTL.
func TestResolverCache_FullRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		steps     []string
		flushAt   int
		wantCalls int
	}{
		{
			name:      "same request is served from cache",
			steps:     []string{`{ hello(id: "1") }`, `{ hello(id: "1") }`},
			wantCalls: 1,
		},
		{name: "different arguments miss", steps: []string{`{ hello(id: "1") }`, `{ hello(id: "2") }`}, wantCalls: 2},
		{
			name:      "every query field is cached",
			steps:     []string{`{ other(id: "1") }`, `{ other(id: "1") }`},
			wantCalls: 1,
		},
		{
			name: "flush drops entries", flushAt: 1,
			steps: []string{`{ hello(id: "1") }`, `{ hello(id: "1") }`}, wantCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newCacheFixture(t, "FULL_REQUEST_CACHING", 60, nil)

			for i, q := range tt.steps {
				if tt.flushAt != 0 && i == tt.flushAt {
					require.NoError(t, f.b.FlushAPICache(f.apiID))
				}

				assert.Contains(t, f.query(t, q), `"hi"`)
			}

			assert.Len(t, f.lambda.calls, tt.wantCalls)
		})
	}
}

func TestResolverCache_TTLExpiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newCacheFixture(t, "FULL_REQUEST_CACHING", 30, nil)

		f.query(t, `{ hello(id: "1") }`)
		time.Sleep(29 * time.Second)
		f.query(t, `{ hello(id: "1") }`)
		assert.Len(t, f.lambda.calls, 1)

		time.Sleep(2 * time.Second)
		f.query(t, `{ hello(id: "1") }`)
		assert.Len(t, f.lambda.calls, 2, "entry must expire after the cache TTL")
	})
}

// PER_RESOLVER_CACHING caches only resolvers with a cachingConfig, keyed by
// cachingKeys; $extensions.evictFromApiCache removes the matching entry.
func TestResolverCache_PerResolverAndEviction(t *testing.T) {
	t.Parallel()

	cc := &appsync.CachingConfig{TTL: 60, CachingKeys: []string{"$context.arguments.id"}}
	f := newCacheFixture(t, "PER_RESOLVER_CACHING", 1, cc)

	f.query(t, `{ hello(id: "1") }`)
	f.query(t, `{ hello(id: "1") }`)
	f.query(t, `{ other(id: "1") }`)
	f.query(t, `{ other(id: "1") }`)
	require.Len(t, f.lambda.calls, 3, "hello cached once, other (no cachingConfig) never cached")

	f.query(t, `mutation { touch(id: "2") }`)
	f.query(t, `{ hello(id: "1") }`)
	assert.Len(t, f.lambda.calls, 3, "evicting id 2 must not touch id 1")

	f.query(t, `mutation { touch(id: "1") }`)
	f.query(t, `{ hello(id: "1") }`)
	assert.Len(t, f.lambda.calls, 4, "evicting id 1 forces a re-resolve")
}

func TestResolverCache_NoCacheNoCaching(t *testing.T) {
	t.Parallel()

	f := newCacheFixture(t, "FULL_REQUEST_CACHING", 60, nil)
	require.NoError(t, f.b.DeleteAPICache(f.apiID))

	f.query(t, `{ hello(id: "1") }`)
	f.query(t, `{ hello(id: "1") }`)
	assert.Len(t, f.lambda.calls, 2)
}
