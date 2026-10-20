package appsync_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// dsFixture is an API with one data source and one Query.run resolver.
func dsFixture(
	t *testing.T, ds *appsync.DataSource, res *appsync.Resolver,
) (*appsync.InMemoryBackend, func() map[string]any) {
	t.Helper()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("DS", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	key, err := b.CreateAPIKey(api.APIID, "", 0)
	require.NoError(t, err)
	_, err = b.StartSchemaCreation(api.APIID, `type Query { run: AWSJSON }`)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	res.FieldName, res.DataSourceName = "run", ds.Name
	_, err = b.CreateResolver(api.APIID, "Query", res)
	require.NoError(t, err)

	return b, func() map[string]any {
		out, execErr := b.ExecuteGraphQL(t.Context(), api.APIID, `query { run }`, "", nil,
			appsync.GraphQLAuth{APIKey: key.ID})
		require.NoError(t, execErr)

		return out
	}
}

func TestHTTPDataSource_SigV4(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth       *appsync.AuthorizationConfig
		name       string
		wantScope  string
		wantSigned bool
	}{
		{name: "unsigned", auth: nil},
		{
			name: "signed", wantSigned: true, wantScope: "/eu-west-1/es/aws4_request",
			auth: &appsync.AuthorizationConfig{
				AuthorizationType: "AWS_IAM",
				AwsIamConfig:      &appsync.AwsIamConfig{SigningRegion: "eu-west-1", SigningServiceName: "es"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu  sync.Mutex
				got string
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				got = r.Header.Get("Authorization")
				mu.Unlock()

				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)

			_, run := dsFixture(t,
				&appsync.DataSource{
					Name: "Web", Type: appsync.DataSourceTypeHTTP,
					HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: srv.URL, AuthorizationConfig: tt.auth},
				},
				&appsync.Resolver{
					RequestMappingTemplate:  `{"version":"2018-05-29","method":"POST","resourcePath":"/x","params":{"body":{"a":1}}}`,
					ResponseMappingTemplate: `$util.toJson($ctx.result.statusCode)`,
				})
			run()

			mu.Lock()
			defer mu.Unlock()

			if !tt.wantSigned {
				assert.Empty(t, got)

				return
			}

			assert.True(t, strings.HasPrefix(got, "AWS4-HMAC-SHA256 "), got)
			assert.Contains(t, got, tt.wantScope)
		})
	}
}

type fakeEB struct {
	entries []appsync.EventBridgeEntry
}

func (f *fakeEB) PutEvents(_ context.Context, e []appsync.EventBridgeEntry) ([]appsync.EventBridgeResult, error) {
	f.entries = append(f.entries, e...)

	return []appsync.EventBridgeResult{{EventID: "ev-1"}}, nil
}

func TestEventBridgeDataSource_Request(t *testing.T) {
	t.Parallel()

	const bus = "arn:aws:events:us-east-1:000000000000:event-bus/b"

	eb := &fakeEB{}
	b, run := dsFixture(t,
		&appsync.DataSource{
			Name: "EB", Type: appsync.DataSourceTypeEventBridge,
			EventBridgeConfig: &appsync.EventBridgeDataSourceConfig{EventBusARN: bus},
		},
		&appsync.Resolver{
			RequestMappingTemplate: `{"version":"2018-05-29","operation":"PutEvents","events":[` +
				`{"source":"my.app","detailType":"T","detail":{"k":"v"},"resources":["r1"]}]}`,
			ResponseMappingTemplate: `$util.toJson($ctx.result)`,
		})
	b.SetEventBridge(eb)

	out := run()

	require.Len(t, eb.entries, 1)
	assert.Equal(t, bus, eb.entries[0].EventBusName)
	assert.JSONEq(t, `{"k":"v"}`, eb.entries[0].Detail)
	assert.Equal(t, []string{"r1"}, eb.entries[0].Resources)

	raw, _ := json.Marshal(out["run"])
	assert.JSONEq(t, `{"Entries":[{"EventId":"ev-1"}],"FailedEntryCount":0}`, string(raw))
}

func TestRDSHelpers(t *testing.T) {
	t.Parallel()

	const result = `{"sqlStatementResults":[{"columnMetadata":[{"label":"id"},{"label":"name"},{"label":"x"}],` +
		`"records":[[{"longValue":1},{"stringValue":"ann"},{"isNull":true}]]}]}`

	b := newTestBackend()

	tests := []struct {
		name, template, want string
	}{
		{"to_json_object", `$util.toJson($util.rds.toJsonObject($ctx.result)[0][0])`, `{"id":1,"name":"ann","x":null}`},
		{"to_json_string", `$util.rds.toJsonString($ctx.result)`, `[[{"id":1,"name":"ann","x":null}]]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctxJSON, _ := json.Marshal(map[string]any{"result": result})
			out, err := b.EvaluateMappingTemplate(tt.template, string(ctxJSON))
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, out)
		})
	}
}

func TestRDSJSModule(t *testing.T) {
	t.Parallel()

	const code = `import {
  sql, select, insert, update, remove, createPgStatement, createMySQLStatement, typeHint
} from '@aws-appsync/utils/rds';
export function request(ctx) {
  const id = 7;
  switch (ctx.arguments.kind) {
    case 'sql': return createPgStatement(
      sql` + "`select * from t where id = ${id} and d = ${typeHint.DATE('2020-01-01')}`" + `);
    case 'select': return createPgStatement(select({
      table: 't', columns: ['a'], limit: 5,
      where: { id: { eq: 7 }, or: [{ n: { gt: 1 } }, { n: { in: [1, 2] } }] },
    }));
    case 'pg_insert': return createPgStatement(insert({ table: 't', values: { a: 1 }, returning: ['a'] }));
    case 'my_update': return createMySQLStatement(update({
      table: 't', values: { a: 2 }, where: { id: { eq: 1 } }, returning: '*' }));
    default: return createPgStatement(remove({ table: 't', where: { id: { eq: 1 } } }));
  }
}
export function response(ctx) { return ctx.result; }`

	b := newTestBackend()

	tests := []struct {
		kind, wantStmts, wantVars string
	}{
		{"sql", `["select * from t where id = :P0 and d = :P1"]`, `{":P0":7,":P1":"2020-01-01"}`},
		{
			"select", `["SELECT a FROM t WHERE id = :P0 AND ((n > :P1) OR (n IN (:P2, :P3))) LIMIT :P4"]`,
			`{":P0":7,":P1":1,":P2":1,":P3":2,":P4":5}`,
		},
		{"pg_insert", `["INSERT INTO t (a) VALUES (:P0) RETURNING a"]`, `{":P0":1}`},
		{
			"my_update", `["UPDATE t SET a = :P0 WHERE id = :P1","SELECT * FROM t WHERE id = :P2"]`,
			`{":P0":2,":P1":1,":P2":1}`,
		},
		{"remove", `["DELETE FROM t WHERE id = :P0"]`, `{":P0":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()

			ctxJSON, _ := json.Marshal(map[string]any{"arguments": map[string]any{"kind": tt.kind}})
			out, err := b.EvaluateCode(code, string(ctxJSON), "request", "APPSYNC_JS")
			require.NoError(t, err)

			var got struct {
				Version     string          `json:"version"`
				Statements  json.RawMessage `json:"statements"`
				VariableMap json.RawMessage `json:"variableMap"`
			}
			require.NoError(t, json.Unmarshal([]byte(out), &got))
			assert.JSONEq(t, tt.wantStmts, string(got.Statements))
			assert.JSONEq(t, tt.wantVars, string(got.VariableMap))
			assert.Equal(t, "2018-05-29", got.Version)
		})
	}
}
