package appsync_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

var (
	errNoFunction = errors.New("no such function")
	errBoom       = errors.New("boom")
)

type rdsRow []any

type catalogRDS struct {
	err   error
	rules []rdsRule
}

type rdsRule struct {
	match string
	rows  []rdsRow
	fail  bool
}

func str(s string) map[string]any { return map[string]any{"stringValue": s} }

func num(n int) map[string]any { return map[string]any{"longValue": float64(n)} }

func (c *catalogRDS) ExecuteRDSStatement(_ context.Context, st appsync.RDSStatement) (map[string]any, error) {
	if c.err != nil {
		return nil, c.err
	}

	for _, r := range c.rules {
		if !strings.Contains(st.SQL, r.match) {
			continue
		}

		if r.fail {
			return nil, errNoFunction
		}

		recs := make([]any, len(r.rows))
		for i, row := range r.rows {
			recs[i] = []any(row)
		}

		return map[string]any{"records": recs}, nil
	}

	return map[string]any{"records": []any{}}, nil
}

func postgresCatalog() *catalogRDS {
	return &catalogRDS{rules: []rdsRule{
		{match: "SELECT version()", rows: []rdsRow{{str("PostgreSQL 16.2")}}},
		{match: "information_schema.columns", rows: []rdsRow{
			{str("orders"), str("id"), str("integer"), str("NO"), nil},
			{str("orders"), str("note"), str("character varying"), str("YES"), num(40)},
			{str("orders"), str("placed"), str("timestamp without time zone"), str("YES"), nil},
			{str("users"), str("id"), str("uuid"), str("NO"), nil},
			{str("users"), str("balance"), str("numeric"), str("YES"), nil},
		}},
		{match: "table_constraints", rows: []rdsRow{
			{str("orders"), str("orders_pkey"), str("id")},
			{str("users"), str("users_pkey"), str("id")},
		}},
		{match: "pg_index", rows: []rdsRow{
			{str("orders"), str("orders_note_placed"), str("note")},
			{str("orders"), str("orders_note_placed"), str("placed")},
		}},
	}}
}

func sqliteCatalog() *catalogRDS {
	return &catalogRDS{rules: []rdsRule{
		{match: "SELECT version()", fail: true},
		{match: "sqlite_version", rows: []rdsRow{{str("3.45.0")}}},
		{match: "FROM sqlite_master", rows: []rdsRow{{str("t")}}},
		{match: "pragma_table_info('t')", rows: []rdsRow{
			{str("a"), str("INTEGER"), num(1), num(1)},
			{str("b"), str("VARCHAR(20)"), num(0), num(2)},
		}},
		{match: "pragma_index_list('t')", rows: []rdsRow{{str("t_b")}}},
		{match: "pragma_index_info('t_b')", rows: []rdsRow{{str("b")}}},
	}}
}

func startIntrospection(
	t *testing.T, b *appsync.InMemoryBackend,
) *appsync.DataSourceIntrospection {
	t.Helper()

	rec, err := b.StartDataSourceIntrospection(t.Context(), &appsync.RDSDataAPIConfig{
		DatabaseName: "db",
		ResourceARN:  "arn:aws:rds:us-east-1:000000000000:cluster:c",
		SecretARN:    "arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
	})
	require.NoError(t, err)

	return rec
}

func TestIntrospectionModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		exec       appsync.RDSDataExecutor
		name       string
		wantDetail string
		wantStatus string
		wantNames  []string
	}{
		{name: "postgres", exec: postgresCatalog(), wantStatus: "SUCCESS", wantNames: []string{"orders", "users"}},
		{name: "sqlite", exec: sqliteCatalog(), wantStatus: "SUCCESS", wantNames: []string{"t"}},
		{
			name: "engine error", exec: &catalogRDS{err: errBoom}, wantStatus: "FAILED",
			wantDetail: "boom",
		},
		{name: "unknown engine", exec: &catalogRDS{}, wantStatus: "FAILED", wantDetail: "could not be identified"},
		{name: "no executor", wantStatus: "FAILED", wantDetail: "not configured"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			if tt.exec != nil {
				b.SetRDSData(tt.exec)
			}

			started := startIntrospection(t, b)
			assert.Equal(t, appsync.DataSourceIntrospectionStatusProcessing, started.IntrospectionStatus)

			got, err := b.GetDataSourceIntrospection(started.IntrospectionID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.IntrospectionStatus)
			assert.Contains(t, got.IntrospectionStatusDetail, tt.wantDetail)

			var names []string

			if got.IntrospectionResult != nil {
				for _, m := range got.IntrospectionResult.Models {
					names = append(names, m.Name)
				}
			}

			assert.Equal(t, tt.wantNames, names)
		})
	}
}

func TestIntrospectionModelContent(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	b.SetRDSData(postgresCatalog())

	got, err := b.GetDataSourceIntrospection(startIntrospection(t, b).IntrospectionID)
	require.NoError(t, err)

	orders := got.IntrospectionResult.Models[0]
	assert.Equal(t, "orders_pkey", orders.PrimaryKey.Name)
	assert.Equal(t, []string{"id"}, orders.PrimaryKey.Fields)
	require.Len(t, orders.Indexes, 1)
	assert.Equal(t, []string{"note", "placed"}, orders.Indexes[0].Fields)
	require.Len(t, orders.Fields, 3)

	id, note, placed := orders.Fields[0], orders.Fields[1], orders.Fields[2]
	assert.Equal(t, "NonNull", id.Type.Kind)
	assert.Equal(t, "Int", id.Type.Type.Name)
	assert.Equal(t, "String", note.Type.Name)
	assert.EqualValues(t, 40, note.Length)
	assert.Equal(t, "AWSDateTime", placed.Type.Name)

	users := got.IntrospectionResult.Models[1]
	assert.Equal(t, "ID", users.Fields[0].Type.Type.Name)
	assert.Equal(t, "Float", users.Fields[1].Type.Name)

	lite := newTestBackend()
	lite.SetRDSData(sqliteCatalog())

	got, err = lite.GetDataSourceIntrospection(startIntrospection(t, lite).IntrospectionID)
	require.NoError(t, err)

	tbl := got.IntrospectionResult.Models[0]
	assert.Equal(t, []string{"a", "b"}, tbl.PrimaryKey.Fields)
	assert.EqualValues(t, 20, tbl.Fields[1].Length)
	assert.Equal(t, "t_b", tbl.Indexes[0].Name)
}

func TestHandlerIntrospectionPagingAndSDL(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	b.SetRDSData(postgresCatalog())
	id := startIntrospection(t, b).IntrospectionID

	type result struct {
		IntrospectionResult struct {
			NextToken string `json:"nextToken"`
			Models    []struct {
				Name string `json:"name"`
				SDL  string `json:"sdl"`
			} `json:"models"`
		} `json:"introspectionResult"`
	}

	get := func(query string) (int, result) {
		rec := doRequest(t, h, http.MethodGet, "/v1/datasources/introspections/"+id+query, nil)

		var out result
		_ = json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1())

		return rec.Code, out
	}

	code, first := get("?maxResults=1&includeModelsSDL=true")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, first.IntrospectionResult.Models, 1)
	assert.Equal(t, "orders", first.IntrospectionResult.Models[0].Name)
	assert.Equal(t, "type orders {\n  id: Int!\n  note: String\n  placed: AWSDateTime\n}",
		first.IntrospectionResult.Models[0].SDL)
	require.NotEmpty(t, first.IntrospectionResult.NextToken)

	code, second := get("?maxResults=1&nextToken=" + first.IntrospectionResult.NextToken)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, second.IntrospectionResult.Models, 1)
	assert.Equal(t, "users", second.IntrospectionResult.Models[0].Name)
	assert.Empty(t, second.IntrospectionResult.Models[0].SDL)
	assert.Empty(t, second.IntrospectionResult.NextToken)

	code, _ = get("?nextToken=not-base64!")
	assert.Equal(t, http.StatusBadRequest, code)
}

type recordingRDS struct {
	inner appsync.RDSDataExecutor
	sqls  []string
}

func (r *recordingRDS) ExecuteRDSStatement(ctx context.Context, st appsync.RDSStatement) (map[string]any, error) {
	r.sqls = append(r.sqls, st.SQL)

	return r.inner.ExecuteRDSStatement(ctx, st)
}

func TestIntrospectionRequestValuesNeverReachSQL(t *testing.T) {
	t.Parallel()

	hostile := []string{`db'; DROP TABLE users; --`, `db" OR 1=1 /*`, "db\x00;--"}

	for _, name := range hostile {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := &recordingRDS{inner: postgresCatalog()}
			b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
			b.SetRDSData(rec)

			_, err := b.StartDataSourceIntrospection(t.Context(), &appsync.RDSDataAPIConfig{
				DatabaseName: name,
				ResourceARN:  name,
				SecretARN:    name,
			})
			require.NoError(t, err)
			require.NotEmpty(t, rec.sqls)

			for _, sql := range rec.sqls {
				assert.NotContains(t, sql, "DROP")
				assert.NotContains(t, sql, "OR 1=1")
			}
		})
	}
}

func TestIntrospectionSQLiteTableNamesAreQuoted(t *testing.T) {
	t.Parallel()

	rec := &recordingRDS{inner: &catalogRDS{rules: []rdsRule{
		{match: "SELECT version()", fail: true},
		{match: "sqlite_version", rows: []rdsRow{{str("3.45.0")}}},
		{match: "FROM sqlite_master", rows: []rdsRow{{str("t');DROP TABLE x;--")}}},
	}}}
	b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
	b.SetRDSData(rec)

	startIntrospection(t, b)

	var found bool

	for _, sql := range rec.sqls {
		if strings.Contains(sql, "DROP") {
			found = true

			assert.Contains(t, sql, "('t'');DROP TABLE x;--')")
		}
	}

	assert.True(t, found)
}

func TestIntrospectionStartResponseOmitsOutcome(t *testing.T) {
	t.Parallel()

	b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
	b.SetRDSData(&catalogRDS{err: errBoom})

	started := startIntrospection(t, b)
	assert.Equal(t, appsync.DataSourceIntrospectionStatusProcessing, started.IntrospectionStatus)
	assert.Empty(t, started.IntrospectionStatusDetail)
	assert.Nil(t, started.IntrospectionResult)

	got, err := b.GetDataSourceIntrospection(started.IntrospectionID)
	require.NoError(t, err)
	assert.Equal(t, appsync.DataSourceIntrospectionStatusFailed, got.IntrospectionStatus)
	assert.Contains(t, got.IntrospectionStatusDetail, "boom")
}

func TestIntrospectionJobsAreBounded(t *testing.T) {
	t.Parallel()

	b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
	b.SetRDSData(postgresCatalog())

	var tick int64

	b.SetClock(func() time.Time {
		tick++

		return time.Unix(0, tick)
	})

	first := startIntrospection(t, b)

	var last *appsync.DataSourceIntrospection

	for range 100 {
		last = startIntrospection(t, b)
	}

	_, err := b.GetDataSourceIntrospection(first.IntrospectionID)
	require.ErrorIs(t, err, appsync.ErrNotFound)

	_, err = b.GetDataSourceIntrospection(last.IntrospectionID)
	require.NoError(t, err)
}

func TestIntrospectionScalarMapping(t *testing.T) {
	t.Parallel()

	tests := []struct{ sqlType, want string }{
		{"integer", "Int"}, {"smallint", "Int"}, {"tinyint", "Int"}, {"serial", "Int"},
		{"interval", "String"}, {"point", "String"}, {"bigint", "Float"}, {"text", "String"},
	}

	for _, tt := range tests {
		t.Run(tt.sqlType, func(t *testing.T) {
			t.Parallel()

			b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
			b.SetRDSData(&catalogRDS{rules: []rdsRule{
				{match: "SELECT version()", rows: []rdsRow{{str("PostgreSQL 16")}}},
				{match: "information_schema.columns", rows: []rdsRow{
					{str("t"), str("c"), str(tt.sqlType), str("YES"), nil},
				}},
			}})

			got, err := b.GetDataSourceIntrospection(startIntrospection(t, b).IntrospectionID)
			require.NoError(t, err)
			require.NotNil(t, got.IntrospectionResult)
			assert.Equal(t, tt.want, got.IntrospectionResult.Models[0].Fields[0].Type.Name)
		})
	}
}
