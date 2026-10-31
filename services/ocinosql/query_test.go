package ocinosql_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/nosql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedUsers(t *testing.T, e *env) {
	t.Helper()

	e.mkTable(t, "users", usersDDL("users"))

	rows := []map[string]any{
		{"id": 1, "name": "ann", "age": 30, "active": true, "info": map[string]any{"city": "Paris", "score": 5}},
		{"id": 2, "name": "bob", "age": 25, "active": false, "info": map[string]any{"city": "Rome", "score": 9}},
		{"id": 3, "name": "cy", "age": 41, "active": true, "info": map[string]any{"city": "Paris", "score": 1}},
		{"id": 4, "name": "dee", "active": true},
		{"id": 5, "name": "eve", "age": 19, "active": false, "info": map[string]any{"score": 7}},
		{"id": 6, "name": "fay", "age": 30, "active": true, "info": map[string]any{"city": "Oslo", "score": 3}},
	}
	for _, r := range rows {
		e.put(t, "users", r)
	}
}

func ids(items []map[string]any) []int {
	out := make([]int, 0, len(items))

	for _, it := range items {
		if v, ok := it["id"].(float64); ok {
			out = append(out, int(v))
		}
	}

	return out
}

func TestQuerySelect(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	tests := []struct {
		name string
		sql  string
		want []int
	}{
		{name: "all", sql: "SELECT * FROM users", want: []int{1, 2, 3, 4, 5, 6}},
		{name: "gt", sql: "SELECT id FROM users WHERE age > 29", want: []int{1, 3, 6}},
		{name: "and", sql: "SELECT id FROM users WHERE age >= 30 AND active = true", want: []int{1, 3, 6}},
		{name: "or", sql: "SELECT id FROM users WHERE name = 'ann' OR name = 'bob'", want: []int{1, 2}},
		{name: "in", sql: "SELECT id FROM users WHERE id IN (1, 3, 5)", want: []int{1, 3, 5}},
		{name: "not in", sql: "SELECT id FROM users WHERE id NOT IN (1, 2)", want: []int{3, 4, 5, 6}},
		{name: "is null", sql: "SELECT id FROM users WHERE age IS NULL", want: []int{4}},
		{name: "is not null", sql: "SELECT id FROM users WHERE info IS NOT NULL", want: []int{1, 2, 3, 5, 6}},
		{name: "json path", sql: "SELECT id FROM users WHERE info.city = 'Paris'", want: []int{1, 3}},
		{name: "alias path", sql: "SELECT u.id FROM users u WHERE u.info.score >= 5", want: []int{1, 2, 5}},
		{name: "table qualified", sql: "SELECT users.id FROM users WHERE users.age = 25", want: []int{2}},
		{name: "not paren", sql: "SELECT id FROM users WHERE NOT (age < 30 OR age IS NULL)", want: []int{1, 3, 6}},
		{name: "between", sql: "SELECT id FROM users WHERE age BETWEEN 19 AND 25", want: []int{2, 5}},
		{name: "neq", sql: "SELECT id FROM users WHERE age <> 30", want: []int{2, 3, 5}},
		{name: "string cmp", sql: "SELECT id FROM users WHERE name >= 'dee'", want: []int{4, 5, 6}},
		{name: "order desc limit", sql: "SELECT id FROM users ORDER BY id DESC LIMIT 2", want: []int{6, 5}},
		{name: "limit offset", sql: "SELECT id FROM users LIMIT 2 OFFSET 2", want: []int{3, 4}},
		{name: "offset limit", sql: "SELECT id FROM users OFFSET 4 LIMIT 5", want: []int{5, 6}},
		{name: "offset past end", sql: "SELECT id FROM users OFFSET 50", want: []int{}},
		{name: "case insensitive", sql: "select ID from USERS where AGE = 41", want: []int{3}},
		{name: "no match", sql: "SELECT id FROM users WHERE name = 'zed'", want: []int{}},
		{name: "trailing semicolon", sql: "SELECT id FROM users WHERE info.score = 9;", want: []int{2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := e.query(t, tt.sql, 0, nil)
			assert.Equal(t, tt.want, ids(resp.Items))
			assert.Positive(t, *resp.Usage.ReadUnitsConsumed)
		})
	}
}

func TestQueryProjection(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	resp := e.query(t, "SELECT name, age AS years, info.city FROM users WHERE id = 1", 0, nil)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, map[string]any{"name": "ann", "years": float64(30), "city": "Paris"}, resp.Items[0])

	resp = e.query(t, "SELECT * FROM users WHERE id = 4", 0, nil)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "dee", resp.Items[0]["name"])
	assert.Nil(t, resp.Items[0]["age"])

	cnt := e.query(t, "SELECT COUNT(*) FROM users WHERE active = true", 0, nil)
	require.Len(t, cnt.Items, 1)
	assert.InDelta(t, 4, cnt.Items[0]["Column_1"], 0)

	cnt = e.query(t, "SELECT COUNT(*) FROM users", 0, nil)
	assert.InDelta(t, 6, cnt.Items[0]["Column_1"], 0)
}

func TestQueryPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	var (
		got   []int
		page  *string
		pages int
	)

	for {
		resp := e.query(t, "SELECT id FROM users", 4, page)
		got = append(got, ids(resp.Items)...)
		pages++

		if resp.OpcNextPage == nil {
			break
		}

		page = resp.OpcNextPage
	}

	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)
	assert.Equal(t, 2, pages)

	resp := e.query(t, "SELECT id FROM users LIMIT 5", 3, nil)
	assert.Equal(t, []int{1, 2, 3}, ids(resp.Items), "page size and statement LIMIT combine")
	require.NotNil(t, resp.OpcNextPage)

	resp = e.query(t, "SELECT id FROM users LIMIT 5", 3, resp.OpcNextPage)
	assert.Equal(t, []int{4, 5}, ids(resp.Items))
	assert.Nil(t, resp.OpcNextPage)
}

func TestQueryVariablesAndPrepared(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	stmt := "DECLARE $min INTEGER; $city STRING; SELECT id FROM users WHERE age > $min AND info.city = $city"

	prep, err := e.client.PrepareStatement(t.Context(), nosql.PrepareStatementRequest{
		CompartmentId: new(testCompartment), Statement: new(stmt), IsGetQueryPlan: new(true),
	})
	require.NoError(t, err)
	require.NotNil(t, prep.Statement)
	assert.NotNil(t, prep.QueryPlan)

	tests := []struct {
		name string
		vars map[string]any
		want []int
		fail bool
	}{
		{name: "bound", vars: map[string]any{"$min": 20, "$city": "Paris"}, want: []int{1, 3}},
		{name: "without dollar", vars: map[string]any{"min": 35, "city": "Paris"}, want: []int{3}},
		{name: "missing", vars: map[string]any{"$min": 20}, fail: true},
		{name: "wrong type", vars: map[string]any{"$min": "x", "$city": "Paris"}, fail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, prepared := range []bool{false, true} {
				statement := stmt
				if prepared {
					statement = *prep.Statement
				}

				resp, qerr := e.client.Query(t.Context(), nosql.QueryRequest{
					CompartmentId: new(testCompartment), Statement: new(statement), IsPrepared: new(prepared),
					Variables: tt.vars})
				if tt.fail {
					assert.Equal(t, http.StatusBadRequest, svcErr(t, qerr).GetHTTPStatusCode())

					continue
				}

				require.NoError(t, qerr)
				assert.Equal(t, tt.want, ids(resp.Items))
			}
		})
	}

	_, err = e.client.Query(t.Context(), nosql.QueryRequest{
		CompartmentId: new(testCompartment), Statement: new("SELECT id FROM users"), IsPrepared: new(true)})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "raw SQL is not a prepared statement")
}

func TestQueryUnsupported(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seedUsers(t, e)

	tests := []struct {
		name   string
		sql    string
		status int
	}{
		{name: "insert", sql: "INSERT INTO users VALUES (9, 'x')", status: 400},
		{name: "update", sql: "UPDATE users SET age = 1 WHERE id = 1", status: 400},
		{name: "delete", sql: "DELETE FROM users WHERE id = 1", status: 400},
		{name: "create table", sql: "CREATE TABLE z (a INTEGER, PRIMARY KEY(a))", status: 400},
		{name: "alter table", sql: "ALTER TABLE users (ADD z INTEGER)", status: 400},
		{name: "drop table", sql: "DROP TABLE users", status: 400},
		{name: "create index", sql: "CREATE INDEX i ON users(age)", status: 400},
		{name: "join", sql: "SELECT * FROM users a JOIN users b ON a.id = b.id", status: 400},
		{name: "group by", sql: "SELECT age FROM users GROUP BY age", status: 400},
		{name: "function", sql: "SELECT lower(name) FROM users", status: 400},
		{name: "like", sql: "SELECT id FROM users WHERE name LIKE 'a%'", status: 400},
		{name: "distinct", sql: "SELECT DISTINCT age FROM users", status: 400},
		{name: "sum", sql: "SELECT SUM(age) FROM users", status: 400},
		{name: "garbage", sql: "BANANA", status: 400},
		{name: "missing from", sql: "SELECT id", status: 400},
		{name: "unterminated string", sql: "SELECT id FROM users WHERE name = 'x", status: 400},
		{name: "unknown column", sql: "SELECT nope FROM users", status: 400},
		{name: "unknown where column", sql: "SELECT id FROM users WHERE nope = 1", status: 400},
		{name: "unknown table", sql: "SELECT * FROM ghosts", status: 404},
		{name: "order by non key", sql: "SELECT id FROM users ORDER BY age", status: 400},
		{name: "undeclared variable", sql: "SELECT id FROM users WHERE id = $x", status: 400},
		{name: "negative limit", sql: "SELECT id FROM users LIMIT -1", status: 400},
		{name: "huge limit", sql: "SELECT id FROM users LIMIT 9223372036854775807", status: 400},
		{name: "huge offset", sql: "SELECT id FROM users OFFSET 4294967296", status: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.Query(t.Context(), nosql.QueryRequest{
				CompartmentId: new(testCompartment), Statement: new(tt.sql)})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode(), se.GetMessage())

			if tt.status == http.StatusBadRequest {
				assert.Equal(t, "InvalidParameter", se.GetCode())
			}
		})
	}
}

func TestSummarizeStatement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name       string
		sql        string
		op         nosql.StatementSummaryOperationEnum
		table      string
		index      string
		ifExists   bool
		ifNotExist bool
		syntaxErr  bool
	}{
		{
			name:  "select",
			sql:   "SELECT * FROM users WHERE id = 1",
			op:    nosql.StatementSummaryOperationSelect,
			table: "users",
		},
		{name: "bad select", sql: "SELECT FROM", op: nosql.StatementSummaryOperationSelect, syntaxErr: true},
		{name: "create table", sql: "CREATE TABLE IF NOT EXISTS t (a INTEGER, PRIMARY KEY(a))",
			op: nosql.StatementSummaryOperationCreateTable, table: "t", ifNotExist: true},
		{
			name:      "bad create",
			sql:       "CREATE TABLE t (a INTEGER)",
			op:        nosql.StatementSummaryOperationCreateTable,
			table:     "t",
			syntaxErr: true,
		},
		{name: "alter", sql: "ALTER TABLE t (ADD b STRING)", op: nosql.StatementSummaryOperationAlterTable, table: "t"},
		{
			name:     "drop table",
			sql:      "DROP TABLE IF EXISTS t",
			op:       nosql.StatementSummaryOperationDropTable,
			table:    "t",
			ifExists: true,
		},
		{name: "create index", sql: "CREATE INDEX IF NOT EXISTS i1 ON t(a)",
			op: nosql.StatementSummaryOperationCreateIndex, table: "t", index: "i1", ifNotExist: true},
		{
			name:  "drop index",
			sql:   "DROP INDEX i1 ON t",
			op:    nosql.StatementSummaryOperationDropIndex,
			table: "t",
			index: "i1",
		},
		{name: "insert", sql: "INSERT INTO t VALUES (1)", op: nosql.StatementSummaryOperationInsert, table: "t"},
		{name: "update", sql: "UPDATE t SET a = 1", op: nosql.StatementSummaryOperationUpdate, table: "t"},
		{name: "delete", sql: "DELETE FROM t", op: nosql.StatementSummaryOperationDelete, table: "t"},
		{name: "garbage", sql: "BANANA PIE", syntaxErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.SummarizeStatement(t.Context(), nosql.SummarizeStatementRequest{
				CompartmentId: new(testCompartment), Statement: new(tt.sql),
			})
			require.NoError(t, err)

			assert.Equal(t, tt.op, resp.Operation)
			assert.Equal(t, tt.syntaxErr, resp.SyntaxError != nil)

			if tt.table != "" {
				require.NotNil(t, resp.TableName)
				assert.Equal(t, tt.table, *resp.TableName)
			}

			if tt.index != "" {
				require.NotNil(t, resp.IndexName)
				assert.Equal(t, tt.index, *resp.IndexName)
			}

			assert.Equal(t, tt.ifExists, resp.IsIfExists != nil && *resp.IsIfExists)
			assert.Equal(t, tt.ifNotExist, resp.IsIfNotExists != nil && *resp.IsIfNotExists)
		})
	}
}
