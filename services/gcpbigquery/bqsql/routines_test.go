package bqsql_test

import (
	"encoding/json/v2"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

type routineCatalog struct {
	*fakeCatalog

	routines map[string]*bqsql.RoutineData
	mu       sync.Mutex
}

func (r *routineCatalog) Routine(ref bqsql.TableRef) (*bqsql.RoutineData, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rd, ok := r.routines[ref.Dataset+"."+strings.ToLower(ref.Table)]

	return rd, ok
}

func (r *routineCatalog) apply(res *bqsql.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, e := range res.Effects {
		key := e.Ref.Dataset + "." + strings.ToLower(e.Ref.Table)

		switch e.Op {
		case bqsql.OpCreateRoutine:
			r.routines[key] = e.Routine
		case bqsql.OpDropRoutine:
			delete(r.routines, key)
		}
	}
}

func newRoutineCatalog(t *testing.T) *routineCatalog {
	t.Helper()

	return &routineCatalog{fakeCatalog: newCatalog(t), routines: map[string]*bqsql.RoutineData{}}
}

func (r *routineCatalog) run(t *testing.T, sql string) (*bqsql.Result, error) {
	t.Helper()

	st, err := bqsql.ParseStatement(sql)
	if err != nil {
		return nil, err
	}

	res, err := bqsql.Run(t.Context(), st, &bqsql.Input{Catalog: r, Project: "p", Dataset: "ds"}, false)
	if err == nil {
		r.apply(res)
	}

	return res, err
}

func TestSQLRoutines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		want  string
		setup []string
	}{
		{
			"scalar function",
			"SELECT add_one(41) AS v, ds.add_one(1) AS w",
			`[["42","2"]]`,
			[]string{"CREATE FUNCTION add_one(x INT64) RETURNS INT64 AS (x + 1)"},
		},
		{
			"function over columns",
			"SELECT initial(name) FROM people WHERE id < 3 ORDER BY id",
			`[["A"],["B"]]`,
			[]string{"CREATE FUNCTION initial(s STRING) AS (UPPER(SUBSTR(s, 1, 1)))"},
		},
		{
			"function calls function",
			"SELECT quad(1.5)",
			`[["6.0"]]`,
			[]string{
				"CREATE FUNCTION dbl(x FLOAT64) AS (x * 2)",
				"CREATE FUNCTION quad(x FLOAT64) AS (dbl(dbl(x)))",
			},
		},
		{
			"any type argument",
			"SELECT first_of(NULL, 'x'), first_of(3, 4)",
			`[["x","3"]]`,
			[]string{"CREATE FUNCTION first_of(a ANY TYPE, b ANY TYPE) AS (IF(a IS NULL, b, a))"},
		},
		{
			"replace function",
			"SELECT f()",
			`[["2"]]`,
			[]string{"CREATE FUNCTION f() AS (1)", "CREATE OR REPLACE FUNCTION f() AS (2)"},
		},
		{
			"drop function",
			"SELECT 1",
			`[["1"]]`,
			[]string{"CREATE FUNCTION f() AS (1)", "DROP FUNCTION f"},
		},
		{
			"table function",
			"SELECT * FROM named(2) ORDER BY id",
			`[["2","bob"],["3",null]]`,
			[]string{"CREATE TABLE FUNCTION named(min_id INT64) AS (SELECT id, name FROM people WHERE id >= min_id)"},
		},
		{
			"table function with table argument",
			"SELECT * FROM top_ids(TABLE people, 2)",
			`[["1"],["2"]]`,
			[]string{
				"CREATE TABLE FUNCTION top_ids(t TABLE<id INT64>, n INT64) AS (SELECT id FROM t ORDER BY id LIMIT n)",
			},
		},
		{
			"table function return types",
			"SELECT id FROM ids() ORDER BY id LIMIT 1",
			`[["1"]]`,
			[]string{"CREATE TABLE FUNCTION ids() RETURNS TABLE<id STRING> AS (SELECT id FROM people)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := newRoutineCatalog(t)

			for _, s := range tt.setup {
				_, err := cat.run(t, s)
				require.NoError(t, err, s)
			}

			res, err := cat.run(t, tt.sql)
			require.NoError(t, err)

			got, merr := json.Marshal(wireRows(res), jsonx.V1())
			require.NoError(t, merr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestSQLRoutineErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		msg   string
		setup []string
		kind  bqsql.ErrKind
	}{
		{"unknown function", "SELECT nope(1)", "Function not found: nope", nil, bqsql.ErrInvalid},
		{
			"duplicate",
			"CREATE FUNCTION f() AS (2)",
			"Already Exists: Routine p:ds.f",
			[]string{"CREATE FUNCTION f() AS (1)"},
			bqsql.ErrExists,
		},
		{"bad body", "CREATE FUNCTION f(x INT64) AS (x + y)", "Unrecognized name: y", nil, bqsql.ErrInvalid},
		{
			"arity",
			"SELECT f(1, 2)",
			"Number of arguments",
			[]string{"CREATE FUNCTION f(x INT64) AS (x)"},
			bqsql.ErrInvalid,
		},
		{"drop missing", "DROP FUNCTION nope", "Not found: Routine nope", nil, bqsql.ErrNotFound},
		{"missing dataset", "CREATE FUNCTION nods.f() AS (1)", "Dataset p:nods", nil, bqsql.ErrNotFound},
		{
			"js",
			"CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE js AS 'return x;'",
			"JAVASCRIPT",
			nil,
			bqsql.ErrUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat := newRoutineCatalog(t)

			for _, s := range tt.setup {
				_, err := cat.run(t, s)
				require.NoError(t, err, s)
			}

			_, err := cat.run(t, tt.sql)
			require.Error(t, err)

			e := bqsql.AsError(err)
			assert.Equal(t, tt.kind, e.Kind)
			assert.Contains(t, e.Message, tt.msg)
		})
	}
}
