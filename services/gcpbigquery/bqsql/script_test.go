package bqsql_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

var errTxState = errors.New("tx state")

type scriptHost struct {
	cat      *routineCatalog
	saved    map[string]*bqsql.TableData
	children []bqsql.ChildEvent
}

func (h *scriptHost) Apply(_ context.Context, effects []bqsql.Effect) error {
	h.cat.apply(&bqsql.Result{Effects: effects})

	h.cat.mu.Lock()
	defer h.cat.mu.Unlock()

	for _, e := range effects {
		key := e.Ref.Dataset + "." + e.Ref.Table

		switch e.Op {
		case bqsql.OpWrite:
			h.cat.tables[key].Rows = e.Rows
			h.cat.tables[key].Fields = e.Fields
		case bqsql.OpCreateTable:
			h.cat.tables[key] = &bqsql.TableData{Ref: e.Ref, Fields: e.Fields, Rows: e.Rows}
		case bqsql.OpDropTable:
			delete(h.cat.tables, key)
		}
	}

	return nil
}

func (h *scriptHost) BeginTx() error {
	h.cat.mu.Lock()
	defer h.cat.mu.Unlock()

	h.saved = map[string]*bqsql.TableData{}

	for k, t := range h.cat.tables {
		cp := *t
		cp.Rows = append([][]any(nil), t.Rows...)
		h.saved[k] = &cp
	}

	return nil
}

func (h *scriptHost) CommitTx() error {
	h.saved = nil

	return nil
}

func (h *scriptHost) RollbackTx() error {
	h.cat.mu.Lock()
	defer h.cat.mu.Unlock()

	if h.saved == nil {
		return errTxState
	}

	h.cat.tables, h.saved = h.saved, nil

	return nil
}

func (h *scriptHost) Child(ev bqsql.ChildEvent) { h.children = append(h.children, ev) }

func runScript(t *testing.T, cat *routineCatalog, sql string) (*bqsql.ScriptResult, *scriptHost, error) {
	t.Helper()

	sc, err := bqsql.ParseScript(sql)
	if err != nil {
		return nil, nil, err
	}

	host := &scriptHost{cat: cat}
	res, err := bqsql.RunScript(t.Context(), sc, &bqsql.Input{Catalog: cat, Project: "p", Dataset: "ds"}, host)

	return res, host, err
}

func TestScripts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			"while",
			"DECLARE x INT64 DEFAULT 0; WHILE x < 5 DO SET x = x + 1; END WHILE; SELECT x;",
			`[["5"]]`,
		},
		{
			"if chain",
			`DECLARE n INT64 DEFAULT 7;
			DECLARE s STRING;
			IF n < 5 THEN SET s = 'small'; ELSEIF n < 10 THEN SET s = 'medium'; ELSE SET s = 'large'; END IF;
			SELECT s;`,
			`[["medium"]]`,
		},
		{
			"loop break continue",
			`DECLARE i INT64 DEFAULT 0; DECLARE total INT64 DEFAULT 0;
			LOOP
				SET i = i + 1;
				IF i > 6 THEN BREAK; END IF;
				IF MOD(i, 2) = 0 THEN CONTINUE; END IF;
				SET total = total + i;
			END LOOP;
			SELECT total;`,
			`[["9"]]`,
		},
		{
			"repeat",
			"DECLARE i INT64 DEFAULT 0; REPEAT SET i = i + 3; UNTIL i >= 10 END REPEAT; SELECT i;",
			`[["12"]]`,
		},
		{
			"for loop over rows",
			`DECLARE total INT64 DEFAULT 0;
			FOR rec IN (SELECT id, name FROM people WHERE id < 3 ORDER BY id) DO
				SET total = total + rec.id * 10;
			END FOR;
			SELECT total;`,
			`[["30"]]`,
		},
		{
			"labeled break",
			`DECLARE n INT64 DEFAULT 0;
			outer_loop: LOOP
				LOOP SET n = n + 1; IF n = 3 THEN LEAVE outer_loop; END IF; END LOOP;
			END LOOP outer_loop;
			SELECT n;`,
			`[["3"]]`,
		},
		{
			"exception handler",
			`BEGIN
				SELECT 1/0;
			EXCEPTION WHEN ERROR THEN
				SELECT STARTS_WITH(@@error.message, 'division by zero') AS msg;
			END;`,
			`[["true"]]`,
		},
		{
			"raise",
			`BEGIN RAISE USING MESSAGE = 'boom'; EXCEPTION WHEN ERROR THEN SELECT @@error.message; END;`,
			`[["boom"]]`,
		},
		{
			"execute immediate",
			`DECLARE got INT64;
			EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM people WHERE id > @min' INTO got USING 1 AS min;
			SELECT got;`,
			`[["2"]]`,
		},
		{
			"tuple set",
			"DECLARE a, b INT64; SET (a, b) = (3, 4); SELECT a * b;",
			`[["12"]]`,
		},
		{
			"dml between statements",
			`CREATE TABLE ds.log (n INT64);
			INSERT INTO ds.log VALUES (1), (2);
			DELETE FROM ds.log WHERE n = 1;
			SELECT n FROM ds.log;`,
			`[["2"]]`,
		},
		{
			"temp function",
			"CREATE TEMP FUNCTION triple(x INT64) AS (x * 3); SELECT triple(5);",
			`[["15"]]`,
		},
		{
			"procedure with out param",
			`CREATE PROCEDURE ds.add_to(IN a INT64, IN b INT64, OUT total INT64)
			BEGIN
				SET total = a + b;
			END;
			DECLARE r INT64;
			CALL ds.add_to(2, 40, r);
			SELECT r;`,
			`[["42"]]`,
		},
		{
			"procedure inout and return",
			`CREATE PROCEDURE ds.bump(INOUT v INT64)
			BEGIN
				SET v = v + 1;
				RETURN;
				SET v = 100;
			END;
			DECLARE n INT64 DEFAULT 1;
			CALL ds.bump(n); CALL ds.bump(n);
			SELECT n;`,
			`[["3"]]`,
		},
		{
			"transaction rollback",
			`CREATE TABLE ds.tx (n INT64);
			INSERT INTO ds.tx VALUES (1);
			BEGIN TRANSACTION;
			INSERT INTO ds.tx VALUES (2);
			ROLLBACK;
			SELECT COUNT(*) FROM ds.tx;`,
			`[["1"]]`,
		},
		{
			"transaction commit",
			`CREATE TABLE ds.tx (n INT64);
			BEGIN TRANSACTION; INSERT INTO ds.tx VALUES (1); COMMIT TRANSACTION;
			SELECT COUNT(*) FROM ds.tx;`,
			`[["1"]]`,
		},
		{"assert ok", "ASSERT 1 = 1 AS 'fine'; SELECT 'ok';", `[["ok"]]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, _, err := runScript(t, newRoutineCatalog(t), tt.sql)
			require.NoError(t, err)
			require.NotNil(t, res.Last)

			got, merr := json.Marshal(wireRows(res.Last), jsonx.V1())
			require.NoError(t, merr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestScriptErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{"assert fails", "ASSERT 1 = 2 AS 'bad value';", "Assertion failed: bad value"},
		{"undeclared set", "SET nope = 1;", "Variable nope has not been declared"},
		{"redeclare", "DECLARE x INT64; DECLARE x INT64;", "already declared"},
		{"unhandled raise", "RAISE USING MESSAGE = 'stop';", "stop"},
		{"commit without begin", "COMMIT;", "active transaction"},
		{"bad condition", "IF 1 THEN SELECT 1; END IF;", "must be of type BOOL"},
		{"missing procedure", "CALL ds.none();", "Not found: Procedure"},
		{"runaway loop", "LOOP END LOOP;", "maximum number of executed statements"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := runScript(t, newRoutineCatalog(t), tt.sql)
			require.Error(t, err)
			assert.Contains(t, bqsql.AsError(err).Message, tt.msg)
		})
	}
}

func TestScriptChildren(t *testing.T) {
	t.Parallel()

	res, host, err := runScript(t, newRoutineCatalog(t), "DECLARE x INT64 DEFAULT 1; SELECT x; SELECT x + 1;")
	require.NoError(t, err)
	assert.Equal(t, 2, res.Children)
	require.Len(t, host.children, 2)
	assert.Equal(t, "SELECT", host.children[0].Statement)
}

func TestScriptTempRoutineStaysLocal(t *testing.T) {
	t.Parallel()

	cat := newRoutineCatalog(t)

	_, _, err := runScript(t, cat, "CREATE TEMP FUNCTION f() AS (1); SELECT f();")
	require.NoError(t, err)

	_, _, err = runScript(t, cat, "SELECT f();")
	require.Error(t, err)
}
