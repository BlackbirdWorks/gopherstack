package dsql

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRewriteSQL(t *testing.T) {
	t.Parallel()

	const raise = "DO $$ BEGIN RAISE EXCEPTION "

	tests := []struct {
		name string
		sql  string
		want string
		// feature, when set, expects an unsupported-feature rejection naming it.
		feature string
	}{
		{name: "async index", sql: "CREATE INDEX ASYNC idx ON t(a)",
			want: "SELECT sys.gs_create_index_async('CREATE INDEX idx ON t(a)') AS job_id"},
		{name: "async unique lowercase trailing semicolon", sql: "create unique index async i on t (a);\n",
			want: "SELECT sys.gs_create_index_async('CREATE UNIQUE INDEX i on t (a)') AS job_id"},
		{name: "async leading comments", sql: "-- note\n/* hi */ CREATE INDEX ASYNC IF NOT EXISTS i ON s.t USING btree (a)",
			want: "SELECT sys.gs_create_index_async('CREATE INDEX IF NOT EXISTS i ON s.t USING btree (a)')" +
				" AS job_id"},
		{name: "async quote escaped", sql: `CREATE INDEX ASYNC "a'b" ON t(x)`,
			want: `SELECT sys.gs_create_index_async('CREATE INDEX "a''b" ON t(x)') AS job_id`},
		{name: "async semicolon in literal", sql: `CREATE INDEX ASYNC i ON t(a) WHERE b = ';x'`,
			want: `SELECT sys.gs_create_index_async('CREATE INDEX i ON t(a) WHERE b = '';x''') AS job_id`},
		{name: "async multiple statements", sql: "CREATE INDEX ASYNC i ON t(a); DROP TABLE t",
			feature: "multiple statements with CREATE INDEX ASYNC"},
		{name: "plain index passes", sql: "CREATE INDEX i ON t(a)", want: "CREATE INDEX i ON t(a)"},
		{name: "async word in table name passes", sql: "CREATE TABLE async_t (a int)", want: "CREATE TABLE async_t (a int)"},
		{name: "select passes", sql: "SELECT 1", want: "SELECT 1"},
		{name: "empty passes", sql: "", want: ""},
		{name: "iam grant", sql: "AWS IAM GRANT example TO 'arn:aws:iam::012345678912:role/example';",
			want: "SELECT sys.gs_iam_grant('example', 'arn:aws:iam::012345678912:role/example')"},
		{name: "iam grant mixed case role folds", sql: "aws iam grant Example to 'arn:x'",
			want: "SELECT sys.gs_iam_grant('example', 'arn:x')"},
		{name: "iam grant quoted role", sql: `AWS IAM GRANT "Ex""ample" TO 'arn:x'`,
			want: `SELECT sys.gs_iam_grant('Ex"ample', 'arn:x')`},
		{name: "iam revoke", sql: "AWS IAM REVOKE example FROM 'arn:x'", want: "SELECT sys.gs_iam_revoke('example', 'arn:x')"},
		{name: "truncate", sql: "TRUNCATE t", feature: "TRUNCATE"},
		{name: "vacuum", sql: "  vacuum analyze t", feature: "VACUUM"},
		{name: "trigger", sql: "CREATE TRIGGER x BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
			feature: "CREATE TRIGGER"},
		{name: "constraint trigger", sql: "create constraint trigger x after insert on t execute function f()",
			feature: "CREATE TRIGGER"},
		{name: "temp table", sql: "CREATE TEMP TABLE x (a int)", feature: "temporary objects"},
		{name: "temporary table", sql: "create global temporary table x (a int)", feature: "temporary objects"},
		{name: "tablespace", sql: "CREATE TABLESPACE ts LOCATION '/x'", feature: "CREATE TABLESPACE"},
		{name: "database", sql: "CREATE DATABASE other", feature: "CREATE DATABASE"},
		{name: "plpgsql function", sql: "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql",
			feature: "PL/pgSQL"},
		{name: "sql function passes", sql: "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
			want: "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := rewriteSQL(tt.sql)

			if tt.feature == "" {
				assert.Equal(t, tt.want, got)

				return
			}

			assert.True(t, strings.HasPrefix(got, raise), got)
			assert.Contains(t, got, tt.feature+" is not supported in Aurora DSQL")
			assert.Contains(t, got, "ERRCODE = '0A000'")
		})
	}
}

func frame(typ byte, body string) []byte {
	out := make([]byte, 5, 5+len(body))
	out[0] = typ
	binary.BigEndian.PutUint32(out[1:], uint32(len(body)+4))

	return append(out, body...)
}

func TestRewriteFrame(t *testing.T) {
	t.Parallel()

	const (
		ddl = "CREATE INDEX ASYNC i ON t(a)"
		sel = "SELECT sys.gs_create_index_async('CREATE INDEX i ON t(a)') AS job_id"
	)

	tests := []struct {
		name string
		typ  byte
		body string
		want string
	}{
		{name: "query rewritten", typ: 'Q', body: ddl + "\x00", want: sel + "\x00"},
		{name: "query untouched", typ: 'Q', body: "SELECT 1\x00", want: "SELECT 1\x00"},
		{name: "parse keeps name and param types", typ: 'P', body: "s1\x00" + ddl + "\x00\x00\x01\x00\x00\x00\x19",
			want: "s1\x00" + sel + "\x00\x00\x01\x00\x00\x00\x19"},
		{name: "parse unnamed untouched", typ: 'P', body: "\x00SELECT $1\x00\x00\x00", want: "\x00SELECT $1\x00\x00\x00"},
		{name: "unterminated query untouched", typ: 'Q', body: ddl, want: ddl},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, string(rewriteFrame(tt.typ, []byte(tt.body))))
		})
	}
}
