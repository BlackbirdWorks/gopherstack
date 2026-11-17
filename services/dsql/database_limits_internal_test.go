package dsql

import (
	"context"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cases run serially in one database; each restores sys.gs_limits to its defaults before the next starts.
func TestDockerDatabaseLimits(t *testing.T) {
	t.Parallel()

	dp := newDataPlane(t)

	tests := []struct {
		name  string
		limit string
		code  string
		msg   string
		stmt  string
		setup []string
		value int64
	}{
		{
			name:  "columns",
			limit: "columns",
			value: 4,
			stmt:  "CREATE TABLE lim_cols (a int, b int, c int, d int, e int)",
			code:  "54011",
			msg:   "tables can have at most 4 columns",
		},
		{
			name: "added column", limit: "columns", value: 4,
			setup: []string{"CREATE TABLE lim_add (a int, b int, c int, d int)"},
			stmt:  "ALTER TABLE lim_add ADD COLUMN e int", code: "54011", msg: "tables can have at most 4 columns",
		},
		{
			name: "indexes", limit: "indexes", value: 1, stmt: "CREATE TABLE lim_idx (a int UNIQUE, b int UNIQUE)",
			code: "54000", msg: "more than 1 indexes per table are not allowed",
		},
		{
			name: "index keys", limit: "index_keys", value: 2,
			stmt: "CREATE TABLE lim_keys (a int, b int, c int, PRIMARY KEY (a, b, c))",
			code: "54011", msg: "more than 2 column keys in an index are not supported",
		},
		{
			name: "extended statistics", limit: "ext_stats", value: 0,
			setup: []string{"CREATE TABLE lim_stats (a int, b int)"},
			stmt:  "CREATE STATISTICS lim_stats_s ON a, b FROM lim_stats",
			code:  "54000", msg: "more than 0 extended statistics per table are not allowed",
		},
		{
			name: "schemas", limit: "schemas", value: 1, stmt: "CREATE SCHEMA lim_schema",
			code: "54000", msg: "more than 1 schemas not allowed",
		},
		{
			name: "views", limit: "views", value: 0, stmt: "CREATE VIEW lim_view AS SELECT 1 AS a",
			code: "54000", msg: "creating more than 0 views not allowed",
		},
		{
			name: "sequences", limit: "sequences", value: 0, stmt: "CREATE SEQUENCE lim_seq",
			code: "54000", msg: "creating more than 0 sequences is not allowed",
		},
		{
			name:  "row size",
			setup: []string{"CREATE TABLE lim_row (id int PRIMARY KEY, v text)"},
			stmt:  "INSERT INTO lim_row SELECT 1, repeat(md5('x'), 70000)",
			code:  "54000",
			msg:   "maximum row size exceeded",
		},
		{
			name:  "column size",
			setup: []string{"CREATE TABLE lim_col (id int PRIMARY KEY, v text)"},
			stmt:  "INSERT INTO lim_col SELECT 1, repeat(md5('x'), 50000)",
			code:  "54000",
			msg:   "maximum column size exceeded",
		},
		{
			name: "transaction size", limit: "txn_bytes", value: 2 << 20,
			setup: []string{"CREATE TABLE lim_txn (id int PRIMARY KEY, v text)"},
			stmt:  "INSERT INTO lim_txn SELECT g, repeat('x', 1000) FROM generate_series(1, 3000) g",
			code:  "54000", msg: "transaction size limit 2mb exceeded",
		},
		{
			name: "drop guard trigger", setup: []string{"CREATE TABLE lim_trg (id int PRIMARY KEY)"},
			stmt: "DO $$ BEGIN DROP TRIGGER gs_dml_ins ON lim_trg; END $$",
			code: "0A000", msg: "DROP TRIGGER is not supported in Aurora DSQL",
		},
	}

	t.Run("cases", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			runLimitCase(t, dp, tt.name, tt.limit, tt.stmt, tt.code, tt.msg, tt.setup, tt.value)
		}
	})
}

func runLimitCase(
	t *testing.T, dp *dataPlane, name, limit, stmt, code, msg string, setup []string, value int64,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
	defer cancel()

	admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

	super, serr := dp.instance(t).superConn(ctx)
	require.NoError(t, serr, name)

	defer func() { _ = super.Close(context.WithoutCancel(t.Context())) }()

	defaults := readLimits(ctx, t, super)
	defer restoreLimits(context.WithoutCancel(t.Context()), t, super, defaults)

	for _, s := range setup {
		_, serr = admin.Exec(ctx, s)
		require.NoError(t, serr, name+": "+s)
	}

	if limit != "" {
		_, serr = super.Exec(ctx, "UPDATE sys.gs_limits SET value = "+strconv.FormatInt(value, 10)+
			" WHERE name = '"+limit+"'")
		require.NoError(t, serr, name)
	}

	_, err := admin.Exec(ctx, stmt)

	var pgErr *pgconn.PgError

	require.ErrorAs(t, err, &pgErr, name)
	assert.Equal(t, code, pgErr.Code, name)
	assert.Equal(t, msg, pgErr.Message, name)
}

func readLimits(ctx context.Context, t *testing.T, c *pgx.Conn) map[string]int64 {
	t.Helper()

	rows, err := c.Query(ctx, "SELECT name, value FROM sys.gs_limits")
	require.NoError(t, err)

	defer rows.Close()

	out := map[string]int64{}

	for rows.Next() {
		var (
			name string
			v    int64
		)

		require.NoError(t, rows.Scan(&name, &v))

		out[name] = v
	}

	require.NoError(t, rows.Err())

	return out
}

func restoreLimits(ctx context.Context, t *testing.T, c *pgx.Conn, limits map[string]int64) {
	t.Helper()

	for name, v := range limits {
		_, err := c.Exec(ctx, "UPDATE sys.gs_limits SET value = $1 WHERE name = $2", v, name)
		assert.NoError(t, err)
	}
}

func TestDockerTableCountLimit(t *testing.T) {
	t.Parallel()

	dp := newDataPlane(t)

	ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
	defer cancel()

	super, err := dp.instance(t).superConn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = super.Close(context.WithoutCancel(t.Context())) })

	admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

	_, err = admin.Exec(ctx, "CREATE TABLE cap_one (a int)")
	require.NoError(t, err)

	_, err = super.Exec(ctx, "UPDATE sys.gs_limits SET value = 1 WHERE name = 'tables'")
	require.NoError(t, err)

	_, err = admin.Exec(ctx, "CREATE TABLE cap_two (a int)")

	var pgErr *pgconn.PgError

	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "54000", pgErr.Code)
	assert.Equal(t, "creating more than 1 tables not allowed", pgErr.Message)
}

func TestDockerCollationIsC(t *testing.T) {
	t.Parallel()

	dp := newDataPlane(t)

	ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
	defer cancel()

	admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

	for _, tt := range []struct{ name, column, want string }{
		{"collate", "datcollate", "C"},
		{"ctype", "datctype", "C"},
		{"encoding", "pg_encoding_to_char(encoding)", "UTF8"},
	} {
		var got string

		q := "SELECT " + tt.column + " FROM pg_database WHERE datname = current_database()"
		require.NoError(t, admin.QueryRow(ctx, q).Scan(&got), tt.name)
		assert.Equal(t, tt.want, got, tt.name)
	}

	var upper, ordered string

	require.NoError(t, admin.QueryRow(ctx, "SELECT min(v) FROM (VALUES ('a'), ('B')) t(v)").Scan(&ordered))
	assert.Equal(t, "B", ordered, "C collation sorts uppercase before lowercase")

	require.NoError(t, admin.QueryRow(ctx, "SELECT upper('é')").Scan(&upper))
	assert.Equal(t, "é", upper, "the C locale does not case-map non-ASCII letters")
}
