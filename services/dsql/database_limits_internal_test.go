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

// Cases share one database: every case that sets a limit must use the same value as any other case on that limit,
// and no case may create a table wider than the lowest columns limit (4).
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
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(t.Context(), dockerEngineWait)
				defer cancel()

				admin := dp.mustOpen(t, "admin", dp.token(t, true), pgx.QueryExecModeSimpleProtocol)

				super, serr := dp.instance(t).superConn(ctx)
				require.NoError(t, serr)
				t.Cleanup(func() { _ = super.Close(context.WithoutCancel(t.Context())) })

				for _, s := range tt.setup {
					_, serr = admin.Exec(ctx, s)
					require.NoError(t, serr, s)
				}

				if tt.limit != "" {
					_, serr = super.Exec(ctx, "UPDATE sys.gs_limits SET value = "+strconv.FormatInt(tt.value, 10)+
						" WHERE name = '"+tt.limit+"'")
					require.NoError(t, serr)
				}

				_, err := admin.Exec(ctx, tt.stmt)

				var pgErr *pgconn.PgError

				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, tt.code, pgErr.Code)
				assert.Equal(t, tt.msg, pgErr.Message)
			})
		}
	})
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
