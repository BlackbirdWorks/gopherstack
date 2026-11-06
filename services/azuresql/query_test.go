package azuresql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

func TestValidateQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "plain select", query: "SELECT 1"},
		{name: "trailing semicolon", query: "select name from sys.databases;"},
		{name: "multiline join", query: "SELECT a.x\nFROM a JOIN b ON a.id = b.id WHERE a.n = 'it''s'"},
		{name: "hint", query: "SELECT * FROM t WITH (NOLOCK)"},
		{name: "keyword in string literal", query: "SELECT 'drop table x; exec y' AS s"},
		{name: "keyword in bracketed name", query: "SELECT [update], [delete] FROM t"},
		{name: "empty", query: "  ", wantErr: "empty"},
		{name: "too long", query: "SELECT " + strings.Repeat("a", 4000), wantErr: "longer than"},
		{name: "insert", query: "INSERT INTO t VALUES (1)", wantErr: "only SELECT"},
		{name: "cte", query: "WITH x AS (SELECT 1 AS a) SELECT * FROM x", wantErr: "only SELECT"},
		{name: "second statement", query: "SELECT 1; DROP TABLE t", wantErr: "single statement"},
		{name: "select into", query: "SELECT * INTO copy FROM t", wantErr: "INTO"},
		{name: "exec", query: "SELECT 1 EXEC sp_who", wantErr: "EXEC"},
		{name: "openrowset", query: "SELECT * FROM OPENROWSET('x','y','z')", wantErr: "OPENROWSET"},
		{name: "system proc", query: "SELECT xp_cmdshell_x FROM t", wantErr: "system procedures"},
		{name: "login hashes", query: "SELECT password_hash FROM sys.sql_logins", wantErr: "SQL_LOGINS"},
		{name: "line comment", query: "SELECT 1 -- hi", wantErr: "comments"},
		{name: "block comment", query: "SELECT /* x */ 1", wantErr: "comments"},
		{name: "control char", query: "SELECT 1\x00", wantErr: "control character"},
		{name: "waitfor", query: "SELECT 1 WAITFOR DELAY '00:01'", wantErr: "WAITFOR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := azuresql.ValidateQuery(tt.query)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, azuresql.ErrInvalid)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateDatabaseName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		db      string
		wantErr bool
	}{
		{name: "simple", db: "master"},
		{name: "with space", db: "my db"},
		{name: "bracket", db: "a]b", wantErr: true},
		{name: "quote", db: "a'b", wantErr: true},
		{name: "empty", db: "", wantErr: true},
		{name: "semicolon", db: "a;b", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := azuresql.ValidateDatabaseName(tt.db)
			if tt.wantErr {
				require.ErrorIs(t, err, azuresql.ErrInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type queryEngine struct {
	*fakeEngine

	gotDB   string
	gotText string
}

func (e *queryEngine) Query(_ context.Context, database, text string, _ int) (azuresql.QueryResult, error) {
	e.gotDB, e.gotText = database, text

	return azuresql.QueryResult{Columns: []string{"one"}, Rows: [][]string{{"1"}}}, nil
}

type queryLauncher struct{ engine *queryEngine }

func (l *queryLauncher) Launch(_ context.Context, _ azuresql.LaunchSpec) (azuresql.Engine, error) {
	l.engine = &queryEngine{fakeEngine: &fakeEngine{logins: map[string]string{}, dbs: map[string]string{}}}

	return l.engine, nil
}

func TestBackendQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		launcher azuresql.Launcher
		wantErr  error
		name     string
		server   string
		db       string
		query    string
	}{
		{name: "runs on the engine", launcher: &queryLauncher{}, server: "SRV", db: "master", query: "SELECT 1"},
		{
			name:     "unknown server",
			launcher: &queryLauncher{},
			server:   "nope",
			db:       "master",
			query:    "SELECT 1",
			wantErr:  azuresql.ErrNotFound,
		},
		{
			name:     "state only",
			launcher: nil,
			server:   "srv",
			db:       "master",
			query:    "SELECT 1",
			wantErr:  azuresql.ErrNoEngine,
		},
		{
			name:     "engine without query support",
			launcher: &fakeLauncher{},
			server:   "srv",
			db:       "master",
			query:    "SELECT 1",
			wantErr:  azuresql.ErrNoEngine,
		},
		{
			name:     "rejects writes",
			launcher: &queryLauncher{},
			server:   "srv",
			db:       "master",
			query:    "DELETE FROM t",
			wantErr:  azuresql.ErrInvalid,
		},
		{
			name:     "rejects bad database",
			launcher: &queryLauncher{},
			server:   "srv",
			db:       "a]b",
			query:    "SELECT 1",
			wantErr:  azuresql.ErrInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t, tt.launcher)
			mustEnsure(t, b, "srv", azuresql.Spec{AdminLogin: "admin", AdminPassword: "pw"})

			res, err := b.Query(t.Context(), tt.server, tt.db, tt.query)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, [][]string{{"1"}}, res.Rows)

			if l, ok := tt.launcher.(*queryLauncher); ok {
				assert.Equal(t, tt.db, l.engine.gotDB)
				assert.Equal(t, tt.query, l.engine.gotText)
			}
		})
	}
}
