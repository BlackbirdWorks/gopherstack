package gcpsql_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

var errFakeSecret = errors.New("secret is disabled")

func (f *fakeEngine) ExecuteSQL(_ context.Context, req gcpsql.ExecRequest) (gcpsql.ExecOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.execReqs = append(f.execReqs, req)

	return f.execOut, nil
}

type fakeSecrets map[string]string

func (f fakeSecrets) AccessSecret(name string) ([]byte, error) {
	v, ok := f[name]
	if !ok {
		return nil, errFakeSecret
	}

	return []byte(v), nil
}

func (e *env) execSettings(access string) *sqladmin.Settings {
	return &sqladmin.Settings{DataApiAccess: access}
}

func TestExecuteSQL(t *testing.T) {
	t.Parallel()

	one := "1"
	tests := []struct {
		outcome  gcpsql.ExecOutcome
		wantUser string
		wantPass string
		name     string
		version  string
		access   string
		payload  sqladmin.ExecuteSqlPayload
		wantCode int
		wantStat bool
	}{
		{
			name: "pg rows", version: "POSTGRES_16", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{Database: "postgres", SqlStatement: "select 1", RowLimit: 5},
			outcome: gcpsql.ExecOutcome{Results: []gcpsql.ExecResult{{
				Columns: []gcpsql.ExecColumn{{Name: "n", Type: "INT4"}},
				Rows:    []gcpsql.ExecRow{{Values: []gcpsql.ExecValue{{Value: one}}}},
			}}},
			wantPass: "pw-root",
		},
		{
			name: "mysql no database", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{SqlStatement: "select 1"}, wantPass: "pw-root",
		},
		{
			name: "pg database required", version: "POSTGRES_16", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{SqlStatement: "select 1"}, wantCode: http.StatusBadRequest,
		},
		{
			name: "access disallowed", version: "MYSQL_8_4", access: "DISALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{SqlStatement: "select 1"}, wantCode: http.StatusForbidden,
		},
		{
			name: "access default", version: "MYSQL_8_4",
			payload: sqladmin.ExecuteSqlPayload{SqlStatement: "select 1"}, wantCode: http.StatusForbidden,
		},
		{
			name: "statement required", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "iam refused", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload:  sqladmin.ExecuteSqlPayload{SqlStatement: "select 1", AutoIamAuthn: true},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "secret password", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{
				SqlStatement: "select 1", User: "app", PasswordSecretVersion: "projects/p/secrets/s/versions/1",
			},
			wantUser: "app", wantPass: "from-secret",
		},
		{
			name: "missing secret", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload: sqladmin.ExecuteSqlPayload{
				SqlStatement: "select 1", User: "app", PasswordSecretVersion: "projects/p/secrets/gone/versions/1",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "database error", version: "MYSQL_8_4", access: "ALLOW_DATA_API",
			payload:  sqladmin.ExecuteSqlPayload{SqlStatement: "selec"},
			outcome:  gcpsql.ExecOutcome{Failure: "role rejected"},
			wantStat: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.backend.SetSecretReader(fakeSecrets{"projects/p/secrets/s/versions/1": "from-secret"})
			e.createInstance(t, "db", tt.version, e.execSettings(tt.access))
			e.launcher.engine(0).execOut = tt.outcome

			resp, err := e.svc.Instances.ExecuteSql(project, "db", &tt.payload).Context(t.Context()).Do()
			if tt.wantCode != 0 {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, statusOf(t, err))

				return
			}

			require.NoError(t, err)

			if tt.wantStat {
				require.NotNil(t, resp.Status)
				assert.Contains(t, resp.Status.Message, "role rejected")
				assert.Empty(t, resp.Results)

				return
			}

			assert.NotNil(t, resp.Metadata)
			assert.Len(t, resp.Results, len(tt.outcome.Results))

			reqs := e.launcher.engine(0).execReqs
			require.Len(t, reqs, 1)
			assert.Equal(t, tt.wantUser, reqs[0].User)
			assert.Equal(t, tt.wantPass, reqs[0].Password)
			assert.Equal(t, tt.payload.RowLimit, reqs[0].RowLimit)
		})
	}
}

func TestExecuteSQLShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, "db", "POSTGRES_16", e.execSettings("ALLOW_DATA_API"))
	e.launcher.engine(0).execOut = gcpsql.ExecOutcome{
		Results: []gcpsql.ExecResult{{
			Columns: []gcpsql.ExecColumn{{Name: "n", Type: "INT4"}, {Name: "s", Type: "TEXT"}},
			Rows:    []gcpsql.ExecRow{{Values: []gcpsql.ExecValue{{Value: "1"}, {NullValue: true}}}},
		}, {Message: "INSERT 0 1"}},
		Messages: []gcpsql.ExecMessage{{Severity: "NOTICE", Message: "NOTICE: hi"}},
	}

	resp, err := e.svc.Instances.ExecuteSql(project, "db", &sqladmin.ExecuteSqlPayload{
		Database: "postgres", SqlStatement: "select 1; insert",
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, resp.Results, 2)
	assert.Equal(t, "INT4", resp.Results[0].Columns[0].Type)
	assert.Equal(t, "1", resp.Results[0].Rows[0].Values[0].Value)
	assert.True(t, resp.Results[0].Rows[0].Values[1].NullValue)
	assert.Equal(t, "INSERT 0 1", resp.Results[1].Message)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "NOTICE", resp.Messages[0].Severity)
	assert.Regexp(t, `^\d+\.\d{3}s$`, resp.Metadata.SqlStatementExecutionTime)
}

func TestExecuteSQLStateOnly(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, nil)
	e.createInstance(t, "db", "MYSQL_8_4", e.execSettings("ALLOW_DATA_API"))

	_, err := e.svc.Instances.ExecuteSql(project, "db", &sqladmin.ExecuteSqlPayload{SqlStatement: "select 1"}).
		Context(t.Context()).Do()
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, statusOf(t, err))
}
