package rds_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

type sqlServerFixture struct {
	*engineFixture
	logins []rds.EngineLogin
	mu     sync.Mutex
}

func newSQLServerFixture(t *testing.T, enabled bool, arch string) *sqlServerFixture {
	t.Helper()

	f := &sqlServerFixture{
		engineFixture: &engineFixture{
			rt: newFakeEngineRuntime(), b: rds.NewInMemoryBackend("123456789012", "us-east-1"),
		},
	}
	t.Cleanup(f.b.Close)
	f.b.EnableEngine(rds.EngineConfig{
		Runtime: f.rt, StartTimeout: engineWait, SQLServer: enabled, HostArch: arch,
		Probe: func(_ context.Context, l rds.EngineLogin) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.logins = append(f.logins, l)

			return nil
		},
	})

	return f
}

func (f *sqlServerFixture) create(t *testing.T, engine, version, dbName, password string) error {
	t.Helper()

	_, err := f.b.CreateDBInstance("db1", engine, "db.m5.large", dbName, "master", "", 20,
		rds.DBInstanceOptions{EngineVersion: version, MasterUserPassword: password})

	return err
}

func TestSQLServerEngineSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		engine  string
		version string
		image   string
		pid     string
	}{
		{"express 2019", "sqlserver-ex", "15.00.4345.5.v1", "mcr.microsoft.com/mssql/server:2019-latest", "Express"},
		{"standard 2022", "sqlserver-se", "16.00.4115.5.v1", "mcr.microsoft.com/mssql/server:2022-latest", "Developer"},
		{
			"enterprise 2017", "sqlserver-ee", "14.00.3465.1.v1",
			"mcr.microsoft.com/mssql/server:2017-latest", "Developer",
		},
		{"web default", "sqlserver-web", "", "mcr.microsoft.com/mssql/server:2022-latest", "Developer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSQLServerFixture(t, true, "amd64")
			require.NoError(t, f.create(t, tt.engine, tt.version, "", "Passw0rd-long"))
			inst := f.waitInstance(t, "db1", "available")

			spec := f.rt.firstSpec()
			assert.Equal(t, tt.image, spec.Image)
			assert.Contains(t, spec.Ports[0], ":1433")
			assert.Contains(t, spec.Env, "ACCEPT_EULA=Y")
			assert.Contains(t, spec.Env, "MSSQL_PID="+tt.pid)
			assert.NotContains(t, spec.Env, "MSSQL_SA_PASSWORD=Passw0rd-long")
			assert.Empty(t, spec.Cmd)
			assert.NotEqual(t, hostPort(t, spec), inst.Port)

			f.mu.Lock()
			defer f.mu.Unlock()
			require.NotEmpty(t, f.logins)
			assert.Equal(t, "sqlserver", f.logins[0].Kind)
			assert.Equal(t, "master", f.logins[0].User)
			assert.Equal(t, "Passw0rd-long", f.logins[0].Password)
			assert.Contains(t, spec.Env, "MSSQL_SA_PASSWORD="+f.logins[0].AdminPassword)
		})
	}
}

func TestSQLServerEngineOptIn(t *testing.T) {
	t.Parallel()

	f := newSQLServerFixture(t, false, "")
	require.NoError(t, f.create(t, "sqlserver-ex", "", "anydb", "short"))
	assert.Equal(t, 0, f.rt.containers())
	assert.Empty(t, f.logins)
}

func TestSQLServerCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dbName   string
		password string
		wantErr  string
	}{
		{"dbname refused", "appdb", "Passw0rd-long", "DBName must be null for engine sqlserver-ex"},
		{"short password", "", "short", "MasterUserPassword is not a valid password"},
		{"long password", "", string(make([]byte, 129)), "MasterUserPassword is not a valid password"},
		{"ok", "", "Passw0rd-long", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newSQLServerFixture(t, true, "amd64")
			err := f.create(t, "sqlserver-ex", "", tt.dbName, tt.password)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSQLServerRefusedOffAmd64(t *testing.T) {
	t.Parallel()

	f := newSQLServerFixture(t, true, "arm64")
	require.NoError(t, f.create(t, "sqlserver-ex", "", "", "Passw0rd-long"))
	f.waitInstance(t, "db1", "failed")
	assert.Contains(t, f.b.EngineFailure("db1"), "amd64-only")
	assert.Equal(t, 0, f.rt.containers())
}
