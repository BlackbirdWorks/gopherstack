package rds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSQLServerImageAndEdition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		engine  string
		version string
		image   string
		pid     string
	}{
		{"2017", "sqlserver-ee", "14.00.3465.1.v1", "mcr.microsoft.com/mssql/server:2017-latest", "Developer"},
		{"2019", "sqlserver-web", "15.00.4345.5.v1", "mcr.microsoft.com/mssql/server:2019-latest", "Developer"},
		{"2022", "sqlserver-se", "16.00.4115.5.v1", "mcr.microsoft.com/mssql/server:2022-latest", "Developer"},
		{"2016 falls back", "sqlserver-ex", "13.00.6300.2.v1", "mcr.microsoft.com/mssql/server:2022-latest", "Express"},
		{"empty", "sqlserver-ex", "", "mcr.microsoft.com/mssql/server:2022-latest", "Express"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.image, sqlServerImage(tt.version))
			assert.Equal(t, tt.pid, sqlServerEdition(tt.engine))
			assert.True(t, sqlServerEngine(tt.engine))
		})
	}
}

func TestSQLServerSAPasswordIsComplexAndStable(t *testing.T) {
	t.Parallel()

	a, b := sqlServerSAPassword("ctr-a"), sqlServerSAPassword("ctr-a")
	assert.Equal(t, a, b)
	assert.NotEqual(t, a, sqlServerSAPassword("ctr-b"))
	assert.Regexp(t, `[A-Z]`, a)
	assert.Regexp(t, `[a-z]`, a)
	assert.Regexp(t, `[0-9]`, a)
	assert.Regexp(t, `[^A-Za-z0-9]`, a)
}

func TestSameDumpUsesSQLServerFingerprint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"same fingerprint", "fp:1\nx\tAAA", "fp:1\nx\tBBB", true},
		{"other fingerprint", "fp:1\nx\tAAA", "fp:2\nx\tAAA", false},
		{"plain equal", "dump", "dump", true},
		{"plain differ", "dump", "dump2", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, sameDump(tt.a, tt.b))
		})
	}
}
