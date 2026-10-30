package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestInitializeServices_TimestreamQueryCatalogWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{}
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	tsqH, ok := byName["TimestreamQuery"].(*timestreamquerybackend.Handler)
	require.True(t, ok)

	tsqBk, ok := tsqH.Backend.(*timestreamquerybackend.InMemoryBackend)
	require.True(t, ok)

	tswH, ok := byName["TimestreamWrite"].(*timestreamwritebackend.Handler)
	require.True(t, ok)

	_, err = tswH.Backend.CreateDatabase("wdb", "", nil)
	require.NoError(t, err)
	_, err = tswH.Backend.CreateTable("wdb", "wtbl", nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "existing table", query: "SELECT * FROM wdb.wtbl"},
		{name: "missing table", query: "SELECT * FROM wdb.nope", wantErr: "Table 'wdb.nope' does not exist"},
		{name: "missing database", query: "SELECT * FROM nodb.wtbl", wantErr: "Database 'nodb' does not exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, qErr := tsqBk.QueryWithOptions(t.Context(), timestreamquerybackend.QueryOptions{QueryString: tt.query})
			if tt.wantErr == "" {
				require.NoError(t, qErr)

				return
			}

			require.ErrorIs(t, qErr, timestreamquerybackend.ErrValidation)
			assert.Contains(t, qErr.Error(), tt.wantErr)
		})
	}
}
