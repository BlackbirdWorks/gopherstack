package redshift_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

type mockRedshiftConfig struct{}

func (m *mockRedshiftConfig) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0)
}

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   any
		wantName string
	}{
		{
			name:     "name_returns_redshift",
			wantName: "Redshift",
		},
		{
			name:     "init_without_config",
			config:   nil,
			wantName: "Redshift",
		},
		{
			name:     "init_with_config",
			config:   &mockRedshiftConfig{},
			wantName: "Redshift",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &redshift.Provider{}
			assert.Equal(t, tt.wantName, p.Name())

			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
		})
	}
}

// ---- ErrNilAppContext ----

func TestProvider_NilAppContextReturnsError(t *testing.T) {
	t.Parallel()

	p := &redshift.Provider{}
	svc, err := p.Init(nil)

	assert.Nil(t, svc)
	require.Error(t, err)
	assert.ErrorIs(t, err, redshift.ErrNilAppContext)
}
