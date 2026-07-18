package stepfunctions_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// mockSFNConfig implements config.Provider for testing.
type mockSFNConfig struct{}

func (m *mockSFNConfig) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "eu-west-1", 0, 0, false, 0)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns_step_functions",
			wantName: "StepFunctions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &stepfunctions.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   any
		wantName string
	}{
		{
			name:     "with_config",
			config:   &mockSFNConfig{},
			wantName: "StepFunctions",
		},
		{
			name:     "without_config",
			config:   nil,
			wantName: "StepFunctions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &stepfunctions.Provider{}
			ctx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantName, svc.Name())
		})
	}
}

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &stepfunctions.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, stepfunctions.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestProviderInit_WithJanitorCtx(t *testing.T) {
	t.Parallel()

	p := &stepfunctions.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}
