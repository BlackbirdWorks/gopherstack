package iam_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIAMProvider_InitWithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config any
		name   string
	}{
		{
			name:   "nil_config_uses_defaults",
			config: nil,
		},
		{
			name:   "with_config_provider",
			config: &mockIAMConfig{accountID: "123456789012"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iam.Provider{}
			appCtx := &service.AppContext{
				Logger: logger.NewTestLogger(),
				Config: tt.config,
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
		})
	}
}

type mockIAMConfig struct {
	accountID string
}

func (m *mockIAMConfig) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig(m.accountID, "us-east-1", 0, 0, false, 0)
}

// TestIAMProvider covers Provider.Name() and Provider.Init().
func TestIAMProvider(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		p := &iam.Provider{}
		assert.Equal(t, "IAM", p.Name())
	})

	t.Run("Init", func(t *testing.T) {
		t.Parallel()
		p := &iam.Provider{}
		appCtx := &service.AppContext{Logger: logger.NewTestLogger()}
		svc, err := p.Init(appCtx)
		require.NoError(t, err)
		require.NotNil(t, svc)
		h, ok := svc.(*iam.Handler)
		require.True(t, ok)
		assert.NotNil(t, h.Backend)
	})
}
