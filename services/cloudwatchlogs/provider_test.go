package cloudwatchlogs_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestHandler_Provider_NilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudwatchlogs.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

// mockLogsConfigProvider implements config.Provider for testing.
type mockLogsConfigProvider struct{}

func (m *mockLogsConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "eu-west-1", 0, 0, false, 0)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cloudwatchlogs.Provider{}
	assert.Equal(t, "CloudWatchLogs", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   config.Provider
		wantName string
	}{
		{
			name:     "WithConfig",
			config:   &mockLogsConfigProvider{},
			wantName: "CloudWatchLogs",
		},
		{
			name: "WithoutConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudwatchlogs.Provider{}
			ctx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, svc.Name())
			}
		})
	}
}
