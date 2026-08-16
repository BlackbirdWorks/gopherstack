package eventbridge_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "eu-west-1", 0, 0, false, 0)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns EventBridge",
			wantName: "EventBridge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &eventbridge.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      *service.AppContext
		wantName string
	}{
		{
			name: "with config",
			ctx: &service.AppContext{
				Logger: slog.Default(),
				Config: &mockConfigProvider{},
			},
			wantName: "EventBridge",
		},
		{
			name: "without config",
			ctx:  &service.AppContext{Logger: slog.Default()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &eventbridge.Provider{}
			svc, err := p.Init(tt.ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, svc.Name())
			}
		})
	}
}

// TestProviderInit_NilCtx verifies ErrNilAppContext is returned when ctx is nil.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &eventbridge.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, eventbridge.ErrNilAppContext)
}
