package kinesis_test

import (
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConfigProvider implements config.Provider for tests.
type fakeConfigProvider struct{}

func (fakeConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "ap-southeast-2", 0, 0, false, 0)
}

// fakeContextConfig wraps fakeConfigProvider for service.AppContext.
type fakeContextConfig struct {
	fakeConfigProvider
}

func TestKinesisProvider_Name(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	assert.Equal(t, "Kinesis", p.Name())
}

func TestKinesisProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        any
		wantRegion    string
		wantAccountID string
	}{
		{
			name:          "WithConfig",
			config:        fakeContextConfig{},
			wantRegion:    "ap-southeast-2",
			wantAccountID: "111111111111",
		},
		{
			name:   "NoConfig",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesis.Provider{}
			ctx := &service.AppContext{
				Config: tt.config,
				Logger: slog.Default(),
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)

			if tt.wantRegion != "" {
				h, ok := svc.(*kinesis.Handler)
				require.True(t, ok)
				assert.Equal(t, tt.wantRegion, h.DefaultRegion)
				assert.Equal(t, tt.wantAccountID, h.AccountID)
			}
		})
	}
}

// TestProviderInit_NilCtx verifies ErrNilAppContext is returned for nil context.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, kinesis.ErrNilAppContext)
}

// TestErrNilAppContextValue verifies the sentinel error value.
func TestErrNilAppContextValue(t *testing.T) {
	t.Parallel()

	require.Error(t, kinesis.ErrNilAppContext)
	assert.Contains(t, kinesis.ErrNilAppContext.Error(), "kinesis")
}

// TestProviderInit_WithContext verifies Init with valid AppContext succeeds.
func TestProviderInit_WithContext(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	ctx := &service.AppContext{}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
