package kms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, kms.ErrNilAppContext)
}

// TestKMSProvider verifies the Provider.
func TestKMSProvider(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	assert.Equal(t, "KMS", p.Name())

	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("123456789012", "ap-southeast-2", 0, 0, false, 0)
}

// TestKMSProviderWithConfig verifies that Init uses config.Provider when available.
func TestKMSProviderWithConfig(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockConfigProvider{},
	}

	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
