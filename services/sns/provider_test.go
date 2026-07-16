package sns_test

import (
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProviderInit verifies the SNS provider initializes correctly.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	assert.Equal(t, "SNS", p.Name())

	log := logger.NewLogger(slog.LevelDebug)
	ctx := &service.AppContext{Logger: log}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "SNS", reg.Name())
}

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestProviderInitNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestProviderInitWithJanitorContext(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}
