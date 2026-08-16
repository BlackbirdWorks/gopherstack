package mwaa_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, mwaa.ErrNilAppContext)
}

func TestProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProviderName(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	assert.Equal(t, "MWAA", p.Name())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &mwaa.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "MWAA", svc.Name())
}
