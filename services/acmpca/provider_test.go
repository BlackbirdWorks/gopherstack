package acmpca_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestACMPCAProvider_Name(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	assert.Equal(t, "ACMPCA", p.Name())
}

func TestACMPCAProvider_Init_WithEmptyCtx(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestACMPCAProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &acmpca.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}
