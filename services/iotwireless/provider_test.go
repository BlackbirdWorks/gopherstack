package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestProvider_Init_NilCtx verifies that Provider.Init returns ErrNilAppContext for nil ctx.
func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &iotwireless.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrNilAppContext)
}
