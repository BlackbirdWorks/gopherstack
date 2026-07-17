package support_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestSupport_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	assert.Equal(t, "Support", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestSupport_Provider_Init_WithJanitorCtx(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

func TestSupport_Provider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &support.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, support.ErrNilAppContext)
}
