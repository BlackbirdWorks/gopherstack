package elb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestProviderInitNilCtx verifies that Init rejects a nil AppContext.
func TestProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &elb.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, elb.ErrNilAppContext)
}

// TestProviderInitValidCtx verifies Init succeeds with a valid context.
func TestProviderInitValidCtx(t *testing.T) {
	t.Parallel()

	p := &elb.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// TestProviderErrNilAppContext is a package-level sentinel check.
func TestProviderErrNilAppContext(t *testing.T) {
	t.Parallel()

	require.Error(t, elb.ErrNilAppContext)
}
