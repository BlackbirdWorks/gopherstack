package mediaconvert_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestProviderInit_NilCtx ensures Init returns ErrNilAppContext on nil context.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mediaconvert.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, mediaconvert.ErrNilAppContext)
}
