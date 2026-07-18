package ssoadmin_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestProviderNilContext verifies that Init returns ErrNilAppContext on nil input.
func TestProviderNilContext(t *testing.T) {
	t.Parallel()

	p := ssoadmin.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ssoadmin.ErrNilAppContext)
}
