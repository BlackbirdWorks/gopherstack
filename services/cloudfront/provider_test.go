package cloudfront_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestRefinement1_ProviderInitNilCtx verifies Provider.Init handles nil context.
func TestRefinement1_ProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudfront.Provider{}
	handler, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, handler)
}
