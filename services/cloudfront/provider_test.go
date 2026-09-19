package cloudfront_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestProviderInitNilCtx verifies Provider.Init handles nil context.
func TestProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudfront.Provider{}
	handler, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, handler)

	if shutdowner, ok := handler.(service.Shutdowner); ok {
		t.Cleanup(func() { shutdowner.Shutdown(context.Background()) })
	}
}
