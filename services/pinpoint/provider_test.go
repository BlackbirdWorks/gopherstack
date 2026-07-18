package pinpoint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestBackendRegionAccountID(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("eu-west-1", "999888777666")
	assert.Equal(t, "eu-west-1", b.Region())
	assert.Equal(t, "999888777666", b.AccountID())
}

// ──────────────────────────────────────────────────
// ErrNilAppContext
// ──────────────────────────────────────────────────

func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &pinpoint.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, pinpoint.ErrNilAppContext)
}

// ──────────────────────────────────────────────────
// ARN index / tag operations on all resource types
// ──────────────────────────────────────────────────
