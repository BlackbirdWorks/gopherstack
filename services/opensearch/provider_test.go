package opensearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &opensearch.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrNilAppContext)
}
