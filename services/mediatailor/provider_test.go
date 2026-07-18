package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &mediatailor.Provider{}
	assert.Equal(t, "MediaTailor", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &mediatailor.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, mediatailor.ErrNilAppContext)
}
