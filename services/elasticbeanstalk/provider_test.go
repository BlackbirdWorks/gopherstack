package elasticbeanstalk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// TestProvider_Init_NilAppContext verifies that Provider.Init returns ErrNilAppContext.
func TestProvider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &elasticbeanstalk.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, elasticbeanstalk.ErrNilAppContext)
}
