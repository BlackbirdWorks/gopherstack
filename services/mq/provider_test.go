package mq_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &mq.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, mq.ErrNilAppContext)
}
