package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CancelAndCommitTransaction_Idempotent(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	// Cancel twice is fine
	require.NoError(t, b.CancelTransaction("txn-idem-1"))
	require.NoError(t, b.CancelTransaction("txn-idem-1"))

	// Commit twice is fine
	status, err := b.CommitTransaction("txn-idem-2")
	require.NoError(t, err)
	assert.Equal(t, "COMMITTED", status)

	status2, err2 := b.CommitTransaction("txn-idem-2")
	require.NoError(t, err2)
	assert.Equal(t, "COMMITTED", status2)
}
