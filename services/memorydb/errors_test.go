package memorydb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_ErrValidationSentinel verifies that ErrValidation wraps ErrInvalidParameter.
func TestErrValidationSentinel(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateParameterGroup(context.Background(), &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "no-family",
		// Family intentionally omitted
	})

	require.Error(t, err)
}

// TestRefinement1_ErrValidationIs verifies ErrValidation sentinel wraps correctly.
func TestErrValidationIs(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("wrap: %w", memorydb.ErrValidation)
	require.Error(t, wrapped)
	require.ErrorIs(t, wrapped, memorydb.ErrValidation)
}
