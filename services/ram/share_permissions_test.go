package ram_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRefinement1_DisassociateResourceSharePermission_Idempotent verifies that calling
// DisassociateResourceSharePermission on a non-associated permission is a no-op.
func TestDisassociateResourceSharePermission_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("dis-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Disassociate a permission that was never associated – should not error.
	err = h.Backend.DisassociateResourceSharePermission(rs.ARN, "arn:aws:ram:us-east-1:000000000000:permission/ghost")
	require.NoError(t, err)
}
