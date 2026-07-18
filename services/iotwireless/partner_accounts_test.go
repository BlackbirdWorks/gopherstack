package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_PartnerAccountARN_UsesRegion verifies that partner account ARN uses the handler region.
func TestInMemoryBackend_PartnerAccountARN_UsesRegion(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()
	arn, err := b.AssociateAwsAccountWithPartnerAccount(testAccountID, testRegion, "partner-001", nil)
	require.NoError(t, err)
	assert.Contains(t, arn, testRegion, "ARN should include the handler region")
	assert.Contains(t, arn, "partner-001")
}
