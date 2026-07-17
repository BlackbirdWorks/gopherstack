package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestRefinement1_HTTPEnableProactiveEngagement tests via HTTP.
func TestHandler_EnableProactiveEngagement(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "EnableProactiveEngagement", nil)
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPDisableProactiveEngagement tests via HTTP.
func TestHandler_DisableProactiveEngagement(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DisableProactiveEngagement", nil)
	assert.Equal(t, 200, rec.Code)
}
