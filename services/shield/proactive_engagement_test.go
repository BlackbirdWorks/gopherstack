package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestAudit_Gap15_EnableProactiveEngagementRequiresContacts verifies contact requirement.
func TestInMemoryBackend_EnableProactiveEngagementRequiresContacts(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	err := b.EnableProactiveEngagement()
	require.Error(t, err, "EnableProactiveEngagement must fail when no contacts configured")
}

// TestAudit_Gap15_EnableProactiveEngagementSucceedsWithContacts verifies success with contacts.
func TestInMemoryBackend_EnableProactiveEngagementSucceedsWithContacts(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "cirt@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))
}

// --- Gap 16: UpdateEmergencyContactSettings validation ---

// TestAudit_Gap18_AssociateProactiveEngagementSetsPending verifies PENDING transition.
func TestInMemoryBackend_AssociateProactiveEngagementSetsPending(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	// Starts DISABLED after CreateSubscription.
	assert.Equal(t, shield.ProactiveEngagementDisabled, shield.GetProactiveEngagementStatus(b))

	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	assert.Equal(t, shield.ProactiveEngagementPending, shield.GetProactiveEngagementStatus(b))
}

// TestAudit_Gap18_AssociateProactiveEngagementDoesNotOverwriteEnabled verifies no regression.
func TestInMemoryBackend_AssociateProactiveEngagementDoesNotOverwriteEnabled(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))

	// Re-associate should not revert ENABLED back to PENDING.
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "other@example.com"},
	}))
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))
}

// --- Gap 19: AssociateHealthCheck validates Route53 ARN ---

// TestRefinement1_EnableDisableProactiveEngagement tests proactive engagement ops.
func TestInMemoryBackend_EnableDisableProactiveEngagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		action  func(*shield.InMemoryBackend) error
		name    string
		wantStr string
		wantErr bool
	}{
		{
			name:  "enable_requires_subscription",
			setup: func(*shield.InMemoryBackend) {},
			action: func(b *shield.InMemoryBackend) error {
				return b.EnableProactiveEngagement()
			},
			wantErr: true,
		},
		{
			name: "enable_with_subscription",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
					{EmailAddress: "sec@example.com"},
				}))
			},
			action: func(b *shield.InMemoryBackend) error {
				return b.EnableProactiveEngagement()
			},
			wantStr: shield.ProactiveEngagementEnabled,
		},
		{
			name: "disable_with_subscription",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
					{EmailAddress: "sec@example.com"},
				}))
				require.NoError(t, b.EnableProactiveEngagement())
			},
			action: func(b *shield.InMemoryBackend) error {
				return b.DisableProactiveEngagement()
			},
			wantStr: shield.ProactiveEngagementDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := tt.action(b)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStr, shield.GetProactiveEngagementStatus(b))
		})
	}
}
