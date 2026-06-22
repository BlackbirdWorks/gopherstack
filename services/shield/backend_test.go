package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *shield.InMemoryBackend) string
		verify func(t *testing.T, b *shield.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "protection_round_trip",
			setup: func(b *shield.InMemoryBackend) string {
				require.NoError(t, b.CreateSubscription())

				const webARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/web"

				p, err := b.CreateProtection("web-app", webARN, nil)
				require.NoError(t, err)

				return p.ID
			},
			verify: func(t *testing.T, b *shield.InMemoryBackend, id string) {
				t.Helper()

				const webARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/web"

				p, err := b.DescribeProtection(id, "")
				require.NoError(t, err)
				assert.Equal(t, "web-app", p.Name)
				assert.Equal(t, id, p.ID)

				// Indexes must be rebuilt.
				p2, err := b.DescribeProtection("", webARN)
				require.NoError(t, err)
				assert.Equal(t, id, p2.ID)
			},
		},
		{
			name: "subscription_round_trip",
			setup: func(b *shield.InMemoryBackend) string {
				require.NoError(t, b.CreateSubscription())

				return ""
			},
			verify: func(t *testing.T, b *shield.InMemoryBackend, _ string) {
				t.Helper()

				sub, err := b.DescribeSubscription()
				require.NoError(t, err)
				assert.Equal(t, "ENABLED", sub.AutoRenew)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *shield.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *shield.InMemoryBackend, _ string) {
				t.Helper()

				assert.Empty(t, b.ListProtections())
				assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := shield.NewInMemoryBackend(testAccountID, testRegion)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := shield.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_ProtectionIndexConsistency(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b.CreateSubscription())

	const resourceARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app"

	p, err := b.CreateProtection("app-protection", resourceARN, nil)
	require.NoError(t, err)

	// Lookup by ARN works via index.
	byARN, err := b.DescribeProtection("", resourceARN)
	require.NoError(t, err)
	assert.Equal(t, p.ID, byARN.ID)

	// Duplicate name rejected.
	const otherARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/other"

	_, err = b.CreateProtection("app-protection", otherARN, nil)
	require.Error(t, err)

	// Duplicate resource ARN rejected.
	_, err = b.CreateProtection("other-name", resourceARN, nil)
	require.Error(t, err)

	// Delete cleans up indexes.
	require.NoError(t, b.DeleteProtection(p.ID))

	// Can now create with same name/ARN.
	_, err = b.CreateProtection("app-protection", resourceARN, nil)
	require.NoError(t, err)
}

func TestShieldHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := shield.NewInMemoryBackend(testAccountID, testRegion)
	h := shield.NewHandler(backend)

	require.NoError(t, backend.CreateSubscription())

	const testARN = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/t"

	_, err := backend.CreateProtection("test", testARN, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := shield.NewInMemoryBackend(testAccountID, testRegion)
	freshH := shield.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	assert.Len(t, fresh.ListProtections(), 1)
}
