package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ec2.InMemoryBackend) string
		verify func(t *testing.T, b *ec2.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ec2.InMemoryBackend) string {
				sg, err := b.CreateSecurityGroup("test-sg", "test security group", "")
				if err != nil {
					return ""
				}

				return sg.ID
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()

				sgs := b.DescribeSecurityGroups([]string{id})
				require.Len(t, sgs, 1)
				assert.Equal(t, "test-sg", sgs[0].Name)
				assert.Equal(t, id, sgs[0].ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ec2.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ec2.InMemoryBackend, _ string) {
				t.Helper()

				sgs := b.DescribeSecurityGroups(nil)
				// Default security groups may exist from initDefaults; just verify restore worked
				assert.NotNil(t, sgs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
