package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestBackend_AssociateDRTLogBucket tests backend DRT log bucket association.
func TestBackend_AssociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		buckets []string
		wantErr bool
	}{
		{
			name:    "single bucket",
			buckets: []string{"my-logs"},
		},
		{
			name:    "two distinct buckets",
			buckets: []string{"bucket-a", "bucket-b"},
		},
		{
			name:    "empty bucket returns error",
			buckets: []string{""},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())

			for _, bucket := range tt.buckets {
				err := b.AssociateDRTLogBucket(bucket)

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
			}

			access := b.DescribeDRTAccess()
			assert.Len(t, access.LogBucketList, len(tt.buckets))
		})
	}
}

// TestBackend_AssociateDRTRole tests backend DRT role association.
func TestBackend_AssociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		wantErr bool
	}{
		{
			name:    "success",
			roleARN: "arn:aws:iam::123:role/DRTRole",
		},
		{
			name:    "empty role arn returns error",
			roleARN: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())
			err := b.AssociateDRTRole(tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			access := b.DescribeDRTAccess()
			assert.Equal(t, tt.roleARN, access.RoleArn)
		})
	}
}

// TestAudit_Gap17_DisassociateDRTRoleIdempotent verifies no error when no role.
func TestInMemoryBackend_DisassociateDRTRoleIdempotent(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DisassociateDRTRole()
	require.NoError(t, err, "DisassociateDRTRole must be idempotent when no role is set")
}

// TestAudit_Gap17_DisassociateDRTRoleAfterAssociate verifies correct role removal.
func TestInMemoryBackend_DisassociateDRTRoleAfterAssociate(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
	require.NoError(t, b.DisassociateDRTRole())
	assert.Empty(t, b.DescribeDRTAccess().RoleArn)
}

// --- Gap 18: AssociateProactiveEngagementDetails sets PENDING status ---

// TestRefinement1_DisassociateDRTLogBucket tests DRT bucket disassociation.
func TestInMemoryBackend_DisassociateDRTLogBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		bucket  string
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateDRTLogBucket("my-bucket"))
			},
			bucket: "my-bucket",
		},
		{
			name: "bucket_not_associated",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
			},
			bucket:  "no-such-bucket",
			wantErr: true,
		},
		{
			name:    "no_drt_access_at_all",
			setup:   func(*shield.InMemoryBackend) {},
			bucket:  "my-bucket",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.DisassociateDRTLogBucket(tt.bucket)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			access := b.DescribeDRTAccess()
			assert.NotContains(t, access.LogBucketList, tt.bucket)
		})
	}
}

// TestRefinement1_DisassociateDRTRole tests DRT role disassociation.
func TestInMemoryBackend_DisassociateDRTRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				require.NoError(t, b.CreateSubscription())
				require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
			},
		},
		{
			name:    "no_role_associated_idempotent",
			setup:   func(*shield.InMemoryBackend) {},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.DisassociateDRTRole()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, b.DescribeDRTAccess().RoleArn)
		})
	}
}
