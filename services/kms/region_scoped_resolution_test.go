package kms_test

// region_scoped_resolution_test.go — regression tests for KeyId-ARN region resolution.
//
// Region-partitioned ops (GetKeyPolicy, PutKeyPolicy, CreateGrant, ListGrants,
// RevokeGrant, RetireGrant) previously resolved a bare/ARN KeyId's canonical UUID via
// resolveKeyID but then indexed their policiesStore/grantsRegion using the REQUEST
// region (getRegion(ctx)), discarding the region resolveKeyID returned for an ARN. So a
// KeyId ARN carrying a different embedded region than the request (e.g. addressing a
// multi-region replica in us-west-2 while the client's default region is us-east-1)
// resolved the UUID correctly but looked in the wrong region's store -> NotFoundException.
//
// DescribeKey/Encrypt/Decrypt never had this bug because they route through lookupKey,
// which honors the ARN's own region. These tests assert every fixed op now agrees with
// that behavior: a replica's full ARN resolves against the replica's region regardless of
// the request region.
//
// Setup models exactly the cross-region shape a real client hits: a multi-region primary
// in the backend default region (us-east-1) replicated into us-west-2, then every op is
// driven with the replica's us-west-2 ARN while ctx carries no region override (so it
// defaults to us-east-1 -- the mismatch that used to break).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// setupCrossRegionReplica creates a multi-region primary in us-east-1 and a replica in
// us-west-2, returning a fresh backend plus the replica's full ARN.
func setupCrossRegionReplica(t *testing.T) (*kms.InMemoryBackend, string) {
	t.Helper()

	ctx := context.Background()
	b := kms.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	primary, err := b.CreateKey(ctx, &kms.CreateKeyInput{MultiRegion: true})
	require.NoError(t, err)

	replica, err := b.ReplicateKey(ctx, &kms.ReplicateKeyInput{
		KeyID:         primary.KeyMetadata.KeyID,
		ReplicaRegion: "us-west-2",
	})
	require.NoError(t, err)

	replicaARN := replica.ReplicaKeyMetadata.Arn
	require.Contains(t, replicaARN, "us-west-2", "replica ARN must carry the replica region")

	return b, replicaARN
}

func TestCrossRegionARN_PutGetKeyPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[{"Sid":"X","Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::000000000000:root"},"Action":"kms:*","Resource":"*"}]}`

	ctx := context.Background()
	b, replicaARN := setupCrossRegionReplica(t)

	// PutKeyPolicy against the replica's us-west-2 ARN while ctx defaults to us-east-1.
	require.NoError(t, b.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
		KeyID:      replicaARN,
		PolicyName: "default",
		Policy:     policy,
	}))

	// GetKeyPolicy addressing the same ARN must read back exactly what we wrote,
	// not the synthesized default (which is what a wrong-region lookup would yield).
	got, err := b.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyID: replicaARN})
	require.NoError(t, err)
	assert.JSONEq(t, policy, got.Policy)
}

func TestCrossRegionARN_GrantLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retire func(t *testing.T, b *kms.InMemoryBackend, replicaARN, grantID, grantToken string) error
		name   string
	}{
		{
			name: "RevokeGrant by ARN + GrantId",
			retire: func(_ *testing.T, b *kms.InMemoryBackend, replicaARN, grantID, _ string) error {
				return b.RevokeGrant(context.Background(), &kms.RevokeGrantInput{
					KeyID:   replicaARN,
					GrantID: grantID,
				})
			},
		},
		{
			name: "RetireGrant by ARN + GrantId",
			retire: func(_ *testing.T, b *kms.InMemoryBackend, replicaARN, grantID, _ string) error {
				return b.RetireGrant(context.Background(), &kms.RetireGrantInput{
					KeyID:   replicaARN,
					GrantID: grantID,
				})
			},
		},
		{
			name: "RetireGrant by GrantToken (no region hint, searches all regions)",
			retire: func(_ *testing.T, b *kms.InMemoryBackend, _, _, grantToken string) error {
				return b.RetireGrant(context.Background(), &kms.RetireGrantInput{
					GrantToken: grantToken,
				})
			},
		},
		{
			name: "RetireGrant by GrantId only (no region hint, searches all regions)",
			retire: func(_ *testing.T, b *kms.InMemoryBackend, _, grantID, _ string) error {
				return b.RetireGrant(context.Background(), &kms.RetireGrantInput{
					GrantID: grantID,
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b, replicaARN := setupCrossRegionReplica(t)

			// CreateGrant against the replica's us-west-2 ARN -> grant stored in us-west-2.
			grant, err := b.CreateGrant(ctx, &kms.CreateGrantInput{
				KeyID:            replicaARN,
				GranteePrincipal: "arn:aws:iam::000000000000:role/app",
				Operations:       []string{"Encrypt", "Decrypt"},
			})
			require.NoError(t, err)
			require.NotEmpty(t, grant.GrantID)
			require.NotEmpty(t, grant.GrantToken)

			// ListGrants addressing the same ARN must find the grant (wrong-region
			// lookup would return an empty list, the perpetual-drift symptom).
			listed, err := b.ListGrants(ctx, &kms.ListGrantsInput{KeyID: replicaARN})
			require.NoError(t, err)
			require.Len(t, listed.Grants, 1)
			assert.Equal(t, grant.GrantID, listed.Grants[0].GrantID)

			// Retire/revoke it via the case-specific path.
			require.NoError(t, tc.retire(t, b, replicaARN, grant.GrantID, grant.GrantToken))

			// Grant is gone.
			after, err := b.ListGrants(ctx, &kms.ListGrantsInput{KeyID: replicaARN})
			require.NoError(t, err)
			assert.Empty(t, after.Grants)
		})
	}
}
