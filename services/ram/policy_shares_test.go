package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

const policyShareResourceARN = "arn:aws:glue:us-east-1:000000000000:database/db1"

func TestPutPolicyBasedShare(t *testing.T) {
	t.Parallel()

	t.Run("creates a CREATED_FROM_POLICY share with resource and principal associations", func(t *testing.T) {
		t.Parallel()

		backend := ram.NewInMemoryBackend("000000000000", "us-east-1")

		err := backend.PutPolicyBasedShare(
			policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
		)
		require.NoError(t, err)

		shares := backend.ListResourceShares("SELF", "")
		require.Len(t, shares, 1)
		share := shares[0]
		assert.Equal(t, "CREATED_FROM_POLICY", share.FeatureSet)
		assert.Equal(t, "000000000000", share.OwningAccountID)
		assert.Equal(t, "ACTIVE", share.Status)

		assocs := backend.GetResourceShareAssociations("", []string{share.ARN})
		var gotResource, gotPrincipal bool

		for _, a := range assocs {
			switch a.AssociationType {
			case "RESOURCE":
				assert.Equal(t, policyShareResourceARN, a.AssociatedEntity)
				gotResource = true
			case "PRINCIPAL":
				assert.Equal(t, "111122223333", a.AssociatedEntity)
				assert.True(t, a.External)
				gotPrincipal = true
			}
		}

		assert.True(t, gotResource, "resource association missing")
		assert.True(t, gotPrincipal, "principal association missing")

		perms := backend.ListResourceSharePermissions(share.ARN)
		require.Len(t, perms, 1)
		assert.Equal(t, "CREATED_FROM_POLICY", perms[0].Permission.PermissionType)

		pv, ok := perms[0].Permission.Versions[perms[0].Version]
		require.True(t, ok)
		assert.Contains(t, pv.PolicyTemplate, "glue:GetDatabase")
	})

	t.Run("second call resyncs principals and the permission instead of duplicating the share", func(t *testing.T) {
		t.Parallel()

		backend := ram.NewInMemoryBackend("000000000000", "us-east-1")

		require.NoError(t, backend.PutPolicyBasedShare(
			policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
		))
		require.NoError(t, backend.PutPolicyBasedShare(
			policyShareResourceARN, []string{"444455556666"}, []string{"glue:GetTable"},
		))

		shares := backend.ListResourceShares("SELF", "")
		require.Len(t, shares, 1, "must resync the existing share, not create a second one")

		assocs := backend.GetResourceShareAssociations("PRINCIPAL", []string{shares[0].ARN})
		status := make(map[string]string, len(assocs))

		for _, a := range assocs {
			status[a.AssociatedEntity] = a.Status
		}

		assert.Equal(t, "DISASSOCIATED", status["111122223333"])
		assert.Equal(t, "ASSOCIATED", status["444455556666"])

		perms := backend.ListResourceSharePermissions(shares[0].ARN)
		require.Len(t, perms, 1, "the derived permission must be updated in place, not duplicated")

		pv, ok := perms[0].Permission.Versions[perms[0].Version]
		require.True(t, ok)
		assert.Contains(t, pv.PolicyTemplate, "glue:GetTable")
		assert.NotContains(t, pv.PolicyTemplate, "glue:GetDatabase")
	})

	t.Run("does not touch a share already promoted to STANDARD", func(t *testing.T) {
		t.Parallel()

		backend := ram.NewInMemoryBackend("000000000000", "us-east-1")

		require.NoError(t, backend.PutPolicyBasedShare(
			policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
		))
		shares := backend.ListResourceShares("SELF", "")
		require.Len(t, shares, 1)

		_, err := backend.PromoteResourceShareCreatedFromPolicy(shares[0].ARN)
		require.NoError(t, err)

		require.NoError(t, backend.PutPolicyBasedShare(
			policyShareResourceARN, []string{"444455556666"}, []string{"glue:GetTable"},
		))

		shares = backend.ListResourceShares("SELF", "")
		require.Len(t, shares, 2, "a promoted share must not be resynced; a fresh policy share is created instead")
	})
}

func TestDeletePolicyBasedShare(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, backend.PutPolicyBasedShare(
		policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
	))

	require.NoError(t, backend.DeletePolicyBasedShare(policyShareResourceARN))

	shares := backend.ListResourceShares("SELF", "")
	assert.Empty(t, shares, "a deleted share must not appear in ListResourceShares")

	// Idempotent: nothing left to delete is a no-op, not an error.
	require.NoError(t, backend.DeletePolicyBasedShare(policyShareResourceARN))
}

// newPolicyShareForStateMachineTest seeds a fresh CREATED_FROM_POLICY share via
// PutPolicyBasedShare (the only real path that ever creates one) for state-machine
// enforcement tests below.
func newPolicyShareForStateMachineTest(t *testing.T) (*ram.InMemoryBackend, *ram.ResourceShare) {
	t.Helper()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.PutPolicyBasedShare(
		policyShareResourceARN, []string{"111122223333"}, []string{"glue:GetDatabase"},
	))

	shares := backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)

	return backend, shares[0]
}

func TestCreatedFromPolicyShare_StateMachine(t *testing.T) {
	t.Parallel()

	t.Run("update is rejected", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)

		_, err := backend.UpdateResourceShare(share.ARN, "new-name", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrOperationNotPermitted)
	})

	t.Run("associate is rejected", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)

		_, err := backend.AssociateResourceShare(
			share.ARN, nil, []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-1"},
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrInvalidStateTransition)
	})

	t.Run("disassociate is rejected", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)

		_, err := backend.DisassociateResourceShare(share.ARN, []string{"111122223333"}, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrInvalidStateTransition)
	})

	t.Run("associate permission is rejected", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)
		perms := backend.ListResourceSharePermissions(share.ARN)
		require.Len(t, perms, 1)

		err := backend.AssociateResourceSharePermission(share.ARN, perms[0].Permission.ARN, false, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrOperationNotPermitted)
	})

	t.Run("disassociate permission is rejected", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)
		perms := backend.ListResourceSharePermissions(share.ARN)
		require.Len(t, perms, 1)

		err := backend.DisassociateResourceSharePermission(share.ARN, perms[0].Permission.ARN)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrInvalidStateTransition)
	})

	t.Run("promote succeeds and lifts the restriction", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)

		promoted, err := backend.PromoteResourceShareCreatedFromPolicy(share.ARN)
		require.NoError(t, err)
		assert.Equal(t, "STANDARD", promoted.FeatureSet)

		_, err = backend.UpdateResourceShare(share.ARN, "renamed", nil)
		require.NoError(t, err, "a promoted share must be fully manageable again")
	})

	t.Run("promoting a non CREATED_FROM_POLICY share is rejected", func(t *testing.T) {
		t.Parallel()

		backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
		share, err := backend.CreateResourceShare("standard-share", false, nil, nil, nil)
		require.NoError(t, err)

		_, err = backend.PromoteResourceShareCreatedFromPolicy(share.ARN)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrInvalidStateTransition)
	})

	t.Run("promoting twice is rejected the second time", func(t *testing.T) {
		t.Parallel()

		backend, share := newPolicyShareForStateMachineTest(t)

		_, err := backend.PromoteResourceShareCreatedFromPolicy(share.ARN)
		require.NoError(t, err)

		_, err = backend.PromoteResourceShareCreatedFromPolicy(share.ARN)
		require.Error(t, err)
		assert.ErrorIs(t, err, ram.ErrInvalidStateTransition)
	})
}
