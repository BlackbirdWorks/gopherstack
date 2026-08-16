package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestModifyVerifiedAccessGroup(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	inst, err := b.CreateVerifiedAccessInstance("orig instance")
	require.NoError(t, err)
	other, err := b.CreateVerifiedAccessInstance("other instance")
	require.NoError(t, err)
	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessGroup(grp.VerifiedAccessGroupID, other.VerifiedAccessInstanceID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)
	assert.Equal(t, other.VerifiedAccessInstanceID, updated.VerifiedAccessInstanceID)

	// Mutation is real: a fresh describe reflects it.
	described := b.DescribeVerifiedAccessGroups([]string{grp.VerifiedAccessGroupID})
	require.Len(t, described, 1)
	assert.Equal(t, "updated", described[0].Description)

	_, err = b.ModifyVerifiedAccessGroup("vagr-missing", "", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessGroupNotFound)

	_, err = b.ModifyVerifiedAccessGroup(grp.VerifiedAccessGroupID, "vai-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessInstanceNotFound)
}

func TestModifyVerifiedAccessInstance(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	inst, err := b.CreateVerifiedAccessInstance("orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessInstance(inst.VerifiedAccessInstanceID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	_, err = b.ModifyVerifiedAccessInstance("vai-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessInstanceNotFound)
}

func TestModifyVerifiedAccessTrustProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tp, err := b.CreateVerifiedAccessTrustProvider("user", "orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessTrustProvider(tp.VerifiedAccessTrustProviderID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	_, err = b.ModifyVerifiedAccessTrustProvider("vatp-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessTrustProviderNF)
}

// ---- Transit Gateway route propagation + unified attachment describe ----
