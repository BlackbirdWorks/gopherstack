package eks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestEKS_FullStatePersistenceRoundTrip is the Phase 3.3 (pkgs/store
// conversion) full-state Snapshot->Restore round-trip test: it populates one
// instance of every store.Table-backed resource (see registerAllTables in
// store_setup.go) plus both plain-map fields left un-converted
// (accessPolicies, encryptionConfigs), snapshots the backend, restores into a
// fresh backend, and verifies every resource -- including the ones no
// pre-existing persistence test exercised together (identity provider
// configs, cluster update records, and access-policy associations) -- is
// still present with its identifying fields intact.
func TestEKS_FullStatePersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateNodegroup(
		"c1", "ng1", "", "", "", "", "",
		nil, 1, 1, 2, eks.NodegroupInput{}, map[string]string{"team": "platform"},
	)
	require.NoError(t, err)

	principalARN := "arn:aws:iam::123456789012:role/r1"

	_, err = b.CreateAccessEntry("c1", principalARN, "STANDARD", "", nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAccessPolicy(
		"c1", principalARN, "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy", nil,
	)
	require.NoError(t, err)

	_, err = b.AssociateEncryptionConfig("c1", []eks.EncryptionConfig{
		{
			Provider:  map[string]string{"keyArn": "arn:aws:kms:us-east-1:123456789012:key/abc"},
			Resources: []string{"secrets"},
		},
	})
	require.NoError(t, err)

	_, err = b.AssociateIdentityProviderConfig("c1", "oidc", "idp1", map[string]string{"issuerUrl": "https://x"}, nil)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "vpc-cni", "", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123456789012:role/fp", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePodIdentityAssociation("c1", "default", "sa1", "arn:aws:iam::123456789012:role/sa", nil)
	require.NoError(t, err)

	_, err = b.CreateCapability("cap1", "v1.0")
	require.NoError(t, err)

	_, err = b.CreateEksAnywhereSubscription("sub1", 3, "Cluster", nil)
	require.NoError(t, err)

	_, err = b.UpdateClusterVersion("c1", "1.33")
	require.NoError(t, err)

	// Snapshot and restore into a brand new backend.
	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c, err := b2.DescribeCluster("c1")
	require.NoError(t, err)
	assert.Equal(t, "test", c.Tags.Clone()["env"])
	require.Len(t, c.EncryptionConfig, 1)
	assert.Equal(t, []string{"secrets"}, c.EncryptionConfig[0].Resources)

	ng, err := b2.DescribeNodegroup("c1", "ng1")
	require.NoError(t, err)
	assert.Equal(t, "platform", ng.Tags.Clone()["team"])

	entry, err := b2.DescribeAccessEntry("c1", principalARN)
	require.NoError(t, err)
	assert.Equal(t, principalARN, entry.PrincipalARN)

	policies, err := b2.ListAssociatedAccessPolicies("c1", principalARN)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy", policies[0].PolicyARN)

	idpCfg, err := b2.DescribeIdentityProviderConfig("c1", "idp1")
	require.NoError(t, err)
	assert.Equal(t, "oidc", idpCfg.Type)

	addon, err := b2.DescribeAddon("c1", "vpc-cni")
	require.NoError(t, err)
	assert.Equal(t, "vpc-cni", addon.AddonName)

	fp, err := b2.DescribeFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, "fp1", fp.FargateProfileName)

	podAssocs, err := b2.ListPodIdentityAssociations("c1")
	require.NoError(t, err)
	require.Len(t, podAssocs, 1)
	assert.Equal(t, "sa1", podAssocs[0].ServiceAccount)

	capa, err := b2.DescribeCapability("cap1")
	require.NoError(t, err)
	assert.Equal(t, "v1.0", capa.Version)

	subs := b2.ListEksAnywhereSubscriptions()
	require.Len(t, subs, 1)
	assert.Equal(t, "sub1", subs[0].Name)

	updateIDs, err := b2.ListUpdates("c1")
	require.NoError(t, err)
	require.Len(t, updateIDs, 1)

	upd, err := b2.DescribeUpdate("c1", updateIDs[0])
	require.NoError(t, err)
	assert.Equal(t, "VersionUpdate", upd.Type)

	assert.Equal(t, 1, b2.ClusterCount())
	assert.Equal(t, 1, b2.NodegroupCount())
	assert.Equal(t, 1, b2.AccessEntryCount())
	assert.Equal(t, 1, b2.AddonCount())
	assert.Equal(t, 1, b2.FargateProfileCount())
	assert.Equal(t, 1, b2.PodIdentityAssociationCount())
	assert.Equal(t, 1, b2.CapabilityCount())
	assert.Equal(t, 1, b2.SubscriptionCount())
}

// TestEKS_SnapshotVersionGuard verifies that Restore discards a snapshot
// whose version does not match the current backendSnapshot shape, resetting
// to an empty backend rather than partially decoding incompatible data.
func TestEKS_SnapshotVersionGuard(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// A malformed/incompatible snapshot (missing the "version" field entirely,
	// as a pre-Phase-3.3 snapshot would be) must be discarded rather than
	// partially applied.
	err = b.Restore(t.Context(), []byte(`{"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ClusterCount())
}
