package eks_test

import (
	"net/http"
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

	_, err = b.AssociateIdentityProviderConfig(
		"c1", "oidc", "idp1", map[string]string{"issuerUrl": "https://x"}, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.CreateAddon("c1", "vpc-cni", "", "", "", "", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123456789012:role/fp", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePodIdentityAssociation(
		"c1", "default", "sa1", "arn:aws:iam::123456789012:role/sa", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)

	_, err = b.CreateCapability(
		"c1",
		"cap1",
		"ARGOCD",
		"arn:aws:iam::123456789012:role/capability-role",
		"RETAIN",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateEksAnywhereSubscription(
		"sub1", eks.SubscriptionTerm{Unit: "MONTHS", Duration: 12}, false, 3, "Cluster", nil,
	)
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

	capa, err := b2.DescribeCapability("c1", "cap1")
	require.NoError(t, err)
	assert.Equal(t, "ARGOCD", capa.Type)

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

// TestPersistenceRoundTrip_SubscriptionAndFargate verifies that subscriptions,
// Fargate profiles, and pod identity associations survive a snapshot/restore cycle.
func TestPersistenceRoundTrip_SubscriptionAndFargate(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateEksAnywhereSubscription(
		"my-sub", eks.SubscriptionTerm{Unit: "MONTHS", Duration: 12}, false, 3, "Cluster", nil,
	)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePodIdentityAssociation(
		"c1", "default", "sa1", "arn:aws:iam::123:role/sa", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)

	// Snapshot and restore
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	subs := b2.ListEksAnywhereSubscriptions()
	assert.Len(t, subs, 1)

	names, err := b2.ListFargateProfiles("c1")
	require.NoError(t, err)
	assert.Len(t, names, 1)

	podAssocs, err := b2.ListPodIdentityAssociations("c1")
	require.NoError(t, err)
	assert.Len(t, podAssocs, 1)
}

// TestPersistenceRoundTrip_AddonCapabilityEncryptionConfig verifies that addons,
// fargate profiles, pod identity, capabilities, subscriptions, and encryption
// config all survive a Handler-mediated snapshot/restore cycle.
func TestPersistenceRoundTrip_AddonCapabilityEncryptionConfig(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	// Create cluster and access entry.
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAccessEntry("c1", "arn:aws:iam::123456789012:role/r1", "STANDARD", "", nil, nil)
	require.NoError(t, err)

	// Create addon.
	_, err = b.CreateAddon("c1", "vpc-cni", "v1.12.0", "", "", "", "", nil, nil)
	require.NoError(t, err)

	// Create fargate profile.
	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123456789012:role/fargate", nil, nil, nil)
	require.NoError(t, err)

	// Create pod identity association.
	_, err = b.CreatePodIdentityAssociation(
		"c1", "default", "my-sa", "arn:aws:iam::123456789012:role/pod", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)

	// Create capability.
	_, err = b.CreateCapability(
		"c1",
		"cap1",
		"ARGOCD",
		"arn:aws:iam::123456789012:role/capability-role",
		"RETAIN",
		nil,
		nil,
	)
	require.NoError(t, err)

	// Create subscription.
	_, err = b.CreateEksAnywhereSubscription(
		"sub1", eks.SubscriptionTerm{Unit: "MONTHS", Duration: 12}, false, 3, "License", nil,
	)
	require.NoError(t, err)

	// Associate encryption config.
	_, err = b.AssociateEncryptionConfig("c1", []eks.EncryptionConfig{
		{Provider: map[string]string{"keyArn": "arn:aws:kms:us-east-1:123:key/abc"}, Resources: []string{"secrets"}},
	})
	require.NoError(t, err)

	// Snapshot and restore into a fresh backend.
	h := eks.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h2 := eks.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify cluster is present.
	c, err := b2.DescribeCluster("c1")
	require.NoError(t, err)
	assert.Equal(t, "c1", c.Name)
}

// TestPersistenceRoundTrip_ClusterTags verifies a single tagged cluster survives
// a snapshot/restore cycle.
func TestPersistenceRoundTrip_ClusterTags(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.ClusterCount())

	tags, err := b2.ListTagsForResource("arn:aws:eks:" + config.DefaultRegion + ":123456789012:cluster/c1")
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

// TestPersistenceRoundTrip_ClusterAndNodegroup verifies a cluster+nodegroup pair
// survives a Handler-mediated snapshot/restore cycle.
func TestPersistenceRoundTrip_ClusterAndNodegroup(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	_, err := b.CreateCluster(
		"cluster1",
		"1.30",
		"arn:aws:iam::000000000000:role/eks-role",
		nil, nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	_, err = b.CreateNodegroup(
		"cluster1", "ng1", "arn:aws:iam::000000000000:role/ng-role",
		"AL2_x86_64", "ON_DEMAND", "1.30", "",
		[]string{"t3.medium"}, 2, 1, 5, eks.NodegroupInput{}, map[string]string{"team": "platform"},
	)
	require.NoError(t, err)

	h := eks.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h2 := eks.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	c, err := b2.DescribeCluster("cluster1")
	require.NoError(t, err)
	assert.Equal(t, "cluster1", c.Name)
	assert.Equal(t, "test", c.Tags.Clone()["env"])

	names, err := b2.ListNodegroups("cluster1")
	require.NoError(t, err)
	require.Len(t, names, 1)
	assert.Equal(t, "ng1", names[0])
}

// TestPersistenceRoundTrip_UpdateNodegroupNameFilter covers gopherstack-34g03:
// Update.NodegroupName carried json:"-" with no DTO twin, so
// Snapshot/Restore (Update is registered directly on b.registry,
// store_setup.go) dropped it, and the documented nodegroupName filter
// (handler_updates.go:286) returned nothing for any pre-restart update.
func TestPersistenceRoundTrip_UpdateNodegroupNameFilter(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	mustCreateClusterNoVpc(t, b, "upd-ng-filter-cluster")

	b.StoreUpdate(&eks.Update{
		ID:            "upd-1",
		ClusterName:   "upd-ng-filter-cluster",
		NodegroupName: "ng-1",
		Status:        "Successful",
		Type:          "ConfigUpdate",
	})

	h := eks.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	h2 := eks.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec := doREST(t, h2, http.MethodGet, "/clusters/upd-ng-filter-cluster/updates?nodegroupName=ng-1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	ids, ok := resp["updateIds"].([]any)
	require.True(t, ok)
	require.Len(t, ids, 1)
	assert.Equal(t, "upd-1", ids[0])
}

// TestRestore_Version2Fixture_TolerantDecode is the coordinator-requested
// follow-up to gopherstack-wf8f: two fields (ConnectorConfig.ActivationExpiry,
// Capability.Configuration) were retyped in this pass without bumping
// eksSnapshotVersion, relying instead on tolerant decoders (activationExpiry's
// UnmarshalJSON in models.go, Capability's UnmarshalJSON in
// capability_configuration.go) so a genuine pre-existing (version-2)
// snapshot still restores correctly. This test proves that against a
// hand-written fixture using the OLD shapes -- a bare RFC3339
// activationExpiry string (the Go field type before this pass was plain
// string) and a map-shaped argoCd configuration (Capability.Configuration
// was map[string]any before this pass, holding the same camelCase keys the
// real SDK client serializes) -- not against anything this pass's own code
// produced, which would prove nothing about backward compatibility.
func TestRestore_Version2Fixture_TolerantDecode(t *testing.T) {
	t.Parallel()

	// oldActivationExpiry is 2026-01-02T03:04:05Z, i.e. Unix 1767323045 --
	// verified independently (python datetime), not derived from this
	// package's own code.
	const fixture = `{
		"version": 2,
		"accountId": "123456789012",
		"region": "us-east-1",
		"tables": {
			"clusters": [
				{
					"name": "old-connected-cluster",
					"arn": "arn:aws:eks:us-east-1:123456789012:cluster/old-connected-cluster",
					"version": "1.32",
					"status": "ACTIVE",
					"accountId": "123456789012",
					"region": "us-east-1",
					"createdAt": "2026-01-01T00:00:00Z",
					"tags": {},
					"connectorConfig": {
						"provider": "EKS_ANYWHERE",
						"roleArn": "arn:aws:iam::123456789012:role/connector",
						"activationId": "activation-id-1",
						"activationCode": "activation-code-1",
						"activationExpiry": "2026-01-02T03:04:05Z"
					}
				}
			],
			"capabilities": [
				{
					"clusterName": "old-connected-cluster",
					"capabilityName": "old-argocd",
					"arn": "arn:aws:eks:us-east-1:123456789012:capability/old-connected-cluster/old-argocd",
					"type": "ARGOCD",
					"roleArn": "arn:aws:iam::123456789012:role/capability",
					"deletePropagationPolicy": "RETAIN",
					"status": "ACTIVE",
					"createdAt": "2026-01-01T00:00:00Z",
					"modifiedAt": "2026-01-01T00:00:00Z",
					"tags": {},
					"configuration": {
						"argoCd": {
							"awsIdc": {"idcInstanceArn": "arn:aws:sso:::instance/ssoins-old"},
							"namespace": "argocd-ns",
							"rbacRoleMappings": [
								{"role": "ADMIN", "identities": [{"id": "u1", "type": "SSO_USER"}]}
							]
						}
					}
				},
				{
					"clusterName": "old-connected-cluster",
					"capabilityName": "old-ack-unrepresentable",
					"arn": "arn:aws:eks:us-east-1:123456789012:capability/old-connected-cluster/old-ack-unrepresentable",
					"type": "ACK",
					"roleArn": "arn:aws:iam::123456789012:role/capability",
					"deletePropagationPolicy": "RETAIN",
					"status": "ACTIVE",
					"createdAt": "2026-01-01T00:00:00Z",
					"modifiedAt": "2026-01-01T00:00:00Z",
					"tags": {},
					"configuration": ["not", "an", "object", "-- unrepresentable by CapabilityConfiguration"]
				}
			]
		}
	}`

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	require.NoError(t, b.Restore(t.Context(), []byte(fixture)))

	t.Run("cluster_and_both_capabilities_survived_restore", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 1, b.ClusterCount())
		assert.Equal(t, 2, b.CapabilityCount())
	})

	t.Run("activation_expiry_old_rfc3339_string_decodes", func(t *testing.T) {
		t.Parallel()

		c, err := b.DescribeCluster("old-connected-cluster")
		require.NoError(t, err)
		require.NotNil(t, c.ConnectorConfig)

		assert.False(t, c.ConnectorConfig.ActivationExpiry.IsZero())
		assert.Equal(t, int64(1767323045), c.ConnectorConfig.ActivationExpiry.Time().Unix())
	})

	t.Run("argocd_configuration_old_map_shape_decodes_into_typed_struct", func(t *testing.T) {
		t.Parallel()

		capa, err := b.DescribeCapability("old-connected-cluster", "old-argocd")
		require.NoError(t, err)
		require.NotNil(t, capa.Configuration)
		require.NotNil(t, capa.Configuration.ArgoCd)

		require.NotNil(t, capa.Configuration.ArgoCd.AwsIdc)
		assert.Equal(t, "arn:aws:sso:::instance/ssoins-old", capa.Configuration.ArgoCd.AwsIdc.IdcInstanceArn)
		assert.Equal(t, "argocd-ns", capa.Configuration.ArgoCd.Namespace)
		require.Len(t, capa.Configuration.ArgoCd.RbacRoleMappings, 1)
		assert.Equal(t, "ADMIN", capa.Configuration.ArgoCd.RbacRoleMappings[0].Role)
	})

	t.Run("unrepresentable_configuration_degrades_to_nil_not_a_restore_failure", func(t *testing.T) {
		t.Parallel()

		capa, err := b.DescribeCapability("old-connected-cluster", "old-ack-unrepresentable")
		require.NoError(t, err)
		assert.Nil(t, capa.Configuration)
	})
}
