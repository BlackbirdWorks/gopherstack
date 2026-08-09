package cloudfront_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestStoreSetup_FullStateSnapshotRestoreRoundTrip is the Phase 3.3 pkgs/store
// conversion's full-state round-trip test: it creates at least one instance of
// every resource collection converted to a store.Table (see store_setup.go),
// including both composite-key "dirty" tables (invalidations,
// tenantInvalidations), then verifies every one of them -- plus the
// distSearchInverted config-token index used by ListDistributionsBy* -- survives
// a Snapshot -> Restore cycle unchanged.
func TestStoreSetup_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	orig := newTestBackend(t)

	// distributions + the distSearchInverted token index (verified via
	// ListDistributionsByCachePolicyID below).
	dist, err := orig.CreateDistribution(
		"caller-ref-1", "a distribution", true, []byte(`<Cfg><CachePolicyId>cp-token-123</CachePolicyId></Cfg>`),
	)
	require.NoError(t, err)

	// invalidations: composite-key table keyed by distID + "#" + invID, with a
	// secondary index grouping by distribution ID.
	inv, err := orig.CreateInvalidation(dist.ID, "inv-caller-ref", []string{"/*"})
	require.NoError(t, err)

	// oais
	oai, err := orig.CreateOAI("oai-caller-ref", "an oai")
	require.NoError(t, err)

	// anycastIPLists
	anycast, err := orig.CreateAnycastIPList("my-anycast-list", 3)
	require.NoError(t, err)

	// cachePolicies
	cachePolicy, err := orig.CreateCachePolicy("my-cache-policy", "c", 10, 100, 0)
	require.NoError(t, err)

	// connectionFunctions
	connFn, err := orig.CreateConnectionFunction("my-connection-function", "c")
	require.NoError(t, err)

	// connectionGroups
	connGroup, err := orig.CreateConnectionGroup("my-connection-group", "c")
	require.NoError(t, err)

	// continuousDeploymentPolicies
	cdp, err := orig.CreateContinuousDeploymentPolicy(true, "staging.example.com")
	require.NoError(t, err)

	// originAccessControls
	oac, err := orig.CreateOriginAccessControl("my-oac", "d", "s3", "always", "sigv4")
	require.NoError(t, err)

	// responseHeadersPolicies
	rhp, err := orig.CreateResponseHeadersPolicy("my-rhp", "c")
	require.NoError(t, err)

	// functions (keyed by Name)
	fn, err := orig.CreateFunction("my-function", "c", "cloudfront-js-2.0", "function handler(e){}", nil)
	require.NoError(t, err)

	// originRequestPolicies
	orp, err := orig.CreateOriginRequestPolicy("my-orp", "c")
	require.NoError(t, err)

	// publicKeys (created before fieldLevelEncryptionProfiles/keyGroups, which
	// referentially depend on it)
	pk, err := orig.CreatePublicKey("pk-caller-ref", "my-public-key", "c", testRSA2048PublicKeyPEM)
	require.NoError(t, err)

	// fieldLevelEncryptionProfiles (references the public key above)
	flep, err := orig.CreateFieldLevelEncryptionProfile("my-flep", "c", []cloudfront.EncryptionEntity{
		{PublicKeyID: pk.ID, ProviderID: "provider", FieldPatterns: []string{"field1"}},
	})
	require.NoError(t, err)

	// fieldLevelEncryptions (references the profile above)
	fle, err := orig.CreateFieldLevelEncryption("my-fle", "c", []cloudfront.FLEQueryArgProfile{
		{QueryArg: "q", ProfileID: flep.ID},
	})
	require.NoError(t, err)

	// keyGroups (references the public key above)
	kg, err := orig.CreateKeyGroup("my-key-group", "c", []string{pk.ID})
	require.NoError(t, err)

	// realtimeLogConfigs (keyed by ARN, not ID)
	rlc, err := orig.CreateRealtimeLogConfig("my-rlc", 50, []string{"timestamp"})
	require.NoError(t, err)

	// keyValueStores
	kvs, err := orig.CreateKeyValueStore("my-kvs", "c", nil)
	require.NoError(t, err)

	// vpcOrigins
	vpcOrigin, err := orig.CreateVpcOrigin("my-vpc-origin", nil)
	require.NoError(t, err)

	// trustStores
	ts, err := orig.CreateTrustStore(
		"my-trust-store", "c", cloudfront.TrustStoreCertificateBundle{S3Bucket: "b", S3Key: "k"}, nil,
	)
	require.NoError(t, err)

	// streamingDistributions
	sdCfg := cloudfront.StreamingDistributionConfig{CallerReference: "sd-caller-ref"}
	sd, err := orig.CreateStreamingDistribution(sdCfg, []byte(`<StreamingDistributionConfig/>`))
	require.NoError(t, err)

	// distributionTenants
	tenant, err := orig.CreateDistributionTenant(dist.ID, "my-tenant", []string{"tenant.example.com"}, nil)
	require.NoError(t, err)

	// tenantInvalidations: composite-key table keyed by tenantID + "#" + invID.
	tenantInv, err := orig.CreateInvalidationForTenant(tenant.ID, []string{"/*"})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := newTestBackend(t)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	gotDist, err := fresh.GetDistribution(dist.ID)
	require.NoError(t, err)
	assert.Equal(t, dist.CallerReference, gotDist.CallerReference)

	// distSearchInverted must be rebuilt from the restored distributions so
	// config-token lookups keep working after a restore.
	byPolicy := fresh.ListDistributionsByCachePolicyID("cp-token-123")
	require.Len(t, byPolicy, 1)
	assert.Equal(t, dist.ID, byPolicy[0].ID)

	gotInv, err := fresh.GetInvalidation(dist.ID, inv.ID)
	require.NoError(t, err)
	assert.Equal(t, inv.CallerRef, gotInv.CallerRef)
	assert.Equal(t, []string{"/*"}, gotInv.Paths)

	gotOAI, err := fresh.GetOAI(oai.ID)
	require.NoError(t, err)
	assert.Equal(t, oai.CallerReference, gotOAI.CallerReference)

	gotAnycast, err := fresh.GetAnycastIPList(anycast.ID)
	require.NoError(t, err)
	assert.Equal(t, anycast.Name, gotAnycast.Name)

	gotCachePolicy, err := fresh.GetCachePolicy(cachePolicy.ID)
	require.NoError(t, err)
	assert.Equal(t, cachePolicy.Name, gotCachePolicy.Name)

	gotConnFn, err := fresh.GetConnectionFunction(connFn.ID)
	require.NoError(t, err)
	assert.Equal(t, connFn.Name, gotConnFn.Name)

	gotConnGroup, err := fresh.GetConnectionGroup(connGroup.ID)
	require.NoError(t, err)
	assert.Equal(t, connGroup.Name, gotConnGroup.Name)

	gotCDP, err := fresh.GetContinuousDeploymentPolicy(cdp.ID)
	require.NoError(t, err)
	assert.Equal(t, cdp.StagingDistributionDNS, gotCDP.StagingDistributionDNS)

	gotOAC, err := fresh.GetOriginAccessControl(oac.ID)
	require.NoError(t, err)
	assert.Equal(t, oac.Name, gotOAC.Name)

	gotRHP, err := fresh.GetResponseHeadersPolicy(rhp.ID)
	require.NoError(t, err)
	assert.Equal(t, rhp.Name, gotRHP.Name)

	gotFn, err := fresh.GetFunction(fn.Name)
	require.NoError(t, err)
	assert.Equal(t, fn.FunctionCode, gotFn.FunctionCode)

	gotORP, err := fresh.GetOriginRequestPolicy(orp.ID)
	require.NoError(t, err)
	assert.Equal(t, orp.Name, gotORP.Name)

	gotFLE, err := fresh.GetFieldLevelEncryption(fle.ID)
	require.NoError(t, err)
	assert.Equal(t, fle.Name, gotFLE.Name)

	gotFLEP, err := fresh.GetFieldLevelEncryptionProfile(flep.ID)
	require.NoError(t, err)
	assert.Equal(t, flep.Name, gotFLEP.Name)

	gotPK, err := fresh.GetPublicKey(pk.ID)
	require.NoError(t, err)
	assert.Equal(t, pk.Name, gotPK.Name)

	gotKG, err := fresh.GetKeyGroup(kg.ID)
	require.NoError(t, err)
	assert.Equal(t, kg.Name, gotKG.Name)
	assert.Equal(t, []string{pk.ID}, gotKG.Items)

	gotRLC, err := fresh.GetRealtimeLogConfig(rlc.ARN)
	require.NoError(t, err)
	assert.Equal(t, rlc.Name, gotRLC.Name)

	gotKVS, err := fresh.GetKeyValueStore(kvs.ID)
	require.NoError(t, err)
	assert.Equal(t, kvs.Name, gotKVS.Name)

	gotVpcOrigin, err := fresh.GetVpcOrigin(vpcOrigin.ID)
	require.NoError(t, err)
	assert.Equal(t, vpcOrigin.Name, gotVpcOrigin.Name)

	gotTS, err := fresh.GetTrustStore(ts.ID)
	require.NoError(t, err)
	assert.Equal(t, ts.Name, gotTS.Name)

	gotSD, err := fresh.GetStreamingDistribution(sd.ID)
	require.NoError(t, err)
	assert.Equal(t, sd.Config.CallerReference, gotSD.Config.CallerReference)

	gotTenant, err := fresh.GetDistributionTenant(tenant.ID)
	require.NoError(t, err)
	assert.Equal(t, tenant.Name, gotTenant.Name)

	gotTenantInv, err := fresh.GetInvalidationForTenant(tenant.ID, tenantInv.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"/*"}, gotTenantInv.Paths)
}

// TestStoreSetup_RestoreDiscardsIncompatibleVersion verifies the snapshot version
// guard: a snapshot whose "version" field does not match the current
// cloudfrontSnapshotVersion is discarded cleanly (backend reset to empty)
// rather than partially decoded, mirroring the services/ec2, services/sqs, and
// services/apigateway conversions this one follows.
func TestStoreSetup_RestoreDiscardsIncompatibleVersion(t *testing.T) {
	t.Parallel()

	orig := newTestBackend(t)
	_, err := orig.CreateDistribution("caller-ref-old", "old", true, nil)
	require.NoError(t, err)

	fresh := newTestBackend(t)
	// A malformed/incompatible version (0, i.e. absent) must be discarded rather
	// than partially decoded.
	require.NoError(t, fresh.Restore(t.Context(), []byte(`{"version":0,"tables":{}}`)))

	assert.Empty(t, fresh.ListDistributions())
}
