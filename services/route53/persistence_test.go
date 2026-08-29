package route53_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *route53.InMemoryBackend) string
		verify func(t *testing.T, b *route53.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *route53.InMemoryBackend) string {
				zone, err := b.CreateHostedZone("example.com.", "ref-001", "test zone", false, "", "", "")
				if err != nil {
					return ""
				}

				return zone.ID
			},
			verify: func(t *testing.T, b *route53.InMemoryBackend, id string) {
				t.Helper()

				zone, err := b.GetHostedZone(id)
				require.NoError(t, err)
				assert.Equal(t, id, zone.ID)
				assert.Equal(t, "example.com.", zone.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *route53.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *route53.InMemoryBackend, _ string) {
				t.Helper()

				zones, err := b.ListHostedZones("", 0, "", "")
				require.NoError(t, err)
				assert.Empty(t, zones.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := route53.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := route53.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises every resource
// family the Phase 3.3 pkgs/store conversion touched -- both the tables
// registered on the backend's registry (zones/records, health checks, key
// signing keys, CIDR collections, query logging configs, reusable delegation
// sets, traffic policy instances, changes) and the maps deliberately left raw
// (traffic policy versions, VPC associations, VPC association
// authorizations, tags) -- in a single Snapshot/Restore round trip, so a
// regression that silently drops any one collection during persistence would
// fail this test even if the collection's own narrower round-trip test above
// still passed.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	zone, err := b.CreateHostedZone("full-state.example.com.", "ref-full-zone", "full state zone", true, "", "", "")
	require.NoError(t, err)

	changeID, err := b.ChangeResourceRecordSets(zone.ID, []route53.Change{
		{
			Action: route53.ChangeActionCreate,
			ResourceRecordSet: route53.ResourceRecordSet{
				Name:    "full-state.example.com.",
				Type:    "A",
				TTL:     300,
				Records: []route53.ResourceRecord{{Value: "192.0.2.10"}},
			},
		},
	})
	require.NoError(t, err)

	hc, err := b.CreateHealthCheck("ref-full-hc", route53.HealthCheckConfig{
		Type:      route53.HealthCheckTypeHTTP,
		IPAddress: "192.0.2.20",
		Port:      80,
	})
	require.NoError(t, err)

	ksk, err := b.CreateKeySigningKey(
		zone.ID, "ignored", "full-state-ksk", "arn:aws:kms:us-east-1:123456789012:key/abc", "",
	)
	require.NoError(t, err)

	col, err := b.CreateCidrCollection("full-state-collection", "ignored")
	require.NoError(t, err)

	_, err = b.ChangeCidrCollection(col.ID, []route53.CidrCollectionChange{
		{LocationName: "loc-1", Action: "PUT", CidrList: []string{"192.0.2.0/24"}},
	}, nil)
	require.NoError(t, err)

	qlc, err := b.CreateQueryLoggingConfig(zone.ID, "arn:aws:logs:us-east-1:123456789012:log-group:/route53/full-state")
	require.NoError(t, err)

	ds, err := b.CreateReusableDelegationSet("ref-full-ds", "")
	require.NoError(t, err)

	tp, err := b.CreateTrafficPolicy("full-state-policy", `{"AWSPolicyFormatVersion":"2015-10-01"}`, "comment")
	require.NoError(t, err)

	tpi, err := b.CreateTrafficPolicyInstance(zone.ID, "full-state-tpi.example.com.", tp.ID, tp.Version, 300)
	require.NoError(t, err)

	require.NoError(t, b.AssociateVPCWithHostedZone(zone.ID, "vpc-full-state", "us-east-1"))

	auth, err := b.CreateVPCAssociationAuthorization(zone.ID, "vpc-full-auth", "us-west-2")
	require.NoError(t, err)

	require.NoError(t, b.ChangeTagsForResource("hostedzone", zone.ID, map[string]string{"env": "test"}, nil))
	require.NoError(t, b.ChangeTagsForResource("healthcheck", hc.ID, map[string]string{"team": "dns"}, nil))

	bareChangeID := strings.TrimPrefix(changeID, "/change/")

	// Snapshot/Restore into a fresh backend.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := route53.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Every table/map's population count must be identical pre- and post-restore.
	assert.Equal(t, route53.ZoneCount(b), route53.ZoneCount(fresh), "zone count")
	assert.Equal(t, route53.HealthCheckCount(b), route53.HealthCheckCount(fresh), "health check count")
	assert.Equal(t, route53.KeySigningKeyCount(b), route53.KeySigningKeyCount(fresh), "KSK count")
	assert.Equal(t, route53.CidrCollectionCount(b), route53.CidrCollectionCount(fresh), "CIDR collection count")
	assert.Equal(
		t, route53.QueryLoggingConfigCount(b), route53.QueryLoggingConfigCount(fresh), "query logging config count",
	)
	assert.Equal(t, route53.DelegationSetCount(b), route53.DelegationSetCount(fresh), "delegation set count")
	assert.Equal(t, route53.TrafficPolicyCount(b), route53.TrafficPolicyCount(fresh), "traffic policy count")
	assert.Equal(
		t,
		route53.TrafficPolicyInstanceCount(b), route53.TrafficPolicyInstanceCount(fresh),
		"traffic policy instance count",
	)
	assert.Equal(t, route53.VPCAssociationCount(b), route53.VPCAssociationCount(fresh), "VPC association count")

	// zones + nested (raw, inline) record sets.
	gotZone, err := fresh.GetHostedZone(zone.ID)
	require.NoError(t, err)
	assert.Equal(t, zone.Name, gotZone.Name)
	assert.True(t, gotZone.PrivateZone)

	rrPage, err := fresh.ListResourceRecordSets(zone.ID, "", "", "", 0)
	require.NoError(t, err)

	var foundARecord bool

	for _, rr := range rrPage.Records {
		if rr.Type == "A" && rr.Name == "full-state.example.com." {
			foundARecord = true

			require.Len(t, rr.Records, 1)
			assert.Equal(t, "192.0.2.10", rr.Records[0].Value)
		}
	}

	assert.True(t, foundARecord, "A record should survive restore")

	// health checks.
	gotHC, err := fresh.GetHealthCheck(hc.ID)
	require.NoError(t, err)
	assert.Equal(t, hc.Config.IPAddress, gotHC.Config.IPAddress)

	// key signing keys (verified via the byZone-indexed GetDNSSEC accessor).
	_, ksks, err := fresh.GetDNSSEC(zone.ID)
	require.NoError(t, err)
	require.Len(t, ksks, 1)
	assert.Equal(t, ksk.Name, ksks[0].Name)
	assert.Equal(t, ksk.KeyManagementServiceArn, ksks[0].KeyManagementServiceArn)

	// CIDR collections (verified via ListCidrLocations/ListCidrBlocks).
	locations, err := fresh.ListCidrLocations(col.ID)
	require.NoError(t, err)
	require.Contains(t, locations, "loc-1")

	blocks, err := fresh.ListCidrBlocks(col.ID, "loc-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.0/24"}, blocks)

	// query logging configs (verified via the byZone-indexed accessor).
	gotQLC, err := fresh.GetQueryLoggingConfig(qlc.ID)
	require.NoError(t, err)
	assert.Equal(t, qlc.CloudWatchLogsLogGroupArn, gotQLC.CloudWatchLogsLogGroupArn)

	// reusable delegation sets.
	gotDS, err := fresh.GetReusableDelegationSet(ds.ID)
	require.NoError(t, err)
	assert.Equal(t, ds.NameServers, gotDS.NameServers)

	// traffic policies (left as a raw map[string][]*TrafficPolicy).
	gotTP, err := fresh.GetTrafficPolicy(tp.ID, tp.Version)
	require.NoError(t, err)
	assert.Equal(t, tp.Document, gotTP.Document)

	// traffic policy instances (verified via the byZone-indexed accessor).
	gotTPI, err := fresh.GetTrafficPolicyInstance(tpi.ID)
	require.NoError(t, err)
	assert.Equal(t, tpi.Name, gotTPI.Name)

	byZone, err := fresh.ListTrafficPolicyInstancesByHostedZone(zone.ID)
	require.NoError(t, err)
	require.Len(t, byZone, 1)
	assert.Equal(t, tpi.ID, byZone[0].ID)

	// VPC associations and authorizations (raw maps).
	assocs, err := fresh.ListVPCAssociations(zone.ID)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
	assert.Equal(t, "vpc-full-state", assocs[0].VPCID)

	auths, err := fresh.ListVPCAssociationAuthorizations(zone.ID)
	require.NoError(t, err)
	require.Len(t, auths, 1)
	assert.Equal(t, auth.VPCID, auths[0].VPCID)

	// changes (keyed via the "/change/" TrimPrefix key function).
	gotChange, err := fresh.GetChange(bareChangeID)
	require.NoError(t, err)
	assert.Equal(t, "INSYNC", gotChange.Status)

	// tags (raw map[string]*tags.Tags, keyed externally by resource ID).
	zoneTags, err := fresh.ListTagsForResource("hostedzone", zone.ID)
	require.NoError(t, err)
	assert.Equal(t, "test", zoneTags["env"])

	hcTags, err := fresh.ListTagsForResource("healthcheck", hc.ID)
	require.NoError(t, err)
	assert.Equal(t, "dns", hcTags["team"])
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_SnapshotRestore_HealthChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *route53.InMemoryBackend) string
		verify func(t *testing.T, b *route53.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "health_check_round_trip",
			setup: func(t *testing.T, b *route53.InMemoryBackend) string {
				t.Helper()

				hc, err := b.CreateHealthCheck("ref-hc-001", route53.HealthCheckConfig{
					Type:             route53.HealthCheckTypeHTTP,
					IPAddress:        "192.0.2.1",
					Port:             80,
					ResourcePath:     "/health",
					FailureThreshold: 3,
				})
				require.NoError(t, err)

				return hc.ID
			},
			verify: func(t *testing.T, b *route53.InMemoryBackend, id string) {
				t.Helper()

				hc, err := b.GetHealthCheck(id)
				require.NoError(t, err)
				assert.Equal(t, id, hc.ID)
				assert.Equal(t, route53.HealthCheckTypeHTTP, hc.Config.Type)
				assert.Equal(t, "192.0.2.1", hc.Config.IPAddress)
				assert.Equal(t, "Healthy", hc.Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := route53.NewInMemoryBackend()
			id := tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := route53.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}
func TestTagsPersistAcrossSnapshotRestore(t *testing.T) {
	t.Parallel()

	original := route53.NewInMemoryBackend()

	hz, err := original.CreateHostedZone("example.com", "ref-tags-persist", "", false, "", "", "")
	require.NoError(t, err)

	require.NoError(t, original.ChangeTagsForResource(
		"hostedzone", hz.ID, map[string]string{"env": "prod"}, nil,
	))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := route53.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	tags, err := fresh.ListTagsForResource("hostedzone", hz.ID)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"],
		"resource tags must be wired into backendSnapshot and survive a restore")
}

func TestSnapshotRestore_KSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantKSKCount int
	}{
		{
			name:         "ksk_survives_snapshot_restore",
			wantKSKCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "", "", "")
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(
				hz.ID, "caller-1", "key1", "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "ACTIVE",
			)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			b2 := route53.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantKSKCount, route53.KeySigningKeyCount(b2))
		})
	}
}

func TestSnapshotRestore_VPCAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantVPCCount int
	}{
		{
			name:         "vpc_association_survives_snapshot_restore",
			wantVPCCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true, "", "", "")
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1"))

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			b2 := route53.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantVPCCount, route53.VPCAssociationCount(b2))
		})
	}
}

// TestSnapshotRestore_DelegationSetSourceUsed confirms the
// DelegationSetSourceUsed bookkeeping flag added by
// CreateReusableDelegationSet's HostedZoneId mode survives a
// Snapshot/Restore round trip — it lives on HostedZone (persisted via
// zoneDataSnapshot's embedded Zone field), not in a separate table, so
// there's no dedicated wiring to miss, but a prior pass found exactly this
// class of bug for the (also embedded-field) tags map.
func TestSnapshotRestore_DelegationSetSourceUsed(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateReusableDelegationSet("ds-ref-extract", hz.ID)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := route53.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// A second extraction attempt against the restored backend must still be
	// rejected — proving the "already extracted" flag survived the round trip
	// rather than silently resetting and allowing a duplicate extraction.
	_, err = b2.CreateReusableDelegationSet("ds-ref-extract-2", hz.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrDelegationSetAlreadyReusable)
}

func TestRoute53_SnapshotRestore_NewOperations(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create a zone.
	zoneRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, zoneRec.Code)
	zoneID := extractZoneID(t, zoneRec.Body.String())

	// Create a CIDR collection.
	cidrBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>snap-cidrs</Name>
  <CallerReference>snap-cidr-ref</CallerReference>
</CreateCidrCollectionRequest>`
	cidrRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", cidrBody)
	require.Equal(t, http.StatusCreated, cidrRec.Code)

	// Create a traffic policy.
	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>snap-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`
	tpRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, tpRec.Code)
	policyID := extractTrafficPolicyID(t, tpRec.Body.String())

	// Snapshot and restore.
	snap := h.Backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	newBackend := route53.NewInMemoryBackend()
	require.NoError(t, newBackend.Restore(t.Context(), snap))
	newHandler := route53.NewHandler(newBackend)

	// Verify zone still accessible.
	getRec := send(t, newHandler, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Verify traffic policy version can be created (policy still exists).
	versionBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
</CreateTrafficPolicyVersionRequest>`
	versionRec := send(t, newHandler, http.MethodPost, "/2013-04-01/trafficpolicy/"+policyID, versionBody)
	assert.Equal(t, http.StatusCreated, versionRec.Code)
}
