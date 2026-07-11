package route53_test

import (
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
				zone, err := b.CreateHostedZone("example.com.", "ref-001", "test zone", false)
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

				zones, err := b.ListHostedZones("", 0)
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

	zone, err := b.CreateHostedZone("full-state.example.com.", "ref-full-zone", "full state zone", true)
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
	})
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
