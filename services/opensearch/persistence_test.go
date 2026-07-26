package opensearch_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestARNIndexRebuiltAfterRestore(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("snap-domain", "OpenSearch_2.11")

	// Snapshot and restore into fresh backend.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Tag operations should work after restore (they rely on arnIndex).
	domain, err := fresh.DescribeDomain("snap-domain")
	require.NoError(t, err)

	err = fresh.AddTags(domain.ARN, map[string]string{"env": "test"})
	require.NoError(t, err)

	tags, err := fresh.ListTags(domain.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestTagsAfterRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("tagged-domain", "")

	domain, err := b.DescribeDomain("tagged-domain")
	require.NoError(t, err)

	require.NoError(t, b.AddTags(domain.ARN, map[string]string{"key": "value", "env": "prod"}))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Tags should survive round-trip
	tags, err := fresh.ListTags(domain.ARN)
	require.NoError(t, err)
	assert.Equal(t, "value", tags["key"])
	assert.Equal(t, "prod", tags["env"])
}

func TestPersistence_PackagesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	pkg, err := b.CreatePackage("my-pkg", "TXT-DICTIONARY", "test package", nil, nil)
	require.NoError(t, err)
	pkgID := pkg.PackageID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	pkgs, err := fresh.DescribePackages([]string{pkgID})
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	assert.Equal(t, "my-pkg", pkgs[0].PackageName)
	assert.Equal(t, "TXT-DICTIONARY", pkgs[0].PackageType)
}

func TestPersistence_VpcEndpointsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("vpc-persist", "")

	domARN := "arn:aws:es:us-east-1:123456789012:domain/vpc-persist"
	ep, err := b.CreateVpcEndpoint(domARN, map[string]any{
		"SubnetIds":        []string{"subnet-abc"},
		"SecurityGroupIds": []string{"sg-abc"},
	})
	require.NoError(t, err)
	epID := ep.VpcEndpointID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	endpoints := fresh.ListVpcEndpoints()
	require.NotEmpty(t, endpoints)
	found := false
	for _, e := range endpoints {
		if e.VpcEndpointID == epID {
			found = true

			break
		}
	}
	assert.True(t, found, "VPC endpoint should persist through snapshot/restore")
}

func TestPersistence_UpgradeHistoryRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("uh-persist", "OpenSearch_2.11")

	err := b.UpgradeDomain("uh-persist", "OpenSearch_2.17")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	history, err := fresh.GetUpgradeHistory("uh-persist")
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, "OpenSearch_2.17", history[0].UpgradeName)
}

func TestPersistence_AutoTunesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("at-persist", "")

	err := b.SetAutoTune("at-persist", "ENABLED", nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Domain should still exist.
	d, err := fresh.DescribeDomain("at-persist")
	require.NoError(t, err)
	assert.Equal(t, "at-persist", d.Name)

	// AutoTune entries should be present.
	tunes, err := fresh.GetAutoTune("at-persist")
	require.NoError(t, err)
	assert.NotEmpty(t, tunes)
}

func TestPersistence_ReservedInstancesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	ri, err := b.PurchaseReservedInstanceOffering("ri-offering-1", "my-reservation", 3)
	require.NoError(t, err)
	riID := ri.ReservedInstanceID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	instances := fresh.DescribeReservedInstances("")
	require.NotEmpty(t, instances)
	found := false
	for _, inst := range instances {
		if inst.ReservedInstanceID == riID {
			found = true
			assert.Equal(t, "my-reservation", inst.ReservationName)
			assert.Equal(t, 3, inst.InstanceCount)

			break
		}
	}
	assert.True(t, found, "reserved instance should persist through snapshot/restore")
}

func TestPersistence_OutboundConnectionsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	conn, err := b.CreateOutboundConnection(
		"test-alias",
		"",
		opensearch.DomainInformation{DomainName: "local-domain"},
		opensearch.DomainInformation{DomainName: "remote-domain"},
		"",
		"",
	)
	require.NoError(t, err)
	connID := conn.ConnectionID

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	conns := fresh.DescribeOutboundConnections()
	require.NotEmpty(t, conns)
	found := false
	for _, c := range conns {
		if c.ConnectionID == connID {
			found = true
			assert.Equal(t, "test-alias", c.ConnectionAlias)

			break
		}
	}
	assert.True(t, found, "outbound connection should persist through snapshot/restore")
}

func TestPersistence_ScheduledActionsRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("sa-persist", "")

	action := &opensearch.ScheduledAction{
		ID:          "sa-001",
		Type:        "SERVICE_SOFTWARE_UPDATE",
		Severity:    "HIGH",
		ScheduledBy: "CUSTOMER",
		Status:      "PENDING_UPDATE",
	}
	opensearch.AddScheduledActionInternal(b, "sa-persist", action)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	actions := fresh.ListScheduledActions("sa-persist")
	require.Len(t, actions, 1)
	assert.Equal(t, "sa-001", actions[0].ID)
	assert.Equal(t, "HIGH", actions[0].Severity)
}

func TestPersistence_DomainIndexesRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("idx-persist", "")

	idx, err := b.CreateIndex(
		"idx-persist", "my-index",
		map[string]any{"properties": map[string]any{"field": "text"}},
		map[string]any{"number_of_shards": 1},
		map[string]any{},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-index", idx.IndexName)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	restoredIdx, err := fresh.GetIndex("idx-persist", "my-index")
	require.NoError(t, err)
	assert.Equal(t, "my-index", restoredIdx.IndexName)
}

func TestPersistence_CountersRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	// Create items to bump counters.
	b.AddDomainInternal("counter-dom", "")
	_, err := b.CreateOutboundConnection(
		"alias1", "",
		opensearch.DomainInformation{DomainName: "local-dom"},
		opensearch.DomainInformation{DomainName: "remote-dom"},
		"", "",
	)
	require.NoError(t, err)
	_, err = b.CreateVpcEndpoint("arn:test", map[string]any{"SubnetIds": []string{"subnet-1"}})
	require.NoError(t, err)
	_, err = b.CreatePackage("pkg-counter", "TXT-DICTIONARY", "", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Creating new items after restore should not reuse old IDs.
	conn2, err := fresh.CreateOutboundConnection(
		"alias2", "",
		opensearch.DomainInformation{DomainName: "local-dom"},
		opensearch.DomainInformation{DomainName: "remote-dom"},
		"", "",
	)
	require.NoError(t, err)
	assert.NotEqual(t, "co-1", conn2.ConnectionID, "counter should be restored, new ID should not collide")

	ep2, err := fresh.CreateVpcEndpoint("arn:test2", map[string]any{"SubnetIds": []string{"subnet-2"}})
	require.NoError(t, err)
	assert.NotEqual(t, "vpce-1", ep2.VpcEndpointID, "VPC endpoint counter should be restored")
}

func TestPersistence_DomainMaintenanceRoundTrip(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddDomainInternal("maint-domain", "")

	m, err := b.StartDomainMaintenance("maint-domain", "REBOOT_NODE", "node-1")
	require.NoError(t, err)
	assert.NotEmpty(t, m.MaintenanceID)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	maintenances, err := fresh.ListDomainMaintenances("maint-domain")
	require.NoError(t, err)
	require.Len(t, maintenances, 1)
	assert.Equal(t, "REBOOT_NODE", maintenances[0].Action)
	assert.Equal(t, "node-1", maintenances[0].NodeID)
}

func TestOpenSearchHandler_Persistence_AdditionalResources(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	// Set up state with new ops
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "snap-domain", EngineVersion: "OpenSearch_2.11"})
	require.NoError(t, err)

	opensearch.SeedInboundConnection(b, "conn-abc")

	_, err = b.AddDataSource("snap-domain", "my-ds", "desc", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
	require.NoError(t, err)

	_, err = b.AddDirectQueryDataSource("my-dq", "desc", json.RawMessage(`{"CloudWatchLog":{}}`), []string{})
	require.NoError(t, err)

	b.AddPackageInternal("pkg-001", "test-pkg", "TXT-DICTIONARY")

	_, err = b.AssociatePackage("pkg-001", "snap-domain")
	require.NoError(t, err)

	_, err = b.AuthorizeVpcEndpointAccess("snap-domain", "111122223333", "")
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify domain persists
	domain, err := fresh.DescribeDomain("snap-domain")
	require.NoError(t, err)
	assert.Equal(t, "snap-domain", domain.Name)

	// Verify inbound connection persists
	conn, err := fresh.AcceptInboundConnection("conn-abc")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", conn.Status)

	// Verify application persists
	app, err := fresh.CreateApplication("another-app", nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, app.ID)
}

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *opensearch.InMemoryBackend) string
		verify func(t *testing.T, b *opensearch.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *opensearch.InMemoryBackend) string {
				domain, err := b.CreateDomain(opensearch.CreateDomainInput{
					Name:          "test-domain",
					EngineVersion: "OpenSearch_2.3",
					ClusterConfig: opensearch.ClusterConfig{
						InstanceType:  "t3.small.search",
						InstanceCount: 1,
					},
				})
				if err != nil {
					return ""
				}

				return domain.Name
			},
			verify: func(t *testing.T, b *opensearch.InMemoryBackend, id string) {
				t.Helper()

				domain, err := b.DescribeDomain(id)
				require.NoError(t, err)
				assert.Equal(t, id, domain.Name)
				assert.Equal(t, "OpenSearch_2.3", domain.EngineVersion)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *opensearch.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *opensearch.InMemoryBackend, _ string) {
				t.Helper()

				names := b.ListDomainNames()
				assert.Empty(t, names)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestOpenSearchHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := opensearch.NewHandler(backend)

	// Create a domain in the backend
	_, err := backend.CreateDomain(opensearch.CreateDomainInput{Name: "snap-domain", EngineVersion: "OpenSearch_2.11"})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := opensearch.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	domain, err := fresh.DescribeDomain("snap-domain")
	require.NoError(t, err)
	assert.Equal(t, "snap-domain", domain.Name)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family, including the pkgs/store "clean"
// tables, the "dirty" DTO tables (dryRuns, autoTunes, domainDataSources,
// domainIndexes), and the plain maps left unconverted (part of the Phase 3.3
// datalayer conversion -- see store_setup.go). It guards against silently
// losing a family in Snapshot/Restore's field wiring.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	domain, err := original.CreateDomain(opensearch.CreateDomainInput{
		Name:          "full-domain",
		EngineVersion: "OpenSearch_2.11",
		ClusterConfig: opensearch.ClusterConfig{InstanceType: "t3.small.search", InstanceCount: 1},
	})
	require.NoError(t, err)

	// "Dirty" DTO tables: domainDataSources, domainIndexes, dryRuns, autoTunes.
	_, err = original.AddDataSource(domain.Name, "ds-1", "a data source", json.RawMessage(`{"S3GlueDataCatalog":{}}`))
	require.NoError(t, err)

	_, err = original.CreateIndex(domain.Name, "idx-1", nil, nil, nil)
	require.NoError(t, err)

	_, err = original.GetDryRunProgress(domain.Name)
	require.NoError(t, err)

	require.NoError(t, original.SetAutoTune(domain.Name, "ENABLED", nil))

	// "Clean" pkgs/store tables not otherwise covered by other persistence tests.
	_, err = original.AddDirectQueryDataSource(
		"dq-1",
		"a direct query source",
		json.RawMessage(`{"CloudWatchLog":{}}`),
		nil,
	)
	require.NoError(t, err)

	_, err = original.CreateOutboundConnection(
		"peer-alias", "",
		opensearch.DomainInformation{DomainName: "local-dom"},
		opensearch.DomainInformation{DomainName: "remote-dom"},
		"", "",
	)
	require.NoError(t, err)

	opensearch.SeedInboundConnection(original, "conn-in-1")

	_, err = original.CreateVpcEndpoint(domain.ARN, map[string]any{"SubnetIds": []string{"subnet-1"}})
	require.NoError(t, err)

	app, err := original.CreateApplication("full-app", nil, nil)
	require.NoError(t, err)

	pkg, err := original.CreatePackage("full-pkg", "TXT-DICTIONARY", "a package", nil, nil)
	require.NoError(t, err)

	_, err = original.PurchaseReservedInstanceOffering("ri-offering-1", "full-ri", 1)
	require.NoError(t, err)

	_, err = original.CreateServerlessCollection("full-coll", "SEARCH", "a collection", "", nil)
	require.NoError(t, err)

	_, err = original.CreateServerlessAccessPolicy("data", "full-ap", "an access policy", "{}")
	require.NoError(t, err)

	_, err = original.CreateServerlessSecurityConfig("saml", "a security config", nil)
	require.NoError(t, err)

	_, err = original.CreateServerlessEncryptionPolicy("encryption", "full-ep", "an encryption policy", "{}")
	require.NoError(t, err)

	_, err = original.CreateServerlessNetworkPolicy("network", "full-np", "a network policy", "{}")
	require.NoError(t, err)

	// Plain maps left unconverted (see store_setup.go's registerAllTables doc).
	_, err = original.AuthorizeVpcEndpointAccess(domain.Name, "123456789012", "")
	require.NoError(t, err)

	opensearch.AddScheduledActionInternal(original, domain.Name, &opensearch.ScheduledAction{ID: "sa-1"})

	_, err = original.AssociatePackage(pkg.PackageID, domain.Name)
	require.NoError(t, err)

	_, err = original.StartDomainMaintenance(domain.Name, "REBOOT_NODE", "")
	require.NoError(t, err)

	require.NoError(t, original.UpgradeDomain(domain.Name, "OpenSearch_2.15"))

	_, err = original.PutDefaultApplicationSetting("arn:aws:es:us-east-1:123456789012:application/app-1", true)
	require.NoError(t, err)

	// Newer "clean" pkgs/store tables added this pass: data source
	// attachments, capabilities, and migrations (see store_setup.go).
	att, err := original.AttachDataSource(app.ID, domain.ARN)
	require.NoError(t, err)

	_, err = original.RegisterCapability(app.ID, "ai-capability")
	require.NoError(t, err)

	mig, err := original.StartMigration(app.ID, domain.ARN)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify every family actually round-tripped.
	gotDomain, err := fresh.DescribeDomain(domain.Name)
	require.NoError(t, err)
	assert.Equal(t, domain.EngineVersion, gotDomain.EngineVersion)

	dataSources, err := fresh.ListDataSources(domain.Name)
	require.NoError(t, err)
	assert.Len(t, dataSources, 1)

	idx, err := fresh.GetIndex(domain.Name, "idx-1")
	require.NoError(t, err)
	assert.Equal(t, "idx-1", idx.IndexName)

	dr, err := fresh.GetDryRunProgress(domain.Name)
	require.NoError(t, err)
	assert.NotEmpty(t, dr.DryRunID)

	autoTune, err := fresh.GetAutoTune(domain.Name)
	require.NoError(t, err)
	assert.NotEmpty(t, autoTune)

	dqSources := fresh.ListDirectQueryDataSources()
	assert.Len(t, dqSources, 1)

	outbound := fresh.DescribeOutboundConnections()
	assert.Len(t, outbound, 1)

	// 2 inbound connections: one mirrored automatically from the outbound
	// connection created above (peer-alias), one seeded directly (conn-in-1).
	inbound := fresh.DescribeInboundConnections()
	assert.Len(t, inbound, 2)

	endpoints := fresh.ListVpcEndpoints()
	assert.Len(t, endpoints, 1)

	apps := fresh.ListApplications()
	require.Len(t, apps, 1)
	assert.Equal(t, app.Name, apps[0].Name)

	pkgs, err := fresh.DescribePackages(nil)
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	assert.Equal(t, pkg.PackageName, pkgs[0].PackageName)

	reserved := fresh.DescribeReservedInstances("")
	assert.Len(t, reserved, 1)

	collections := fresh.BatchGetServerlessCollections(nil, nil)
	assert.Len(t, collections, 1)

	assert.Len(t, fresh.ListServerlessAccessPolicies(""), 1)
	assert.Len(t, fresh.ListServerlessSecurityConfigs(""), 1)
	assert.Len(t, fresh.ListServerlessEncryptionPolicies(""), 1)
	assert.Len(t, fresh.ListServerlessNetworkPolicies(""), 1)

	principals, err := fresh.ListVpcEndpointAccess(domain.Name)
	require.NoError(t, err)
	assert.Len(t, principals, 1)

	actions := fresh.ListScheduledActions(domain.Name)
	assert.Len(t, actions, 1)

	assert.Equal(t, []string{domain.Name}, fresh.ListDomainsForPackage(pkg.PackageID))

	maintenances, err := fresh.ListDomainMaintenances(domain.Name)
	require.NoError(t, err)
	assert.Len(t, maintenances, 1)

	history, err := fresh.GetUpgradeHistory(domain.Name)
	require.NoError(t, err)
	assert.Len(t, history, 1)

	assert.Equal(t, "arn:aws:es:us-east-1:123456789012:application/app-1",
		fresh.GetDefaultApplicationSetting())

	gotAtt, err := fresh.DescribeDataSourceAttachment(app.ID, domain.ARN)
	require.NoError(t, err)
	assert.Equal(t, att.AttachmentID, gotAtt.AttachmentID)

	gotCap, err := fresh.GetCapability(app.ID, "ai-capability")
	require.NoError(t, err)
	assert.Equal(t, "active", gotCap.Status)

	gotMig, err := fresh.GetMigration(mig.MigrationID)
	require.NoError(t, err)
	assert.Equal(t, mig.MigrationID, gotMig.MigrationID)
}

func TestOpenSearchHandler_Routing(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("000000000000", "us-east-1"))

	assert.Equal(t, "OpenSearch", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{"domain path", "/2021-01-01/opensearch/domain", true},
		{"tags path", "/2021-01-01/tags", true},
		{"no match", "/other", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}
