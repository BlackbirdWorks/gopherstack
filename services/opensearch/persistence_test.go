package opensearch_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

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
	_, err = original.AddDataSource(domain.Name, "ds-1", "a data source", "S3")
	require.NoError(t, err)

	_, err = original.CreateIndex(domain.Name, "idx-1", nil, nil, nil)
	require.NoError(t, err)

	_, err = original.GetDryRunProgress(domain.Name)
	require.NoError(t, err)

	require.NoError(t, original.SetAutoTune(domain.Name, "ENABLED", nil))

	// "Clean" pkgs/store tables not otherwise covered by other persistence tests.
	_, err = original.AddDirectQueryDataSource("dq-1", "a direct query source", "spark", nil)
	require.NoError(t, err)

	_, err = original.CreateOutboundConnection("peer-alias", nil, nil)
	require.NoError(t, err)

	opensearch.SeedInboundConnection(original, "conn-in-1")

	_, err = original.CreateVpcEndpoint(domain.ARN, nil)
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

	_, err = original.UpdateScheduledAction(domain.Name, &opensearch.ScheduledAction{ID: "sa-1"})
	require.NoError(t, err)

	_, err = original.AssociatePackage(pkg.PackageID, domain.Name)
	require.NoError(t, err)

	_, err = original.StartDomainMaintenance(domain.Name, "REBOOT_NODE", "")
	require.NoError(t, err)

	require.NoError(t, original.UpgradeDomain(domain.Name, "OpenSearch_2.15"))

	require.NoError(t, original.PutDefaultApplicationSettings("OBSERVABILITY_ANALYTICS", []opensearch.AppSetting{
		{Key: "k", Value: "v"},
	}))

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

	inbound := fresh.DescribeInboundConnections()
	assert.Len(t, inbound, 1)

	endpoints := fresh.ListVpcEndpoints()
	assert.Len(t, endpoints, 1)

	apps := fresh.ListApplications()
	require.Len(t, apps, 1)
	assert.Equal(t, app.Name, apps[0].Name)

	pkgs, err := fresh.DescribePackages(nil)
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	assert.Equal(t, pkg.PackageName, pkgs[0].PackageName)

	reserved := fresh.DescribeReservedInstances()
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

	settings, err := fresh.GetDefaultApplicationSettings("OBSERVABILITY_ANALYTICS")
	require.NoError(t, err)
	require.Len(t, settings, 1)
	assert.Equal(t, "v", settings[0].Value)
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
