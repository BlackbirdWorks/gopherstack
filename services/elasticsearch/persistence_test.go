package elasticsearch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearch_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *elasticsearch.InMemoryBackend)
		verify func(t *testing.T, b *elasticsearch.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *elasticsearch.InMemoryBackend) {},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				names := b.ListDomainNames(context.Background())
				assert.Empty(t, names)
			},
		},
		{
			name: "domain_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(
					context.Background(),
					elasticsearch.CreateDomainInput{
						Name:                 "my-domain",
						ElasticsearchVersion: "7.10",
						ClusterConfig: elasticsearch.ClusterConfig{
							InstanceType:  "t3.small.elasticsearch",
							InstanceCount: 1,
						},
						EBSOptions: elasticsearch.EBSOptions{
							EBSEnabled: true,
							VolumeType: "gp2",
							VolumeSize: 10,
						},
					},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				names := b.ListDomainNames(context.Background())
				require.Len(t, names, 1)
				assert.Equal(t, "my-domain", names[0])

				d, err := b.DescribeDomain(context.Background(), "my-domain")
				require.NoError(t, err)
				assert.Equal(t, "7.10", d.ElasticsearchVersion)
				assert.Equal(t, "Active", d.Status)
				assert.NotEmpty(t, d.ARN)
			},
		},
		{
			name: "tags_preserved_via_arn",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				d, err := b.CreateDomain(
					context.Background(),
					elasticsearch.CreateDomainInput{Name: "tagged-domain"},
				)
				require.NoError(t, err)

				require.NoError(t, b.AddTags(context.Background(), d.ARN, map[string]string{"team": "platform"}))
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				d, err := b.DescribeDomain(context.Background(), "tagged-domain")
				require.NoError(t, err)

				tagMap, err := b.ListTags(context.Background(), d.ARN)
				require.NoError(t, err)
				assert.Equal(t, "platform", tagMap["team"])

				// ARN index must be rebuilt: ARN lookup must work.
				tagMap2, err := b.ListTags(context.Background(), d.ARN)
				require.NoError(t, err)
				assert.Equal(t, tagMap, tagMap2)
			},
		},
		{
			name: "package_and_association_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				_, err := b.CreateDomain(ctx, elasticsearch.CreateDomainInput{Name: "pkg-domain"})
				require.NoError(t, err)

				pkg, err := b.CreatePackage(ctx, "my-dict", "TXT-DICTIONARY", "custom dictionary")
				require.NoError(t, err)

				require.NoError(t, b.AssociatePackage(ctx, pkg.ID, "pkg-domain"))
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				require.Equal(t, 1, b.PackageCount())

				pkgs := b.DescribePackages(ctx, nil)
				require.Len(t, pkgs, 1)
				assert.Equal(t, "my-dict", pkgs[0].Name)

				// packagesByName (raw map) must be preserved: creating the same
				// name again must still fail as already-existing.
				_, err := b.CreatePackage(ctx, "my-dict", "TXT-DICTIONARY", "dup")
				require.Error(t, err)

				// packageAssociations (raw map) must be preserved.
				domains, err := b.ListDomainsForPackage(ctx, pkgs[0].ID)
				require.NoError(t, err)
				assert.Equal(t, []string{"pkg-domain"}, domains)

				domainPkgs := b.ListPackagesForDomain(ctx, "pkg-domain")
				require.Len(t, domainPkgs, 1)
				assert.Equal(t, "my-dict", domainPkgs[0].Name)
			},
		},
		{
			name: "cross_cluster_connections_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				b.AddInboundConnectionInternal(ctx, elasticsearch.InboundConnection{
					ConnectionID:     "in-1",
					ConnectionStatus: "PENDING_ACCEPTANCE",
					SourceDomainInfo: elasticsearch.CrossClusterDomainInfo{DomainName: "remote-src"},
					DestDomainInfo:   elasticsearch.CrossClusterDomainInfo{DomainName: "local-dst"},
				})

				_, err := b.CreateOutboundCrossClusterSearchConnection(
					ctx,
					elasticsearch.CrossClusterDomainInfo{DomainName: "local-src"},
					elasticsearch.CrossClusterDomainInfo{DomainName: "remote-dst"},
					"my-alias",
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				require.Equal(t, 1, b.InboundConnectionCount())
				require.Equal(t, 1, b.OutboundConnectionCount())

				inbound := b.DescribeInboundCrossClusterSearchConnections(ctx)
				require.Len(t, inbound, 1)
				assert.Equal(t, "in-1", inbound[0].ConnectionID)
				assert.Equal(t, "local-dst", inbound[0].DestDomainInfo.DomainName)

				outbound := b.DescribeOutboundCrossClusterSearchConnections(ctx)
				require.Len(t, outbound, 1)
				assert.Equal(t, "my-alias", outbound[0].ConnectionAlias)
			},
		},
		{
			name: "vpc_endpoint_and_access_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				d, err := b.CreateDomain(ctx, elasticsearch.CreateDomainInput{Name: "vpc-domain"})
				require.NoError(t, err)

				_, err = b.CreateVpcEndpoint(ctx, d.ARN, map[string]string{"VPCId": "vpc-123"})
				require.NoError(t, err)

				require.NoError(t, b.AuthorizeVpcEndpointAccess(ctx, "vpc-domain", "555566667777"))
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				require.Equal(t, 1, b.VpcEndpointCount())

				endpoints := b.ListVpcEndpoints(ctx)
				require.Len(t, endpoints, 1)
				assert.Equal(t, "vpc-123", endpoints[0].VpcOptions["VPCId"])

				domainEndpoints := b.ListVpcEndpointsForDomain(ctx, "vpc-domain")
				require.Len(t, domainEndpoints, 1)

				// vpcAccess (raw map) must be preserved.
				access, err := b.ListVpcEndpointAccess(ctx, "vpc-domain")
				require.NoError(t, err)
				assert.Equal(t, []string{"555566667777"}, access)
			},
		},
		{
			name: "reserved_instance_preserved",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				offerings := b.DescribeReservedElasticsearchInstanceOfferings()
				require.NotEmpty(t, offerings)

				_, err := b.PurchaseReservedElasticsearchInstanceOffering(
					ctx, offerings[0].OfferingID, "my-reservation", 2,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				ctx := context.Background()

				instances := b.DescribeReservedElasticsearchInstances(ctx)
				require.Len(t, instances, 1)
				assert.Equal(t, "my-reservation", instances[0].ReservationName)
				assert.Equal(t, 2, instances[0].Count)
			},
		},
		{
			name: "multi_region_isolation",
			setup: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				for _, region := range []string{"us-east-1", "us-west-2"} {
					ctx := elasticsearch.WithRegionForTest(context.Background(), region)

					_, err := b.CreateDomain(ctx, elasticsearch.CreateDomainInput{Name: "shared-name"})
					require.NoError(t, err)
				}
			},
			verify: func(t *testing.T, b *elasticsearch.InMemoryBackend) {
				t.Helper()

				require.Equal(t, 2, b.DomainCount())

				for _, region := range []string{"us-east-1", "us-west-2"} {
					ctx := elasticsearch.WithRegionForTest(context.Background(), region)

					d, err := b.DescribeDomain(ctx, "shared-name")
					require.NoError(t, err)
					assert.Contains(t, d.ARN, region)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns no
// error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDomain(t.Context(), elasticsearch.CreateDomainInput{Name: "seed-domain"})
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.DomainCount())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape) decodes
// with Version == 0, which mismatches elasticsearchSnapshotVersion and is
// discarded the same way any other incompatible version is -- not partially
// applied.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDomain(t.Context(), elasticsearch.CreateDomainInput{Name: "seed-domain"})
	require.NoError(t, err)

	// Pre-Phase-3.3 shape: plain region-nested resource maps, no "version" or
	// "tables" key.
	err = b.Restore(
		t.Context(),
		[]byte(`{"domains":{"us-east-1":{"seed-domain":{"name":"seed-domain"}}}}`),
	)
	require.NoError(t, err)

	assert.Equal(t, 0, b.DomainCount())
}

// TestHandler_SnapshotRestoreDelegate verifies that Handler.Snapshot/Restore
// delegate to the backend, matching the codecommit/codepipeline/emr pattern.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(b)

	_, err := b.CreateDomain(t.Context(), elasticsearch.CreateDomainInput{Name: "handler-domain"})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := elasticsearch.NewHandler(b2)

	require.NoError(t, h2.Restore(t.Context(), snap))
	assert.Equal(t, 1, b2.DomainCount())
}
