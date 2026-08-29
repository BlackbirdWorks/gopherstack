package elasticsearch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestDescribeInboundCrossClusterSearchConnections_Filters proves the
// operation applies its Filters parameter (Filternames documented on
// api_op_DescribeInboundCrossClusterSearchConnections.go's Input struct)
// instead of always returning every connection, as
// handleDescribeInboundCrossClusterSearchConnections did before this fix
// (it never read the request body at all).
func TestDescribeInboundCrossClusterSearchConnections_Filters(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)

	backend.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{
		ConnectionID:     "cs-match",
		ConnectionStatus: "ACTIVE",
		SourceDomainInfo: elasticsearch.CrossClusterDomainInfo{DomainName: "source-a", OwnerID: "111111111111"},
		DestDomainInfo:   elasticsearch.CrossClusterDomainInfo{DomainName: "dest-a"},
	})
	backend.AddInboundConnectionInternal(context.Background(), elasticsearch.InboundConnection{
		ConnectionID:     "cs-other",
		ConnectionStatus: "ACTIVE",
		SourceDomainInfo: elasticsearch.CrossClusterDomainInfo{DomainName: "source-b", OwnerID: "222222222222"},
		DestDomainInfo:   elasticsearch.CrossClusterDomainInfo{DomainName: "dest-b"},
	})

	out, err := client.DescribeInboundCrossClusterSearchConnections(
		t.Context(), &elasticsearchsdk.DescribeInboundCrossClusterSearchConnectionsInput{
			Filters: []types.Filter{{
				Name:   aws.String("cross-cluster-search-connection-id"),
				Values: []string{"cs-match"},
			}},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.CrossClusterSearchConnections, 1)
	require.Equal(t, "cs-match", aws.ToString(out.CrossClusterSearchConnections[0].CrossClusterSearchConnectionId))
}

// TestDescribeOutboundCrossClusterSearchConnections_Filters proves the
// sibling outbound operation applies its own Filters parameter the same
// way, driven end to end through CreateOutboundCrossClusterSearchConnection.
func TestDescribeOutboundCrossClusterSearchConnections_Filters(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)

	match, err := client.CreateOutboundCrossClusterSearchConnection(
		t.Context(), &elasticsearchsdk.CreateOutboundCrossClusterSearchConnectionInput{
			SourceDomainInfo:      &types.DomainInformation{DomainName: aws.String("local-a")},
			DestinationDomainInfo: &types.DomainInformation{DomainName: aws.String("remote-a")},
			ConnectionAlias:       aws.String("alias-a"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateOutboundCrossClusterSearchConnection(
		t.Context(), &elasticsearchsdk.CreateOutboundCrossClusterSearchConnectionInput{
			SourceDomainInfo:      &types.DomainInformation{DomainName: aws.String("local-b")},
			DestinationDomainInfo: &types.DomainInformation{DomainName: aws.String("remote-b")},
			ConnectionAlias:       aws.String("alias-b"),
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeOutboundCrossClusterSearchConnections(
		t.Context(), &elasticsearchsdk.DescribeOutboundCrossClusterSearchConnectionsInput{
			Filters: []types.Filter{{
				Name:   aws.String("destination-domain-info.domain-name"),
				Values: []string{"remote-a"},
			}},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.CrossClusterSearchConnections, 1)
	require.Equal(t, aws.ToString(match.CrossClusterSearchConnectionId),
		aws.ToString(out.CrossClusterSearchConnections[0].CrossClusterSearchConnectionId))
}

// TestDescribePackages_Filters proves DescribePackages applies its Filters
// parameter (Name in PackageID/PackageName/PackageStatus, per
// types.DescribePackagesFilterName) instead of reading a "PackageIDs" key no
// real client ever sends -- handleDescribePackages's request struct before
// this fix.
func TestDescribePackages_Filters(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)

	first, err := client.CreatePackage(t.Context(), &elasticsearchsdk.CreatePackageInput{
		PackageName: aws.String("pkg-one"),
		PackageType: types.PackageTypeTxtDictionary,
		PackageSource: &types.PackageSource{
			S3BucketName: aws.String("bucket"), S3Key: aws.String("pkg-one.zip"),
		},
	})
	require.NoError(t, err)
	_, err = client.CreatePackage(t.Context(), &elasticsearchsdk.CreatePackageInput{
		PackageName: aws.String("pkg-two"),
		PackageType: types.PackageTypeTxtDictionary,
		PackageSource: &types.PackageSource{
			S3BucketName: aws.String("bucket"), S3Key: aws.String("pkg-two.zip"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribePackages(t.Context(), &elasticsearchsdk.DescribePackagesInput{
		Filters: []types.DescribePackagesFilter{{
			Name:  types.DescribePackagesFilterNamePackageName,
			Value: []string{"pkg-one"},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.PackageDetailsList, 1)
	require.Equal(t, aws.ToString(first.PackageDetails.PackageID), aws.ToString(out.PackageDetailsList[0].PackageID))
}

// TestListDomainNames_EngineTypeFilter proves ListDomainNames applies its
// EngineType query parameter (api_op_ListDomainNames.go's Input doc
// comment) -- this backend only ever manages Elasticsearch-engine domains
// (OpenSearch domains are the separate services/opensearch API), so
// filtering by EngineTypeOpenSearch must return none.
func TestListDomainNames_EngineTypeFilter(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)

	_, err := client.CreateElasticsearchDomain(t.Context(), &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String("engine-filter-domain"),
	})
	require.NoError(t, err)

	openSearch, err := client.ListDomainNames(t.Context(), &elasticsearchsdk.ListDomainNamesInput{
		EngineType: types.EngineTypeOpenSearch,
	})
	require.NoError(t, err)
	require.Empty(t, openSearch.DomainNames)

	elasticsearchOnly, err := client.ListDomainNames(t.Context(), &elasticsearchsdk.ListDomainNamesInput{
		EngineType: types.EngineTypeElasticsearch,
	})
	require.NoError(t, err)
	require.Len(t, elasticsearchOnly.DomainNames, 1)
}
