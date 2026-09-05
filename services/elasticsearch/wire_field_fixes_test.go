package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// Test_SDKRoundTrip_CreateOutboundCrossClusterSearchConnection_DomainInfo proves two
// independent bugs are fixed. First, CreateOutboundCrossClusterSearchConnectionInput's
// SourceDomainInfo/DestinationDomainInfo (both required members,
// api_op_CreateOutboundCrossClusterSearchConnection.go) round-trip correctly -- the handler
// previously decoded the request body into fields tagged "LocalDomainInfo"/"RemoteDomainInfo",
// names copied from this package's own internal OutboundConnection struct rather than the real
// wire shape (deserializers.go:13122's
// awsRestjson1_deserializeDocumentOutboundCrossClusterSearchConnection only recognizes
// "SourceDomainInfo"/"DestinationDomainInfo"); the sibling InboundConnection type already used
// the correct names. Second, CreateOutboundCrossClusterSearchConnectionOutput is flat --
// unlike its Delete/Accept/Reject siblings, it has no CrossClusterSearchConnection wrapper
// (deserializers.go:1253) -- but the handler wrapped it the same way as those siblings, so a
// real client's ConnectionAlias/ConnectionStatus/CrossClusterSearchConnectionId/
// SourceDomainInfo/DestinationDomainInfo were all nested one level too deep to ever decode.
func Test_SDKRoundTrip_CreateOutboundCrossClusterSearchConnection_DomainInfo(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)
	ctx := t.Context()

	out, err := client.CreateOutboundCrossClusterSearchConnection(ctx,
		&elasticsearchsdk.CreateOutboundCrossClusterSearchConnectionInput{
			ConnectionAlias: aws.String("rt-outbound-alias"),
			SourceDomainInfo: &types.DomainInformation{
				OwnerId:    aws.String("123456789012"),
				DomainName: aws.String("rt-source-domain"),
				Region:     aws.String("us-east-1"),
			},
			DestinationDomainInfo: &types.DomainInformation{
				OwnerId:    aws.String("999999999999"),
				DomainName: aws.String("rt-dest-domain"),
				Region:     aws.String("eu-west-1"),
			},
		})
	require.NoError(t, err, "CreateOutboundCrossClusterSearchConnection should succeed")

	require.NotNil(
		t, out.CrossClusterSearchConnectionId,
		"CrossClusterSearchConnectionId must be at the response root, not nested",
	)
	require.NotNil(t, out.SourceDomainInfo, "SourceDomainInfo must round-trip, not be silently dropped")
	require.NotNil(t, out.DestinationDomainInfo, "DestinationDomainInfo must round-trip, not be silently dropped")
	assert.Equal(t, "rt-source-domain", aws.ToString(out.SourceDomainInfo.DomainName))
	assert.Equal(t, "123456789012", aws.ToString(out.SourceDomainInfo.OwnerId))
	assert.Equal(t, "rt-dest-domain", aws.ToString(out.DestinationDomainInfo.DomainName))
	assert.Equal(t, "999999999999", aws.ToString(out.DestinationDomainInfo.OwnerId))

	descOut, err := client.DescribeOutboundCrossClusterSearchConnections(ctx,
		&elasticsearchsdk.DescribeOutboundCrossClusterSearchConnectionsInput{})
	require.NoError(t, err, "DescribeOutboundCrossClusterSearchConnections should succeed")
	require.Len(t, descOut.CrossClusterSearchConnections, 1)

	described := descOut.CrossClusterSearchConnections[0]
	require.NotNil(t, described.SourceDomainInfo)
	require.NotNil(t, described.DestinationDomainInfo)
	assert.Equal(t, "rt-source-domain", aws.ToString(described.SourceDomainInfo.DomainName))
	assert.Equal(t, "rt-dest-domain", aws.ToString(described.DestinationDomainInfo.DomainName))

	delOut, err := client.DeleteOutboundCrossClusterSearchConnection(ctx,
		&elasticsearchsdk.DeleteOutboundCrossClusterSearchConnectionInput{
			CrossClusterSearchConnectionId: out.CrossClusterSearchConnectionId,
		})
	require.NoError(t, err, "DeleteOutboundCrossClusterSearchConnection should succeed")
	require.NotNil(t, delOut.CrossClusterSearchConnection)
	assert.Equal(t, "rt-source-domain", aws.ToString(delOut.CrossClusterSearchConnection.SourceDomainInfo.DomainName))
}

// TestDescribeElasticsearchDomainConfig_ColdStorageOptions_RealClient covers
// gopherstack-y1zn. buildDomainConfigOutput emitted a flat
// "ColdStorageEnabled" boolean; types.ColdStorageOptions
// (elasticsearchservice@v1.45.4 deserializers.go) wraps Enabled in a nested
// object under "ColdStorageOptions" -- there is no flat member. A typed
// client's ClusterConfig.ColdStorageOptions stays nil against the flat key.
func TestDescribeElasticsearchDomainConfig_ColdStorageOptions_RealClient(t *testing.T) {
	t.Parallel()

	b := elasticsearch.NewInMemoryBackend("123456789012", "us-east-1")
	h := elasticsearch.NewHandler(b)
	client := newTestElasticsearchClient(t, h)
	ctx := t.Context()

	b.AddDomainInternal(ctx, elasticsearch.Domain{
		Name:                 "y1zn-cs-domain",
		ARN:                  "arn:aws:es:us-east-1:123456789012:domain/y1zn-cs-domain",
		ElasticsearchVersion: "7.10",
		Status:               "Active",
		ClusterConfig:        elasticsearch.ClusterConfig{ColdStorageEnabled: true},
	})

	out, err := client.DescribeElasticsearchDomainConfig(ctx, &elasticsearchsdk.DescribeElasticsearchDomainConfigInput{
		DomainName: aws.String("y1zn-cs-domain"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DomainConfig)
	require.NotNil(t, out.DomainConfig.ElasticsearchClusterConfig)
	require.NotNil(t, out.DomainConfig.ElasticsearchClusterConfig.Options)
	require.NotNil(t, out.DomainConfig.ElasticsearchClusterConfig.Options.ColdStorageOptions,
		"ColdStorageOptions must decode -- it is a nested object, not a flat ColdStorageEnabled key")
	assert.True(t, aws.ToBool(out.DomainConfig.ElasticsearchClusterConfig.Options.ColdStorageOptions.Enabled))
}

// TestDissociatePackage_DomainPackageStatus_RealSDKClient proves
// DomainPackageDetails.DomainPackageStatus (elasticsearchservice@v1.45.4
// types/enums.go:189-198) decodes as the real
// types.DomainPackageStatusDissociating member, not the non-member string
// "DISSOCIATED" the handler previously emitted -- the enum only has
// ASSOCIATING/ASSOCIATION_FAILED/ACTIVE/DISSOCIATING/DISSOCIATION_FAILED,
// no terminal "DISSOCIATED". A typed client decodes any string into
// DomainPackageStatus without error, so the wrong value produced no decode
// failure.
func TestDissociatePackage_DomainPackageStatus_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)
	ctx := t.Context()

	const domainName = "rt-dissociate-domain"

	_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err, "CreateElasticsearchDomain should succeed")

	pkgOut, err := client.CreatePackage(ctx, &elasticsearchsdk.CreatePackageInput{
		PackageName: aws.String("rt-dissociate-package"),
		PackageType: types.PackageTypeTxtDictionary,
		PackageSource: &types.PackageSource{
			S3BucketName: aws.String("rt-dissociate-bucket"),
			S3Key:        aws.String("dict.txt"),
		},
	})
	require.NoError(t, err, "CreatePackage should succeed")

	_, err = client.AssociatePackage(ctx, &elasticsearchsdk.AssociatePackageInput{
		DomainName: aws.String(domainName),
		PackageID:  pkgOut.PackageDetails.PackageID,
	})
	require.NoError(t, err, "AssociatePackage should succeed")

	out, err := client.DissociatePackage(ctx, &elasticsearchsdk.DissociatePackageInput{
		DomainName: aws.String(domainName),
		PackageID:  pkgOut.PackageDetails.PackageID,
	})
	require.NoError(t, err, "DissociatePackage should succeed")

	require.NotNil(t, out.DomainPackageDetails)
	assert.Equal(t, types.DomainPackageStatusDissociating, out.DomainPackageDetails.DomainPackageStatus)
}
