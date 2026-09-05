package memorydb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// newMemorydbSDKClient stands up a real aws-sdk-go-v2 memorydb client against
// an httptest server running h, wired through the same pkgs/service
// registry/router used in production. It signs requests for us-east-1
// unless an explicit region is passed, so a caller can stand up a second
// client signed for a different region against the same handler/backend to
// prove cross-region isolation.
func newMemorydbSDKClient(t *testing.T, h *memorydb.Handler, region ...string) *memorydbsdk.Client {
	t.Helper()

	signingRegion := "us-east-1"
	if len(region) > 0 {
		signingRegion = region[0]
	}

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(signingRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return memorydbsdk.NewFromConfig(cfg, func(o *memorydbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDescribeClusters_IpDiscovery_RealClient proves Cluster.IpDiscovery
// round-trips to a real client. The wire key was "IPDiscovery" (wrong case);
// awsjson1.1 is case-sensitive on the client's own deserializer (an exact
// Go switch-case match, confirmed in deserializers.go), so the real key is
// "IpDiscovery" and every prior response silently zeroed this field for any
// real caller.
func TestDescribeClusters_IpDiscovery_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("wire-cluster"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	out, err := client.DescribeClusters(ctx, &memorydbsdk.DescribeClustersInput{
		ClusterName: aws.String("wire-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.Clusters, 1)
	require.NotEmpty(t, out.Clusters[0].IpDiscovery, "Cluster.IpDiscovery must round-trip under its real wire key")
}

// TestDescribeMultiRegionParameters_RealClient proves DescribeMultiRegionParameters
// works end-to-end for a real client. Two independent wire-key bugs were
// stacked here: the request's group-name filter was read under
// "ParameterGroupName" (the real key is "MultiRegionParameterGroupName", a
// required field -- confirmed via api_op_DescribeMultiRegionParameters.go
// and its serializer), so a real client's request always decoded an empty
// group name and failed; and the response's list was emitted under
// "Parameters" instead of the real "MultiRegionParameters", so even a
// request that got through would come back empty.
func TestDescribeMultiRegionParameters_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	out, err := client.DescribeMultiRegionParameters(ctx, &memorydbsdk.DescribeMultiRegionParametersInput{
		MultiRegionParameterGroupName: aws.String("default.memorydb-redis7.multiregion"),
	})
	require.NoError(t, err)
	require.NotEmpty(
		t, out.MultiRegionParameters, "a real client's request/response must round-trip under the real wire keys",
	)
}

// TestDescribeMultiRegionParameterGroups_NameFilter_RealClient proves the
// MultiRegionParameterGroupName filter on DescribeMultiRegionParameterGroups
// is honored. Same wrong request key as DescribeMultiRegionParameters above,
// but optional on this op, so the old bug was silent: a real client's name
// filter was always ignored and every group came back instead of one.
func TestDescribeMultiRegionParameterGroups_NameFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	out, err := client.DescribeMultiRegionParameterGroups(ctx, &memorydbsdk.DescribeMultiRegionParameterGroupsInput{
		MultiRegionParameterGroupName: aws.String("default.memorydb-redis7.multiregion"),
	})
	require.NoError(t, err)
	require.Len(
		t, out.MultiRegionParameterGroups, 1, "the name filter must scope the result to the one requested group",
	)
}

// TestCreateMultiRegionCluster_NumShards_RealClient proves NumShards is read
// from a real client's CreateMultiRegionCluster request (previously not even
// in the request struct -- a discarded input) and surfaces back as
// NumberOfShards on DescribeMultiRegionClusters (previously never modeled on
// the response type at all).
func TestCreateMultiRegionCluster_NumShards_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	created, err := client.CreateMultiRegionCluster(ctx, &memorydbsdk.CreateMultiRegionClusterInput{
		MultiRegionClusterNameSuffix: aws.String("wire-numshards"),
		NodeType:                     aws.String("db.r6g.large"),
		NumShards:                    aws.Int32(3),
	})
	require.NoError(t, err)
	require.EqualValues(t, 3, aws.ToInt32(created.MultiRegionCluster.NumberOfShards))

	out, err := client.DescribeMultiRegionClusters(ctx, &memorydbsdk.DescribeMultiRegionClustersInput{
		MultiRegionClusterName: created.MultiRegionCluster.MultiRegionClusterName,
	})
	require.NoError(t, err)
	require.Len(t, out.MultiRegionClusters, 1)
	require.EqualValues(t, 3, aws.ToInt32(out.MultiRegionClusters[0].NumberOfShards))
}

// TestCreateSnapshot_ClusterConfigMultiRegionFields_RealClient proves
// ClusterConfiguration.MultiRegionClusterName/MultiRegionParameterGroupName
// round-trip on a snapshot taken from a cluster that belongs to a
// multi-Region cluster. Both are real types.ClusterConfiguration members
// (confirmed in types.go) that were never modeled at all.
func TestCreateSnapshot_ClusterConfigMultiRegionFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	mrc, err := client.CreateMultiRegionCluster(ctx, &memorydbsdk.CreateMultiRegionClusterInput{
		MultiRegionClusterNameSuffix:  aws.String("wire-snap-mrc"),
		NodeType:                      aws.String("db.r6g.large"),
		MultiRegionParameterGroupName: aws.String("default.memorydb-redis7.multiregion"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName:            aws.String("wire-snap-cluster"),
		NodeType:               aws.String("db.r6g.large"),
		ACLName:                aws.String("open-access"),
		MultiRegionClusterName: mrc.MultiRegionCluster.MultiRegionClusterName,
	})
	require.NoError(t, err)

	snap, err := client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName:  aws.String("wire-snap-cluster"),
		SnapshotName: aws.String("wire-snap"),
	})
	require.NoError(t, err)
	require.NotNil(t, snap.Snapshot.ClusterConfiguration)
	require.Equal(t,
		aws.ToString(mrc.MultiRegionCluster.MultiRegionClusterName),
		aws.ToString(snap.Snapshot.ClusterConfiguration.MultiRegionClusterName),
	)
	require.Equal(t,
		"default.memorydb-redis7.multiregion",
		aws.ToString(snap.Snapshot.ClusterConfiguration.MultiRegionParameterGroupName),
	)
}

// TestDescribeReservedNodes_DurationAndOfferingIDFilters_RealClient proves
// Duration and ReservedNodesOfferingId are honored as request filters on
// DescribeReservedNodes -- both real DescribeReservedNodesInput members
// (confirmed via api_op_DescribeReservedNodes.go) that were never modeled
// on the request at all.
func TestDescribeReservedNodes_DurationAndOfferingIDFilters_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	offerings, err := client.DescribeReservedNodesOfferings(ctx, &memorydbsdk.DescribeReservedNodesOfferingsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.ReservedNodesOfferings)
	offeringID := offerings.ReservedNodesOfferings[0].ReservedNodesOfferingId

	purchased, err := client.PurchaseReservedNodesOffering(ctx, &memorydbsdk.PurchaseReservedNodesOfferingInput{
		ReservedNodesOfferingId: offeringID,
	})
	require.NoError(t, err)

	out, err := client.DescribeReservedNodes(ctx, &memorydbsdk.DescribeReservedNodesInput{
		ReservedNodesOfferingId: offeringID,
	})
	require.NoError(t, err)
	require.Len(t, out.ReservedNodes, 1)
	require.Equal(
		t, aws.ToString(purchased.ReservedNode.ReservationId), aws.ToString(out.ReservedNodes[0].ReservationId),
	)

	none, err := client.DescribeReservedNodes(ctx, &memorydbsdk.DescribeReservedNodesInput{
		ReservedNodesOfferingId: aws.String("no-such-offering-id"),
	})
	require.NoError(t, err)
	require.Empty(t, none.ReservedNodes, "an unmatched ReservedNodesOfferingId filter must exclude every reservation")
}

// TestDescribeEngineVersions_Pagination_RealClient proves MaxResults/NextToken
// reach the query on DescribeEngineVersions -- previously parsed into the
// request but never consulted by the handler, so every call returned the
// full catalog in one page regardless of MaxResults.
func TestDescribeEngineVersions_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newMemorydbSDKClient(t, h)
	ctx := t.Context()

	full, err := client.DescribeEngineVersions(ctx, &memorydbsdk.DescribeEngineVersionsInput{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(full.EngineVersions), 2, "need at least 2 catalog entries to prove truncation")

	page1, err := client.DescribeEngineVersions(ctx, &memorydbsdk.DescribeEngineVersionsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.EngineVersions, 1)
	require.NotNil(t, page1.NextToken, "a truncated page must carry a NextToken")

	page2, err := client.DescribeEngineVersions(ctx, &memorydbsdk.DescribeEngineVersionsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.EngineVersions, 1)
	require.NotEqual(t,
		aws.ToString(page1.EngineVersions[0].Engine)+aws.ToString(page1.EngineVersions[0].EngineVersion),
		aws.ToString(page2.EngineVersions[0].Engine)+aws.ToString(page2.EngineVersions[0].EngineVersion),
		"the second page must resume after the first, not repeat it",
	)
}
