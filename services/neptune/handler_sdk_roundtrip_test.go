package neptune_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// newTestNeptuneClient stands up the real aws-sdk-go-v2 Neptune client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through the
// genuine SDK serializer/deserializer (rather than string-matching the raw XML
// body, as most other tests in this package do) is what actually proves a
// response is wire-compatible: a response can look plausible as a string yet
// still make the SDK's XML deserializer fail outright (e.g. a missing
// "*Result" element the deserializer unconditionally looks for via
// decoder.GetElement) -- exactly the class of bug this test guards against.
func newTestNeptuneClient(t *testing.T, h *neptune.Handler) *neptunesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return neptunesdk.NewFromConfig(cfg, func(o *neptunesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

const testRegion = "us-east-1"

// Test_SDKRoundTrip_ApplyPendingMaintenanceAction proves the real SDK client
// can deserialize an ApplyPendingMaintenanceAction response. Before the fix,
// the handler returned an empty <ApplyPendingMaintenanceActionResponse/> body
// with no <ApplyPendingMaintenanceActionResult> element; the SDK's generated
// deserializer unconditionally calls decoder.GetElement("...Result") and
// returns a *smithy.DeserializationError when that element is absent, so
// every real client call failed even though gopherstack answered HTTP 200.
func Test_SDKRoundTrip_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	out, err := client.ApplyPendingMaintenanceAction(
		t.Context(),
		&neptunesdk.ApplyPendingMaintenanceActionInput{
			ResourceIdentifier: aws.String("arn:aws:rds:us-east-1:000000000000:cluster:some-cluster"),
			ApplyAction:        aws.String("system-update"),
			OptInType:          aws.String("immediate"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.ResourcePendingMaintenanceActions)
}

// Test_SDKRoundTrip_DeleteDBClusterEndpoint proves the real SDK client can
// deserialize a DeleteDBClusterEndpoint response, and that the deleted
// endpoint's fields (which AWS echoes back in the response) actually come
// through -- the handler used to return an empty body for this op even
// though the real DeleteDBClusterEndpointOutput carries the endpoint details
// as a flat payload with no httpPayload wrapper member.
func Test_SDKRoundTrip_DeleteDBClusterEndpoint(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-cluster"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterEndpoint(ctx, &neptunesdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("rt-endpoint"),
		DBClusterIdentifier:         aws.String("rt-cluster"),
		EndpointType:                aws.String("READER"),
	})
	require.NoError(t, err)

	out, err := client.DeleteDBClusterEndpoint(ctx, &neptunesdk.DeleteDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("rt-endpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBClusterEndpointIdentifier)
	require.Equal(t, "rt-endpoint", *out.DBClusterEndpointIdentifier)
	require.NotNil(t, out.DBClusterIdentifier)
	require.Equal(t, "rt-cluster", *out.DBClusterIdentifier)
}

// Test_SDKRoundTrip_ModifyDBClusterSnapshotAttribute proves the real SDK
// client can deserialize a ModifyDBClusterSnapshotAttribute response and that
// the "restore" attribute's added values actually round-trip through
// Describe -- the handler used to no-op both operations (Modify never
// persisted the values, Describe always returned an empty attribute list)
// and Modify's response body used to omit the required *Result element
// entirely, which -- like ApplyPendingMaintenanceAction above -- makes the
// real SDK client's deserializer fail outright.
func Test_SDKRoundTrip_ModifyDBClusterSnapshotAttribute(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-snap-cluster"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &neptunesdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("rt-snap"),
		DBClusterIdentifier:         aws.String("rt-snap-cluster"),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBClusterSnapshotAttribute(ctx, &neptunesdk.ModifyDBClusterSnapshotAttributeInput{
		DBClusterSnapshotIdentifier: aws.String("rt-snap"),
		AttributeName:               aws.String("restore"),
		ValuesToAdd:                 []string{"123456789012"},
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.DBClusterSnapshotAttributesResult)

	descOut, err := client.DescribeDBClusterSnapshotAttributes(
		ctx, &neptunesdk.DescribeDBClusterSnapshotAttributesInput{
			DBClusterSnapshotIdentifier: aws.String("rt-snap"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, descOut.DBClusterSnapshotAttributesResult)
	found := false
	for _, attr := range descOut.DBClusterSnapshotAttributesResult.DBClusterSnapshotAttributes {
		if aws.ToString(attr.AttributeName) == "restore" {
			found = true
			require.Contains(t, attr.AttributeValues, "123456789012")
		}
	}
	require.True(t, found, "expected a restore attribute in the describe response")
}

// Test_SDKRoundTrip_DescribeDBClusterEndpoints_ListsEndpoints proves the real
// SDK client sees DescribeDBClusterEndpoints' top-level list. The handler
// wrapped each entry in <DBClusterEndpoint>, but the real deserializer
// (neptune@v1.48.4 deserializers.go:12676) matches <DBClusterEndpointList>
// for this field -- unrecognized elements are skipped silently, so a real
// client always saw an empty slice regardless of what was stored.
func Test_SDKRoundTrip_DescribeDBClusterEndpoints_ListsEndpoints(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-ep-cluster"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterEndpoint(ctx, &neptunesdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("rt-endpoint"),
		DBClusterIdentifier:         aws.String("rt-ep-cluster"),
		EndpointType:                aws.String("ANY"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusterEndpoints(ctx, &neptunesdk.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String("rt-ep-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusterEndpoints, 1)
	assert.Equal(t, "rt-endpoint", aws.ToString(out.DBClusterEndpoints[0].DBClusterEndpointIdentifier))
}

// Test_SDKRoundTrip_EventSubscription_SourceIDs proves the real SDK client
// sees an event subscription's SourceIds. The handler wrapped each entry in
// <member>, but the real deserializer (neptune@v1.48.4
// deserializers.go:22089) matches <SourceId> for this field -- unrecognized
// elements are skipped silently, so a real client always saw an empty slice.
func Test_SDKRoundTrip_EventSubscription_SourceIDs(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	out, err := client.CreateEventSubscription(t.Context(), &neptunesdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("rt-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:rt-topic"),
		SourceType:       aws.String("db-cluster"),
		SourceIds:        []string{"rt-source-cluster"},
	})
	require.NoError(t, err)
	require.Len(t, out.EventSubscription.SourceIdsList, 1)
	assert.Equal(t, "rt-source-cluster", out.EventSubscription.SourceIdsList[0])
}

// Test_SDKRoundTrip_DescribeGlobalClusters_ListsClusters proves the real SDK
// client sees DescribeGlobalClusters' top-level list. The handler wrapped
// each entry in <GlobalCluster>, but the real deserializer (neptune@v1.48.4
// deserializers.go:18396) matches <GlobalClusterMember> -- unrecognized
// elements are skipped silently, so a real client always saw an empty slice.
func Test_SDKRoundTrip_DescribeGlobalClusters_ListsClusters(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-gc-source"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateGlobalCluster(ctx, &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("rt-gc"),
		SourceDBClusterIdentifier: aws.String("rt-gc-source"),
	})
	require.NoError(t, err)

	out, err := client.DescribeGlobalClusters(ctx, &neptunesdk.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("rt-gc"),
	})
	require.NoError(t, err)
	require.Len(t, out.GlobalClusters, 1)
	assert.Equal(t, "rt-gc", aws.ToString(out.GlobalClusters[0].GlobalClusterIdentifier))
}

// Test_SDKRoundTrip_RestoreDBClusterFromSnapshot proves the real SDK client's
// RestoreDBClusterFromSnapshotInput.SnapshotIdentifier reaches the backend.
// The real serializer (awsAwsquery_serializeOpDocumentRestoreDBClusterFromSnapshotInput,
// neptune@v1.48.4 serializers.go:7631) encodes the field as "SnapshotIdentifier";
// the handler used to read "DBClusterSnapshotIdentifier" instead (that key is
// valid for CreateDBClusterSnapshot/DescribeDBClusterSnapshots, not this op),
// so every real client's snapshot ID was silently dropped and the restore
// always failed with DBClusterSnapshotNotFoundFault.
func Test_SDKRoundTrip_RestoreDBClusterFromSnapshot(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-restore-source"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &neptunesdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("rt-restore-snap"),
		DBClusterIdentifier:         aws.String("rt-restore-source"),
	})
	require.NoError(t, err)

	out, err := client.RestoreDBClusterFromSnapshot(ctx, &neptunesdk.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier: aws.String("rt-restored"),
		SnapshotIdentifier:  aws.String("rt-restore-snap"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBCluster)
	assert.Equal(t, "rt-restored", aws.ToString(out.DBCluster.DBClusterIdentifier))
}
