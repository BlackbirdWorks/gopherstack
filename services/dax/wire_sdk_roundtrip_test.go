package dax_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	daxtypes "github.com/aws/aws-sdk-go-v2/service/dax/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dax"
)

// newTestDAXSDKClient stands up the real aws-sdk-go-v2 dax client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- so a shape is verified
// by the real client's own deserializer, not gopherstack's own JSON tags.
func newTestDAXSDKClient(t *testing.T, h *dax.Handler) *daxsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return daxsdk.NewFromConfig(cfg, func(o *daxsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDescribeEvents_NodeRebootSourceType_SDKRoundTrip proves that an event
// emitted for a node-level action (RebootNode) carries a SourceType the real
// SDK's types.SourceType enum actually defines. Real DAX's SourceType enum
// (aws-sdk-go-v2/service/dax/types/enums.go) has exactly three values --
// CLUSTER, PARAMETER_GROUP, SUBNET_GROUP -- with no NODE value, so a
// node-level event must be reported under SourceTypeCluster (with the node's
// owning cluster as SourceName), matching how every other cluster-scoped
// event in this backend is already reported.
func TestDescribeEvents_NodeRebootSourceType_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
	h := dax.NewHandler(backend)
	client := newTestDAXSDKClient(t, h)

	const clusterName = "reboot-src-type"

	created, err := client.CreateCluster(t.Context(), &daxsdk.CreateClusterInput{
		ClusterName:       aws.String(clusterName),
		NodeType:          aws.String("dax.r5.large"),
		IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/DAXRole"),
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.Cluster.Nodes)

	nodeID := aws.ToString(created.Cluster.Nodes[0].NodeId)

	_, err = client.RebootNode(t.Context(), &daxsdk.RebootNodeInput{
		ClusterName: aws.String(clusterName),
		NodeId:      aws.String(nodeID),
	})
	require.NoError(t, err)

	events, err := client.DescribeEvents(t.Context(), &daxsdk.DescribeEventsInput{
		SourceName: aws.String(clusterName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, events.Events)

	var rebootEvent *daxtypes.Event

	for i := range events.Events {
		ev := &events.Events[i]
		if ev.Message != nil && aws.ToString(ev.Message) == "Node "+nodeID+" reboot initiated." {
			rebootEvent = ev

			break
		}
	}

	require.NotNil(t, rebootEvent, "expected a reboot-initiated event for node %s", nodeID)
	assert.Equal(t, daxtypes.SourceTypeCluster, rebootEvent.SourceType,
		"node-level events must report SourceType CLUSTER -- the real SourceType "+
			"enum has no NODE value (enums.go), so any other value is a fabricated "+
			"wire value the real service never emits")
}
