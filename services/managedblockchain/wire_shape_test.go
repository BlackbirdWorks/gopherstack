package managedblockchain_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	managedblockchainsdk "github.com/aws/aws-sdk-go-v2/service/managedblockchain"
	mbctypes "github.com/aws/aws-sdk-go-v2/service/managedblockchain/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

// newSDKTestClient stands up the real aws-sdk-go-v2 managedblockchain client
// against an httptest server running this package's Handler through the same
// pkgs/service registry/router used in production. Round-tripping through the
// genuine SDK serializer/deserializer -- rather than decoding the raw JSON
// body with ad-hoc structs -- is what actually proves a response is
// wire-compatible (matches services/mediaconvert/wire_shape_test.go's
// pattern).
func newSDKTestClient(t *testing.T, h *managedblockchain.Handler) *managedblockchainsdk.Client {
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

	return managedblockchainsdk.NewFromConfig(cfg, func(o *managedblockchainsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_NodeCloudwatchLogConfig proves that
// Node.LogPublishingConfiguration.Fabric.ChaincodeLogs.Cloudwatch.Enabled
// decodes through the real SDK client, and that UpdateNode's MemberId (sent
// by a real client only in the JSON body, never as a query parameter)
// resolves correctly server-side.
//
// Before this pass's fix, both of these silently broke a real client:
//   - the response nested the flag under "CloudWatch" (capital W) instead of
//     the real "Cloudwatch" key (aws-sdk-go-v2 managedblockchain@v1.34.4
//     awsRestjson1_deserializeDocumentLogConfigurations, deserializers.go:4999,
//     whose case-sensitive switch only matches "Cloudwatch") -- a real client
//     would silently decode ChaincodeLogs.Cloudwatch as nil.
//   - UpdateNode required memberId as a query parameter, but a real client
//     sends MemberId only in the request body (serializers.go:2259 binds no
//     query parameter for it, serializers.go:2285 serializes it into the
//     body) -- a real client's UpdateNode call was rejected outright.
func Test_SDKRoundTrip_NodeCloudwatchLogConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newSDKTestClient(t, h)
	ctx := t.Context()

	netOut, err := client.CreateNetwork(ctx, &managedblockchainsdk.CreateNetworkInput{
		Name:             aws.String("wire-shape-net"),
		Framework:        mbctypes.FrameworkHyperledgerFabric,
		FrameworkVersion: aws.String("2.2"),
		VotingPolicy: &mbctypes.VotingPolicy{
			ApprovalThresholdPolicy: &mbctypes.ApprovalThresholdPolicy{
				ProposalDurationInHours: aws.Int32(24),
				ThresholdPercentage:     aws.Int32(50),
				ThresholdComparator:     mbctypes.ThresholdComparatorGreaterThan,
			},
		},
		MemberConfiguration: &mbctypes.MemberConfiguration{
			Name: aws.String("wire-shape-member"),
			FrameworkConfiguration: &mbctypes.MemberFrameworkConfiguration{
				Fabric: &mbctypes.MemberFabricConfiguration{
					AdminUsername: aws.String("admin"),
					AdminPassword: aws.String("Passw0rd!"),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, netOut.NetworkId)
	require.NotNil(t, netOut.MemberId)

	nodeOut, err := client.CreateNode(ctx, &managedblockchainsdk.CreateNodeInput{
		NetworkId: netOut.NetworkId,
		MemberId:  netOut.MemberId,
		NodeConfiguration: &mbctypes.NodeConfiguration{
			InstanceType:     aws.String("bc.t3.small"),
			AvailabilityZone: aws.String("us-east-1a"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, nodeOut.NodeId)

	_, err = client.UpdateNode(ctx, &managedblockchainsdk.UpdateNodeInput{
		NetworkId: netOut.NetworkId,
		NodeId:    nodeOut.NodeId,
		MemberId:  netOut.MemberId,
		LogPublishingConfiguration: &mbctypes.NodeLogPublishingConfiguration{
			Fabric: &mbctypes.NodeFabricLogPublishingConfiguration{
				ChaincodeLogs: &mbctypes.LogConfigurations{
					Cloudwatch: &mbctypes.LogConfiguration{Enabled: aws.Bool(true)},
				},
			},
		},
	})
	require.NoError(t, err, "UpdateNode must succeed with MemberId carried in the body, as a real client sends it")

	getOut, err := client.GetNode(ctx, &managedblockchainsdk.GetNodeInput{
		NetworkId: netOut.NetworkId,
		NodeId:    nodeOut.NodeId,
		MemberId:  netOut.MemberId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Node)
	require.NotNil(t, getOut.Node.LogPublishingConfiguration)
	require.NotNil(t, getOut.Node.LogPublishingConfiguration.Fabric)
	require.NotNil(t, getOut.Node.LogPublishingConfiguration.Fabric.ChaincodeLogs)
	require.NotNil(t, getOut.Node.LogPublishingConfiguration.Fabric.ChaincodeLogs.Cloudwatch,
		"real client must decode the Cloudwatch-keyed log config, not silently drop it")
	require.NotNil(t, getOut.Node.LogPublishingConfiguration.Fabric.ChaincodeLogs.Cloudwatch.Enabled)
	require.True(t, *getOut.Node.LogPublishingConfiguration.Fabric.ChaincodeLogs.Cloudwatch.Enabled)
}
