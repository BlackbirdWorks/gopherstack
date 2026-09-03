package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeVpcEndpointConnections_ServiceIdFilter_RealClient covers
// handleDescribeVpcEndpointConnections. DescribeVpcEndpointConnectionsInput
// has no ServiceId/ServiceIds field at all (ec2@v1.319.1
// api_op_DescribeVpcEndpointConnections.go: only DryRun, Filters, MaxResults,
// NextToken) -- a real client narrows by service only via a "service-id"
// Filter (serializers.go:82487, awsEc2query_serializeOpDocumentDescribeVpcEndpointConnectionsInput).
// The handler instead read a bare "ServiceId.N" list that a real client can
// never send, so the filter was always silently ignored and every call
// returned every connection regardless of service.
func TestDescribeVpcEndpointConnections_ServiceIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	_, err := client.AcceptVpcEndpointConnections(ctx, &ec2sdk.AcceptVpcEndpointConnectionsInput{
		ServiceId:      aws.String("vpce-svc-aaaaaaaa"),
		VpcEndpointIds: []string{"vpce-aaaaaaaa"},
	})
	require.NoError(t, err)

	_, err = client.AcceptVpcEndpointConnections(ctx, &ec2sdk.AcceptVpcEndpointConnectionsInput{
		ServiceId:      aws.String("vpce-svc-bbbbbbbb"),
		VpcEndpointIds: []string{"vpce-bbbbbbbb"},
	})
	require.NoError(t, err)

	out, err := client.DescribeVpcEndpointConnections(ctx, &ec2sdk.DescribeVpcEndpointConnectionsInput{
		Filters: []types.Filter{
			{Name: aws.String("service-id"), Values: []string{"vpce-svc-aaaaaaaa"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.VpcEndpointConnections, 1,
		"service-id filter ignored - DescribeVpcEndpointConnections returned every connection")
	assert.Equal(t, "vpce-svc-aaaaaaaa", aws.ToString(out.VpcEndpointConnections[0].ServiceId))
}

// TestDescribeVpcEndpointConnectionNotifications_IdFilter_RealClient covers
// handleDescribeVpcEndpointConnectionNotifications. ConnectionNotificationId
// on DescribeVpcEndpointConnectionNotificationsInput is a scalar *string
// serialized as a bare "ConnectionNotificationId" key (serializers.go:82458),
// not a list. The handler read it as an indexed list
// ("ConnectionNotificationId.1", parseMemberList), a key a real client never
// sends, so the ID filter was always silently ignored and every call
// returned every notification.
func TestDescribeVpcEndpointConnectionNotifications_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	first, err := client.CreateVpcEndpointConnectionNotification(
		ctx, &ec2sdk.CreateVpcEndpointConnectionNotificationInput{
			ConnectionEvents:          []string{"Accept"},
			ConnectionNotificationArn: aws.String("arn:aws:sns:us-east-1:000000000000:topic-a"),
			ServiceId:                 aws.String("vpce-svc-aaaaaaaa"),
		})
	require.NoError(t, err)

	_, err = client.CreateVpcEndpointConnectionNotification(
		ctx, &ec2sdk.CreateVpcEndpointConnectionNotificationInput{
			ConnectionEvents:          []string{"Accept"},
			ConnectionNotificationArn: aws.String("arn:aws:sns:us-east-1:000000000000:topic-b"),
			ServiceId:                 aws.String("vpce-svc-bbbbbbbb"),
		})
	require.NoError(t, err)

	firstID := first.ConnectionNotification.ConnectionNotificationId

	out, err := client.DescribeVpcEndpointConnectionNotifications(
		ctx, &ec2sdk.DescribeVpcEndpointConnectionNotificationsInput{
			ConnectionNotificationId: firstID,
		})
	require.NoError(t, err)
	require.Len(t, out.ConnectionNotificationSet, 1,
		"ConnectionNotificationId filter ignored - returned every notification")
	assert.Equal(t, aws.ToString(firstID), aws.ToString(out.ConnectionNotificationSet[0].ConnectionNotificationId))
}

// TestDescribeNetworkInsightsAnalyses_PathIdFilter_RealClient covers
// handleDescribeNetworkInsightsAnalyses. NetworkInsightsPathId is a distinct
// scalar filter field on DescribeNetworkInsightsAnalysesInput, serialized as
// a bare "NetworkInsightsPathId" key (serializers.go:79838) alongside the
// NetworkInsightsAnalysisIds list. The handler never read it at all, so
// narrowing analyses to one path was silently ignored.
func TestDescribeNetworkInsightsAnalyses_PathIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	pathA, err := client.CreateNetworkInsightsPath(ctx, &ec2sdk.CreateNetworkInsightsPathInput{
		Source:      aws.String("eni-aaaaaaaaaaaaaaaaa"),
		Destination: aws.String("eni-bbbbbbbbbbbbbbbbb"),
		Protocol:    types.ProtocolTcp,
	})
	require.NoError(t, err)

	pathB, err := client.CreateNetworkInsightsPath(ctx, &ec2sdk.CreateNetworkInsightsPathInput{
		Source:      aws.String("eni-ccccccccccccccccc"),
		Destination: aws.String("eni-ddddddddddddddddd"),
		Protocol:    types.ProtocolTcp,
	})
	require.NoError(t, err)

	_, err = client.StartNetworkInsightsAnalysis(ctx, &ec2sdk.StartNetworkInsightsAnalysisInput{
		NetworkInsightsPathId: pathA.NetworkInsightsPath.NetworkInsightsPathId,
	})
	require.NoError(t, err)

	_, err = client.StartNetworkInsightsAnalysis(ctx, &ec2sdk.StartNetworkInsightsAnalysisInput{
		NetworkInsightsPathId: pathB.NetworkInsightsPath.NetworkInsightsPathId,
	})
	require.NoError(t, err)

	out, err := client.DescribeNetworkInsightsAnalyses(ctx, &ec2sdk.DescribeNetworkInsightsAnalysesInput{
		NetworkInsightsPathId: pathA.NetworkInsightsPath.NetworkInsightsPathId,
	})
	require.NoError(t, err)
	require.Len(t, out.NetworkInsightsAnalyses, 1,
		"NetworkInsightsPathId filter ignored - returned analyses for every path")
	assert.Equal(
		t,
		aws.ToString(pathA.NetworkInsightsPath.NetworkInsightsPathId),
		aws.ToString(out.NetworkInsightsAnalyses[0].NetworkInsightsPathId),
	)
}

// TestDescribeNetworkInsightsAccessScopeAnalyses_ScopeIdFilter_RealClient
// covers handleDescribeNetworkInsightsAccessScopeAnalyses.
// NetworkInsightsAccessScopeId is a distinct scalar filter field on
// DescribeNetworkInsightsAccessScopeAnalysesInput, serialized as a bare
// "NetworkInsightsAccessScopeId" key (serializers.go:79751) alongside the
// NetworkInsightsAccessScopeAnalysisIds list. The handler never read it, so
// narrowing analyses to one access scope was silently ignored.
func TestDescribeNetworkInsightsAccessScopeAnalyses_ScopeIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	scopeA, err := client.CreateNetworkInsightsAccessScope(
		ctx, &ec2sdk.CreateNetworkInsightsAccessScopeInput{},
	)
	require.NoError(t, err)

	scopeB, err := client.CreateNetworkInsightsAccessScope(
		ctx, &ec2sdk.CreateNetworkInsightsAccessScopeInput{},
	)
	require.NoError(t, err)

	_, err = client.StartNetworkInsightsAccessScopeAnalysis(
		ctx, &ec2sdk.StartNetworkInsightsAccessScopeAnalysisInput{
			NetworkInsightsAccessScopeId: scopeA.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId,
		})
	require.NoError(t, err)

	_, err = client.StartNetworkInsightsAccessScopeAnalysis(
		ctx, &ec2sdk.StartNetworkInsightsAccessScopeAnalysisInput{
			NetworkInsightsAccessScopeId: scopeB.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId,
		})
	require.NoError(t, err)

	out, err := client.DescribeNetworkInsightsAccessScopeAnalyses(
		ctx, &ec2sdk.DescribeNetworkInsightsAccessScopeAnalysesInput{
			NetworkInsightsAccessScopeId: scopeA.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId,
		})
	require.NoError(t, err)
	require.Len(t, out.NetworkInsightsAccessScopeAnalyses, 1,
		"NetworkInsightsAccessScopeId filter ignored - returned analyses for every scope")
	assert.Equal(
		t,
		aws.ToString(scopeA.NetworkInsightsAccessScope.NetworkInsightsAccessScopeId),
		aws.ToString(out.NetworkInsightsAccessScopeAnalyses[0].NetworkInsightsAccessScopeId),
	)
}
