package appstream_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestDescribeSessions_Limit_PaginatesRealState proves DescribeSessionsInput's
// Limit (reqfielddiff slice 5, gopherstack-xhu2t) is honoured: previously
// DescribeSessions had no pagination at all and always returned every
// matching session in one call.
func TestDescribeSessions_Limit_PaginatesRealState(t *testing.T) {
	t.Parallel()

	client := newTestAppStreamClient(t, appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1")))
	ctx := t.Context()

	_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("s5-sess-stack")})
	require.NoError(t, err)

	_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
		Name:         aws.String("s5-sess-fleet"),
		InstanceType: aws.String("stream.standard.medium"),
		ImageName:    aws.String("some-image"),
		ComputeCapacity: &types.ComputeCapacity{
			DesiredInstances: aws.Int32(1),
		},
	})
	require.NoError(t, err)

	const total = 5

	for i := range total {
		_, err = client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
			StackName: aws.String("s5-sess-stack"),
			FleetName: aws.String("s5-sess-fleet"),
			UserId:    aws.String(fmt.Sprintf("s5-sess-user-%d", i)),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
		StackName: aws.String("s5-sess-stack"),
		FleetName: aws.String("s5-sess-fleet"),
		Limit:     aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.Sessions, 2)
	require.NotNil(t, page1.NextToken, "a page short of the full result must carry a continuation token")

	page2, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
		StackName: aws.String("s5-sess-stack"),
		FleetName: aws.String("s5-sess-fleet"),
		Limit:     aws.Int32(2),
		NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Sessions, 2)

	page3, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
		StackName: aws.String("s5-sess-stack"),
		FleetName: aws.String("s5-sess-fleet"),
		Limit:     aws.Int32(2),
		NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Sessions, 1)
	assert.Nil(t, page3.NextToken, "the final page must not carry a continuation token")

	seen := map[string]bool{}
	for _, s := range append(append(page1.Sessions, page2.Sessions...), page3.Sessions...) {
		seen[aws.ToString(s.Id)] = true
	}
	assert.Len(t, seen, total, "pagination must be duplicate-free and complete")
}

// TestAppBlockBuilder_EnableDefaultInternetAccess_SurvivesWireConversion
// proves CreateAppBlockBuilderInput/UpdateAppBlockBuilderInput's
// EnableDefaultInternetAccess (reqfielddiff slice 5) is stored and echoed
// back -- previously undeclared anywhere in this op family (a different
// field of the same name on Fleet/ImageBuilder inputs was already handled,
// which is what let this one go unnoticed).
func TestAppBlockBuilder_EnableDefaultInternetAccess_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestAppStreamClient(t, appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1")))
	ctx := t.Context()

	createOut, err := client.CreateAppBlockBuilder(ctx, &appstreamsdk.CreateAppBlockBuilderInput{
		Name:                        aws.String("s5-appblock-builder"),
		Platform:                    types.AppBlockBuilderPlatformTypeWindowsServer2019,
		InstanceType:                aws.String("stream.standard.medium"),
		EnableDefaultInternetAccess: aws.Bool(true),
		VpcConfig: &types.VpcConfig{
			SubnetIds: []string{"subnet-1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.AppBlockBuilder.EnableDefaultInternetAccess)
	assert.True(t, *createOut.AppBlockBuilder.EnableDefaultInternetAccess)

	updateOut, err := client.UpdateAppBlockBuilder(ctx, &appstreamsdk.UpdateAppBlockBuilderInput{
		Name:                        aws.String("s5-appblock-builder"),
		EnableDefaultInternetAccess: aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.AppBlockBuilder.EnableDefaultInternetAccess)
	assert.False(t, *updateOut.AppBlockBuilder.EnableDefaultInternetAccess)
}
