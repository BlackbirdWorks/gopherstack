package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDistributions_SummaryHasFullShape proves DistributionSummary
// (the ListDistributions/ListDistributionsBy* item shape) decodes Origins,
// DefaultCacheBehavior, CacheBehaviors, CustomErrorResponses, WebACLId, and
// Staging -- all real members of cloudfront@v1.67.4's types.DistributionSummary,
// four of them required. Origins/DefaultCacheBehavior were declared on the
// wire struct but never populated (always emitted empty); CacheBehaviors,
// CustomErrorResponses, WebACLId, and Staging were missing as struct members
// entirely. A typed decoder silently leaves an unpopulated/absent field at
// its zero value, so only asserting the actual decoded content (not just "no
// error") proves the fix.
func TestListDistributions_SummaryHasFullShape(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	client := newTestCloudFrontClient(t, h)
	ctx := t.Context()

	const (
		originID   = "origin-full-shape"
		pathPatern = "/images/*"
		webACLID   = "web-acl-full-shape"
	)

	createOut, err := client.CreateDistribution(ctx, &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("list-summary-full-shape"),
			Comment:         aws.String("full shape"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String(originID), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String(originID),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
			CacheBehaviors: &types.CacheBehaviors{
				Quantity: aws.Int32(1),
				Items: []types.CacheBehavior{
					{
						PathPattern:          aws.String(pathPatern),
						TargetOriginId:       aws.String(originID),
						ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
					},
				},
			},
			CustomErrorResponses: &types.CustomErrorResponses{
				Quantity: aws.Int32(1),
				Items: []types.CustomErrorResponse{
					{ErrorCode: aws.Int32(404)},
				},
			},
		},
	})
	require.NoError(t, err)
	distID := aws.ToString(createOut.Distribution.Id)

	_, err = client.AssociateDistributionWebACL(ctx, &cfsdk.AssociateDistributionWebACLInput{
		Id:        aws.String(distID),
		WebACLArn: aws.String(webACLID),
	})
	require.NoError(t, err)

	listOut, err := client.ListDistributions(ctx, &cfsdk.ListDistributionsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.DistributionList.Items, 1)

	item := listOut.DistributionList.Items[0]

	require.NotNil(t, item.Origins)
	require.Len(t, item.Origins.Items, 1)
	assert.Equal(t, originID, aws.ToString(item.Origins.Items[0].Id))

	require.NotNil(t, item.DefaultCacheBehavior)
	assert.Equal(t, originID, aws.ToString(item.DefaultCacheBehavior.TargetOriginId))

	require.NotNil(t, item.CacheBehaviors)
	require.Len(t, item.CacheBehaviors.Items, 1)
	assert.Equal(t, pathPatern, aws.ToString(item.CacheBehaviors.Items[0].PathPattern))

	require.NotNil(t, item.CustomErrorResponses)
	require.Len(t, item.CustomErrorResponses.Items, 1)
	assert.Equal(t, int32(404), aws.ToInt32(item.CustomErrorResponses.Items[0].ErrorCode))

	assert.Equal(t, webACLID, aws.ToString(item.WebACLId))
	assert.False(t, aws.ToBool(item.Staging))
}

// TestListDistributions_SummaryStagingReflectsCopy proves DistributionSummary.Staging
// (required) is true only for a distribution created via CopyDistribution,
// never for the primary CreateDistribution-created one.
func TestListDistributions_SummaryStagingReflectsCopy(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	client := newTestCloudFrontClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateDistribution(ctx, &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("list-summary-staging-primary"),
			Comment:         aws.String("primary"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String("o1"), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("o1"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)
	primaryID := aws.ToString(createOut.Distribution.Id)

	copyOut, err := client.CopyDistribution(ctx, &cfsdk.CopyDistributionInput{
		PrimaryDistributionId: aws.String(primaryID),
		CallerReference:       aws.String("list-summary-staging-copy"),
		Staging:               aws.Bool(true),
	})
	require.NoError(t, err)
	stagingID := aws.ToString(copyOut.Distribution.Id)

	listOut, err := client.ListDistributions(ctx, &cfsdk.ListDistributionsInput{})
	require.NoError(t, err)

	byID := map[string]types.DistributionSummary{}
	for _, item := range listOut.DistributionList.Items {
		byID[aws.ToString(item.Id)] = item
	}

	require.Contains(t, byID, primaryID)
	require.Contains(t, byID, stagingID)
	assert.False(t, aws.ToBool(byID[primaryID].Staging))
	assert.True(t, aws.ToBool(byID[stagingID].Staging))
}
