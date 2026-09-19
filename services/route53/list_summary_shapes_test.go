package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for route53's five flagged List ops:
// ListCidrBlocks, ListCidrCollections, ListCidrLocations,
// ListHostedZonesByVPC and ListTrafficPolicies all already emitted exactly
// their real Summary member set, with the correct <member>/named-child
// element wrapping (verified against route53@v1.65.6 deserializers.go) --
// no leaks, no gaps.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("cidr collections, locations, and blocks exact", func(t *testing.T) {
		t.Parallel()

		h := route53.NewHandler(route53.NewInMemoryBackend())
		client := newTestRoute53Client(t, h)
		ctx := t.Context()

		created, err := client.CreateCidrCollection(ctx, &route53sdk.CreateCidrCollectionInput{
			CallerReference: aws.String("ref1"),
			Name:            aws.String("my-cidrs"),
		})
		require.NoError(t, err)

		colsOut, err := client.ListCidrCollections(ctx, &route53sdk.ListCidrCollectionsInput{})
		require.NoError(t, err)
		require.Len(t, colsOut.CidrCollections, 1)
		col := colsOut.CidrCollections[0]
		assert.Equal(t, "my-cidrs", aws.ToString(col.Name))
		assert.NotEmpty(t, aws.ToString(col.Id))
		assert.NotEmpty(t, aws.ToString(col.Arn))
		assert.NotNil(t, col.Version)

		_, err = client.ChangeCidrCollection(ctx, &route53sdk.ChangeCidrCollectionInput{
			Id: created.Collection.Id,
			Changes: []types.CidrCollectionChange{
				{
					Action:       types.CidrCollectionChangeActionPut,
					CidrList:     []string{"10.0.0.0/24"},
					LocationName: aws.String("loc1"),
				},
			},
		})
		require.NoError(t, err)

		locsOut, err := client.ListCidrLocations(ctx, &route53sdk.ListCidrLocationsInput{
			CollectionId: created.Collection.Id,
		})
		require.NoError(t, err)
		require.Len(t, locsOut.CidrLocations, 1)
		assert.Equal(t, "loc1", aws.ToString(locsOut.CidrLocations[0].LocationName))

		blocksOut, err := client.ListCidrBlocks(ctx, &route53sdk.ListCidrBlocksInput{
			CollectionId: created.Collection.Id,
			LocationName: aws.String("loc1"),
		})
		require.NoError(t, err)
		require.Len(t, blocksOut.CidrBlocks, 1)
		assert.Equal(t, "10.0.0.0/24", aws.ToString(blocksOut.CidrBlocks[0].CidrBlock))
		assert.Equal(t, "loc1", aws.ToString(blocksOut.CidrBlocks[0].LocationName))
	})

	t.Run("hosted zones by vpc exact", func(t *testing.T) {
		t.Parallel()

		h := route53.NewHandler(route53.NewInMemoryBackend())
		client := newTestRoute53Client(t, h)
		ctx := t.Context()

		_, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("vpc-summary.example.com."),
			CallerReference: aws.String("ref-vpc-summary"),
			HostedZoneConfig: &types.HostedZoneConfig{
				PrivateZone: true,
			},
			VPC: &types.VPC{
				VPCId:     aws.String("vpc-summary-1"),
				VPCRegion: types.VPCRegionUsEast1,
			},
		})
		require.NoError(t, err)

		out, err := client.ListHostedZonesByVPC(ctx, &route53sdk.ListHostedZonesByVPCInput{
			VPCId:     aws.String("vpc-summary-1"),
			VPCRegion: types.VPCRegionUsEast1,
		})
		require.NoError(t, err)
		require.Len(t, out.HostedZoneSummaries, 1)
		z := out.HostedZoneSummaries[0]
		assert.Equal(t, "vpc-summary.example.com.", aws.ToString(z.Name))
		assert.NotEmpty(t, aws.ToString(z.HostedZoneId))
		require.NotNil(t, z.Owner)
		assert.NotEmpty(t, aws.ToString(z.Owner.OwningAccount))
	})

	t.Run("traffic policies exact", func(t *testing.T) {
		t.Parallel()

		h := route53.NewHandler(route53.NewInMemoryBackend())
		client := newTestRoute53Client(t, h)
		ctx := t.Context()

		doc := `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
			`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

		_, err := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
			Name:     aws.String("tp1"),
			Document: aws.String(doc),
		})
		require.NoError(t, err)

		out, err := client.ListTrafficPolicies(ctx, &route53sdk.ListTrafficPoliciesInput{})
		require.NoError(t, err)
		require.Len(t, out.TrafficPolicySummaries, 1)
		tp := out.TrafficPolicySummaries[0]
		assert.Equal(t, "tp1", aws.ToString(tp.Name))
		assert.NotEmpty(t, aws.ToString(tp.Id))
		assert.Equal(t, int32(1), aws.ToInt32(tp.LatestVersion))
		assert.Equal(t, int32(1), aws.ToInt32(tp.TrafficPolicyCount))
		assert.NotEmpty(t, tp.Type)
	})
}
