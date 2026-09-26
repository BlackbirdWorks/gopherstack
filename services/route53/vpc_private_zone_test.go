package route53_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateHostedZone_VPCImpliesPrivateZone covers gopherstack-101r:
// aws_route53_zone sends a request VPC element without ever setting
// HostedZoneConfig.PrivateZone (api_op_CreateHostedZone.go: "If you don't
// specify a comment or the PrivateZone element, omit HostedZoneConfig"),
// so createHostedZone must infer PrivateZone from the presence of VPC, and
// the zone must accept further VPC associations afterward.
func TestCreateHostedZone_VPCImpliesPrivateZone(t *testing.T) {
	t.Parallel()

	client := newTestRoute53Client(t, newHandler(t))
	ctx := t.Context()

	created, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("private.example.com."),
		CallerReference: aws.String("private-zone-ref"),
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-abc123"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.HostedZone.Config)
	assert.True(t, created.HostedZone.Config.PrivateZone)

	zoneID := aws.ToString(created.HostedZone.Id)

	got, err := client.GetHostedZone(ctx, &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
	require.NoError(t, err)
	require.NotNil(t, got.HostedZone.Config)
	assert.True(t, got.HostedZone.Config.PrivateZone)
	require.Len(t, got.VPCs, 1)

	_, err = client.AssociateVPCWithHostedZone(ctx, &route53sdk.AssociateVPCWithHostedZoneInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-def456"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)

	after, err := client.GetHostedZone(ctx, &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
	require.NoError(t, err)
	assert.Len(t, after.VPCs, 2)

	// terraform-provider-aws's aws_route53_zone_association read
	// (zone_association.go findZoneAssociationByThreePartKey) compares
	// ListHostedZonesByVPC's HostedZoneId directly against the resource's
	// bare zone_id attribute (no "/hostedzone/" stripping) -- gopherstack-101r.
	byVPC, err := client.ListHostedZonesByVPC(ctx, &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String("vpc-def456"),
		VPCRegion: types.VPCRegionUsEast1,
	})
	require.NoError(t, err)
	require.Len(t, byVPC.HostedZoneSummaries, 1)
	assert.Equal(t, strings.TrimPrefix(zoneID, "/hostedzone/"), aws.ToString(byVPC.HostedZoneSummaries[0].HostedZoneId))
}
