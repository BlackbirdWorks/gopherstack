package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListHostedZones_DelegationSetIdFilter verifies ListHostedZones'
// DelegationSetId param (route53@v1.65.6 api_op_ListHostedZones.go) restricts
// results to zones associated with that reusable delegation set.
func TestListHostedZones_DelegationSetIdFilter(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	rds, err := client.CreateReusableDelegationSet(t.Context(), &route53sdk.CreateReusableDelegationSetInput{
		CallerReference: aws.String("lfp-rds-ref"),
	})
	require.NoError(t, err)
	rdsID := aws.ToString(rds.DelegationSet.Id)

	assoc, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("lfp-assoc.example.com."),
		CallerReference: aws.String("lfp-assoc-ref"),
		DelegationSetId: aws.String(rdsID),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assoc.HostedZone.Id)

	_, err = client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("lfp-other.example.com."),
		CallerReference: aws.String("lfp-other-ref"),
	})
	require.NoError(t, err)

	out, err := client.ListHostedZones(t.Context(), &route53sdk.ListHostedZonesInput{
		DelegationSetId: aws.String(rdsID),
	})
	require.NoError(t, err)

	require.Len(t, out.HostedZones, 1)
	require.Equal(t, assocID, aws.ToString(out.HostedZones[0].Id))
}

// TestListHostedZones_HostedZoneTypeFilter verifies the HostedZoneType param
// restricts results to private zones only.
func TestListHostedZones_HostedZoneTypeFilter(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	priv, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("lfp-private.example.com."),
		CallerReference: aws.String("lfp-private-ref"),
		HostedZoneConfig: &types.HostedZoneConfig{
			PrivateZone: true,
		},
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-lfp1234"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	privID := aws.ToString(priv.HostedZone.Id)

	_, err = client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("lfp-public.example.com."),
		CallerReference: aws.String("lfp-public-ref"),
	})
	require.NoError(t, err)

	out, err := client.ListHostedZones(t.Context(), &route53sdk.ListHostedZonesInput{
		HostedZoneType: types.HostedZoneTypePrivateHostedZone,
	})
	require.NoError(t, err)

	require.Len(t, out.HostedZones, 1)
	require.Equal(t, privID, aws.ToString(out.HostedZones[0].Id))
}

// TestListHostedZonesByVPC_MaxItemsTruncates verifies MaxItems
// (route53@v1.65.6 api_op_ListHostedZonesByVPC.go) truncates the returned
// HostedZoneSummaries -- gopherstack-lfp1 parsed it into the response echo
// but never passed it to the backend call.
func TestListHostedZonesByVPC_MaxItemsTruncates(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	vpcID := "vpc-lfp5678"
	for i := range 3 {
		_, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("lfp-vpc" + string(rune('a'+i)) + ".example.com."),
			CallerReference: aws.String("lfp-vpc-ref-" + string(rune('a'+i))),
			HostedZoneConfig: &types.HostedZoneConfig{
				PrivateZone: true,
			},
			VPC: &types.VPC{
				VPCId:     aws.String(vpcID),
				VPCRegion: types.VPCRegionUsEast1,
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListHostedZonesByVPC(t.Context(), &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String(vpcID),
		VPCRegion: types.VPCRegionUsEast1,
		MaxItems:  aws.Int32(2),
	})
	require.NoError(t, err)

	require.Len(t, out.HostedZoneSummaries, 2)
}
