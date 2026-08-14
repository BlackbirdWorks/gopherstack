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

// TestRoute53ListOps_MarkerEchoed_RealClient drives three List* operations
// through the real aws-sdk-go-v2 client with an explicit Marker on the
// request and asserts it comes back unchanged.
//
// ListHostedZonesOutput.Marker, ListHealthChecksOutput.Marker, and
// ListReusableDelegationSetsOutput.Marker are all required members
// (api_op_ListHostedZones.go:23-28, api_op_ListHealthChecks.go,
// api_op_ListReusableDelegationSets.go — "For the second and subsequent
// calls ..., Marker is the value that you specified for the marker
// parameter in the request that produced the current response. This
// member is required."). gopherstack's response structs only carried
// NextMarker (the optional next-page cursor) and silently dropped the
// required echo-back of the request's own marker, so the SDK decoded a
// permanently empty string regardless of what was requested — a zero
// value indistinguishable from "no marker was ever sent".
func TestRoute53ListOps_MarkerEchoed_RealClient(t *testing.T) {
	t.Parallel()

	const wantMarker = "sweep2-marker-xyz"

	tests := []struct {
		call func(t *testing.T, client *route53sdk.Client) *string
		name string
	}{
		{
			name: "hostedzones",
			call: func(t *testing.T, client *route53sdk.Client) *string {
				t.Helper()

				out, err := client.ListHostedZones(t.Context(), &route53sdk.ListHostedZonesInput{
					Marker: aws.String(wantMarker),
				})
				require.NoError(t, err)

				return out.Marker
			},
		},
		{
			name: "healthchecks",
			call: func(t *testing.T, client *route53sdk.Client) *string {
				t.Helper()

				out, err := client.ListHealthChecks(t.Context(), &route53sdk.ListHealthChecksInput{
					Marker: aws.String(wantMarker),
				})
				require.NoError(t, err)

				return out.Marker
			},
		},
		{
			name: "reusabledelegationsets",
			call: func(t *testing.T, client *route53sdk.Client) *string {
				t.Helper()

				out, err := client.ListReusableDelegationSets(t.Context(), &route53sdk.ListReusableDelegationSetsInput{
					Marker: aws.String(wantMarker),
				})
				require.NoError(t, err)

				return out.Marker
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend())
			client := newTestRoute53Client(t, h)

			gotMarker := tt.call(t, client)

			require.NotNil(t, gotMarker)
			assert.Equal(t, wantMarker, *gotMarker)
		})
	}
}

// TestListHostedZonesByVPC_MaxItemsPresent_RealClient drives
// ListHostedZonesByVPC through the real client. MaxItems is a required
// member of ListHostedZonesByVPCOutput (api_op_ListHostedZonesByVPC.go:36-40
// — "The value that you specified for MaxItems in the most recent
// ListHostedZonesByVPC request. This member is required."), but
// gopherstack's listHZByVPCResponse struct had no MaxItems field at all, so
// the SDK always decoded a nil *int32 regardless of what was requested.
func TestListHostedZonesByVPC_MaxItemsPresent_RealClient(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("sweep2-vpc.example.com."),
		CallerReference: aws.String("sweep2-vpc-ref"),
		HostedZoneConfig: &types.HostedZoneConfig{
			PrivateZone: true,
		},
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-sweep2"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, zone.HostedZone.Id)

	out, err := client.ListHostedZonesByVPC(t.Context(), &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String("vpc-sweep2"),
		VPCRegion: types.VPCRegionUsEast1,
		MaxItems:  aws.Int32(5),
	})
	require.NoError(t, err)
	require.NotNil(t, out.MaxItems)
	assert.EqualValues(t, 5, *out.MaxItems)
	require.Len(t, out.HostedZoneSummaries, 1)
}
