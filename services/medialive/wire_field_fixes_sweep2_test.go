package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	medialivesdktypes "github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestDescribeOffering_ResourceSpecification_SpecialFeature_RealClient covers
// gopherstack-6flj: DescribeOfferingOutput.ResourceSpecification is
// types.ReservationResourceSpecification (medialive@v1.101.4 types/types.go),
// which declares SpecialFeature as a real member. The backend's
// OfferingResourceSpecification model already tracks SpecialFeature (it's
// even read as a ListReservations query filter), but toOfferingOutput's
// manually-built resourceSpecification map never included the "specialFeature"
// key, so a real client's SpecialFeature was always empty regardless of the
// tracked value.
func TestDescribeOffering_ResourceSpecification_SpecialFeature_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	out, err := client.DescribeOffering(ctx, &medialivesdk.DescribeOfferingInput{
		OfferingId: aws.String("87654321"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ResourceSpecification)
	assert.Equal(t, "AUDIO_NORMALIZATION", string(out.ResourceSpecification.SpecialFeature))
}

// TestDescribeReservation_ResourceSpecification_SpecialFeature_RealClient
// covers the same gap on the sibling Reservation path (toReservationOutput
// shares the identical bug, since Reservation.ResourceSpecification is
// copied wholesale from the purchased Offering at PurchaseOffering time --
// reservations.go's PurchaseOffering).
func TestDescribeReservation_ResourceSpecification_SpecialFeature_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	purchased, err := client.PurchaseOffering(ctx, &medialivesdk.PurchaseOfferingInput{
		OfferingId: aws.String("87654321"),
		Count:      aws.Int32(1),
		Name:       aws.String("r1"),
	})
	require.NoError(t, err)
	require.NotNil(t, purchased.Reservation.ResourceSpecification)
	assert.Equal(
		t,
		"AUDIO_NORMALIZATION",
		string(purchased.Reservation.ResourceSpecification.SpecialFeature),
	)

	described, err := client.DescribeReservation(ctx, &medialivesdk.DescribeReservationInput{
		ReservationId: purchased.Reservation.ReservationId,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ResourceSpecification)
	assert.Equal(t, "AUDIO_NORMALIZATION", string(described.ResourceSpecification.SpecialFeature))
}

// TestListInputSecurityGroups_Tags_RealClient covers gopherstack-21my:
// ListInputSecurityGroupsOutput reuses types.InputSecurityGroup itself for
// each item (no separate summary shape), which declares Tags as a real
// member (medialive@v1.101.4 types/types.go). The backend tracks tags on
// InputSecurityGroup and the singular DescribeInputSecurityGroup emits them,
// but ListInputSecurityGroups's per-item map never included "tags", so a
// real client's per-group Tags was always empty regardless of what was set
// at creation.
func TestListInputSecurityGroups_Tags_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateInputSecurityGroup(ctx, &medialivesdk.CreateInputSecurityGroupInput{
		WhitelistRules: []medialivesdktypes.InputWhitelistRuleCidr{
			{Cidr: aws.String("10.0.0.0/24")},
		},
		Tags: map[string]string{"env": "prod", "team": "video"},
	})
	require.NoError(t, err)

	out, err := client.ListInputSecurityGroups(ctx, &medialivesdk.ListInputSecurityGroupsInput{})
	require.NoError(t, err)
	require.Len(t, out.InputSecurityGroups, 1)
	assert.Equal(
		t,
		map[string]string{"env": "prod", "team": "video"},
		out.InputSecurityGroups[0].Tags,
	)
}

// TestListMultiplexes_SettingsAndTags_RealClient covers gopherstack-21my:
// ListMultiplexesOutput items use types.MultiplexSummary, which declares
// both MultiplexSettings (*types.MultiplexSettingsSummary, carrying
// TransportStreamBitrate) and Tags as real members (medialive@v1.101.4
// types/types.go). The backend tracks both on Multiplex and the singular
// DescribeMultiplex emits them, but ListMultiplexes's per-item map never
// included either key, so a real client's MultiplexSettings was always nil
// and Tags always empty regardless of what CreateMultiplex set.
func TestListMultiplexes_SettingsAndTags_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateMultiplex(ctx, &medialivesdk.CreateMultiplexInput{
		Name:              aws.String("mux1"),
		AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		MultiplexSettings: &medialivesdktypes.MultiplexSettings{
			TransportStreamBitrate: aws.Int32(12345678),
			TransportStreamId:      aws.Int32(1),
		},
		Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	out, err := client.ListMultiplexes(ctx, &medialivesdk.ListMultiplexesInput{})
	require.NoError(t, err)
	require.Len(t, out.Multiplexes, 1)
	require.NotNil(t, out.Multiplexes[0].MultiplexSettings)
	assert.Equal(t, int32(12345678), aws.ToInt32(out.Multiplexes[0].MultiplexSettings.TransportStreamBitrate))
	assert.Equal(t, map[string]string{"env": "prod"}, out.Multiplexes[0].Tags)
}
