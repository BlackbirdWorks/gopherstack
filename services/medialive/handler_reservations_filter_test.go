package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListReservations_RealClient_FilterByCodec drives ListReservations
// through the real aws-sdk-go-v2 client with the "codec" query filter
// (ListReservationsInput.Codec, api_op_ListReservations.go, bound as
// httpQuery in awsRestjson1_serializeOpHttpBindingsListReservationsInput).
// The handler read only maxResults/nextToken and discarded every other
// ListReservationsInput filter entirely, so a client asking for AVC
// reservations got HEVC ones back too. Reservations inherit their
// ResourceSpecification (codec/resolution/resourceType/etc.) from the
// offering purchased, which this backend does track per reservation --
// unlike ChannelClass (never modeled on Offering/Reservation at all, left
// as a disclosed gap) -- and an account can purchase an unbounded number of
// reservations, so this is not the "at most a few values" case that
// justifies leaving a filter unimplemented.
func TestListReservations_RealClient_FilterByCodec(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMediaLiveClient(t, h)

	avc, err := client.PurchaseOffering(t.Context(), &medialivesdk.PurchaseOfferingInput{
		OfferingId: aws.String("87654321"), // HD AVC output
		Count:      aws.Int32(1),
		Name:       aws.String("avc-reservation"),
	})
	require.NoError(t, err)

	hevc, err := client.PurchaseOffering(t.Context(), &medialivesdk.PurchaseOfferingInput{
		OfferingId: aws.String("12345678"), // UHD HEVC output
		Count:      aws.Int32(1),
		Name:       aws.String("hevc-reservation"),
	})
	require.NoError(t, err)

	out, err := client.ListReservations(t.Context(), &medialivesdk.ListReservationsInput{
		Codec: aws.String("AVC"),
	})
	require.NoError(t, err)
	require.Len(t, out.Reservations, 1)
	assert.Equal(t, aws.ToString(avc.Reservation.ReservationId), aws.ToString(out.Reservations[0].ReservationId))
	assert.NotEqual(t, aws.ToString(hevc.Reservation.ReservationId), aws.ToString(out.Reservations[0].ReservationId))
}
