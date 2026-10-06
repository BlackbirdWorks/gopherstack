package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListReservations must honour the codec query filter (ListReservationsInput.Codec is httpQuery-bound).
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

func TestListReservations_RealClient_FilterByChannelClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		channelClass string
		want         int
	}{
		{"standard_matches_none", "STANDARD", 0},
		{"single_pipeline_matches_none", "SINGLE_PIPELINE", 0},
		{"unset_matches_all", "", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaLiveClient(t, newTestHandler(t))
			_, err := client.PurchaseOffering(t.Context(), &medialivesdk.PurchaseOfferingInput{
				OfferingId: aws.String("87654321"), Count: aws.Int32(1), Name: aws.String("r"),
			})
			require.NoError(t, err)

			in := &medialivesdk.ListReservationsInput{}
			if tt.channelClass != "" {
				in.ChannelClass = aws.String(tt.channelClass)
			}

			out, err := client.ListReservations(t.Context(), in)
			require.NoError(t, err)
			assert.Len(t, out.Reservations, tt.want)
		})
	}
}
