package redshift_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	redshiftserverlesssdk "github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// newTestServerlessClient stands up the real aws-sdk-go-v2 redshiftserverless
// client against an httptest server running this package's ServerlessHandler,
// wired through the same pkgs/service registry/router used in production --
// the serverless counterpart of newTestRedshiftClient in
// handler_sdk_roundtrip_test.go. Round-tripping through the genuine SDK
// deserializer is what proves the reservation family's wire shape, not just
// that a JSON blob happens to string-match.
func newTestServerlessClient(t *testing.T, h *redshift.ServerlessHandler) *redshiftserverlesssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return redshiftserverlesssdk.NewFromConfig(cfg, func(o *redshiftserverlesssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_ListReservationOfferings locks gopherstack-ztx0's seeded
// catalog: every ReservationOffering member decodes through the real SDK
// deserializer, in deterministic order, for all four (duration, OfferingType)
// combinations AWS's pricing docs enumerate (1yr/3yr x ALL_UPFRONT/NO_UPFRONT).
func TestSDKRoundTrip_ListReservationOfferings(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)

	out, err := client.ListReservationOfferings(t.Context(), &redshiftserverlesssdk.ListReservationOfferingsInput{})
	require.NoError(t, err)
	require.Len(t, out.ReservationOfferingsList, 4)
	assert.Nil(t, out.NextToken)

	wantOfferingIDs := []string{
		"capacity-1yr-all-upfront",
		"capacity-1yr-no-upfront",
		"capacity-3yr-all-upfront",
		"capacity-3yr-no-upfront",
	}

	gotOfferingIDs := make([]string, len(out.ReservationOfferingsList))
	for i, o := range out.ReservationOfferingsList {
		gotOfferingIDs[i] = aws.ToString(o.OfferingId)
	}

	assert.Equal(t, wantOfferingIDs, gotOfferingIDs, "must be returned in the same order on every call")

	oneYearAllUpfront := out.ReservationOfferingsList[0]
	assert.Equal(t, "capacity-1yr-all-upfront", aws.ToString(oneYearAllUpfront.OfferingId))
	assert.Equal(t, types.OfferingTypeAllUpfront, oneYearAllUpfront.OfferingType)
	assert.Equal(t, "USD", aws.ToString(oneYearAllUpfront.CurrencyCode))
	assert.EqualValues(t, 365*24*60*60, oneYearAllUpfront.Duration)
	assert.InDelta(t, 0, oneYearAllUpfront.HourlyCharge, 0, "unverified price left at 0, see PARITY.md")
	assert.InDelta(t, 0, oneYearAllUpfront.UpfrontCharge, 0, "unverified price left at 0, see PARITY.md")

	threeYearNoUpfront := out.ReservationOfferingsList[3]
	assert.Equal(t, "capacity-3yr-no-upfront", aws.ToString(threeYearNoUpfront.OfferingId))
	assert.Equal(t, types.OfferingTypeNoUpfront, threeYearNoUpfront.OfferingType)
	assert.EqualValues(t, 3*365*24*60*60, threeYearNoUpfront.Duration)
}

// TestSDKRoundTrip_ListReservationOfferings_Pagination proves MaxResults/
// NextToken are real, not silently ignored, matching every other serverless
// List op's pagination convention in this package.
func TestSDKRoundTrip_ListReservationOfferings_Pagination(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)

	first, err := client.ListReservationOfferings(t.Context(), &redshiftserverlesssdk.ListReservationOfferingsInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.ReservationOfferingsList, 2)
	require.NotNil(t, first.NextToken)

	second, err := client.ListReservationOfferings(t.Context(), &redshiftserverlesssdk.ListReservationOfferingsInput{
		MaxResults: aws.Int32(2),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.ReservationOfferingsList, 2)
	assert.Nil(t, second.NextToken)

	assert.NotEqual(t,
		aws.ToString(first.ReservationOfferingsList[0].OfferingId),
		aws.ToString(second.ReservationOfferingsList[0].OfferingId),
	)
}

func TestSDKRoundTrip_GetReservationOffering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		wantErr    bool
	}{
		{name: "known offering", offeringID: "capacity-1yr-all-upfront"},
		{name: "unknown offering", offeringID: "no-such-offering", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
			client := newTestServerlessClient(t, h)

			out, err := client.GetReservationOffering(t.Context(), &redshiftserverlesssdk.GetReservationOfferingInput{
				OfferingId: aws.String(tt.offeringID),
			})

			if tt.wantErr {
				require.Error(t, err)

				var notFound *types.ResourceNotFoundException
				require.ErrorAs(t, err, &notFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.offeringID, aws.ToString(out.ReservationOffering.OfferingId))
		})
	}
}

// TestSDKRoundTrip_ReservationLifecycle proves CreateReservation ->
// GetReservation/ListReservations show a consistent object: dates derived
// from the offering's own duration, capacity/status/ARN real, and the
// embedded Offering fully populated (not just an ID reference) -- matching
// Reservation.Offering being a real *ReservationOffering member, not a
// string, on the wire (deserializers.go's "offering" case).
func TestSDKRoundTrip_ReservationLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	before := time.Now().UTC()

	created, err := client.CreateReservation(ctx, &redshiftserverlesssdk.CreateReservationInput{
		Capacity:   64,
		OfferingId: aws.String("capacity-1yr-no-upfront"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Reservation)

	res := created.Reservation
	assert.EqualValues(t, 64, res.Capacity)
	assert.Equal(t, "payment-pending", aws.ToString(res.Status))
	assert.NotEmpty(t, aws.ToString(res.ReservationId))
	assert.Contains(t, aws.ToString(res.ReservationArn), "reservation/")
	require.NotNil(t, res.Offering)
	assert.Equal(t, "capacity-1yr-no-upfront", aws.ToString(res.Offering.OfferingId))
	assert.Equal(t, types.OfferingTypeNoUpfront, res.Offering.OfferingType)

	require.NotNil(t, res.StartDate)
	require.NotNil(t, res.EndDate)
	assert.WithinDuration(t, before, *res.StartDate, 5*time.Second)

	wantEnd := res.StartDate.Add(365 * 24 * time.Hour)
	assert.WithinDuration(t, wantEnd, *res.EndDate, time.Second,
		"EndDate must be StartDate + the offering's own duration (365 days for a 1yr offering)")

	gotten, err := client.GetReservation(ctx, &redshiftserverlesssdk.GetReservationInput{
		ReservationId: res.ReservationId,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(res.ReservationId), aws.ToString(gotten.Reservation.ReservationId))
	assert.Equal(t, res.Capacity, gotten.Reservation.Capacity)
	assert.Equal(t, res.StartDate.Unix(), gotten.Reservation.StartDate.Unix())
	assert.Equal(t, res.EndDate.Unix(), gotten.Reservation.EndDate.Unix())

	listed, err := client.ListReservations(ctx, &redshiftserverlesssdk.ListReservationsInput{})
	require.NoError(t, err)
	require.Len(t, listed.ReservationsList, 1)
	assert.Equal(t, aws.ToString(res.ReservationId), aws.ToString(listed.ReservationsList[0].ReservationId))
}

func TestSDKRoundTrip_CreateReservation_UnknownOffering(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)

	_, err := client.CreateReservation(t.Context(), &redshiftserverlesssdk.CreateReservationInput{
		Capacity:   32,
		OfferingId: aws.String("no-such-offering"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

func TestSDKRoundTrip_GetReservation_UnknownID(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)

	_, err := client.GetReservation(t.Context(), &redshiftserverlesssdk.GetReservationInput{
		ReservationId: aws.String("no-such-reservation"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSDKRoundTrip_ListReservations_Pagination proves ListReservations'
// MaxResults/NextToken page across multiple real reservations, same
// convention as every other serverless List op.
func TestSDKRoundTrip_ListReservations_Pagination(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	for range 3 {
		_, err := client.CreateReservation(ctx, &redshiftserverlesssdk.CreateReservationInput{
			Capacity:   8,
			OfferingId: aws.String("capacity-1yr-all-upfront"),
		})
		require.NoError(t, err)
	}

	first, err := client.ListReservations(ctx, &redshiftserverlesssdk.ListReservationsInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.ReservationsList, 2)
	require.NotNil(t, first.NextToken)

	second, err := client.ListReservations(ctx, &redshiftserverlesssdk.ListReservationsInput{
		MaxResults: aws.Int32(2),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.ReservationsList, 1)
	assert.Nil(t, second.NextToken)
}
