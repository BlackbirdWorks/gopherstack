package redshift

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless capacity reservations
//
// Shapes verified against aws-sdk-go-v2/service/redshiftserverless@v1.38.5:
// types.Reservation/types.ReservationOffering (types/types.go),
// awsAwsjson11_deserializeDocumentReservation/ReservationOffering
// (deserializers.go) for wire keys, and api_op_CreateReservation.go/
// api_op_GetReservation.go/api_op_GetReservationOffering.go/
// api_op_ListReservationOfferings.go/api_op_ListReservations.go for the op
// shapes and declared error sets. This closes gopherstack-ztx0, superseding
// the "deliberately deferred" call recorded in PARITY.md's 2026-08-13 pass
// (bd gopherstack-v4wu): that entry's objection was that
// HourlyCharge/UpfrontCharge are free-floating commercial rates with no
// SDK-enumerable catalog -- resolved here by seeding only the
// duration/OfferingType combinations AWS's own docs enumerate and leaving
// the two price fields at their zero value with the citation trail below,
// rather than inventing a plausible-looking number.
// ---------------------------------------------------------------------------

var (
	// ErrReservationSLNotFound is returned when a serverless reservation does
	// not exist.
	ErrReservationSLNotFound = errors.New("ResourceNotFoundException")
	// ErrReservationOfferingSLNotFound is returned when a serverless
	// reservation offering ID matches nothing in the seeded catalog.
	ErrReservationOfferingSLNotFound = errors.New("ResourceNotFoundException")
)

// Reservation represents a Redshift Serverless capacity reservation (the
// "Reservation" shape).
type Reservation struct {
	StartDate      time.Time            `json:"startDate"`
	EndDate        time.Time            `json:"endDate"`
	Offering       *ReservationOffering `json:"offering,omitempty"`
	ReservationArn string               `json:"reservationArn"`
	ReservationID  string               `json:"reservationId"`
	Status         string               `json:"status"`
	Capacity       int32                `json:"capacity"`
}

// ReservationOffering represents the payment schedule for a serverless
// reservation (the "ReservationOffering" shape).
type ReservationOffering struct {
	OfferingID    string  `json:"offeringId"`
	OfferingType  string  `json:"offeringType"`
	CurrencyCode  string  `json:"currencyCode"`
	HourlyCharge  float64 `json:"hourlyCharge"`
	UpfrontCharge float64 `json:"upfrontCharge"`
	Duration      int32   `json:"duration"`
}

const (
	// slResOfferAllUpfront/slResOfferNoUpfront are OfferingType's only two
	// real enum values (types.OfferingType, redshiftserverless@v1.38.5/
	// types/enums.go) -- unlike classic Redshift's ReservedNodeOffering,
	// there is no "Partial Upfront" tier for serverless reservations.
	slResOfferAllUpfront = "ALL_UPFRONT"
	slResOfferNoUpfront  = "NO_UPFRONT"

	// slReservationStatusPaymentPending is Reservation.Status's real initial
	// wire value ("payment-pending", confirmed verbatim in types.Reservation's
	// doc comment: "payment-pending, active, payment-failed, retired" -- note
	// the word order, reason-then-state, which is the OPPOSITE of classic
	// Redshift's ReservedNode.State ("pending-payment", state-then-reason;
	// see TestPurchaseReservedNodeOffering_State). This backend has no
	// payment gateway or janitor to advance a reservation past this state,
	// so -- mirroring PurchaseReservedNodeOffering's own identical honest
	// limitation for classic ReservedNode, which also never leaves
	// "pending-payment" -- every reservation created here stays
	// "payment-pending" forever. Disclosed in PARITY.md.
	slReservationStatusPaymentPending = "payment-pending"

	slReservationIDHexBytes = 8 // bytes for a random reservation ID (16-char hex)
)

// serverlessReservationOfferingsCatalog is a small, honest, static catalog: one
// entry per (duration, OfferingType) combination AWS's own docs enumerate --
// 1-year and 3-year terms (aws.amazon.com/redshift/pricing/: "1-year and
// 3-year options" for serverless reservations), crossed with OfferingType's
// only two real values above. HourlyCharge/UpfrontCharge are left at 0: the
// pricing page (aws.amazon.com/redshift/pricing/, fetched 2026-09-11) states
// discounts as percentages off on-demand ("up to 45% for a 3-year term or up
// to 24% for a 1-year term"), and the linked reserved-capacity guide
// (docs.aws.amazon.com/redshift/latest/mgmt/serverless-billing-reserved.html,
// same fetch) gives a THIRD, inconsistent set of percentages elsewhere on the
// same page ("up to 20 percent with the no-upfront option, or up to 24
// percent when you pay all-upfront") and never states a $/RPU-hour or
// upfront dollar example anywhere. On-demand RPU-hour pricing itself varies
// by region, so multiplying a region's on-demand rate by one of these
// conflicting percentages would produce a specific-looking number this
// backend cannot actually verify against AWS -- exactly the invented-price
// failure mode PARITY.md's prior deferral (2026-08-13, gopherstack-v4wu)
// warned about. Left at zero and disclosed here and in PARITY.md rather than
// guessed.
//
//nolint:gochecknoglobals // static catalog, same convention as reserved_nodes.go's defaultReservedNodeOfferings
var serverlessReservationOfferingsCatalog = []*ReservationOffering{
	{
		OfferingID:   "capacity-1yr-all-upfront",
		OfferingType: slResOfferAllUpfront,
		CurrencyCode: currencyUSD,
		Duration:     durationOneYearSec,
	},
	{
		OfferingID:   "capacity-1yr-no-upfront",
		OfferingType: slResOfferNoUpfront,
		CurrencyCode: currencyUSD,
		Duration:     durationOneYearSec,
	},
	{
		OfferingID:   "capacity-3yr-all-upfront",
		OfferingType: slResOfferAllUpfront,
		CurrencyCode: currencyUSD,
		Duration:     durationThreeYearSec,
	},
	{
		OfferingID:   "capacity-3yr-no-upfront",
		OfferingType: slResOfferNoUpfront,
		CurrencyCode: currencyUSD,
		Duration:     durationThreeYearSec,
	},
}

func cloneReservationOffering(o *ReservationOffering) *ReservationOffering {
	cp := *o

	return &cp
}

// GetReservationOffering returns a single seeded catalog entry by ID.
func (b *InMemoryBackend) GetReservationOffering(offeringID string) (*ReservationOffering, error) {
	for _, o := range serverlessReservationOfferingsCatalog {
		if o.OfferingID == offeringID {
			return cloneReservationOffering(o), nil
		}
	}

	return nil, fmt.Errorf("%w: reservation offering %q not found", ErrReservationOfferingSLNotFound, offeringID)
}

// ListReservationOfferings returns the seeded catalog, paginated the same
// way every other serverless List op in this package is (see
// decodeServerlessPageToken).
func (b *InMemoryBackend) ListReservationOfferings(maxResults int, nextToken string) ([]*ReservationOffering, string) {
	list := make([]*ReservationOffering, 0, len(serverlessReservationOfferingsCatalog))
	for _, o := range serverlessReservationOfferingsCatalog {
		list = append(list, cloneReservationOffering(o))
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*ReservationOffering{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

func cloneReservation(r *Reservation) *Reservation {
	cp := *r
	if r.Offering != nil {
		cp.Offering = cloneReservationOffering(r.Offering)
	}

	return &cp
}

// CreateReservation creates a reservation against a catalog offering.
// ClientToken (idempotency-only on the real API) is accepted for wire
// compatibility but has no observable effect here, the same accept-and-
// discard convention this package already uses for other write-only fields.
func (b *InMemoryBackend) CreateReservation(capacity int32, offeringID string) (*Reservation, error) {
	if offeringID == "" {
		return nil, fmt.Errorf("%w: offeringId is required", ErrServerlessValidation)
	}

	if capacity <= 0 {
		return nil, fmt.Errorf("%w: capacity must be a positive number of RPUs", ErrServerlessValidation)
	}

	offering, err := b.GetReservationOffering(offeringID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateReservation")
	defer b.mu.Unlock()

	id := randomHex(slReservationIDHexBytes)
	start := time.Now().UTC()

	res := &Reservation{
		ReservationID:  id,
		ReservationArn: arn.Build("redshift-serverless", b.region, b.accountID, "reservation/"+id),
		Capacity:       capacity,
		Offering:       offering,
		StartDate:      start,
		EndDate:        start.Add(time.Duration(offering.Duration) * time.Second),
		Status:         slReservationStatusPaymentPending,
	}
	b.slReservations.Put(res)
	b.slReservationIdx.insert(id)

	return cloneReservation(res), nil
}

// GetReservation returns a reservation by ID.
func (b *InMemoryBackend) GetReservation(reservationID string) (*Reservation, error) {
	b.mu.RLock("GetReservation")
	defer b.mu.RUnlock()

	res, ok := b.slReservations.Get(reservationID)
	if !ok {
		return nil, fmt.Errorf("%w: reservation %q not found", ErrReservationSLNotFound, reservationID)
	}

	return cloneReservation(res), nil
}

// ListReservations returns reservations, paginated like every other
// serverless List op in this package.
func (b *InMemoryBackend) ListReservations(maxResults int, nextToken string) ([]*Reservation, string) {
	b.mu.RLock("ListReservations")
	defer b.mu.RUnlock()

	keys := b.slReservationIdx.ordered()
	list := make([]*Reservation, 0, len(keys))

	for _, id := range keys {
		res, ok := b.slReservations.Get(id)
		if !ok {
			continue
		}

		list = append(list, cloneReservation(res))
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*Reservation{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
