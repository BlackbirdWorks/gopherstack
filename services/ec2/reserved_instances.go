package ec2

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) DescribeReservedInstances(ids []string) []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	var result []*ReservedInstance

	for _, ri := range b.reservedInstances.All() {
		if len(ids) > 0 && !slices.Contains(ids, ri.ReservedInstancesID) {
			continue
		}

		cp := *ri
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesID < result[j].ReservedInstancesID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesOfferings(
	instanceType, az, productDesc, offeringClass string,
) []*ReservedInstancesOffering {
	b.mu.RLock("DescribeReservedInstancesOfferings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesOffering

	for _, o := range b.reservedInstancesOfferings.All() {
		if instanceType != "" && o.InstanceType != instanceType {
			continue
		}

		if az != "" && o.AvailabilityZone != az {
			continue
		}

		if productDesc != "" && o.ProductDescription != productDesc {
			continue
		}

		if offeringClass != "" && o.OfferingClass != offeringClass {
			continue
		}

		cp := *o
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesOfferingID < result[j].ReservedInstancesOfferingID
	})

	return result
}

func (b *InMemoryBackend) PurchaseReservedInstancesOffering(
	offeringID string,
	instanceCount int,
) (*ReservedInstance, error) {
	b.mu.Lock("PurchaseReservedInstancesOffering")
	defer b.mu.Unlock()

	offering, ok := b.reservedInstancesOfferings.Get(offeringID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReservedInstancesOfferingNotFound, offeringID)
	}

	id := "r-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	ri := &ReservedInstance{
		ReservedInstancesID: id,
		InstanceType:        offering.InstanceType,
		AvailabilityZone:    offering.AvailabilityZone,
		ProductDescription:  offering.ProductDescription,
		OfferingType:        offering.OfferingType,
		OfferingClass:       offering.OfferingClass,
		Duration:            offering.Duration,
		FixedPrice:          offering.FixedPrice,
		UsagePrice:          offering.UsagePrice,
		InstanceCount:       instanceCount,
		State:               SpotFleetStateActive,
		Start:               now,
		End:                 now.Add(time.Duration(offering.Duration) * time.Second),
	}
	b.reservedInstances.Put(ri)

	cp := *ri

	return &cp, nil
}

func (b *InMemoryBackend) CreateReservedInstancesListing(
	reservedInstancesID string,
	instanceCount int,
	schedules []PriceScheduleEntry,
) (*ReservedInstancesListing, error) {
	if len(schedules) == 0 {
		return nil, fmt.Errorf("%w: PriceSchedules is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReservedInstancesListing")
	defer b.mu.Unlock()

	// Real AWS marks the schedule for the remaining term active and the rest
	// pending; this backend has no time-elapsing term engine, so it honors
	// the documented convention that schedules are supplied longest-term
	// first and marks only the first one active.
	rendered := make([]PriceScheduleEntry, len(schedules))
	copy(rendered, schedules)
	rendered[0].Active = true

	id := "rsl-" + uuid.New().String()[:8]
	l := &ReservedInstancesListing{
		ReservedInstancesListingID: id,
		ReservedInstancesID:        reservedInstancesID,
		Status:                     SpotFleetStateActive,
		PriceSchedules:             rendered,
		InstanceCounts:             []InstanceCountEntry{{State: "available", InstanceCount: instanceCount}},
	}
	b.reservedInstancesListings.Put(l)

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) CancelReservedInstancesListing(id string) (*ReservedInstancesListing, error) {
	b.mu.Lock("CancelReservedInstancesListing")
	defer b.mu.Unlock()

	l, ok := b.reservedInstancesListings.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReservedInstancesListingNotFound, id)
	}

	l.Status = "cancelled"

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) DescribeReservedInstancesListings(ids []string) []*ReservedInstancesListing {
	b.mu.RLock("DescribeReservedInstancesListings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesListing

	for _, l := range b.reservedInstancesListings.All() {
		if len(ids) > 0 && !slices.Contains(ids, l.ReservedInstancesListingID) {
			continue
		}

		cp := *l
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesListingID < result[j].ReservedInstancesListingID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesModifications(ids []string) []*ReservedInstancesModification {
	b.mu.RLock("DescribeReservedInstancesModifications")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesModification

	for _, m := range b.reservedInstancesModifications.All() {
		if len(ids) > 0 && !slices.Contains(ids, m.ReservedInstancesModificationID) {
			continue
		}

		cp := *m
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesModificationID < result[j].ReservedInstancesModificationID
	})

	return result
}

func (b *InMemoryBackend) ModifyReservedInstances(
	ids []string,
	targets []ReservedInstancesConfigurationTarget,
) (*ReservedInstancesModification, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: ReservedInstancesIds is required", ErrInvalidParameter)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: TargetConfigurations is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyReservedInstances")
	defer b.mu.Unlock()

	for _, riID := range ids {
		if _, ok := b.reservedInstances.Get(riID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrReservedInstancesNotFound, riID)
		}
	}

	results := make([]ReservedInstancesModificationResult, 0, len(targets))
	for _, t := range targets {
		results = append(results, ReservedInstancesModificationResult{TargetConfiguration: t})
	}

	id := "rimod-" + uuid.New().String()[:8]
	m := &ReservedInstancesModification{
		ReservedInstancesModificationID: id,
		Status:                          "fulfilled",
		StatusMessage:                   "Modification fulfilled",
		ReservedInstancesIDs:            append([]string(nil), ids...),
		ModificationResults:             results,
	}
	b.reservedInstancesModifications.Put(m)

	cp := *m

	return &cp, nil
}

// reservedInstanceStateQueued is the real ReservedInstanceState enum value
// ("queued") DeleteQueuedReservedInstances requires a target to be in; see
// QueuedPurchaseDeletionResult's doc comment for why no Reserved Instance in
// this backend is ever actually in that state.
const reservedInstanceStateQueued = "queued"

// Real DeleteQueuedReservedInstancesErrorCode enum values (types.go).
const (
	deleteQueuedRIErrCodeIDInvalid = "reserved-instances-id-invalid"
	deleteQueuedRIErrCodeNotQueued = "reserved-instances-not-in-queued-state"
)

// DeleteQueuedReservedInstances reports real per-ID success/failure
// (types.SuccessfulQueuedPurchaseDeletion / types.FailedQueuedPurchaseDeletion)
// instead of silently deleting whatever Reserved Instance IDs happen to match,
// which would incorrectly let this call delete an ACTIVE (non-queued)
// reservation -- something real AWS never does through this operation.
func (b *InMemoryBackend) DeleteQueuedReservedInstances(ids []string) []QueuedPurchaseDeletionResult {
	b.mu.Lock("DeleteQueuedReservedInstances")
	defer b.mu.Unlock()

	results := make([]QueuedPurchaseDeletionResult, 0, len(ids))

	for _, id := range ids {
		ri, ok := b.reservedInstances.Get(id)

		switch {
		case !ok:
			results = append(results, QueuedPurchaseDeletionResult{
				ReservedInstancesID: id,
				Failed:              true,
				ErrorCode:           deleteQueuedRIErrCodeIDInvalid,
				ErrorMessage:        fmt.Sprintf("The reserved instance ID '%s' does not exist", id),
			})
		case ri.State != reservedInstanceStateQueued:
			results = append(results, QueuedPurchaseDeletionResult{
				ReservedInstancesID: id,
				Failed:              true,
				ErrorCode:           deleteQueuedRIErrCodeNotQueued,
				ErrorMessage:        fmt.Sprintf("The reserved instance '%s' is not in the queued state", id),
			})
		default:
			b.reservedInstances.Delete(id)
			delete(b.tags, id)
			results = append(results, QueuedPurchaseDeletionResult{ReservedInstancesID: id})
		}
	}

	return results
}

// reservedInstanceOfferingClassConvertible is types.OfferingClassTypeConvertible
// (ec2@v1.329.0 types/enums.go:9661). Only Convertible Reserved Instances can
// be exchanged (api_op_GetReservedInstancesExchangeQuote.go doc comment).
const reservedInstanceOfferingClassConvertible = "convertible"

// TargetConfigurationRequest is one TargetConfiguration.N entry of a
// GetReservedInstancesExchangeQuote/AcceptReservedInstancesExchangeQuote
// request (types.TargetConfigurationRequest, ec2@v1.329.0
// types/types.go:23860).
type TargetConfigurationRequest struct {
	OfferingID    string
	InstanceCount int
}

// ReservationValue mirrors types.ReservationValue (ec2@v1.329.0
// types/types.go:19584-19597): RemainingTotalValue is the sum of
// RemainingUpfrontValue + HourlyPrice * hours remaining.
type ReservationValue struct {
	HourlyPrice           float64
	RemainingTotalValue   float64
	RemainingUpfrontValue float64
}

// ReservedInstanceQuoteValue is one entry of a quote's
// ReservedInstanceValueSet (types.ReservedInstanceReservationValue,
// ec2@v1.329.0 types/types.go:19709).
type ReservedInstanceQuoteValue struct {
	ReservedInstancesID string
	Value               ReservationValue
}

// TargetConfigurationQuoteValue is one entry of a quote's
// TargetConfigurationValueSet (types.TargetReservationValue, ec2@v1.329.0
// types/types.go:23926).
type TargetConfigurationQuoteValue struct {
	OfferingID    string
	InstanceCount int
	Value         ReservationValue
}

// ReservedInstancesExchangeQuote is the computed result of
// GetReservedInstancesExchangeQuote (types.GetReservedInstancesExchangeQuoteOutput,
// ec2@v1.329.0 api_op_GetReservedInstancesExchangeQuote.go).
type ReservedInstancesExchangeQuote struct {
	CurrencyCode                        string
	ValidationFailureReason             string
	OutputReservedInstancesWillExpireAt time.Time
	ReservedInstanceValueSet            []ReservedInstanceQuoteValue
	TargetConfigurationValueSet         []TargetConfigurationQuoteValue
	ReservedInstanceValueRollup         ReservationValue
	TargetConfigurationValueRollup      ReservationValue
	PaymentDue                          float64
	IsValidExchange                     bool
}

// nonExchangeableReason returns a validation-failure message when ris
// contains any non-convertible Reserved Instance, or "" when every RI is
// convertible and therefore eligible for exchange.
func nonExchangeableReason(ris []*ReservedInstance) string {
	for _, ri := range ris {
		if ri.OfferingClass != reservedInstanceOfferingClassConvertible {
			return fmt.Sprintf(
				"The Reserved Instance %s has offering class %q; only Convertible Reserved Instances can be exchanged",
				ri.ReservedInstancesID, ri.OfferingClass,
			)
		}
	}

	return ""
}

// reservationValueFor computes ri's remaining value as of now: the upfront
// price is prorated by the fraction of its term still remaining, and the
// hourly usage price accrues for the remaining hours (ReservationValue's
// doc comment formula, types/types.go:19589-19591).
func reservationValueFor(ri *ReservedInstance, now time.Time) ReservationValue {
	remaining := max(ri.End.Sub(now), 0)

	var remainingFraction float64
	if ri.Duration > 0 {
		remainingFraction = min(remaining.Seconds()/float64(ri.Duration), 1)
	}

	upfront := ri.FixedPrice * remainingFraction

	return ReservationValue{
		HourlyPrice:           ri.UsagePrice,
		RemainingUpfrontValue: upfront,
		RemainingTotalValue:   upfront + ri.UsagePrice*remaining.Hours(),
	}
}

// targetReservationValueFor computes the value of count instances of a
// freshly-started target offering: the full (unprorated) upfront price plus
// usage price accrued across the offering's whole term.
func targetReservationValueFor(offering *ReservedInstancesOffering, count int) ReservationValue {
	upfront := offering.FixedPrice * float64(count)
	hourly := offering.UsagePrice * float64(count)
	hours := float64(offering.Duration) / float64(time.Hour/time.Second)

	return ReservationValue{
		HourlyPrice:           hourly,
		RemainingUpfrontValue: upfront,
		RemainingTotalValue:   upfront + hourly*hours,
	}
}

// GetReservedInstancesExchangeQuote computes a quote for exchanging
// reservedInstanceIDs (which must all be Convertible Reserved Instances) for
// the given target offerings. An unknown reservedInstanceID returns
// ErrReservedInstancesNotFound (InvalidReservedInstancesId); an unknown
// target OfferingID returns ErrReservedInstancesOfferingNotFound
// (InvalidReservedInstancesOfferingId). A non-convertible source RI is not
// an error: it yields IsValidExchange=false with ValidationFailureReason
// set, matching "If the exchange cannot be performed, the reason is
// returned in the response" (api_op_GetReservedInstancesExchangeQuote.go).
//
// TargetConfigurationRequest.InstanceCount is documented as "reserved and
// cannot be specified in a request" (types/types.go:23867-23869) -- real
// AWS derives the purchased count automatically to match the exchanged
// value. This backend has no such solver: it uses the caller-supplied count
// (defaulting to 1) to size the target value. See services/ec2/PARITY.md.
func (b *InMemoryBackend) GetReservedInstancesExchangeQuote(
	reservedInstanceIDs []string, targets []TargetConfigurationRequest,
) (*ReservedInstancesExchangeQuote, error) {
	b.mu.RLock("GetReservedInstancesExchangeQuote")
	defer b.mu.RUnlock()

	ris := make([]*ReservedInstance, 0, len(reservedInstanceIDs))

	for _, id := range reservedInstanceIDs {
		ri, ok := b.reservedInstances.Get(id)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReservedInstancesNotFound, id)
		}

		ris = append(ris, ri)
	}

	if reason := nonExchangeableReason(ris); reason != "" {
		return &ReservedInstancesExchangeQuote{ValidationFailureReason: reason}, nil
	}

	now := time.Now().UTC()
	quote := &ReservedInstancesExchangeQuote{
		IsValidExchange: true,
		// types/types.go:19729-19731: the only supported currency.
		CurrencyCode: hostReservationCurrencyUSD,
	}

	var earliestEnd time.Time

	for _, ri := range ris {
		val := reservationValueFor(ri, now)
		quote.ReservedInstanceValueSet = append(quote.ReservedInstanceValueSet, ReservedInstanceQuoteValue{
			ReservedInstancesID: ri.ReservedInstancesID,
			Value:               val,
		})
		quote.ReservedInstanceValueRollup.HourlyPrice += val.HourlyPrice
		quote.ReservedInstanceValueRollup.RemainingTotalValue += val.RemainingTotalValue
		quote.ReservedInstanceValueRollup.RemainingUpfrontValue += val.RemainingUpfrontValue

		if earliestEnd.IsZero() || ri.End.Before(earliestEnd) {
			earliestEnd = ri.End
		}
	}

	quote.OutputReservedInstancesWillExpireAt = earliestEnd

	for _, t := range targets {
		offering, ok := b.reservedInstancesOfferings.Get(t.OfferingID)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReservedInstancesOfferingNotFound, t.OfferingID)
		}

		count := t.InstanceCount
		if count == 0 {
			count = 1
		}

		val := targetReservationValueFor(offering, count)
		quote.TargetConfigurationValueSet = append(
			quote.TargetConfigurationValueSet,
			TargetConfigurationQuoteValue{OfferingID: t.OfferingID, InstanceCount: count, Value: val},
		)
		quote.TargetConfigurationValueRollup.HourlyPrice += val.HourlyPrice
		quote.TargetConfigurationValueRollup.RemainingTotalValue += val.RemainingTotalValue
		quote.TargetConfigurationValueRollup.RemainingUpfrontValue += val.RemainingUpfrontValue
	}

	quote.PaymentDue = max(
		quote.TargetConfigurationValueRollup.RemainingTotalValue-quote.ReservedInstanceValueRollup.RemainingTotalValue,
		0,
	)

	return quote, nil
}
