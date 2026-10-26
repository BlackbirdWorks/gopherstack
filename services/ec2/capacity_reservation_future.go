package ec2

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	crStateAssessing   = "assessing"
	crStateScheduled   = "scheduled"
	crStateCancelling  = "cancelling"
	crStateExpired     = "expired"
	crStateFailed      = "failed"
	crStatePending     = "pending"
	crStateUnsupported = "unsupported"

	crDeliveryIncremental = "incremental"
	crEndDateLimited      = "limited"
	crEndDateUnlimited    = "unlimited"
	crMatchTargeted       = "targeted"
	crWindDownCharges     = "commitment-wind-down"

	crFutureMinLead        = 5 * 24 * time.Hour
	crFutureMaxLead        = 120 * 24 * time.Hour
	crFutureMinVCPUs       = 32
	crMaxCommitmentSeconds = 200000000
	crMaxInstanceCount     = 1000
	crAssessmentPeriod     = time.Minute

	crDateChangeMaxPushout  = 30 * 24 * time.Hour
	crDateChangeQuoteTTL    = 24 * time.Hour
	crDateChangeQuoteMargin = time.Hour
)

// ErrCapacityReservationIncorrectState maps to the EC2 IncorrectState code.
var ErrCapacityReservationIncorrectState = errors.New("IncorrectState")

// CapacityReservationModificationQuote is a date-change quote for a
// future-dated Capacity Reservation.
type CapacityReservationModificationQuote struct {
	CreateTime              time.Time `json:"createTime"`
	ExpirationTime          time.Time `json:"expirationTime"`
	CurrentStartDate        time.Time `json:"currentStartDate"`
	OriginalStartDate       time.Time `json:"originalStartDate"`
	NewStartDate            time.Time `json:"newStartDate"`
	NewCommitmentEndDate    time.Time `json:"newCommitmentEndDate"`
	QuoteID                 string    `json:"quoteId,omitempty"`
	CapacityReservationID   string    `json:"capacityReservationId,omitempty"`
	CurrentReservationState string    `json:"currentReservationState,omitempty"`
	QuoteState              string    `json:"quoteState,omitempty"`
	NewCommitmentDuration   int64     `json:"newCommitmentDuration,omitempty"`
	CurrentInstanceCount    int32     `json:"currentInstanceCount,omitempty"`
	Used                    bool      `json:"used,omitempty"`
}

// FutureDatedCapacityReservationSpec carries the future-dated inputs of
// CreateCapacityReservation.
type FutureDatedCapacityReservationSpec struct {
	StartDate          time.Time
	EndDate            time.Time
	Tenancy            string
	DeliveryPreference string
	InstanceType       string
	EndDateType        string
	InstanceMatch      string
	InstancePlatform   string
	AvailabilityZone   string
	CommitmentDuration int64
	InstanceCount      int
	HasCommitment      bool
	Outpost            bool
	PlacementGroup     bool
}

// ModifyCapacityReservationParams carries the optional inputs of
// ModifyCapacityReservation.
type ModifyCapacityReservationParams struct {
	InstanceCount         *int
	StartDate             time.Time
	EndDate               time.Time
	EndDateType           string
	InstanceMatchCriteria string
	QuoteID               string
	AcceptTerms           bool
	EndDateSet            bool
}

// CapacityReservationAdjustment is the ModifyCapacityReservation result.
type CapacityReservationAdjustment struct {
	StartDate          time.Time
	CommitmentEndDate  time.Time
	EndDate            time.Time
	AdjustmentStatus   string
	EndDateType        string
	CommitmentDuration int64
}

func (cr *CapacityReservation) futureDated() bool { return !cr.StartDate.IsZero() }

func (cr *CapacityReservation) commitmentEnd() time.Time {
	if cr.CommitmentDuration == 0 || cr.StartDate.IsZero() {
		return time.Time{}
	}

	return cr.StartDate.Add(time.Duration(cr.CommitmentDuration) * time.Second)
}

// advanceCapacityReservation moves cr through the time-driven part of its
// state machine: assessing -> scheduled -> active -> expired, and
// cancelling -> cancelled.
func advanceCapacityReservation(cr *CapacityReservation, now time.Time) {
	if cr.State == crStateAssessing && !now.Before(cr.AssessmentEnd) {
		cr.State = crStateScheduled
	}

	if cr.State == crStateScheduled && !now.Before(cr.StartDate) {
		cr.State = stateActive
		cr.AvailableInstanceCount = cr.TotalInstanceCount
	}

	if cr.State == stateActive && cr.EndDateType == crEndDateLimited && !now.Before(cr.EndDate) {
		cr.State = crStateExpired
		cr.AvailableInstanceCount = 0
	}

	if cr.State == crStateCancelling && !now.Before(cr.CancellingUntil) {
		cr.State = stateCancelled
	}
}

func effectiveCapacityReservationState(cr *CapacityReservation, now time.Time) string {
	cp := *cr
	advanceCapacityReservation(&cp, now)

	return cp.State
}

func (b *InMemoryBackend) advanceCapacityReservationsLocked() {
	now := b.now().UTC()
	for _, cr := range b.capacityReservations.All() {
		advanceCapacityReservation(cr, now)
	}
}

func crInstanceFamilyAllowed(instanceType string) bool {
	family, _, _ := strings.Cut(instanceType, ".")
	idx := strings.IndexAny(family, "0123456789")

	if idx < 0 {
		return false
	}

	switch family[:idx] {
	case "c", "m", "r", "i", "t", "g":
		return true
	}

	return false
}

func validateFutureDated(spec *FutureDatedCapacityReservationSpec, now time.Time) error {
	if spec.Outpost || spec.PlacementGroup {
		return fmt.Errorf(
			"%w: OutpostArn and PlacementGroupArn are not supported for future-dated Capacity Reservations",
			ErrInvalidParameterCombination,
		)
	}

	if !spec.HasCommitment || spec.CommitmentDuration < 1 || spec.CommitmentDuration > crMaxCommitmentSeconds {
		return fmt.Errorf("%w: CommitmentDuration (1-%d seconds) is required for future-dated Capacity Reservations",
			ErrInvalidParameter, crMaxCommitmentSeconds)
	}

	if spec.DeliveryPreference != crDeliveryIncremental {
		return fmt.Errorf("%w: DeliveryPreference must be %s for future-dated Capacity Reservations (got %q)",
			ErrInvalidParameter, crDeliveryIncremental, spec.DeliveryPreference)
	}

	if spec.InstanceMatch != crMatchTargeted {
		return fmt.Errorf("%w: InstanceMatchCriteria must be %s for future-dated Capacity Reservations",
			ErrInvalidParameter, crMatchTargeted)
	}

	if spec.StartDate.Before(now.Add(crFutureMinLead)) || spec.StartDate.After(now.Add(crFutureMaxLead)) {
		return fmt.Errorf("%w: StartDate must be between 5 and 120 days in the future", ErrInvalidParameter)
	}

	if err := validateFutureDatedCapacity(spec); err != nil {
		return err
	}

	commitEnd := spec.StartDate.Add(time.Duration(spec.CommitmentDuration) * time.Second)
	if spec.EndDateType == crEndDateLimited && spec.EndDate.Before(commitEnd) {
		return fmt.Errorf("%w: EndDate can't be within the commitment duration", ErrInvalidParameter)
	}

	return nil
}

func validateFutureDatedCapacity(spec *FutureDatedCapacityReservationSpec) error {
	if !crInstanceFamilyAllowed(spec.InstanceType) {
		return fmt.Errorf(
			"%w: future-dated Capacity Reservations support the C, M, R, I, T, and G instance families only",
			ErrInvalidParameter,
		)
	}

	catalog, ok := instanceTypeCatalog[spec.InstanceType]
	if !ok || catalog.vcpus == 0 {
		return fmt.Errorf("%w: unknown instance type %q", ErrInvalidParameter, spec.InstanceType)
	}

	if int64(catalog.vcpus)*int64(spec.InstanceCount) < crFutureMinVCPUs {
		return fmt.Errorf("%w: future-dated Capacity Reservations require at least %d vCPUs",
			ErrInvalidParameter, crFutureMinVCPUs)
	}

	return nil
}

func validateEndDate(endDateType string, endDate time.Time) error {
	switch endDateType {
	case "", crEndDateUnlimited:
		if !endDate.IsZero() {
			return fmt.Errorf(
				"%w: EndDate must be omitted when EndDateType is unlimited",
				ErrInvalidParameterCombination,
			)
		}
	case crEndDateLimited:
		if endDate.IsZero() {
			return fmt.Errorf("%w: EndDate is required when EndDateType is limited", ErrInvalidParameterCombination)
		}
	default:
		return fmt.Errorf("%w: invalid EndDateType %q", ErrInvalidParameter, endDateType)
	}

	return nil
}

// CreateFutureDatedCapacityReservation creates a Capacity Reservation, future-dated
// when spec.StartDate is set, otherwise active immediately.
func (b *InMemoryBackend) CreateFutureDatedCapacityReservation(
	spec *FutureDatedCapacityReservationSpec,
	tags map[string]string,
) (*CapacityReservation, error) {
	if spec.InstanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	if spec.InstanceCount < 1 || spec.InstanceCount > crMaxInstanceCount {
		return nil, fmt.Errorf("%w: InstanceCount must be between 1 and %d", ErrInvalidParameter, crMaxInstanceCount)
	}

	if err := validateEndDate(spec.EndDateType, spec.EndDate); err != nil {
		return nil, err
	}

	if spec.StartDate.IsZero() && (spec.HasCommitment || spec.DeliveryPreference != "") {
		return nil, fmt.Errorf(
			"%w: CommitmentDuration and DeliveryPreference apply to future-dated Capacity Reservations only",
			ErrInvalidParameterCombination)
	}

	b.mu.Lock("CreateFutureDatedCapacityReservation")
	defer b.mu.Unlock()

	now := b.now().UTC()
	b.advanceCapacityReservationsLocked()

	cr := &CapacityReservation{
		CapacityReservationID:  awsid.New("cr-"),
		InstanceType:           spec.InstanceType,
		AvailabilityZone:       spec.AvailabilityZone,
		InstanceMatchCriteria:  spec.InstanceMatch,
		Tenancy:                spec.Tenancy,
		InstancePlatform:       spec.InstancePlatform,
		TotalInstanceCount:     spec.InstanceCount,
		AvailableInstanceCount: spec.InstanceCount,
		State:                  stateActive,
		CreateTime:             now,
		OwnedBy:                b.AccountID,
		EndDateType:            spec.EndDateType,
		EndDate:                spec.EndDate.UTC(),
	}

	if !spec.StartDate.IsZero() {
		if err := validateFutureDated(spec, now); err != nil {
			return nil, err
		}

		cr.State = crStateAssessing
		cr.AvailableInstanceCount = 0
		cr.StartDate = spec.StartDate.UTC()
		cr.OriginalStartDate = cr.StartDate
		cr.AssessmentEnd = now.Add(crAssessmentPeriod)
		cr.CommitmentDuration = spec.CommitmentDuration
		cr.DeliveryPreference = spec.DeliveryPreference
	}

	b.capacityReservations.Put(cr)
	b.setTagsLocked(cr.CapacityReservationID, tags)

	cp := *cr

	return &cp, nil
}

// CancelCapacityReservationWithQuote cancels a reservation, enforcing the
// commitment-duration quote requirement.
func (b *InMemoryBackend) CancelCapacityReservationWithQuote(id, quoteID, charges string) error {
	if id == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	b.advanceCapacityReservationsLocked()

	cr, ok := b.capacityReservations.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, id)
	}

	now := b.now().UTC()

	switch {
	case cr.State == crStateScheduled || (cr.State == stateActive && now.Before(cr.commitmentEnd())):
		if err := b.checkCancellationQuoteLocked(cr, quoteID, charges, now); err != nil {
			return err
		}

		cr.State = crStateCancelling
		cr.AvailableInstanceCount = 0
		cr.CancellingUntil = cr.commitmentEnd()

		return nil
	case cr.State == crStateCancelling,
		cr.State == crStateExpired,
		cr.State == crStateFailed,
		cr.State == crStateUnsupported:
		return fmt.Errorf("%w: Capacity Reservation %s is %s", ErrCapacityReservationIncorrectState, id, cr.State)
	}

	if cr.Interruptible && cr.State != stateCancelled {
		b.releaseInterruptibleToSourceLocked(cr)
	}

	cr.State = stateCancelled

	return nil
}

func (b *InMemoryBackend) checkCancellationQuoteLocked(
	cr *CapacityReservation, quoteID, charges string, now time.Time,
) error {
	if quoteID == "" || charges != crWindDownCharges {
		return fmt.Errorf(
			"%w: cancelling in the %s state during the commitment requires QuoteId and ApplyCancellationCharges=%s",
			ErrInvalidParameter, cr.State, crWindDownCharges)
	}

	q, ok := b.capacityReservationCancellationQuotes.Get(quoteID)
	if !ok || q.CapacityReservationID != cr.CapacityReservationID {
		return fmt.Errorf("%w: cancellation quote %s not found for %s",
			ErrInvalidParameter, quoteID, cr.CapacityReservationID)
	}

	if !now.Before(q.ExpirationTime) {
		return fmt.Errorf("%w: cancellation quote %s is %s", ErrInvalidParameter, quoteID,
			capacityReservationCancellationQuoteStateExpired)
	}

	return nil
}

// CreateCapacityReservationDateChangeQuote quotes a start-date push-out.
func (b *InMemoryBackend) CreateCapacityReservationDateChangeQuote(
	id string, newStart time.Time, tags map[string]string,
) (*CapacityReservationModificationQuote, error) {
	if id == "" || newStart.IsZero() {
		return nil, fmt.Errorf("%w: CapacityReservationId and NewStartDate are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityReservationDateChangeQuote")
	defer b.mu.Unlock()

	b.advanceCapacityReservationsLocked()

	cr, ok := b.capacityReservations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, id)
	}

	if !cr.futureDated() || cr.State != crStateScheduled {
		return nil, fmt.Errorf(
			"%w: date change quotes require an undelivered future-dated Capacity Reservation in the scheduled state (is %s)",
			ErrCapacityReservationIncorrectState,
			cr.State,
		)
	}

	newStart = newStart.UTC()
	if !newStart.After(cr.StartDate) {
		return nil, fmt.Errorf("%w: NewStartDate must be later than the current start date", ErrInvalidParameter)
	}

	if newStart.After(cr.OriginalStartDate.Add(crDateChangeMaxPushout)) {
		return nil, fmt.Errorf("%w: cumulative pushout is limited to 30 days from the original start date",
			ErrInvalidParameter)
	}

	now := b.now().UTC()

	expiry := now.Add(crDateChangeQuoteTTL)
	if latest := cr.StartDate.Add(-crDateChangeQuoteMargin); latest.Before(expiry) {
		expiry = latest
	}

	if !expiry.After(now) {
		return nil, fmt.Errorf("%w: the start date is too close to request a date change", ErrInvalidParameter)
	}

	q := &CapacityReservationModificationQuote{
		QuoteID:                 awsid.New("crmq-"),
		CapacityReservationID:   id,
		QuoteState:              capacityReservationCancellationQuoteStateActive,
		CreateTime:              now,
		ExpirationTime:          expiry,
		CurrentInstanceCount:    toInt32Clamped(cr.TotalInstanceCount),
		CurrentReservationState: cr.State,
		CurrentStartDate:        cr.StartDate,
		OriginalStartDate:       cr.OriginalStartDate,
		NewStartDate:            newStart,
		NewCommitmentDuration:   cr.CommitmentDuration,
		NewCommitmentEndDate:    newStart.Add(time.Duration(cr.CommitmentDuration) * time.Second),
	}
	b.capacityReservationModificationQuotes.Put(q)
	b.setTagsLocked(q.QuoteID, tags)

	cp := *q

	return &cp, nil
}

// DescribeCapacityReservationDateChangeQuotes lists quotes, deriving expiry at read time.
func (b *InMemoryBackend) DescribeCapacityReservationDateChangeQuotes(
	ids []string,
) []*CapacityReservationModificationQuote {
	b.mu.RLock("DescribeCapacityReservationDateChangeQuotes")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)
	now := b.now().UTC()

	var out []*CapacityReservationModificationQuote

	for _, q := range b.capacityReservationModificationQuotes.All() {
		if len(idSet) > 0 && !idSet[q.QuoteID] {
			continue
		}

		cp := *q
		if !now.Before(cp.ExpirationTime) {
			cp.QuoteState = capacityReservationCancellationQuoteStateExpired
		}

		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].QuoteID < out[j].QuoteID })

	return out
}

// ApplyCapacityReservationModification applies ModifyCapacityReservation.
func (b *InMemoryBackend) ApplyCapacityReservationModification(
	id string, p *ModifyCapacityReservationParams,
) (*CapacityReservationAdjustment, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCapacityReservation")
	defer b.mu.Unlock()

	b.advanceCapacityReservationsLocked()

	cr, ok := b.capacityReservations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, id)
	}

	if err := b.checkModifiableLocked(cr, p); err != nil {
		return nil, err
	}

	adj := &CapacityReservationAdjustment{}

	if !p.StartDate.IsZero() {
		if err := b.applyDateChangeLocked(cr, p, adj); err != nil {
			return nil, err
		}
	}

	if p.InstanceCount != nil {
		cr.TotalInstanceCount = *p.InstanceCount
		cr.AvailableInstanceCount = *p.InstanceCount
	}

	if p.EndDateSet {
		cr.EndDateType = p.EndDateType
		cr.EndDate = p.EndDate.UTC()
	}

	if p.InstanceMatchCriteria != "" {
		cr.InstanceMatchCriteria = p.InstanceMatchCriteria
	}

	return adj, nil
}

func (b *InMemoryBackend) checkModifiableLocked(cr *CapacityReservation, p *ModifyCapacityReservationParams) error {
	isDateChange := !p.StartDate.IsZero()
	if p.QuoteID != "" && !isDateChange {
		return fmt.Errorf("%w: QuoteId requires StartDate", ErrInvalidParameterCombination)
	}

	if p.EndDateSet {
		if err := validateEndDate(p.EndDateType, p.EndDate); err != nil {
			return err
		}
	}

	switch cr.State {
	case crStateAssessing, crStatePending, crStateExpired, stateCancelled, crStateCancelling,
		crStateFailed, crStateUnsupported:
		return fmt.Errorf("%w: Capacity Reservation %s in state %s can't be modified",
			ErrCapacityReservationIncorrectState, cr.CapacityReservationID, cr.State)
	case crStateScheduled:
		if p.InstanceCount != nil || p.EndDateSet || p.InstanceMatchCriteria != "" {
			return fmt.Errorf("%w: only tags and the start date (with a quote) can change while scheduled",
				ErrCapacityReservationIncorrectState)
		}
	case stateActive:
		if isDateChange {
			return fmt.Errorf("%w: the start date of a delivered Capacity Reservation can't change",
				ErrCapacityReservationIncorrectState)
		}

		return checkCommitmentModification(cr, p, b.now())
	}

	return nil
}

func checkCommitmentModification(cr *CapacityReservation, p *ModifyCapacityReservationParams, now time.Time) error {
	end := cr.commitmentEnd()
	if end.IsZero() || !now.Before(end) {
		return nil
	}

	if p.InstanceCount != nil && *p.InstanceCount < cr.TotalInstanceCount {
		return fmt.Errorf("%w: can't decrease the instance count within the commitment duration", ErrInvalidParameter)
	}

	if p.EndDateSet && p.EndDateType == crEndDateLimited && p.EndDate.Before(end) {
		return fmt.Errorf("%w: EndDate can't be within the commitment duration", ErrInvalidParameter)
	}

	return nil
}

func (b *InMemoryBackend) applyDateChangeLocked(
	cr *CapacityReservation, p *ModifyCapacityReservationParams, adj *CapacityReservationAdjustment,
) error {
	if p.QuoteID == "" || !p.AcceptTerms {
		return fmt.Errorf("%w: changing StartDate requires QuoteId and AcceptModificationTerms=true",
			ErrInvalidParameter)
	}

	q, ok := b.capacityReservationModificationQuotes.Get(p.QuoteID)
	if !ok || q.CapacityReservationID != cr.CapacityReservationID {
		return fmt.Errorf("%w: quote %s not found for %s", ErrInvalidParameter, p.QuoteID, cr.CapacityReservationID)
	}

	if q.Used || !b.now().Before(q.ExpirationTime) {
		return fmt.Errorf("%w: quote %s is no longer active", ErrInvalidParameter, p.QuoteID)
	}

	if !p.StartDate.UTC().Equal(q.NewStartDate) {
		return fmt.Errorf("%w: StartDate doesn't match the quoted start date", ErrInvalidParameter)
	}

	q.Used = true
	cr.StartDate = q.NewStartDate
	cr.AdjustmentStatus = "applied"

	adj.AdjustmentStatus = cr.AdjustmentStatus
	adj.StartDate = cr.StartDate
	adj.CommitmentEndDate = q.NewCommitmentEndDate
	adj.CommitmentDuration = q.NewCommitmentDuration
	adj.EndDate = cr.EndDate
	adj.EndDateType = cr.EndDateType

	return nil
}
