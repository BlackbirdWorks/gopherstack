package ec2

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- errors ----

var (
	// ErrCapacityReservationFleetNotFound is returned when a Capacity Reservation
	// Fleet is not found.
	ErrCapacityReservationFleetNotFound = errors.New("InvalidCapacityReservationFleetId.NotFound")
	// ErrCapacityBlockOfferingNotFound is returned when a Capacity Block offering
	// is not found (e.g. it expired or was never issued by DescribeCapacityBlockOfferings).
	ErrCapacityBlockOfferingNotFound = errors.New("InvalidCapacityBlockOfferingId.NotFound")
	// ErrCapacityBlockExtensionOfferingNotFound is returned when a Capacity Block
	// extension offering is not found.
	ErrCapacityBlockExtensionOfferingNotFound = errors.New(
		"InvalidCapacityBlockExtensionOfferingId.NotFound",
	)
	// ErrCapacityManagerDataExportNotFound is returned when a Capacity Manager
	// data export configuration is not found.
	ErrCapacityManagerDataExportNotFound = errors.New("InvalidCapacityManagerDataExportId.NotFound")
	// ErrCapacityReservationBillingRequestNotFound is returned when there is no
	// billing ownership request for a given Capacity Reservation.
	ErrCapacityReservationBillingRequestNotFound = errors.New(
		"InvalidCapacityReservationBillingRequestId.NotFound",
	)
)

// ---- constants ----

const (
	capacityManagerStatusEnabled  = "enabled"
	capacityManagerStatusDisabled = "disabled"

	crFleetAllocDefault   = "prioritized"
	crFleetMatchOpen      = "open"
	crFleetTenancyDefault = "default"

	billingRequestPending  = "pending"
	billingRequestRejected = "rejected"
	billingRequestRevoked  = "revoked"

	// capacityBlockOfferingLeadTime is how far in the future a newly generated
	// Capacity Block offering's start date is set, mirroring AWS's practice of
	// only offering capacity starting some time after the request.
	capacityBlockOfferingLeadTime = 24 * time.Hour
)

// ---- models ----

// CapacityReservationFleetInstanceSpec describes one instance type entry within
// a Capacity Reservation Fleet, both as request input and as the resolved,
// stored specification (once TotalInstanceCount/CapacityReservationID are set).
type CapacityReservationFleetInstanceSpec struct {
	AvailabilityZone      string  `json:"availabilityZone,omitempty"`
	InstanceType          string  `json:"instanceType,omitempty"`
	InstancePlatform      string  `json:"instancePlatform,omitempty"`
	CapacityReservationID string  `json:"capacityReservationId,omitempty"`
	Priority              int32   `json:"priority,omitempty"`
	Weight                float64 `json:"weight,omitempty"`
	TotalInstanceCount    int32   `json:"totalInstanceCount,omitempty"`
	EbsOptimized          bool    `json:"ebsOptimized,omitempty"`
}

// CapacityReservationFleet represents an EC2 Capacity Reservation Fleet (crf-*).
type CapacityReservationFleet struct {
	CreateTime                 time.Time                              `json:"createTime"`
	EndDate                    *time.Time                             `json:"endDate,omitempty"`
	CapacityReservationFleetID string                                 `json:"capacityReservationFleetId,omitempty"`
	AllocationStrategy         string                                 `json:"allocationStrategy,omitempty"`
	State                      string                                 `json:"state,omitempty"`
	InstanceMatchCriteria      string                                 `json:"instanceMatchCriteria,omitempty"`
	Tenancy                    string                                 `json:"tenancy,omitempty"`
	Tags                       map[string]string                      `json:"tags,omitempty"`
	InstanceTypeSpecifications []CapacityReservationFleetInstanceSpec `json:"instanceTypeSpecifications,omitempty"`
	TotalTargetCapacity        int32                                  `json:"totalTargetCapacity,omitempty"`
	TotalFulfilledCapacity     float64                                `json:"totalFulfilledCapacity,omitempty"`
}

// CapacityBlockOffering represents a purchasable Capacity Block offering. Offerings
// are generated on demand by DescribeCapacityBlockOfferings from a static, realistic
// catalog and cached so a subsequent PurchaseCapacityBlock call can reference them.
type CapacityBlockOffering struct {
	StartDate                  time.Time `json:"startDate"`
	EndDate                    time.Time `json:"endDate"`
	CapacityBlockOfferingID    string    `json:"capacityBlockOfferingId,omitempty"`
	InstanceType               string    `json:"instanceType,omitempty"`
	AvailabilityZone           string    `json:"availabilityZone,omitempty"`
	Tenancy                    string    `json:"tenancy,omitempty"`
	CurrencyCode               string    `json:"currencyCode,omitempty"`
	UpfrontPrice               string    `json:"upfrontPrice,omitempty"`
	InstanceCount              int32     `json:"instanceCount,omitempty"`
	CapacityBlockDurationHours int32     `json:"capacityBlockDurationHours,omitempty"`
}

// CapacityBlockExtensionOffering represents a purchasable extension to an
// existing Capacity Block reservation. Like CapacityBlockOffering, generated on
// demand and cached for a subsequent PurchaseCapacityBlockExtension call.
type CapacityBlockExtensionOffering struct {
	CapacityBlockExtensionStartDate     time.Time `json:"capacityBlockExtensionStartDate"`
	CapacityBlockExtensionEndDate       time.Time `json:"capacityBlockExtensionEndDate"`
	CapacityBlockExtensionOfferingID    string    `json:"capacityBlockExtensionOfferingId,omitempty"`
	CapacityReservationID               string    `json:"capacityReservationId,omitempty"`
	InstanceType                        string    `json:"instanceType,omitempty"`
	AvailabilityZone                    string    `json:"availabilityZone,omitempty"`
	CurrencyCode                        string    `json:"currencyCode,omitempty"`
	UpfrontPrice                        string    `json:"upfrontPrice,omitempty"`
	InstanceCount                       int32     `json:"instanceCount,omitempty"`
	CapacityBlockExtensionDurationHours int32     `json:"capacityBlockExtensionDurationHours,omitempty"`
}

// CapacityBlockExtension represents a purchased Capacity Block extension.
type CapacityBlockExtension struct {
	CapacityBlockExtensionStartDate     time.Time `json:"capacityBlockExtensionStartDate"`
	CapacityBlockExtensionEndDate       time.Time `json:"capacityBlockExtensionEndDate"`
	CapacityBlockExtensionPurchaseDate  time.Time `json:"capacityBlockExtensionPurchaseDate"`
	CapacityBlockExtensionOfferingID    string    `json:"capacityBlockExtensionOfferingId,omitempty"`
	CapacityReservationID               string    `json:"capacityReservationId,omitempty"`
	AvailabilityZone                    string    `json:"availabilityZone,omitempty"`
	CapacityBlockExtensionStatus        string    `json:"capacityBlockExtensionStatus,omitempty"`
	CapacityBlockExtensionDurationHours int32     `json:"capacityBlockExtensionDurationHours,omitempty"`
}

// CapacityBlock represents a purchased Capacity Block (cb-*), grouping the one
// or more underlying Capacity Reservations that back it.
type CapacityBlock struct {
	CreateDate             time.Time         `json:"createDate"`
	StartDate              time.Time         `json:"startDate"`
	EndDate                time.Time         `json:"endDate"`
	CapacityBlockID        string            `json:"capacityBlockId,omitempty"`
	AvailabilityZone       string            `json:"availabilityZone,omitempty"`
	State                  string            `json:"state,omitempty"`
	Tags                   map[string]string `json:"tags,omitempty"`
	CapacityReservationIDs []string          `json:"capacityReservationIds,omitempty"`
}

// CapacityReservationBillingRequest tracks a pending/resolved billing-owner
// transfer request for a shared Capacity Reservation.
type CapacityReservationBillingRequest struct {
	LastUpdateTime                  time.Time `json:"lastUpdateTime"`
	CapacityReservationID           string    `json:"capacityReservationId,omitempty"`
	RequestedBy                     string    `json:"requestedBy,omitempty"`
	UnusedReservationBillingOwnerID string    `json:"unusedReservationBillingOwnerId,omitempty"`
	Status                          string    `json:"status,omitempty"`
	StatusMessage                   string    `json:"statusMessage,omitempty"`
}

// CapacityManagerDataExport represents a configured Capacity Manager data
// export delivering periodic usage data to an S3 bucket.
type CapacityManagerDataExport struct {
	CreateTime                  time.Time         `json:"createTime"`
	Tags                        map[string]string `json:"tags,omitempty"`
	CapacityManagerDataExportID string            `json:"capacityManagerDataExportId,omitempty"`
	OutputFormat                string            `json:"outputFormat,omitempty"`
	S3BucketName                string            `json:"s3BucketName,omitempty"`
	S3BucketPrefix              string            `json:"s3BucketPrefix,omitempty"`
	Schedule                    string            `json:"schedule,omitempty"`
	LatestDeliveryStatus        string            `json:"latestDeliveryStatus,omitempty"`
}

// CapacityManagerState tracks the account-level enable/disable state of
// Capacity Manager.
type CapacityManagerState struct {
	Status              string `json:"status,omitempty"`
	OrganizationsAccess bool   `json:"organizationsAccess,omitempty"`
}

// CapacityReservationFleetCancellation reports the state transition of a
// successfully cancelled Capacity Reservation Fleet.
type CapacityReservationFleetCancellation struct {
	CapacityReservationFleetID string `json:"capacityReservationFleetId,omitempty"`
	PreviousState              string `json:"previousState,omitempty"`
	CurrentState               string `json:"currentState,omitempty"`
}

// FailedCapacityReservationFleetCancellation reports a Capacity Reservation
// Fleet that could not be cancelled.
type FailedCapacityReservationFleetCancellation struct {
	CapacityReservationFleetID string `json:"capacityReservationFleetId,omitempty"`
	ErrorCode                  string `json:"errorCode,omitempty"`
	ErrorMessage               string `json:"errorMessage,omitempty"`
}

// ---- helpers ----

// matchesCapacityFilter reports whether value satisfies the given filter set for key.
// If key is absent from filters, the filter does not restrict the match (returns true).
func matchesCapacityFilter(filters map[string][]string, key, value string) bool {
	values, ok := filters[key]
	if !ok {
		return true
	}

	return slices.Contains(values, value)
}

// toIDSet builds a set of the given IDs for O(1) membership checks, used by
// Describe* methods to filter results down to explicitly requested IDs.
func toIDSet(ids []string) map[string]bool {
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	return idSet
}

// idAndFiltersMatch reports whether id is present in idSet (an empty idSet
// matches everything) and every check in checks passes. Used by Describe*
// methods to combine an ID allowlist with EC2 Filter.N conditions.
func idAndFiltersMatch(idSet map[string]bool, id string, checks ...bool) bool {
	if len(idSet) > 0 && !idSet[id] {
		return false
	}

	return !slices.Contains(checks, false)
}

// toInt32Clamped converts n to int32, clamping to the int32 range. EC2
// instance/export counts tracked by this backend are always small in
// practice, but the explicit bounds check keeps the conversion safe
// regardless.
func toInt32Clamped(n int) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n)
	}
}

// capacityBlockHourlyRate returns a static, realistic per-instance hourly rate
// (USD) for the given instance type, falling back to a generic GPU-class rate
// for unrecognized types. Capacity Blocks are typically purchased for
// accelerator-heavy instance types.
const (
	rateP548xlarge           = 98.32
	rateP4d24xlarge          = 32.77
	rateP4de24xlarge         = 40.96
	rateTrn132xlarge         = 21.50
	rateTrn1n32xlarge        = 24.78
	rateDefaultCapacityBlock = 16.50
)

func capacityBlockHourlyRate(instanceType string) float64 {
	rates := map[string]float64{
		"p5.48xlarge":    rateP548xlarge,
		"p4d.24xlarge":   rateP4d24xlarge,
		"p4de.24xlarge":  rateP4de24xlarge,
		"trn1.32xlarge":  rateTrn132xlarge,
		"trn1n.32xlarge": rateTrn1n32xlarge,
	}
	if rate, ok := rates[instanceType]; ok {
		return rate
	}

	return rateDefaultCapacityBlock
}

// ---- Capacity Reservation Fleet ----

// CreateCapacityReservationFleet creates a Capacity Reservation Fleet, allocating
// the total target capacity across the given instance type specifications using
// the "prioritized" allocation strategy: the lowest-Priority spec is fulfilled
// first (up to the full target capacity), remaining specs are fulfilled with any
// leftover capacity. A backing CapacityReservation is created for each spec that
// receives a non-zero instance count.
func (b *InMemoryBackend) CreateCapacityReservationFleet(
	specs []CapacityReservationFleetInstanceSpec,
	totalTargetCapacity int32,
	allocationStrategy, instanceMatchCriteria, tenancy string,
	endDate *time.Time,
	tags map[string]string,
) (*CapacityReservationFleet, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: InstanceTypeSpecifications is required", ErrInvalidParameter)
	}

	if totalTargetCapacity <= 0 {
		return nil, fmt.Errorf("%w: TotalTargetCapacity must be a positive integer", ErrInvalidParameter)
	}

	if allocationStrategy == "" {
		allocationStrategy = crFleetAllocDefault
	}

	if instanceMatchCriteria == "" {
		instanceMatchCriteria = crFleetMatchOpen
	}

	if tenancy == "" {
		tenancy = crFleetTenancyDefault
	}

	b.mu.Lock("CreateCapacityReservationFleet")
	defer b.mu.Unlock()

	resolved := make([]CapacityReservationFleetInstanceSpec, len(specs))
	copy(resolved, specs)
	sort.SliceStable(resolved, func(i, j int) bool { return resolved[i].Priority < resolved[j].Priority })

	now := time.Now().UTC()

	var fulfilled float64

	remaining := float64(totalTargetCapacity)
	for i := range resolved {
		if resolved[i].AvailabilityZone == "" {
			resolved[i].AvailabilityZone = b.Region + "a"
		}

		if remaining <= 0 {
			continue
		}

		weight := resolved[i].Weight
		if weight <= 0 {
			weight = 1
		}

		instanceCount := int32(math.Ceil(remaining / weight))
		crID := "cr-" + uuid.New().String()[:8]

		b.capacityReservations[crID] = &CapacityReservation{
			CapacityReservationID:  crID,
			InstanceType:           resolved[i].InstanceType,
			AvailabilityZone:       resolved[i].AvailabilityZone,
			TotalInstanceCount:     int(instanceCount),
			AvailableInstanceCount: int(instanceCount),
			State:                  stateActive,
			CreateTime:             now,
			OwnedBy:                b.AccountID,
		}

		resolved[i].CapacityReservationID = crID
		resolved[i].TotalInstanceCount = instanceCount

		provided := weight * float64(instanceCount)
		if provided > remaining {
			fulfilled += remaining
			remaining = 0
		} else {
			fulfilled += provided
			remaining -= provided
		}
	}

	fleet := &CapacityReservationFleet{
		CapacityReservationFleetID: "crf-" + uuid.New().String()[:8],
		AllocationStrategy:         allocationStrategy,
		State:                      stateActive,
		InstanceMatchCriteria:      instanceMatchCriteria,
		Tenancy:                    tenancy,
		TotalTargetCapacity:        totalTargetCapacity,
		TotalFulfilledCapacity:     fulfilled,
		CreateTime:                 now,
		EndDate:                    endDate,
		InstanceTypeSpecifications: resolved,
		Tags:                       tags,
	}
	b.capacityReservationFleets[fleet.CapacityReservationFleetID] = fleet

	cp := *fleet

	return &cp, nil
}

// DescribeCapacityReservationFleets returns Capacity Reservation Fleets matching
// the given IDs (all, if empty) and filters (state, tenancy, allocation-strategy,
// instance-match-criteria), sorted by ID.
func (b *InMemoryBackend) DescribeCapacityReservationFleets(
	ids []string,
	filters map[string][]string,
) []*CapacityReservationFleet {
	b.mu.RLock("DescribeCapacityReservationFleets")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityReservationFleet

	for _, fleet := range b.capacityReservationFleets {
		matched := idAndFiltersMatch(idSet, fleet.CapacityReservationFleetID,
			matchesCapacityFilter(filters, "state", fleet.State),
			matchesCapacityFilter(filters, "tenancy", fleet.Tenancy),
			matchesCapacityFilter(filters, "allocation-strategy", fleet.AllocationStrategy),
			matchesCapacityFilter(filters, "instance-match-criteria", fleet.InstanceMatchCriteria),
		)
		if !matched {
			continue
		}

		cp := *fleet
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityReservationFleetID < result[j].CapacityReservationFleetID
	})

	return result
}

// ModifyCapacityReservationFleet updates the total target capacity and/or end
// date of a Capacity Reservation Fleet. When totalTargetCapacity is non-nil, the
// fleet's highest-priority instance type specification (and its backing
// CapacityReservation) is resized to match the new target.
func (b *InMemoryBackend) ModifyCapacityReservationFleet(
	fleetID string,
	totalTargetCapacity *int32,
	endDate *time.Time,
	removeEndDate bool,
) error {
	if fleetID == "" {
		return fmt.Errorf("%w: CapacityReservationFleetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCapacityReservationFleet")
	defer b.mu.Unlock()

	fleet, ok := b.capacityReservationFleets[fleetID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationFleetNotFound, fleetID)
	}

	if removeEndDate {
		fleet.EndDate = nil
	} else if endDate != nil {
		fleet.EndDate = endDate
	}

	if totalTargetCapacity != nil {
		b.resizeFleetPrimarySpecLocked(fleet, *totalTargetCapacity)
	}

	return nil
}

// resizeFleetPrimarySpecLocked applies a new total target capacity to fleet,
// resizing its highest-priority instance type specification (and backing
// CapacityReservation) to match. Must be called with b.mu held.
func (b *InMemoryBackend) resizeFleetPrimarySpecLocked(fleet *CapacityReservationFleet, totalTargetCapacity int32) {
	fleet.TotalTargetCapacity = totalTargetCapacity
	fleet.TotalFulfilledCapacity = float64(totalTargetCapacity)

	if len(fleet.InstanceTypeSpecifications) == 0 {
		return
	}

	primary := &fleet.InstanceTypeSpecifications[0]

	weight := primary.Weight
	if weight <= 0 {
		weight = 1
	}

	primary.TotalInstanceCount = int32(math.Ceil(float64(totalTargetCapacity) / weight))

	cr, found := b.capacityReservations[primary.CapacityReservationID]
	if !found {
		return
	}

	cr.TotalInstanceCount = int(primary.TotalInstanceCount)
	cr.AvailableInstanceCount = int(primary.TotalInstanceCount)
}

// CancelCapacityReservationFleets cancels the given Capacity Reservation Fleets
// and all of their backing Capacity Reservations. It returns the successfully
// cancelled fleets (with previous/current state) and any fleet IDs that could
// not be found.
func (b *InMemoryBackend) CancelCapacityReservationFleets(
	fleetIDs []string,
) ([]CapacityReservationFleetCancellation, []FailedCapacityReservationFleetCancellation) {
	b.mu.Lock("CancelCapacityReservationFleets")
	defer b.mu.Unlock()

	var (
		successful []CapacityReservationFleetCancellation
		failed     []FailedCapacityReservationFleetCancellation
	)

	for _, id := range fleetIDs {
		fleet, ok := b.capacityReservationFleets[id]
		if !ok {
			failed = append(failed, FailedCapacityReservationFleetCancellation{
				CapacityReservationFleetID: id,
				ErrorCode:                  "InvalidCapacityReservationFleetId.NotFound",
				ErrorMessage:               fmt.Sprintf("The Capacity Reservation Fleet ID '%s' does not exist", id),
			})

			continue
		}

		previousState := fleet.State
		fleet.State = stateCancelled

		for _, spec := range fleet.InstanceTypeSpecifications {
			if cr, found := b.capacityReservations[spec.CapacityReservationID]; found {
				cr.State = stateCancelled
			}
		}

		successful = append(successful, CapacityReservationFleetCancellation{
			CapacityReservationFleetID: fleet.CapacityReservationFleetID,
			PreviousState:              previousState,
			CurrentState:               fleet.State,
		})
	}

	return successful, failed
}

// ---- Capacity Block ----

// CapacityBlockReservationStatus mirrors the availability of one Capacity
// Reservation within a Capacity Block.
type CapacityBlockReservationStatus struct {
	CapacityReservationID    string `json:"capacityReservationId,omitempty"`
	TotalCapacity            int32  `json:"totalCapacity,omitempty"`
	TotalAvailableCapacity   int32  `json:"totalAvailableCapacity,omitempty"`
	TotalUnavailableCapacity int32  `json:"totalUnavailableCapacity,omitempty"`
}

// CapacityBlockStatus reports capacity/interconnect availability for a purchased
// Capacity Block.
type CapacityBlockStatus struct {
	CapacityBlockID             string                           `json:"capacityBlockId,omitempty"`
	InterconnectStatus          string                           `json:"interconnectStatus,omitempty"`
	CapacityReservationStatuses []CapacityBlockReservationStatus `json:"capacityReservationStatuses,omitempty"`
	TotalCapacity               int32                            `json:"totalCapacity,omitempty"`
	TotalAvailableCapacity      int32                            `json:"totalAvailableCapacity,omitempty"`
	TotalUnavailableCapacity    int32                            `json:"totalUnavailableCapacity,omitempty"`
}

// DescribeCapacityBlockOfferings generates a static, realistic catalog of
// purchasable Capacity Block offerings for the requested instance type and
// duration across the region's "a" and "b" Availability Zones, caching each
// generated offering so a subsequent PurchaseCapacityBlock call can reference it.
func (b *InMemoryBackend) DescribeCapacityBlockOfferings(
	instanceType string,
	durationHours, instanceCount int32,
) ([]*CapacityBlockOffering, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	if durationHours <= 0 {
		return nil, fmt.Errorf("%w: CapacityDurationHours must be a positive integer", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		instanceCount = 1
	}

	b.mu.Lock("DescribeCapacityBlockOfferings")
	defer b.mu.Unlock()

	hourlyRate := capacityBlockHourlyRate(instanceType)
	upfront := hourlyRate * float64(durationHours) * float64(instanceCount)
	start := time.Now().UTC().Add(capacityBlockOfferingLeadTime)

	var offerings []*CapacityBlockOffering

	for _, azSuffix := range []string{"a", "b"} {
		offering := &CapacityBlockOffering{
			CapacityBlockOfferingID:    "cbo-" + uuid.New().String()[:8],
			InstanceType:               instanceType,
			AvailabilityZone:           b.Region + azSuffix,
			Tenancy:                    crFleetTenancyDefault,
			CurrencyCode:               "USD",
			UpfrontPrice:               fmt.Sprintf("%.2f", upfront),
			InstanceCount:              instanceCount,
			CapacityBlockDurationHours: durationHours,
			StartDate:                  start,
			EndDate:                    start.Add(time.Duration(durationHours) * time.Hour),
		}
		b.capacityBlockOfferings[offering.CapacityBlockOfferingID] = offering
		offerings = append(offerings, offering)
	}

	return offerings, nil
}

// PurchaseCapacityBlock purchases a previously described Capacity Block
// offering, creating a backing CapacityReservation and a CapacityBlock record.
func (b *InMemoryBackend) PurchaseCapacityBlock(
	offeringID, instancePlatform string,
	tags map[string]string,
) (*CapacityBlock, *CapacityReservation, error) {
	if offeringID == "" {
		return nil, nil, fmt.Errorf("%w: CapacityBlockOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("PurchaseCapacityBlock")
	defer b.mu.Unlock()

	offering, ok := b.capacityBlockOfferings[offeringID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityBlockOfferingNotFound, offeringID)
	}

	if instancePlatform == "" {
		instancePlatform = "Linux/UNIX"
	}

	crID := "cr-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	cr := &CapacityReservation{
		CapacityReservationID:  crID,
		InstanceType:           offering.InstanceType,
		AvailabilityZone:       offering.AvailabilityZone,
		InstancePlatform:       instancePlatform,
		TotalInstanceCount:     int(offering.InstanceCount),
		AvailableInstanceCount: int(offering.InstanceCount),
		State:                  stateActive,
		CreateTime:             now,
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations[crID] = cr

	block := &CapacityBlock{
		CapacityBlockID:        "cb-" + uuid.New().String()[:8],
		AvailabilityZone:       offering.AvailabilityZone,
		State:                  stateActive,
		CreateDate:             now,
		StartDate:              offering.StartDate,
		EndDate:                offering.EndDate,
		Tags:                   tags,
		CapacityReservationIDs: []string{crID},
	}
	b.capacityBlocks[block.CapacityBlockID] = block

	delete(b.capacityBlockOfferings, offeringID)

	blockCopy := *block
	crCopy := *cr

	return &blockCopy, &crCopy, nil
}

// findCapacityBlockByReservationIDLocked returns the CapacityBlock backing the
// given CapacityReservation, if any. Must be called with b.mu held.
func (b *InMemoryBackend) findCapacityBlockByReservationIDLocked(reservationID string) *CapacityBlock {
	for _, block := range b.capacityBlocks {
		if slices.Contains(block.CapacityReservationIDs, reservationID) {
			return block
		}
	}

	return nil
}

// DescribeCapacityBlockExtensionOfferings generates a purchasable extension
// offering for the given Capacity Reservation, starting at the Capacity Block's
// current end date.
func (b *InMemoryBackend) DescribeCapacityBlockExtensionOfferings(
	reservationID string,
	durationHours int32,
) ([]*CapacityBlockExtensionOffering, error) {
	if reservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if durationHours <= 0 {
		return nil, fmt.Errorf(
			"%w: CapacityBlockExtensionDurationHours must be a positive integer",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DescribeCapacityBlockExtensionOfferings")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[reservationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, reservationID)
	}

	block := b.findCapacityBlockByReservationIDLocked(reservationID)

	extStart := time.Now().UTC()
	if block != nil {
		extStart = block.EndDate
	}

	hourlyRate := capacityBlockHourlyRate(cr.InstanceType)
	upfront := hourlyRate * float64(durationHours) * float64(cr.TotalInstanceCount)

	offering := &CapacityBlockExtensionOffering{
		CapacityBlockExtensionOfferingID:    "cbeo-" + uuid.New().String()[:8],
		CapacityReservationID:               reservationID,
		InstanceType:                        cr.InstanceType,
		AvailabilityZone:                    cr.AvailabilityZone,
		CurrencyCode:                        "USD",
		UpfrontPrice:                        fmt.Sprintf("%.2f", upfront),
		InstanceCount:                       toInt32Clamped(cr.TotalInstanceCount),
		CapacityBlockExtensionDurationHours: durationHours,
		CapacityBlockExtensionStartDate:     extStart,
		CapacityBlockExtensionEndDate:       extStart.Add(time.Duration(durationHours) * time.Hour),
	}
	b.capacityBlockExtensionOfferings[offering.CapacityBlockExtensionOfferingID] = offering

	return []*CapacityBlockExtensionOffering{offering}, nil
}

// PurchaseCapacityBlockExtension purchases a previously described Capacity
// Block extension offering, extending the backing Capacity Block's end date.
func (b *InMemoryBackend) PurchaseCapacityBlockExtension(
	extensionOfferingID, reservationID string,
) (*CapacityBlockExtension, error) {
	if extensionOfferingID == "" {
		return nil, fmt.Errorf("%w: CapacityBlockExtensionOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("PurchaseCapacityBlockExtension")
	defer b.mu.Unlock()

	offering, ok := b.capacityBlockExtensionOfferings[extensionOfferingID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityBlockExtensionOfferingNotFound, extensionOfferingID)
	}

	if reservationID != "" && reservationID != offering.CapacityReservationID {
		return nil, fmt.Errorf(
			"%w: CapacityReservationId does not match the offering's reservation",
			ErrInvalidParameter,
		)
	}

	now := time.Now().UTC()
	ext := &CapacityBlockExtension{
		CapacityBlockExtensionOfferingID:    offering.CapacityBlockExtensionOfferingID,
		CapacityReservationID:               offering.CapacityReservationID,
		AvailabilityZone:                    offering.AvailabilityZone,
		CapacityBlockExtensionStatus:        "payment-succeeded",
		CapacityBlockExtensionDurationHours: offering.CapacityBlockExtensionDurationHours,
		CapacityBlockExtensionStartDate:     offering.CapacityBlockExtensionStartDate,
		CapacityBlockExtensionEndDate:       offering.CapacityBlockExtensionEndDate,
		CapacityBlockExtensionPurchaseDate:  now,
	}
	b.capacityBlockExtensions[ext.CapacityBlockExtensionOfferingID] = ext

	if block := b.findCapacityBlockByReservationIDLocked(offering.CapacityReservationID); block != nil {
		block.EndDate = offering.CapacityBlockExtensionEndDate
	}

	delete(b.capacityBlockExtensionOfferings, extensionOfferingID)

	return ext, nil
}

// DescribeCapacityBlocks returns Capacity Blocks matching the given IDs (all, if
// empty) and filters (capacity-block-id, availability-zone, state), sorted by ID.
func (b *InMemoryBackend) DescribeCapacityBlocks(
	ids []string,
	filters map[string][]string,
) []*CapacityBlock {
	b.mu.RLock("DescribeCapacityBlocks")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityBlock

	for _, block := range b.capacityBlocks {
		matched := idAndFiltersMatch(idSet, block.CapacityBlockID,
			matchesCapacityFilter(filters, "capacity-block-id", block.CapacityBlockID),
			matchesCapacityFilter(filters, "availability-zone", block.AvailabilityZone),
			matchesCapacityFilter(filters, "state", block.State),
		)
		if !matched {
			continue
		}

		cp := *block
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityBlockID < result[j].CapacityBlockID })

	return result
}

// DescribeCapacityBlockStatus returns the capacity/interconnect availability of
// the given Capacity Blocks (all, if ids is empty), filtered by interconnect-status.
func (b *InMemoryBackend) DescribeCapacityBlockStatus(
	ids []string,
	filters map[string][]string,
) []*CapacityBlockStatus {
	b.mu.RLock("DescribeCapacityBlockStatus")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityBlockStatus

	const interconnectOK = "ok"

	for _, block := range b.capacityBlocks {
		matched := idAndFiltersMatch(idSet, block.CapacityBlockID,
			matchesCapacityFilter(filters, "interconnect-status", interconnectOK),
		)
		if !matched {
			continue
		}

		status := &CapacityBlockStatus{
			CapacityBlockID:    block.CapacityBlockID,
			InterconnectStatus: interconnectOK,
		}

		for _, crID := range block.CapacityReservationIDs {
			cr, ok := b.capacityReservations[crID]
			if !ok {
				continue
			}

			total := toInt32Clamped(cr.TotalInstanceCount)
			available := toInt32Clamped(cr.AvailableInstanceCount)
			status.TotalCapacity += total
			status.TotalAvailableCapacity += available
			status.TotalUnavailableCapacity += total - available
			status.CapacityReservationStatuses = append(status.CapacityReservationStatuses,
				CapacityBlockReservationStatus{
					CapacityReservationID:    crID,
					TotalCapacity:            total,
					TotalAvailableCapacity:   available,
					TotalUnavailableCapacity: total - available,
				})
		}

		result = append(result, status)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityBlockID < result[j].CapacityBlockID })

	return result
}

// DescribeCapacityBlockExtensionHistory returns purchased Capacity Block
// extensions for the given Capacity Reservation IDs (all, if empty), filtered by
// capacity-reservation-id, availability-zone, and capacity-block-extension-status.
func (b *InMemoryBackend) DescribeCapacityBlockExtensionHistory(
	reservationIDs []string,
	filters map[string][]string,
) []*CapacityBlockExtension {
	b.mu.RLock("DescribeCapacityBlockExtensionHistory")
	defer b.mu.RUnlock()

	idSet := toIDSet(reservationIDs)

	var result []*CapacityBlockExtension

	for _, ext := range b.capacityBlockExtensions {
		matched := idAndFiltersMatch(idSet, ext.CapacityReservationID,
			matchesCapacityFilter(filters, "capacity-reservation-id", ext.CapacityReservationID),
			matchesCapacityFilter(filters, "availability-zone", ext.AvailabilityZone),
			matchesCapacityFilter(filters, "capacity-block-extension-status", ext.CapacityBlockExtensionStatus),
		)
		if !matched {
			continue
		}

		cp := *ext
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityBlockExtensionOfferingID < result[j].CapacityBlockExtensionOfferingID
	})

	return result
}

// ---- Capacity Reservation splitting / instance movement ----

// CreateCapacityReservationBySplitting splits instanceCount instances off of
// the source Capacity Reservation into a brand-new destination reservation with
// the same instance type and Availability Zone.
func (b *InMemoryBackend) CreateCapacityReservationBySplitting(
	sourceID string,
	instanceCount int,
	tags map[string]string,
) (*CapacityReservation, *CapacityReservation, error) {
	if sourceID == "" {
		return nil, nil, fmt.Errorf("%w: SourceCapacityReservationId is required", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		return nil, nil, fmt.Errorf("%w: InstanceCount must be a positive integer", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityReservationBySplitting")
	defer b.mu.Unlock()

	src, ok := b.capacityReservations[sourceID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceID)
	}

	if src.AvailableInstanceCount < instanceCount {
		return nil, nil, fmt.Errorf(
			"%w: source Capacity Reservation only has %d available instances",
			ErrCapacityReservationFull, src.AvailableInstanceCount,
		)
	}

	src.AvailableInstanceCount -= instanceCount
	src.TotalInstanceCount -= instanceCount

	dst := &CapacityReservation{
		CapacityReservationID:  "cr-" + uuid.New().String()[:8],
		InstanceType:           src.InstanceType,
		AvailabilityZone:       src.AvailabilityZone,
		InstancePlatform:       src.InstancePlatform,
		TotalInstanceCount:     instanceCount,
		AvailableInstanceCount: instanceCount,
		State:                  stateActive,
		CreateTime:             time.Now().UTC(),
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations[dst.CapacityReservationID] = dst

	if len(tags) > 0 {
		b.tags[dst.CapacityReservationID] = tags
	}

	srcCopy := *src
	dstCopy := *dst

	return &dstCopy, &srcCopy, nil
}

// MoveCapacityReservationInstances moves instanceCount instances from the
// source Capacity Reservation into the destination Capacity Reservation. Both
// reservations must already exist and share the same instance type.
func (b *InMemoryBackend) MoveCapacityReservationInstances(
	sourceID, destinationID string,
	instanceCount int,
) (*CapacityReservation, *CapacityReservation, error) {
	if sourceID == "" || destinationID == "" {
		return nil, nil, fmt.Errorf(
			"%w: SourceCapacityReservationId and DestinationCapacityReservationId are required",
			ErrInvalidParameter,
		)
	}

	if instanceCount <= 0 {
		return nil, nil, fmt.Errorf("%w: InstanceCount must be a positive integer", ErrInvalidParameter)
	}

	b.mu.Lock("MoveCapacityReservationInstances")
	defer b.mu.Unlock()

	src, ok := b.capacityReservations[sourceID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceID)
	}

	dst, ok := b.capacityReservations[destinationID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, destinationID)
	}

	if src.AvailableInstanceCount < instanceCount {
		return nil, nil, fmt.Errorf(
			"%w: source Capacity Reservation only has %d available instances",
			ErrCapacityReservationFull, src.AvailableInstanceCount,
		)
	}

	src.AvailableInstanceCount -= instanceCount
	src.TotalInstanceCount -= instanceCount
	dst.AvailableInstanceCount += instanceCount
	dst.TotalInstanceCount += instanceCount

	srcCopy := *src
	dstCopy := *dst

	return &dstCopy, &srcCopy, nil
}

// ---- Capacity Reservation billing ownership ----

// AssociateCapacityReservationBillingOwner creates (or replaces) a pending
// request to assign billing for the given Capacity Reservation to another
// account.
func (b *InMemoryBackend) AssociateCapacityReservationBillingOwner(
	reservationID, billingOwnerID string,
) error {
	if reservationID == "" || billingOwnerID == "" {
		return fmt.Errorf(
			"%w: CapacityReservationId and UnusedReservationBillingOwnerId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AssociateCapacityReservationBillingOwner")
	defer b.mu.Unlock()

	if _, ok := b.capacityReservations[reservationID]; !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, reservationID)
	}

	b.capacityReservationBillingRequests[reservationID] = &CapacityReservationBillingRequest{
		CapacityReservationID:           reservationID,
		RequestedBy:                     b.AccountID,
		UnusedReservationBillingOwnerID: billingOwnerID,
		Status:                          billingRequestPending,
		LastUpdateTime:                  time.Now().UTC(),
	}

	return nil
}

// DisassociateCapacityReservationBillingOwner revokes a pending or accepted
// billing ownership request for the given Capacity Reservation.
func (b *InMemoryBackend) DisassociateCapacityReservationBillingOwner(
	reservationID, billingOwnerID string,
) error {
	if reservationID == "" || billingOwnerID == "" {
		return fmt.Errorf(
			"%w: CapacityReservationId and UnusedReservationBillingOwnerId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DisassociateCapacityReservationBillingOwner")
	defer b.mu.Unlock()

	req, ok := b.capacityReservationBillingRequests[reservationID]
	if !ok || req.UnusedReservationBillingOwnerID != billingOwnerID {
		return fmt.Errorf("%w: %s", ErrCapacityReservationBillingRequestNotFound, reservationID)
	}

	req.Status = billingRequestRevoked
	req.LastUpdateTime = time.Now().UTC()

	return nil
}

// RejectCapacityReservationBillingOwnership rejects a pending billing ownership
// request sent to this account for the given Capacity Reservation.
func (b *InMemoryBackend) RejectCapacityReservationBillingOwnership(
	reservationID string,
) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectCapacityReservationBillingOwnership")
	defer b.mu.Unlock()

	req, ok := b.capacityReservationBillingRequests[reservationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationBillingRequestNotFound, reservationID)
	}

	req.Status = billingRequestRejected
	req.LastUpdateTime = time.Now().UTC()

	return nil
}

// DescribeCapacityReservationBillingRequests returns billing ownership requests
// matching the given Capacity Reservation IDs (all, if empty) and filters
// (status).
func (b *InMemoryBackend) DescribeCapacityReservationBillingRequests(
	ids []string,
	filters map[string][]string,
) []*CapacityReservationBillingRequest {
	b.mu.RLock("DescribeCapacityReservationBillingRequests")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityReservationBillingRequest

	for _, req := range b.capacityReservationBillingRequests {
		if len(idSet) > 0 && !idSet[req.CapacityReservationID] {
			continue
		}

		if !matchesCapacityFilter(filters, "status", req.Status) {
			continue
		}

		cp := *req
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityReservationID < result[j].CapacityReservationID
	})

	return result
}

// ---- Capacity Manager ----

// EnableCapacityManager enables Capacity Manager for the account, optionally
// enabling cross-account Organizations data aggregation.
func (b *InMemoryBackend) EnableCapacityManager(organizationsAccess bool) (string, bool) {
	b.mu.Lock("EnableCapacityManager")
	defer b.mu.Unlock()

	b.capacityManagerState.Status = capacityManagerStatusEnabled
	b.capacityManagerState.OrganizationsAccess = organizationsAccess

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// DisableCapacityManager disables Capacity Manager for the account, clearing
// any Organizations data aggregation setting.
func (b *InMemoryBackend) DisableCapacityManager() (string, bool) {
	b.mu.Lock("DisableCapacityManager")
	defer b.mu.Unlock()

	b.capacityManagerState.Status = capacityManagerStatusDisabled
	b.capacityManagerState.OrganizationsAccess = false

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// UpdateCapacityManagerOrganizationsAccess updates the Organizations
// cross-account data aggregation setting for Capacity Manager.
func (b *InMemoryBackend) UpdateCapacityManagerOrganizationsAccess(
	organizationsAccess bool,
) (string, bool) {
	b.mu.Lock("UpdateCapacityManagerOrganizationsAccess")
	defer b.mu.Unlock()

	b.capacityManagerState.OrganizationsAccess = organizationsAccess

	return b.capacityManagerState.Status, b.capacityManagerState.OrganizationsAccess
}

// CapacityManagerAttributes reports the current Capacity Manager status and
// data ingestion state for the account.
type CapacityManagerAttributes struct {
	Status                 string
	IngestionStatus        string
	IngestionStatusMessage string
	OrganizationsAccess    bool
	DataExportCount        int32
}

// GetCapacityManagerAttributes returns the current Capacity Manager status,
// data export count, and ingestion state for the account.
func (b *InMemoryBackend) GetCapacityManagerAttributes() *CapacityManagerAttributes {
	b.mu.RLock("GetCapacityManagerAttributes")
	defer b.mu.RUnlock()

	attrs := &CapacityManagerAttributes{
		Status:              b.capacityManagerState.Status,
		OrganizationsAccess: b.capacityManagerState.OrganizationsAccess,
		DataExportCount:     toInt32Clamped(len(b.capacityManagerDataExports)),
	}

	if attrs.Status == capacityManagerStatusEnabled {
		attrs.IngestionStatus = "initial-ingestion-in-progress"
		attrs.IngestionStatusMessage = "Capacity Manager is ingesting historical data; this may take several hours."
	}

	return attrs
}

// GetCapacityManagerMetricData always returns an empty result set: this backend
// does not simulate the historical utilization pipeline Capacity Manager
// aggregates from, so there is no ingested data to report. The response shape
// matches AWS's for an account with no data points yet available.
func (b *InMemoryBackend) GetCapacityManagerMetricData() []MetricDataResult {
	return nil
}

// MetricDataResult is a placeholder for a single Capacity Manager metric data
// result. It is never populated by this backend (see GetCapacityManagerMetricData)
// but is defined so callers have a stable, documented return type.
type MetricDataResult struct {
	MetricName string
	Timestamps []time.Time
	Values     []float64
}

// GetCapacityManagerMetricDimensions always returns an empty result set for the
// same reason as GetCapacityManagerMetricData.
func (b *InMemoryBackend) GetCapacityManagerMetricDimensions() []CapacityManagerDimension {
	return nil
}

// CapacityManagerDimension is a placeholder for a single Capacity Manager
// dimension combination. Never populated by this backend.
type CapacityManagerDimension struct {
	Dimensions map[string]string
}

// CreateCapacityManagerDataExport configures a new periodic Capacity Manager
// data export to the given S3 bucket.
func (b *InMemoryBackend) CreateCapacityManagerDataExport(
	outputFormat, s3BucketName, s3BucketPrefix, schedule string,
	tags map[string]string,
) (*CapacityManagerDataExport, error) {
	if outputFormat == "" || s3BucketName == "" || schedule == "" {
		return nil, fmt.Errorf(
			"%w: OutputFormat, S3BucketName, and Schedule are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateCapacityManagerDataExport")
	defer b.mu.Unlock()

	export := &CapacityManagerDataExport{
		CapacityManagerDataExportID: "cmde-" + uuid.New().String()[:8],
		OutputFormat:                outputFormat,
		S3BucketName:                s3BucketName,
		S3BucketPrefix:              s3BucketPrefix,
		Schedule:                    schedule,
		CreateTime:                  time.Now().UTC(),
		Tags:                        tags,
	}
	b.capacityManagerDataExports[export.CapacityManagerDataExportID] = export

	return export, nil
}

// DescribeCapacityManagerDataExports returns Capacity Manager data export
// configurations matching the given IDs (all, if empty).
func (b *InMemoryBackend) DescribeCapacityManagerDataExports(
	ids []string,
) []*CapacityManagerDataExport {
	b.mu.RLock("DescribeCapacityManagerDataExports")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityManagerDataExport

	for _, export := range b.capacityManagerDataExports {
		if len(idSet) > 0 && !idSet[export.CapacityManagerDataExportID] {
			continue
		}

		cp := *export
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityManagerDataExportID < result[j].CapacityManagerDataExportID
	})

	return result
}

// DeleteCapacityManagerDataExport removes a Capacity Manager data export
// configuration, returning its ID.
func (b *InMemoryBackend) DeleteCapacityManagerDataExport(id string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("%w: CapacityManagerDataExportId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCapacityManagerDataExport")
	defer b.mu.Unlock()

	if _, ok := b.capacityManagerDataExports[id]; !ok {
		return "", fmt.Errorf("%w: %s", ErrCapacityManagerDataExportNotFound, id)
	}

	delete(b.capacityManagerDataExports, id)

	return id, nil
}
