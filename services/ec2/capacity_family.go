package ec2

import (
	"errors"
	"math"
	"slices"
	"time"
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

	capacityReservationCancellationQuoteStateActive  = "active"
	capacityReservationCancellationQuoteStateExpired = "expired"

	capacityManagerTagKeyStatusActivated = "activated"
	capacityManagerTagKeyStatusSuspended = "suspended"
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
	CreateDate             time.Time `json:"createDate"`
	StartDate              time.Time `json:"startDate"`
	EndDate                time.Time `json:"endDate"`
	CapacityBlockID        string    `json:"capacityBlockId,omitempty"`
	AvailabilityZone       string    `json:"availabilityZone,omitempty"`
	State                  string    `json:"state,omitempty"`
	CapacityReservationIDs []string  `json:"capacityReservationIds,omitempty"`
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
	CreateTime                  time.Time `json:"createTime"`
	CapacityManagerDataExportID string    `json:"capacityManagerDataExportId,omitempty"`
	OutputFormat                string    `json:"outputFormat,omitempty"`
	S3BucketName                string    `json:"s3BucketName,omitempty"`
	S3BucketPrefix              string    `json:"s3BucketPrefix,omitempty"`
	Schedule                    string    `json:"schedule,omitempty"`
	LatestDeliveryStatus        string    `json:"latestDeliveryStatus,omitempty"`
}

// CapacityManagerState tracks the account-level enable/disable state of
// Capacity Manager.
type CapacityManagerState struct {
	MonitoredTagKeys    map[string]*CapacityManagerMonitoredTagKey `json:"monitoredTagKeys,omitempty"`
	Status              string                                     `json:"status,omitempty"`
	OrganizationsAccess bool                                       `json:"organizationsAccess,omitempty"`
}

// CapacityManagerMonitoredTagKey describes a tag key Capacity Manager
// includes as a dimension in capacity metric data, set via
// UpdateCapacityManagerMonitoredTagKeys.
type CapacityManagerMonitoredTagKey struct {
	TagKey                  string `json:"tagKey,omitempty"`
	Status                  string `json:"status,omitempty"`
	StatusMessage           string `json:"statusMessage,omitempty"`
	CapacityManagerProvided bool   `json:"capacityManagerProvided,omitempty"`
}

// CapacityReservationCancellationQuote represents a quote for cancelling a
// future-dated Capacity Reservation during its commitment duration, created
// via CreateCapacityReservationCancellationQuote.
type CapacityReservationCancellationQuote struct {
	CreateTime                             time.Time `json:"createTime"`
	ExpirationTime                         time.Time `json:"expirationTime"`
	CapacityReservationCancellationQuoteID string    `json:"capacityReservationCancellationQuoteId,omitempty"`
	CapacityReservationID                  string    `json:"capacityReservationId,omitempty"`
	QuoteState                             string    `json:"quoteState,omitempty"`
	CurrentReservationState                string    `json:"currentReservationState,omitempty"`
	CurrentInstanceCount                   int32     `json:"currentInstanceCount,omitempty"`
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
