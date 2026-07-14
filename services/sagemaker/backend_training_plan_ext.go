package sagemaker

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// trainingPlanStatusScheduled mirrors AWS's TrainingPlanStatus "Scheduled"
// value, reached once a plan has been purchased against a real offering.
const trainingPlanStatusScheduled = "Scheduled"

// Static catalog constants: currency code and the duration/instance-count
// values used to build trainingPlanOfferingCatalog below.
const (
	catalogCurrencyUSD = "USD"

	durationHours7Days  = 168
	durationHours14Days = 336
	durationHours30Days = 720

	p5InstanceCount    = 8
	trn2InstanceCount  = 16
	gb200InstanceCount = 72
)

// trainingPlanSortByStartTime is the "StartTime" TrainingPlanSortBy value
// (also reused as the ListTrainingPlans filter field name for training plan
// start time comparisons).
const trainingPlanSortByStartTime = "StartTime"

// ErrReservedCapacityNotFound is returned when a reserved capacity ARN does not exist.
var ErrReservedCapacityNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// durationFromHoursMinutes converts a whole-hours + additional-minutes pair
// (as used throughout the TrainingPlan/ReservedCapacity wire shapes) into a
// [time.Duration].
func durationFromHoursMinutes(hours, minutes int64) time.Duration {
	return time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute
}

// ---------------------------------------------------------------------------
// ReservedCapacity — the compute backing a TrainingPlan.
// ---------------------------------------------------------------------------

// UltraServer represents a single UltraServer node group within a Reserved
// Capacity, mirroring types.UltraServer.
type UltraServer struct {
	AvailabilityZone             string `json:"AvailabilityZone"`
	InstanceType                 string `json:"InstanceType"`
	UltraServerID                string `json:"UltraServerId"`
	UltraServerType              string `json:"UltraServerType"`
	HealthStatus                 string `json:"HealthStatus,omitempty"`
	TotalInstanceCount           int32  `json:"TotalInstanceCount"`
	AvailableInstanceCount       int32  `json:"AvailableInstanceCount,omitempty"`
	AvailableSpareInstanceCount  int32  `json:"AvailableSpareInstanceCount,omitempty"`
	ConfiguredSpareInstanceCount int32  `json:"ConfiguredSpareInstanceCount,omitempty"`
	InUseInstanceCount           int32  `json:"InUseInstanceCount,omitempty"`
}

// ReservedCapacity represents provisioned compute capacity backing a training plan.
type ReservedCapacity struct {
	StartTime              time.Time      `json:"StartTime"`
	EndTime                time.Time      `json:"EndTime"`
	Status                 string         `json:"Status"`
	ReservedCapacityArn    string         `json:"ReservedCapacityArn"`
	InstanceType           string         `json:"InstanceType"`
	AvailabilityZone       string         `json:"AvailabilityZone,omitempty"`
	ReservedCapacityType   string         `json:"ReservedCapacityType,omitempty"`
	TrainingPlanArn        string         `json:"-"`
	UltraServers           []*UltraServer `json:"-"`
	DurationHours          int64          `json:"DurationHours,omitempty"`
	DurationMinutes        int64          `json:"DurationMinutes,omitempty"`
	TotalInstanceCount     int32          `json:"TotalInstanceCount"`
	AvailableInstanceCount int32          `json:"AvailableInstanceCount,omitempty"`
	InUseInstanceCount     int32          `json:"InUseInstanceCount,omitempty"`
}

func cloneReservedCapacity(rc *ReservedCapacity) *ReservedCapacity {
	cp := *rc
	cp.UltraServers = make([]*UltraServer, len(rc.UltraServers))

	for i, u := range rc.UltraServers {
		uCopy := *u
		cp.UltraServers[i] = &uCopy
	}

	return &cp
}

// toSummary projects a ReservedCapacity to the ReservedCapacitySummary shape
// nested inside TrainingPlan / DescribeTrainingPlan responses.
func (rc *ReservedCapacity) toSummary() *ReservedCapacitySummary {
	start := rc.StartTime
	end := rc.EndTime

	return &ReservedCapacitySummary{
		InstanceType:         rc.InstanceType,
		ReservedCapacityArn:  rc.ReservedCapacityArn,
		Status:               rc.Status,
		AvailabilityZone:     rc.AvailabilityZone,
		ReservedCapacityType: rc.ReservedCapacityType,
		TotalInstanceCount:   rc.TotalInstanceCount,
		DurationHours:        rc.DurationHours,
		DurationMinutes:      rc.DurationMinutes,
		StartTime:            &start,
		EndTime:              &end,
	}
}

// createReservedCapacity provisions a ReservedCapacity (and, for UltraServer
// offerings, its backing UltraServer) from a catalog offering. Called with
// b.mu held.
func (b *InMemoryBackend) createReservedCapacity(
	region, trainingPlanArn string,
	rco ReservedCapacityOffering,
	now time.Time,
	spareInstanceCountPerUltraServer int32,
) *ReservedCapacity {
	rcArn := arn.Build("sagemaker", region, b.accountID, "reserved-capacity/"+generateID())
	end := now.Add(durationFromHoursMinutes(rco.DurationHours, rco.DurationMinutes))

	rc := &ReservedCapacity{
		ReservedCapacityArn:    rcArn,
		InstanceType:           rco.InstanceType,
		AvailabilityZone:       rco.AvailabilityZone,
		Status:                 statusActive,
		TrainingPlanArn:        trainingPlanArn,
		TotalInstanceCount:     rco.InstanceCount,
		AvailableInstanceCount: rco.InstanceCount,
		DurationHours:          rco.DurationHours,
		DurationMinutes:        rco.DurationMinutes,
		StartTime:              now,
		EndTime:                end,
	}

	if rco.IsUltraServer {
		rc.ReservedCapacityType = "UltraServer"
		spare := spareInstanceCountPerUltraServer
		available := max(rco.InstanceCount-spare, 0)

		rc.UltraServers = []*UltraServer{{
			AvailabilityZone:             rco.AvailabilityZone,
			InstanceType:                 rco.InstanceType,
			UltraServerID:                generateID(),
			UltraServerType:              rco.UltraServerType,
			HealthStatus:                 "Healthy",
			TotalInstanceCount:           rco.InstanceCount,
			AvailableInstanceCount:       available,
			ConfiguredSpareInstanceCount: spare,
		}}
	} else {
		rc.ReservedCapacityType = "Instance"
	}

	b.reservedCapacitiesStore(region).Put(rc)

	return rc
}

// DescribeReservedCapacity returns a reserved capacity by ARN.
func (b *InMemoryBackend) DescribeReservedCapacity(
	ctx context.Context,
	reservedCapacityArn string,
) (*ReservedCapacity, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeReservedCapacity")
	defer b.mu.RUnlock()

	rc, ok := b.reservedCapacitiesStoreRO(region).Get(reservedCapacityArn)
	if !ok {
		return nil, fmt.Errorf("%w: reserved capacity %q not found", ErrReservedCapacityNotFound, reservedCapacityArn)
	}

	return cloneReservedCapacity(rc), nil
}

// ListUltraServersByReservedCapacity lists the UltraServers backing a reserved capacity.
func (b *InMemoryBackend) ListUltraServersByReservedCapacity(
	ctx context.Context,
	reservedCapacityArn, nextToken string,
	maxResults int32,
) ([]*UltraServer, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListUltraServersByReservedCapacity")
	defer b.mu.RUnlock()

	rc, ok := b.reservedCapacitiesStoreRO(region).Get(reservedCapacityArn)
	if !ok {
		return nil, "", fmt.Errorf(
			"%w: reserved capacity %q not found", ErrReservedCapacityNotFound, reservedCapacityArn,
		)
	}

	list := make([]*UltraServer, len(rc.UltraServers))
	for i, u := range rc.UltraServers {
		uCopy := *u
		list[i] = &uCopy
	}

	page, next := paginateSlice(list, nextToken, maxResults)

	return page, next, nil
}

// ---------------------------------------------------------------------------
// Static training plan offering catalog.
// ---------------------------------------------------------------------------

// ReservedCapacityOffering describes one block of compute within a catalog
// TrainingPlanOffering, mirroring types.ReservedCapacityOffering.
type ReservedCapacityOffering struct {
	InstanceType     string
	AvailabilityZone string
	UltraServerType  string
	DurationHours    int64
	DurationMinutes  int64
	InstanceCount    int32
	IsUltraServer    bool
}

// TrainingPlanOffering is a catalog entry returned by SearchTrainingPlanOfferings.
type TrainingPlanOffering struct {
	TrainingPlanOfferingID    string
	CurrencyCode              string
	UpfrontFee                string
	TargetResources           []string
	ReservedCapacityOfferings []ReservedCapacityOffering
	DurationHours             int64
	DurationMinutes           int64
}

// trainingPlanOfferingCatalog is a static, realistic catalog of purchasable
// training plan offerings, mirroring what SearchTrainingPlanOfferings returns
// in real AWS for common GPU reservation requests.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var trainingPlanOfferingCatalog = []TrainingPlanOffering{
	{
		TrainingPlanOfferingID: "tpo-p5-48xlarge-30d",
		TargetResources:        []string{"training-job"},
		CurrencyCode:           catalogCurrencyUSD,
		UpfrontFee:             "184320.00",
		DurationHours:          durationHours30Days,
		ReservedCapacityOfferings: []ReservedCapacityOffering{
			{
				InstanceType: "ml.p5.48xlarge", InstanceCount: p5InstanceCount,
				AvailabilityZone: "a", DurationHours: durationHours30Days,
			},
		},
	},
	{
		TrainingPlanOfferingID: "tpo-trn2-48xlarge-14d",
		TargetResources:        []string{"training-job", "hyperpod-cluster"},
		CurrencyCode:           catalogCurrencyUSD,
		UpfrontFee:             "42000.00",
		DurationHours:          durationHours14Days,
		ReservedCapacityOfferings: []ReservedCapacityOffering{
			{
				InstanceType: "ml.trn2.48xlarge", InstanceCount: trn2InstanceCount,
				AvailabilityZone: "a", DurationHours: durationHours14Days,
			},
		},
	},
	{
		TrainingPlanOfferingID: "tpo-ultraserver-gb200-30d",
		TargetResources:        []string{"hyperpod-cluster"},
		CurrencyCode:           catalogCurrencyUSD,
		UpfrontFee:             "552000.00",
		DurationHours:          durationHours30Days,
		ReservedCapacityOfferings: []ReservedCapacityOffering{
			{
				InstanceType:     "ml.p6e-gb200.36xlarge",
				InstanceCount:    gb200InstanceCount,
				AvailabilityZone: "b",
				DurationHours:    durationHours30Days,
				IsUltraServer:    true,
				UltraServerType:  "ml.u-p6e-gb200x72",
			},
		},
	},
}

func findTrainingPlanOffering(id string) (*TrainingPlanOffering, bool) {
	if id == "" {
		return nil, false
	}

	for i := range trainingPlanOfferingCatalog {
		if trainingPlanOfferingCatalog[i].TrainingPlanOfferingID == id {
			return &trainingPlanOfferingCatalog[i], true
		}
	}

	return nil, false
}

func offeringMatchesInstanceType(o *TrainingPlanOffering, instanceType string) bool {
	if instanceType == "" {
		return true
	}

	for _, rco := range o.ReservedCapacityOfferings {
		if rco.InstanceType == instanceType {
			return true
		}
	}

	return false
}

func offeringMatchesUltraServerType(o *TrainingPlanOffering, ultraServerType string) bool {
	if ultraServerType == "" {
		return true
	}

	for _, rco := range o.ReservedCapacityOfferings {
		if rco.IsUltraServer && rco.UltraServerType == ultraServerType {
			return true
		}
	}

	return false
}

func offeringMatchesTargetResources(o *TrainingPlanOffering, targets []string) bool {
	if len(targets) == 0 {
		return true
	}

	for _, want := range targets {
		if slices.Contains(o.TargetResources, want) {
			return true
		}
	}

	return false
}

// TrainingPlanExtensionOffering is a purchasable extension for an existing
// training plan, returned by SearchTrainingPlanOfferings when TrainingPlanArn
// is specified.
type TrainingPlanExtensionOffering struct {
	StartDate                       time.Time `json:"StartDate"`
	EndDate                         time.Time `json:"EndDate"`
	TrainingPlanExtensionOfferingID string    `json:"TrainingPlanExtensionOfferingId"`
	CurrencyCode                    string    `json:"CurrencyCode,omitempty"`
	AvailabilityZone                string    `json:"AvailabilityZone,omitempty"`
	DurationHours                   int32     `json:"DurationHours"`
}

// pendingTrainingPlanExtension is the internal state recorded when
// SearchTrainingPlanOfferings generates an extension offering, consumed by a
// later ExtendTrainingPlan call.
type pendingTrainingPlanExtension struct {
	StartDate       time.Time `json:"StartDate"`
	EndDate         time.Time `json:"EndDate"`
	TrainingPlanArn string    `json:"TrainingPlanArn"`
	CurrencyCode    string    `json:"CurrencyCode"`
	ID              string    `json:"id"`
	DurationHours   int32     `json:"DurationHours"`
}

// SearchTrainingPlanOfferingsParams bundles the filter criteria accepted by
// SearchTrainingPlanOfferings.
type SearchTrainingPlanOfferingsParams struct {
	InstanceType     string
	UltraServerType  string
	TrainingPlanArn  string
	TargetResources  []string
	DurationHours    int64
	InstanceCount    int32
	UltraServerCount int32
}

// SearchTrainingPlanOfferings searches the static offering catalog and, when a
// TrainingPlanArn is provided, generates extension offerings for that plan.
func (b *InMemoryBackend) SearchTrainingPlanOfferings(
	ctx context.Context,
	params SearchTrainingPlanOfferingsParams,
) ([]TrainingPlanOffering, []TrainingPlanExtensionOffering, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("SearchTrainingPlanOfferings")
	defer b.mu.Unlock()

	offerings := make([]TrainingPlanOffering, 0, len(trainingPlanOfferingCatalog))

	for i := range trainingPlanOfferingCatalog {
		o := &trainingPlanOfferingCatalog[i]
		if !offeringMatchesInstanceType(o, params.InstanceType) {
			continue
		}

		if !offeringMatchesUltraServerType(o, params.UltraServerType) {
			continue
		}

		if !offeringMatchesTargetResources(o, params.TargetResources) {
			continue
		}

		if params.DurationHours > 0 && o.DurationHours != params.DurationHours {
			continue
		}

		offerings = append(offerings, *o)
	}

	if params.TrainingPlanArn == "" {
		return offerings, nil, nil
	}

	plan, ok := b.findTrainingPlanByARN(region, params.TrainingPlanArn)
	if !ok {
		return nil, nil, fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, params.TrainingPlanArn)
	}

	return offerings, b.generateExtensionOfferings(region, plan), nil
}

// generateExtensionOfferings creates 7-day and 30-day extension offerings for
// plan, recording pending state consumed by a later ExtendTrainingPlan call.
// Called with b.mu held.
func (b *InMemoryBackend) generateExtensionOfferings(
	region string,
	plan *TrainingPlan,
) []TrainingPlanExtensionOffering {
	base := time.Now()
	if plan.EndTime != nil {
		base = *plan.EndTime
	}

	durations := []int32{durationHours7Days, durationHours30Days}
	out := make([]TrainingPlanExtensionOffering, 0, len(durations))
	store := b.trainingPlanExtensionOfferingsStore(region)

	for _, d := range durations {
		id := "ext-" + generateID()
		end := base.Add(time.Duration(d) * time.Hour)

		store.Put(&pendingTrainingPlanExtension{
			TrainingPlanArn: plan.TrainingPlanArn,
			DurationHours:   d,
			StartDate:       base,
			EndDate:         end,
			CurrencyCode:    plan.CurrencyCode,
			ID:              id,
		})

		out = append(out, TrainingPlanExtensionOffering{
			TrainingPlanExtensionOfferingID: id,
			DurationHours:                   d,
			StartDate:                       base,
			EndDate:                         end,
			CurrencyCode:                    plan.CurrencyCode,
		})
	}

	return out
}

// ExtendTrainingPlan consumes a pending extension offering (returned earlier
// by SearchTrainingPlanOfferings) and extends the associated plan's duration.
func (b *InMemoryBackend) ExtendTrainingPlan(
	ctx context.Context,
	extensionOfferingID string,
) ([]*TrainingPlanExtension, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ExtendTrainingPlan")
	defer b.mu.Unlock()

	offerings := b.trainingPlanExtensionOfferingsStore(region)

	pending, ok := offerings.Get(extensionOfferingID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: extension offering %q not found", ErrValidation, extensionOfferingID,
		)
	}

	plan, ok := b.findTrainingPlanByARN(region, pending.TrainingPlanArn)
	if !ok {
		return nil, fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, pending.TrainingPlanArn)
	}

	ext := &TrainingPlanExtension{
		TrainingPlanExtensionOfferingID: extensionOfferingID,
		DurationHours:                   pending.DurationHours,
		StartDate:                       pending.StartDate,
		EndDate:                         pending.EndDate,
		CurrencyCode:                    pending.CurrencyCode,
		PaymentStatus:                   "Paid",
		ExtendedAt:                      time.Now(),
	}
	plan.Extensions = append(plan.Extensions, ext)
	plan.DurationHours += int64(pending.DurationHours)
	endDate := pending.EndDate
	plan.EndTime = &endDate
	offerings.Delete(extensionOfferingID)

	return append([]*TrainingPlanExtension(nil), plan.Extensions...), nil
}

// DescribeTrainingPlanExtensionHistory returns the extension history for a training plan.
func (b *InMemoryBackend) DescribeTrainingPlanExtensionHistory(
	ctx context.Context,
	trainingPlanArn, nextToken string,
	maxResults int32,
) ([]*TrainingPlanExtension, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeTrainingPlanExtensionHistory")
	defer b.mu.RUnlock()

	plan, ok := b.findTrainingPlanByARN(region, trainingPlanArn)
	if !ok {
		return nil, "", fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, trainingPlanArn)
	}

	list := make([]*TrainingPlanExtension, len(plan.Extensions))
	for i, e := range plan.Extensions {
		eCopy := *e
		list[i] = &eCopy
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ExtendedAt.Before(list[j].ExtendedAt) })

	page, next := paginateSlice(list, nextToken, maxResults)

	return page, next, nil
}

// findTrainingPlanByARN scans the region's training plans for a matching ARN.
// Called with b.mu held (read or write).
func (b *InMemoryBackend) findTrainingPlanByARN(region, trainingPlanArn string) (*TrainingPlan, bool) {
	for _, t := range b.trainingPlansStoreRO(region).All() {
		if t.TrainingPlanArn == trainingPlanArn {
			return t, true
		}
	}

	return nil, false
}

// ---------------------------------------------------------------------------
// ListTrainingPlans
// ---------------------------------------------------------------------------

// ListTrainingPlansParams bundles the filter/sort criteria for ListTrainingPlans.
type ListTrainingPlansParams struct {
	StatusEquals string
	SortBy       string
	SortOrder    string
	NextToken    string
	MaxResults   int32
}

// ListTrainingPlans lists training plans, optionally filtered by status and sorted.
func (b *InMemoryBackend) ListTrainingPlans(
	ctx context.Context,
	params ListTrainingPlansParams,
) ([]*TrainingPlan, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTrainingPlans")
	defer b.mu.RUnlock()

	store := b.trainingPlansStoreRO(region)
	list := make([]*TrainingPlan, 0, store.Len())

	for _, t := range store.All() {
		if params.StatusEquals != "" && t.Status != params.StatusEquals {
			continue
		}

		list = append(list, cloneTrainingPlan(t))
	}

	sortTrainingPlans(list, params.SortBy, params.SortOrder)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func trainingPlanStartTime(t *TrainingPlan) time.Time {
	if t.StartTime != nil {
		return *t.StartTime
	}

	return t.CreationTime
}

func sortTrainingPlans(list []*TrainingPlan, sortBy, sortOrder string) {
	desc := sortOrder == "Descending"

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch sortBy {
		case keyStatus:
			less = list[i].Status < list[j].Status
		case trainingPlanSortByStartTime:
			less = trainingPlanStartTime(list[i]).Before(trainingPlanStartTime(list[j]))
		default:
			less = list[i].TrainingPlanName < list[j].TrainingPlanName
		}

		if desc {
			return !less
		}

		return less
	})
}
