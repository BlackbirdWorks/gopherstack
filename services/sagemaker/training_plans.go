package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrTrainingPlanNotFound is returned when a training plan does not exist.
var ErrTrainingPlanNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// TrainingPlan
// ---------------------------------------------------------------------------

// ReservedCapacitySummary is the wire shape nested inside a TrainingPlan for
// each Reserved Capacity backing it.
type ReservedCapacitySummary struct {
	EndTime              *time.Time `json:"EndTime,omitempty"`
	StartTime            *time.Time `json:"StartTime,omitempty"`
	InstanceType         string     `json:"InstanceType"`
	ReservedCapacityArn  string     `json:"ReservedCapacityArn"`
	Status               string     `json:"Status"`
	AvailabilityZone     string     `json:"AvailabilityZone,omitempty"`
	ReservedCapacityType string     `json:"ReservedCapacityType,omitempty"`
	TotalInstanceCount   int32      `json:"TotalInstanceCount"`
	DurationHours        int64      `json:"DurationHours,omitempty"`
	DurationMinutes      int64      `json:"DurationMinutes,omitempty"`
}

// TrainingPlan represents a SageMaker training plan.
type TrainingPlan struct {
	CreationTime              time.Time                  `json:"CreationTime"`
	StartTime                 *time.Time                 `json:"StartTime,omitempty"`
	EndTime                   *time.Time                 `json:"EndTime,omitempty"`
	Tags                      map[string]string          `json:"Tags,omitempty"`
	UpfrontFee                string                     `json:"-"`
	CurrencyCode              string                     `json:"CurrencyCode,omitempty"`
	StatusMessage             string                     `json:"StatusMessage,omitempty"`
	TrainingPlanName          string                     `json:"TrainingPlanName"`
	TrainingPlanArn           string                     `json:"TrainingPlanArn"`
	Status                    string                     `json:"Status"`
	Extensions                []*TrainingPlanExtension   `json:"-"`
	ReservedCapacitySummaries []*ReservedCapacitySummary `json:"ReservedCapacitySummaries,omitempty"`
	TargetResources           []string                   `json:"-"`
	DurationHours             int64                      `json:"DurationHours,omitempty"`
	DurationMinutes           int64                      `json:"DurationMinutes,omitempty"`
	TotalInstanceCount        int32                      `json:"-"`
	AvailableInstanceCount    int32                      `json:"AvailableInstanceCount,omitempty"`
	InUseInstanceCount        int32                      `json:"InUseInstanceCount,omitempty"`
}

// TrainingPlanExtension records one purchased extension of a training plan's
// duration, mirroring types.TrainingPlanExtension.
type TrainingPlanExtension struct {
	ExtendedAt                      time.Time `json:"ExtendedAt"`
	StartDate                       time.Time `json:"StartDate"`
	EndDate                         time.Time `json:"EndDate"`
	TrainingPlanExtensionOfferingID string    `json:"TrainingPlanExtensionOfferingId"`
	CurrencyCode                    string    `json:"CurrencyCode,omitempty"`
	PaymentStatus                   string    `json:"PaymentStatus,omitempty"`
	DurationHours                   int32     `json:"DurationHours"`
}

func cloneTrainingPlan(t *TrainingPlan) *TrainingPlan {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)
	cp.TargetResources = append([]string(nil), t.TargetResources...)

	cp.ReservedCapacitySummaries = make([]*ReservedCapacitySummary, len(t.ReservedCapacitySummaries))
	for i, rc := range t.ReservedCapacitySummaries {
		rcCopy := *rc
		cp.ReservedCapacitySummaries[i] = &rcCopy
	}

	cp.Extensions = make([]*TrainingPlanExtension, len(t.Extensions))
	for i, ext := range t.Extensions {
		extCopy := *ext
		cp.Extensions[i] = &extCopy
	}

	return &cp
}

// CreateTrainingPlan creates a training plan. If offeringID matches a catalog
// entry from SearchTrainingPlanOfferings, the plan inherits its duration,
// pricing and target resources and a backing ReservedCapacity is provisioned;
// otherwise a minimal Active plan is created (offeringID == "").
func (b *InMemoryBackend) CreateTrainingPlan(
	ctx context.Context,
	name, offeringID string,
	spareInstanceCountPerUltraServer int32,
	tags map[string]string,
) (*TrainingPlan, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateTrainingPlan")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", ErrValidation)
	}

	store := b.trainingPlansStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: training plan %q already exists", ErrValidation, name)
	}

	planARN := arn.Build("sagemaker", region, b.accountID, "training-plan/"+name)
	now := time.Now()

	t := &TrainingPlan{
		TrainingPlanName: name,
		TrainingPlanArn:  planARN,
		Status:           statusActive,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
	}

	if offering, ok := findTrainingPlanOffering(offeringID); ok {
		b.applyOfferingToPlan(region, t, offering, now, spareInstanceCountPerUltraServer)
	}

	store.Put(t)

	return cloneTrainingPlan(t), nil
}

// applyOfferingToPlan populates a newly created plan's duration, pricing,
// target resources and backing reserved capacities from a catalog offering.
// Called with b.mu held.
func (b *InMemoryBackend) applyOfferingToPlan(
	region string,
	t *TrainingPlan,
	offering *TrainingPlanOffering,
	now time.Time,
	spareInstanceCountPerUltraServer int32,
) {
	t.Status = trainingPlanStatusScheduled
	t.CurrencyCode = offering.CurrencyCode
	t.UpfrontFee = offering.UpfrontFee
	t.DurationHours = offering.DurationHours
	t.DurationMinutes = offering.DurationMinutes
	t.TargetResources = append([]string(nil), offering.TargetResources...)
	startTime := now
	endTime := now.Add(durationFromHoursMinutes(offering.DurationHours, offering.DurationMinutes))
	t.StartTime = &startTime
	t.EndTime = &endTime

	for _, rco := range offering.ReservedCapacityOfferings {
		rc := b.createReservedCapacity(region, t.TrainingPlanArn, rco, now, spareInstanceCountPerUltraServer)
		t.TotalInstanceCount += rc.TotalInstanceCount
		t.AvailableInstanceCount += rc.AvailableInstanceCount
		t.ReservedCapacitySummaries = append(t.ReservedCapacitySummaries, rc.toSummary())
	}
}

// DescribeTrainingPlan returns a training plan by name.
func (b *InMemoryBackend) DescribeTrainingPlan(ctx context.Context, name string) (*TrainingPlan, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeTrainingPlan")
	defer b.mu.RUnlock()

	t, ok := b.trainingPlansStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, name)
	}

	return cloneTrainingPlan(t), nil
}
