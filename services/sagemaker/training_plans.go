package sagemaker

import (
	"context"
	"encoding/json"
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

// MarshalJSON emits StartTime/EndTime as AWS awsjson1.1 epoch-seconds
// numbers rather than Go's default RFC3339 strings — this struct is nested
// (unconverted) inside TrainingPlan.ReservedCapacitySummaries, which is
// marshaled directly by handleDescribeTrainingPlan.
func (r *ReservedCapacitySummary) MarshalJSON() ([]byte, error) {
	type alias ReservedCapacitySummary

	return json.Marshal(struct {
		*alias
		StartTime *float64 `json:"StartTime,omitempty"`
		EndTime   *float64 `json:"EndTime,omitempty"`
	}{
		alias:     (*alias)(r),
		StartTime: epochSecondsPtr(r.StartTime),
		EndTime:   epochSecondsPtr(r.EndTime),
	})
}

// UnmarshalJSON is the inverse of [ReservedCapacitySummary.MarshalJSON],
// read by persistence.go's snapshot restore path.
func (r *ReservedCapacitySummary) UnmarshalJSON(data []byte) error {
	type alias ReservedCapacitySummary

	aux := struct {
		*alias
		StartTime *float64 `json:"StartTime,omitempty"`
		EndTime   *float64 `json:"EndTime,omitempty"`
	}{alias: (*alias)(r)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	r.StartTime = timeFromEpochSecondsPtr(aux.StartTime)
	r.EndTime = timeFromEpochSecondsPtr(aux.EndTime)

	return nil
}

// TrainingPlan represents a SageMaker training plan.
//
// UpfrontFee/TargetResources/TotalInstanceCount are all real, optional
// DescribeTrainingPlanOutput members (api_op_DescribeTrainingPlan.go) --
// previously tagged json:"-", so handleDescribeTrainingPlan's direct
// json.Marshal(result) silently dropped all three even though
// trainingPlanSummaryJSON (handler_training_plan.go, ListTrainingPlans) had
// already been projecting them into the List response the whole time.
type TrainingPlan struct {
	CreationTime              time.Time                  `json:"CreationTime"`
	StartTime                 *time.Time                 `json:"StartTime,omitempty"`
	EndTime                   *time.Time                 `json:"EndTime,omitempty"`
	Tags                      map[string]string          `json:"Tags,omitempty"`
	UpfrontFee                string                     `json:"UpfrontFee,omitempty"`
	CurrencyCode              string                     `json:"CurrencyCode,omitempty"`
	StatusMessage             string                     `json:"StatusMessage,omitempty"`
	TrainingPlanName          string                     `json:"TrainingPlanName"`
	TrainingPlanArn           string                     `json:"TrainingPlanArn"`
	Status                    string                     `json:"Status"`
	Extensions                []*TrainingPlanExtension   `json:"-"`
	ReservedCapacitySummaries []*ReservedCapacitySummary `json:"ReservedCapacitySummaries,omitempty"`
	TargetResources           []string                   `json:"TargetResources,omitempty"`
	DurationHours             int64                      `json:"DurationHours,omitempty"`
	DurationMinutes           int64                      `json:"DurationMinutes,omitempty"`
	TotalInstanceCount        int32                      `json:"TotalInstanceCount,omitempty"`
	AvailableInstanceCount    int32                      `json:"AvailableInstanceCount,omitempty"`
	InUseInstanceCount        int32                      `json:"InUseInstanceCount,omitempty"`
}

// TrainingPlanExtension records one purchased extension of a training plan's
// duration, mirroring types.TrainingPlanExtension.
type TrainingPlanExtension struct {
	ExtendedAt                      time.Time `json:"-"`
	StartDate                       time.Time `json:"-"`
	EndDate                         time.Time `json:"-"`
	TrainingPlanExtensionOfferingID string    `json:"TrainingPlanExtensionOfferingId"`
	CurrencyCode                    string    `json:"CurrencyCode,omitempty"`
	PaymentStatus                   string    `json:"PaymentStatus,omitempty"`
	DurationHours                   int32     `json:"DurationHours"`
}

// MarshalJSON emits ExtendedAt/StartDate/EndDate as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings --
// ExtendTrainingPlan and SearchTrainingPlanOfferings both marshal
// []*TrainingPlanExtension directly (handler_training_plan.go), and this
// type is also embedded in TrainingPlan.Extensions, which rides along on
// TrainingPlan's own persistence round trip.
func (e TrainingPlanExtension) MarshalJSON() ([]byte, error) {
	type alias TrainingPlanExtension

	return json.Marshal(struct {
		alias
		ExtendedAt float64 `json:"ExtendedAt"`
		StartDate  float64 `json:"StartDate"`
		EndDate    float64 `json:"EndDate"`
	}{
		alias:      alias(e),
		ExtendedAt: epochSeconds(e.ExtendedAt),
		StartDate:  epochSeconds(e.StartDate),
		EndDate:    epochSeconds(e.EndDate),
	})
}

// UnmarshalJSON is the inverse of [TrainingPlanExtension.MarshalJSON], read
// by TrainingPlan's persistence round trip (see persistence.go).
func (e *TrainingPlanExtension) UnmarshalJSON(data []byte) error {
	type alias TrainingPlanExtension

	aux := struct {
		*alias
		ExtendedAt float64 `json:"ExtendedAt"`
		StartDate  float64 `json:"StartDate"`
		EndDate    float64 `json:"EndDate"`
	}{alias: (*alias)(e)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	e.ExtendedAt = timeFromEpochSeconds(aux.ExtendedAt)
	e.StartDate = timeFromEpochSeconds(aux.StartDate)
	e.EndDate = timeFromEpochSeconds(aux.EndDate)

	return nil
}

// MarshalJSON emits CreationTime/StartTime/EndTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeTrainingPlan.
func (t *TrainingPlan) MarshalJSON() ([]byte, error) {
	type alias TrainingPlan

	return json.Marshal(struct {
		*alias
		StartTime    *float64 `json:"StartTime,omitempty"`
		EndTime      *float64 `json:"EndTime,omitempty"`
		CreationTime float64  `json:"CreationTime"`
	}{
		alias:        (*alias)(t),
		CreationTime: epochSeconds(t.CreationTime),
		StartTime:    epochSecondsPtr(t.StartTime),
		EndTime:      epochSecondsPtr(t.EndTime),
	})
}

// UnmarshalJSON is the inverse of [TrainingPlan.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (t *TrainingPlan) UnmarshalJSON(data []byte) error {
	type alias TrainingPlan

	aux := struct {
		*alias
		StartTime    *float64 `json:"StartTime,omitempty"`
		EndTime      *float64 `json:"EndTime,omitempty"`
		CreationTime float64  `json:"CreationTime"`
	}{alias: (*alias)(t)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	t.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	t.StartTime = timeFromEpochSecondsPtr(aux.StartTime)
	t.EndTime = timeFromEpochSecondsPtr(aux.EndTime)

	return nil
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
