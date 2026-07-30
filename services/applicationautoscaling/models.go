package applicationautoscaling

import (
	"time"
)

// SuspendedState represents the suspension configuration for a scalable target.
// Each field independently suspends a category of scaling activity.
type SuspendedState struct {
	DynamicScalingInSuspended  bool `json:"dynamicScalingInSuspended"`
	DynamicScalingOutSuspended bool `json:"dynamicScalingOutSuspended"`
	ScheduledScalingSuspended  bool `json:"scheduledScalingSuspended"`
}

// ScalableTargetAction holds the capacity bounds for a scheduled action.
type ScalableTargetAction struct {
	MinCapacity *int32 `json:"minCapacity,omitempty"`
	MaxCapacity *int32 `json:"maxCapacity,omitempty"`
}

// ScalableTarget represents a registered Application Auto Scaling scalable target.
type ScalableTarget struct {
	CreationTime      time.Time         `json:"creationTime"`
	LastModifiedTime  time.Time         `json:"lastModifiedTime"`
	SuspendedState    *SuspendedState   `json:"suspendedState,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
	PredictedCapacity *int32            `json:"predictedCapacity,omitempty"`
	RoleARN           string            `json:"roleArn,omitempty"`
	ARN               string            `json:"arn"`
	ScalableDimension string            `json:"scalableDimension"`
	ServiceNamespace  string            `json:"serviceNamespace"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	ResourceID        string            `json:"resourceId"`
	MinCapacity       int32             `json:"minCapacity"`
	MaxCapacity       int32             `json:"maxCapacity"`
}

// Alarm mirrors the CloudWatch alarm reference AWS attaches to
// TargetTrackingScaling/StepScaling policies (PutScalingPolicy and
// DescribeScalingPolicies both return this on the wire). Real AWS creates
// backing CloudWatch alarms server-side; gopherstack has no cross-service
// reference to the cloudwatch backend to create a real one, so this is
// never populated (see PARITY.md gaps -- honestly empty, not fabricated).
type Alarm struct {
	AlarmARN  string `json:"alarmArn"`
	AlarmName string `json:"alarmName"`
}

// ScalingPolicy represents an Application Auto Scaling scaling policy.
type ScalingPolicy struct {
	CreationTime            time.Time      `json:"creationTime"`
	LastModifiedTime        time.Time      `json:"lastModifiedTime"`
	TargetTrackingConfig    map[string]any `json:"targetTrackingConfig,omitempty"`
	StepScalingConfig       map[string]any `json:"stepScalingConfig,omitempty"`
	PredictiveScalingConfig map[string]any `json:"predictiveScalingConfig,omitempty"`
	PolicyType              string         `json:"policyType"`
	PolicyName              string         `json:"policyName"`
	ResourceID              string         `json:"resourceId"`
	ARN                     string         `json:"arn"`
	ScalableDimension       string         `json:"scalableDimension"`
	ServiceNamespace        string         `json:"serviceNamespace"`
	Alarms                  []Alarm        `json:"alarms,omitempty"`
}

// ScheduledAction represents an Application Auto Scaling scheduled action.
type ScheduledAction struct {
	StartTime            *time.Time            `json:"startTime,omitempty"`
	EndTime              *time.Time            `json:"endTime,omitempty"`
	CreationTime         time.Time             `json:"creationTime"`
	LastModifiedTime     time.Time             `json:"lastModifiedTime"`
	ScalableTargetAction *ScalableTargetAction `json:"scalableTargetAction,omitempty"`
	ScheduledActionName  string                `json:"scheduledActionName"`
	ResourceID           string                `json:"resourceId"`
	ARN                  string                `json:"arn"`
	Schedule             string                `json:"schedule"`
	ScalableDimension    string                `json:"scalableDimension"`
	ServiceNamespace     string                `json:"serviceNamespace"`
	Timezone             string                `json:"timezone,omitempty"`
}

// NotScaledReason mirrors the real AWS NotScaledReason type: it explains why
// a scaling activity did not change capacity, and is only ever returned when
// a client sets IncludeNotScaledActivities=true. gopherstack has no metric
// evaluation loop capable of deciding "not scaled" outcomes (see
// PARITY.md), so NotScaledReasons on [ScalingActivity] is always empty
// rather than fabricated -- documented, not a fabricated stub.
type NotScaledReason struct {
	CurrentCapacity *int32 `json:"currentCapacity,omitempty"`
	MaxCapacity     *int32 `json:"maxCapacity,omitempty"`
	MinCapacity     *int32 `json:"minCapacity,omitempty"`
	Code            string `json:"code"`
}

// ScalingActivity records a capacity-changing activity on a scalable target,
// returned by DescribeScalingActivities.
type ScalingActivity struct {
	StartTime         time.Time `json:"StartTime"`
	EndTime           time.Time `json:"EndTime"`
	ActivityID        string    `json:"ActivityId"`
	ServiceNamespace  string    `json:"ServiceNamespace"`
	ResourceID        string    `json:"ResourceId"`
	ScalableDimension string    `json:"ScalableDimension"`
	Description       string    `json:"Description"`
	Cause             string    `json:"Cause"`
	StatusCode        string    `json:"StatusCode"`
	StatusMessage     string    `json:"StatusMessage"`
	// Details holds supplementary JSON detail AWS attaches to some activities
	// (e.g. which step adjustment fired). gopherstack has no such detail to
	// report, so this is always empty.
	Details string `json:"details,omitempty"`
	// NotScaledReasons is always empty; see [NotScaledReason].
	NotScaledReasons []NotScaledReason `json:"notScaledReasons,omitempty"`
}

// CapacityForecastData holds the timestamps and capacity values for a forecast.
type CapacityForecastData struct {
	Timestamps []time.Time
	Values     []float64
}

// LoadForecastData holds the timestamps, values, and a metric specification label for a load forecast.
type LoadForecastData struct {
	MetricSpecification string
	Timestamps          []time.Time
	Values              []float64
}
