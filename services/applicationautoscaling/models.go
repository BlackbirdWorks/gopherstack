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
	ResourceID        string            `json:"resourceId"`
	ARN               string            `json:"arn"`
	RoleARN           string            `json:"roleArn,omitempty"`
	ScalableDimension string            `json:"scalableDimension"`
	ServiceNamespace  string            `json:"serviceNamespace"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	MinCapacity       int32             `json:"minCapacity"`
	MaxCapacity       int32             `json:"maxCapacity"`
}

// ScalingPolicy represents an Application Auto Scaling scaling policy.
type ScalingPolicy struct {
	CreationTime         time.Time      `json:"creationTime"`
	LastModifiedTime     time.Time      `json:"lastModifiedTime"`
	TargetTrackingConfig map[string]any `json:"targetTrackingConfig,omitempty"`
	StepScalingConfig    map[string]any `json:"stepScalingConfig,omitempty"`
	PolicyType           string         `json:"policyType"`
	PolicyName           string         `json:"policyName"`
	ResourceID           string         `json:"resourceId"`
	ARN                  string         `json:"arn"`
	ScalableDimension    string         `json:"scalableDimension"`
	ServiceNamespace     string         `json:"serviceNamespace"`
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
