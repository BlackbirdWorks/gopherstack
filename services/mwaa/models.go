package mwaa

import "time"

// epochSecondsNow returns the current time as Unix epoch seconds (float64).
// The AWS SDK v2 deserializes MWAA timestamps as JSON numbers (__timestampUnix).
func epochSecondsNow() float64 {
	return float64(time.Now().Unix())
}

// Environment represents an MWAA environment.
type Environment struct {
	Tags                 map[string]string `json:"Tags,omitempty"`
	NetworkConfiguration *NetworkConfig    `json:"NetworkConfiguration,omitempty"`
	Name                 string            `json:"Name"`
	AirflowVersion       string            `json:"AirflowVersion"`
	ExecutionRoleArn     string            `json:"ExecutionRoleArn"`
	SourceBucketArn      string            `json:"SourceBucketArn"`
	EnvironmentClass     string            `json:"EnvironmentClass"`
	WebserverURL         string            `json:"WebserverUrl"`
	WebserverAccessMode  string            `json:"WebserverAccessMode"`
	DagS3Path            string            `json:"DagS3Path"`
	Status               string            `json:"Status"`
	ARN                  string            `json:"Arn"`
	CreatedAt            float64           `json:"CreatedAt"`
	MaxWorkers           int32             `json:"MaxWorkers"`
	MinWorkers           int32             `json:"MinWorkers"`
}

// NetworkConfig holds the VPC networking configuration.
type NetworkConfig struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
	SubnetIDs        []string `json:"SubnetIds"`
}

// createEnvironmentRequest is the request body for creating an MWAA environment.
type createEnvironmentRequest struct {
	NetworkConfiguration *NetworkConfig    `json:"NetworkConfiguration"`
	Tags                 map[string]string `json:"Tags"`
	DagS3Path            string            `json:"DagS3Path"`
	ExecutionRoleArn     string            `json:"ExecutionRoleArn"`
	SourceBucketArn      string            `json:"SourceBucketArn"`
	AirflowVersion       string            `json:"AirflowVersion"`
	EnvironmentClass     string            `json:"EnvironmentClass"`
	WebserverAccessMode  string            `json:"WebserverAccessMode"`
	MaxWorkers           int32             `json:"MaxWorkers"`
	MinWorkers           int32             `json:"MinWorkers"`
}

// updateEnvironmentRequest is the request body for updating an MWAA environment.
type updateEnvironmentRequest struct {
	NetworkConfiguration *NetworkConfig `json:"NetworkConfiguration"`
	DagS3Path            string         `json:"DagS3Path"`
	ExecutionRoleArn     string         `json:"ExecutionRoleArn"`
	SourceBucketArn      string         `json:"SourceBucketArn"`
	AirflowVersion       string         `json:"AirflowVersion"`
	EnvironmentClass     string         `json:"EnvironmentClass"`
	WebserverAccessMode  string         `json:"WebserverAccessMode"`
	MaxWorkers           int32          `json:"MaxWorkers"`
	MinWorkers           int32          `json:"MinWorkers"`
}

// invokeRestAPIRequest is the request body for InvokeRestAPI.
type invokeRestAPIRequest struct {
	Body            any    `json:"Body"`
	QueryParameters any    `json:"QueryParameters"`
	Method          string `json:"Method"`
	Path            string `json:"Path"`
}

// InvokeRestAPIResponse is the response body for InvokeRestAPI.
type InvokeRestAPIResponse struct {
	RestAPIResponse   any   `json:"RestApiResponse"`
	RestAPIStatusCode int32 `json:"RestApiStatusCode"`
}

// Dimension represents an internal MWAA metric dimension.
type Dimension struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// StatisticSet represents the statistical values for a metric.
type StatisticSet struct {
	Maximum     *float64 `json:"Maximum,omitempty"`
	Minimum     *float64 `json:"Minimum,omitempty"`
	SampleCount *int32   `json:"SampleCount,omitempty"`
	Sum         *float64 `json:"Sum,omitempty"`
}

// MetricDatum represents a single metric data point for PublishMetrics.
type MetricDatum struct {
	StatisticValues *StatisticSet `json:"StatisticValues,omitempty"`
	Timestamp       *float64      `json:"Timestamp,omitempty"`
	Value           *float64      `json:"Value,omitempty"`
	MetricName      string        `json:"MetricName"`
	Unit            string        `json:"Unit,omitempty"`
	Dimensions      []Dimension   `json:"Dimensions,omitempty"`
}

// publishMetricsRequest is the request body for PublishMetrics.
type publishMetricsRequest struct {
	MetricData []MetricDatum `json:"MetricData"`
}
