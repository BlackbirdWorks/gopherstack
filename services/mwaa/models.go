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
