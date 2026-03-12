package mwaa

import "time"

// Environment represents an MWAA environment.
type Environment struct {
	CreatedAt            time.Time         `json:"CreatedAt"`
	NetworkConfiguration *NetworkConfig    `json:"NetworkConfiguration,omitempty"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	AirflowVersion       string            `json:"AirflowVersion"`
	ExecutionRoleArn     string            `json:"ExecutionRoleArn"`
	SourceBucketArn      string            `json:"SourceBucketArn"`
	Name                 string            `json:"Name"`
	EnvironmentClass     string            `json:"EnvironmentClass"`
	WebserverURL         string            `json:"WebserverUrl"`
	WebserverAccessMode  string            `json:"WebserverAccessMode"`
	DagS3Path            string            `json:"DagS3Path"`
	Status               string            `json:"Status"`
	ARN                  string            `json:"Arn"`
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
