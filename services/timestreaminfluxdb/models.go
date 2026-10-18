package timestreaminfluxdb

import (
	"encoding/json"
	"time"
)

const (
	statusCreating  = "CREATING"
	statusAvailable = "AVAILABLE"
	statusDeleting  = "DELETING"
	statusUpdating  = "UPDATING"
	statusRebooting = "REBOOTING"
	statusRestoring = "RESTORING"
	statusFailed    = "FAILED"
	statusDeleted   = "DELETED"

	statusUpdatingInstanceType   = "UPDATING_INSTANCE_TYPE"
	statusUpdatingDeploymentType = "UPDATING_DEPLOYMENT_TYPE"

	backupInProgress = "IN_PROGRESS"
	backupCompleted  = "COMPLETED"
	backupOnDemand   = "ON_DEMAND"

	engineV2    = "INFLUXDB_V2"
	modePrimary = "PRIMARY"
	modeReplica = "REPLICA"

	deploySingleAZ     = "SINGLE_AZ"
	deployMultiAZ      = "WITH_MULTIAZ_STANDBY"
	deployReadReplicas = "MULTI_NODE_READ_REPLICAS"

	resInstance = "DB_INSTANCE"
	resCluster  = "DB_CLUSTER"
)

// Pending is a transitional status that lasts until Until, or until the engine reports when Engine is set.
type Pending struct {
	Until  time.Time `json:"until"`
	Kind   string    `json:"kind"`
	Engine bool      `json:"engine,omitempty"`
}

// LogDelivery is the S3 log delivery configuration.
type LogDelivery struct {
	Bucket  string `json:"bucket"`
	Enabled bool   `json:"enabled"`
}

// Maintenance is the maintenance window.
type Maintenance struct {
	Window   string `json:"window"`
	Timezone string `json:"timezone"`
}

// BackupConfig is an automated backup schedule.
type BackupConfig struct {
	CustomSchedule string `json:"customSchedule,omitempty"`
	Type           string `json:"type"`
	RetentionDays  int32  `json:"retentionDays"`
	Enabled        bool   `json:"enabled"`
}

// ClusterConfig describes a cluster's node roles.
type ClusterConfig struct {
	DedicatedCompactor   *bool  `json:"dedicatedCompactor,omitempty"`
	IngestQueryInstances *int32 `json:"ingestQueryInstances,omitempty"`
	QueryOnlyInstances   *int32 `json:"queryOnlyInstances,omitempty"`
}

// Placement holds the settings shared by instances, clusters and the backups of both.
type Placement struct {
	LogDelivery        *LogDelivery   `json:"logDelivery,omitempty"`
	Maintenance        *Maintenance   `json:"maintenance,omitempty"`
	DBInstanceType     string         `json:"dbInstanceType"`
	DBStorageType      string         `json:"dbStorageType"`
	DeploymentType     string         `json:"deploymentType"`
	NetworkType        string         `json:"networkType"`
	KMSKeyID           string         `json:"kmsKeyId,omitempty"`
	ParameterGroupID   string         `json:"parameterGroupId,omitempty"`
	SubnetIDs          []string       `json:"subnetIds"`
	SecurityGroupIDs   []string       `json:"securityGroupIds"`
	BackupConfigs      []BackupConfig `json:"backupConfigs,omitempty"`
	AllocatedStorage   int32          `json:"allocatedStorage"`
	Port               int32          `json:"port"`
	PubliclyAccessible bool           `json:"publiclyAccessible"`
}

// DBInstance is a Timestream for InfluxDB instance.
type DBInstance struct {
	CreatedAt    time.Time         `json:"createdAt"`
	Tags         map[string]string `json:"tags,omitempty"`
	Mode         string            `json:"mode,omitempty"`
	Username     string            `json:"username"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	ARN          string            `json:"arn"`
	AccountID    string            `json:"accountId"`
	Region       string            `json:"region"`
	ClusterID    string            `json:"clusterId,omitempty"`
	Password     string            `json:"-"`
	Failed       string            `json:"failed,omitempty"`
	Organization string            `json:"organization"`
	Bucket       string            `json:"bucket"`
	AZ           string            `json:"az"`
	StandbyAZ    string            `json:"standbyAz,omitempty"`
	EngineType   string            `json:"engineType"`
	EngineHost   string            `json:"engineHost,omitempty"`
	Pending      Pending           `json:"pending"`
	Placement    Placement         `json:"placement"`
	EnginePort   int32             `json:"enginePort,omitempty"`
}

// DBCluster is a Timestream for InfluxDB cluster.
type DBCluster struct {
	CreatedAt    time.Time         `json:"createdAt"`
	Tags         map[string]string `json:"tags,omitempty"`
	Config       *ClusterConfig    `json:"config,omitempty"`
	Password     string            `json:"-"`
	Pending      Pending           `json:"pending"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	ARN          string            `json:"arn"`
	AccountID    string            `json:"accountId"`
	Region       string            `json:"region"`
	Username     string            `json:"username"`
	Organization string            `json:"organization"`
	Bucket       string            `json:"bucket"`
	EngineType   string            `json:"engineType"`
	FailoverMode string            `json:"failoverMode"`
	Placement    Placement         `json:"placement"`
}

// DBParameterGroup is a named set of InfluxDB parameters.
type DBParameterGroup struct {
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	ARN         string            `json:"arn"`
	Description string            `json:"description,omitempty"`
	AccountID   string            `json:"accountId"`
	Region      string            `json:"region"`
	Parameters  json.RawMessage   `json:"parameters,omitempty"`
}

// DBBackup is a backup of an instance or cluster.
type DBBackup struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	ClusterConfig *ClusterConfig    `json:"clusterConfig,omitempty"`
	Pending       Pending           `json:"pending"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	ARN           string            `json:"arn"`
	AccountID     string            `json:"accountId"`
	Region        string            `json:"region"`
	ResourceID    string            `json:"resourceId"`
	ResourceType  string            `json:"resourceType"`
	EngineType    string            `json:"engineType"`
	FailoverMode  string            `json:"failoverMode,omitempty"`
	Type          string            `json:"type"`
	Username      string            `json:"username"`
	Organization  string            `json:"organization"`
	Bucket        string            `json:"bucket"`
	Placement     Placement         `json:"placement"`
	RetentionDays int32             `json:"retentionDays"`
}
