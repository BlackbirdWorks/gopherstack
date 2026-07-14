package dms

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// DataMigration represents an AWS DMS data migration.
type DataMigration struct {
	CreationTime         time.Time  `json:"creationTime"`
	Tags                 *tags.Tags `json:"-"`
	DataMigrationName    string     `json:"dataMigrationName"`
	DataMigrationArn     string     `json:"dataMigrationArn"`
	MigrationProjectArn  string     `json:"migrationProjectArn"`
	DataMigrationType    string     `json:"dataMigrationType"`
	ServiceAccessRoleArn string     `json:"serviceAccessRoleArn"`
	DataMigrationStatus  string     `json:"dataMigrationStatus"`
	AccountID            string     `json:"accountId"`
	Region               string     `json:"region"`
	SelectionRules       string     `json:"selectionRules,omitempty"`
	NumberOfJobs         int32      `json:"numberOfJobs"`
	EnableCloudwatchLogs bool       `json:"enableCloudwatchLogs"`
}

// DataProvider represents an AWS DMS data provider.
type DataProvider struct {
	CreationTime     time.Time  `json:"creationTime"`
	Tags             *tags.Tags `json:"-"`
	DataProviderName string     `json:"dataProviderName"`
	DataProviderArn  string     `json:"dataProviderArn"`
	Engine           string     `json:"engine"`
	Description      string     `json:"description,omitempty"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"region"`
}

// EventSubscription represents an AWS DMS event notification subscription.
type EventSubscription struct {
	CreationTime     time.Time  `json:"creationTime"`
	Tags             *tags.Tags `json:"-"`
	SubscriptionName string     `json:"subscriptionName"`
	SnsTopicArn      string     `json:"snsTopicArn"`
	SourceType       string     `json:"sourceType,omitempty"`
	Status           string     `json:"status"`
	AccountID        string     `json:"accountId"`
	Region           string     `json:"region"`
	SourceIDsList    []string   `json:"sourceIdsList,omitempty"`
	EventCategories  []string   `json:"eventCategories,omitempty"`
	Enabled          bool       `json:"enabled"`
}

// FleetAdvisorCollector represents an AWS DMS Fleet Advisor collector.
type FleetAdvisorCollector struct {
	CreatedDate           time.Time  `json:"createdDate"`
	Tags                  *tags.Tags `json:"-"`
	CollectorName         string     `json:"collectorName"`
	CollectorReferencedID string     `json:"collectorReferencedId"`
	CollectorVersion      string     `json:"collectorVersion"`
	Description           string     `json:"description,omitempty"`
	ServiceAccessRoleArn  string     `json:"serviceAccessRoleArn"`
	S3BucketName          string     `json:"s3BucketName"`
	CollectorHealthCheck  string     `json:"collectorHealthCheck"`
	LastDataReceived      string     `json:"lastDataReceived,omitempty"`
	RegisteredDate        string     `json:"registeredDate,omitempty"`
	ModifiedDate          string     `json:"modifiedDate,omitempty"`
	AccountID             string     `json:"accountId"`
	Region                string     `json:"region"`
}

// InstanceProfile represents an AWS DMS instance profile.
type InstanceProfile struct {
	CreationTime          time.Time  `json:"creationTime"`
	Tags                  *tags.Tags `json:"-"`
	InstanceProfileName   string     `json:"instanceProfileName"`
	InstanceProfileArn    string     `json:"instanceProfileArn"`
	AvailabilityZone      string     `json:"availabilityZone,omitempty"`
	KmsKeyArn             string     `json:"kmsKeyArn,omitempty"`
	NetworkType           string     `json:"networkType,omitempty"`
	Description           string     `json:"description,omitempty"`
	SubnetGroupIdentifier string     `json:"subnetGroupIdentifier,omitempty"`
	AccountID             string     `json:"accountId"`
	Region                string     `json:"region"`
	PubliclyAccessible    bool       `json:"publiclyAccessible"`
}

// ReplicationInstance represents an AWS DMS replication instance.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationInstance.
type ReplicationInstance struct {
	CreationTime                  time.Time  `json:"creationTime"`
	Tags                          *tags.Tags `json:"-"`
	ReplicationInstanceIdentifier string     `json:"replicationInstanceIdentifier"`
	ReplicationInstanceArn        string     `json:"replicationInstanceArn"`
	ReplicationInstanceClass      string     `json:"replicationInstanceClass"`
	EngineVersion                 string     `json:"engineVersion"`
	AvailabilityZone              string     `json:"availabilityZone"`
	ReplicationInstanceStatus     string     `json:"replicationInstanceStatus"`
	PrivateIPAddress              string     `json:"privateIpAddress"`
	AccountID                     string     `json:"accountId"`
	Region                        string     `json:"region"`
	AllocatedStorage              int32      `json:"allocatedStorage"`
	MultiAZ                       bool       `json:"multiAZ"`
	AutoMinorVersionUpgrade       bool       `json:"autoMinorVersionUpgrade"`
	PubliclyAccessible            bool       `json:"publiclyAccessible"`
}

// Endpoint represents an AWS DMS endpoint.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateEndpoint.
type Endpoint struct {
	CreationTime       time.Time  `json:"creationTime"`
	Tags               *tags.Tags `json:"-"`
	EndpointIdentifier string     `json:"endpointIdentifier"`
	EndpointArn        string     `json:"endpointArn"`
	EndpointType       string     `json:"endpointType"`
	EngineName         string     `json:"engineName"`
	ServerName         string     `json:"serverName,omitempty"`
	DatabaseName       string     `json:"databaseName,omitempty"`
	Username           string     `json:"username,omitempty"`
	Status             string     `json:"status"`
	AccountID          string     `json:"accountId"`
	Region             string     `json:"region"`
	Port               int32      `json:"port,omitempty"`
}

// ReplicationTask represents an AWS DMS replication task.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationTask.
type ReplicationTask struct {
	CreationTime              time.Time  `json:"creationTime"`
	Tags                      *tags.Tags `json:"-"`
	ReplicationTaskIdentifier string     `json:"replicationTaskIdentifier"`
	ReplicationTaskArn        string     `json:"replicationTaskArn"`
	SourceEndpointArn         string     `json:"sourceEndpointArn"`
	TargetEndpointArn         string     `json:"targetEndpointArn"`
	ReplicationInstanceArn    string     `json:"replicationInstanceArn"`
	MigrationType             string     `json:"migrationType"`
	TableMappings             string     `json:"tableMappings,omitempty"`
	ReplicationTaskSettings   string     `json:"replicationTaskSettings,omitempty"`
	Status                    string     `json:"status"`
	AccountID                 string     `json:"accountId"`
	Region                    string     `json:"region"`
}

// Certificate represents a DMS certificate.
type Certificate struct {
	CertificateIdentifier string
	CertificateArn        string
	CertificatePem        string
	AccountID             string
	Region                string
}

// ReplicationSubnetGroup represents a DMS replication subnet group.
type ReplicationSubnetGroup struct {
	Tags                              *tags.Tags `json:"-"`
	ReplicationSubnetGroupIdentifier  string
	ReplicationSubnetGroupArn         string
	ReplicationSubnetGroupDescription string
	VpcID                             string
	AccountID                         string
	Region                            string
}

// MigrationProject represents a DMS migration project.
type MigrationProject struct {
	Tags                       *tags.Tags `json:"-"`
	MigrationProjectName       string
	MigrationProjectArn        string
	MigrationProjectIdentifier string
	Description                string
	AccountID                  string
	Region                     string
}

// ReplicationConfig represents a DMS replication config.
// ReplicationConfig also tracks the runtime state of its associated DMS
// Serverless "Replication" resource (Status, StartReplicationType). AWS
// models the replication config (returned by CreateReplicationConfig /
// DescribeReplicationConfigs / ModifyReplicationConfig) and the replication
// runtime state (returned by StartReplication / StopReplication /
// DescribeReplications) as two distinct API shapes, but a single config has
// at most one associated replication in this in-memory emulation, so the
// runtime fields live on the same struct rather than a separate table.
type ReplicationConfig struct {
	Tags                        *tags.Tags `json:"-"`
	ReplicationConfigIdentifier string
	ReplicationConfigArn        string
	ReplicationType             string
	SourceEndpointArn           string
	TargetEndpointArn           string
	AccountID                   string
	Region                      string
	// Status is the runtime status of the associated Replication resource
	// (created, running, stopped, ...). It is never surfaced on the
	// ReplicationConfig wire shape itself -- only via StartReplication /
	// StopReplication / DescribeReplications, which report on the
	// "Replication" resource, not the "ReplicationConfig" resource.
	Status string
	// StartReplicationType records the StartReplicationType passed to the
	// most recent StartReplication call (start-replication, resume-processing,
	// or reload-target), echoed back on the Replication resource.
	StartReplicationType string
}

// AssessmentRun represents a DMS pre-migration assessment run.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// AssessmentRun carries no other region-derived field, so the value needs
// its own copy to serve as a pure store.Table/store.Index key input.
type AssessmentRun struct {
	ReplicationTaskAssessmentRunArn string
	ReplicationTaskArn              string
	AssessmentRunName               string
	Status                          string
	Region                          string
}

// Connection represents a DMS connection between a replication instance and an endpoint.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// Connection carries no other region-derived field, so the value needs its
// own copy to serve as a pure store.Table/store.Index key input.
type Connection struct {
	ReplicationInstanceArn        string
	ReplicationInstanceIdentifier string
	EndpointArn                   string
	EndpointIdentifier            string
	Status                        string
	LastFailureMessage            string
	Region                        string
}

// Event records an operational event emitted by a DMS resource.
type Event struct {
	SourceIdentifier string
	SourceType       string
	Message          string
	Date             string
	EventCategories  []string
}

// Recommendation is a target-engine recommendation from Fleet Advisor.
type Recommendation struct {
	DatabaseID string
	EngineName string
	Status     string
}

// FleetAdvisorDatabase is a database discovered by a Fleet Advisor collector.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// FleetAdvisorDatabase carries no other region-derived field, so the value
// needs its own copy to serve as a pure store.Table/store.Index key input.
type FleetAdvisorDatabase struct {
	DatabaseID            string
	DatabaseName          string
	IPAddress             string
	EngineName            string
	CollectorReferencedID string
	Region                string
}

// MetadataModelRequest tracks a metadata model operation (assessment, conversion, etc.).
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// MetadataModelRequest carries no other region-derived field, so the value
// needs its own copy to serve as a pure store.Table/store.Index key input.
type MetadataModelRequest struct {
	RequestIdentifier          string
	MigrationProjectIdentifier string
	Status                     string
	RequestType                string
	SelectionRules             string
	Region                     string
}
