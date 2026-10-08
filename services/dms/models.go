package dms

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// DataMigration represents an AWS DMS data migration.
type DataMigration struct {
	CreationTime         time.Time           `json:"creationTime"`
	Tags                 *tags.Tags          `json:"-"`
	DataMigrationName    string              `json:"dataMigrationName"`
	DataMigrationArn     string              `json:"dataMigrationArn"`
	MigrationProjectArn  string              `json:"migrationProjectArn"`
	DataMigrationType    string              `json:"dataMigrationType"`
	ServiceAccessRoleArn string              `json:"serviceAccessRoleArn"`
	DataMigrationStatus  string              `json:"dataMigrationStatus"`
	AccountID            string              `json:"accountId"`
	Region               string              `json:"region"`
	SelectionRules       string              `json:"selectionRules,omitempty"`
	SourceDataSettings   []SourceDataSetting `json:"sourceDataSettings,omitempty"`
	TargetDataSettings   []TargetDataSetting `json:"targetDataSettings,omitempty"`
	NumberOfJobs         int32               `json:"numberOfJobs"`
	EnableCloudwatchLogs bool                `json:"enableCloudwatchLogs"`
}

// SourceDataSetting mirrors types.SourceDataSetting; its timestamps are
// Iso8601DateTime strings on the wire (serializers.go:9640).
type SourceDataSetting struct {
	CDCStartPosition string `json:"cdcStartPosition,omitempty"`
	SlotName         string `json:"slotName,omitempty"`
	CDCStartTime     string `json:"cdcStartTime,omitempty"`
	CDCStopTime      string `json:"cdcStopTime,omitempty"`
}

// TargetDataSetting mirrors types.TargetDataSetting.
type TargetDataSetting struct {
	TablePreparationMode string `json:"tablePreparationMode,omitempty"`
}

// CreateDataMigrationParams groups CreateDataMigrationInput's members.
type CreateDataMigrationParams struct {
	Tags                       map[string]string
	Name                       string
	MigrationProjectIdentifier string
	DataMigrationType          string
	ServiceAccessRoleArn       string
	SelectionRules             string
	SourceDataSettings         []SourceDataSetting
	TargetDataSettings         []TargetDataSetting
	NumberOfJobs               int32
	EnableCloudwatchLogs       bool
}

// ModifyDataMigrationParams groups ModifyDataMigrationInput's members; nil
// pointers, empty strings and nil slices mean "not supplied".
type ModifyDataMigrationParams struct {
	NumberOfJobs         *int32
	EnableCloudwatchLogs *bool
	SelectionRules       *string
	NameOrArn            string
	NewName              string
	DataMigrationType    string
	ServiceAccessRoleArn string
	SourceDataSettings   []SourceDataSetting
	TargetDataSettings   []TargetDataSetting
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
	Settings         string     `json:"settings,omitempty"`
	Virtual          bool       `json:"virtual,omitempty"`
}

// CreateDataProviderParams groups CreateDataProviderInput's members.
type CreateDataProviderParams struct {
	Tags        map[string]string
	Name        string
	Engine      string
	Description string
	Settings    string
	Virtual     bool
}

// KerberosAuthenticationSettings mirrors types.KerberosAuthenticationSettings.
type KerberosAuthenticationSettings struct {
	KeyCacheSecretIamArn string `json:"keyCacheSecretIamArn,omitempty"`
	KeyCacheSecretID     string `json:"keyCacheSecretId,omitempty"`
	Krb5FileContents     string `json:"krb5FileContents,omitempty"`
}

// ModifyEventSubscriptionParams groups ModifyEventSubscriptionInput's members;
// nil pointers, empty strings and nil slices mean "not supplied".
type ModifyEventSubscriptionParams struct {
	Enabled         *bool
	Name            string
	SnsTopicArn     string
	SourceType      string
	EventCategories []string
}

// ModifyInstanceProfileParams groups ModifyInstanceProfileInput's members;
// empty strings and nil values mean "not supplied".
type ModifyInstanceProfileParams struct {
	PubliclyAccessible    *bool
	NameOrArn             string
	NewName               string
	AvailabilityZone      string
	Description           string
	NetworkType           string
	KmsKeyArn             string
	SubnetGroupIdentifier string
	VpcSecurityGroups     []string
}

// ModifyDataProviderParams groups ModifyDataProviderInput's members; nil
// pointers mean "not supplied" and keep the stored value.
type ModifyDataProviderParams struct {
	Virtual       *bool
	ExactSettings *bool
	NameOrArn     string
	NewName       string
	Engine        string
	Description   string
	Settings      string
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
	VpcSecurityGroups     []string   `json:"vpcSecurityGroups,omitempty"`
	PubliclyAccessible    bool       `json:"publiclyAccessible"`
}

// ReplicationInstance represents an AWS DMS replication instance.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateReplicationInstance.
type ReplicationInstance struct {
	CreationTime                   time.Time                       `json:"creationTime"`
	Tags                           *tags.Tags                      `json:"-"`
	KerberosAuthenticationSettings *KerberosAuthenticationSettings `json:"kerberosAuthenticationSettings,omitempty"`
	ReplicationInstanceIdentifier  string                          `json:"replicationInstanceIdentifier"`
	ReplicationInstanceArn         string                          `json:"replicationInstanceArn"`
	ReplicationInstanceClass       string                          `json:"replicationInstanceClass"`
	EngineVersion                  string                          `json:"engineVersion"`
	AvailabilityZone               string                          `json:"availabilityZone"`
	ReplicationInstanceStatus      string                          `json:"replicationInstanceStatus"`
	PrivateIPAddress               string                          `json:"privateIpAddress"`
	AccountID                      string                          `json:"accountId"`
	Region                         string                          `json:"region"`
	KmsKeyID                       string                          `json:"kmsKeyId,omitempty"`
	DNSNameServers                 string                          `json:"dnsNameServers,omitempty"`
	NetworkType                    string                          `json:"networkType,omitempty"`
	PreferredMaintenanceWindow     string                          `json:"preferredMaintenanceWindow,omitempty"`
	ReplicationSubnetGroupID       string                          `json:"replicationSubnetGroupId,omitempty"`
	VpcSecurityGroupIDs            []string                        `json:"vpcSecurityGroupIds,omitempty"`
	AllocatedStorage               int32                           `json:"allocatedStorage"`
	MultiAZ                        bool                            `json:"multiAZ"`
	AutoMinorVersionUpgrade        bool                            `json:"autoMinorVersionUpgrade"`
	PubliclyAccessible             bool                            `json:"publiclyAccessible"`
}

// Endpoint represents an AWS DMS endpoint.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTagsToResource or CreateEndpoint.
//
// Password is accepted on CreateEndpoint/ModifyEndpoint and stored here, but
// (matching the real Endpoint wire type, which has no Password field) it is
// never put on the wire by any Describe/Create/Modify response -- see
// endpointJSON in handler_endpoints.go. Engine-specific settings blocks are
// stored as raw JSON (S3Settings, EngineSettings) and echoed back.
type Endpoint struct {
	CreationTime              time.Time         `json:"creationTime"`
	Tags                      *tags.Tags        `json:"-"`
	EngineSettings            map[string]string `json:"engineSettings,omitempty"`
	Status                    string            `json:"status"`
	Region                    string            `json:"region"`
	EngineName                string            `json:"engineName"`
	ServerName                string            `json:"serverName,omitempty"`
	DatabaseName              string            `json:"databaseName,omitempty"`
	Username                  string            `json:"username,omitempty"`
	Password                  string            `json:"password,omitempty"`
	EndpointArn               string            `json:"endpointArn"`
	AccountID                 string            `json:"accountId"`
	EndpointType              string            `json:"endpointType"`
	CertificateArn            string            `json:"certificateArn,omitempty"`
	ExtraConnectionAttributes string            `json:"extraConnectionAttributes,omitempty"`
	KmsKeyID                  string            `json:"kmsKeyId,omitempty"`
	ServiceAccessRoleArn      string            `json:"serviceAccessRoleArn,omitempty"`
	SslMode                   string            `json:"sslMode,omitempty"`
	ExternalTableDefinition   string            `json:"externalTableDefinition,omitempty"`
	S3Settings                string            `json:"s3Settings,omitempty"`
	EndpointIdentifier        string            `json:"endpointIdentifier"`
	Port                      int32             `json:"port,omitempty"`
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
	CdcStartPosition          string     `json:"cdcStartPosition,omitempty"`
	CdcStopPosition           string     `json:"cdcStopPosition,omitempty"`
	TaskData                  string     `json:"taskData,omitempty"`
}

// Certificate represents a DMS certificate.
type Certificate struct {
	Tags                  *tags.Tags `json:"-"`
	CertificateIdentifier string
	CertificateArn        string
	CertificatePem        string
	CertificateWallet     string `json:",omitempty"`
	KmsKeyID              string
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
	SubnetIDs                         []string
}

// DataProviderDescriptor mirrors the real AWS DataProviderDescriptor wire
// shape (databasemigrationservice@v1.66.4 types.go:528-544): a resolved data
// provider identity plus the caller's Secrets Manager pass-through fields.
type DataProviderDescriptor struct {
	DataProviderArn             string
	DataProviderName            string
	SecretsManagerAccessRoleArn string
	SecretsManagerSecretId      string //nolint:revive,staticcheck // matches the AWS wire field name.
}

// MigrationProject represents a DMS migration project.
type MigrationProject struct {
	Tags                                  *tags.Tags               `json:"-"`
	SchemaConversionApplicationAttributes *SCApplicationAttributes `json:",omitempty"`
	MigrationProjectName                  string
	MigrationProjectArn                   string
	MigrationProjectIdentifier            string
	Description                           string
	AccountID                             string
	Region                                string
	InstanceProfileArn                    string
	InstanceProfileName                   string
	TransformationRules                   string `json:",omitempty"`
	SourceDataProviderDescriptors         []DataProviderDescriptor
	TargetDataProviderDescriptors         []DataProviderDescriptor
}

// SCApplicationAttributes mirrors types.SCApplicationAttributes.
type SCApplicationAttributes struct {
	S3BucketPath    string
	S3BucketRoleArn string
}

// CreateMigrationProjectParams groups CreateMigrationProjectInput's members.
type CreateMigrationProjectParams struct {
	SchemaConversionApplicationAttributes *SCApplicationAttributes
	Tags                                  map[string]string
	Name                                  string
	Description                           string
	InstanceProfileIdentifier             string
	TransformationRules                   string
	SourceDescriptors                     []DataProviderDescriptorInput
	TargetDescriptors                     []DataProviderDescriptorInput
}

// ModifyMigrationProjectParams groups ModifyMigrationProjectInput's members;
// nil pointers and nil slices mean "not supplied" and keep the stored value.
type ModifyMigrationProjectParams struct {
	SchemaConversionApplicationAttributes *SCApplicationAttributes
	Description                           *string
	InstanceProfileIdentifier             *string
	MigrationProjectName                  *string
	TransformationRules                   *string
	NameOrArn                             string
	SourceDescriptors                     []DataProviderDescriptorInput
	TargetDescriptors                     []DataProviderDescriptorInput
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
	ComputeConfig               *ComputeConfig
	ReplicationConfigIdentifier string
	ReplicationConfigArn        string
	ReplicationType             string
	SourceEndpointArn           string
	TargetEndpointArn           string
	TableMappings               string
	ReplicationSettings         string `json:",omitempty"`
	SupplementalSettings        string `json:",omitempty"`
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
	// CdcStartTime/CdcStartPosition/CdcStopPosition record the CDC window of the most recent StartReplication.
	CdcStartTime     *time.Time `json:",omitempty"`
	CdcStartPosition string     `json:",omitempty"`
	CdcStopPosition  string     `json:",omitempty"`
}

// ComputeConfig mirrors types.ComputeConfig (types.go:190) -- configuration
// parameters for provisioning a DMS Serverless replication. Every member is
// optional; only the ComputeConfig pointer itself is a required
// CreateReplicationConfigInput member.
type ComputeConfig struct {
	MaxCapacityUnits           *int32
	MinCapacityUnits           *int32
	MultiAZ                    *bool
	AvailabilityZone           string
	DNSNameServers             string
	KMSKeyID                   string
	PreferredMaintenanceWindow string
	ReplicationSubnetGroupID   string
	VPCSecurityGroupIDs        []string
}

// CreateReplicationConfigParams groups CreateReplicationConfigInput's fields
// beyond Tags, so the CreateReplicationConfig backend method signature stays
// manageable as fields are added.
type CreateReplicationConfigParams struct {
	ComputeConfig     *ComputeConfig
	Identifier        string
	ReplicationType   string
	SourceEndpointArn string
	TargetEndpointArn string
	TableMappings     string

	ReplicationSettings  string
	SupplementalSettings string
	ResourceIdentifier   string
}

// ModifyReplicationConfigParams groups the ModifyReplicationConfigInput
// members this backend models; empty strings leave the stored value alone.
type ModifyReplicationConfigParams struct {
	ComputeConfig        *ComputeConfig
	IdentifierOrArn      string
	ReplicationType      string
	TableMappings        string
	SourceEndpointArn    string
	TargetEndpointArn    string
	ReplicationSettings  string
	SupplementalSettings string
	NewIdentifier        string
}

// IndividualAssessment represents one named check run as part of a
// premigration AssessmentRun (mirrors types.ReplicationTaskIndividualAssessment).
type IndividualAssessment struct {
	StartDate                              time.Time
	ReplicationTaskIndividualAssessmentArn string
	IndividualAssessmentName               string
	ReplicationTaskAssessmentRunArn        string
	ReplicationTaskArn                     string
	Status                                 string
}

// AssessmentRunResultStatistic mirrors
// types.ReplicationTaskAssessmentRunResultStatistic: aggregated pass/fail
// counts of the individual assessments run as part of an AssessmentRun.
type AssessmentRunResultStatistic struct {
	Cancelled int32
	Error     int32
	Failed    int32
	Passed    int32
	Skipped   int32
	Warning   int32
}

// AssessmentRun represents a DMS pre-migration assessment run.
//
// Region supports Phase 3.3's store.Table keying (see store_setup.go) --
// AssessmentRun carries no other region-derived field, so the value needs
// its own copy to serve as a pure store.Table/store.Index key input.
type AssessmentRun struct {
	CreationDate                    time.Time
	ReplicationTaskAssessmentRunArn string
	ReplicationTaskArn              string
	AssessmentRunName               string
	Status                          string
	ServiceAccessRoleArn            string
	ResultLocationBucket            string
	ResultLocationFolder            string
	ResultEncryptionMode            string
	ResultKmsKeyArn                 string `json:",omitempty"`
	Region                          string
	Tags                            *tags.Tags `json:"-"`
	IndividualAssessments           []*IndividualAssessment
	ResultStatistic                 AssessmentRunResultStatistic
	IsLatestTaskAssessmentRun       bool
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

// Event records an operational event emitted by a DMS resource. Date
// deserializes from a json.Number via smithytime.ParseEpochSeconds --
// confirmed against aws-sdk-go-v2/service/databasemigrationservice@v1.66.4's
// deserializers.go (awsAwsjson11_deserializeDocumentEvent, case "Date").
type Event struct {
	SourceIdentifier string
	SourceType       string
	Message          string
	Date             time.Time
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
	// PropertiesDefinition is StartMetadataModelCreationInput.Properties'
	// StatementProperties.Definition (types.go:5207) -- required only for
	// "creation" requests, always empty for other RequestTypes. Not surfaced
	// on any DescribeMetadataModel* response wire shape (types.SchemaConversionRequest
	// has no matching field, same as SelectionRules above), tracked here for
	// internal state fidelity only.
	PropertiesDefinition string
}
