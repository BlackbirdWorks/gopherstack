package rds

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrInstanceNotFound is returned when an RDS instance does not exist.
	ErrInstanceNotFound = errors.New("DBInstanceNotFound")
	// ErrInstanceAlreadyExists is returned when an RDS instance already exists.
	ErrInstanceAlreadyExists = errors.New("DBInstanceAlreadyExists")
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = errors.New("DBSnapshotNotFound")
	// ErrSnapshotAlreadyExists is returned when a snapshot already exists.
	ErrSnapshotAlreadyExists = errors.New("DBSnapshotAlreadyExists")
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = errors.New("DBSubnetGroupNotFound")
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = errors.New("DBSubnetGroupAlreadyExists")
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = errors.New("InvalidParameterValue")
	// ErrUnknownAction is returned for unrecognized RDS actions.
	ErrUnknownAction = errors.New("InvalidAction")
	// ErrInvalidDBInstanceState is returned when an instance operation is invalid given its current state.
	ErrInvalidDBInstanceState = errors.New("InvalidDBInstanceState")

	// ErrParameterGroupNotFound is returned when a DB parameter group does not exist.
	ErrParameterGroupNotFound = errors.New("DBParameterGroupNotFound")
	// ErrParameterGroupAlreadyExists is returned when a DB parameter group already exists.
	ErrParameterGroupAlreadyExists = errors.New("DBParameterGroupAlreadyExists")
	// ErrOptionGroupNotFound is returned when an option group does not exist.
	ErrOptionGroupNotFound = errors.New("OptionGroupNotFound")
	// ErrOptionGroupAlreadyExists is returned when an option group already exists.
	ErrOptionGroupAlreadyExists = errors.New("OptionGroupAlreadyExists")
	// ErrClusterNotFound is returned when a DB cluster does not exist.
	ErrClusterNotFound = errors.New("DBClusterNotFound")
	// ErrClusterAlreadyExists is returned when a DB cluster already exists.
	ErrClusterAlreadyExists = errors.New("DBClusterAlreadyExists")
	// ErrClusterSnapshotNotFound is returned when a DB cluster snapshot does not exist.
	ErrClusterSnapshotNotFound = errors.New("DBClusterSnapshotNotFound")
	// ErrClusterSnapshotAlreadyExists is returned when a DB cluster snapshot already exists.
	ErrClusterSnapshotAlreadyExists = errors.New("DBClusterSnapshotAlreadyExists")
	// ErrClusterEndpointNotFound is returned when a DB cluster endpoint does not exist.
	ErrClusterEndpointNotFound = errors.New("DBClusterEndpointNotFound")
	// ErrClusterEndpointAlreadyExists is returned when a DB cluster endpoint already exists.
	ErrClusterEndpointAlreadyExists = errors.New("DBClusterEndpointAlreadyExists")
	// ErrExportTaskNotFound is returned when an export task does not exist.
	ErrExportTaskNotFound = errors.New("ExportTaskNotFound")
	// ErrExportTaskAlreadyExists is returned when an export task already exists.
	ErrExportTaskAlreadyExists = errors.New("ExportTaskAlreadyExists")
	// ErrGlobalClusterNotFound is returned when a global cluster does not exist.
	ErrGlobalClusterNotFound = errors.New("GlobalClusterNotFound")
	// ErrGlobalClusterAlreadyExists is returned when a global cluster already exists.
	ErrGlobalClusterAlreadyExists = errors.New("GlobalClusterAlreadyExists")
	// ErrInvalidDBClusterStateFault is returned when a cluster operation is invalid given its current state.
	ErrInvalidDBClusterStateFault = errors.New("InvalidDBClusterStateFault")
	// ErrInvalidGlobalClusterState is returned when a global cluster operation is invalid given its current state.
	ErrInvalidGlobalClusterState = errors.New("InvalidGlobalClusterStateFault")
	// ErrEventSubscriptionNotFound is returned when an event subscription does not exist.
	ErrEventSubscriptionNotFound = errors.New("SubscriptionNotFound")
	// ErrEventSubscriptionAlreadyExists is returned when an event subscription already exists.
	ErrEventSubscriptionAlreadyExists = errors.New("SubscriptionAlreadyExist")
	// ErrDBSecurityGroupNotFound is returned when a DB security group does not exist.
	ErrDBSecurityGroupNotFound = errors.New("DBSecurityGroupNotFound")
	// ErrDBSecurityGroupAlreadyExists is returned when a DB security group already exists.
	ErrDBSecurityGroupAlreadyExists = errors.New("DBSecurityGroupAlreadyExists")
	// ErrBlueGreenDeploymentNotFound is returned when a Blue/Green Deployment does not exist.
	ErrBlueGreenDeploymentNotFound = errors.New("BlueGreenDeploymentNotFound")
	// ErrBlueGreenDeploymentAlreadyExists is returned when a Blue/Green Deployment already exists.
	ErrBlueGreenDeploymentAlreadyExists = errors.New("BlueGreenDeploymentAlreadyExists")
	// ErrNoServerlessV2Config is a sentinel indicating no ServerlessV2ScalingConfiguration was provided.
	ErrNoServerlessV2Config = errors.New("noServerlessV2Config")

	// ErrDBShardGroupNotFound is returned when a DB shard group does not exist.
	ErrDBShardGroupNotFound = errors.New("DBShardGroupNotFound")
	// ErrDBShardGroupAlreadyExists is returned when a DB shard group already exists.
	ErrDBShardGroupAlreadyExists = errors.New("DBShardGroupAlreadyExists")

	// ErrIntegrationNotFound is returned when an integration does not exist.
	ErrIntegrationNotFound = errors.New("IntegrationNotFound")
	// ErrIntegrationAlreadyExists is returned when an integration already exists.
	ErrIntegrationAlreadyExists = errors.New("IntegrationAlreadyExists")

	// ErrTenantDatabaseNotFound is returned when a tenant database does not exist.
	ErrTenantDatabaseNotFound = errors.New("TenantDatabaseNotFound")
	// ErrTenantDatabaseAlreadyExists is returned when a tenant database already exists.
	ErrTenantDatabaseAlreadyExists = errors.New("TenantDatabaseAlreadyExists")

	// ErrDBClusterAutomatedBackupNotFound is returned when a cluster automated backup does not exist.
	ErrDBClusterAutomatedBackupNotFound = errors.New("DBClusterAutomatedBackupNotFound")
	// ErrDBInstanceAutomatedBackupNotFound is returned when an instance automated backup does not exist.
	ErrDBInstanceAutomatedBackupNotFound = errors.New("DBInstanceAutomatedBackupNotFound")
)

const (
	defaultPort             = 5432
	mysqlPort               = 3306
	defaultInstanceClass    = "db.t3.micro"
	defaultAllocatedStorage = 20

	instanceStatusModifying = "modifying"
	instanceStatusDeleting  = "deleting"
	instanceStatusAvailable = "available"
	instanceStatusStopped   = "stopped"

	subscriptionStatusActive           = "active"
	backtrackStatusApplying            = "applying"
	blueGreenDeploymentStatusAvailable = "available"
	ipRangeStatusAuthorized            = "authorized"
	instanceTransitionDelay            = 250 * time.Millisecond
	reconcilerDivisor                  = 5
	maxEvents                          = 512
	percentProgressComplete            = 100

	engineMySQL            = "mysql"
	engineMariaDB          = "mariadb"
	enginePostgres         = "postgres"
	engineAuroraMySQL      = "aurora-mysql"
	engineAuroraPostgresql = "aurora-postgresql"

	currencyUSD              = "USD"
	reservedValidFrom        = "2021-05-25T00:00:00Z"
	reservedAllUpfront       = "All Upfront"
	clusterEndpointReadWrite = "READ_WRITE"
	opDescribeGlobalClusters = "DescribeGlobalClusters"
)

// VpcSecurityGroupMembership represents a VPC security group association.
type VpcSecurityGroupMembership struct {
	VpcSecurityGroupID string `json:"vpcSecurityGroupId"`
	Status             string `json:"status"`
}

// DBClusterMember represents a member instance in a DB cluster.
type DBClusterMember struct {
	DBInstanceIdentifier        string `json:"dbInstanceIdentifier"`
	DBClusterParameterGroupName string `json:"dbClusterParameterGroupName"`
	PromotionTier               int    `json:"promotionTier"`
	IsClusterWriter             bool   `json:"isClusterWriter"`
}

// GlobalClusterMember represents a member cluster in a global cluster.
type GlobalClusterMember struct {
	DBClusterArn          string `json:"dbClusterArn"`
	GlobalWriteForwarding bool   `json:"globalWriteForwarding"`
	IsWriter              bool   `json:"isWriter"`
}

// CustomDBEngineVersion represents a custom engine version for RDS.
type CustomDBEngineVersion struct {
	Engine        string `json:"engine"`
	EngineVersion string `json:"engineVersion"`
	Status        string `json:"status"`
	Description   string `json:"description"`
}

// DBInstance represents an RDS database instance.
type DBInstance struct {
	InstanceCreateTime                time.Time                    `json:"instanceCreateTime"`
	DBInstanceIdentifier              string                       `json:"dbInstanceIdentifier"`
	DbiResourceID                     string                       `json:"dbiResourceID"`
	DBInstanceClass                   string                       `json:"dbInstanceClass"`
	DBClusterIdentifier               string                       `json:"dbClusterIdentifier,omitempty"`
	Engine                            string                       `json:"engine"`
	EngineVersion                     string                       `json:"engineVersion"`
	DBInstanceStatus                  string                       `json:"dbInstanceStatus"`
	MasterUsername                    string                       `json:"masterUsername"`
	DBName                            string                       `json:"dbName"`
	Endpoint                          string                       `json:"endpoint"`
	VpcID                             string                       `json:"vpcID"`
	DBSubnetGroupName                 string                       `json:"dbSubnetGroupName"`
	DBParameterGroupName              string                       `json:"dbParameterGroupName"`
	OptionGroupName                   string                       `json:"optionGroupName,omitempty"`
	ReplicaSourceDBInstanceIdentifier string                       `json:"replicaSourceDBInstanceIdentifier"`
	AvailabilityZone                  string                       `json:"availabilityZone"`
	StorageType                       string                       `json:"storageType"`
	LicenseModel                      string                       `json:"licenseModel,omitempty"`
	MonitoringRoleArn                 string                       `json:"monitoringRoleArn,omitempty"`
	EnhancedMonitoringResourceArn     string                       `json:"enhancedMonitoringResourceArn,omitempty"`
	PreferredMaintenanceWindow        string                       `json:"preferredMaintenanceWindow,omitempty"`
	PreferredBackupWindow             string                       `json:"preferredBackupWindow,omitempty"`
	KmsKeyID                          string                       `json:"kmsKeyID,omitempty"`
	VpcSecurityGroups                 []VpcSecurityGroupMembership `json:"vpcSecurityGroups,omitempty"`
	ReadReplicaIdentifiers            []string                     `json:"readReplicaIdentifiers,omitempty"`
	EnabledCloudwatchLogsExports      []string                     `json:"enabledCloudwatchLogsExports,omitempty"`
	Port                              int                          `json:"port"`
	AllocatedStorage                  int                          `json:"allocatedStorage"`
	Iops                              int                          `json:"iops,omitempty"`
	StorageThroughput                 int                          `json:"storageThroughput,omitempty"`
	BackupRetentionPeriod             int                          `json:"backupRetentionPeriod"`
	MonitoringInterval                int                          `json:"monitoringInterval,omitempty"`
	EngineLifecycleSupport            string                       `json:"engineLifecycleSupport,omitempty"`
	MultiAZ                           bool                         `json:"multiAZ"`
	StorageEncrypted                  bool                         `json:"storageEncrypted"`
	IAMDatabaseAuthenticationEnabled  bool                         `json:"iamDatabaseAuthenticationEnabled"`
	DeletionProtection                bool                         `json:"deletionProtection"`
	CopyTagsToSnapshot                bool                         `json:"copyTagsToSnapshot,omitempty"`
	PubliclyAccessible                bool                         `json:"publiclyAccessible,omitempty"`
	PerformanceInsightsEnabled        bool                         `json:"performanceInsightsEnabled,omitempty"`
	StorageOptimized                  bool                         `json:"storageOptimized,omitempty"`
	OptimizedWrites                   bool                         `json:"optimizedWrites,omitempty"`
}

// DBSnapshot represents an RDS database snapshot.
type DBSnapshot struct {
	SnapshotCreateTime   time.Time `json:"snapshotCreateTime"`
	DBSnapshotIdentifier string    `json:"dbSnapshotIdentifier"`
	DBInstanceIdentifier string    `json:"dbInstanceIdentifier"`
	Engine               string    `json:"engine"`
	EngineVersion        string    `json:"engineVersion"`
	Status               string    `json:"status"`
	StorageType          string    `json:"storageType"`
	OptionGroupName      string    `json:"optionGroupName"`
	KmsKeyID             string    `json:"kmsKeyID,omitempty"`
	SourceRegion         string    `json:"sourceRegion,omitempty"`
	SnapshotType         string    `json:"snapshotType,omitempty"`
	AllocatedStorage     int       `json:"allocatedStorage"`
	Port                 int       `json:"port"`
	PercentProgress      int       `json:"percentProgress"`
	StorageEncrypted     bool      `json:"storageEncrypted"`
	CopyTagsToSnapshot   bool      `json:"copyTagsToSnapshot,omitempty"`
}

// DBSubnetGroup represents an RDS DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string   `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string   `json:"dbSubnetGroupDescription"`
	VpcID                    string   `json:"vpcID"`
	Status                   string   `json:"status"`
	SubnetIDs                []string `json:"subnetIDs"`
}

// Tag is a key/value tag attached to an RDS resource.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DBParameter represents a single RDS parameter.
type DBParameter struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
	Description    string `json:"description"`
	ApplyType      string `json:"applyType"`
	DataType       string `json:"dataType"`
	IsModifiable   bool   `json:"isModifiable"`
}

// DBParameterGroup represents an RDS DB parameter group.
type DBParameterGroup struct {
	Parameters             map[string]DBParameter `json:"parameters"`
	DBParameterGroupName   string                 `json:"dbParameterGroupName"`
	DBParameterGroupFamily string                 `json:"dbParameterGroupFamily"`
	Description            string                 `json:"description"`
}

// OptionGroupOption represents an option within an option group.
type OptionGroupOption struct {
	OptionName    string `json:"optionName"`
	OptionVersion string `json:"optionVersion"`
}

// OptionGroup represents an RDS option group.
type OptionGroup struct {
	OptionGroupName        string              `json:"optionGroupName"`
	OptionGroupDescription string              `json:"optionGroupDescription"`
	EngineName             string              `json:"engineName"`
	MajorEngineVersion     string              `json:"majorEngineVersion"`
	Options                []OptionGroupOption `json:"options"`
}

// ServerlessV2ScalingConfiguration holds Aurora Serverless v2 capacity settings.
type ServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `json:"minCapacity"`
	MaxCapacity float64 `json:"maxCapacity"`
}

// DBCluster represents an Aurora-style RDS cluster.
type DBCluster struct {
	ClusterCreateTime               time.Time                         `json:"clusterCreateTime"`
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration `json:"serverlessV2ScalingConfiguration,omitempty"`
	Endpoint                        string                            `json:"endpoint"`
	ActivityStreamStatus            string                            `json:"activityStreamStatus"`
	Status                          string                            `json:"status"`
	MasterUsername                  string                            `json:"masterUsername"`
	DatabaseName                    string                            `json:"databaseName"`
	DBClusterParameterGroupName     string                            `json:"dbClusterParameterGroupName"`
	Engine                          string                            `json:"engine"`
	EngineVersion                   string                            `json:"engineVersion,omitempty"`
	ActivityStreamAuditPolicy       string                            `json:"activityStreamAuditPolicy"`
	DBClusterIdentifier             string                            `json:"dbClusterIdentifier"`
	ActivityStreamKinesisStreamName string                            `json:"activityStreamKinesisStreamName"`
	ActivityStreamKMSKeyID          string                            `json:"activityStreamKmsKeyId"`
	ActivityStreamMode              string                            `json:"activityStreamMode"`
	PreferredBackupWindow           string                            `json:"preferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow      string                            `json:"preferredMaintenanceWindow,omitempty"`
	KmsKeyID                        string                            `json:"kmsKeyID,omitempty"`
	MonitoringRoleArn               string                            `json:"monitoringRoleArn,omitempty"`
	DBClusterMembers                []DBClusterMember                 `json:"dbClusterMembers,omitempty"`
	ReaderAvailabilityZones         []string                          `json:"readerAvailabilityZones,omitempty"`
	AvailabilityZones               []string                          `json:"availabilityZones,omitempty"`
	EnabledCloudwatchLogsExports    []string                          `json:"enabledCloudwatchLogsExports,omitempty"`
	Port                            int                               `json:"port"`
	ServerlessCapacity              int                               `json:"serverlessCapacity"`
	MonitoringInterval              int                               `json:"monitoringInterval,omitempty"`
	BacktrackWindow                 int64                             `json:"backtrackWindow,omitempty"`
	StorageType                     string                            `json:"storageType,omitempty"`
	ReaderEndpoint                  string                            `json:"readerEndpoint,omitempty"`
	NetworkType                     string                            `json:"networkType,omitempty"`
	EngineLifecycleSupport          string                            `json:"engineLifecycleSupport,omitempty"`
	HTTPEndpointEnabled             bool                              `json:"httpEndpointEnabled"`
	MultiAZ                         bool                              `json:"multiAZ,omitempty"`
	StorageEncrypted                bool                              `json:"storageEncrypted,omitempty"`
	CopyTagsToSnapshot              bool                              `json:"copyTagsToSnapshot,omitempty"`
	DeletionProtection              bool                              `json:"deletionProtection,omitempty"`
	OptimizedWrites                 bool                              `json:"optimizedWrites,omitempty"`
}

// DBClusterSnapshot represents an RDS cluster snapshot.
type DBClusterSnapshot struct {
	SnapshotCreateTime          time.Time `json:"snapshotCreateTime"`
	DBClusterSnapshotIdentifier string    `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string    `json:"dbClusterIdentifier"`
	Engine                      string    `json:"engine"`
	EngineVersion               string    `json:"engineVersion,omitempty"`
	Status                      string    `json:"status"`
	PercentProgress             int       `json:"percentProgress"`
	StorageEncrypted            bool      `json:"storageEncrypted,omitempty"`
}

// DBClusterEndpoint represents a custom endpoint for an RDS cluster.
type DBClusterEndpoint struct {
	DBClusterEndpointIdentifier string `json:"dbClusterEndpointIdentifier"`
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	EndpointType                string `json:"endpointType"`
	Status                      string `json:"status"`
	Endpoint                    string `json:"endpoint"`
}

// ExportTask represents an RDS export task.
type ExportTask struct {
	ExportTaskIdentifier string `json:"exportTaskIdentifier"`
	SourceArn            string `json:"sourceArn"`
	Status               string `json:"status"`
	S3Bucket             string `json:"s3Bucket"`
}

// GlobalCluster represents an RDS global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string                `json:"globalClusterIdentifier"`
	Engine                  string                `json:"engine"`
	EngineVersion           string                `json:"engineVersion"`
	Status                  string                `json:"status"`
	PrimaryRegion           string                `json:"primaryRegion,omitempty"`
	GlobalClusterMembers    []GlobalClusterMember `json:"globalClusterMembers,omitempty"`
	ClusterARNs             []string              `json:"clusterARNs"`
	StorageEncrypted        bool                  `json:"storageEncrypted"`
	DeletionProtection      bool                  `json:"deletionProtection"`
}

// DBEngineVersion represents an available RDS engine version.
type DBEngineVersion struct {
	Engine              string `json:"engine"`
	EngineVersion       string `json:"engineVersion"`
	DBEngineDescription string `json:"dbEngineDescription"`
}

// DBSnapshotAttribute represents an attribute of a DB snapshot.
type DBSnapshotAttribute struct {
	AttributeName   string   `json:"attributeName"`
	AttributeValues []string `json:"attributeValues"`
}

// DBSnapshotAttributesResult holds attributes for a DB snapshot.
type DBSnapshotAttributesResult struct {
	DBSnapshotIdentifier string                `json:"dbSnapshotIdentifier"`
	DBSnapshotAttributes []DBSnapshotAttribute `json:"dbSnapshotAttributes"`
}

// DBClusterSnapshotAttributesResult holds attributes for a cluster snapshot.
type DBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                `json:"dbClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes []DBSnapshotAttribute `json:"dbClusterSnapshotAttributes"`
}

// Certificate represents an RDS CA certificate.
type Certificate struct {
	CertificateIdentifier string `json:"certificateIdentifier"`
	CertificateType       string `json:"certificateType"`
	ValidTill             string `json:"validTill"`
	ValidFrom             string `json:"validFrom"`
	Thumbprint            string `json:"thumbprint"`
	CustomerOverride      bool   `json:"customerOverride"`
}

// AccountAttribute represents an RDS account quota attribute.
type AccountAttribute struct {
	AttributeName string `json:"attributeName"`
	Used          int    `json:"used"`
	Max           int    `json:"max"`
}

// PendingMaintenanceAction represents a pending maintenance action for a resource.
type PendingMaintenanceAction struct {
	ResourceIdentifier   string `json:"resourceIdentifier"`
	Action               string `json:"action"`
	AutoAppliedAfterDate string `json:"autoAppliedAfterDate"`
	CurrentApplyDate     string `json:"currentApplyDate"`
	Description          string `json:"description"`
}

// SourceRegion represents an available source region for cross-region operations.
type SourceRegion struct {
	RegionName string `json:"regionName"`
	Endpoint   string `json:"endpoint"`
	Status     string `json:"status"`
}

// DBMajorEngineVersion represents a major engine version.
type DBMajorEngineVersion struct {
	Engine             string `json:"engine"`
	MajorEngineVersion string `json:"majorEngineVersion"`
	Status             string `json:"status"`
}

// ReservedDBInstance represents a purchased reserved DB instance.
type ReservedDBInstance struct {
	ReservedDBInstanceID          string  `json:"reservedDBInstanceId"`
	ReservedDBInstancesOfferingID string  `json:"reservedDBInstancesOfferingId"`
	DBInstanceClass               string  `json:"dbInstanceClass"`
	StartTime                     string  `json:"startTime"`
	ProductDescription            string  `json:"productDescription"`
	OfferingType                  string  `json:"offeringType"`
	State                         string  `json:"state"`
	CurrencyCode                  string  `json:"currencyCode"`
	FixedPrice                    float64 `json:"fixedPrice"`
	UsagePrice                    float64 `json:"usagePrice"`
	Duration                      int     `json:"duration"`
	DBInstanceCount               int     `json:"dbInstanceCount"`
	MultiAZ                       bool    `json:"multiAZ"`
}

// ReservedDBInstancesOffering represents an available reserved DB instance offering.
type ReservedDBInstancesOffering struct {
	ReservedDBInstancesOfferingID string  `json:"reservedDBInstancesOfferingId"`
	DBInstanceClass               string  `json:"dbInstanceClass"`
	ProductDescription            string  `json:"productDescription"`
	OfferingType                  string  `json:"offeringType"`
	CurrencyCode                  string  `json:"currencyCode"`
	FixedPrice                    float64 `json:"fixedPrice"`
	UsagePrice                    float64 `json:"usagePrice"`
	Duration                      int     `json:"duration"`
	MultiAZ                       bool    `json:"multiAZ"`
}

// DBRecommendation represents an RDS performance recommendation.
type DBRecommendation struct {
	RecommendationID string `json:"recommendationId"`
	TypeID           string `json:"typeId"`
	Severity         string `json:"severity"`
	Status           string `json:"status"`
	Description      string `json:"description"`
	Reason           string `json:"reason"`
	ResourceARN      string `json:"resourceArn"`
	UpdatedTime      string `json:"updatedTime"`
	CreatedTime      string `json:"createdTime"`
}

// OrderableDBInstanceOption represents an orderable DB instance option.
type OrderableDBInstanceOption struct {
	Engine          string `json:"engine"`
	EngineVersion   string `json:"engineVersion"`
	DBInstanceClass string `json:"dbInstanceClass"`
	MultiAZCapable  bool   `json:"multiAZCapable"`
}

// DBLogFile represents a log file for a DB instance.
type DBLogFile struct {
	LogFileName string `json:"logFileName"`
	Size        int64  `json:"size"`
}

// EventSubscription represents an RDS event notification subscription.
type EventSubscription struct {
	SubscriptionName string   `json:"subscriptionName"`
	SnsTopicArn      string   `json:"snsTopicArn"`
	Status           string   `json:"status"`
	SourceType       string   `json:"sourceType"`
	SourceIDs        []string `json:"sourceIds"`
	EventCategories  []string `json:"eventCategories,omitempty"`
	Enabled          bool     `json:"enabled"`
}

// Event represents a published RDS lifecycle event.
type Event struct {
	CreatedAt        time.Time `json:"createdAt"`
	Message          string    `json:"message"`
	SourceIdentifier string    `json:"sourceIdentifier"`
	SourceType       string    `json:"sourceType"`
}

// IPRange represents a CIDR IP range authorized for a DB security group.
type IPRange struct {
	CIDRIP string `json:"cidrip"`
	Status string `json:"status"`
}

// DBSecurityGroup represents an RDS DB security group.
type DBSecurityGroup struct {
	DBSecurityGroupName        string    `json:"dbSecurityGroupName"`
	DBSecurityGroupDescription string    `json:"dbSecurityGroupDescription"`
	IPRanges                   []IPRange `json:"ipRanges"`
}

// BlueGreenDeployment represents an RDS Blue/Green Deployment.
type BlueGreenDeployment struct {
	BlueGreenDeploymentIdentifier string `json:"blueGreenDeploymentIdentifier"`
	BlueGreenDeploymentName       string `json:"blueGreenDeploymentName"`
	Source                        string `json:"source"`
	Target                        string `json:"target,omitempty"`
	Status                        string `json:"status"`
}

// DBClusterBacktrack represents backtrack information for an Aurora cluster.
type DBClusterBacktrack struct {
	DBClusterIdentifier string `json:"dbClusterIdentifier"`
	BacktrackIdentifier string `json:"backtrackIdentifier"`
	BacktrackTo         string `json:"backtrackTo"`
	Status              string `json:"status"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// DBShardGroup represents an Aurora Limitless DB shard group.
type DBShardGroup struct {
	DBShardGroupIdentifier string  `json:"dbShardGroupIdentifier"`
	DBClusterIdentifier    string  `json:"dbClusterIdentifier"`
	Status                 string  `json:"status"`
	Endpoint               string  `json:"endpoint,omitempty"`
	MaxACU                 float64 `json:"maxACU,omitempty"`
	MinACU                 float64 `json:"minACU,omitempty"`
	ComputeRedundancy      int     `json:"computeRedundancy,omitempty"`
	PubliclyAccessible     bool    `json:"publiclyAccessible,omitempty"`
}

// Integration represents an RDS zero-ETL integration to Amazon Redshift.
type Integration struct {
	CreatedAt             time.Time `json:"createdAt"`
	IntegrationArn        string    `json:"integrationArn"`
	IntegrationName       string    `json:"integrationName"`
	SourceArn             string    `json:"sourceArn"`
	TargetArn             string    `json:"targetArn"`
	KmsKeyID              string    `json:"kmsKeyId,omitempty"`
	DataFilter            string    `json:"dataFilter,omitempty"`
	IntegrationDescription string   `json:"integrationDescription,omitempty"`
	Status                string    `json:"status"`
}

// TenantDatabase represents a tenant database within a multi-tenant RDS instance.
type TenantDatabase struct {
	CreatedAt            time.Time `json:"createdAt"`
	DBInstanceIdentifier string    `json:"dbInstanceIdentifier"`
	TenantDBName         string    `json:"tenantDBName"`
	MasterUsername       string    `json:"masterUsername"`
	TenantDatabaseARN    string    `json:"tenantDatabaseArn"`
	DbiResourceID        string    `json:"dbiResourceId"`
	Status               string    `json:"status"`
}

// DBClusterAutomatedBackup represents an automated backup record for an RDS cluster.
type DBClusterAutomatedBackup struct {
	DBClusterIdentifier   string `json:"dbClusterIdentifier"`
	DBClusterResourceID   string `json:"dbClusterResourceId"`
	Engine                string `json:"engine"`
	EngineVersion         string `json:"engineVersion"`
	Region                string `json:"region"`
	Status                string `json:"status"`
	BackupRetentionPeriod int    `json:"backupRetentionPeriod"`
	StorageEncrypted      bool   `json:"storageEncrypted"`
}

// DBSnapshotTenantDatabase represents a tenant database within a DB snapshot.
type DBSnapshotTenantDatabase struct {
	DBSnapshotIdentifier string `json:"dbSnapshotIdentifier"`
	DBInstanceIdentifier string `json:"dbInstanceIdentifier"`
	TenantDatabaseName   string `json:"tenantDatabaseName"`
	Engine               string `json:"engine"`
	Status               string `json:"status"`
}

// DBInstanceAutomatedBackup represents an automated backup record for an RDS instance.
type DBInstanceAutomatedBackup struct {
	DBInstanceIdentifier  string `json:"dbInstanceIdentifier"`
	DbiResourceID         string `json:"dbiResourceId"`
	Engine                string `json:"engine"`
	EngineVersion         string `json:"engineVersion"`
	DBInstanceArn         string `json:"dbInstanceArn"`
	Region                string `json:"region"`
	Status                string `json:"status"`
	AllocatedStorage      int    `json:"allocatedStorage"`
	Port                  int    `json:"port"`
	BackupRetentionPeriod int    `json:"backupRetentionPeriod"`
	Encrypted             bool   `json:"encrypted"`
}

// DBInstanceOptions holds optional fields for CreateDBInstance and ModifyDBInstance.
type DBInstanceOptions struct {
	EngineVersion                    string
	StorageType                      string
	AvailabilityZone                 string
	DBParameterGroupName             string
	OptionGroupName                  string
	SourceRegion                     string
	LicenseModel                     string
	MonitoringRoleArn                string
	PreferredMaintenanceWindow       string
	PreferredBackupWindow            string
	KmsKeyID                         string
	DBClusterIdentifier              string
	EngineLifecycleSupport           string
	VpcSecurityGroupIDs              []string
	EnabledCloudwatchLogsExports     []string
	BackupRetentionPeriod            int
	Iops                             int
	StorageThroughput                int
	MonitoringInterval               int
	MultiAZ                          bool
	MultiAZSet                       bool
	StorageEncrypted                 bool
	IAMDatabaseAuthenticationEnabled bool
	IAMDatabaseAuthSet               bool
	DeletionProtection               bool
	DeletionProtectionSet            bool
	CopyTagsToSnapshot               bool
	AllowMajorVersionUpgrade         bool
	ApplyImmediately                 bool
	PubliclyAccessible               bool
	PerformanceInsightsEnabled       bool
	StorageOptimized                 bool
	OptimizedWrites                  bool
}

// CopyDBSnapshotOptions holds optional fields for CopyDBSnapshot.
type CopyDBSnapshotOptions struct {
	KmsKeyID     string
	SourceRegion string
	CopyTags     bool
}

// DBClusterOptions holds optional fields for CreateDBCluster and ModifyDBCluster.
type DBClusterOptions struct {
	EngineVersion                string
	KmsKeyID                     string
	PreferredBackupWindow        string
	PreferredMaintenanceWindow   string
	MonitoringRoleArn            string
	StorageType                  string
	NetworkType                  string
	EngineLifecycleSupport       string
	EnabledCloudwatchLogsExports []string
	AvailabilityZones            []string
	BacktrackWindow              int64
	MonitoringInterval           int
	MultiAZ                      bool
	StorageEncrypted             bool
	StorageEncryptedChanged      bool
	CopyTagsToSnapshot           bool
	DeletionProtection           bool
	DeletionProtectionSet        bool
	OptimizedWrites              bool
}

// InMemoryBackend is the in-memory store for RDS resources.
type InMemoryBackend struct {
	dnsRegistrar              DNSRegistrar
	clusterEndpoints          map[string]*DBClusterEndpoint
	parameterGroups           map[string]*DBParameterGroup
	snapshots                 map[string]*DBSnapshot
	subnetGroups              map[string]*DBSubnetGroup
	tags                      map[string][]Tag
	instances                 map[string]*DBInstance
	clusterParameterGroups    map[string]*DBParameterGroup
	optionGroups              map[string]*OptionGroup
	clusters                  map[string]*DBCluster
	instanceReadyAt           map[string]time.Time
	clusterSnapshots          map[string]*DBClusterSnapshot
	eventSubscriptions        map[string]*EventSubscription
	globalClusters            map[string]*GlobalCluster
	clusterRoles              map[string][]string
	instanceRoles             map[string][]string
	exportTasks               map[string]*ExportTask
	mu                        *lockmetrics.RWMutex
	dbSecurityGroups          map[string]*DBSecurityGroup
	blueGreenDeployments      map[string]*BlueGreenDeployment
	snapshotAttributes        map[string]*DBSnapshotAttributesResult
	clusterSnapshotAttributes map[string]*DBClusterSnapshotAttributesResult
	reservedInstances         map[string]*ReservedDBInstance
	recommendations           map[string]*DBRecommendation
	proxies                   map[string]*DBProxy
	proxyTargetGroups         map[string]*DBProxyTargetGroup
	proxyTargets              map[string][]DBProxyTarget
	proxyEndpoints            map[string]*DBProxyEndpoint
	customEngineVersions      map[string]*CustomDBEngineVersion
	fisFailoverFaults         map[string]time.Time
	automatedBackups          map[string]*DBInstanceAutomatedBackup
	shardGroups               map[string]*DBShardGroup
	integrations              map[string]*Integration
	tenantDatabases           map[string]*TenantDatabase
	clusterAutomatedBackups   map[string]*DBClusterAutomatedBackup
	snapshotTenantDatabases   map[string][]*DBSnapshotTenantDatabase
	stopCh                    chan struct{}
	accountID                 string
	region                    string
	events                    []Event
}

// NewInMemoryBackend creates a new InMemoryBackend with a background reconciler.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		instances:                 make(map[string]*DBInstance),
		instanceReadyAt:           make(map[string]time.Time),
		snapshots:                 make(map[string]*DBSnapshot),
		subnetGroups:              make(map[string]*DBSubnetGroup),
		tags:                      make(map[string][]Tag),
		parameterGroups:           make(map[string]*DBParameterGroup),
		clusterParameterGroups:    make(map[string]*DBParameterGroup),
		optionGroups:              make(map[string]*OptionGroup),
		clusters:                  make(map[string]*DBCluster),
		clusterSnapshots:          make(map[string]*DBClusterSnapshot),
		clusterEndpoints:          make(map[string]*DBClusterEndpoint),
		exportTasks:               make(map[string]*ExportTask),
		globalClusters:            make(map[string]*GlobalCluster),
		clusterRoles:              make(map[string][]string),
		instanceRoles:             make(map[string][]string),
		eventSubscriptions:        make(map[string]*EventSubscription),
		events:                    make([]Event, 0),
		dbSecurityGroups:          make(map[string]*DBSecurityGroup),
		blueGreenDeployments:      make(map[string]*BlueGreenDeployment),
		fisFailoverFaults:         make(map[string]time.Time),
		snapshotAttributes:        make(map[string]*DBSnapshotAttributesResult),
		clusterSnapshotAttributes: make(map[string]*DBClusterSnapshotAttributesResult),
		reservedInstances:         make(map[string]*ReservedDBInstance),
		recommendations:           make(map[string]*DBRecommendation),
		proxies:                   make(map[string]*DBProxy),
		proxyTargetGroups:         make(map[string]*DBProxyTargetGroup),
		proxyTargets:              make(map[string][]DBProxyTarget),
		proxyEndpoints:            make(map[string]*DBProxyEndpoint),
		automatedBackups:          make(map[string]*DBInstanceAutomatedBackup),
		customEngineVersions:      make(map[string]*CustomDBEngineVersion),
		shardGroups:               make(map[string]*DBShardGroup),
		integrations:              make(map[string]*Integration),
		tenantDatabases:           make(map[string]*TenantDatabase),
		clusterAutomatedBackups:   make(map[string]*DBClusterAutomatedBackup),
		snapshotTenantDatabases:   make(map[string][]*DBSnapshotTenantDatabase),
		stopCh:                    make(chan struct{}),
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("rds"),
	}

	go b.runReconciler()

	return b
}

// Close stops the background reconciler goroutine.
func (b *InMemoryBackend) Close() {
	close(b.stopCh)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.instances = make(map[string]*DBInstance)
	b.instanceReadyAt = make(map[string]time.Time)
	b.snapshots = make(map[string]*DBSnapshot)
	b.subnetGroups = make(map[string]*DBSubnetGroup)
	b.tags = make(map[string][]Tag)
	b.parameterGroups = make(map[string]*DBParameterGroup)
	b.clusterParameterGroups = make(map[string]*DBParameterGroup)
	b.optionGroups = make(map[string]*OptionGroup)
	b.clusters = make(map[string]*DBCluster)
	b.clusterSnapshots = make(map[string]*DBClusterSnapshot)
	b.clusterEndpoints = make(map[string]*DBClusterEndpoint)
	b.exportTasks = make(map[string]*ExportTask)
	b.globalClusters = make(map[string]*GlobalCluster)
	b.clusterRoles = make(map[string][]string)
	b.instanceRoles = make(map[string][]string)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.events = make([]Event, 0)
	b.dbSecurityGroups = make(map[string]*DBSecurityGroup)
	b.blueGreenDeployments = make(map[string]*BlueGreenDeployment)
	b.fisFailoverFaults = make(map[string]time.Time)
	b.snapshotAttributes = make(map[string]*DBSnapshotAttributesResult)
	b.clusterSnapshotAttributes = make(map[string]*DBClusterSnapshotAttributesResult)
	b.reservedInstances = make(map[string]*ReservedDBInstance)
	b.recommendations = make(map[string]*DBRecommendation)
	b.proxies = make(map[string]*DBProxy)
	b.proxyTargetGroups = make(map[string]*DBProxyTargetGroup)
	b.proxyTargets = make(map[string][]DBProxyTarget)
	b.proxyEndpoints = make(map[string]*DBProxyEndpoint)
	b.automatedBackups = make(map[string]*DBInstanceAutomatedBackup)
	b.customEngineVersions = make(map[string]*CustomDBEngineVersion)
	b.shardGroups = make(map[string]*DBShardGroup)
	b.integrations = make(map[string]*Integration)
	b.tenantDatabases = make(map[string]*TenantDatabase)
	b.clusterAutomatedBackups = make(map[string]*DBClusterAutomatedBackup)
	b.snapshotTenantDatabases = make(map[string][]*DBSnapshotTenantDatabase)
}

// SetDNSRegistrar wires a DNS server so RDS instance hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = dns
	b.mu.Unlock()
}

// enginePort returns the default port for the given database engine.
func enginePort(engine string) int {
	switch engine {
	case engineMySQL, engineMariaDB, engineAuroraMySQL:
		return mysqlPort
	default:
		return defaultPort
	}
}

func (b *InMemoryBackend) reconcileInstancesLocked() {
	now := time.Now()

	for id, inst := range b.instances {
		readyAt, hasReadyAt := b.instanceReadyAt[id]
		if hasReadyAt && !readyAt.IsZero() && now.After(readyAt) {
			inst.DBInstanceStatus = instanceStatusAvailable
			delete(b.instanceReadyAt, id)
			b.publishInstanceEventLocked(id, "DB instance is now available")
		}
	}
}

// runReconciler periodically transitions DB instances that have passed their
// ready-at timestamp. It runs as a long-lived background goroutine until Close() is called.
func (b *InMemoryBackend) runReconciler() {
	ticker := time.NewTicker(instanceTransitionDelay / reconcilerDivisor)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock("runReconciler")
			b.reconcileInstancesLocked()
			b.mu.Unlock()
		}
	}
}

func (b *InMemoryBackend) publishInstanceEventLocked(id, msg string) {
	event := Event{
		Message:          msg,
		SourceIdentifier: id,
		SourceType:       "db-instance",
		CreatedAt:        time.Now(),
	}

	b.events = append(b.events, event)
	if len(b.events) > maxEvents {
		b.events = b.events[len(b.events)-maxEvents:]
	}
}

// CreateDBInstance creates a new RDS DB instance.

// normalizeDBInstanceDefaults fills in empty/zero values with AWS defaults.
func normalizeDBInstanceDefaults(
	engine, instanceClass string,
	allocatedStorage int,
	masterUser, region string,
	opts *DBInstanceOptions,
) (string, string, int, string) {
	if engine == "" {
		engine = "postgres"
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	if allocatedStorage <= 0 {
		allocatedStorage = defaultAllocatedStorage
	}
	if masterUser == "" {
		masterUser = "admin"
	}
	if opts.StorageType == "" {
		opts.StorageType = "gp2"
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = region + "a"
	}

	return engine, instanceClass, allocatedStorage, masterUser
}

// maybeRegisterAutomatedBackup registers an automated backup entry if retention > 0.
func (b *InMemoryBackend) maybeRegisterAutomatedBackup(
	id, engine string,
	port, allocatedStorage int,
	opts DBInstanceOptions,
) {
	if opts.BackupRetentionPeriod <= 0 {
		return
	}

	b.automatedBackups[id] = &DBInstanceAutomatedBackup{
		DBInstanceIdentifier:  id,
		DbiResourceID:         id,
		Engine:                engine,
		EngineVersion:         opts.EngineVersion,
		DBInstanceArn:         fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", b.region, b.accountID, id),
		Region:                b.region,
		Status:                instanceStatusAvailable,
		AllocatedStorage:      allocatedStorage,
		Port:                  port,
		BackupRetentionPeriod: opts.BackupRetentionPeriod,
		Encrypted:             opts.StorageEncrypted,
	}
}

func (b *InMemoryBackend) CreateDBInstance(
	id, engine, instanceClass, dbName, masterUser, paramGroupName string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBInstance")
	b.reconcileInstancesLocked()

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	engine, instanceClass, allocatedStorage, masterUser = normalizeDBInstanceDefaults(
		engine, instanceClass, allocatedStorage, masterUser, b.region, &opts,
	)

	port := enginePort(engine)
	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)

	vpcSGs := make([]VpcSecurityGroupMembership, 0, len(opts.VpcSecurityGroupIDs))
	for _, sgID := range opts.VpcSecurityGroupIDs {
		vpcSGs = append(vpcSGs, VpcSecurityGroupMembership{VpcSecurityGroupID: sgID, Status: subscriptionStatusActive})
	}

	inst := &DBInstance{
		InstanceCreateTime:               time.Now().UTC(),
		DBInstanceIdentifier:             id,
		DbiResourceID:                    id,
		DBInstanceClass:                  instanceClass,
		DBClusterIdentifier:              opts.DBClusterIdentifier,
		Engine:                           engine,
		EngineVersion:                    opts.EngineVersion,
		DBInstanceStatus:                 instanceStatusAvailable,
		MasterUsername:                   masterUser,
		DBName:                           dbName,
		Endpoint:                         endpoint,
		Port:                             port,
		AllocatedStorage:                 allocatedStorage,
		DBParameterGroupName:             paramGroupName,
		OptionGroupName:                  opts.OptionGroupName,
		MultiAZ:                          opts.MultiAZ,
		StorageType:                      opts.StorageType,
		StorageEncrypted:                 opts.StorageEncrypted,
		AvailabilityZone:                 opts.AvailabilityZone,
		BackupRetentionPeriod:            opts.BackupRetentionPeriod,
		IAMDatabaseAuthenticationEnabled: opts.IAMDatabaseAuthenticationEnabled,
		DeletionProtection:               opts.DeletionProtection,
		Iops:                             opts.Iops,
		StorageThroughput:                opts.StorageThroughput,
		LicenseModel:                     opts.LicenseModel,
		MonitoringInterval:               opts.MonitoringInterval,
		MonitoringRoleArn:                opts.MonitoringRoleArn,
		PreferredMaintenanceWindow:       opts.PreferredMaintenanceWindow,
		PreferredBackupWindow:            opts.PreferredBackupWindow,
		KmsKeyID:                         opts.KmsKeyID,
		CopyTagsToSnapshot:               opts.CopyTagsToSnapshot,
		EnabledCloudwatchLogsExports:     opts.EnabledCloudwatchLogsExports,
		VpcSecurityGroups:                vpcSGs,
		ReadReplicaIdentifiers:           []string{},
		PubliclyAccessible:               opts.PubliclyAccessible,
		PerformanceInsightsEnabled:       opts.PerformanceInsightsEnabled,
	}
	b.instances[id] = inst
	b.publishInstanceEventLocked(id, "DB instance created")

	// If joining a cluster, add this instance to the cluster's member list.
	if opts.DBClusterIdentifier != "" {
		if cluster, exists := b.clusters[opts.DBClusterIdentifier]; exists {
			cluster.DBClusterMembers = append(cluster.DBClusterMembers, DBClusterMember{
				DBInstanceIdentifier: id,
				IsClusterWriter:      len(cluster.DBClusterMembers) == 0,
			})
		}
	}
	b.maybeRegisterAutomatedBackup(id, engine, port, allocatedStorage, opts)
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// rdsARN constructs the ARN for an RDS resource.
// The format is: arn:aws:rds:{region}:{accountID}:{resourceType}:{id}.
func (b *InMemoryBackend) rdsARN(resourceType, id string) string {
	return fmt.Sprintf("arn:aws:rds:%s:%s:%s:%s", b.region, b.accountID, resourceType, id)
}

// DeleteDBInstance removes the DB instance with the given identifier.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("DeleteDBInstance")
	b.reconcileInstancesLocked()

	inst, exists := b.instances[id]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	if inst.DeletionProtection {
		b.mu.Unlock()

		return nil, fmt.Errorf(
			"%w: cannot delete protected DB Instance %s, disable deletion protection first",
			ErrInvalidDBInstanceState, id,
		)
	}

	if inst.DBInstanceStatus == instanceStatusDeleting {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s is already being deleted", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusDeleting
	b.publishInstanceEventLocked(id, "DB instance deletion started")
	cp := *inst
	b.publishInstanceEventLocked(id, "DB instance deleted")

	// Remove this instance from its source's ReadReplicaIdentifiers.
	if inst.ReplicaSourceDBInstanceIdentifier != "" {
		if src, srcExists := b.instances[inst.ReplicaSourceDBInstanceIdentifier]; srcExists {
			src.ReadReplicaIdentifiers = slices.DeleteFunc(src.ReadReplicaIdentifiers, func(s string) bool {
				return s == id
			})
		}
	}

	delete(b.instances, id)
	delete(b.tags, b.rdsARN("db", id))
	delete(b.instanceRoles, id)
	delete(b.instanceReadyAt, id)
	delete(b.automatedBackups, id)

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeDBInstances returns instances. If id is non-empty, returns only that instance.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")

	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			b.mu.RUnlock()

			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst
		b.mu.RUnlock()

		return []DBInstance{cp}, nil
	}

	instances := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		instances = append(instances, *inst)
	}
	b.mu.RUnlock()
	slices.SortFunc(instances, func(a, b DBInstance) int {
		if a.DBInstanceIdentifier < b.DBInstanceIdentifier {
			return -1
		}
		if a.DBInstanceIdentifier > b.DBInstanceIdentifier {
			return 1
		}

		return 0
	})

	return instances, nil
}

// ModifyDBInstance modifies properties of an existing DB instance.

// applyDBInstanceModifications applies non-zero/non-empty field values from opts to inst.

// applyParamGroupUpdate validates and applies a parameter group change.
func (b *InMemoryBackend) applyParamGroupUpdate(inst *DBInstance, paramGroupName string) error {
	if paramGroupName == "" {
		return nil
	}

	if _, pgExists := b.parameterGroups[paramGroupName]; !pgExists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, paramGroupName)
	}

	inst.DBParameterGroupName = paramGroupName

	return nil
}

// applyVpcSecurityGroups updates VPC security group memberships on an instance.
func applyVpcSecurityGroups(inst *DBInstance, sgIDs []string) {
	if len(sgIDs) == 0 {
		return
	}

	vpcSGs := make([]VpcSecurityGroupMembership, 0, len(sgIDs))
	for _, sgID := range sgIDs {
		vpcSGs = append(vpcSGs, VpcSecurityGroupMembership{VpcSecurityGroupID: sgID, Status: subscriptionStatusActive})
	}

	inst.VpcSecurityGroups = vpcSGs
}

// applyDBInstanceSchedulingOpts applies monitoring/maintenance window fields.
func applyDBInstanceSchedulingOpts(inst *DBInstance, opts DBInstanceOptions) {
	if opts.MonitoringInterval >= 0 && opts.MonitoringInterval != inst.MonitoringInterval {
		inst.MonitoringInterval = opts.MonitoringInterval
	}
	if opts.MonitoringRoleArn != "" {
		inst.MonitoringRoleArn = opts.MonitoringRoleArn
	}
	if opts.PreferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.PreferredBackupWindow != "" {
		inst.PreferredBackupWindow = opts.PreferredBackupWindow
	}
}

// applyDBInstanceFlags applies boolean flag fields from opts to inst.
// Fields with a corresponding *Set sentinel are applied unconditionally when the sentinel is true,
// allowing callers to explicitly set the flag to false (e.g., disable DeletionProtection).
func applyDBInstanceFlags(inst *DBInstance, opts DBInstanceOptions) {
	if opts.MultiAZSet {
		inst.MultiAZ = opts.MultiAZ
	} else if opts.MultiAZ {
		inst.MultiAZ = opts.MultiAZ
	}
	if opts.IAMDatabaseAuthSet {
		inst.IAMDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
	} else if opts.IAMDatabaseAuthenticationEnabled {
		inst.IAMDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
	}
	if opts.DeletionProtectionSet {
		inst.DeletionProtection = opts.DeletionProtection
	} else if opts.DeletionProtection {
		inst.DeletionProtection = opts.DeletionProtection
	}
	if opts.CopyTagsToSnapshot {
		inst.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.PubliclyAccessible {
		inst.PubliclyAccessible = opts.PubliclyAccessible
	}
	if opts.PerformanceInsightsEnabled {
		inst.PerformanceInsightsEnabled = opts.PerformanceInsightsEnabled
	}
	if opts.StorageOptimized {
		inst.StorageOptimized = true
	}
	if opts.OptimizedWrites {
		inst.OptimizedWrites = true
	}
	if opts.EngineLifecycleSupport != "" {
		inst.EngineLifecycleSupport = opts.EngineLifecycleSupport
	}
}

func (b *InMemoryBackend) applyDBInstanceModifications(
	inst *DBInstance,
	instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
) error {
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if allocatedStorage > 0 {
		inst.AllocatedStorage = allocatedStorage
	}
	if opts.StorageType != "" {
		inst.StorageType = opts.StorageType
	}
	if opts.BackupRetentionPeriod >= 0 {
		inst.BackupRetentionPeriod = opts.BackupRetentionPeriod
	}
	applyDBInstanceFlags(inst, opts)
	if err := b.applyParamGroupUpdate(inst, opts.DBParameterGroupName); err != nil {
		return err
	}

	if opts.OptionGroupName != "" {
		inst.OptionGroupName = opts.OptionGroupName
	}

	if opts.Iops > 0 {
		inst.Iops = opts.Iops
	}

	if opts.StorageThroughput > 0 {
		inst.StorageThroughput = opts.StorageThroughput
	}

	if opts.LicenseModel != "" {
		inst.LicenseModel = opts.LicenseModel
	}

	applyDBInstanceSchedulingOpts(inst, opts)

	applyVpcSecurityGroups(inst, opts.VpcSecurityGroupIDs)

	if len(opts.EnabledCloudwatchLogsExports) > 0 {
		inst.EnabledCloudwatchLogsExports = opts.EnabledCloudwatchLogsExports
	}

	if opts.EngineVersion != "" {
		inst.EngineVersion = opts.EngineVersion
	}

	return nil
}

func (b *InMemoryBackend) ModifyDBInstance(
	id, instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	if err := b.applyDBInstanceModifications(inst, instanceClass, allocatedStorage, opts); err != nil {
		return nil, err
	}
	inst.DBInstanceStatus = instanceStatusModifying
	b.instanceReadyAt[id] = time.Now().Add(instanceTransitionDelay)
	b.publishInstanceEventLocked(id, "DB instance modification started")
	cp := *inst

	return &cp, nil
}

// CreateDBSnapshot creates a snapshot of the given DB instance.
func (b *InMemoryBackend) CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	inst, exists := b.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     inst.AllocatedStorage,
		Port:                 inst.Port,
		StorageType:          inst.StorageType,
		StorageEncrypted:     inst.StorageEncrypted,
		SnapshotType:         "manual",
		OptionGroupName:      inst.OptionGroupName,
		PercentProgress:      percentProgressComplete,
	}
	if inst.StorageEncrypted {
		snap.KmsKeyID = inst.KmsKeyID
	}
	b.snapshots[snapshotID] = snap

	cp := *snap

	return &cp, nil
}

// DescribeDBSnapshots returns snapshots. If snapshotID is non-empty, returns only that snapshot.
func (b *InMemoryBackend) DescribeDBSnapshots(snapshotID string) ([]DBSnapshot, error) {
	b.mu.RLock("DescribeDBSnapshots")
	defer b.mu.RUnlock()

	if snapshotID != "" {
		snap, exists := b.snapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
		}

		return []DBSnapshot{*snap}, nil
	}

	snaps := make([]DBSnapshot, 0, len(b.snapshots))
	for _, snap := range b.snapshots {
		snaps = append(snaps, *snap)
	}
	slices.SortFunc(snaps, func(a, b DBSnapshot) int {
		if a.DBSnapshotIdentifier < b.DBSnapshotIdentifier {
			return -1
		}
		if a.DBSnapshotIdentifier > b.DBSnapshotIdentifier {
			return 1
		}

		return 0
	})

	return snaps, nil
}

// DeleteDBSnapshot removes the given snapshot.
func (b *InMemoryBackend) DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error) {
	b.mu.Lock("DeleteDBSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := *snap
	delete(b.snapshots, snapshotID)
	delete(b.tags, b.rdsARN("snapshot", snapshotID))

	return &cp, nil
}

// CopyDBSnapshot creates a copy of the given snapshot with a new identifier.
func (b *InMemoryBackend) CopyDBSnapshot(
	sourceSnapshotID, targetSnapshotID string,
	opts CopyDBSnapshotOptions,
) (*DBSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CopyDBSnapshot")
	defer b.mu.Unlock()

	src, exists := b.snapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, sourceSnapshotID)
	}
	if _, alreadyExists := b.snapshots[targetSnapshotID]; alreadyExists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, targetSnapshotID)
	}

	kmsKeyID := opts.KmsKeyID
	if kmsKeyID == "" && src.StorageEncrypted {
		kmsKeyID = src.KmsKeyID
	}

	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: targetSnapshotID,
		DBInstanceIdentifier: src.DBInstanceIdentifier,
		Engine:               src.Engine,
		EngineVersion:        src.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     src.AllocatedStorage,
		Port:                 src.Port,
		StorageType:          src.StorageType,
		StorageEncrypted:     src.StorageEncrypted,
		KmsKeyID:             kmsKeyID,
		SourceRegion:         opts.SourceRegion,
		OptionGroupName:      src.OptionGroupName,
		PercentProgress:      percentProgressComplete,
	}
	b.snapshots[targetSnapshotID] = snap
	cp := *snap

	return &cp, nil
}

// RestoreDBInstanceFromDBSnapshot creates a new DB instance from the given snapshot.
func (b *InMemoryBackend) RestoreDBInstanceFromDBSnapshot(
	id, snapshotID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreDBInstanceFromDBSnapshot")
	b.reconcileInstancesLocked()

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	if opts.StorageType == "" {
		opts.StorageType = snap.StorageType
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = b.region + "a"
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	port := snap.Port
	if port == 0 {
		port = enginePort(snap.Engine)
	}

	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		Engine:               snap.Engine,
		EngineVersion:        snap.EngineVersion,
		DBInstanceStatus:     instanceStatusAvailable,
		Endpoint:             endpoint,
		Port:                 port,
		AllocatedStorage:     snap.AllocatedStorage,
		StorageType:          opts.StorageType,
		StorageEncrypted:     snap.StorageEncrypted,
		AvailabilityZone:     opts.AvailabilityZone,
		MultiAZ:              opts.MultiAZ,
		DeletionProtection:   opts.DeletionProtection,
	}
	b.instances[id] = inst
	b.publishInstanceEventLocked(id, "DB instance restored from snapshot")
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// RestoreDBInstanceToPointInTime creates a new DB instance as a point-in-time restore of the source.
func (b *InMemoryBackend) RestoreDBInstanceToPointInTime(
	id, sourceID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TargetDBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceDBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreDBInstanceToPointInTime")
	b.reconcileInstancesLocked()

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	source, exists := b.instances[sourceID]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}

	if opts.StorageType == "" {
		opts.StorageType = source.StorageType
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = source.AvailabilityZone
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		DBInstanceClass:      source.DBInstanceClass,
		Engine:               source.Engine,
		EngineVersion:        source.EngineVersion,
		DBInstanceStatus:     instanceStatusAvailable,
		MasterUsername:       source.MasterUsername,
		DBName:               source.DBName,
		Endpoint:             endpoint,
		Port:                 source.Port,
		AllocatedStorage:     source.AllocatedStorage,
		DBParameterGroupName: source.DBParameterGroupName,
		StorageType:          opts.StorageType,
		StorageEncrypted:     source.StorageEncrypted,
		AvailabilityZone:     opts.AvailabilityZone,
		MultiAZ:              opts.MultiAZ,
		DeletionProtection:   opts.DeletionProtection,
	}
	b.instances[id] = inst
	b.publishInstanceEventLocked(id, "DB instance restored to point in time")
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// StartDBInstance starts a stopped DB instance.
func (b *InMemoryBackend) StartDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StartDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusStopped {
		return nil, fmt.Errorf("%w: instance %s is not in stopped state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusAvailable
	cp := *inst

	return &cp, nil
}

// StopDBInstance stops a running DB instance.
func (b *InMemoryBackend) StopDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StopDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusAvailable {
		return nil, fmt.Errorf("%w: instance %s is not in available state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusStopped
	cp := *inst

	return &cp, nil
}

// CreateDBSubnetGroup creates a DB subnet group.
func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}

	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)

	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		SubnetIDs:                ids,
		Status:                   "Complete",
	}
	b.subnetGroups[name] = sg

	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns subnet groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error) {
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}

		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)

		return []DBSubnetGroup{cp}, nil
	}

	sgs := make([]DBSubnetGroup, 0, len(b.subnetGroups))

	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		sgs = append(sgs, cp)
	}

	return sgs, nil
}

// DeleteDBSubnetGroup removes the given subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	delete(b.subnetGroups, name)
	delete(b.tags, b.rdsARN("subgrp", name))

	return nil
}

// AddTagsToResource adds or overwrites tags on the resource identified by arn.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	current := b.tags[arn]
	// Build an index for O(1) key lookup.
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}

	b.tags[arn] = current
}

// RemoveTagsFromResource removes the named tags from the resource identified by arn.
func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}

	current := b.tags[arn]
	kept := make([]Tag, 0, len(current))

	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}

	b.tags[arn] = kept
}

// ListTagsForResource returns the tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}

// CreateDBParameterGroup creates a new DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.parameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// copyDBParameterGroup returns a deep copy of the given parameter group.
func copyDBParameterGroup(pg *DBParameterGroup) DBParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]DBParameter, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return cp
}

// DescribeDBParameterGroups returns parameter groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeDBParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}
	slices.SortFunc(result, func(a, b DBParameterGroup) int {
		if a.DBParameterGroupName < b.DBParameterGroupName {
			return -1
		}
		if a.DBParameterGroupName > b.DBParameterGroupName {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteDBParameterGroup removes the given parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(name string) error {
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(b.parameterGroups, name)
	delete(b.tags, b.rdsARN("pg", name))

	return nil
}

// ModifyDBParameterGroup modifies parameters in a parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(name string, params []DBParameter) (*DBParameterGroup, error) {
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	for _, p := range params {
		pg.Parameters[p.ParameterName] = p
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DescribeDBParameters returns parameters for a parameter group.
func (b *InMemoryBackend) DescribeDBParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBParameters")
	defer b.mu.RUnlock()
	pg, exists := b.parameterGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	result := make([]DBParameter, 0, len(pg.Parameters))
	for _, p := range pg.Parameters {
		result = append(result, p)
	}
	slices.SortFunc(result, func(a, b DBParameter) int {
		if a.ParameterName < b.ParameterName {
			return -1
		}
		if a.ParameterName > b.ParameterName {
			return 1
		}

		return 0
	})

	return result, nil
}

// ResetDBParameterGroup resets parameters in a parameter group.
func (b *InMemoryBackend) ResetDBParameterGroup(
	name string,
	resetAll bool,
	params []string,
) (*DBParameterGroup, error) {
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if resetAll {
		for k, p := range pg.Parameters {
			p.ParameterValue = ""
			pg.Parameters[k] = p
		}
	} else {
		for _, pName := range params {
			if p, ok := pg.Parameters[pName]; ok {
				p.ParameterValue = ""
				pg.Parameters[pName] = p
			}
		}
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// CreateOptionGroup creates a new option group.
func (b *InMemoryBackend) CreateOptionGroup(name, engine, majorVersion, description string) (*OptionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: OptionGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; exists {
		return nil, fmt.Errorf("%w: option group %s already exists", ErrOptionGroupAlreadyExists, name)
	}
	og := &OptionGroup{
		OptionGroupName:        name,
		OptionGroupDescription: description,
		EngineName:             engine,
		MajorEngineVersion:     majorVersion,
		Options:                []OptionGroupOption{},
	}
	b.optionGroups[name] = og
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// DescribeOptionGroups returns option groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeOptionGroups(name string) ([]OptionGroup, error) {
	b.mu.RLock("DescribeOptionGroups")
	defer b.mu.RUnlock()
	if name != "" {
		og, exists := b.optionGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
		}
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)

		return []OptionGroup{cp}, nil
	}
	result := make([]OptionGroup, 0, len(b.optionGroups))
	for _, og := range b.optionGroups {
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)
		result = append(result, cp)
	}
	slices.SortFunc(result, func(a, b OptionGroup) int {
		if a.OptionGroupName < b.OptionGroupName {
			return -1
		}
		if a.OptionGroupName > b.OptionGroupName {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteOptionGroup removes the given option group.
func (b *InMemoryBackend) DeleteOptionGroup(name string) error {
	b.mu.Lock("DeleteOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; !exists {
		return fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	delete(b.optionGroups, name)
	delete(b.tags, b.rdsARN("og", name))

	return nil
}

// ModifyOptionGroup modifies an option group by adding/removing options.
func (b *InMemoryBackend) ModifyOptionGroup(
	name string,
	optionsToAdd []OptionGroupOption,
	optionsToRemove []string,
) (*OptionGroup, error) {
	b.mu.Lock("ModifyOptionGroup")
	defer b.mu.Unlock()
	og, exists := b.optionGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	removeSet := make(map[string]bool, len(optionsToRemove))
	for _, o := range optionsToRemove {
		removeSet[o] = true
	}
	kept := make([]OptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		if !removeSet[o.OptionName] {
			kept = append(kept, o)
		}
	}
	kept = append(kept, optionsToAdd...)
	og.Options = kept
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// CreateDBCluster creates a new DB cluster.
func (b *InMemoryBackend) CreateDBCluster(
	id, engine, masterUser, dbName, paramGroupName string,
	port int,
	serverlessV2Cfg *ServerlessV2ScalingConfiguration,
	opts DBClusterOptions,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = "aurora-postgresql"
	}
	if paramGroupName == "" {
		paramGroupName = "default." + engine
	}
	if port <= 0 {
		port = enginePort(engine)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	networkType := opts.NetworkType
	if networkType == "" {
		networkType = "IPV4"
	}
	cluster := &DBCluster{
		ClusterCreateTime:            time.Now().UTC(),
		DBClusterIdentifier:          id,
		Engine:                       engine,
		EngineVersion:                opts.EngineVersion,
		Status:                       instanceStatusAvailable,
		MasterUsername:               masterUser,
		DatabaseName:                 dbName,
		DBClusterParameterGroupName:  paramGroupName,
		Endpoint:                     endpoint,
		ReaderEndpoint:               readerEndpoint,
		NetworkType:                  networkType,
		StorageType:                  opts.StorageType,
		EngineLifecycleSupport:       opts.EngineLifecycleSupport,
		OptimizedWrites:              opts.OptimizedWrites,
		Port:                         port,
		ServerlessV2ScalingConfig:    serverlessV2Cfg,
		KmsKeyID:                     opts.KmsKeyID,
		PreferredBackupWindow:        opts.PreferredBackupWindow,
		PreferredMaintenanceWindow:   opts.PreferredMaintenanceWindow,
		MonitoringRoleArn:            opts.MonitoringRoleArn,
		EnabledCloudwatchLogsExports: opts.EnabledCloudwatchLogsExports,
		AvailabilityZones:            opts.AvailabilityZones,
		BacktrackWindow:              opts.BacktrackWindow,
		MonitoringInterval:           opts.MonitoringInterval,
		MultiAZ:                      opts.MultiAZ,
		StorageEncrypted:             opts.StorageEncrypted,
		CopyTagsToSnapshot:           opts.CopyTagsToSnapshot,
		DeletionProtection:           opts.DeletionProtection,
		DBClusterMembers:             []DBClusterMember{},
	}
	b.clusters[id] = cluster
	cp := *cluster

	return &cp, nil
}

// DescribeDBClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		cluster, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}
		cp := *cluster

		return []DBCluster{cp}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, cluster := range b.clusters {
		result = append(result, *cluster)
	}
	slices.SortFunc(result, func(a, b DBCluster) int {
		if a.DBClusterIdentifier < b.DBClusterIdentifier {
			return -1
		}
		if a.DBClusterIdentifier > b.DBClusterIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteDBCluster removes the given cluster.
func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := *cluster
	// Clear the cluster association on any member instances so they appear standalone.
	for _, member := range cluster.DBClusterMembers {
		if inst, ok := b.instances[member.DBInstanceIdentifier]; ok {
			inst.DBClusterIdentifier = ""
		}
	}
	delete(b.clusters, id)
	delete(b.tags, b.rdsARN("cluster", id))
	delete(b.fisFailoverFaults, id)
	delete(b.clusterRoles, id)

	return &cp, nil
}

// applyDBClusterOpts applies DBClusterOptions fields to a cluster in-place.
func applyDBClusterOpts(cluster *DBCluster, paramGroupName string, opts DBClusterOptions) {
	if paramGroupName != "" {
		cluster.DBClusterParameterGroupName = paramGroupName
	}
	if opts.EngineVersion != "" {
		cluster.EngineVersion = opts.EngineVersion
	}
	if opts.KmsKeyID != "" {
		cluster.KmsKeyID = opts.KmsKeyID
	}
	if opts.PreferredBackupWindow != "" {
		cluster.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.PreferredMaintenanceWindow != "" {
		cluster.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.MonitoringRoleArn != "" {
		cluster.MonitoringRoleArn = opts.MonitoringRoleArn
	}
	if opts.BacktrackWindow > 0 {
		cluster.BacktrackWindow = opts.BacktrackWindow
	}
	if opts.MonitoringInterval >= 0 {
		cluster.MonitoringInterval = opts.MonitoringInterval
	}
	if opts.MultiAZ {
		cluster.MultiAZ = opts.MultiAZ
	}
	if opts.StorageEncryptedChanged {
		cluster.StorageEncrypted = opts.StorageEncrypted
	}
	if opts.CopyTagsToSnapshot {
		cluster.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.DeletionProtectionSet {
		cluster.DeletionProtection = opts.DeletionProtection
	} else if opts.DeletionProtection {
		cluster.DeletionProtection = true
	}
	if len(opts.EnabledCloudwatchLogsExports) > 0 {
		cluster.EnabledCloudwatchLogsExports = opts.EnabledCloudwatchLogsExports
	}
	if opts.StorageType != "" {
		cluster.StorageType = opts.StorageType
	}
	if opts.NetworkType != "" {
		cluster.NetworkType = opts.NetworkType
	}
	if opts.EngineLifecycleSupport != "" {
		cluster.EngineLifecycleSupport = opts.EngineLifecycleSupport
	}
	if opts.OptimizedWrites {
		cluster.OptimizedWrites = true
	}
}

// ModifyDBCluster modifies a DB cluster.
func (b *InMemoryBackend) ModifyDBCluster(id, paramGroupName string, opts DBClusterOptions) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	applyDBClusterOpts(cluster, paramGroupName, opts)
	cp := *cluster

	return &cp, nil
}

// CreateDBClusterParameterGroup creates a new cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns cluster parameter groups.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}
	slices.SortFunc(result, func(a, b DBParameterGroup) int {
		if a.DBParameterGroupName < b.DBParameterGroupName {
			return -1
		}
		if a.DBParameterGroupName > b.DBParameterGroupName {
			return 1
		}

		return 0
	})

	return result, nil
}

// CreateDBClusterSnapshot creates a snapshot of the given cluster.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		SnapshotCreateTime:          time.Now().UTC(),
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         clusterID,
		Engine:                      cluster.Engine,
		EngineVersion:               cluster.EngineVersion,
		Status:                      instanceStatusAvailable,
		PercentProgress:             percentProgressComplete,
		StorageEncrypted:            cluster.StorageEncrypted,
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshots returns cluster snapshots.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID string) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(b.clusterSnapshots))
	for _, snap := range b.clusterSnapshots {
		result = append(result, *snap)
	}
	slices.SortFunc(result, func(a, b DBClusterSnapshot) int {
		if a.DBClusterSnapshotIdentifier < b.DBClusterSnapshotIdentifier {
			return -1
		}
		if a.DBClusterSnapshotIdentifier > b.DBClusterSnapshotIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// CreateDBInstanceReadReplica creates a read replica of the given source instance.
// sourceRegion is optional; when non-empty it indicates a cross-region replica.
func (b *InMemoryBackend) CreateDBInstanceReadReplica(id, sourceID, sourceRegion string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstanceReadReplica")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	var (
		instanceClass    string
		engine           string
		engineVersion    string
		masterUser       string
		port             int
		allocatedStorage int
	)

	source, sourceExists := b.instances[sourceID]
	switch {
	case sourceExists:
		instanceClass = source.DBInstanceClass
		engine = source.Engine
		engineVersion = source.EngineVersion
		masterUser = source.MasterUsername
		port = source.Port
		allocatedStorage = source.AllocatedStorage
	case sourceRegion != "":
		// Cross-region replica: source instance lives in another region.
		// Use defaults; the caller should supply a valid ARN as sourceID.
		engine = enginePostgres
		port = defaultPort
		allocatedStorage = defaultAllocatedStorage
	default:
		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	replica := &DBInstance{
		DBInstanceIdentifier:              id,
		DbiResourceID:                     id,
		DBInstanceClass:                   instanceClass,
		Engine:                            engine,
		EngineVersion:                     engineVersion,
		DBInstanceStatus:                  instanceStatusAvailable,
		MasterUsername:                    masterUser,
		Endpoint:                          endpoint,
		Port:                              port,
		AllocatedStorage:                  allocatedStorage,
		ReplicaSourceDBInstanceIdentifier: sourceID,
	}
	b.instances[id] = replica
	b.publishInstanceEventLocked(id, "DB read replica created")

	// Track reverse read replica reference on source instance.
	if source != nil {
		source.ReadReplicaIdentifiers = append(source.ReadReplicaIdentifiers, id)
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *replica

	return &cp, nil
}

// DescribeDBInstanceAutomatedBackups returns automated backup records for instances.
// If instanceID is non-empty, filters to that instance.
func (b *InMemoryBackend) DescribeDBInstanceAutomatedBackups(instanceID string) []DBInstanceAutomatedBackup {
	b.mu.RLock("DescribeDBInstanceAutomatedBackups")
	defer b.mu.RUnlock()

	result := make([]DBInstanceAutomatedBackup, 0, len(b.automatedBackups))
	for _, ab := range b.automatedBackups {
		if instanceID != "" && ab.DBInstanceIdentifier != instanceID {
			continue
		}
		result = append(result, *ab)
	}

	return result
}

// PromoteReadReplica promotes a read replica to a standalone instance.
func (b *InMemoryBackend) PromoteReadReplica(id string) (*DBInstance, error) {
	b.mu.Lock("PromoteReadReplica")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	// Remove promoted instance from source's ReadReplicaIdentifiers.
	if inst.ReplicaSourceDBInstanceIdentifier != "" {
		if src, srcExists := b.instances[inst.ReplicaSourceDBInstanceIdentifier]; srcExists {
			src.ReadReplicaIdentifiers = slices.DeleteFunc(src.ReadReplicaIdentifiers, func(s string) bool {
				return s == id
			})
		}
	}

	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots the given instance.
func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.DBInstanceStatus = instanceStatusAvailable
	delete(b.instanceReadyAt, id)
	b.publishInstanceEventLocked(id, "DB instance reboot completed")
	cp := *inst

	return &cp, nil
}

// CreateCustomDBEngineVersion creates a custom DB engine version.
func (b *InMemoryBackend) CreateCustomDBEngineVersion(
	engine, engineVersion, description string,
) (*CustomDBEngineVersion, error) {
	if engine == "" {
		return nil, fmt.Errorf("%w: Engine is required", ErrInvalidParameter)
	}
	if engineVersion == "" {
		return nil, fmt.Errorf("%w: EngineVersion is required", ErrInvalidParameter)
	}

	key := engine + ":" + engineVersion
	b.mu.Lock("CreateCustomDBEngineVersion")
	defer b.mu.Unlock()

	if _, exists := b.customEngineVersions[key]; exists {
		return nil, fmt.Errorf(
			"%w: custom engine version %s/%s already exists",
			ErrInstanceAlreadyExists,
			engine,
			engineVersion,
		)
	}

	cev := &CustomDBEngineVersion{
		Engine:        engine,
		EngineVersion: engineVersion,
		Status:        instanceStatusAvailable,
		Description:   description,
	}
	b.customEngineVersions[key] = cev
	cp := *cev

	return &cp, nil
}

// DeleteCustomDBEngineVersion deletes a custom DB engine version.
func (b *InMemoryBackend) DeleteCustomDBEngineVersion(engine, engineVersion string) (*CustomDBEngineVersion, error) {
	key := engine + ":" + engineVersion
	b.mu.Lock("DeleteCustomDBEngineVersion")
	defer b.mu.Unlock()

	cev, exists := b.customEngineVersions[key]
	if !exists {
		return nil, fmt.Errorf("%w: custom engine version %s/%s not found", ErrInstanceNotFound, engine, engineVersion)
	}

	cp := *cev
	cp.Status = instanceStatusDeleting
	delete(b.customEngineVersions, key)

	return &cp, nil
}

// ModifyCustomDBEngineVersion modifies a custom DB engine version.
func (b *InMemoryBackend) ModifyCustomDBEngineVersion(
	engine, engineVersion, description, status string,
) (*CustomDBEngineVersion, error) {
	key := engine + ":" + engineVersion
	b.mu.Lock("ModifyCustomDBEngineVersion")
	defer b.mu.Unlock()

	cev, exists := b.customEngineVersions[key]
	if !exists {
		return nil, fmt.Errorf("%w: custom engine version %s/%s not found", ErrInstanceNotFound, engine, engineVersion)
	}

	if description != "" {
		cev.Description = description
	}
	if status != "" {
		cev.Status = status
	}

	cp := *cev

	return &cp, nil
}

// DescribeDBEngineVersions returns available engine versions, filtered by engine and/or version.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: enginePostgres, EngineVersion: "14.10", DBEngineDescription: "PostgreSQL 14.10"},
		{Engine: enginePostgres, EngineVersion: "15.5", DBEngineDescription: "PostgreSQL 15.5"},
		{Engine: engineMySQL, EngineVersion: "8.0.35", DBEngineDescription: "MySQL 8.0.35"},
		{Engine: engineMariaDB, EngineVersion: "10.6.14", DBEngineDescription: "MariaDB 10.6.14"},
		{Engine: engineAuroraMySQL, EngineVersion: "3.04.0", DBEngineDescription: "Aurora MySQL 3.04.0"},
		{Engine: engineAuroraPostgresql, EngineVersion: "14.9", DBEngineDescription: "Aurora PostgreSQL 14.9"},
		{Engine: engineAuroraPostgresql, EngineVersion: "15.4", DBEngineDescription: "Aurora PostgreSQL 15.4"},
	}
	if engine == "" && engineVersion == "" {
		return all
	}
	result := make([]DBEngineVersion, 0, len(all))
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}

// DescribeOrderableDBInstanceOptions returns orderable instance options for the given engine.
func (b *InMemoryBackend) DescribeOrderableDBInstanceOptions(engine, engineVersion string) []OrderableDBInstanceOption {
	classes := []string{defaultInstanceClass, "db.t3.small", "db.t3.medium", "db.r5.large", "db.r5.xlarge"}
	if engine == "" {
		engine = "postgres"
	}
	versions := b.DescribeDBEngineVersions(engine, engineVersion)
	if len(versions) == 0 {
		versions = []DBEngineVersion{{Engine: engine, EngineVersion: engineVersion}}
	}
	result := make([]OrderableDBInstanceOption, 0, len(classes)*len(versions))
	for _, v := range versions {
		for _, class := range classes {
			result = append(result, OrderableDBInstanceOption{
				Engine:          v.Engine,
				EngineVersion:   v.EngineVersion,
				DBInstanceClass: class,
				MultiAZCapable:  true,
			})
		}
	}

	return result
}

// DescribeDBLogFiles returns the log files for the given instance.
func (b *InMemoryBackend) DescribeDBLogFiles(instanceID string) ([]DBLogFile, error) {
	b.mu.RLock("DescribeDBLogFiles")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return []DBLogFile{}, nil
}

// DownloadDBLogFilePortion returns log file content for the given instance.
func (b *InMemoryBackend) DownloadDBLogFilePortion(instanceID, _ string) (string, error) {
	b.mu.RLock("DownloadDBLogFilePortion")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return "", fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return "", nil
}

// StartDBCluster starts a stopped DB cluster.
func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cluster.Status = instanceStatusAvailable
	cp := *cluster

	return &cp, nil
}

// StopDBCluster stops a running DB cluster.
func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cluster.Status = "stopped"
	cp := *cluster

	return &cp, nil
}

// DeleteDBClusterSnapshot removes the given cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	delete(b.clusterSnapshots, snapshotID)
	delete(b.tags, b.rdsARN("cluster-snapshot", snapshotID))

	return &cp, nil
}

// RestoreDBClusterFromSnapshot creates a new DB cluster from the given snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(clusterID, snapshotID, engine string) (*DBCluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if engine == "" {
		engine = snap.Engine
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", clusterID, b.accountID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		Engine:                      engine,
		Status:                      instanceStatusAvailable,
		DBClusterParameterGroupName: "default." + engine,
		Endpoint:                    endpoint,
		Port:                        enginePort(engine),
	}
	b.clusters[clusterID] = cluster
	cp := *cluster

	return &cp, nil
}

// RestoreDBClusterToPointInTime creates a new DB cluster as a point-in-time restore of the source cluster.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(clusterID, sourceClusterID string) (*DBCluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if sourceClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	source, exists := b.clusters[sourceClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: source cluster %s not found", ErrClusterNotFound, sourceClusterID)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", clusterID, b.accountID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		Engine:                      source.Engine,
		Status:                      instanceStatusAvailable,
		MasterUsername:              source.MasterUsername,
		DatabaseName:                source.DatabaseName,
		DBClusterParameterGroupName: source.DBClusterParameterGroupName,
		Endpoint:                    endpoint,
		Port:                        source.Port,
	}
	b.clusters[clusterID] = cluster
	cp := *cluster

	return &cp, nil
}

// CopyDBClusterSnapshot creates a copy of the given cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	source, srcExists := b.clusterSnapshots[sourceSnapshotID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, dstExists := b.clusterSnapshots[targetSnapshotID]; dstExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterIdentifier:         source.DBClusterIdentifier,
		Engine:                      source.Engine,
		Status:                      instanceStatusAvailable,
	}
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap

	return &cp, nil
}

// CreateDBClusterEndpoint creates a custom endpoint for the given cluster.
func (b *InMemoryBackend) CreateDBClusterEndpoint(
	endpointID, clusterID, endpointType string,
) (*DBClusterEndpoint, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: DBClusterEndpointIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterEndpoint")
	defer b.mu.Unlock()
	if _, exists := b.clusterEndpoints[endpointID]; exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s already exists", ErrClusterEndpointAlreadyExists, endpointID)
	}
	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if endpointType == "" {
		endpointType = "ANY"
	}
	ep := &DBClusterEndpoint{
		DBClusterEndpointIdentifier: endpointID,
		DBClusterIdentifier:         clusterID,
		EndpointType:                endpointType,
		Status:                      instanceStatusAvailable,
		Endpoint: fmt.Sprintf(
			"%s.cluster-custom.%s.%s.rds.amazonaws.com",
			endpointID,
			b.accountID,
			b.region,
		),
	}
	b.clusterEndpoints[endpointID] = ep
	cp := *ep

	return &cp, nil
}

// DescribeDBClusterEndpoints returns cluster endpoints, filtered by cluster or endpoint ID.
func (b *InMemoryBackend) DescribeDBClusterEndpoints(clusterID, endpointID string) ([]DBClusterEndpoint, error) {
	b.mu.RLock("DescribeDBClusterEndpoints")
	defer b.mu.RUnlock()
	if endpointID != "" {
		ep, exists := b.clusterEndpoints[endpointID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
		}
		cp := *ep

		return []DBClusterEndpoint{cp}, nil
	}
	result := make([]DBClusterEndpoint, 0, len(b.clusterEndpoints))
	for _, ep := range b.clusterEndpoints {
		if clusterID != "" && ep.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *ep)
	}

	return result, nil
}

// DeleteDBClusterEndpoint removes the given custom cluster endpoint.
func (b *InMemoryBackend) DeleteDBClusterEndpoint(endpointID string) (*DBClusterEndpoint, error) {
	b.mu.Lock("DeleteDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpoints[endpointID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	cp := *ep
	delete(b.clusterEndpoints, endpointID)
	delete(b.tags, b.rdsARN("cluster-endpoint", endpointID))

	return &cp, nil
}

// DescribeValidDBInstanceModifications returns the valid modifications for the given instance.
// This is a stub that returns a minimal set of valid instance classes.
func (b *InMemoryBackend) DescribeValidDBInstanceModifications(id string) (*DBInstance, error) {
	b.mu.RLock("DescribeValidDBInstanceModifications")
	defer b.mu.RUnlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

// StartExportTask creates a new export task for the given source ARN.
func (b *InMemoryBackend) StartExportTask(taskID, sourceARN, s3Bucket string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StartExportTask")
	defer b.mu.Unlock()
	if _, exists := b.exportTasks[taskID]; exists {
		return nil, fmt.Errorf("%w: export task %s already exists", ErrExportTaskAlreadyExists, taskID)
	}
	task := &ExportTask{
		ExportTaskIdentifier: taskID,
		SourceArn:            sourceARN,
		Status:               "complete",
		S3Bucket:             s3Bucket,
	}
	b.exportTasks[taskID] = task
	cp := *task

	return &cp, nil
}

// DescribeExportTasks returns export tasks, optionally filtered by task ID.
func (b *InMemoryBackend) DescribeExportTasks(taskID string) ([]ExportTask, error) {
	b.mu.RLock("DescribeExportTasks")
	defer b.mu.RUnlock()
	if taskID != "" {
		task, exists := b.exportTasks[taskID]
		if !exists {
			return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
		}
		cp := *task

		return []ExportTask{cp}, nil
	}
	result := make([]ExportTask, 0, len(b.exportTasks))
	for _, task := range b.exportTasks {
		result = append(result, *task)
	}

	return result, nil
}

// CancelExportTask cancels and removes the export task with the given identifier.
func (b *InMemoryBackend) CancelExportTask(taskID string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CancelExportTask")
	defer b.mu.Unlock()
	task, exists := b.exportTasks[taskID]
	if !exists {
		return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
	}
	task.Status = "canceled"
	cp := *task
	delete(b.exportTasks, taskID)

	return &cp, nil
}

// CreateGlobalCluster creates a new global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	id, engine, engineVersion string,
	storageEncrypted, deletionProtection bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()

	if _, exists := b.globalClusters[id]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}

	if engine == "" {
		engine = "aurora-postgresql"
	}

	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		Engine:                  engine,
		EngineVersion:           engineVersion,
		Status:                  instanceStatusAvailable,
		StorageEncrypted:        storageEncrypted,
		DeletionProtection:      deletionProtection,
	}
	b.globalClusters[id] = gc
	cp := *gc

	return &cp, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(id string) ([]GlobalCluster, error) {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()

	if id != "" {
		gc, exists := b.globalClusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
		}
		cp := *gc

		return []GlobalCluster{cp}, nil
	}

	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		result = append(result, *gc)
	}

	return result, nil
}

// DeleteGlobalCluster removes the given global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(id string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	if gc.DeletionProtection {
		return nil, fmt.Errorf(
			"%w: cannot delete protected global cluster %s, disable deletion protection first",
			ErrInvalidGlobalClusterState, id,
		)
	}

	cp := *gc
	delete(b.globalClusters, id)

	return &cp, nil
}

// ModifyGlobalCluster modifies properties of a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	id, newGlobalClusterID, engineVersion string,
	deletionProtection *bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	if newGlobalClusterID != "" && newGlobalClusterID != id {
		if _, alreadyExists := b.globalClusters[newGlobalClusterID]; alreadyExists {
			return nil, fmt.Errorf(
				"%w: global cluster %s already exists",
				ErrGlobalClusterAlreadyExists,
				newGlobalClusterID,
			)
		}
		delete(b.globalClusters, id)
		gc.GlobalClusterIdentifier = newGlobalClusterID
		b.globalClusters[newGlobalClusterID] = gc
	}
	if engineVersion != "" {
		gc.EngineVersion = engineVersion
	}
	if deletionProtection != nil {
		gc.DeletionProtection = *deletionProtection
	}

	cp := *gc

	return &cp, nil
}

// AddRoleToDBCluster associates an IAM role with the given DB cluster.
func (b *InMemoryBackend) AddRoleToDBCluster(clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AddRoleToDBCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if slices.Contains(b.clusterRoles[clusterID], roleARN) {
		return nil
	}

	b.clusterRoles[clusterID] = append(b.clusterRoles[clusterID], roleARN)

	return nil
}

// AddRoleToDBInstance associates an IAM role with the given DB instance.
func (b *InMemoryBackend) AddRoleToDBInstance(instanceID, roleARN string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AddRoleToDBInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[instanceID]; !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	if slices.Contains(b.instanceRoles[instanceID], roleARN) {
		return nil
	}

	b.instanceRoles[instanceID] = append(b.instanceRoles[instanceID], roleARN)

	return nil
}

// AddSourceIdentifierToSubscription adds a source identifier to an event notification subscription.
// If the subscription does not exist it is created automatically.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	subscriptionName, sourceIdentifier string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName must not be empty", ErrInvalidParameter)
	}
	if sourceIdentifier == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()

	sub, exists := b.eventSubscriptions[subscriptionName]
	if !exists {
		sub = &EventSubscription{
			SubscriptionName: subscriptionName,
			Status:           subscriptionStatusActive,
			SourceIDs:        []string{},
		}
		b.eventSubscriptions[subscriptionName] = sub
	}

	if !slices.Contains(sub.SourceIDs, sourceIdentifier) {
		sub.SourceIDs = append(sub.SourceIDs, sourceIdentifier)
	}

	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
// The resource is identified by its ARN. This implementation validates the resource exists
// and returns a stub response.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	resourceID, applyAction string,
) (string, error) {
	if resourceID == "" {
		return "", fmt.Errorf("%w: ResourceIdentifier must not be empty", ErrInvalidParameter)
	}
	if applyAction == "" {
		return "", fmt.Errorf("%w: ApplyAction must not be empty", ErrInvalidParameter)
	}

	b.mu.RLock("ApplyPendingMaintenanceAction")
	defer b.mu.RUnlock()

	id := rdsIDFromARN(resourceID)

	// Validate that the referenced resource exists (instance or cluster).
	if _, ok := b.instances[id]; !ok {
		if _, ok2 := b.clusters[id]; !ok2 {
			return "", fmt.Errorf("%w: resource %s not found", ErrInstanceNotFound, resourceID)
		}
	}

	return resourceID, nil
}

// AuthorizeDBSecurityGroupIngress authorizes CIDR IP access to a DB security group.
// If the group does not exist it is created automatically (matching AWS behaviour for legacy VPC-less accounts).
func (b *InMemoryBackend) AuthorizeDBSecurityGroupIngress(
	groupName, cidrIP string,
) (*DBSecurityGroup, error) {
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBSecurityGroupName must not be empty", ErrInvalidParameter)
	}
	if cidrIP == "" {
		return nil, fmt.Errorf("%w: CIDRIP must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeDBSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, exists := b.dbSecurityGroups[groupName]
	if !exists {
		sg = &DBSecurityGroup{
			DBSecurityGroupName: groupName,
			IPRanges:            []IPRange{},
		}
		b.dbSecurityGroups[groupName] = sg
	}

	for _, r := range sg.IPRanges {
		if r.CIDRIP == cidrIP {
			cp := *sg
			cp.IPRanges = make([]IPRange, len(sg.IPRanges))
			copy(cp.IPRanges, sg.IPRanges)

			return &cp, nil
		}
	}

	sg.IPRanges = append(sg.IPRanges, IPRange{CIDRIP: cidrIP, Status: ipRangeStatusAuthorized})

	cp := *sg
	cp.IPRanges = make([]IPRange, len(sg.IPRanges))
	copy(cp.IPRanges, sg.IPRanges)

	return &cp, nil
}

// BacktrackDBCluster backtracks an Aurora DB cluster to a specific time.
func (b *InMemoryBackend) BacktrackDBCluster(
	clusterID, backtrackTo string,
) (*DBClusterBacktrack, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if backtrackTo == "" {
		return nil, fmt.Errorf("%w: BacktrackTo must not be empty", ErrInvalidParameter)
	}

	b.mu.RLock("BacktrackDBCluster")
	defer b.mu.RUnlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	result := &DBClusterBacktrack{
		DBClusterIdentifier: clusterID,
		BacktrackIdentifier: newBacktrackID(),
		BacktrackTo:         backtrackTo,
		Status:              backtrackStatusApplying,
	}

	return result, nil
}

// CopyDBClusterParameterGroup creates a copy of the source cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*DBParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBClusterParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBClusterParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()

	src, exists := b.clusterParameterGroups[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrParameterGroupNotFound,
			sourceGroupName,
		)
	}

	if _, alreadyExists := b.clusterParameterGroups[targetGroupName]; alreadyExists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			targetGroupName,
		)
	}

	pg := copyParameterGroupTo(src, targetGroupName, targetDescription)
	b.clusterParameterGroups[targetGroupName] = pg

	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// CopyDBParameterGroup creates a copy of the source parameter group.
func (b *InMemoryBackend) CopyDBParameterGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*DBParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBParameterGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyDBParameterGroup")
	defer b.mu.Unlock()

	src, exists := b.parameterGroups[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf(
			"%w: parameter group %s not found",
			ErrParameterGroupNotFound,
			sourceGroupName,
		)
	}

	if _, alreadyExists := b.parameterGroups[targetGroupName]; alreadyExists {
		return nil, fmt.Errorf(
			"%w: parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			targetGroupName,
		)
	}

	pg := copyParameterGroupTo(src, targetGroupName, targetDescription)
	b.parameterGroups[targetGroupName] = pg

	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// copyParameterGroupTo returns a new DBParameterGroup that is a copy of src with the given
// target name and description. The caller is responsible for storing it in the appropriate map.
func copyParameterGroupTo(src *DBParameterGroup, targetName, targetDescription string) *DBParameterGroup {
	if targetDescription == "" {
		targetDescription = src.Description
	}

	pg := &DBParameterGroup{
		DBParameterGroupName:   targetName,
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            targetDescription,
		Parameters:             make(map[string]DBParameter, len(src.Parameters)),
	}
	maps.Copy(pg.Parameters, src.Parameters)

	return pg
}

// CopyOptionGroup creates a copy of the source option group.
func (b *InMemoryBackend) CopyOptionGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*OptionGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceOptionGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetOptionGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyOptionGroup")
	defer b.mu.Unlock()

	src, exists := b.optionGroups[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, sourceGroupName)
	}

	if _, alreadyExists := b.optionGroups[targetGroupName]; alreadyExists {
		return nil, fmt.Errorf(
			"%w: option group %s already exists",
			ErrOptionGroupAlreadyExists,
			targetGroupName,
		)
	}

	if targetDescription == "" {
		targetDescription = src.OptionGroupDescription
	}

	opts := make([]OptionGroupOption, len(src.Options))
	copy(opts, src.Options)

	og := &OptionGroup{
		OptionGroupName:        targetGroupName,
		OptionGroupDescription: targetDescription,
		EngineName:             src.EngineName,
		MajorEngineVersion:     src.MajorEngineVersion,
		Options:                opts,
	}
	b.optionGroups[targetGroupName] = og

	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// CreateBlueGreenDeployment creates a new Blue/Green Deployment.
func (b *InMemoryBackend) CreateBlueGreenDeployment(
	name, source string,
) (*BlueGreenDeployment, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: BlueGreenDeploymentName must not be empty", ErrInvalidParameter)
	}
	if source == "" {
		return nil, fmt.Errorf("%w: Source must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateBlueGreenDeployment")
	defer b.mu.Unlock()

	id := "bgd-" + name

	if _, exists := b.blueGreenDeployments[id]; exists {
		return nil, fmt.Errorf(
			"%w: Blue/Green Deployment %s already exists",
			ErrBlueGreenDeploymentAlreadyExists,
			name,
		)
	}

	target := source + "-green"
	deployment := &BlueGreenDeployment{
		BlueGreenDeploymentIdentifier: id,
		BlueGreenDeploymentName:       name,
		Source:                        source,
		Target:                        target,
		Status:                        blueGreenDeploymentStatusAvailable,
	}
	b.blueGreenDeployments[id] = deployment

	cp := *deployment

	return &cp, nil
}

// RemoveRoleFromDBCluster disassociates an IAM role from the given cluster.
// Returns an error if the cluster does not exist. Removing a role that is not associated is a no-op.
func (b *InMemoryBackend) RemoveRoleFromDBCluster(clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveRoleFromDBCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	roles := b.clusterRoles[clusterID]
	idx := slices.Index(roles, roleARN)
	if idx >= 0 {
		b.clusterRoles[clusterID] = slices.Delete(roles, idx, idx+1)
	}

	return nil
}

// RemoveRoleFromDBInstance disassociates an IAM role from the given instance.
// Returns an error if the instance does not exist. Removing a role that is not associated is a no-op.
func (b *InMemoryBackend) RemoveRoleFromDBInstance(instanceID, roleARN string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveRoleFromDBInstance")
	defer b.mu.Unlock()

	if _, exists := b.instances[instanceID]; !exists {
		return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	roles := b.instanceRoles[instanceID]
	idx := slices.Index(roles, roleARN)
	if idx >= 0 {
		b.instanceRoles[instanceID] = slices.Delete(roles, idx, idx+1)
	}

	return nil
}

// RemoveSourceIdentifierFromSubscription removes a source identifier from an event subscription.
// Returns an error if the subscription does not exist.
func (b *InMemoryBackend) RemoveSourceIdentifierFromSubscription(
	subscriptionName, sourceIdentifier string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName must not be empty", ErrInvalidParameter)
	}
	if sourceIdentifier == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()

	sub, exists := b.eventSubscriptions[subscriptionName]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, subscriptionName)
	}

	idx := slices.Index(sub.SourceIDs, sourceIdentifier)
	if idx >= 0 {
		sub.SourceIDs = slices.Delete(sub.SourceIDs, idx, idx+1)
	}

	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

const backtrackIDBytes = 8

// newBacktrackID generates a unique backtrack identifier using random bytes.
func newBacktrackID() string {
	buf := make([]byte, backtrackIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "backtrack-unknown"
	}

	return "backtrack-" + hex.EncodeToString(buf)
}

// ---- Seed helpers ----

// AddClusterInternal creates a DB cluster directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(id, engine string) *DBCluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	c := &DBCluster{
		DBClusterIdentifier: id,
		Engine:              engine,
		Status:              instanceStatusAvailable,
	}
	b.clusters[id] = c
	cp := *c

	return &cp
}

// AddInstanceInternal creates a DB instance directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddInstanceInternal(id, engine string) *DBInstance {
	b.mu.Lock("AddInstanceInternal")
	defer b.mu.Unlock()

	inst := &DBInstance{
		DBInstanceIdentifier: id,
		Engine:               engine,
		DBInstanceStatus:     instanceStatusAvailable,
		DBInstanceClass:      defaultInstanceClass,
		AllocatedStorage:     defaultAllocatedStorage,
	}
	b.instances[id] = inst
	cp := *inst

	return &cp
}

// AddEventSubscriptionInternal creates an event subscription directly. Used for seeding tests.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicArn string) *EventSubscription {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()

	sub := &EventSubscription{
		SubscriptionName: name,
		SnsTopicArn:      snsTopicArn,
		Status:           subscriptionStatusActive,
		SourceIDs:        []string{},
	}
	b.eventSubscriptions[name] = sub
	cp := *sub
	cp.SourceIDs = make([]string, 0)

	return &cp
}

// AddBlueGreenDeploymentInternal creates a Blue/Green Deployment directly. Used for seeding tests.
func (b *InMemoryBackend) AddBlueGreenDeploymentInternal(name, source string) *BlueGreenDeployment {
	b.mu.Lock("AddBlueGreenDeploymentInternal")
	defer b.mu.Unlock()

	id := "bgd-" + name
	d := &BlueGreenDeployment{
		BlueGreenDeploymentIdentifier: id,
		BlueGreenDeploymentName:       name,
		Source:                        source,
		Status:                        blueGreenDeploymentStatusAvailable,
	}
	b.blueGreenDeployments[id] = d
	cp := *d

	return &cp
}

// AddSecurityGroupInternal creates a DB security group directly. Used for seeding tests.
func (b *InMemoryBackend) AddSecurityGroupInternal(name, description string) *DBSecurityGroup {
	b.mu.Lock("AddSecurityGroupInternal")
	defer b.mu.Unlock()

	sg := &DBSecurityGroup{
		DBSecurityGroupName:        name,
		DBSecurityGroupDescription: description,
		IPRanges:                   []IPRange{},
	}
	b.dbSecurityGroups[name] = sg
	cp := *sg
	cp.IPRanges = make([]IPRange, 0)

	return &cp
}
