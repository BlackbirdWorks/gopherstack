package neptune

// ServerlessV2ScalingConfiguration holds Neptune Serverless v2 capacity settings.
type ServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `json:"minCapacity"`
	MaxCapacity float64 `json:"maxCapacity"`
}

// MasterUserManagedSecret holds the ARN of the Secrets Manager secret for the master user password.
type MasterUserManagedSecret struct {
	SecretARN    string `json:"secretArn"`
	SecretStatus string `json:"secretStatus"`
}

// DBClusterCreateOptions holds optional fields for CreateDBCluster.
type DBClusterCreateOptions struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration
	DBSubnetGroupName               string
	StorageType                     string
	EngineVersion                   string
	EngineMode                      string
	KmsKeyID                        string
	PreferredBackupWindow           string
	MasterUsername                  string
	PreferredMaintenanceWindow      string
	AvailabilityZones               []string
	VpcSecurityGroupIDs             []string
	BackupRetentionPeriod           int
	EnableIAMDatabaseAuthentication bool
	ManageMasterUserPassword        bool
	StorageEncrypted                bool
	DeletionProtection              bool
	CopyTagsToSnapshot              bool
}

// DBClusterModifyOptions holds optional fields for ModifyDBCluster.
type DBClusterModifyOptions struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration
	EngineVersion                   string
	PreferredBackupWindow           string
	PreferredMaintenanceWindow      string
	VpcSecurityGroupIDs             []string
	BackupRetentionPeriod           int
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	ManageMasterUserPassword        bool
	DeletionProtection              bool
	DeletionProtectionSet           bool
	CopyTagsToSnapshot              bool
	CopyTagsToSnapshotSet           bool
	BackupRetentionPeriodSet        bool
}

// DBClusterDeleteOptions holds optional fields for DeleteDBCluster.
type DBClusterDeleteOptions struct {
	FinalDBSnapshotIdentifier string
	SkipFinalSnapshot         bool
}

// DBClusterMember represents a single DB instance member of a Neptune cluster.
type DBClusterMember struct {
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	IsClusterWriter      bool   `json:"IsClusterWriter"`
}

// DBCluster represents an Amazon Neptune DB cluster.
type DBCluster struct {
	// region is the AWS region this cluster belongs to. It is the outer half
	// of the composite key ("region|DBClusterIdentifier") used by the
	// backend's flat store.Table[DBCluster] (see store_setup.go), which
	// replaces the old map[string]map[string]*DBCluster nesting (outer key =
	// region). Unexported so it never appears in Neptune wire responses
	// (those are built by cloneCluster/hand-assembled describe results, never
	// by marshaling DBCluster directly), but persistence.go must carry it
	// through a DTO explicitly since json.Marshal never sees unexported fields.
	region                          string
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration `json:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret         *MasterUserManagedSecret          `json:"MasterUserManagedSecret,omitempty"`
	KmsKeyID                        string                            `json:"KmsKeyID"`
	HostedZoneID                    string                            `json:"HostedZoneId"`
	Engine                          string                            `json:"Engine"`
	EngineVersion                   string                            `json:"EngineVersion"`
	DBClusterIdentifier             string                            `json:"DBClusterIdentifier"`
	Status                          string                            `json:"Status"`
	DBClusterParameterGroupName     string                            `json:"DBClusterParameterGroupName"`
	DBSubnetGroupName               string                            `json:"DBSubnetGroupName"`
	Endpoint                        string                            `json:"Endpoint"`
	ReaderEndpoint                  string                            `json:"ReaderEndpoint"`
	PreferredBackupWindow           string                            `json:"PreferredBackupWindow"`
	PreferredMaintenanceWindow      string                            `json:"PreferredMaintenanceWindow"`
	DBClusterArn                    string                            `json:"DBClusterArn"`
	DBClusterResourceID             string                            `json:"DbClusterResourceId"`
	ClusterCreateTime               string                            `json:"ClusterCreateTime"`
	StorageType                     string                            `json:"StorageType"`
	EngineMode                      string                            `json:"EngineMode"`
	MasterUsername                  string                            `json:"MasterUsername"`
	AvailabilityZones               []string                          `json:"AvailabilityZones"`
	VpcSecurityGroupIDs             []string                          `json:"VpcSecurityGroupIds"`
	AssociatedRoles                 []string                          `json:"AssociatedRoles"`
	DBClusterMembers                []DBClusterMember                 `json:"DBClusterMembers"`
	Port                            int                               `json:"Port"`
	BackupRetentionPeriod           int                               `json:"BackupRetentionPeriod"`
	AllocatedStorage                int                               `json:"AllocatedStorage"`
	EnableIAMDatabaseAuthentication bool                              `json:"EnableIAMDatabaseAuthentication"`
	StorageEncrypted                bool                              `json:"StorageEncrypted"`
	MultiAZ                         bool                              `json:"MultiAZ"`
	DeletionProtection              bool                              `json:"DeletionProtection"`
	CopyTagsToSnapshot              bool                              `json:"CopyTagsToSnapshot"`
}

// DBInstance represents an Amazon Neptune DB instance.
type DBInstance struct {
	// region is the AWS region this instance belongs to; see DBCluster.region
	// for the composite-key rationale (store_setup.go/persistence.go).
	region                          string
	DBInstanceIdentifier            string `json:"DBInstanceIdentifier"`
	DBInstanceArn                   string `json:"DBInstanceArn"`
	DBClusterIdentifier             string `json:"DBClusterIdentifier"`
	DBInstanceClass                 string `json:"DBInstanceClass"`
	Engine                          string `json:"Engine"`
	EngineVersion                   string `json:"EngineVersion"`
	DBInstanceStatus                string `json:"DBInstanceStatus"`
	InstanceCreateTime              string `json:"InstanceCreateTime"`
	Endpoint                        string `json:"Endpoint"`
	DBSubnetGroupName               string `json:"DBSubnetGroupName"`
	DBParameterGroupName            string `json:"DBParameterGroupName"`
	PreferredMaintenanceWindow      string `json:"PreferredMaintenanceWindow"`
	PreferredBackupWindow           string `json:"PreferredBackupWindow"`
	AvailabilityZone                string `json:"AvailabilityZone"`
	Port                            int    `json:"Port"`
	PromotionTier                   int    `json:"PromotionTier"`
	StorageEncrypted                bool   `json:"StorageEncrypted"`
	AutoMinorVersionUpgrade         bool   `json:"AutoMinorVersionUpgrade"`
	CopyTagsToSnapshot              bool   `json:"CopyTagsToSnapshot"`
	EnableIAMDatabaseAuthentication bool   `json:"EnableIAMDatabaseAuthentication"`
	MultiAZ                         bool   `json:"MultiAZ"`
	PubliclyAccessible              bool   `json:"PubliclyAccessible"`
}

// DBInstanceCreateOptions holds optional fields for CreateDBInstance.
type DBInstanceCreateOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	PromotionTier                   int
	AutoMinorVersionUpgrade         bool
	CopyTagsToSnapshot              bool
	EnableIAMDatabaseAuthentication bool
	StorageEncrypted                bool
}

// DBInstanceModifyOptions holds optional fields for ModifyDBInstance.
type DBInstanceModifyOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	PromotionTier                   int
	AutoMinorVersionUpgrade         bool
	AutoMinorVersionUpgradeSet      bool
	CopyTagsToSnapshot              bool
	CopyTagsToSnapshotSet           bool
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	PromotionTierSet                bool
}

// DBSubnetGroup represents a Neptune DB subnet group.
type DBSubnetGroup struct {
	// region is the AWS region this subnet group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                   string
	DBSubnetGroupName        string   `json:"DBSubnetGroupName"`
	DBSubnetGroupArn         string   `json:"DBSubnetGroupArn"`
	DBSubnetGroupDescription string   `json:"DBSubnetGroupDescription"`
	VpcID                    string   `json:"VpcID"`
	Status                   string   `json:"Status"`
	SubnetIDs                []string `json:"SubnetIDs"`
}

// Tag is a key-value pair tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// DBClusterParameterGroup represents a Neptune DB cluster parameter group.
type DBClusterParameterGroup struct {
	// region is the AWS region this cluster parameter group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	DBClusterParameterGroupName string `json:"DBClusterParameterGroupName"`
	DBClusterParameterGroupArn  string `json:"DBClusterParameterGroupArn"`
	DBParameterGroupFamily      string `json:"DBParameterGroupFamily"`
	Description                 string `json:"Description"`
}

// DBClusterSnapshot represents a Neptune DB cluster snapshot.
type DBClusterSnapshot struct {
	// region is the AWS region this cluster snapshot belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	DBClusterSnapshotIdentifier string `json:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `json:"DBClusterSnapshotArn"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	Engine                      string `json:"Engine"`
	EngineVersion               string `json:"EngineVersion"`
	Status                      string `json:"Status"`
	SnapshotType                string `json:"SnapshotType"`
	SnapshotCreateTime          string `json:"SnapshotCreateTime"`
	ClusterCreateTime           string `json:"ClusterCreateTime"`
	KmsKeyID                    string `json:"KmsKeyId"`
	VpcID                       string `json:"VpcId"`
	// RestoreAttributeValues holds the account IDs (or "all") authorized to
	// copy/restore this manual snapshot via the "restore" DB cluster snapshot
	// attribute -- the only attribute Neptune's API models (see
	// ModifyDBClusterSnapshotAttribute/DescribeDBClusterSnapshotAttributes).
	RestoreAttributeValues           []string `json:"RestoreAttributeValues"`
	Port                             int      `json:"Port"`
	PercentProgress                  int      `json:"PercentProgress"`
	AllocatedStorage                 int      `json:"AllocatedStorage"`
	StorageEncrypted                 bool     `json:"StorageEncrypted"`
	IAMDatabaseAuthenticationEnabled bool     `json:"IAMDatabaseAuthenticationEnabled"`
}

// DBParameterGroup represents a Neptune DB parameter group.
type DBParameterGroup struct {
	// region is the AWS region this parameter group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                 string
	DBParameterGroupName   string `json:"DBParameterGroupName"`
	DBParameterGroupArn    string `json:"DBParameterGroupArn"`
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	Description            string `json:"Description"`
}

// DBClusterEndpoint represents a Neptune DB cluster custom endpoint.
type DBClusterEndpoint struct {
	// region is the AWS region this cluster endpoint belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                              string
	DBClusterEndpointIdentifier         string   `json:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier                 string   `json:"DBClusterIdentifier"`
	DBClusterEndpointArn                string   `json:"DBClusterEndpointArn"`
	DBClusterEndpointResourceIdentifier string   `json:"DBClusterEndpointResourceIdentifier"`
	EndpointType                        string   `json:"EndpointType"`
	CustomEndpointType                  string   `json:"CustomEndpointType"`
	Status                              string   `json:"Status"`
	Endpoint                            string   `json:"Endpoint"`
	StaticMembers                       []string `json:"StaticMembers"`
	ExcludedMembers                     []string `json:"ExcludedMembers"`
}

// EventSubscription represents a Neptune event subscription.
type EventSubscription struct {
	// region is the AWS region this event subscription belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                   string
	CustSubscriptionID       string   `json:"CustSubscriptionID"`
	SnsTopicARN              string   `json:"SnsTopicARN"`
	EventSubscriptionArn     string   `json:"EventSubscriptionArn"`
	Status                   string   `json:"Status"`
	SourceType               string   `json:"SourceType"`
	SubscriptionCreationTime string   `json:"SubscriptionCreationTime"`
	SourceIDs                []string `json:"SourceIDs"`
	EventCategoriesList      []string `json:"EventCategoriesList"`
	Enabled                  bool     `json:"Enabled"`
}

// GlobalCluster represents a Neptune global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string                `json:"GlobalClusterIdentifier"`
	GlobalClusterArn        string                `json:"GlobalClusterArn"`
	GlobalClusterResourceID string                `json:"GlobalClusterResourceId"`
	Status                  string                `json:"Status"`
	Engine                  string                `json:"Engine"`
	EngineVersion           string                `json:"EngineVersion"`
	GlobalClusterMembers    []GlobalClusterMember `json:"GlobalClusterMembers"`
	StorageEncrypted        bool                  `json:"StorageEncrypted"`
	DeletionProtection      bool                  `json:"DeletionProtection"`
}

// GlobalClusterMember represents a member cluster in a global cluster.
type GlobalClusterMember struct {
	DBClusterARN string `json:"DBClusterARN"`
	IsWriter     bool   `json:"IsWriter"`
}

// DBClusterFilters holds filter values for DescribeDBClusters.
type DBClusterFilters struct {
	Engine        string
	EngineVersion string
	Status        string
}

// ParameterValue is a single persisted parameter override applied to a DB
// (cluster) parameter group via ModifyDBParameterGroup/
// ModifyDBClusterParameterGroup, keyed by parameter name in the backend's
// per-group override store (see parameter_catalog.go).
type ParameterValue struct {
	ParameterValue string `json:"ParameterValue"`
	ApplyMethod    string `json:"ApplyMethod"`
}

// ParameterInput is a single parameter name/value/apply-method triple
// supplied by a caller to Modify/Reset(DBCluster)ParameterGroup.
type ParameterInput struct {
	ParameterName  string
	ParameterValue string
	ApplyMethod    string
}

// EngineParameter is a fully-resolved parameter description as returned by
// DescribeDBParameters/DescribeDBClusterParameters/
// DescribeEngineDefaultParameters/DescribeEngineDefaultClusterParameters --
// the static catalog definition (AllowedValues/ApplyType/DataType/
// Description/IsModifiable/MinimumEngineVersion) merged with any per-group
// override (ParameterValue/ApplyMethod/Source).
type EngineParameter struct {
	ParameterName        string `json:"ParameterName"`
	ParameterValue       string `json:"ParameterValue"`
	Description          string `json:"Description"`
	Source               string `json:"Source"`
	ApplyType            string `json:"ApplyType"`
	DataType             string `json:"DataType"`
	AllowedValues        string `json:"AllowedValues"`
	MinimumEngineVersion string `json:"MinimumEngineVersion"`
	ApplyMethod          string `json:"ApplyMethod"`
	IsModifiable         bool   `json:"IsModifiable"`
}

// GlobalClusterModifyOptions holds optional fields for ModifyGlobalCluster.
type GlobalClusterModifyOptions struct {
	NewGlobalClusterIdentifier string
	EngineVersion              string
	DeletionProtection         bool
	DeletionProtectionSet      bool
	AllowMajorVersionUpgrade   bool
}

// PendingMaintenanceAction is a single queued maintenance action for a
// resource (see ApplyPendingMaintenanceAction/DescribePendingMaintenanceActions).
type PendingMaintenanceAction struct {
	Action               string `json:"Action"`
	Description          string `json:"Description"`
	AutoAppliedAfterDate string `json:"AutoAppliedAfterDate"`
	CurrentApplyDate     string `json:"CurrentApplyDate"`
	ForcedApplyDate      string `json:"ForcedApplyDate"`
	OptInStatus          string `json:"OptInStatus"`
}

// ResourcePendingMaintenanceActions bundles a resource's queued maintenance
// actions with the resource's ARN.
type ResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                     `json:"ResourceIdentifier"`
	PendingMaintenanceActionDetails []PendingMaintenanceAction `json:"PendingMaintenanceActionDetails"`
}

// Event is a single account activity event as returned by DescribeEvents,
// recorded by recordEvent at the point of the underlying state change (see
// events.go).
type Event struct {
	SourceIdentifier string   `json:"SourceIdentifier"`
	SourceType       string   `json:"SourceType"`
	Message          string   `json:"Message"`
	Date             string   `json:"Date"`
	EventCategories  []string `json:"EventCategories"`
}
