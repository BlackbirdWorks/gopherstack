package mgn

// Wire request/response shapes for the restjson1 protocol. Field names are the
// Go SDK's own exported field name with only its first rune lowercased (e.g.
// "applicationID" keeps ID capitalized), per aws-sdk-go-v2/service/mgn@v1.48.4's
// serializers.go/deserializers.go -- see PARITY.md. *string "DateTime"-suffixed
// members wire as bare RFC3339 strings; real smithy *time.Time fields (Network
// Migration's CreatedAt/UpdatedAt/EndedAt) are epoch-seconds via pkgs/awstime.Epoch.

// ---- shared nested shapes ----

type licensingWire struct {
	OsByol bool `json:"osByol,omitempty"`
}

type ssmParameterWire struct {
	ParameterName string `json:"parameterName"`
	ParameterType string `json:"parameterType"`
}

type ssmDocumentWire struct {
	Parameters            map[string][]ssmParameterWire `json:"parameters,omitempty"`
	ExternalParameters    map[string]dynamicPathWire    `json:"externalParameters,omitempty"`
	ActionName            string                        `json:"actionName"`
	SsmDocumentName       string                        `json:"ssmDocumentName"`
	TimeoutSeconds        int32                         `json:"timeoutSeconds,omitempty"`
	MustSucceedForCutover bool                          `json:"mustSucceedForCutover,omitempty"`
}

// dynamicPathWire mirrors the one SsmExternalParameter union member this
// SDK version has (SsmExternalParameterMemberDynamicPath) -- see models.go's
// ssmDocument doc comment.
type dynamicPathWire struct {
	DynamicPath string `json:"dynamicPath"`
}

type postLaunchActionsWire struct {
	CloudWatchLogGroupName string            `json:"cloudWatchLogGroupName,omitempty"`
	Deployment             string            `json:"deployment,omitempty"`
	S3LogBucket            string            `json:"s3LogBucket,omitempty"`
	S3OutputKeyPrefix      string            `json:"s3OutputKeyPrefix,omitempty"`
	SsmDocuments           []ssmDocumentWire `json:"ssmDocuments,omitempty"`
}

type launchTemplateDiskConfWire struct {
	VolumeType string `json:"volumeType,omitempty"`
	Iops       int64  `json:"iops,omitempty"`
	Throughput int64  `json:"throughput,omitempty"`
}

type storageConfigurationWire struct {
	FsxOntapConfiguration *fsxOntapConfigurationWire `json:"fsxOntapConfiguration,omitempty"`
	StorageType           string                     `json:"storageType"`
}

type fsxOntapConfigurationWire struct {
	CredentialsSecretArn    string `json:"credentialsSecretArn"`
	StorageVirtualMachineID string `json:"storageVirtualMachineId"`
}

type replicatedDiskWire struct {
	DeviceName      string `json:"deviceName,omitempty"`
	StagingDiskType string `json:"stagingDiskType,omitempty"`
	Iops            int64  `json:"iops,omitempty"`
	Throughput      int64  `json:"throughput,omitempty"`
	IsBootDisk      bool   `json:"isBootDisk,omitempty"`
}

// ---- SourceServer and nested shapes ----

type cpuWire struct {
	ModelName string `json:"modelName,omitempty"`
	Cores     int64  `json:"cores,omitempty"`
}

type diskWire struct {
	DeviceName string `json:"deviceName,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
}

type identificationHintsWire struct {
	AwsInstanceID string `json:"awsInstanceID,omitempty"`
	Fqdn          string `json:"fqdn,omitempty"`
	Hostname      string `json:"hostname,omitempty"`
	VMPath        string `json:"vmPath,omitempty"`
	VMWareUUID    string `json:"vmWareUuid,omitempty"`
}

type networkInterfaceWire struct {
	MacAddress string   `json:"macAddress,omitempty"`
	Ips        []string `json:"ips,omitempty"`
	IsPrimary  bool     `json:"isPrimary,omitempty"`
}

type osWire struct {
	FullString string `json:"fullString,omitempty"`
}

type sourcePropertiesWire struct {
	IdentificationHints     *identificationHintsWire `json:"identificationHints,omitempty"`
	Os                      *osWire                  `json:"os,omitempty"`
	LastUpdatedDateTime     string                   `json:"lastUpdatedDateTime,omitempty"`
	RecommendedInstanceType string                   `json:"recommendedInstanceType,omitempty"`
	Cpus                    []cpuWire                `json:"cpus,omitempty"`
	Disks                   []diskWire               `json:"disks,omitempty"`
	NetworkInterfaces       []networkInterfaceWire   `json:"networkInterfaces,omitempty"`
	RAMBytes                int64                    `json:"ramBytes,omitempty"`
}

type connectorActionWire struct {
	ConnectorArn         string `json:"connectorArn,omitempty"`
	CredentialsSecretArn string `json:"credentialsSecretArn,omitempty"`
}

type dataReplicationErrorWire struct {
	Error    string `json:"error,omitempty"`
	RawError string `json:"rawError,omitempty"`
}

type dataReplicationInitiationStepWire struct {
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

type dataReplicationInitiationWire struct {
	NextAttemptDateTime string                              `json:"nextAttemptDateTime,omitempty"`
	StartDateTime       string                              `json:"startDateTime,omitempty"`
	Steps               []dataReplicationInitiationStepWire `json:"steps,omitempty"`
}

type replicatedDiskInfoWire struct {
	DeviceName             string `json:"deviceName,omitempty"`
	BackloggedStorageBytes int64  `json:"backloggedStorageBytes,omitempty"`
	ReplicatedStorageBytes int64  `json:"replicatedStorageBytes,omitempty"`
	RescannedStorageBytes  int64  `json:"rescannedStorageBytes,omitempty"`
	TotalStorageBytes      int64  `json:"totalStorageBytes,omitempty"`
}

type dataReplicationInfoWire struct {
	DataReplicationError      *dataReplicationErrorWire      `json:"dataReplicationError,omitempty"`
	DataReplicationInitiation *dataReplicationInitiationWire `json:"dataReplicationInitiation,omitempty"`
	DataReplicationState      string                         `json:"dataReplicationState,omitempty"`
	EtaDateTime               string                         `json:"etaDateTime,omitempty"`
	LagDuration               string                         `json:"lagDuration,omitempty"`
	LastSnapshotDateTime      string                         `json:"lastSnapshotDateTime,omitempty"`
	ReplicatorID              string                         `json:"replicatorID,omitempty"`
	ReplicatedDisks           []replicatedDiskInfoWire       `json:"replicatedDisks,omitempty"`
}

type lastKnownCheckWire struct {
	CheckedAt *float64 `json:"checkedAt,omitempty"`
	Error     string   `json:"error,omitempty"`
	Name      string   `json:"name,omitempty"`
	Status    string   `json:"status,omitempty"`
	Type      string   `json:"type,omitempty"`
}

type launchedInstanceWire struct {
	Ec2InstanceID            string               `json:"ec2InstanceID,omitempty"`
	FirstBoot                string               `json:"firstBoot,omitempty"`
	JobID                    string               `json:"jobID,omitempty"`
	LastKnownFsxChecksStatus string               `json:"lastKnownFsxChecksStatus,omitempty"`
	LastKnownChecks          []lastKnownCheckWire `json:"lastKnownChecks,omitempty"`
}

type timestampedJobRefWire struct {
	APICallDateTime string `json:"apiCallDateTime,omitempty"`
	JobID           string `json:"jobID,omitempty"`
}

type timestampedWire struct {
	APICallDateTime string `json:"apiCallDateTime,omitempty"`
}

type lifeCycleLastTestWire struct {
	Finalized *timestampedWire       `json:"finalized,omitempty"`
	Initiated *timestampedJobRefWire `json:"initiated,omitempty"`
	Reverted  *timestampedWire       `json:"reverted,omitempty"`
}

type lifeCycleLastCutoverWire struct {
	Finalized *timestampedWire       `json:"finalized,omitempty"`
	Initiated *timestampedJobRefWire `json:"initiated,omitempty"`
	Reverted  *timestampedWire       `json:"reverted,omitempty"`
}

type lifeCycleWire struct {
	LastCutover                *lifeCycleLastCutoverWire `json:"lastCutover,omitempty"`
	LastTest                   *lifeCycleLastTestWire    `json:"lastTest,omitempty"`
	AddedToServiceDateTime     string                    `json:"addedToServiceDateTime,omitempty"`
	ElapsedReplicationDuration string                    `json:"elapsedReplicationDuration,omitempty"`
	FirstByteDateTime          string                    `json:"firstByteDateTime,omitempty"`
	LastSeenByServiceDateTime  string                    `json:"lastSeenByServiceDateTime,omitempty"`
	State                      string                    `json:"state,omitempty"`
}

// sourceServerWire mirrors the 13-field flattened types.SourceServer shape
// (PARITY.md wire-trap #1) -- used both standalone (Describe's Items[]) and
// embedded via anonymous struct-field flattening on the 11 ops that flatten
// it directly onto their own Output.
type sourceServerWire struct {
	Tags                   map[string]string        `json:"tags,omitempty"`
	ConnectorAction        *connectorActionWire     `json:"connectorAction,omitempty"`
	DataReplicationInfo    *dataReplicationInfoWire `json:"dataReplicationInfo,omitempty"`
	LaunchedInstance       *launchedInstanceWire    `json:"launchedInstance,omitempty"`
	LifeCycle              *lifeCycleWire           `json:"lifeCycle,omitempty"`
	SourceProperties       *sourcePropertiesWire    `json:"sourceProperties,omitempty"`
	ApplicationID          string                   `json:"applicationID,omitempty"`
	Arn                    string                   `json:"arn,omitempty"`
	FqdnForActionFramework string                   `json:"fqdnForActionFramework,omitempty"`
	ReplicationType        string                   `json:"replicationType,omitempty"`
	SourceServerID         string                   `json:"sourceServerID,omitempty"`
	UserProvidedID         string                   `json:"userProvidedID,omitempty"`
	VcenterClientID        string                   `json:"vcenterClientID,omitempty"`
	IsArchived             bool                     `json:"isArchived,omitempty"`
}

type describeSourceServersFiltersWire struct {
	ApplicationIDs   []string `json:"applicationIDs,omitempty"`
	IsArchived       *bool    `json:"isArchived,omitempty"`
	LifeCycleStates  []string `json:"lifeCycleStates,omitempty"`
	ReplicationTypes []string `json:"replicationTypes,omitempty"`
	SourceServerIDs  []string `json:"sourceServerIDs,omitempty"`
}

type describeSourceServersRequest struct {
	Filters    *describeSourceServersFiltersWire `json:"filters,omitempty"`
	AccountID  string                            `json:"accountID,omitempty"`
	NextToken  string                            `json:"nextToken,omitempty"`
	MaxResults int32                             `json:"maxResults,omitempty"`
}

type describeSourceServersResponse struct {
	NextToken string             `json:"nextToken,omitempty"`
	Items     []sourceServerWire `json:"items"`
}

type sourceServerIDRequest struct {
	SourceServerID string `json:"sourceServerID"`
	AccountID      string `json:"accountID,omitempty"`
}

// Platform (real wire field: "platform") is deliberately not modeled here --
// see SourceServerUpdate's doc comment (sourceservers.go) for why: JSON
// unmarshal silently ignores unknown keys, so a caller-sent "platform" is
// accepted and dropped, matching the real SDK's own SourceServer output
// shape, which has no Platform field to read it back from either.
type updateSourceServerRequest struct {
	ConnectorAction        *connectorActionWire `json:"connectorAction,omitempty"`
	FqdnForActionFramework *string              `json:"fqdnForActionFramework,omitempty"`
	UserProvidedID         *string              `json:"userProvidedID,omitempty"`
	SourceServerID         string               `json:"sourceServerID"`
	AccountID              string               `json:"accountID,omitempty"`
}

type updateSourceServerReplicationTypeRequest struct {
	SourceServerID  string `json:"sourceServerID"`
	ReplicationType string `json:"replicationType"`
	AccountID       string `json:"accountID,omitempty"`
}

type changeServerLifeCycleStateWire struct {
	State string `json:"state"`
}

type changeServerLifeCycleStateRequest struct {
	LifeCycle      *changeServerLifeCycleStateWire `json:"lifeCycle"`
	SourceServerID string                          `json:"sourceServerID"`
	AccountID      string                          `json:"accountID,omitempty"`
}

type startBatchRequest struct {
	Tags            map[string]string `json:"tags,omitempty"`
	AccountID       string            `json:"accountID,omitempty"`
	SourceServerIDs []string          `json:"sourceServerIDs"`
}

type jobEnvelope struct {
	Job *jobWire `json:"job,omitempty"`
}

// ---- Job ----

type participatingServerWire struct {
	SourceServerID        string `json:"sourceServerID"`
	LaunchStatus          string `json:"launchStatus,omitempty"`
	LaunchedEc2InstanceID string `json:"launchedEc2InstanceID,omitempty"`
}

type jobWire struct {
	Tags                 map[string]string         `json:"tags,omitempty"`
	JobID                string                    `json:"jobID"`
	Arn                  string                    `json:"arn,omitempty"`
	CreationDateTime     string                    `json:"creationDateTime,omitempty"`
	EndDateTime          string                    `json:"endDateTime,omitempty"`
	InitiatedBy          string                    `json:"initiatedBy,omitempty"`
	Status               string                    `json:"status,omitempty"`
	Type                 string                    `json:"type,omitempty"`
	ParticipatingServers []participatingServerWire `json:"participatingServers,omitempty"`
}

type describeJobsFiltersWire struct {
	FromDate string   `json:"fromDate,omitempty"`
	ToDate   string   `json:"toDate,omitempty"`
	JobIDs   []string `json:"jobIDs,omitempty"`
}

type describeJobsRequest struct {
	Filters    *describeJobsFiltersWire `json:"filters,omitempty"`
	AccountID  string                   `json:"accountID,omitempty"`
	NextToken  string                   `json:"nextToken,omitempty"`
	MaxResults int32                    `json:"maxResults,omitempty"`
}

type describeJobsResponse struct {
	NextToken string    `json:"nextToken,omitempty"`
	Items     []jobWire `json:"items"`
}

type jobIDRequest struct {
	JobID     string `json:"jobID"`
	AccountID string `json:"accountID,omitempty"`
}

type describeJobLogItemsRequest struct {
	JobID      string `json:"jobID"`
	AccountID  string `json:"accountID,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

type jobLogEventDataWire struct {
	ConversionServerID string `json:"conversionServerID,omitempty"`
	RawError           string `json:"rawError,omitempty"`
	SourceServerID     string `json:"sourceServerID,omitempty"`
	TargetInstanceID   string `json:"targetInstanceID,omitempty"`
}

type jobLogWire struct {
	EventData   *jobLogEventDataWire `json:"eventData,omitempty"`
	Event       string               `json:"event,omitempty"`
	LogDateTime string               `json:"logDateTime,omitempty"`
}

type describeJobLogItemsResponse struct {
	NextToken string       `json:"nextToken,omitempty"`
	Items     []jobLogWire `json:"items"`
}

// ---- Launch configuration (per-server) ----

type launchConfigurationWire struct {
	Licensing                           *licensingWire         `json:"licensing,omitempty"`
	PostLaunchActions                   *postLaunchActionsWire `json:"postLaunchActions,omitempty"`
	BootMode                            string                 `json:"bootMode,omitempty"`
	Ec2LaunchTemplateID                 string                 `json:"ec2LaunchTemplateID,omitempty"`
	LaunchDisposition                   string                 `json:"launchDisposition,omitempty"`
	MapAutoTaggingMpeID                 string                 `json:"mapAutoTaggingMpeID,omitempty"`
	Name                                string                 `json:"name,omitempty"`
	SourceServerID                      string                 `json:"sourceServerID,omitempty"`
	TargetInstanceTypeRightSizingMethod string                 `json:"targetInstanceTypeRightSizingMethod,omitempty"`
	CopyPrivateIP                       bool                   `json:"copyPrivateIp,omitempty"`
	CopyTags                            bool                   `json:"copyTags,omitempty"`
	EnableMapAutoTagging                bool                   `json:"enableMapAutoTagging,omitempty"`
}

type getLaunchConfigurationRequest struct {
	SourceServerID string `json:"sourceServerID"`
	AccountID      string `json:"accountID,omitempty"`
}

type updateLaunchConfigurationRequest struct {
	Licensing                           *licensingWire         `json:"licensing,omitempty"`
	PostLaunchActions                   *postLaunchActionsWire `json:"postLaunchActions,omitempty"`
	BootMode                            *string                `json:"bootMode,omitempty"`
	LaunchDisposition                   *string                `json:"launchDisposition,omitempty"`
	MapAutoTaggingMpeID                 *string                `json:"mapAutoTaggingMpeID,omitempty"`
	Name                                *string                `json:"name,omitempty"`
	TargetInstanceTypeRightSizingMethod *string                `json:"targetInstanceTypeRightSizingMethod,omitempty"`
	CopyPrivateIP                       *bool                  `json:"copyPrivateIp,omitempty"`
	CopyTags                            *bool                  `json:"copyTags,omitempty"`
	EnableMapAutoTagging                *bool                  `json:"enableMapAutoTagging,omitempty"`
	SourceServerID                      string                 `json:"sourceServerID"`
	AccountID                           string                 `json:"accountID,omitempty"`
}

type launchConfigurationTemplateWire struct {
	Licensing                           *licensingWire              `json:"licensing,omitempty"`
	PostLaunchActions                   *postLaunchActionsWire      `json:"postLaunchActions,omitempty"`
	LargeVolumeConf                     *launchTemplateDiskConfWire `json:"largeVolumeConf,omitempty"`
	SmallVolumeConf                     *launchTemplateDiskConfWire `json:"smallVolumeConf,omitempty"`
	Tags                                map[string]string           `json:"tags,omitempty"`
	LaunchConfigurationTemplateID       string                      `json:"launchConfigurationTemplateID"`
	Arn                                 string                      `json:"arn,omitempty"`
	BootMode                            string                      `json:"bootMode,omitempty"`
	Ec2LaunchTemplateID                 string                      `json:"ec2LaunchTemplateID,omitempty"`
	LaunchDisposition                   string                      `json:"launchDisposition,omitempty"`
	MapAutoTaggingMpeID                 string                      `json:"mapAutoTaggingMpeID,omitempty"`
	ParametersEncryptionKey             string                      `json:"parametersEncryptionKey,omitempty"`
	TargetInstanceTypeRightSizingMethod string                      `json:"targetInstanceTypeRightSizingMethod,omitempty"`
	SmallVolumeMaxSize                  int64                       `json:"smallVolumeMaxSize,omitempty"`
	AssociatePublicIPAddress            bool                        `json:"associatePublicIpAddress,omitempty"`
	CopyPrivateIP                       bool                        `json:"copyPrivateIp,omitempty"`
	CopyTags                            bool                        `json:"copyTags,omitempty"`
	EnableMapAutoTagging                bool                        `json:"enableMapAutoTagging,omitempty"`
	EnableParametersEncryption          bool                        `json:"enableParametersEncryption,omitempty"`
}

type createLaunchConfigurationTemplateRequest struct {
	Licensing                           *licensingWire              `json:"licensing,omitempty"`
	PostLaunchActions                   *postLaunchActionsWire      `json:"postLaunchActions,omitempty"`
	LargeVolumeConf                     *launchTemplateDiskConfWire `json:"largeVolumeConf,omitempty"`
	SmallVolumeConf                     *launchTemplateDiskConfWire `json:"smallVolumeConf,omitempty"`
	Tags                                map[string]string           `json:"tags,omitempty"`
	BootMode                            string                      `json:"bootMode,omitempty"`
	LaunchDisposition                   string                      `json:"launchDisposition,omitempty"`
	MapAutoTaggingMpeID                 string                      `json:"mapAutoTaggingMpeID,omitempty"`
	ParametersEncryptionKey             string                      `json:"parametersEncryptionKey,omitempty"`
	TargetInstanceTypeRightSizingMethod string                      `json:"targetInstanceTypeRightSizingMethod,omitempty"`
	SmallVolumeMaxSize                  int64                       `json:"smallVolumeMaxSize,omitempty"`
	AssociatePublicIPAddress            bool                        `json:"associatePublicIpAddress,omitempty"`
	CopyPrivateIP                       bool                        `json:"copyPrivateIp,omitempty"`
	CopyTags                            bool                        `json:"copyTags,omitempty"`
	EnableMapAutoTagging                bool                        `json:"enableMapAutoTagging,omitempty"`
	EnableParametersEncryption          bool                        `json:"enableParametersEncryption,omitempty"`
}

type launchConfigurationTemplateIDRequest struct {
	LaunchConfigurationTemplateID string `json:"launchConfigurationTemplateID"`
}

type describeLaunchConfigurationTemplatesRequest struct {
	NextToken                      string   `json:"nextToken,omitempty"`
	LaunchConfigurationTemplateIDs []string `json:"launchConfigurationTemplateIDs,omitempty"`
	MaxResults                     int32    `json:"maxResults,omitempty"`
}

type describeLaunchConfigurationTemplatesResponse struct {
	NextToken string                            `json:"nextToken,omitempty"`
	Items     []launchConfigurationTemplateWire `json:"items"`
}

type updateLaunchConfigurationTemplateRequest struct {
	LaunchDisposition                   *string                     `json:"launchDisposition,omitempty"`
	SmallVolumeMaxSize                  *int64                      `json:"smallVolumeMaxSize,omitempty"`
	LargeVolumeConf                     *launchTemplateDiskConfWire `json:"largeVolumeConf,omitempty"`
	SmallVolumeConf                     *launchTemplateDiskConfWire `json:"smallVolumeConf,omitempty"`
	BootMode                            *string                     `json:"bootMode,omitempty"`
	PostLaunchActions                   *postLaunchActionsWire      `json:"postLaunchActions,omitempty"`
	EnableMapAutoTagging                *bool                       `json:"enableMapAutoTagging,omitempty"`
	Licensing                           *licensingWire              `json:"licensing,omitempty"`
	TargetInstanceTypeRightSizingMethod *string                     `json:"targetInstanceTypeRightSizingMethod,omitempty"`
	MapAutoTaggingMpeID                 *string                     `json:"mapAutoTaggingMpeID,omitempty"`
	AssociatePublicIPAddress            *bool                       `json:"associatePublicIpAddress,omitempty"`
	CopyPrivateIP                       *bool                       `json:"copyPrivateIp,omitempty"`
	CopyTags                            *bool                       `json:"copyTags,omitempty"`
	LaunchConfigurationTemplateID       string                      `json:"launchConfigurationTemplateID"`
}

// ---- Replication configuration (per-server) ----

type replicationConfigurationWire struct {
	StorageConfiguration                *storageConfigurationWire `json:"storageConfiguration,omitempty"`
	StagingAreaTags                     map[string]string         `json:"stagingAreaTags,omitempty"`
	SourceServerID                      string                    `json:"sourceServerID,omitempty"`
	ReplicationServerInstanceType       string                    `json:"replicationServerInstanceType,omitempty"`
	StagingAreaSubnetID                 string                    `json:"stagingAreaSubnetID,omitempty"`
	DataPlaneRouting                    string                    `json:"dataPlaneRouting,omitempty"`
	DefaultLargeStagingDiskType         string                    `json:"defaultLargeStagingDiskType,omitempty"`
	EbsEncryption                       string                    `json:"ebsEncryption,omitempty"`
	EbsEncryptionKeyArn                 string                    `json:"ebsEncryptionKeyArn,omitempty"`
	InternetProtocol                    string                    `json:"internetProtocol,omitempty"`
	Name                                string                    `json:"name,omitempty"`
	ReplicationServersSecurityGroupsIDs []string                  `json:"replicationServersSecurityGroupsIDs,omitempty"`
	ReplicatedDisks                     []replicatedDiskWire      `json:"replicatedDisks,omitempty"`
	BandwidthThrottling                 int64                     `json:"bandwidthThrottling,omitempty"`
	AssociateDefaultSecurityGroup       bool                      `json:"associateDefaultSecurityGroup,omitempty"`
	CreatePublicIP                      bool                      `json:"createPublicIP,omitempty"`
	StoreSnapshotOnLocalZone            bool                      `json:"storeSnapshotOnLocalZone,omitempty"`
	UseDedicatedReplicationServer       bool                      `json:"useDedicatedReplicationServer,omitempty"`
	UseFipsEndpoint                     bool                      `json:"useFipsEndpoint,omitempty"`
}

type getReplicationConfigurationRequest struct {
	SourceServerID string `json:"sourceServerID"`
	AccountID      string `json:"accountID,omitempty"`
}

type updateReplicationConfigurationRequest struct {
	ReplicationServerInstanceType       *string                   `json:"replicationServerInstanceType,omitempty"`
	StagingAreaTags                     map[string]string         `json:"stagingAreaTags,omitempty"`
	UseFipsEndpoint                     *bool                     `json:"useFipsEndpoint,omitempty"`
	UseDedicatedReplicationServer       *bool                     `json:"useDedicatedReplicationServer,omitempty"`
	DataPlaneRouting                    *string                   `json:"dataPlaneRouting,omitempty"`
	DefaultLargeStagingDiskType         *string                   `json:"defaultLargeStagingDiskType,omitempty"`
	EbsEncryption                       *string                   `json:"ebsEncryption,omitempty"`
	EbsEncryptionKeyArn                 *string                   `json:"ebsEncryptionKeyArn,omitempty"`
	Name                                *string                   `json:"name,omitempty"`
	InternetProtocol                    *string                   `json:"internetProtocol,omitempty"`
	StoreSnapshotOnLocalZone            *bool                     `json:"storeSnapshotOnLocalZone,omitempty"`
	CreatePublicIP                      *bool                     `json:"createPublicIP,omitempty"`
	StorageConfiguration                *storageConfigurationWire `json:"storageConfiguration,omitempty"`
	StagingAreaSubnetID                 *string                   `json:"stagingAreaSubnetID,omitempty"`
	BandwidthThrottling                 *int64                    `json:"bandwidthThrottling,omitempty"`
	AssociateDefaultSecurityGroup       *bool                     `json:"associateDefaultSecurityGroup,omitempty"`
	SourceServerID                      string                    `json:"sourceServerID"`
	AccountID                           string                    `json:"accountID,omitempty"`
	ReplicationServersSecurityGroupsIDs []string                  `json:"replicationServersSecurityGroupsIDs,omitempty"`
	ReplicatedDisks                     []replicatedDiskWire      `json:"replicatedDisks,omitempty"`
}

type replicationConfigurationTemplateWire struct {
	StorageConfiguration                *storageConfigurationWire `json:"storageConfiguration,omitempty"`
	StagingAreaTags                     map[string]string         `json:"stagingAreaTags,omitempty"`
	Tags                                map[string]string         `json:"tags,omitempty"`
	EbsEncryption                       string                    `json:"ebsEncryption,omitempty"`
	InternetProtocol                    string                    `json:"internetProtocol,omitempty"`
	Arn                                 string                    `json:"arn,omitempty"`
	DataPlaneRouting                    string                    `json:"dataPlaneRouting,omitempty"`
	DefaultLargeStagingDiskType         string                    `json:"defaultLargeStagingDiskType,omitempty"`
	StagingAreaSubnetID                 string                    `json:"stagingAreaSubnetID,omitempty"`
	EbsEncryptionKeyArn                 string                    `json:"ebsEncryptionKeyArn,omitempty"`
	ReplicationConfigurationTemplateID  string                    `json:"replicationConfigurationTemplateID"`
	ReplicationServerInstanceType       string                    `json:"replicationServerInstanceType,omitempty"`
	ReplicationServersSecurityGroupsIDs []string                  `json:"replicationServersSecurityGroupsIDs,omitempty"`
	BandwidthThrottling                 int64                     `json:"bandwidthThrottling,omitempty"`
	AssociateDefaultSecurityGroup       bool                      `json:"associateDefaultSecurityGroup,omitempty"`
	CreatePublicIP                      bool                      `json:"createPublicIP,omitempty"`
	StoreSnapshotOnLocalZone            bool                      `json:"storeSnapshotOnLocalZone,omitempty"`
	UseDedicatedReplicationServer       bool                      `json:"useDedicatedReplicationServer,omitempty"`
	UseFipsEndpoint                     bool                      `json:"useFipsEndpoint,omitempty"`
}

type createReplicationConfigurationTemplateRequest struct {
	StorageConfiguration                *storageConfigurationWire `json:"storageConfiguration,omitempty"`
	StagingAreaTags                     map[string]string         `json:"stagingAreaTags"`
	Tags                                map[string]string         `json:"tags,omitempty"`
	InternetProtocol                    string                    `json:"internetProtocol,omitempty"`
	StagingAreaSubnetID                 string                    `json:"stagingAreaSubnetID"`
	DefaultLargeStagingDiskType         string                    `json:"defaultLargeStagingDiskType"`
	EbsEncryption                       string                    `json:"ebsEncryption"`
	EbsEncryptionKeyArn                 string                    `json:"ebsEncryptionKeyArn,omitempty"`
	DataPlaneRouting                    string                    `json:"dataPlaneRouting"`
	ReplicationServerInstanceType       string                    `json:"replicationServerInstanceType"`
	ReplicationServersSecurityGroupsIDs []string                  `json:"replicationServersSecurityGroupsIDs"`
	BandwidthThrottling                 int64                     `json:"bandwidthThrottling,omitempty"`
	AssociateDefaultSecurityGroup       bool                      `json:"associateDefaultSecurityGroup"`
	CreatePublicIP                      bool                      `json:"createPublicIP"`
	StoreSnapshotOnLocalZone            bool                      `json:"storeSnapshotOnLocalZone,omitempty"`
	UseDedicatedReplicationServer       bool                      `json:"useDedicatedReplicationServer"`
	UseFipsEndpoint                     bool                      `json:"useFipsEndpoint,omitempty"`
}

type replicationConfigurationTemplateIDRequest struct {
	ReplicationConfigurationTemplateID string `json:"replicationConfigurationTemplateID"`
}

type describeReplicationConfigurationTemplatesRequest struct {
	NextToken                           string   `json:"nextToken,omitempty"`
	ReplicationConfigurationTemplateIDs []string `json:"replicationConfigurationTemplateIDs,omitempty"`
	MaxResults                          int32    `json:"maxResults,omitempty"`
}

type describeReplicationConfigurationTemplatesResponse struct {
	NextToken string                                 `json:"nextToken,omitempty"`
	Items     []replicationConfigurationTemplateWire `json:"items"`
}

type updateReplicationConfigurationTemplateRequest struct {
	ReplicationServerInstanceType       *string                   `json:"replicationServerInstanceType,omitempty"`
	CreatePublicIP                      *bool                     `json:"createPublicIP,omitempty"`
	UseFipsEndpoint                     *bool                     `json:"useFipsEndpoint,omitempty"`
	DataPlaneRouting                    *string                   `json:"dataPlaneRouting,omitempty"`
	DefaultLargeStagingDiskType         *string                   `json:"defaultLargeStagingDiskType,omitempty"`
	EbsEncryption                       *string                   `json:"ebsEncryption,omitempty"`
	EbsEncryptionKeyArn                 *string                   `json:"ebsEncryptionKeyArn,omitempty"`
	InternetProtocol                    *string                   `json:"internetProtocol,omitempty"`
	StagingAreaTags                     map[string]string         `json:"stagingAreaTags,omitempty"`
	UseDedicatedReplicationServer       *bool                     `json:"useDedicatedReplicationServer,omitempty"`
	StorageConfiguration                *storageConfigurationWire `json:"storageConfiguration,omitempty"`
	BandwidthThrottling                 *int64                    `json:"bandwidthThrottling,omitempty"`
	AssociateDefaultSecurityGroup       *bool                     `json:"associateDefaultSecurityGroup,omitempty"`
	StagingAreaSubnetID                 *string                   `json:"stagingAreaSubnetID,omitempty"`
	StoreSnapshotOnLocalZone            *bool                     `json:"storeSnapshotOnLocalZone,omitempty"`
	ReplicationConfigurationTemplateID  string                    `json:"replicationConfigurationTemplateID"`
	ReplicationServersSecurityGroupsIDs []string                  `json:"replicationServersSecurityGroupsIDs,omitempty"`
}

// ---- Application / Wave ----

type applicationAggregatedStatusWire struct {
	HealthStatus       string `json:"healthStatus,omitempty"`
	LastUpdateDateTime string `json:"lastUpdateDateTime,omitempty"`
	ProgressStatus     string `json:"progressStatus,omitempty"`
	TotalSourceServers int64  `json:"totalSourceServers,omitempty"`
}

type applicationWire struct {
	AggregatedStatus     *applicationAggregatedStatusWire `json:"applicationAggregatedStatus,omitempty"`
	Tags                 map[string]string                `json:"tags,omitempty"`
	ApplicationID        string                           `json:"applicationID,omitempty"`
	Arn                  string                           `json:"arn,omitempty"`
	CreationDateTime     string                           `json:"creationDateTime,omitempty"`
	Description          string                           `json:"description,omitempty"`
	LastModifiedDateTime string                           `json:"lastModifiedDateTime,omitempty"`
	Name                 string                           `json:"name,omitempty"`
	WaveID               string                           `json:"waveID,omitempty"`
	IsArchived           bool                             `json:"isArchived,omitempty"`
}

type createApplicationRequest struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	AccountID   string            `json:"accountID,omitempty"`
	Description string            `json:"description,omitempty"`
}

type updateApplicationRequest struct {
	Description   *string `json:"description,omitempty"`
	Name          *string `json:"name,omitempty"`
	ApplicationID string  `json:"applicationID"`
	AccountID     string  `json:"accountID,omitempty"`
}

type applicationIDRequest struct {
	ApplicationID string `json:"applicationID"`
	AccountID     string `json:"accountID,omitempty"`
}

type listApplicationsFiltersWire struct {
	IsArchived     *bool    `json:"isArchived,omitempty"`
	ApplicationIDs []string `json:"applicationIDs,omitempty"`
	WaveIDs        []string `json:"waveIDs,omitempty"`
}

type listApplicationsRequest struct {
	Filters    *listApplicationsFiltersWire `json:"filters,omitempty"`
	AccountID  string                       `json:"accountID,omitempty"`
	NextToken  string                       `json:"nextToken,omitempty"`
	MaxResults int32                        `json:"maxResults,omitempty"`
}

type listApplicationsResponse struct {
	NextToken string            `json:"nextToken,omitempty"`
	Items     []applicationWire `json:"items"`
}

type associateSourceServersRequest struct {
	ApplicationID   string   `json:"applicationID"`
	AccountID       string   `json:"accountID,omitempty"`
	SourceServerIDs []string `json:"sourceServerIDs"`
}

type waveAggregatedStatusWire struct {
	HealthStatus               string `json:"healthStatus,omitempty"`
	LastUpdateDateTime         string `json:"lastUpdateDateTime,omitempty"`
	ProgressStatus             string `json:"progressStatus,omitempty"`
	ReplicationStartedDateTime string `json:"replicationStartedDateTime,omitempty"`
	TotalApplications          int64  `json:"totalApplications,omitempty"`
}

type waveWire struct {
	AggregatedStatus     *waveAggregatedStatusWire `json:"waveAggregatedStatus,omitempty"`
	Tags                 map[string]string         `json:"tags,omitempty"`
	Arn                  string                    `json:"arn,omitempty"`
	CreationDateTime     string                    `json:"creationDateTime,omitempty"`
	Description          string                    `json:"description,omitempty"`
	LastModifiedDateTime string                    `json:"lastModifiedDateTime,omitempty"`
	Name                 string                    `json:"name,omitempty"`
	WaveID               string                    `json:"waveID,omitempty"`
	IsArchived           bool                      `json:"isArchived,omitempty"`
}

type createWaveRequest struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	AccountID   string            `json:"accountID,omitempty"`
	Description string            `json:"description,omitempty"`
}

type updateWaveRequest struct {
	Description *string `json:"description,omitempty"`
	Name        *string `json:"name,omitempty"`
	WaveID      string  `json:"waveID"`
	AccountID   string  `json:"accountID,omitempty"`
}

type waveIDRequest struct {
	WaveID    string `json:"waveID"`
	AccountID string `json:"accountID,omitempty"`
}

type listWavesFiltersWire struct {
	IsArchived *bool    `json:"isArchived,omitempty"`
	WaveIDs    []string `json:"waveIDs,omitempty"`
}

type listWavesRequest struct {
	Filters    *listWavesFiltersWire `json:"filters,omitempty"`
	AccountID  string                `json:"accountID,omitempty"`
	NextToken  string                `json:"nextToken,omitempty"`
	MaxResults int32                 `json:"maxResults,omitempty"`
}

type listWavesResponse struct {
	NextToken string     `json:"nextToken,omitempty"`
	Items     []waveWire `json:"items"`
}

type associateApplicationsRequest struct {
	WaveID         string   `json:"waveID"`
	AccountID      string   `json:"accountID,omitempty"`
	ApplicationIDs []string `json:"applicationIDs"`
}

// ---- Connector ----

type connectorSsmCommandConfigWire struct {
	CloudWatchLogGroupName  string `json:"cloudWatchLogGroupName,omitempty"`
	OutputS3BucketName      string `json:"outputS3BucketName,omitempty"`
	CloudWatchOutputEnabled bool   `json:"cloudWatchOutputEnabled"`
	S3OutputEnabled         bool   `json:"s3OutputEnabled"`
}

type connectorWire struct {
	SsmCommandConfig *connectorSsmCommandConfigWire `json:"ssmCommandConfig,omitempty"`
	Tags             map[string]string              `json:"tags,omitempty"`
	ConnectorID      string                         `json:"connectorID,omitempty"`
	Arn              string                         `json:"arn,omitempty"`
	Name             string                         `json:"name,omitempty"`
	SsmInstanceID    string                         `json:"ssmInstanceID,omitempty"`
}

type createConnectorRequest struct {
	SsmCommandConfig *connectorSsmCommandConfigWire `json:"ssmCommandConfig,omitempty"`
	Tags             map[string]string              `json:"tags,omitempty"`
	Name             string                         `json:"name"`
	SsmInstanceID    string                         `json:"ssmInstanceID"`
}

type updateConnectorRequest struct {
	SsmCommandConfig *connectorSsmCommandConfigWire `json:"ssmCommandConfig,omitempty"`
	Name             *string                        `json:"name,omitempty"`
	SsmInstanceID    *string                        `json:"ssmInstanceID,omitempty"`
	ConnectorID      string                         `json:"connectorID"`
}

type connectorIDRequest struct {
	ConnectorID string `json:"connectorID"`
}

type listConnectorsFiltersWire struct {
	ConnectorIDs []string `json:"connectorIDs,omitempty"`
}

type listConnectorsRequest struct {
	Filters    *listConnectorsFiltersWire `json:"filters,omitempty"`
	NextToken  string                     `json:"nextToken,omitempty"`
	MaxResults int32                      `json:"maxResults,omitempty"`
}

type listConnectorsResponse struct {
	NextToken string          `json:"nextToken,omitempty"`
	Items     []connectorWire `json:"items"`
}

// ---- vCenter clients ----

type vcenterClientWire struct {
	SourceServerTags map[string]string `json:"sourceServerTags,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
	VcenterClientID  string            `json:"vcenterClientID,omitempty"`
	Arn              string            `json:"arn,omitempty"`
	DatacenterName   string            `json:"datacenterName,omitempty"`
	Hostname         string            `json:"hostname,omitempty"`
	LastSeenDatetime string            `json:"lastSeenDatetime,omitempty"`
	VcenterUUID      string            `json:"vcenterUUID,omitempty"`
}

type describeVcenterClientsResponse struct {
	NextToken string              `json:"nextToken,omitempty"`
	Items     []vcenterClientWire `json:"items"`
}

type vcenterClientIDRequest struct {
	VcenterClientID string `json:"vcenterClientID"`
}

// ---- Export / Import ----

type exportTaskSummaryWire struct {
	ApplicationsCount int64 `json:"applicationsCount,omitempty"`
	ServersCount      int64 `json:"serversCount,omitempty"`
	WavesCount        int64 `json:"wavesCount,omitempty"`
}

type exportTaskWire struct {
	Summary            *exportTaskSummaryWire `json:"summary,omitempty"`
	Tags               map[string]string      `json:"tags,omitempty"`
	ExportID           string                 `json:"exportID,omitempty"`
	Arn                string                 `json:"arn,omitempty"`
	CreationDateTime   string                 `json:"creationDateTime,omitempty"`
	EndDateTime        string                 `json:"endDateTime,omitempty"`
	S3Bucket           string                 `json:"s3Bucket,omitempty"`
	S3BucketOwner      string                 `json:"s3BucketOwner,omitempty"`
	S3Key              string                 `json:"s3Key,omitempty"`
	Status             string                 `json:"status,omitempty"`
	ProgressPercentage float32                `json:"progressPercentage,omitempty"`
}

type startExportRequest struct {
	Tags          map[string]string `json:"tags,omitempty"`
	S3Bucket      string            `json:"s3Bucket"`
	S3BucketOwner string            `json:"s3BucketOwner,omitempty"`
	S3Key         string            `json:"s3Key"`
}

type exportTaskEnvelope struct {
	ExportTask *exportTaskWire `json:"exportTask,omitempty"`
}

type listExportsFiltersWire struct {
	ExportIDs []string `json:"exportIDs,omitempty"`
}

type listExportsRequest struct {
	Filters    *listExportsFiltersWire `json:"filters,omitempty"`
	NextToken  string                  `json:"nextToken,omitempty"`
	MaxResults int32                   `json:"maxResults,omitempty"`
}

type listExportsResponse struct {
	NextToken string           `json:"nextToken,omitempty"`
	Items     []exportTaskWire `json:"items"`
}

type exportIDRequest struct {
	ExportID   string `json:"exportID"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

type listExportErrorsResponse struct {
	NextToken string     `json:"nextToken,omitempty"`
	Items     []struct{} `json:"items"`
}

type s3BucketSourceWire struct {
	S3Bucket      string `json:"s3Bucket"`
	S3BucketOwner string `json:"s3BucketOwner,omitempty"`
	S3Key         string `json:"s3Key"`
}

type countPairWire struct {
	CreatedCount  int64 `json:"createdCount,omitempty"`
	ModifiedCount int64 `json:"modifiedCount,omitempty"`
}

type importTaskSummaryWire struct {
	Applications *countPairWire `json:"applications,omitempty"`
	Servers      *countPairWire `json:"servers,omitempty"`
	Waves        *countPairWire `json:"waves,omitempty"`
}

type importTaskWire struct {
	Summary            *importTaskSummaryWire `json:"summary,omitempty"`
	S3BucketSource     *s3BucketSourceWire    `json:"s3BucketSource,omitempty"`
	Tags               map[string]string      `json:"tags,omitempty"`
	ImportID           string                 `json:"importID,omitempty"`
	Arn                string                 `json:"arn,omitempty"`
	CreationDateTime   string                 `json:"creationDateTime,omitempty"`
	EndDateTime        string                 `json:"endDateTime,omitempty"`
	Status             string                 `json:"status,omitempty"`
	ProgressPercentage float32                `json:"progressPercentage,omitempty"`
}

type startImportRequest struct {
	S3BucketSource *s3BucketSourceWire `json:"s3BucketSource"`
	Tags           map[string]string   `json:"tags,omitempty"`
	ClientToken    string              `json:"clientToken,omitempty"`
}

type importTaskEnvelope struct {
	ImportTask *importTaskWire `json:"importTask,omitempty"`
}

type listImportsFiltersWire struct {
	ImportIDs []string `json:"importIDs,omitempty"`
}

type listImportsRequest struct {
	Filters    *listImportsFiltersWire `json:"filters,omitempty"`
	NextToken  string                  `json:"nextToken,omitempty"`
	MaxResults int32                   `json:"maxResults,omitempty"`
}

type listImportsResponse struct {
	NextToken string           `json:"nextToken,omitempty"`
	Items     []importTaskWire `json:"items"`
}

type importIDRequest struct {
	ImportID   string `json:"importID"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

// importErrorDataWire mirrors types.ImportErrorData. AccountID/ApplicationID/
// Ec2LaunchTemplateID are never populated by this backend (see models.go's
// ImportErrorData doc comment), so they are omitted entirely rather than
// wire-coded as empty strings.
type importErrorDataWire struct {
	RawError  string `json:"rawError,omitempty"`
	RowNumber int64  `json:"rowNumber,omitempty"`
}

// importTaskErrorWire mirrors types.ImportTaskError.
type importTaskErrorWire struct {
	ErrorData     *importErrorDataWire `json:"errorData,omitempty"`
	ErrorDateTime string               `json:"errorDateTime,omitempty"`
	ErrorType     string               `json:"errorType,omitempty"`
}

type listImportErrorsResponse struct {
	NextToken string                `json:"nextToken,omitempty"`
	Items     []importTaskErrorWire `json:"items"`
}

type enrichmentTargetS3ConfigurationWire struct {
	S3Bucket      string `json:"s3Bucket"`
	S3BucketOwner string `json:"s3BucketOwner"`
	S3Key         string `json:"s3Key"`
}

type enrichmentSourceS3ConfigurationWire struct {
	S3Bucket      string `json:"s3Bucket"`
	S3BucketOwner string `json:"s3BucketOwner"`
	S3Key         string `json:"s3Key"`
}

type startImportFileEnrichmentRequest struct {
	SourceS3Configuration *enrichmentSourceS3ConfigurationWire `json:"sourceS3Configuration"`
	TargetS3Configuration *enrichmentTargetS3ConfigurationWire `json:"targetS3Configuration"`
	Tags                  map[string]string                    `json:"tags,omitempty"`
}

type checksumWire struct {
	EncryptionAlgorithm string `json:"encryptionAlgorithm,omitempty"`
	Hash                string `json:"hash,omitempty"`
}

type importFileEnrichmentWire struct {
	Checksum       *checksumWire                        `json:"checksum,omitempty"`
	S3BucketTarget *enrichmentTargetS3ConfigurationWire `json:"s3BucketTarget,omitempty"`
	CreatedAt      *float64                             `json:"createdAt,omitempty"`
	EndedAt        *float64                             `json:"endedAt,omitempty"`
	JobID          string                               `json:"jobID,omitempty"`
	Status         string                               `json:"status,omitempty"`
	StatusDetails  string                               `json:"statusDetails,omitempty"`
}

type listImportFileEnrichmentsFiltersWire struct {
	JobIDs []string `json:"jobIDs,omitempty"`
}

type listImportFileEnrichmentsRequest struct {
	Filters    *listImportFileEnrichmentsFiltersWire `json:"filters,omitempty"`
	NextToken  string                                `json:"nextToken,omitempty"`
	MaxResults int32                                 `json:"maxResults,omitempty"`
}

type listImportFileEnrichmentsResponse struct {
	NextToken string                     `json:"nextToken,omitempty"`
	Items     []importFileEnrichmentWire `json:"items"`
}

// ---- Post-launch actions ----

type sourceServerActionDocumentWire struct {
	ExternalParameters    map[string]dynamicPathWire    `json:"externalParameters,omitempty"`
	Parameters            map[string][]ssmParameterWire `json:"parameters,omitempty"`
	ActionID              string                        `json:"actionID,omitempty"`
	ActionName            string                        `json:"actionName,omitempty"`
	Category              string                        `json:"category,omitempty"`
	Description           string                        `json:"description,omitempty"`
	DocumentIdentifier    string                        `json:"documentIdentifier,omitempty"`
	DocumentVersion       string                        `json:"documentVersion,omitempty"`
	Order                 int32                         `json:"order"`
	TimeoutSeconds        int32                         `json:"timeoutSeconds,omitempty"`
	Active                bool                          `json:"active,omitempty"`
	MustSucceedForCutover bool                          `json:"mustSucceedForCutover,omitempty"`
}

type putSourceServerActionRequest struct {
	ExternalParameters    map[string]dynamicPathWire    `json:"externalParameters,omitempty"`
	Parameters            map[string][]ssmParameterWire `json:"parameters,omitempty"`
	SourceServerID        string                        `json:"sourceServerID"`
	ActionID              string                        `json:"actionID"`
	ActionName            string                        `json:"actionName"`
	AccountID             string                        `json:"accountID,omitempty"`
	Category              string                        `json:"category,omitempty"`
	Description           string                        `json:"description,omitempty"`
	DocumentIdentifier    string                        `json:"documentIdentifier"`
	DocumentVersion       string                        `json:"documentVersion,omitempty"`
	Order                 int32                         `json:"order"`
	TimeoutSeconds        int32                         `json:"timeoutSeconds,omitempty"`
	Active                bool                          `json:"active,omitempty"`
	MustSucceedForCutover bool                          `json:"mustSucceedForCutover,omitempty"`
}

type listSourceServerActionsFiltersWire struct {
	ActionIDs []string `json:"actionIDs,omitempty"`
}

type listSourceServerActionsRequest struct {
	Filters        *listSourceServerActionsFiltersWire `json:"filters,omitempty"`
	SourceServerID string                              `json:"sourceServerID"`
	AccountID      string                              `json:"accountID,omitempty"`
	NextToken      string                              `json:"nextToken,omitempty"`
	MaxResults     int32                               `json:"maxResults,omitempty"`
}

type listSourceServerActionsResponse struct {
	NextToken string                           `json:"nextToken,omitempty"`
	Items     []sourceServerActionDocumentWire `json:"items"`
}

type removeSourceServerActionRequest struct {
	ActionID       string `json:"actionID"`
	SourceServerID string `json:"sourceServerID"`
	AccountID      string `json:"accountID,omitempty"`
}

type templateActionDocumentWire struct {
	ExternalParameters    map[string]dynamicPathWire    `json:"externalParameters,omitempty"`
	Parameters            map[string][]ssmParameterWire `json:"parameters,omitempty"`
	ActionID              string                        `json:"actionID,omitempty"`
	ActionName            string                        `json:"actionName,omitempty"`
	Category              string                        `json:"category,omitempty"`
	Description           string                        `json:"description,omitempty"`
	DocumentIdentifier    string                        `json:"documentIdentifier,omitempty"`
	DocumentVersion       string                        `json:"documentVersion,omitempty"`
	OperatingSystem       string                        `json:"operatingSystem,omitempty"`
	Order                 int32                         `json:"order"`
	TimeoutSeconds        int32                         `json:"timeoutSeconds,omitempty"`
	Active                bool                          `json:"active,omitempty"`
	MustSucceedForCutover bool                          `json:"mustSucceedForCutover,omitempty"`
}

type putTemplateActionRequest struct {
	ExternalParameters            map[string]dynamicPathWire    `json:"externalParameters,omitempty"`
	Parameters                    map[string][]ssmParameterWire `json:"parameters,omitempty"`
	LaunchConfigurationTemplateID string                        `json:"launchConfigurationTemplateID"`
	ActionID                      string                        `json:"actionID"`
	ActionName                    string                        `json:"actionName"`
	Category                      string                        `json:"category,omitempty"`
	Description                   string                        `json:"description,omitempty"`
	DocumentIdentifier            string                        `json:"documentIdentifier"`
	DocumentVersion               string                        `json:"documentVersion,omitempty"`
	OperatingSystem               string                        `json:"operatingSystem,omitempty"`
	Order                         int32                         `json:"order"`
	TimeoutSeconds                int32                         `json:"timeoutSeconds,omitempty"`
	Active                        bool                          `json:"active,omitempty"`
	MustSucceedForCutover         bool                          `json:"mustSucceedForCutover,omitempty"`
}

type templateActionsFiltersWire struct {
	ActionIDs []string `json:"actionIDs,omitempty"`
}

type listTemplateActionsRequest struct {
	Filters                       *templateActionsFiltersWire `json:"filters,omitempty"`
	LaunchConfigurationTemplateID string                      `json:"launchConfigurationTemplateID"`
	NextToken                     string                      `json:"nextToken,omitempty"`
	MaxResults                    int32                       `json:"maxResults,omitempty"`
}

type listTemplateActionsResponse struct {
	NextToken string                       `json:"nextToken,omitempty"`
	Items     []templateActionDocumentWire `json:"items"`
}

type removeTemplateActionRequest struct {
	ActionID                      string `json:"actionID"`
	LaunchConfigurationTemplateID string `json:"launchConfigurationTemplateID"`
}

// ---- Service init / managed accounts ----

type listManagedAccountsRequest struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

type managedAccountWire struct {
	AccountID string `json:"accountId,omitempty"`
}

type listManagedAccountsResponse struct {
	NextToken string               `json:"nextToken,omitempty"`
	Items     []managedAccountWire `json:"items"`
}

// ---- Tags ----

type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"tags"`
}

// ---- Network Migration: definitions & mapper segments/constructs ----

type targetNetworkWire struct {
	Topology       string `json:"topology"`
	InboundCidr    string `json:"inboundCidr,omitempty"`
	InspectionCidr string `json:"inspectionCidr,omitempty"`
	OutboundCidr   string `json:"outboundCidr,omitempty"`
}

type targetS3ConfigurationWire struct {
	S3Bucket      string `json:"s3Bucket"`
	S3BucketOwner string `json:"s3BucketOwner"`
}

type sourceS3ConfigurationWire struct {
	S3Bucket      string `json:"s3Bucket"`
	S3BucketOwner string `json:"s3BucketOwner"`
	S3Key         string `json:"s3Key"`
}

type sourceConfigurationWire struct {
	SourceS3Configuration sourceS3ConfigurationWire `json:"sourceS3Configuration"`
	SourceEnvironment     string                    `json:"sourceEnvironment"`
}

type networkMigrationDefinitionWire struct {
	TargetNetwork                *targetNetworkWire         `json:"targetNetwork,omitempty"`
	TargetS3Configuration        *targetS3ConfigurationWire `json:"targetS3Configuration,omitempty"`
	ScopeTags                    map[string]string          `json:"scopeTags,omitempty"`
	Tags                         map[string]string          `json:"tags,omitempty"`
	CreatedAt                    *float64                   `json:"createdAt,omitempty"`
	UpdatedAt                    *float64                   `json:"updatedAt,omitempty"`
	Arn                          string                     `json:"arn,omitempty"`
	Name                         string                     `json:"name,omitempty"`
	Description                  string                     `json:"description,omitempty"`
	NetworkMigrationDefinitionID string                     `json:"networkMigrationDefinitionID,omitempty"`
	TargetDeployment             string                     `json:"targetDeployment,omitempty"`
	SourceConfigurations         []sourceConfigurationWire  `json:"sourceConfigurations,omitempty"`
}

type networkMigrationDefinitionSummaryWire struct {
	ScopeTags                    map[string]string `json:"scopeTags,omitempty"`
	Tags                         map[string]string `json:"tags,omitempty"`
	Arn                          string            `json:"arn,omitempty"`
	Name                         string            `json:"name,omitempty"`
	NetworkMigrationDefinitionID string            `json:"networkMigrationDefinitionID,omitempty"`
	SourceEnvironment            string            `json:"sourceEnvironment,omitempty"`
}

type createNetworkMigrationDefinitionRequest struct {
	TargetNetwork         *targetNetworkWire         `json:"targetNetwork"`
	TargetS3Configuration *targetS3ConfigurationWire `json:"targetS3Configuration"`
	ScopeTags             map[string]string          `json:"scopeTags,omitempty"`
	Tags                  map[string]string          `json:"tags,omitempty"`
	Name                  string                     `json:"name"`
	Description           string                     `json:"description,omitempty"`
	TargetDeployment      string                     `json:"targetDeployment,omitempty"`
	SourceConfigurations  []sourceConfigurationWire  `json:"sourceConfigurations,omitempty"`
}

type networkMigrationDefinitionIDRequest struct {
	NetworkMigrationDefinitionID string `json:"networkMigrationDefinitionID"`
}

type targetNetworkUpdateWire struct {
	InboundCidr    string `json:"inboundCidr,omitempty"`
	InspectionCidr string `json:"inspectionCidr,omitempty"`
	OutboundCidr   string `json:"outboundCidr,omitempty"`
	Topology       string `json:"topology,omitempty"`
}

type targetS3ConfigurationUpdateWire struct {
	S3Bucket      string `json:"s3Bucket,omitempty"`
	S3BucketOwner string `json:"s3BucketOwner,omitempty"`
}

type updateNetworkMigrationDefinitionRequest struct {
	TargetNetworkUpdate          *targetNetworkUpdateWire         `json:"targetNetwork,omitempty"`
	TargetS3Configuration        *targetS3ConfigurationUpdateWire `json:"targetS3Configuration,omitempty"`
	ScopeTags                    map[string]string                `json:"scopeTags,omitempty"`
	Name                         *string                          `json:"name,omitempty"`
	Description                  *string                          `json:"description,omitempty"`
	NetworkMigrationDefinitionID string                           `json:"networkMigrationDefinitionID"`
	TargetDeployment             string                           `json:"targetDeployment,omitempty"`
	SourceConfigurations         []sourceConfigurationWire        `json:"sourceConfigurations,omitempty"`
}

type listNetworkMigrationDefinitionsFiltersWire struct {
	NetworkMigrationDefinitionIDs []string `json:"networkMigrationDefinitionIDs,omitempty"`
}

type listNetworkMigrationDefinitionsRequest struct {
	Filters    *listNetworkMigrationDefinitionsFiltersWire `json:"filters,omitempty"`
	NextToken  string                                      `json:"nextToken,omitempty"`
	MaxResults int32                                       `json:"maxResults,omitempty"`
}

type listNetworkMigrationDefinitionsResponse struct {
	NextToken string                                  `json:"nextToken,omitempty"`
	Items     []networkMigrationDefinitionSummaryWire `json:"items"`
}

type nmScopeRequest struct {
	NetworkMigrationDefinitionID string `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string `json:"networkMigrationExecutionID"`
}

type startNMCodeGenerationRequest struct {
	NetworkMigrationDefinitionID    string   `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID     string   `json:"networkMigrationExecutionID"`
	CodeGenerationOutputFormatTypes []string `json:"codeGenerationOutputFormatTypes,omitempty"`
}

type getNMMapperSegmentConstructRequest struct {
	NetworkMigrationDefinitionID string `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string `json:"networkMigrationExecutionID"`
	SegmentID                    string `json:"segmentID"`
	ConstructID                  string `json:"constructID"`
}

type listNMMapperSegmentConstructsRequest struct {
	NetworkMigrationDefinitionID string `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string `json:"networkMigrationExecutionID"`
	SegmentID                    string `json:"segmentID"`
	NextToken                    string `json:"nextToken,omitempty"`
	MaxResults                   int32  `json:"maxResults,omitempty"`
}

// nmJobFiltersWire mirrors the identical {jobIDs: []string} Filters shape
// declared by ListNetworkMigrationAnalyses/CodeGenerations/Deployments/
// Mappings/MappingUpdates -- the five listNMScopedRequest ops with a real
// job-details Output; the other listNMScopedRequest ops (AnalysisResults,
// CodeGenerationSegments, DeployedStacks, MapperSegments) either have no
// Filters member at all or a differently-shaped one, so this field stays
// nil and unused for those, which is harmless.
type nmJobFiltersWire struct {
	JobIDs []string `json:"jobIDs,omitempty"`
}

type listNMScopedRequest struct {
	Filters                      *nmJobFiltersWire `json:"filters,omitempty"`
	NetworkMigrationDefinitionID string            `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string            `json:"networkMigrationExecutionID"`
	NextToken                    string            `json:"nextToken,omitempty"`
	MaxResults                   int32             `json:"maxResults,omitempty"`
}

type genericItemsResponse struct {
	NextToken string     `json:"nextToken,omitempty"`
	Items     []struct{} `json:"items"`
}

type updateNMMapperSegmentRequest struct {
	ScopeTags                    map[string]string `json:"scopeTags,omitempty"`
	NetworkMigrationDefinitionID string            `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string            `json:"networkMigrationExecutionID"`
	SegmentID                    string            `json:"segmentID"`
}

type startNMMappingRequest struct {
	NetworkMigrationDefinitionID string `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string `json:"networkMigrationExecutionID"`
	SecurityGroupMappingStrategy string `json:"securityGroupMappingStrategy,omitempty"`
}

type startNMMappingUpdateRequest struct {
	NetworkMigrationDefinitionID string           `json:"networkMigrationDefinitionID"`
	NetworkMigrationExecutionID  string           `json:"networkMigrationExecutionID"`
	Constructs                   []map[string]any `json:"constructs,omitempty"`
	Segments                     []map[string]any `json:"segments,omitempty"`
}

type jobIDResponse struct {
	JobID string `json:"jobID,omitempty"`
}

// ---- Network Migration: analysis, code generation, deployment, executions ----

type codeGenerationOutputFormatStatusDetailsWire struct {
	Status           string `json:"status,omitempty"`
	StatusDetailList string `json:"statusDetailList,omitempty"`
}

type codeGenStatusMap map[string]codeGenerationOutputFormatStatusDetailsWire

type networkMigrationJobDetailsWire struct {
	CodeGenStatusMap codeGenStatusMap `json:"codeGenerationOutputFormatStatusDetailsMap,omitempty"`

	CreatedAt                    *float64 `json:"createdAt,omitempty"`
	EndedAt                      *float64 `json:"endedAt,omitempty"`
	JobID                        string   `json:"jobID,omitempty"`
	NetworkMigrationDefinitionID string   `json:"networkMigrationDefinitionID,omitempty"`
	NetworkMigrationExecutionID  string   `json:"networkMigrationExecutionID,omitempty"`
	Status                       string   `json:"status,omitempty"`
	StatusDetails                string   `json:"statusDetails,omitempty"`
}

type listNMJobDetailsResponse struct {
	NextToken string                           `json:"nextToken,omitempty"`
	Items     []networkMigrationJobDetailsWire `json:"items"`
}

type networkMigrationExecutionWire struct {
	Tags                         map[string]string `json:"tags,omitempty"`
	CreatedAt                    *float64          `json:"createdAt,omitempty"`
	UpdatedAt                    *float64          `json:"updatedAt,omitempty"`
	Activity                     string            `json:"activity,omitempty"`
	NetworkMigrationDefinitionID string            `json:"networkMigrationDefinitionID,omitempty"`
	NetworkMigrationExecutionID  string            `json:"networkMigrationExecutionID,omitempty"`
	Stage                        string            `json:"stage,omitempty"`
	Status                       string            `json:"status,omitempty"`
}

type listNMExecutionsFiltersWire struct {
	NetworkMigrationExecutionIDs      []string `json:"networkMigrationExecutionIDs,omitempty"`
	NetworkMigrationExecutionStatuses []string `json:"networkMigrationExecutionStatuses,omitempty"`
}

type listNMExecutionsRequest struct {
	Filters                      *listNMExecutionsFiltersWire `json:"filters,omitempty"`
	NetworkMigrationDefinitionID string                       `json:"networkMigrationDefinitionID"`
	NextToken                    string                       `json:"nextToken,omitempty"`
	MaxResults                   int32                        `json:"maxResults,omitempty"`
}

type listNMExecutionsResponse struct {
	NextToken string                          `json:"nextToken,omitempty"`
	Items     []networkMigrationExecutionWire `json:"items"`
}
