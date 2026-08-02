package mgn

import "time"

// asyncTransitionDelay is the simulated delay between each hop of a
// resource's state-transition chain (replication init steps, Job progress,
// export/import progress, network-migration job progress), matching
// services/directconnect/services/resiliencehub/services/outposts/
// services/grafana's identically-named constant.
const asyncTransitionDelay = 100 * time.Millisecond

// defaultPageLimit is the page size used by every Describe/List operation
// when the caller omits MaxResults, matching this campaign's convention.
const defaultPageLimit = 100

// LifeCycleState wire values (types.LifeCycleState, 10 values -- the richest
// state enum in this service). See sourceservers.go's doc comment for the
// (documented, inferred -- not SDK-specified) legal transition table.
const (
	LifeCycleStateStopped             = "STOPPED"
	LifeCycleStateNotReady            = "NOT_READY"
	LifeCycleStateReadyForTest        = "READY_FOR_TEST"
	LifeCycleStateTesting             = "TESTING"
	LifeCycleStateReadyForCutover     = "READY_FOR_CUTOVER"
	LifeCycleStateCuttingOver         = "CUTTING_OVER"
	LifeCycleStateCutover             = "CUTOVER"
	LifeCycleStateDisconnected        = "DISCONNECTED"
	LifeCycleStateDiscovered          = "DISCOVERED"
	LifeCycleStatePendingInstallation = "PENDING_INSTALLATION"
)

// ChangeServerLifeCycleStateSourceServerLifecycleState wire values -- the 3
// caller-settable LifeCycleState targets ChangeServerLifeCycleState exposes
// (types.ChangeServerLifeCycleStateSourceServerLifecycleState). Every other
// LifeCycleState value is system-driven only (see sourceservers.go).
const (
	ChangeLifecycleReadyForTest    = "READY_FOR_TEST"
	ChangeLifecycleReadyForCutover = "READY_FOR_CUTOVER"
	ChangeLifecycleCutover         = "CUTOVER"
)

// DataReplicationState wire values (types.DataReplicationState, 12 values).
const (
	DataReplicationStateStopped                 = "STOPPED"
	DataReplicationStateInitiating              = "INITIATING"
	DataReplicationStateInitialSync             = "INITIAL_SYNC"
	DataReplicationStateBacklog                 = "BACKLOG"
	DataReplicationStateCreatingSnapshot        = "CREATING_SNAPSHOT"
	DataReplicationStateContinuous              = "CONTINUOUS"
	DataReplicationStatePaused                  = "PAUSED"
	DataReplicationStateRescan                  = "RESCAN"
	DataReplicationStateStalled                 = "STALLED"
	DataReplicationStateDisconnected            = "DISCONNECTED"
	DataReplicationStatePendingSnapshotShipping = "PENDING_SNAPSHOT_SHIPPING"
	DataReplicationStateShippingSnapshot        = "SHIPPING_SNAPSHOT"
)

// DataReplicationInitiationStepName wire values -- the fixed 12-step
// sequence a simulated replication initiation walks, in order.
const (
	StepWait                            = "WAIT"
	StepCreateSecurityGroup             = "CREATE_SECURITY_GROUP"
	StepLaunchReplicationServer         = "LAUNCH_REPLICATION_SERVER"
	StepBootReplicationServer           = "BOOT_REPLICATION_SERVER"
	StepAuthenticateWithService         = "AUTHENTICATE_WITH_SERVICE"
	StepDownloadReplicationSoftware     = "DOWNLOAD_REPLICATION_SOFTWARE"
	StepCreateStagingDisks              = "CREATE_STAGING_DISKS"
	StepAttachStagingDisks              = "ATTACH_STAGING_DISKS"
	StepPairReplicationServerWithAgent  = "PAIR_REPLICATION_SERVER_WITH_AGENT"
	StepConnectAgentToReplicationServer = "CONNECT_AGENT_TO_REPLICATION_SERVER"
	StepStartDataTransfer               = "START_DATA_TRANSFER"
	StepSetupFsxProxy                   = "SETUP_FSX_PROXY"
)

// dataReplicationInitiationStepNames returns the ordered step-name walk
// every new SourceServer's DataReplicationInitiation.Steps takes, per
// DataReplicationInitiationStepName's 12 documented values. A function
// (not a package-level slice var) so no caller can accidentally mutate
// shared backing storage (gochecknoglobals) -- each call returns a fresh
// slice.
func dataReplicationInitiationStepNames() []string {
	return []string{
		StepWait,
		StepCreateSecurityGroup,
		StepLaunchReplicationServer,
		StepBootReplicationServer,
		StepAuthenticateWithService,
		StepDownloadReplicationSoftware,
		StepCreateStagingDisks,
		StepAttachStagingDisks,
		StepPairReplicationServerWithAgent,
		StepConnectAgentToReplicationServer,
		StepStartDataTransfer,
		StepSetupFsxProxy,
	}
}

// DataReplicationInitiationStepStatus wire values.
const (
	StepStatusNotStarted = "NOT_STARTED"
	StepStatusInProgress = "IN_PROGRESS"
	StepStatusSucceeded  = "SUCCEEDED"
	StepStatusFailed     = "FAILED"
	StepStatusSkipped    = "SKIPPED"
)

// ReplicationType wire values (types.ReplicationType).
const (
	ReplicationTypeAgentBased       = "AGENT_BASED"
	ReplicationTypeSnapshotShipping = "SNAPSHOT_SHIPPING"
)

// JobStatus wire values -- only 3 values, no FAILED at the Job level
// (types.JobStatus).
const (
	JobStatusPending   = "PENDING"
	JobStatusStarted   = "STARTED"
	JobStatusCompleted = "COMPLETED"
)

// JobType wire values (types.JobType).
const (
	JobTypeLaunch    = "LAUNCH"
	JobTypeTerminate = "TERMINATE"
)

// InitiatedBy wire values (types.InitiatedBy).
const (
	InitiatedByStartTest    = "START_TEST"
	InitiatedByStartCutover = "START_CUTOVER"
	InitiatedByTerminate    = "TERMINATE"
)

// LaunchStatus wire values (types.LaunchStatus) -- ParticipatingServer's
// per-server progress within a Job.
const (
	LaunchStatusPending    = "PENDING"
	LaunchStatusInProgress = "IN_PROGRESS"
	LaunchStatusLaunched   = "LAUNCHED"
	LaunchStatusTerminated = "TERMINATED"
)

// JobLogEvent wire values (types.JobLogEvent, 16 values) -- the honest
// granular steps a simulated Job progression walks through per
// ParticipatingServer, in order (SERVER_SKIPPED/CLEANUP_*/LAUNCH_FAILED/
// JOB_CANCEL are never emitted by this deterministic, always-succeeds
// simulation -- see jobs.go).
const (
	JobLogEventJobStart        = "JOB_START"
	JobLogEventSnapshotStart   = "SNAPSHOT_START"
	JobLogEventSnapshotEnd     = "SNAPSHOT_END"
	JobLogEventConversionStart = "CONVERSION_START"
	JobLogEventConversionEnd   = "CONVERSION_END"
	JobLogEventLaunchStart     = "LAUNCH_START"
	JobLogEventJobEnd          = "JOB_END"
)

// ApplicationHealthStatus / WaveHealthStatus wire values -- distinct enum
// types on the wire but identical values (types.ApplicationHealthStatus,
// types.WaveHealthStatus).
const (
	HealthStatusHealthy = "HEALTHY"
	HealthStatusLagging = "LAGGING"
	HealthStatusError   = "ERROR"
)

// ApplicationProgressStatus / WaveProgressStatus wire values (identical
// values, distinct enum types).
const (
	ProgressStatusNotStarted = "NOT_STARTED"
	ProgressStatusInProgress = "IN_PROGRESS"
	ProgressStatusCompleted  = "COMPLETED"
)

// ExportStatus / ImportStatus wire values (types.ExportStatus,
// types.ImportStatus -- identical 4 values).
const (
	TaskStatusPending   = "PENDING"
	TaskStatusStarted   = "STARTED"
	TaskStatusFailed    = "FAILED"
	TaskStatusSucceeded = "SUCCEEDED"
)

// BootMode wire values (types.BootMode).
const (
	BootModeLegacyBios = "LEGACY_BIOS"
	BootModeUefi       = "UEFI"
	BootModeUseSource  = "USE_SOURCE"
)

// LaunchDisposition wire values (types.LaunchDisposition).
const (
	LaunchDispositionStopped = "STOPPED"
	LaunchDispositionStarted = "STARTED"
)

// TargetInstanceTypeRightSizingMethod wire values.
const (
	RightSizingMethodNone  = "NONE"
	RightSizingMethodBasic = "BASIC"
)

// ReplicationConfigurationDataPlaneRouting wire values.
const (
	DataPlaneRoutingPrivateIP = "PRIVATE_IP"
	DataPlaneRoutingPublicIP  = "PUBLIC_IP"
)

// ReplicationConfigurationDefaultLargeStagingDiskType wire values.
const (
	StagingDiskTypeGp2 = "GP2"
	StagingDiskTypeSt1 = "ST1"
	StagingDiskTypeGp3 = "GP3"
)

// ReplicationConfigurationEbsEncryption wire values.
const (
	EbsEncryptionDefault = "DEFAULT"
	EbsEncryptionCustom  = "CUSTOM"
)

// InternetProtocol wire values.
const (
	InternetProtocolIPv4 = "IPV4"
	InternetProtocolIPv6 = "IPV6"
)

// StorageType wire values (types.StorageType).
const (
	StorageTypeEbs      = "EBS"
	StorageTypeFsxOntap = "FSX_ONTAP"
)

// SourceEnvironment wire values (types.SourceEnvironment, 8 values).
const (
	SourceEnvironmentNsx                   = "NSX"
	SourceEnvironmentVsphere               = "VSPHERE"
	SourceEnvironmentFortigateFirewall     = "FORTIGATE_FIREWALL"
	SourceEnvironmentPaloAltoFirewall      = "PALO_ALTO_FIREWALL"
	SourceEnvironmentCiscoAci              = "CISCO_ACI"
	SourceEnvironmentLogicalModel          = "LOGICAL_MODEL"
	SourceEnvironmentModelizeIt            = "MODELIZE_IT"
	SourceEnvironmentAwsDiscoveryCollector = "AWS_DISCOVERY_COLLECTOR"
)

// TargetNetworkTopology wire values.
const (
	TargetNetworkTopologyIsolatedVpc = "ISOLATED_VPC"
	TargetNetworkTopologyHubAndSpoke = "HUB_AND_SPOKE"
)

// TargetDeployment wire values.
const (
	TargetDeploymentSingleAccount = "SINGLE_ACCOUNT"
	TargetDeploymentMultiAccount  = "MULTI_ACCOUNT"
)

// ExecutionStage / ExecutionStageActivity wire values (types.ExecutionStage,
// types.ExecutionStageActivity -- Activity adds MAPPING_UPDATE, everything
// else identical).
const (
	StageMapping                = "MAPPING"
	StageMappingUpdate          = "MAPPING_UPDATE" // Activity-only
	StageCodeGeneration         = "CODE_GENERATION"
	StageDeploy                 = "DEPLOY"
	StageDeployedStacksDeletion = "DEPLOYED_STACKS_DELETION"
	StageAnalyze                = "ANALYZE"
)

// ExecutionStatus / NetworkMigrationJobStatus wire values (identical 4
// values, distinct enum types).
const (
	NMStatusPending   = "PENDING"
	NMStatusStarted   = "STARTED"
	NMStatusSucceeded = "SUCCEEDED"
	NMStatusFailed    = "FAILED"
)

// SecurityGroupMappingStrategy wire values.
const (
	SGMappingStrategyMap     = "MAP"
	SGMappingStrategySkip    = "SKIP"
	SGMappingStrategyMapDhcp = "MAP_DHCP"
)

// NetworkMigrationMapperSegmentType wire values.
const (
	MapperSegmentTypeWorkload  = "WORKLOAD"
	MapperSegmentTypeAppliance = "APPLIANCE"
)

// ValidationExceptionReason wire values -- lower-camelCase, unlike every
// other enum in this service (PARITY.md wire-trap #6).
const (
	ValidationReasonUnknownOperation      = "unknownOperation"
	ValidationReasonCannotParse           = "cannotParse"
	ValidationReasonFieldValidationFailed = "fieldValidationFailed"
	ValidationReasonOther                 = "other"
)

// defaultTotalStorageBytes is this emulator's documented, invented default
// TotalStorageBytes for a newly seeded SourceServer's single replicated disk
// when the seed caller does not specify one -- NOT an AWS-published value
// (no real source machine exists to measure). Chosen as a round, plausible
// 10 GiB single-disk size.
const defaultTotalStorageBytes = 10 * 1024 * 1024 * 1024

// resource-kind labels used in client-exception messages (ResourceType on
// ConflictException/ResourceNotFoundException/ServiceQuotaExceededException
// wire shapes) and internally for building tag-instance Prometheus names.
const (
	resourceSourceServer        = "SOURCE_SERVER"
	resourceJob                 = "JOB"
	resourceApplication         = "APPLICATION"
	resourceWave                = "WAVE"
	resourceConnector           = "CONNECTOR"
	resourceVcenterClient       = "VCENTER_CLIENT"
	resourceLaunchTemplate      = "LAUNCH_CONFIGURATION_TEMPLATE"
	resourceReplicationTemplate = "REPLICATION_CONFIGURATION_TEMPLATE"
	resourceExportTask          = "EXPORT_TASK"
	resourceImportTask          = "IMPORT_TASK"
	resourceAction              = "ACTION"
	resourceTaggable            = "RESOURCE"
	resourceNMDefinition        = "NETWORK_MIGRATION_DEFINITION"
	resourceNMExecution         = "NETWORK_MIGRATION_EXECUTION"
	resourceNMSegment           = "NETWORK_MIGRATION_MAPPER_SEGMENT"
	resourceNMConstruct         = "NETWORK_MIGRATION_MAPPER_SEGMENT_CONSTRUCT"
)
