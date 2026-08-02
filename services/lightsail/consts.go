package lightsail

import "time"

// asyncTransitionDelay is the simulated delay between each hop of a
// resource's/operation's state-transition chain, matching services/
// directconnect's asyncTransitionDelay / services/outposts'
// orderTransitionDelay / services/grafana's workspaceTransitionDelay.
const asyncTransitionDelay = 100 * time.Millisecond

// defaultPageLimit is the page size used by every Get*/List op when the
// caller omits PageToken-driving MaxResults (this service's Input structs
// carry no MaxResults member at all -- pagination is purely PageToken-in/
// PageToken-out -- so this bounds page size independent of caller input).
const defaultPageLimit = 100

// OperationStatus wire values (types.OperationStatus) -- exactly 5, per
// PARITY.md section 2.
const (
	OperationStatusNotStarted = "NotStarted"
	OperationStatusStarted    = "Started"
	OperationStatusFailed     = "Failed"
	OperationStatusCompleted  = "Completed"
	OperationStatusSucceeded  = "Succeeded"
)

// ResourceType wire values (types.ResourceType) -- 20 values, PARITY.md
// section 5.1.
const (
	ResourceTypeContainerService           = "ContainerService"
	ResourceTypeInstance                   = "Instance"
	ResourceTypeStaticIP                   = "StaticIp"
	ResourceTypeKeyPair                    = "KeyPair"
	ResourceTypeInstanceSnapshot           = "InstanceSnapshot"
	ResourceTypeDomain                     = "Domain"
	ResourceTypePeeredVpc                  = "PeeredVpc"
	ResourceTypeLoadBalancer               = "LoadBalancer"
	ResourceTypeLoadBalancerTLSCertificate = "LoadBalancerTlsCertificate"
	ResourceTypeDisk                       = "Disk"
	ResourceTypeDiskSnapshot               = "DiskSnapshot"
	ResourceTypeRelationalDatabase         = "RelationalDatabase"
	ResourceTypeRelationalDatabaseSnapshot = "RelationalDatabaseSnapshot"
	ResourceTypeExportSnapshotRecord       = "ExportSnapshotRecord"
	ResourceTypeCloudFormationStackRecord  = "CloudFormationStackRecord"
	ResourceTypeAlarm                      = "Alarm"
	ResourceTypeContactMethod              = "ContactMethod"
	ResourceTypeDistribution               = "Distribution"
	ResourceTypeCertificate                = "Certificate"
	ResourceTypeBucket                     = "Bucket"
)

// InstanceState numeric codes -- see models.go's Instance doc comment: an
// UNCONFIRMED, EC2-style convention, since this SDK module publishes no
// typed enum or numeric mapping for Lightsail instance state at all.
const (
	InstanceStateCodePending      = 0
	InstanceStateCodeRunning      = 16
	InstanceStateCodeShuttingDown = 32
	InstanceStateCodeTerminated   = 48
	InstanceStateCodeStopping     = 64
	InstanceStateCodeStopped      = 80
)

const (
	InstanceStateNamePending      = "pending"
	InstanceStateNameRunning      = "running"
	InstanceStateNameShuttingDown = "shutting-down"
	InstanceStateNameTerminated   = "terminated"
	InstanceStateNameStopping     = "stopping"
	InstanceStateNameStopped      = "stopped"
)

// DiskState wire values (types.DiskState) -- a real typed 5-value SDK enum.
const (
	DiskStatePending   = "pending"
	DiskStateError     = "error"
	DiskStateAvailable = "available"
	DiskStateInUse     = "in-use"
	DiskStateUnknown   = "unknown"
)

// DiskSnapshotState / InstanceSnapshotState wire values.
const (
	SnapshotStatePending   = "pending"
	SnapshotStateError     = "error"
	SnapshotStateCompleted = "completed"
	SnapshotStateAvailable = "available"
	SnapshotStateUnknown   = "unknown"
)

// AutoMountStatus wire values (types.AutoMountStatus, 4 values). This
// emulator has no real guest OS to mount anything inside (PARITY.md 4.2),
// so AttachDisk with AutoMounting=true tracks straight to Mounted as
// bookkeeping only, never real guest-side mount detection.
const (
	AutoMountStatusFailed     = "Failed"
	AutoMountStatusPending    = "Pending"
	AutoMountStatusMounted    = "Mounted"
	AutoMountStatusNotMounted = "NotMounted"
)

// PortStateOpen is the only PortState value this backend ever reports --
// the real port state for Lightsail instances is always "open" per the
// SDK's own doc comment (PARITY.md 4.1).
const PortStateOpen = "open"

// AddOnType wire values (2 values).
const (
	AddOnTypeAutoSnapshot       = "AutoSnapshot"
	AddOnTypeStopInstanceOnIdle = "StopInstanceOnIdle"
)

// AutoSnapshotStatus wire values.
const (
	AutoSnapshotStatusSuccess    = "Success"
	AutoSnapshotStatusFailed     = "Failed"
	AutoSnapshotStatusInProgress = "InProgress"
	AutoSnapshotStatusNotFound   = "NotFound"
)

// LoadBalancerState wire values (5 values).
const (
	LoadBalancerStateActive         = "active"
	LoadBalancerStateProvisioning   = "provisioning"
	LoadBalancerStateActiveImpaired = "active_impaired"
	LoadBalancerStateFailed         = "failed"
	LoadBalancerStateUnknown        = "unknown"
)

// LoadBalancerProtocol wire values (2 values -- Lightsail LBs are a
// single-listener simplification, PARITY.md 4.4).
const (
	LoadBalancerProtocolHTTPHTTPS = "HTTP_HTTPS"
	LoadBalancerProtocolHTTP      = "HTTP"
)

// InstanceHealthState wire values (6 values).
const (
	InstanceHealthInitial     = "initial"
	InstanceHealthHealthy     = "healthy"
	InstanceHealthUnhealthy   = "unhealthy"
	InstanceHealthUnused      = "unused"
	InstanceHealthDraining    = "draining"
	InstanceHealthUnavailable = "unavailable"
)

// LoadBalancerTlsCertificateStatus / CertificateStatus wire values (7
// values, shared shape between family M and family V).
const (
	CertificateStatusPendingValidation  = "PENDING_VALIDATION"
	CertificateStatusIssued             = "ISSUED"
	CertificateStatusInactive           = "INACTIVE"
	CertificateStatusExpired            = "EXPIRED"
	CertificateStatusValidationTimedOut = "VALIDATION_TIMED_OUT"
	CertificateStatusRevoked            = "REVOKED"
	CertificateStatusFailed             = "FAILED"
)

// RelationalDatabaseState -- NO typed SDK enum exists anywhere in this
// module (PARITY.md 4.3); these string values are an honest, UNCONFIRMED
// convention following general AWS RDS-family behavior, not sourced from
// this SDK.
const (
	RelationalDatabaseStateCreating  = "creating"
	RelationalDatabaseStateAvailable = "available"
	RelationalDatabaseStateModifying = "modifying"
	RelationalDatabaseStateBackingUp = "backing-up"
	RelationalDatabaseStateDeleting  = "deleting"
	RelationalDatabaseStateStarting  = "starting"
	RelationalDatabaseStateStopping  = "stopping"
	RelationalDatabaseStateStopped   = "stopped"
	RelationalDatabaseStateRebooting = "rebooting"
)

// RelationalDatabasePasswordVersion wire values (3 values).
const (
	PasswordVersionCurrent  = "CURRENT"
	PasswordVersionPrevious = "PREVIOUS"
	PasswordVersionPending  = "PENDING"
)

// ContainerServiceState wire values (7 values, PARITY.md 4.5).
const (
	ContainerServiceStatePending   = "PENDING"
	ContainerServiceStateReady     = "READY"
	ContainerServiceStateRunning   = "RUNNING"
	ContainerServiceStateUpdating  = "UPDATING"
	ContainerServiceStateDeleting  = "DELETING"
	ContainerServiceStateDisabled  = "DISABLED"
	ContainerServiceStateDeploying = "DEPLOYING"
)

// ContainerServiceStateDetailCode wire values (9 values, populated only
// during PENDING/DEPLOYING/UPDATING, PARITY.md 4.5).
const (
	ContainerServiceDetailCreatingSystemResources       = "CREATING_SYSTEM_RESOURCES"
	ContainerServiceDetailCreatingNetworkInfrastructure = "CREATING_NETWORK_INFRASTRUCTURE"
	ContainerServiceDetailProvisioningCertificate       = "PROVISIONING_CERTIFICATE"
	ContainerServiceDetailProvisioningService           = "PROVISIONING_SERVICE"
	ContainerServiceDetailCreatingDeployment            = "CREATING_DEPLOYMENT"
	ContainerServiceDetailEvaluatingHealthCheck         = "EVALUATING_HEALTH_CHECK"
	ContainerServiceDetailActivatingDeployment          = "ACTIVATING_DEPLOYMENT"
	ContainerServiceDetailCertificateLimitExceeded      = "CERTIFICATE_LIMIT_EXCEEDED"
	ContainerServiceDetailUnknownError                  = "UNKNOWN_ERROR"
)

// ContainerServiceDeploymentState wire values (4 values, tracked
// per-deployment via CurrentDeployment/NextDeployment).
const (
	ContainerDeploymentStateActivating = "ACTIVATING"
	ContainerDeploymentStateActive     = "ACTIVE"
	ContainerDeploymentStateInactive   = "INACTIVE"
	ContainerDeploymentStateFailed     = "FAILED"
)

// BucketState.Code -- free-form *string with exactly 2 documented example
// values in its own SDK comment ("OK"/"Unknown", PARITY.md 4.6).
const (
	BucketStateOK      = "OK"
	BucketStateUnknown = "Unknown"
)

// ObjectVersioning wire values (3 documented values, PARITY.md 4.6).
const (
	ObjectVersioningEnabled      = "Enabled"
	ObjectVersioningSuspended    = "Suspended"
	ObjectVersioningNeverEnabled = "NeverEnabled"
)

// AccessKey Status (types.StatusType).
const (
	AccessKeyStatusActive   = "Active"
	AccessKeyStatusInactive = "Inactive"
)

// AlarmState wire values (3 values).
const (
	AlarmStateOK               = "OK"
	AlarmStateAlarm            = "ALARM"
	AlarmStateInsufficientData = "INSUFFICIENT_DATA"
)

// ContactMethodStatus wire values (3 values, PARITY.md 4.8). Verification
// is never real email/SMS delivery in this emulator -- see
// alarms_contacts.go.
const (
	ContactMethodStatusPendingVerification = "PendingVerification"
	ContactMethodStatusValid               = "Valid"
	ContactMethodStatusInvalid             = "Invalid"
)

// ContactProtocol wire values (2 values).
const (
	ContactProtocolEmail = "Email"
	ContactProtocolSMS   = "SMS"
)

// RecordState wire values (used by ExportSnapshotRecord/
// CloudFormationStackRecord).
const (
	RecordStateStarted   = "STARTED"
	RecordStateSucceeded = "SUCCEEDED"
	RecordStateFailed    = "FAILED"
)

// GUI session status values (Status enum -- includes settingUpInstance/
// failedStartingGUISession/failedStoppingGUISession per PARITY.md family
// AA, plus "ready"/"stopped" for the steady states).
const (
	GUISessionStatusSettingUp = "settingUpInstance"
	GUISessionStatusReady     = "ready"
	GUISessionStatusStopped   = "stopped"
	GUISessionStatusFailed    = "failedStartingGUISession"
)

// distributionRegion is the literal Region every Distribution reports
// regardless of the backend's own configured region -- LightsailDistribution.
// Location's own SDK doc comment: "all distributions are located in the
// us-east-1 Region" (PARITY.md 4.7), a real quotable AWS behavior.
const distributionRegion = "us-east-1"

// domainGlobalRegion is the literal region segment Domain ARNs use --
// confirmed via Domain.Arn's own SDK doc-comment example
// "arn:aws:lightsail:global:..." (PARITY.md 4.7/5.1).
const domainGlobalRegion = "global"

// InstancePlatform wire values (types.InstancePlatform, 2 values).
const (
	InstancePlatformLinuxUnix = "LINUX_UNIX"
	InstancePlatformWindows   = "WINDOWS"
)

// ipAddressTypeDualStack is the default IpAddressType this backend applies
// when a caller omits it on a create call.
const ipAddressTypeDualStack = "dualstack"

// blueprintGroupUbuntu names the seed Ubuntu blueprint's Group value,
// shared between referencedata.go's seed and instances.go's
// defaultUsernameForBlueprint lookup.
const blueprintGroupUbuntu = "ubuntu"

// Seed Bundle InstanceType values (also reused as their BundleID prefixes).
const (
	bundleInstanceTypeNano   = "nano"
	bundleInstanceTypeMicro  = "micro"
	bundleInstanceTypeSmall  = "small"
	bundleInstanceTypeMedium = "medium"
)

// certificateValidityMonths bounds the synthetic NotAfter this backend sets
// on a newly-issued Certificate/LoadBalancerTLSCertificate -- Let's
// Encrypt's own real-world default certificate lifetime is 3 months, a
// defensible, documented stand-in since this SDK module publishes no
// specific Lightsail-certificate validity period.
const certificateValidityMonths = 3
