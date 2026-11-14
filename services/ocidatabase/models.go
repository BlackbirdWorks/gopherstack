package ocidatabase

import "time"

// Lifecycle states of an Autonomous Database (SDK AutonomousDatabaseLifecycleStateEnum).
const (
	statePending     = "PROVISIONING"
	stateAvailable   = "AVAILABLE"
	stateStopping    = "STOPPING"
	stateStopped     = "STOPPED"
	stateStarting    = "STARTING"
	stateRestarting  = "RESTARTING"
	stateUpdating    = "UPDATING"
	stateScaling     = "SCALE_IN_PROGRESS"
	stateRestoring   = "RESTORE_IN_PROGRESS"
	stateBackingUp   = "BACKUP_IN_PROGRESS"
	stateTerminating = "TERMINATING"
	stateTerminated  = "TERMINATED"
	stateUnavailable = "UNAVAILABLE"
)

// Backup lifecycle states (SDK AutonomousDatabaseBackupLifecycleStateEnum).
const (
	backupCreating = "CREATING"
	backupActive   = "ACTIVE"
	backupDeleting = "DELETING"
	backupDeleted  = "DELETED"
)

// Database workloads (SDK CreateAutonomousDatabaseBaseDBWorkloadEnum) and compute models.
const (
	workloadOLTP = "OLTP"
	workloadDW   = "DW"
	workloadAJD  = "AJD"
	workloadAPEX = "APEX"
	workloadLH   = "LH"
	modelECPU    = "ECPU"
	modelOCPU    = "OCPU"
)

// ADB is one stored Autonomous Database.
type ADB struct {
	Created       time.Time                 `json:"created"`
	TransitionAt  *time.Time                `json:"transitionAt,omitempty"`
	WalletRotated *time.Time                `json:"walletRotated,omitempty"`
	FreeformTags  map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags   map[string]map[string]any `json:"definedTags,omitempty"`
	ID            string                    `json:"id"`
	CompartmentID string                    `json:"compartmentId"`
	DBName        string                    `json:"dbName"`
	DisplayName   string                    `json:"displayName"`
	State         string                    `json:"state"`
	Details       string                    `json:"details,omitempty"`
	Target        string                    `json:"target,omitempty"`
	Workload      string                    `json:"workload"`
	ComputeModel  string                    `json:"computeModel"`
	LicenseModel  string                    `json:"licenseModel"`
	DBVersion     string                    `json:"dbVersion"`
	CharacterSet  string                    `json:"characterSet"`
	NCharacterSet string                    `json:"ncharacterSet"`
	SubnetID      string                    `json:"subnetId,omitempty"`
	EndpointLabel string                    `json:"privateEndpointLabel,omitempty"`
	EndpointIP    string                    `json:"privateEndpointIp,omitempty"`
	Etag          string                    `json:"etag"`
	UniqueID      string                    `json:"uniqueId"`
	Whitelisted   []string                  `json:"whitelistedIps,omitempty"`
	NsgIDs        []string                  `json:"nsgIds,omitempty"`
	ComputeCount  float32                   `json:"computeCount"`
	StorageGBs    int                       `json:"storageGBs"`
	BackupDays    int                       `json:"backupRetentionDays"`
	Seq           int64                     `json:"seq"`
	IsFreeTier    bool                      `json:"isFreeTier"`
	AutoScaling   bool                      `json:"autoScaling"`
	StorageScales bool                      `json:"storageAutoScaling"`
	Mtls          bool                      `json:"mtlsRequired"`
	AccessControl bool                      `json:"accessControl"`
	Engine        bool                      `json:"engine,omitempty"`
}

// Backup is one Autonomous Database backup record.
type Backup struct {
	Started       time.Time  `json:"started"`
	Ended         *time.Time `json:"ended,omitempty"`
	AvailableTill *time.Time `json:"availableTill,omitempty"`
	TransitionAt  *time.Time `json:"transitionAt,omitempty"`
	ID            string     `json:"id"`
	CompartmentID string     `json:"compartmentId"`
	DBID          string     `json:"autonomousDatabaseId"`
	DisplayName   string     `json:"displayName"`
	Type          string     `json:"type"`
	State         string     `json:"state"`
	Details       string     `json:"details,omitempty"`
	DBVersion     string     `json:"dbVersion"`
	SizeTBs       float64    `json:"sizeInTBs"`
	Seq           int64      `json:"seq"`
	RetentionDays int        `json:"retentionDays"`
	IsAutomatic   bool       `json:"isAutomatic"`
}

// WorkRequest tracks one asynchronous operation.
type WorkRequest struct {
	Accepted     time.Time  `json:"accepted"`
	FinishAt     *time.Time `json:"finishAt,omitempty"`
	FinishedAt   *time.Time `json:"finishedAt,omitempty"`
	ID           string     `json:"id"`
	Type         string     `json:"type"`
	Compartment  string     `json:"compartmentId"`
	ResourceID   string     `json:"resourceId"`
	ResourceType string     `json:"resourceType"`
	Action       string     `json:"action"`
	Status       string     `json:"status"`
	ErrCode      string     `json:"errCode,omitempty"`
	ErrMsg       string     `json:"errMsg,omitempty"`
	Seq          int64      `json:"seq"`
}

type createIn struct {
	OcpuCount            *float64                  `json:"ocpuCount"`
	CPUCoreCount         *int                      `json:"cpuCoreCount"`
	DisplayName          *string                   `json:"displayName"`
	AdminPassword        *string                   `json:"adminPassword"`
	SecretID             *string                   `json:"secretId"`
	Source               *string                   `json:"source"`
	DBWorkload           *string                   `json:"dbWorkload"`
	ComputeModel         *string                   `json:"computeModel"`
	CompartmentID        *string                   `json:"compartmentId"`
	DBVersion            *string                   `json:"dbVersion"`
	CharacterSet         *string                   `json:"characterSet"`
	NcharacterSet        *string                   `json:"ncharacterSet"`
	SubnetID             *string                   `json:"subnetId"`
	PrivateEndpointLabel *string                   `json:"privateEndpointLabel"`
	DBName               *string                   `json:"dbName"`
	PrivateEndpointIP    *string                   `json:"privateEndpointIp"`
	LicenseModel         *string                   `json:"licenseModel"`
	ComputeCount         *float64                  `json:"computeCount"`
	StorageTBs           *int                      `json:"dataStorageSizeInTBs"`
	StorageGBs           *int                      `json:"dataStorageSizeInGBs"`
	BackupDays           *int                      `json:"backupRetentionPeriodInDays"`
	IsFreeTier           *bool                     `json:"isFreeTier"`
	IsDedicated          *bool                     `json:"isDedicated"`
	IsDevTier            *bool                     `json:"isDevTier"`
	AutoScaling          *bool                     `json:"isAutoScalingEnabled"`
	StorageAutoScaling   *bool                     `json:"isAutoScalingForStorageEnabled"`
	Mtls                 *bool                     `json:"isMtlsConnectionRequired"`
	AccessControl        *bool                     `json:"isAccessControlEnabled"`
	DefinedTags          map[string]map[string]any `json:"definedTags"`
	FreeformTags         map[string]string         `json:"freeformTags"`
	NsgIDs               []string                  `json:"nsgIds"`
	Whitelisted          []string                  `json:"whitelistedIps"`
}

type updateIn struct {
	DBName               *string                   `json:"dbName"`
	DisplayName          *string                   `json:"displayName"`
	AdminPassword        *string                   `json:"adminPassword"`
	SecretID             *string                   `json:"secretId"`
	DBWorkload           *string                   `json:"dbWorkload"`
	ComputeModel         *string                   `json:"computeModel"`
	LicenseModel         *string                   `json:"licenseModel"`
	DBVersion            *string                   `json:"dbVersion"`
	SubnetID             *string                   `json:"subnetId"`
	PrivateEndpointLabel *string                   `json:"privateEndpointLabel"`
	PrivateEndpointIP    *string                   `json:"privateEndpointIp"`
	ComputeCount         *float64                  `json:"computeCount"`
	OcpuCount            *float64                  `json:"ocpuCount"`
	CPUCoreCount         *int                      `json:"cpuCoreCount"`
	StorageTBs           *int                      `json:"dataStorageSizeInTBs"`
	StorageGBs           *int                      `json:"dataStorageSizeInGBs"`
	BackupDays           *int                      `json:"backupRetentionPeriodInDays"`
	IsFreeTier           *bool                     `json:"isFreeTier"`
	AutoScaling          *bool                     `json:"isAutoScalingEnabled"`
	StorageAutoScaling   *bool                     `json:"isAutoScalingForStorageEnabled"`
	Mtls                 *bool                     `json:"isMtlsConnectionRequired"`
	AccessControl        *bool                     `json:"isAccessControlEnabled"`
	Whitelisted          *[]string                 `json:"whitelistedIps"`
	NsgIDs               *[]string                 `json:"nsgIds"`
	FreeformTags         map[string]string         `json:"freeformTags"`
	DefinedTags          map[string]map[string]any `json:"definedTags"`
}

type stopIn struct {
	Force *bool `json:"isForceStop"`
}

type restartIn struct {
	Force *bool `json:"isForceRestart"`
}

type restoreIn struct {
	Timestamp *time.Time `json:"timestamp"`
	SCN       *string    `json:"databaseSCN"`
	Latest    *bool      `json:"latest"`
}

type compartmentIn struct {
	Compartment string `json:"compartmentId"`
}

type walletIn struct {
	Password     *string `json:"password"`
	GenerateType *string `json:"generateType"`
	IsRegional   *bool   `json:"isRegional"`
}

type walletUpdateIn struct {
	ShouldRotate *bool `json:"shouldRotate"`
	GracePeriod  *int  `json:"gracePeriod"`
}

type backupIn struct {
	DBID          *string `json:"autonomousDatabaseId"`
	DisplayName   *string `json:"displayName"`
	RetentionDays *int    `json:"retentionPeriodInDays"`
	LongTerm      *bool   `json:"isLongTermBackup"`
}

// Backup types (SDK AutonomousDatabaseBackupTypeEnum).
const (
	backupIncremental = "INCREMENTAL"
	backupLongTerm    = "LONGTERM"
)
