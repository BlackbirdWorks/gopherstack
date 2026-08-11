package backup

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	VaultTypeBackupVault = "BACKUP_VAULT"
	VaultTypeAirGapped   = "LOGICALLY_AIR_GAPPED_BACKUP_VAULT"
)

// Lifecycle holds backup retention lifecycle settings for a rule or recovery point.
type Lifecycle struct {
	MoveToColdStorageAfterDays          int64 `json:"moveToColdStorageAfterDays,omitempty"`
	DeleteAfterDays                     int64 `json:"deleteAfterDays,omitempty"`
	OptInToArchiveForSupportedResources bool  `json:"optInToArchiveForSupportedResources,omitempty"`
}

// CalculatedLifecycle holds computed lifecycle transition timestamps for a recovery point.
type CalculatedLifecycle struct {
	MoveToColdStorageAt *time.Time `json:"moveToColdStorageAt,omitempty"`
	DeleteAt            *time.Time `json:"deleteAt,omitempty"`
}

// CopyAction defines a cross-vault copy triggered by a backup rule.
type CopyAction struct {
	DestinationBackupVaultArn string    `json:"destinationBackupVaultArn"`
	Lifecycle                 Lifecycle `json:"lifecycle,omitzero"`
}

// TagCondition is a single tag-based resource selection filter.
type TagCondition struct {
	ConditionType  string `json:"conditionType"`
	ConditionKey   string `json:"conditionKey"`
	ConditionValue string `json:"conditionValue"`
}

// StringCondition holds a single key/value match condition for resource selection.
type StringCondition struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SelectionConditions holds fine-grained string-match conditions for resource selection.
type SelectionConditions struct {
	StringEquals    []StringCondition `json:"stringEquals,omitempty"`
	StringLike      []StringCondition `json:"stringLike,omitempty"`
	StringNotEquals []StringCondition `json:"stringNotEquals,omitempty"`
	StringNotLike   []StringCondition `json:"stringNotLike,omitempty"`
}

// AdvancedBackupSetting enables resource-type-specific backup options (e.g., Windows VSS).
type AdvancedBackupSetting struct {
	BackupOptions map[string]string `json:"backupOptions,omitempty"`
	ResourceType  string            `json:"resourceType"`
}

// ControlInputParameter is a single name/value pair configuring a framework
// control (e.g. ParameterName "requiredRetentionDays", ParameterValue "35").
type ControlInputParameter struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
}

// ControlScope defines what a framework control evaluates: specific
// resource IDs, resource types, and/or a single tag key/value pair.
type ControlScope struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	ComplianceResourceIDs   []string          `json:"complianceResourceIds,omitempty"`
	ComplianceResourceTypes []string          `json:"complianceResourceTypes,omitempty"`
}

// FrameworkControl represents a compliance control within an audit framework.
type FrameworkControl struct {
	ControlScope           *ControlScope           `json:"controlScope,omitempty"`
	ControlName            string                  `json:"controlName"`
	ControlInputParameters []ControlInputParameter `json:"controlInputParameters,omitempty"`
}

// ReportDeliveryChannel specifies the S3 destination and format for report output.
type ReportDeliveryChannel struct {
	S3BucketName string   `json:"s3BucketName"`
	S3KeyPrefix  string   `json:"s3KeyPrefix,omitempty"`
	Formats      []string `json:"formats,omitempty"`
}

// ReportSetting specifies the template, frameworks, and account/OU/Region
// scope driving a report plan.
type ReportSetting struct {
	ReportTemplate     string   `json:"reportTemplate"`
	FrameworkArns      []string `json:"frameworkArns,omitempty"`
	Accounts           []string `json:"accounts,omitempty"`
	OrganizationUnits  []string `json:"organizationUnits,omitempty"`
	Regions            []string `json:"regions,omitempty"`
	NumberOfFrameworks int32    `json:"numberOfFrameworks,omitempty"`
}

// Vault represents an AWS Backup vault.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateBackupVault.
type Vault struct {
	CreationTime           time.Time  `json:"creationTime"`
	Tags                   *tags.Tags `json:"tags,omitempty"`
	BackupVaultName        string     `json:"backupVaultName"`
	BackupVaultArn         string     `json:"backupVaultArn"`
	EncryptionKeyArn       string     `json:"encryptionKeyArn,omitempty"`
	CreatorRequestID       string     `json:"creatorRequestId,omitempty"`
	VaultType              string     `json:"vaultType,omitempty"`
	AccountID              string     `json:"accountId"`
	Region                 string     `json:"region"`
	NumberOfRecoveryPoints int64      `json:"numberOfRecoveryPoints"`
	MinRetentionDays       int64      `json:"minRetentionDays,omitempty"`
	MaxRetentionDays       int64      `json:"maxRetentionDays,omitempty"`
}

// Rule represents a single rule in a backup plan.
type Rule struct {
	RecoveryPointTags          map[string]string `json:"recoveryPointTags,omitempty"`
	Lifecycle                  *Lifecycle        `json:"lifecycle,omitempty"`
	RuleName                   string            `json:"ruleName"`
	RuleID                     string            `json:"ruleId,omitempty"`
	TargetVaultName            string            `json:"targetVaultName"`
	ScheduleExpression         string            `json:"scheduleExpression,omitempty"`
	ScheduleExpressionTimezone string            `json:"scheduleExpressionTimezone,omitempty"`
	CopyActions                []CopyAction      `json:"copyActions,omitempty"`
	StartWindowMinutes         int64             `json:"startWindowMinutes,omitempty"`
	CompletionWindowMinutes    int64             `json:"completionWindowMinutes,omitempty"`
	EnableContinuousBackup     bool              `json:"enableContinuousBackup,omitempty"`
}

// Plan represents an AWS Backup plan.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateBackupPlan.
type Plan struct {
	CreationTime           time.Time               `json:"creationTime"`
	UpdateTime             *time.Time              `json:"updateTime,omitempty"`
	Tags                   *tags.Tags              `json:"tags,omitempty"`
	BackupPlanName         string                  `json:"backupPlanName"`
	BackupPlanArn          string                  `json:"backupPlanArn"`
	BackupPlanID           string                  `json:"backupPlanId"`
	VersionID              string                  `json:"versionId"`
	AccountID              string                  `json:"accountId"`
	Region                 string                  `json:"region"`
	Rules                  []Rule                  `json:"rules"`
	AdvancedBackupSettings []AdvancedBackupSetting `json:"advancedBackupSettings,omitempty"`
}

// Job represents an AWS Backup job.
type Job struct {
	CreationTime              time.Time  `json:"creationTime"`
	CompletionTime            *time.Time `json:"completionTime,omitempty"`
	ExpectedCompletionDate    *time.Time `json:"expectedCompletionDate,omitempty"`
	StartBy                   *time.Time `json:"startBy,omitempty"`
	ResourceArn               string     `json:"resourceArn,omitempty"`
	BackupJobID               string     `json:"backupJobId"`
	BackupVaultName           string     `json:"backupVaultName"`
	BackupVaultArn            string     `json:"backupVaultArn"`
	ResourceType              string     `json:"resourceType,omitempty"`
	IAMRoleArn                string     `json:"iamRoleArn,omitempty"`
	State                     string     `json:"state"`
	AccountID                 string     `json:"accountId"`
	Region                    string     `json:"region"`
	RecoveryPointArn          string     `json:"recoveryPointArn,omitempty"`
	PercentDone               string     `json:"percentDone,omitempty"`
	MessageCategory           string     `json:"messageCategory,omitempty"`
	ParentJobID               string     `json:"parentJobId,omitempty"`
	CompositeMemberIdentifier string     `json:"compositeMemberIdentifier,omitempty"`
	BytesTransferred          int64      `json:"bytesTransferred,omitempty"`
	BackupSizeInBytes         int64      `json:"backupSizeInBytes,omitempty"`
	IsParent                  bool       `json:"isParent,omitempty"`
}

// Selection represents an AWS Backup selection (resources assigned to a plan).
type Selection struct {
	CreationTime  time.Time            `json:"creationTime"`
	Conditions    *SelectionConditions `json:"conditions,omitempty"`
	SelectionName string               `json:"selectionName"`
	SelectionID   string               `json:"selectionId"`
	BackupPlanID  string               `json:"backupPlanId"`
	IAMRoleArn    string               `json:"iamRoleArn,omitempty"`
	Resources     []string             `json:"resources,omitempty"`
	NotResources  []string             `json:"notResources,omitempty"`
	ListOfTags    []TagCondition       `json:"listOfTags,omitempty"`
}

// Framework represents an AWS Backup audit framework.
type Framework struct {
	CreationTime         time.Time          `json:"creationTime"`
	Tags                 *tags.Tags         `json:"tags,omitempty"`
	FrameworkName        string             `json:"frameworkName"`
	FrameworkArn         string             `json:"frameworkArn"`
	FrameworkDescription string             `json:"frameworkDescription,omitempty"`
	FrameworkStatus      string             `json:"frameworkStatus,omitempty"`
	DeploymentStatus     string             `json:"deploymentStatus,omitempty"`
	FrameworkControls    []FrameworkControl `json:"frameworkControls,omitempty"`
}

// LegalHold represents an AWS Backup legal hold.
type LegalHold struct {
	CreationDate           time.Time               `json:"creationDate"`
	RecoveryPointSelection *RecoveryPointSelection `json:"recoveryPointSelection,omitempty"`
	Title                  string                  `json:"title"`
	Description            string                  `json:"description"`
	LegalHoldID            string                  `json:"legalHoldId"`
	LegalHoldArn           string                  `json:"legalHoldArn"`
	Status                 string                  `json:"status"`
}

// DateRange is an inclusive Unix-time window ([FromDate, ToDate]) used to
// filter recovery points by CreationDate.
type DateRange struct {
	FromDate *time.Time `json:"fromDate,omitempty"`
	ToDate   *time.Time `json:"toDate,omitempty"`
}

// RecoveryPointSelection specifies which recovery points a legal hold
// applies to: by containing vault, by resource ARN, and/or by creation-date
// window. A CreateLegalHold call with no selection (or an empty one) covers
// every recovery point -- there is no additional constraint to apply.
type RecoveryPointSelection struct {
	DateRange           *DateRange `json:"dateRange,omitempty"`
	ResourceIdentifiers []string   `json:"resourceIdentifiers,omitempty"`
	VaultNames          []string   `json:"vaultNames,omitempty"`
}

// ReportPlan represents an AWS Backup report plan.
type ReportPlan struct {
	Tags                  *tags.Tags             `json:"tags,omitempty"`
	ReportDeliveryChannel *ReportDeliveryChannel `json:"reportDeliveryChannel,omitempty"`
	ReportSetting         *ReportSetting         `json:"reportSetting,omitempty"`
	CreationTime          time.Time              `json:"creationTime"`
	ReportPlanName        string                 `json:"reportPlanName"`
	ReportPlanArn         string                 `json:"reportPlanArn"`
	ReportPlanDescription string                 `json:"reportPlanDescription,omitempty"`
}

// RestoreAccessVault represents an AWS Backup restore access backup vault.
type RestoreAccessVault struct {
	CreationDate                 time.Time `json:"creationDate"`
	RestoreAccessBackupVaultName string    `json:"restoreAccessBackupVaultName"`
	RestoreAccessBackupVaultArn  string    `json:"restoreAccessBackupVaultArn"`
	SourceBackupVaultArn         string    `json:"sourceBackupVaultArn"`
	// SourceBackupVaultName is not on the AWS wire shape; it is resolved from
	// SourceBackupVaultArn at creation time so ListRestoreAccessBackupVaults /
	// RevokeRestoreAccessBackupVault -- which AWS addresses by source vault
	// NAME, not ARN (see their real nested paths under
	// /logically-air-gapped-backup-vaults/{BackupVaultName}/...) -- can filter
	// without re-parsing the ARN on every call.
	SourceBackupVaultName string `json:"sourceBackupVaultName"`
	VaultState            string `json:"vaultState"`
}

// RestoreTestingPlan represents an AWS Backup restore testing plan.
type RestoreTestingPlan struct {
	CreationTime           time.Time `json:"creationTime"`
	RestoreTestingPlanName string    `json:"restoreTestingPlanName"`
	RestoreTestingPlanArn  string    `json:"restoreTestingPlanArn"`
	ScheduleExpression     string    `json:"scheduleExpression,omitempty"`
	StartWindowHours       int64     `json:"startWindowHours,omitempty"`
}

// RestoreTestingSelection represents a selection within a restore testing plan.
type RestoreTestingSelection struct {
	CreationTime                time.Time                    `json:"creationTime"`
	ProtectedResourceConditions *ProtectedResourceConditions `json:"protectedResourceConditions,omitempty"`
	RestoreMetadataOverrides    map[string]string            `json:"restoreMetadataOverrides,omitempty"`
	RestoreTestingPlanName      string                       `json:"restoreTestingPlanName"`
	RestoreTestingSelectionName string                       `json:"restoreTestingSelectionName"`
	RestoreTestingPlanArn       string                       `json:"restoreTestingPlanArn"`
	ProtectedResourceType       string                       `json:"protectedResourceType,omitempty"`
	// IAMRoleArn is required by real AWS (StartRestoreJob's IamRoleArn for
	// scheduled restore tests uses this role) but was entirely absent from
	// this backend's model until this pass.
	IAMRoleArn            string   `json:"iamRoleArn,omitempty"`
	ProtectedResourceArns []string `json:"protectedResourceArns,omitempty"`
	ValidationWindowHours int64    `json:"validationWindowHours,omitempty"`
}

// KeyValue is a single tag key/value pair used by ProtectedResourceConditions.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ProtectedResourceConditions filters a restore testing selection's
// wildcard ProtectedResourceArns (["*"]) down to resources matching (or not
// matching) specific tag key/value pairs.
type ProtectedResourceConditions struct {
	StringEquals    []KeyValue `json:"stringEquals,omitempty"`
	StringNotEquals []KeyValue `json:"stringNotEquals,omitempty"`
}

// RestoreTestingSelectionInput holds the fields accepted by
// CreateRestoreTestingSelection/UpdateRestoreTestingSelection beyond the
// plan/selection name -- mirrors AWS's RestoreTestingSelectionForCreate /
// RestoreTestingSelectionForUpdate closely enough that both ops can share
// one Go-side type.
type RestoreTestingSelectionInput struct {
	ProtectedResourceConditions *ProtectedResourceConditions
	RestoreMetadataOverrides    map[string]string
	ProtectedResourceType       string
	IAMRoleArn                  string
	ProtectedResourceArns       []string
	ValidationWindowHours       int64
}

// RecoveryPoint represents an AWS Backup recovery point.
type RecoveryPoint struct {
	CreationDate              time.Time            `json:"creationDate"`
	CompletionDate            *time.Time           `json:"completionDate,omitempty"`
	Lifecycle                 *Lifecycle           `json:"lifecycle,omitempty"`
	CalculatedLifecycle       *CalculatedLifecycle `json:"calculatedLifecycle,omitempty"`
	RecoveryPointArn          string               `json:"recoveryPointArn"`
	BackupVaultName           string               `json:"backupVaultName"`
	BackupVaultArn            string               `json:"backupVaultArn"`
	ResourceArn               string               `json:"resourceArn,omitempty"`
	ResourceType              string               `json:"resourceType,omitempty"`
	IAMRoleArn                string               `json:"iamRoleArn,omitempty"`
	Status                    string               `json:"status"`
	StorageClass              string               `json:"storageClass,omitempty"`
	EncryptionKeyArn          string               `json:"encryptionKeyArn,omitempty"`
	ParentRecoveryPointArn    string               `json:"parentRecoveryPointArn,omitempty"`
	CompositeMemberIdentifier string               `json:"compositeMemberIdentifier,omitempty"`
	SourceBackupVaultArn      string               `json:"sourceBackupVaultArn,omitempty"`
	BackupSizeInBytes         int64                `json:"backupSizeInBytes,omitempty"`
	IsEncrypted               bool                 `json:"isEncrypted,omitempty"`
}

// CopyJob represents an AWS Backup copy job.
type CopyJob struct {
	CreationDate                time.Time  `json:"creationDate"`
	CompletionDate              *time.Time `json:"completionDate,omitempty"`
	CopyJobID                   string     `json:"copyJobId"`
	SourceBackupVaultArn        string     `json:"sourceBackupVaultArn,omitempty"`
	DestinationBackupVaultArn   string     `json:"destinationBackupVaultArn,omitempty"`
	DestinationRecoveryPointArn string     `json:"destinationRecoveryPointArn,omitempty"`
	ResourceArn                 string     `json:"resourceArn,omitempty"`
	ResourceType                string     `json:"resourceType,omitempty"`
	IAMRoleArn                  string     `json:"iamRoleArn,omitempty"`
	State                       string     `json:"state"`
	AccountID                   string     `json:"accountId"`
	Region                      string     `json:"region"`
}

// VaultAccessPolicy holds an access policy document for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself, and carries a
// real json tag (not "-") since this table is persisted -- see store_setup.go.
type VaultAccessPolicy struct {
	VaultName string `json:"vaultName"`
	Policy    string `json:"policy"`
}

// VaultLockConfig holds the lock configuration for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself, and carries a
// real json tag (not "-") since this table is persisted -- see store_setup.go.
type VaultLockConfig struct {
	LockDate          *time.Time `json:"lockDate,omitempty"`
	VaultName         string     `json:"vaultName"`
	MinRetentionDays  int64      `json:"minRetentionDays,omitempty"`
	MaxRetentionDays  int64      `json:"maxRetentionDays,omitempty"`
	ChangeableForDays int64      `json:"changeableForDays,omitempty"`
}

// VaultNotificationConfig holds notification settings for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself, and carries a
// real json tag (not "-") since this table is persisted -- see store_setup.go.
type VaultNotificationConfig struct {
	VaultName         string   `json:"vaultName"`
	SNSTopicArn       string   `json:"snsTopicArn"`
	BackupVaultEvents []string `json:"backupVaultEvents"`
}

// InMemoryBackend is the in-memory store for AWS Backup resources.
//
// Resource collections are *store.Table[T] (see pkgs/store's package doc),
// every one registered on registry so Snapshot/Restore covers it -- see
// store_setup.go. A handful of maps remain plain map[string]string because
// their values are not *T (mirroring services/ses's "policies" precedent).
type InMemoryBackend struct {
	regionSettings                 *RegionSettings
	mu                             *lockmetrics.RWMutex
	registry                       *store.Registry
	vaults                         *store.Table[Vault]
	plans                          *store.Table[Plan]
	jobs                           *store.Table[Job]
	selections                     *store.Table[Selection] // composite key: planID#selectionID
	selectionsByPlan               *store.Index[Selection] // grouped by BackupPlanID
	frameworks                     *store.Table[Framework]
	legalHolds                     *store.Table[LegalHold]
	reportPlans                    *store.Table[ReportPlan]
	restoreAccessVaults            *store.Table[RestoreAccessVault]
	restoreTestingPlans            *store.Table[RestoreTestingPlan]
	restoreTestingSelections       *store.Table[RestoreTestingSelection] // composite key: planName#selectionName
	restoreTestingSelectionsByPlan *store.Index[RestoreTestingSelection] // grouped by RestoreTestingPlanName
	recoveryPoints                 *store.Table[RecoveryPoint]           // composite key: vaultName#rpArn
	recoveryPointsByVault          *store.Index[RecoveryPoint]           // grouped by BackupVaultName
	copyJobs                       *store.Table[CopyJob]
	vaultAccessPolicies            *store.Table[VaultAccessPolicy]
	vaultLockConfigs               *store.Table[VaultLockConfig]
	vaultNotifications             *store.Table[VaultNotificationConfig]
	mpaApprovals                   map[string]string // vaultName → mpaApprovalTeamArn
	vaultARNIndex                  map[string]string // ARN → vault name
	planARNIndex                   map[string]string // ARN → plan name
	planIDIndex                    map[string]string // plan ID → plan name
	frameworkARNIndex              map[string]string // ARN → framework name
	reportPlanARNIndex             map[string]string // ARN → report plan name
	restoreJobs                    *store.Table[RestoreJob]
	reportJobs                     *store.Table[ReportJob]
	scanJobs                       *store.Table[ScanJob]
	tieringConfigs                 *store.Table[TieringConfiguration]
	protectedResources             *store.Table[ProtectedResource]
	globalSettings                 map[string]string
	recoveryPointIndexStatus       map[string]string // vaultName:rpArn → index status
	globalSettingsLastUpdate       time.Time
	accountID                      string
	region                         string
}

// RestoreJob represents an AWS Backup restore job.
type RestoreJob struct {
	CompletionDate   *time.Time        `json:"completionDate,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	StartTime        time.Time         `json:"startTime"`
	RestoreJobID     string            `json:"restoreJobId"`
	RecoveryPointArn string            `json:"recoveryPointArn"`
	IAMRoleArn       string            `json:"iamRoleArn"`
	ResourceArn      string            `json:"resourceArn,omitempty"`
	ResourceType     string            `json:"resourceType,omitempty"`
	BackupVaultName  string            `json:"backupVaultName,omitempty"`
	BackupVaultArn   string            `json:"backupVaultArn,omitempty"`
	AccountID        string            `json:"accountId,omitempty"`
	// CreatedResourceArn is the ARN of the resource StartRestoreJob created
	// once the restore completes -- real AWS would provision an actual new
	// resource of ResourceType; this emulator synthesizes a plausible ARN
	// instead (see restore_jobs.go's StartRestoreJob).
	CreatedResourceArn      string `json:"createdResourceArn,omitempty"`
	Status                  string `json:"status"`
	StatusMessage           string `json:"statusMessage,omitempty"`
	PercentDone             string `json:"percentDone,omitempty"`
	ValidationStatus        string `json:"validationStatus,omitempty"`
	ValidationStatusMessage string `json:"validationStatusMessage,omitempty"`
	BackupSizeInBytes       int64  `json:"backupSizeInBytes,omitempty"`
}

// ReportJob represents an AWS Backup report job.
type ReportJob struct {
	CreationTime   time.Time  `json:"creationTime"`
	CompletionTime *time.Time `json:"completionTime,omitempty"`
	ReportJobID    string     `json:"reportJobId"`
	ReportPlanArn  string     `json:"reportPlanArn"`
	Status         string     `json:"status"`
}

// ScanJob represents an AWS Backup restore testing scan job.
type ScanJob struct {
	CreationTime   time.Time  `json:"creationTime"`
	CompletionTime *time.Time `json:"completionTime,omitempty"`
	ScanJobID      string     `json:"scanJobId"`
	BackupVaultArn string     `json:"backupVaultArn"`
	Status         string     `json:"status"`
}

// ResourceSelection specifies which resources within a vault a tiering
// configuration applies to, and the age threshold (in days) after which
// matching objects transition to the low-cost warm storage tier.
type ResourceSelection struct {
	ResourceType              string   `json:"resourceType"`
	Resources                 []string `json:"resources"`
	TieringDownSettingsInDays int64    `json:"tieringDownSettingsInDays"`
}

// TieringConfiguration holds a tiering configuration.
//
// Keyed by TieringConfigurationName (matching AWS -- CreateTieringConfigurationInput's
// TieringConfigurationInputForCreate nests BackupVaultName + ResourceSelection
// under a top-level TieringConfigurationName, and Get/List/Delete/Update all
// address configurations by that name, never by vault name).
type TieringConfiguration struct {
	CreationTime             time.Time           `json:"creationTime"`
	LastUpdatedTime          *time.Time          `json:"lastUpdatedTime,omitempty"`
	TieringConfigurationName string              `json:"tieringConfigurationName"`
	TieringConfigurationArn  string              `json:"tieringConfigurationArn"`
	BackupVaultName          string              `json:"backupVaultName"`
	CreatorRequestID         string              `json:"creatorRequestId,omitempty"`
	ResourceSelection        []ResourceSelection `json:"resourceSelection"`
}

// ProtectedResource represents a resource protected by AWS Backup.
type ProtectedResource struct {
	LastBackupTime  time.Time `json:"lastBackupTime"`
	ResourceArn     string    `json:"resourceArn"`
	ResourceName    string    `json:"resourceName,omitempty"`
	ResourceType    string    `json:"resourceType"`
	BackupVaultName string    `json:"backupVaultName,omitempty"`
}

// RegionSettings holds per-region backup preferences.
type RegionSettings struct {
	ResourceTypeManagementPreference map[string]bool `json:"resourceTypeManagementPreference"`
	ResourceTypeOptInPreference      map[string]bool `json:"resourceTypeOptInPreference"`
}

// ---- Global/Region settings ----
