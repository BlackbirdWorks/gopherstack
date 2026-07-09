package backup

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	VaultTypeBackupVault = "BACKUP_VAULT"
	VaultTypeAirGapped   = "LOGICALLY_AIR_GAPPED_BACKUP_VAULT"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	// vaultNameRe matches valid vault names: 2–50 alphanumeric, hyphen, or underscore chars.
	vaultNameRe = regexp.MustCompile(`^[a-zA-Z0-9_\-]{2,50}$`)

	// validVaultEvents is the canonical set of AWS Backup vault notification events.
	validVaultEvents = map[string]struct{}{ //nolint:gochecknoglobals // package-level lookup table
		"BACKUP_JOB_STARTED":       {},
		"BACKUP_JOB_COMPLETED":     {},
		"BACKUP_JOB_SUCCESSFUL":    {},
		"BACKUP_JOB_FAILED":        {},
		"BACKUP_JOB_EXPIRED":       {},
		"RESTORE_JOB_STARTED":      {},
		"RESTORE_JOB_COMPLETED":    {},
		"RESTORE_JOB_SUCCESSFUL":   {},
		"RESTORE_JOB_FAILED":       {},
		"COPY_JOB_STARTED":         {},
		"COPY_JOB_SUCCESSFUL":      {},
		"COPY_JOB_FAILURE":         {},
		"RECOVERY_POINT_MODIFIED":  {},
		"BACKUP_PLAN_CREATED":      {},
		"BACKUP_PLAN_MODIFIED":     {},
		"S3_BACKUP_OBJECT_FAILED":  {},
		"S3_RESTORE_OBJECT_FAILED": {},
	}
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

// FrameworkControl represents a compliance control within an audit framework.
type FrameworkControl struct {
	ControlInputParameters map[string]string `json:"controlInputParameters,omitempty"`
	ControlScope           map[string]any    `json:"controlScope,omitempty"`
	ControlName            string            `json:"controlName"`
}

// ReportDeliveryChannel specifies the S3 destination and format for report output.
type ReportDeliveryChannel struct {
	S3BucketName string   `json:"s3BucketName"`
	Formats      []string `json:"formats,omitempty"`
}

// ReportSetting specifies the template and frameworks driving a report plan.
type ReportSetting struct {
	ReportTemplate string   `json:"reportTemplate"`
	FrameworkArns  []string `json:"frameworkArns,omitempty"`
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
	CreationDate time.Time `json:"creationDate"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	LegalHoldID  string    `json:"legalHoldId"`
	LegalHoldArn string    `json:"legalHoldArn"`
	Status       string    `json:"status"`
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
	VaultState                   string    `json:"vaultState"`
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
	CreationTime                time.Time `json:"creationTime"`
	RestoreTestingPlanName      string    `json:"restoreTestingPlanName"`
	RestoreTestingSelectionName string    `json:"restoreTestingSelectionName"`
	RestoreTestingPlanArn       string    `json:"restoreTestingPlanArn"`
	ProtectedResourceType       string    `json:"protectedResourceType,omitempty"`
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
	CreationDate              time.Time  `json:"creationDate"`
	CompletionDate            *time.Time `json:"completionDate,omitempty"`
	CopyJobID                 string     `json:"copyJobId"`
	SourceBackupVaultArn      string     `json:"sourceBackupVaultArn,omitempty"`
	DestinationBackupVaultArn string     `json:"destinationBackupVaultArn,omitempty"`
	ResourceArn               string     `json:"resourceArn,omitempty"`
	ResourceType              string     `json:"resourceType,omitempty"`
	IAMRoleArn                string     `json:"iamRoleArn,omitempty"`
	State                     string     `json:"state"`
	AccountID                 string     `json:"accountId"`
	Region                    string     `json:"region"`
}

// VaultAccessPolicy holds an access policy document for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself (this table is
// never persisted, so a json:"-" key field carries no round-trip risk --
// see store_setup.go).
type VaultAccessPolicy struct {
	VaultName string `json:"-"`
	Policy    string `json:"policy"`
}

// VaultLockConfig holds the lock configuration for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself (this table is
// never persisted, so a json:"-" key field carries no round-trip risk --
// see store_setup.go).
type VaultLockConfig struct {
	LockDate          *time.Time `json:"lockDate,omitempty"`
	VaultName         string     `json:"-"`
	MinRetentionDays  int64      `json:"minRetentionDays,omitempty"`
	MaxRetentionDays  int64      `json:"maxRetentionDays,omitempty"`
	ChangeableForDays int64      `json:"changeableForDays,omitempty"`
}

// VaultNotificationConfig holds notification settings for a backup vault.
//
// VaultName is not part of the AWS wire shape; it exists purely so
// store.Table's keyFn can derive a key from the value itself (this table is
// never persisted, so a json:"-" key field carries no round-trip risk --
// see store_setup.go).
type VaultNotificationConfig struct {
	VaultName         string   `json:"-"`
	SNSTopicArn       string   `json:"snsTopicArn"`
	BackupVaultEvents []string `json:"backupVaultEvents"`
}

// InMemoryBackend is the in-memory store for AWS Backup resources.
//
// Resource collections are *store.Table[T] (see pkgs/store's package doc).
// PERSISTED tables are registered on registry; VOLATILE tables (never part
// of backendSnapshot) are constructed but deliberately not registered --
// see store_setup.go for the full split and rationale. A handful of maps
// remain plain map[string]string because their values are not *T (mirroring
// services/ses's "policies" precedent).
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
	recoveryPoints                 *store.Table[RecoveryPoint]           // composite key: vaultName#rpArn; VOLATILE
	recoveryPointsByVault          *store.Index[RecoveryPoint]           // grouped by BackupVaultName
	copyJobs                       *store.Table[CopyJob]                 // VOLATILE
	vaultAccessPolicies            *store.Table[VaultAccessPolicy]       // VOLATILE
	vaultLockConfigs               *store.Table[VaultLockConfig]         // VOLATILE
	vaultNotifications             *store.Table[VaultNotificationConfig] // VOLATILE
	mpaApprovals                   map[string]string                     // vaultName → mpaApprovalTeamArn
	vaultARNIndex                  map[string]string                     // ARN → vault name
	planARNIndex                   map[string]string                     // ARN → plan name
	planIDIndex                    map[string]string                     // plan ID → plan name
	frameworkARNIndex              map[string]string                     // ARN → framework name
	reportPlanARNIndex             map[string]string                     // ARN → report plan name
	// batch1 additions (all VOLATILE -- never part of backendSnapshot)
	restoreJobs              *store.Table[RestoreJob]
	reportJobs               *store.Table[ReportJob]
	scanJobs                 *store.Table[ScanJob]
	tieringConfigs           *store.Table[TieringConfiguration]
	protectedResources       *store.Table[ProtectedResource]
	globalSettings           map[string]string
	recoveryPointLifecycle   map[string]string // vaultName:rpArn → lifecycle spec
	recoveryPointIndexStatus map[string]string // vaultName:rpArn → index status
	restoreValidations       map[string]string // restoreJobID → validation status
	globalSettingsLastUpdate time.Time
	accountID                string
	region                   string
}

// NewInMemoryBackend creates a new in-memory Backup backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                 store.NewRegistry(),
		mpaApprovals:             make(map[string]string),
		vaultARNIndex:            make(map[string]string),
		planARNIndex:             make(map[string]string),
		planIDIndex:              make(map[string]string),
		frameworkARNIndex:        make(map[string]string),
		reportPlanARNIndex:       make(map[string]string),
		globalSettings:           make(map[string]string),
		recoveryPointLifecycle:   make(map[string]string),
		recoveryPointIndexStatus: make(map[string]string),
		restoreValidations:       make(map[string]string),
		accountID:                accountID,
		region:                   region,
		mu:                       lockmetrics.New("backup"),
	}

	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// isValidVaultName reports whether name is an acceptable AWS Backup vault name:
// 2–50 alphanumeric or hyphen characters.
func isValidVaultName(name string) bool {
	return vaultNameRe.MatchString(name)
}

// CreateBackupVault creates a new backup vault.
func (b *InMemoryBackend) CreateBackupVault(
	name, encryptionKeyArn, creatorRequestID string,
	kv map[string]string,
) (*Vault, error) {
	b.mu.Lock("CreateBackupVault")
	defer b.mu.Unlock()

	if !isValidVaultName(name) {
		return nil, fmt.Errorf(
			"%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters",
			ErrValidation,
		)
	}

	if existing, ok := b.vaults.Get(name); ok {
		// Idempotent create: same CreatorRequestId returns existing vault.
		if creatorRequestID != "" && existing.CreatorRequestID == creatorRequestID {
			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf("%w: vault %s already exists", ErrAlreadyExists, name)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "backup-vault:"+name)
	t := tags.New("backup.vault." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	v := &Vault{
		BackupVaultName:  name,
		BackupVaultArn:   vaultARN,
		EncryptionKeyArn: encryptionKeyArn,
		CreatorRequestID: creatorRequestID,
		VaultType:        VaultTypeBackupVault,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.vaults.Put(v)
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// DescribeBackupVault returns a vault by name.
func (b *InMemoryBackend) DescribeBackupVault(name string) (*Vault, error) {
	b.mu.RLock("DescribeBackupVault")
	defer b.mu.RUnlock()

	v, ok := b.vaults.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}
	cp := *v

	return &cp, nil
}

// ListBackupVaults returns all backup vaults sorted by name.
func (b *InMemoryBackend) ListBackupVaults() []*Vault {
	b.mu.RLock("ListBackupVaults")
	defer b.mu.RUnlock()

	all := b.vaults.All()
	list := make([]*Vault, 0, len(all))
	for _, v := range all {
		cp := *v
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Vault) int {
		if a.BackupVaultName < b.BackupVaultName {
			return -1
		}
		if a.BackupVaultName > b.BackupVaultName {
			return 1
		}

		return 0
	})

	return list
}

// DeleteBackupVault deletes a vault by name.
func (b *InMemoryBackend) DeleteBackupVault(name string) error {
	b.mu.Lock("DeleteBackupVault")
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(name)
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}

	if v.NumberOfRecoveryPoints > 0 {
		return fmt.Errorf("%w: vault %s has %d recovery points; delete them first",
			ErrValidation, name, v.NumberOfRecoveryPoints)
	}

	delete(b.vaultARNIndex, v.BackupVaultArn)
	b.vaults.Delete(name)
	v.Tags.Close()

	return nil
}

// CreateBackupPlan creates a new backup plan.
func (b *InMemoryBackend) CreateBackupPlan(
	planName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
	kv map[string]string,
) (*Plan, error) {
	b.mu.Lock("CreateBackupPlan")
	defer b.mu.Unlock()

	if b.plans.Has(planName) {
		return nil, fmt.Errorf("%w: plan %s already exists", ErrAlreadyExists, planName)
	}

	id := uuid.NewString()
	planARN := arn.Build("backup", b.region, b.accountID, "backup-plan:"+id)
	t := tags.New("backup.plan." + planName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	p := &Plan{
		BackupPlanName:         planName,
		BackupPlanArn:          planARN,
		BackupPlanID:           id,
		VersionID:              uuid.NewString(),
		Rules:                  rules,
		AdvancedBackupSettings: advancedSettings,
		AccountID:              b.accountID,
		Region:                 b.region,
		CreationTime:           time.Now().UTC(),
		Tags:                   t,
	}
	b.plans.Put(p)
	b.planARNIndex[planARN] = planName
	b.planIDIndex[id] = planName
	cp := *p
	cp.Rules = make([]Rule, len(p.Rules))
	copy(cp.Rules, p.Rules)

	return &cp, nil
}

// GetBackupPlan returns a backup plan by ID or name.
func (b *InMemoryBackend) GetBackupPlan(idOrName string) (*Plan, error) {
	b.mu.RLock("GetBackupPlan")
	defer b.mu.RUnlock()

	// Try by name first.
	if p, ok := b.plans.Get(idOrName); ok {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)

		return &cp, nil
	}
	// Try by ID using the O(1) index.
	if name, ok := b.planIDIndex[idOrName]; ok {
		p, _ := b.plans.Get(name)
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
}

// ListBackupPlans returns all backup plans sorted by name.
func (b *InMemoryBackend) ListBackupPlans() []*Plan {
	b.mu.RLock("ListBackupPlans")
	defer b.mu.RUnlock()

	all := b.plans.All()
	list := make([]*Plan, 0, len(all))
	for _, p := range all {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Plan) int {
		if a.BackupPlanName < b.BackupPlanName {
			return -1
		}
		if a.BackupPlanName > b.BackupPlanName {
			return 1
		}

		return 0
	})

	return list
}

// UpdateBackupPlan updates an existing backup plan.
func (b *InMemoryBackend) UpdateBackupPlan(
	idOrName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
) (*Plan, error) {
	b.mu.Lock("UpdateBackupPlan")
	defer b.mu.Unlock()

	// Find by name first, then by ID using the O(1) index.
	var found *Plan
	if p, ok := b.plans.Get(idOrName); ok {
		found = p
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		found, _ = b.plans.Get(idName)
	}

	if found == nil {
		return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	found.Rules = rules
	if advancedSettings != nil {
		found.AdvancedBackupSettings = advancedSettings
	}
	found.VersionID = uuid.NewString()
	now := time.Now().UTC()
	found.UpdateTime = &now
	cp := *found
	cp.Rules = make([]Rule, len(found.Rules))
	copy(cp.Rules, found.Rules)

	return &cp, nil
}

// DeleteBackupPlan deletes a backup plan by ID or name.
func (b *InMemoryBackend) DeleteBackupPlan(idOrName string) error {
	b.mu.Lock("DeleteBackupPlan")
	defer b.mu.Unlock()

	// Resolve by name or by ID using the O(1) index.
	var planName string
	if b.plans.Has(idOrName) {
		planName = idOrName
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		planName = idName
	} else {
		return fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	p, _ := b.plans.Get(planName)
	delete(b.planARNIndex, p.BackupPlanArn)
	delete(b.planIDIndex, p.BackupPlanID)
	b.plans.Delete(planName)
	p.Tags.Close()

	return nil
}

// StartBackupJob starts a new backup job.
func (b *InMemoryBackend) StartBackupJob(
	vaultName, resourceArn, iamRoleArn, resourceType string,
) (*Job, error) {
	b.mu.Lock("StartBackupJob")
	defer b.mu.Unlock()

	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if iamRoleArn == "" {
		return nil, fmt.Errorf("%w: IamRoleArn is required", ErrValidation)
	}

	vault, ok := b.vaults.Get(vaultName)
	if !ok {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	jobID := uuid.NewString()
	j := &Job{
		BackupJobID:     jobID,
		BackupVaultName: vaultName,
		BackupVaultArn:  vault.BackupVaultArn,
		ResourceArn:     resourceArn,
		IAMRoleArn:      iamRoleArn,
		ResourceType:    resourceType,
		State:           statusCreated,
		AccountID:       b.accountID,
		Region:          b.region,
		CreationTime:    time.Now().UTC(),
	}
	b.jobs.Put(j)
	cp := *j

	return &cp, nil
}

// DescribeBackupJob returns a backup job by ID.
func (b *InMemoryBackend) DescribeBackupJob(jobID string) (*Job, error) {
	b.mu.RLock("DescribeBackupJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: backup job %s not found", ErrNotFound, jobID)
	}
	cp := *j

	return &cp, nil
}

// ListBackupJobs returns all backup jobs, optionally filtered by vault name.
// Results are sorted by creation time (newest first).
func (b *InMemoryBackend) ListBackupJobs(vaultName string) []*Job {
	b.mu.RLock("ListBackupJobs")
	defer b.mu.RUnlock()

	all := b.jobs.All()
	list := make([]*Job, 0, len(all))
	for _, j := range all {
		if vaultName != "" && j.BackupVaultName != vaultName {
			continue
		}
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Job) int {
		// Newest first.
		if a.CreationTime.After(b.CreationTime) {
			return -1
		}
		if a.CreationTime.Before(b.CreationTime) {
			return 1
		}

		return 0
	})

	return list
}

// TagResource adds tags to a resource by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) TagResource(resourceArn string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)
		v.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)
		p.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)
		f.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)
		rp.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// ListTags returns tags for a resource by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) ListTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)

		return v.Tags.Clone(), nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)

		return p.Tags.Clone(), nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)

		return f.Tags.Clone(), nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)

		return rp.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// UntagResource removes the given tag keys from a resource identified by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)
		v.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)
		p.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)
		f.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)
		rp.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// AssociateBackupVaultMpaApprovalTeam associates an MPA approval team with a backup vault.
func (b *InMemoryBackend) AssociateBackupVaultMpaApprovalTeam(
	vaultName, mpaApprovalTeamArn string,
) error {
	b.mu.Lock("AssociateBackupVaultMpaApprovalTeam")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if mpaApprovalTeamArn == "" {
		return fmt.Errorf("%w: MpaApprovalTeamArn is required", ErrValidation)
	}

	b.mpaApprovals[vaultName] = mpaApprovalTeamArn

	return nil
}

// CancelLegalHold cancels (deletes) a legal hold by ID.
func (b *InMemoryBackend) CancelLegalHold(legalHoldID string) error {
	b.mu.Lock("CancelLegalHold")
	defer b.mu.Unlock()

	if !b.legalHolds.Has(legalHoldID) {
		return fmt.Errorf("%w: legal hold %s not found", ErrNotFound, legalHoldID)
	}

	b.legalHolds.Delete(legalHoldID)

	return nil
}

// CreateBackupSelection creates a backup selection for a plan.
func (b *InMemoryBackend) CreateBackupSelection(
	planID, selectionName, iamRoleArn string,
	resources, notResources []string,
	listOfTags []TagCondition,
	conditions *SelectionConditions,
) (*Selection, error) {
	b.mu.Lock("CreateBackupSelection")
	defer b.mu.Unlock()

	if selectionName == "" {
		return nil, fmt.Errorf("%w: SelectionName is required", ErrValidation)
	}

	// Resolve planID: accept either a plan ID (from planIDIndex) or a plan name.
	if _, found := b.planIDIndex[planID]; !found {
		// planID is not a known ID — try it as a plan name.
		p, exists := b.plans.Get(planID)
		if !exists {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
		// Switch planID to the canonical UUID stored on the plan.
		planID = p.BackupPlanID
	}

	selectionID := uuid.NewString()
	sel := &Selection{
		SelectionID:   selectionID,
		SelectionName: selectionName,
		BackupPlanID:  planID,
		IAMRoleArn:    iamRoleArn,
		Resources:     resources,
		NotResources:  notResources,
		ListOfTags:    listOfTags,
		Conditions:    conditions,
		CreationTime:  time.Now().UTC(),
	}
	b.selections.Put(sel)
	cp := *sel

	return &cp, nil
}

// CreateFramework creates an audit framework.
func (b *InMemoryBackend) CreateFramework(
	name, description string,
	controls []FrameworkControl,
) (*Framework, error) {
	b.mu.Lock("CreateFramework")
	defer b.mu.Unlock()

	if b.frameworks.Has(name) {
		return nil, fmt.Errorf("%w: framework %s already exists", ErrAlreadyExists, name)
	}

	frameworkARN := arn.Build("backup", b.region, b.accountID, "framework:"+name)
	t := tags.New("backup.framework." + name + ".tags")
	f := &Framework{
		FrameworkName:        name,
		FrameworkArn:         frameworkARN,
		FrameworkDescription: description,
		FrameworkControls:    controls,
		FrameworkStatus:      "ACTIVE",
		DeploymentStatus:     "COMPLETED",
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.frameworks.Put(f)
	b.frameworkARNIndex[frameworkARN] = name
	cp := *f

	return &cp, nil
}

// CreateLegalHold creates a legal hold.
func (b *InMemoryBackend) CreateLegalHold(title, description string) (*LegalHold, error) {
	b.mu.Lock("CreateLegalHold")
	defer b.mu.Unlock()

	id := uuid.NewString()
	lhARN := arn.Build("backup", b.region, b.accountID, "legal-hold:"+id)
	lh := &LegalHold{
		LegalHoldID:  id,
		LegalHoldArn: lhARN,
		Title:        title,
		Description:  description,
		Status:       statusActive,
		CreationDate: time.Now().UTC(),
	}
	b.legalHolds.Put(lh)
	cp := *lh

	return &cp, nil
}

// CreateLogicallyAirGappedBackupVault creates a logically air-gapped backup vault.
func (b *InMemoryBackend) CreateLogicallyAirGappedBackupVault(
	name, creatorRequestID string,
	minRetentionDays, maxRetentionDays int64,
	kv map[string]string,
) (*Vault, error) {
	b.mu.Lock("CreateLogicallyAirGappedBackupVault")
	defer b.mu.Unlock()

	if !isValidVaultName(name) {
		return nil, fmt.Errorf(
			"%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters",
			ErrValidation,
		)
	}

	if minRetentionDays <= 0 {
		return nil, fmt.Errorf("%w: MinRetentionDays must be greater than 0", ErrValidation)
	}

	if maxRetentionDays < minRetentionDays {
		return nil, fmt.Errorf("%w: MaxRetentionDays must be >= MinRetentionDays", ErrValidation)
	}

	if b.vaults.Has(name) {
		return nil, fmt.Errorf("%w: vault %s already exists", ErrAlreadyExists, name)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "backup-vault:"+name)
	t := tags.New("backup.vault." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	v := &Vault{
		BackupVaultName:  name,
		BackupVaultArn:   vaultARN,
		CreatorRequestID: creatorRequestID,
		VaultType:        VaultTypeAirGapped,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		MinRetentionDays: minRetentionDays,
		MaxRetentionDays: maxRetentionDays,
		Tags:             t,
	}
	b.vaults.Put(v)
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// CreateReportPlan creates a report plan.
func (b *InMemoryBackend) CreateReportPlan(
	name, description string,
	deliveryChannel *ReportDeliveryChannel,
	setting *ReportSetting,
) (*ReportPlan, error) {
	b.mu.Lock("CreateReportPlan")
	defer b.mu.Unlock()

	if b.reportPlans.Has(name) {
		return nil, fmt.Errorf("%w: report plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "report-plan:"+name)
	t := tags.New("backup.report-plan." + name + ".tags")
	rp := &ReportPlan{
		ReportPlanName:        name,
		ReportPlanArn:         planARN,
		ReportPlanDescription: description,
		ReportDeliveryChannel: deliveryChannel,
		ReportSetting:         setting,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
	}
	b.reportPlans.Put(rp)
	b.reportPlanARNIndex[planARN] = name
	cp := *rp

	return &cp, nil
}

// CreateRestoreAccessBackupVault creates a restore access backup vault.
func (b *InMemoryBackend) CreateRestoreAccessBackupVault(
	sourceVaultArn, vaultName string,
	_ /* creatorRequestID */ string,
	_ /* kv */ map[string]string,
) (*RestoreAccessVault, error) {
	b.mu.Lock("CreateRestoreAccessBackupVault")
	defer b.mu.Unlock()

	if vaultName == "" {
		vaultName = uuid.NewString()
	}

	if b.restoreAccessVaults.Has(vaultName) {
		return nil, fmt.Errorf(
			"%w: restore access vault %s already exists",
			ErrAlreadyExists,
			vaultName,
		)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "restore-access-backup-vault:"+vaultName)
	rav := &RestoreAccessVault{
		RestoreAccessBackupVaultName: vaultName,
		RestoreAccessBackupVaultArn:  vaultARN,
		SourceBackupVaultArn:         sourceVaultArn,
		VaultState:                   statusCreating,
		CreationDate:                 time.Now().UTC(),
	}
	b.restoreAccessVaults.Put(rav)
	cp := *rav

	return &cp, nil
}

// CreateRestoreTestingPlan creates a restore testing plan.
func (b *InMemoryBackend) CreateRestoreTestingPlan(
	name, scheduleExpression string,
	startWindowHours int64,
) (*RestoreTestingPlan, error) {
	b.mu.Lock("CreateRestoreTestingPlan")
	defer b.mu.Unlock()

	if b.restoreTestingPlans.Has(name) {
		return nil, fmt.Errorf("%w: restore testing plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "restore-testing-plan:"+name)
	rtp := &RestoreTestingPlan{
		RestoreTestingPlanName: name,
		RestoreTestingPlanArn:  planARN,
		ScheduleExpression:     scheduleExpression,
		StartWindowHours:       startWindowHours,
		CreationTime:           time.Now().UTC(),
	}
	b.restoreTestingPlans.Put(rtp)
	cp := *rtp

	return &cp, nil
}

// CreateRestoreTestingSelection creates a selection within a restore testing plan.
func (b *InMemoryBackend) CreateRestoreTestingSelection(
	planName, selectionName, protectedResourceType string,
) (*RestoreTestingSelection, error) {
	b.mu.Lock("CreateRestoreTestingSelection")
	defer b.mu.Unlock()

	rtp, found := b.restoreTestingPlans.Get(planName)
	if !found {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	if b.restoreTestingSelections.Has(restoreTestingSelectionKey(planName, selectionName)) {
		return nil, fmt.Errorf(
			"%w: restore testing selection %s already exists",
			ErrAlreadyExists,
			selectionName,
		)
	}

	sel := &RestoreTestingSelection{
		RestoreTestingPlanName:      planName,
		RestoreTestingSelectionName: selectionName,
		RestoreTestingPlanArn:       rtp.RestoreTestingPlanArn,
		ProtectedResourceType:       protectedResourceType,
		CreationTime:                time.Now().UTC(),
	}
	b.restoreTestingSelections.Put(sel)
	cp := *sel

	return &cp, nil
}

// Reset clears all state, returning the backend to a clean initial state.
// Tags resources are properly closed before discarding.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, v := range b.vaults.All() {
		if v.Tags != nil {
			v.Tags.Close()
		}
	}

	for _, p := range b.plans.All() {
		if p.Tags != nil {
			p.Tags.Close()
		}
	}

	for _, f := range b.frameworks.All() {
		if f.Tags != nil {
			f.Tags.Close()
		}
	}

	for _, rp := range b.reportPlans.All() {
		if rp.Tags != nil {
			rp.Tags.Close()
		}
	}

	// PERSISTED tables: vaults, plans, jobs, selections, frameworks,
	// legalHolds, reportPlans, restoreAccessVaults, restoreTestingPlans,
	// restoreTestingSelections (and their indexes) -- see store_setup.go.
	b.registry.ResetAll()

	// VOLATILE tables are not registered on b.registry, so they are reset
	// individually here -- see store_setup.go.
	b.recoveryPoints.Reset()
	b.copyJobs.Reset()
	b.vaultAccessPolicies.Reset()
	b.vaultLockConfigs.Reset()
	b.vaultNotifications.Reset()
	b.restoreJobs.Reset()
	b.reportJobs.Reset()
	b.scanJobs.Reset()
	b.tieringConfigs.Reset()
	b.protectedResources.Reset()

	b.mpaApprovals = make(map[string]string)
	b.vaultARNIndex = make(map[string]string)
	b.planARNIndex = make(map[string]string)
	b.planIDIndex = make(map[string]string)
	b.frameworkARNIndex = make(map[string]string)
	b.reportPlanARNIndex = make(map[string]string)
	b.globalSettings = make(map[string]string)
	b.regionSettings = nil
	b.recoveryPointLifecycle = make(map[string]string)
	b.recoveryPointIndexStatus = make(map[string]string)
	b.restoreValidations = make(map[string]string)
}

// --- Recovery Point methods ---

// ListRecoveryPointsByBackupVault returns all recovery points for a vault.
func (b *InMemoryBackend) ListRecoveryPointsByBackupVault(
	vaultName string,
) ([]*RecoveryPoint, error) {
	b.mu.RLock("ListRecoveryPointsByBackupVault")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	pts := b.recoveryPointsByVault.Get(vaultName)
	list := make([]*RecoveryPoint, 0, len(pts))
	for _, rp := range pts {
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RecoveryPoint) int {
		if a.CreationDate.After(b.CreationDate) {
			return -1
		}
		if a.CreationDate.Before(b.CreationDate) {
			return 1
		}

		return 0
	})

	return list, nil
}

// DescribeRecoveryPoint returns a specific recovery point.
func (b *InMemoryBackend) DescribeRecoveryPoint(
	vaultName, recoveryPointArn string,
) (*RecoveryPoint, error) {
	b.mu.RLock("DescribeRecoveryPoint")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	rp, ok := b.recoveryPoints.Get(recoveryPointKey(vaultName, recoveryPointArn))
	if !ok {
		return nil, fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	cp := *rp

	return &cp, nil
}

// GetRecoveryPointRestoreMetadata returns restore metadata for a recovery point.
func (b *InMemoryBackend) GetRecoveryPointRestoreMetadata(
	vaultName, recoveryPointArn string,
) (map[string]string, error) {
	b.mu.RLock("GetRecoveryPointRestoreMetadata")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if !b.recoveryPoints.Has(recoveryPointKey(vaultName, recoveryPointArn)) {
		return nil, fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	return map[string]string{}, nil
}

// DeleteRecoveryPoint deletes a recovery point from a vault.
func (b *InMemoryBackend) DeleteRecoveryPoint(vaultName, recoveryPointArn string) error {
	b.mu.Lock("DeleteRecoveryPoint")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	key := recoveryPointKey(vaultName, recoveryPointArn)
	if !b.recoveryPoints.Has(key) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	b.recoveryPoints.Delete(key)
	if v, ok := b.vaults.Get(vaultName); ok {
		if v.NumberOfRecoveryPoints > 0 {
			v.NumberOfRecoveryPoints--
		}
	}

	return nil
}

// DisassociateRecoveryPoint disassociates a recovery point from a vault.
func (b *InMemoryBackend) DisassociateRecoveryPoint(vaultName, recoveryPointArn string) error {
	b.mu.Lock("DisassociateRecoveryPoint")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	key := recoveryPointKey(vaultName, recoveryPointArn)
	if !b.recoveryPoints.Has(key) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	b.recoveryPoints.Delete(key)

	return nil
}

// DisassociateRecoveryPointFromParent disassociates a recovery point from its parent.
func (b *InMemoryBackend) DisassociateRecoveryPointFromParent(
	vaultName, recoveryPointArn string,
) error {
	b.mu.Lock("DisassociateRecoveryPointFromParent")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if !b.recoveryPoints.Has(recoveryPointKey(vaultName, recoveryPointArn)) {
		return fmt.Errorf("%w: recovery point %s not found", ErrNotFound, recoveryPointArn)
	}

	return nil
}

// AddRecoveryPoint adds a recovery point to a vault (used internally and in tests).
func (b *InMemoryBackend) AddRecoveryPoint(vaultName string, rp *RecoveryPoint) error {
	b.mu.Lock("AddRecoveryPoint")
	defer b.mu.Unlock()

	vault, ok := b.vaults.Get(vaultName)
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	cp := *rp
	cp.BackupVaultName = vaultName
	b.recoveryPoints.Put(&cp)
	vault.NumberOfRecoveryPoints++

	return nil
}

// --- Vault compliance methods ---

// PutBackupVaultAccessPolicy sets an access policy for a vault.
// The policy must be valid JSON representing an IAM policy document.
func (b *InMemoryBackend) PutBackupVaultAccessPolicy(vaultName, policy string) error {
	b.mu.Lock("PutBackupVaultAccessPolicy")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if policy != "" {
		var doc map[string]any
		if err := json.Unmarshal([]byte(policy), &doc); err != nil {
			return fmt.Errorf(
				"%w: Policy must be a valid JSON document: %s",
				ErrValidation,
				err.Error(),
			)
		}
	}

	b.vaultAccessPolicies.Put(&VaultAccessPolicy{VaultName: vaultName, Policy: policy})

	return nil
}

// GetBackupVaultAccessPolicy returns the access policy for a vault.
func (b *InMemoryBackend) GetBackupVaultAccessPolicy(vaultName string) (*VaultAccessPolicy, error) {
	b.mu.RLock("GetBackupVaultAccessPolicy")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	pol, ok := b.vaultAccessPolicies.Get(vaultName)
	if !ok {
		return nil, fmt.Errorf("%w: no access policy for vault %s", ErrNotFound, vaultName)
	}

	cp := *pol

	return &cp, nil
}

// DeleteBackupVaultAccessPolicy deletes the access policy for a vault.
func (b *InMemoryBackend) DeleteBackupVaultAccessPolicy(vaultName string) error {
	b.mu.Lock("DeleteBackupVaultAccessPolicy")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	b.vaultAccessPolicies.Delete(vaultName)

	return nil
}

// PutBackupVaultLockConfiguration sets the lock configuration for a vault.
// If a LockDate already exists and has passed, the configuration is immutable.
func (b *InMemoryBackend) PutBackupVaultLockConfiguration(
	vaultName string,
	cfg *VaultLockConfig,
) error {
	b.mu.Lock("PutBackupVaultLockConfiguration")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	if existing, ok := b.vaultLockConfigs.Get(vaultName); ok {
		if existing.LockDate != nil && time.Now().UTC().After(*existing.LockDate) {
			return fmt.Errorf(
				"%w: vault lock configuration is immutable: LockDate %s has passed",
				ErrValidation,
				existing.LockDate.Format(time.RFC3339),
			)
		}
	}

	cp := *cfg
	cp.VaultName = vaultName
	if cp.ChangeableForDays > 0 && cp.LockDate == nil {
		lockDate := time.Now().UTC().Add(
			time.Duration(cp.ChangeableForDays) * 24 * time.Hour,
		)
		cp.LockDate = &lockDate
	}
	b.vaultLockConfigs.Put(&cp)

	return nil
}

// GetBackupVaultLockConfig returns the lock configuration for a vault.
func (b *InMemoryBackend) GetBackupVaultLockConfig(vaultName string) (*VaultLockConfig, error) {
	b.mu.RLock("GetBackupVaultLockConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.vaultLockConfigs.Get(vaultName)
	if !ok {
		return nil, fmt.Errorf("%w: no lock config for vault %s", ErrNotFound, vaultName)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteBackupVaultLockConfiguration deletes the lock configuration for a vault.
func (b *InMemoryBackend) DeleteBackupVaultLockConfiguration(vaultName string) error {
	b.mu.Lock("DeleteBackupVaultLockConfiguration")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	b.vaultLockConfigs.Delete(vaultName)

	return nil
}

// PutBackupVaultNotifications sets notification configuration for a vault.
// Events are validated against the canonical AWS Backup event enum.
func (b *InMemoryBackend) PutBackupVaultNotifications(
	vaultName string,
	cfg *VaultNotificationConfig,
) error {
	b.mu.Lock("PutBackupVaultNotifications")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	for _, ev := range cfg.BackupVaultEvents {
		if _, valid := validVaultEvents[ev]; !valid {
			return fmt.Errorf(
				"%w: invalid BackupVaultEvent %q; must be one of the allowed event types",
				ErrValidation,
				ev,
			)
		}
	}

	cp := *cfg
	cp.VaultName = vaultName
	cp.BackupVaultEvents = make([]string, len(cfg.BackupVaultEvents))
	copy(cp.BackupVaultEvents, cfg.BackupVaultEvents)
	b.vaultNotifications.Put(&cp)

	return nil
}

// GetBackupVaultNotifications returns notification configuration for a vault.
func (b *InMemoryBackend) GetBackupVaultNotifications(
	vaultName string,
) (*VaultNotificationConfig, error) {
	b.mu.RLock("GetBackupVaultNotifications")
	defer b.mu.RUnlock()

	if !b.vaults.Has(vaultName) {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	cfg, ok := b.vaultNotifications.Get(vaultName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: no notification configuration for vault %s",
			ErrNotFound,
			vaultName,
		)
	}

	cp := *cfg
	cp.BackupVaultEvents = make([]string, len(cfg.BackupVaultEvents))
	copy(cp.BackupVaultEvents, cfg.BackupVaultEvents)

	return &cp, nil
}

// DeleteBackupVaultNotifications deletes notification configuration for a vault.
func (b *InMemoryBackend) DeleteBackupVaultNotifications(vaultName string) error {
	b.mu.Lock("DeleteBackupVaultNotifications")
	defer b.mu.Unlock()

	if !b.vaults.Has(vaultName) {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	b.vaultNotifications.Delete(vaultName)

	return nil
}

// --- Backup Selection read/delete methods ---

// GetBackupSelection returns a specific backup selection.
func (b *InMemoryBackend) GetBackupSelection(planID, selectionID string) (*Selection, error) {
	b.mu.RLock("GetBackupSelection")
	defer b.mu.RUnlock()

	// Resolve planID to canonical ID if needed.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	sel, ok := b.selections.Get(selectionKey(planID, selectionID))
	if !ok {
		return nil, fmt.Errorf("%w: backup selection %s not found", ErrNotFound, selectionID)
	}

	cp := *sel

	return &cp, nil
}

// ListBackupSelections returns all backup selections for a plan.
func (b *InMemoryBackend) ListBackupSelections(planID string) ([]*Selection, error) {
	b.mu.RLock("ListBackupSelections")
	defer b.mu.RUnlock()

	// Resolve planID.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	sels := b.selectionsByPlan.Get(planID)
	list := make([]*Selection, 0, len(sels))
	for _, sel := range sels {
		cp := *sel
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Selection) int {
		if a.SelectionName < b.SelectionName {
			return -1
		}
		if a.SelectionName > b.SelectionName {
			return 1
		}

		return 0
	})

	return list, nil
}

// DeleteBackupSelection deletes a backup selection.
func (b *InMemoryBackend) DeleteBackupSelection(planID, selectionID string) error {
	b.mu.Lock("DeleteBackupSelection")
	defer b.mu.Unlock()

	// Resolve planID.
	if _, found := b.planIDIndex[planID]; !found {
		if p, exists := b.plans.Get(planID); exists {
			planID = p.BackupPlanID
		} else {
			return fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
	}

	key := selectionKey(planID, selectionID)
	if !b.selections.Has(key) {
		return fmt.Errorf("%w: backup selection %s not found", ErrNotFound, selectionID)
	}

	b.selections.Delete(key)

	return nil
}

// --- Copy Job methods ---

// ListCopyJobs returns all copy jobs.
func (b *InMemoryBackend) ListCopyJobs() []*CopyJob {
	b.mu.RLock("ListCopyJobs")
	defer b.mu.RUnlock()

	all := b.copyJobs.All()
	list := make([]*CopyJob, 0, len(all))
	for _, j := range all {
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *CopyJob) int {
		if a.CreationDate.After(b.CreationDate) {
			return -1
		}
		if a.CreationDate.Before(b.CreationDate) {
			return 1
		}

		return 0
	})

	return list
}

// DescribeCopyJob returns a copy job by ID.
func (b *InMemoryBackend) DescribeCopyJob(copyJobID string) (*CopyJob, error) {
	b.mu.RLock("DescribeCopyJob")
	defer b.mu.RUnlock()

	j, ok := b.copyJobs.Get(copyJobID)
	if !ok {
		return nil, fmt.Errorf("%w: copy job %s not found", ErrNotFound, copyJobID)
	}

	cp := *j

	return &cp, nil
}

// --- Restore Testing read/update/delete methods ---

// GetRestoreTestingPlan returns a restore testing plan by name.
func (b *InMemoryBackend) GetRestoreTestingPlan(planName string) (*RestoreTestingPlan, error) {
	b.mu.RLock("GetRestoreTestingPlan")
	defer b.mu.RUnlock()

	rtp, ok := b.restoreTestingPlans.Get(planName)
	if !ok {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	cp := *rtp

	return &cp, nil
}

// ListRestoreTestingPlans returns all restore testing plans.
func (b *InMemoryBackend) ListRestoreTestingPlans() []*RestoreTestingPlan {
	b.mu.RLock("ListRestoreTestingPlans")
	defer b.mu.RUnlock()

	all := b.restoreTestingPlans.All()
	list := make([]*RestoreTestingPlan, 0, len(all))
	for _, rtp := range all {
		cp := *rtp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RestoreTestingPlan) int {
		if a.RestoreTestingPlanName < b.RestoreTestingPlanName {
			return -1
		}
		if a.RestoreTestingPlanName > b.RestoreTestingPlanName {
			return 1
		}

		return 0
	})

	return list
}

// UpdateRestoreTestingPlan updates a restore testing plan.
func (b *InMemoryBackend) UpdateRestoreTestingPlan(
	planName, scheduleExpression string,
	startWindowHours int64,
) (*RestoreTestingPlan, error) {
	b.mu.Lock("UpdateRestoreTestingPlan")
	defer b.mu.Unlock()

	rtp, ok := b.restoreTestingPlans.Get(planName)
	if !ok {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	rtp.ScheduleExpression = scheduleExpression
	if startWindowHours > 0 {
		rtp.StartWindowHours = startWindowHours
	}
	cp := *rtp

	return &cp, nil
}

// DeleteRestoreTestingPlan deletes a restore testing plan and all its selections.
func (b *InMemoryBackend) DeleteRestoreTestingPlan(planName string) error {
	b.mu.Lock("DeleteRestoreTestingPlan")
	defer b.mu.Unlock()

	if !b.restoreTestingPlans.Has(planName) {
		return fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	b.restoreTestingPlans.Delete(planName)

	// Cascade-delete every selection under this plan. Clone the index's
	// result first: deleting from the table while ranging over the live
	// index slice would mutate it out from under the loop.
	for _, sel := range slices.Clone(b.restoreTestingSelectionsByPlan.Get(planName)) {
		b.restoreTestingSelections.Delete(
			restoreTestingSelectionKey(sel.RestoreTestingPlanName, sel.RestoreTestingSelectionName),
		)
	}

	return nil
}

// GetRestoreTestingSelection returns a specific restore testing selection.
func (b *InMemoryBackend) GetRestoreTestingSelection(
	planName, selectionName string,
) (*RestoreTestingSelection, error) {
	b.mu.RLock("GetRestoreTestingSelection")
	defer b.mu.RUnlock()

	if !b.restoreTestingPlans.Has(planName) {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	sel, ok := b.restoreTestingSelections.Get(restoreTestingSelectionKey(planName, selectionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: restore testing selection %s not found",
			ErrNotFound,
			selectionName,
		)
	}

	cp := *sel

	return &cp, nil
}

// ListRestoreTestingSelections returns all selections for a restore testing plan.
func (b *InMemoryBackend) ListRestoreTestingSelections(
	planName string,
) ([]*RestoreTestingSelection, error) {
	b.mu.RLock("ListRestoreTestingSelections")
	defer b.mu.RUnlock()

	if !b.restoreTestingPlans.Has(planName) {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	sels := b.restoreTestingSelectionsByPlan.Get(planName)
	list := make([]*RestoreTestingSelection, 0, len(sels))
	for _, sel := range sels {
		cp := *sel
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RestoreTestingSelection) int {
		if a.RestoreTestingSelectionName < b.RestoreTestingSelectionName {
			return -1
		}
		if a.RestoreTestingSelectionName > b.RestoreTestingSelectionName {
			return 1
		}

		return 0
	})

	return list, nil
}

// UpdateRestoreTestingSelection updates a restore testing selection.
func (b *InMemoryBackend) UpdateRestoreTestingSelection(
	planName, selectionName, protectedResourceType string,
) (*RestoreTestingSelection, error) {
	b.mu.Lock("UpdateRestoreTestingSelection")
	defer b.mu.Unlock()

	if !b.restoreTestingPlans.Has(planName) {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	sel, ok := b.restoreTestingSelections.Get(restoreTestingSelectionKey(planName, selectionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: restore testing selection %s not found",
			ErrNotFound,
			selectionName,
		)
	}

	sel.ProtectedResourceType = protectedResourceType
	cp := *sel

	return &cp, nil
}

// DeleteRestoreTestingSelection deletes a restore testing selection.
func (b *InMemoryBackend) DeleteRestoreTestingSelection(planName, selectionName string) error {
	b.mu.Lock("DeleteRestoreTestingSelection")
	defer b.mu.Unlock()

	if !b.restoreTestingPlans.Has(planName) {
		return fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	key := restoreTestingSelectionKey(planName, selectionName)
	if !b.restoreTestingSelections.Has(key) {
		return fmt.Errorf("%w: restore testing selection %s not found", ErrNotFound, selectionName)
	}

	b.restoreTestingSelections.Delete(key)

	return nil
}

// --- Framework read/update/delete methods ---

// DescribeFramework returns a framework by name.
func (b *InMemoryBackend) DescribeFramework(name string) (*Framework, error) {
	b.mu.RLock("DescribeFramework")
	defer b.mu.RUnlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	cp := *f

	return &cp, nil
}

// ListFrameworks returns all frameworks.
func (b *InMemoryBackend) ListFrameworks() []*Framework {
	b.mu.RLock("ListFrameworks")
	defer b.mu.RUnlock()

	all := b.frameworks.All()
	list := make([]*Framework, 0, len(all))
	for _, f := range all {
		cp := *f
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Framework) int {
		if a.FrameworkName < b.FrameworkName {
			return -1
		}
		if a.FrameworkName > b.FrameworkName {
			return 1
		}

		return 0
	})

	return list
}

// UpdateFramework updates a framework's description.
func (b *InMemoryBackend) UpdateFramework(name, description string) (*Framework, error) {
	b.mu.Lock("UpdateFramework")
	defer b.mu.Unlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	f.FrameworkDescription = description
	cp := *f

	return &cp, nil
}

// DeleteFramework deletes a framework.
func (b *InMemoryBackend) DeleteFramework(name string) error {
	b.mu.Lock("DeleteFramework")
	defer b.mu.Unlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	delete(b.frameworkARNIndex, f.FrameworkArn)
	b.frameworks.Delete(name)
	f.Tags.Close()

	return nil
}

// --- Report Plan read/update/delete methods ---

// ListReportPlans returns all report plans.
func (b *InMemoryBackend) ListReportPlans() []*ReportPlan {
	b.mu.RLock("ListReportPlans")
	defer b.mu.RUnlock()

	all := b.reportPlans.All()
	list := make([]*ReportPlan, 0, len(all))
	for _, rp := range all {
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *ReportPlan) int {
		if a.ReportPlanName < b.ReportPlanName {
			return -1
		}
		if a.ReportPlanName > b.ReportPlanName {
			return 1
		}

		return 0
	})

	return list
}

// DescribeReportPlan returns a report plan by name.
func (b *InMemoryBackend) DescribeReportPlan(name string) (*ReportPlan, error) {
	b.mu.RLock("DescribeReportPlan")
	defer b.mu.RUnlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	cp := *rp

	return &cp, nil
}

// UpdateReportPlan updates a report plan's description.
func (b *InMemoryBackend) UpdateReportPlan(name, description string) (*ReportPlan, error) {
	b.mu.Lock("UpdateReportPlan")
	defer b.mu.Unlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	rp.ReportPlanDescription = description
	cp := *rp

	return &cp, nil
}

// DeleteReportPlan deletes a report plan.
func (b *InMemoryBackend) DeleteReportPlan(name string) error {
	b.mu.Lock("DeleteReportPlan")
	defer b.mu.Unlock()

	rp, ok := b.reportPlans.Get(name)
	if !ok {
		return fmt.Errorf("%w: report plan %s not found", ErrNotFound, name)
	}

	delete(b.reportPlanARNIndex, rp.ReportPlanArn)
	b.reportPlans.Delete(name)
	rp.Tags.Close()

	return nil
}
