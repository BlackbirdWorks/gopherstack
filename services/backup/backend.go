package backup

import (
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	// vaultNameRe matches valid vault names: 2–50 alphanumeric-or-hyphen chars.
	vaultNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-]{2,50}$`)
)

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
	AccountID              string     `json:"accountId"`
	Region                 string     `json:"region"`
	NumberOfRecoveryPoints int64      `json:"numberOfRecoveryPoints"`
	MinRetentionDays       int64      `json:"minRetentionDays,omitempty"`
	MaxRetentionDays       int64      `json:"maxRetentionDays,omitempty"`
}

// Rule represents a single rule in a backup plan.
type Rule struct {
	RuleName                string `json:"ruleName"`
	TargetVaultName         string `json:"targetVaultName"`
	ScheduleExpression      string `json:"scheduleExpression,omitempty"`
	StartWindowMinutes      int64  `json:"startWindowMinutes,omitempty"`
	CompletionWindowMinutes int64  `json:"completionWindowMinutes,omitempty"`
}

// Plan represents an AWS Backup plan.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateBackupPlan.
type Plan struct {
	CreationTime   time.Time  `json:"creationTime"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	BackupPlanName string     `json:"backupPlanName"`
	BackupPlanArn  string     `json:"backupPlanArn"`
	BackupPlanID   string     `json:"backupPlanId"`
	VersionID      string     `json:"versionId"`
	AccountID      string     `json:"accountId"`
	Region         string     `json:"region"`
	Rules          []Rule     `json:"rules"`
}

// Job represents an AWS Backup job.
type Job struct {
	CreationTime    time.Time  `json:"creationTime"`
	CompletionTime  *time.Time `json:"completionTime,omitempty"`
	ResourceArn     string     `json:"resourceArn,omitempty"`
	BackupJobID     string     `json:"backupJobId"`
	BackupVaultName string     `json:"backupVaultName"`
	BackupVaultArn  string     `json:"backupVaultArn"`
	ResourceType    string     `json:"resourceType,omitempty"`
	IAMRoleArn      string     `json:"iamRoleArn,omitempty"`
	State           string     `json:"state"`
	AccountID       string     `json:"accountId"`
	Region          string     `json:"region"`
}

// Selection represents an AWS Backup selection (resources assigned to a plan).
type Selection struct {
	CreationTime  time.Time `json:"creationTime"`
	SelectionName string    `json:"selectionName"`
	SelectionID   string    `json:"selectionId"`
	BackupPlanID  string    `json:"backupPlanId"`
	IAMRoleArn    string    `json:"iamRoleArn,omitempty"`
}

// Framework represents an AWS Backup audit framework.
type Framework struct {
	CreationTime         time.Time  `json:"creationTime"`
	Tags                 *tags.Tags `json:"tags,omitempty"`
	FrameworkName        string     `json:"frameworkName"`
	FrameworkArn         string     `json:"frameworkArn"`
	FrameworkDescription string     `json:"frameworkDescription,omitempty"`
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
	CreationTime          time.Time  `json:"creationTime"`
	Tags                  *tags.Tags `json:"tags,omitempty"`
	ReportPlanName        string     `json:"reportPlanName"`
	ReportPlanArn         string     `json:"reportPlanArn"`
	ReportPlanDescription string     `json:"reportPlanDescription,omitempty"`
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
}

// RestoreTestingSelection represents a selection within a restore testing plan.
type RestoreTestingSelection struct {
	CreationTime                time.Time `json:"creationTime"`
	RestoreTestingPlanName      string    `json:"restoreTestingPlanName"`
	RestoreTestingSelectionName string    `json:"restoreTestingSelectionName"`
	RestoreTestingPlanArn       string    `json:"restoreTestingPlanArn"`
	ProtectedResourceType       string    `json:"protectedResourceType,omitempty"`
}

// InMemoryBackend is the in-memory store for AWS Backup resources.
type InMemoryBackend struct {
	vaults                   map[string]*Vault
	plans                    map[string]*Plan
	jobs                     map[string]*Job
	selections               map[string]map[string]*Selection               // planID → selectionID → selection
	frameworks               map[string]*Framework                          // frameworkName → framework
	legalHolds               map[string]*LegalHold                          // legalHoldID → legalHold
	reportPlans              map[string]*ReportPlan                         // reportPlanName → reportPlan
	restoreAccessVaults      map[string]*RestoreAccessVault                 // vaultName → vault
	restoreTestingPlans      map[string]*RestoreTestingPlan                 // planName → plan
	restoreTestingSelections map[string]map[string]*RestoreTestingSelection // planName → selectionName → selection
	mpaApprovals             map[string]string                              // vaultName → mpaApprovalTeamArn
	vaultARNIndex            map[string]string                              // ARN → vault name
	planARNIndex             map[string]string                              // ARN → plan name
	planIDIndex              map[string]string                              // plan ID → plan name
	frameworkARNIndex        map[string]string                              // ARN → framework name
	reportPlanARNIndex       map[string]string                              // ARN → report plan name
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string
}

// NewInMemoryBackend creates a new in-memory Backup backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		vaults:                   make(map[string]*Vault),
		plans:                    make(map[string]*Plan),
		jobs:                     make(map[string]*Job),
		selections:               make(map[string]map[string]*Selection),
		frameworks:               make(map[string]*Framework),
		legalHolds:               make(map[string]*LegalHold),
		reportPlans:              make(map[string]*ReportPlan),
		restoreAccessVaults:      make(map[string]*RestoreAccessVault),
		restoreTestingPlans:      make(map[string]*RestoreTestingPlan),
		restoreTestingSelections: make(map[string]map[string]*RestoreTestingSelection),
		mpaApprovals:             make(map[string]string),
		vaultARNIndex:            make(map[string]string),
		planARNIndex:             make(map[string]string),
		planIDIndex:              make(map[string]string),
		frameworkARNIndex:        make(map[string]string),
		reportPlanARNIndex:       make(map[string]string),
		accountID:                accountID,
		region:                   region,
		mu:                       lockmetrics.New("backup"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

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
		return nil, fmt.Errorf("%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters", ErrValidation)
	}

	if _, ok := b.vaults[name]; ok {
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
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.vaults[name] = v
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// DescribeBackupVault returns a vault by name.
func (b *InMemoryBackend) DescribeBackupVault(name string) (*Vault, error) {
	b.mu.RLock("DescribeBackupVault")
	defer b.mu.RUnlock()

	v, ok := b.vaults[name]
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

	list := make([]*Vault, 0, len(b.vaults))
	for _, v := range b.vaults {
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

	v, ok := b.vaults[name]
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}

	delete(b.vaultARNIndex, v.BackupVaultArn)
	delete(b.vaults, name)
	v.Tags.Close()

	return nil
}

// CreateBackupPlan creates a new backup plan.
func (b *InMemoryBackend) CreateBackupPlan(planName string, rules []Rule, kv map[string]string) (*Plan, error) {
	b.mu.Lock("CreateBackupPlan")
	defer b.mu.Unlock()

	if _, ok := b.plans[planName]; ok {
		return nil, fmt.Errorf("%w: plan %s already exists", ErrAlreadyExists, planName)
	}

	id := uuid.NewString()
	planARN := arn.Build("backup", b.region, b.accountID, "backup-plan:"+id)
	t := tags.New("backup.plan." + planName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	p := &Plan{
		BackupPlanName: planName,
		BackupPlanArn:  planARN,
		BackupPlanID:   id,
		VersionID:      uuid.NewString(),
		Rules:          rules,
		AccountID:      b.accountID,
		Region:         b.region,
		CreationTime:   time.Now().UTC(),
		Tags:           t,
	}
	b.plans[planName] = p
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
	if p, ok := b.plans[idOrName]; ok {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)

		return &cp, nil
	}
	// Try by ID using the O(1) index.
	if name, ok := b.planIDIndex[idOrName]; ok {
		p := b.plans[name]
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

	list := make([]*Plan, 0, len(b.plans))
	for _, p := range b.plans {
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
func (b *InMemoryBackend) UpdateBackupPlan(idOrName string, rules []Rule) (*Plan, error) {
	b.mu.Lock("UpdateBackupPlan")
	defer b.mu.Unlock()

	// Find by name first, then by ID using the O(1) index.
	var found *Plan
	if p, ok := b.plans[idOrName]; ok {
		found = p
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		found = b.plans[idName]
	}

	if found == nil {
		return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	found.Rules = rules
	found.VersionID = uuid.NewString()
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
	if _, ok := b.plans[idOrName]; ok {
		planName = idOrName
	} else if idName, ok2 := b.planIDIndex[idOrName]; ok2 {
		planName = idName
	} else {
		return fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	p := b.plans[planName]
	delete(b.planARNIndex, p.BackupPlanArn)
	delete(b.planIDIndex, p.BackupPlanID)
	delete(b.plans, planName)
	p.Tags.Close()

	return nil
}

// StartBackupJob starts a new backup job.
func (b *InMemoryBackend) StartBackupJob(vaultName, resourceArn, iamRoleArn, resourceType string) (*Job, error) {
	b.mu.Lock("StartBackupJob")
	defer b.mu.Unlock()

	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	if iamRoleArn == "" {
		return nil, fmt.Errorf("%w: IamRoleArn is required", ErrValidation)
	}

	if _, ok := b.vaults[vaultName]; !ok {
		return nil, fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	vault := b.vaults[vaultName]
	jobID := uuid.NewString()
	j := &Job{
		BackupJobID:     jobID,
		BackupVaultName: vaultName,
		BackupVaultArn:  vault.BackupVaultArn,
		ResourceArn:     resourceArn,
		IAMRoleArn:      iamRoleArn,
		ResourceType:    resourceType,
		State:           "CREATED",
		AccountID:       b.accountID,
		Region:          b.region,
		CreationTime:    time.Now().UTC(),
	}
	b.jobs[jobID] = j
	cp := *j

	return &cp, nil
}

// DescribeBackupJob returns a backup job by ID.
func (b *InMemoryBackend) DescribeBackupJob(jobID string) (*Job, error) {
	b.mu.RLock("DescribeBackupJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[jobID]
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

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
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
		b.vaults[name].Tags.Merge(kv)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		b.plans[name].Tags.Merge(kv)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		b.frameworks[name].Tags.Merge(kv)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		b.reportPlans[name].Tags.Merge(kv)

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
		return b.vaults[name].Tags.Clone(), nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		return b.plans[name].Tags.Clone(), nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		return b.frameworks[name].Tags.Clone(), nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		return b.reportPlans[name].Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// UntagResource removes the given tag keys from a resource identified by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		b.vaults[name].Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		b.plans[name].Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		b.frameworks[name].Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		b.reportPlans[name].Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// AssociateBackupVaultMpaApprovalTeam associates an MPA approval team with a backup vault.
func (b *InMemoryBackend) AssociateBackupVaultMpaApprovalTeam(vaultName, mpaApprovalTeamArn string) error {
	b.mu.Lock("AssociateBackupVaultMpaApprovalTeam")
	defer b.mu.Unlock()

	if _, ok := b.vaults[vaultName]; !ok {
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

	if _, ok := b.legalHolds[legalHoldID]; !ok {
		return fmt.Errorf("%w: legal hold %s not found", ErrNotFound, legalHoldID)
	}

	delete(b.legalHolds, legalHoldID)

	return nil
}

// CreateBackupSelection creates a backup selection for a plan.
func (b *InMemoryBackend) CreateBackupSelection(planID, selectionName, iamRoleArn string) (*Selection, error) {
	b.mu.Lock("CreateBackupSelection")
	defer b.mu.Unlock()

	if selectionName == "" {
		return nil, fmt.Errorf("%w: SelectionName is required", ErrValidation)
	}

	// Resolve planID: accept either a plan ID (from planIDIndex) or a plan name.
	if _, found := b.planIDIndex[planID]; !found {
		// planID is not a known ID — try it as a plan name.
		p, exists := b.plans[planID]
		if !exists {
			return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, planID)
		}
		// Switch planID to the canonical UUID stored on the plan.
		planID = p.BackupPlanID
	}

	if b.selections[planID] == nil {
		b.selections[planID] = make(map[string]*Selection)
	}

	selectionID := uuid.NewString()
	sel := &Selection{
		SelectionID:   selectionID,
		SelectionName: selectionName,
		BackupPlanID:  planID,
		IAMRoleArn:    iamRoleArn,
		CreationTime:  time.Now().UTC(),
	}
	b.selections[planID][selectionID] = sel
	cp := *sel

	return &cp, nil
}

// CreateFramework creates an audit framework.
func (b *InMemoryBackend) CreateFramework(name, description string) (*Framework, error) {
	b.mu.Lock("CreateFramework")
	defer b.mu.Unlock()

	if _, ok := b.frameworks[name]; ok {
		return nil, fmt.Errorf("%w: framework %s already exists", ErrAlreadyExists, name)
	}

	frameworkARN := arn.Build("backup", b.region, b.accountID, "framework:"+name)
	t := tags.New("backup.framework." + name + ".tags")
	f := &Framework{
		FrameworkName:        name,
		FrameworkArn:         frameworkARN,
		FrameworkDescription: description,
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.frameworks[name] = f
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
		Status:       "ACTIVE",
		CreationDate: time.Now().UTC(),
	}
	b.legalHolds[id] = lh
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
		return nil, fmt.Errorf("%w: BackupVaultName must be 2-50 alphanumeric or hyphen characters", ErrValidation)
	}

	if minRetentionDays <= 0 {
		return nil, fmt.Errorf("%w: MinRetentionDays must be greater than 0", ErrValidation)
	}

	if maxRetentionDays < minRetentionDays {
		return nil, fmt.Errorf("%w: MaxRetentionDays must be >= MinRetentionDays", ErrValidation)
	}

	if _, ok := b.vaults[name]; ok {
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
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		MinRetentionDays: minRetentionDays,
		MaxRetentionDays: maxRetentionDays,
		Tags:             t,
	}
	b.vaults[name] = v
	b.vaultARNIndex[vaultARN] = name
	cp := *v

	return &cp, nil
}

// CreateReportPlan creates a report plan.
func (b *InMemoryBackend) CreateReportPlan(name, description string) (*ReportPlan, error) {
	b.mu.Lock("CreateReportPlan")
	defer b.mu.Unlock()

	if _, ok := b.reportPlans[name]; ok {
		return nil, fmt.Errorf("%w: report plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "report-plan:"+name)
	t := tags.New("backup.report-plan." + name + ".tags")
	rp := &ReportPlan{
		ReportPlanName:        name,
		ReportPlanArn:         planARN,
		ReportPlanDescription: description,
		CreationTime:          time.Now().UTC(),
		Tags:                  t,
	}
	b.reportPlans[name] = rp
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

	if _, ok := b.restoreAccessVaults[vaultName]; ok {
		return nil, fmt.Errorf("%w: restore access vault %s already exists", ErrAlreadyExists, vaultName)
	}

	vaultARN := arn.Build("backup", b.region, b.accountID, "restore-access-backup-vault:"+vaultName)
	rav := &RestoreAccessVault{
		RestoreAccessBackupVaultName: vaultName,
		RestoreAccessBackupVaultArn:  vaultARN,
		SourceBackupVaultArn:         sourceVaultArn,
		VaultState:                   "CREATING",
		CreationDate:                 time.Now().UTC(),
	}
	b.restoreAccessVaults[vaultName] = rav
	cp := *rav

	return &cp, nil
}

// CreateRestoreTestingPlan creates a restore testing plan.
func (b *InMemoryBackend) CreateRestoreTestingPlan(name, scheduleExpression string) (*RestoreTestingPlan, error) {
	b.mu.Lock("CreateRestoreTestingPlan")
	defer b.mu.Unlock()

	if _, ok := b.restoreTestingPlans[name]; ok {
		return nil, fmt.Errorf("%w: restore testing plan %s already exists", ErrAlreadyExists, name)
	}

	planARN := arn.Build("backup", b.region, b.accountID, "restore-testing-plan:"+name)
	rtp := &RestoreTestingPlan{
		RestoreTestingPlanName: name,
		RestoreTestingPlanArn:  planARN,
		ScheduleExpression:     scheduleExpression,
		CreationTime:           time.Now().UTC(),
	}
	b.restoreTestingPlans[name] = rtp
	b.restoreTestingSelections[name] = make(map[string]*RestoreTestingSelection)
	cp := *rtp

	return &cp, nil
}

// CreateRestoreTestingSelection creates a selection within a restore testing plan.
func (b *InMemoryBackend) CreateRestoreTestingSelection(
	planName, selectionName, protectedResourceType string,
) (*RestoreTestingSelection, error) {
	b.mu.Lock("CreateRestoreTestingSelection")
	defer b.mu.Unlock()

	rtp, found := b.restoreTestingPlans[planName]
	if !found {
		return nil, fmt.Errorf("%w: restore testing plan %s not found", ErrNotFound, planName)
	}

	if _, exists := b.restoreTestingSelections[planName][selectionName]; exists {
		return nil, fmt.Errorf("%w: restore testing selection %s already exists", ErrAlreadyExists, selectionName)
	}

	sel := &RestoreTestingSelection{
		RestoreTestingPlanName:      planName,
		RestoreTestingSelectionName: selectionName,
		RestoreTestingPlanArn:       rtp.RestoreTestingPlanArn,
		ProtectedResourceType:       protectedResourceType,
		CreationTime:                time.Now().UTC(),
	}
	b.restoreTestingSelections[planName][selectionName] = sel
	cp := *sel

	return &cp, nil
}

// Reset clears all state, returning the backend to a clean initial state.
// Tags resources are properly closed before discarding.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, v := range b.vaults {
		if v.Tags != nil {
			v.Tags.Close()
		}
	}

	for _, p := range b.plans {
		if p.Tags != nil {
			p.Tags.Close()
		}
	}

	for _, f := range b.frameworks {
		if f.Tags != nil {
			f.Tags.Close()
		}
	}

	for _, rp := range b.reportPlans {
		if rp.Tags != nil {
			rp.Tags.Close()
		}
	}

	b.vaults = make(map[string]*Vault)
	b.plans = make(map[string]*Plan)
	b.jobs = make(map[string]*Job)
	b.selections = make(map[string]map[string]*Selection)
	b.frameworks = make(map[string]*Framework)
	b.legalHolds = make(map[string]*LegalHold)
	b.reportPlans = make(map[string]*ReportPlan)
	b.restoreAccessVaults = make(map[string]*RestoreAccessVault)
	b.restoreTestingPlans = make(map[string]*RestoreTestingPlan)
	b.restoreTestingSelections = make(map[string]map[string]*RestoreTestingSelection)
	b.mpaApprovals = make(map[string]string)
	b.vaultARNIndex = make(map[string]string)
	b.planARNIndex = make(map[string]string)
	b.planIDIndex = make(map[string]string)
	b.frameworkARNIndex = make(map[string]string)
	b.reportPlanARNIndex = make(map[string]string)
}
