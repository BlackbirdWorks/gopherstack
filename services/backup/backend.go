package backup

import (
	"fmt"
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

// InMemoryBackend is the in-memory store for AWS Backup resources.
type InMemoryBackend struct {
	vaults        map[string]*Vault
	plans         map[string]*Plan
	jobs          map[string]*Job
	vaultARNIndex map[string]string // ARN → vault name
	planARNIndex  map[string]string // ARN → plan name
	planIDIndex   map[string]string // plan ID → plan name
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new in-memory Backup backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		vaults:        make(map[string]*Vault),
		plans:         make(map[string]*Plan),
		jobs:          make(map[string]*Job),
		vaultARNIndex: make(map[string]string),
		planARNIndex:  make(map[string]string),
		planIDIndex:   make(map[string]string),
		accountID:     accountID,
		region:        region,
		mu:            lockmetrics.New("backup"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateBackupVault creates a new backup vault.
func (b *InMemoryBackend) CreateBackupVault(
	name, encryptionKeyArn, creatorRequestID string,
	kv map[string]string,
) (*Vault, error) {
	b.mu.Lock("CreateBackupVault")
	defer b.mu.Unlock()

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

// ListBackupVaults returns all backup vaults.
func (b *InMemoryBackend) ListBackupVaults() []*Vault {
	b.mu.RLock("ListBackupVaults")
	defer b.mu.RUnlock()

	list := make([]*Vault, 0, len(b.vaults))
	for _, v := range b.vaults {
		cp := *v
		list = append(list, &cp)
	}

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

// ListBackupPlans returns all backup plans.
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

	return list
}

// TagResource adds tags to a resource by ARN.
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

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// ListTags returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		return b.vaults[name].Tags.Clone(), nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		return b.plans[name].Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}
