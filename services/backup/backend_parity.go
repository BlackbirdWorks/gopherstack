package backup

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	defaultMaxResults = 1000
	maxAllowedResults = 1000

	// completedJobBytes is the simulated transfer / backup size for completed jobs.
	completedJobBytes = 1024
)

// ---- Rule validation ----

// validateRules checks that each rule has required fields and that rule names are unique.
func validateRules(rules []Rule) error {
	seen := make(map[string]struct{}, len(rules))
	for i, r := range rules {
		if r.RuleName == "" {
			return fmt.Errorf("%w: rule[%d]: RuleName is required", ErrValidation, i)
		}
		if r.TargetVaultName == "" {
			return fmt.Errorf(
				"%w: rule %q: TargetBackupVaultName is required",
				ErrValidation,
				r.RuleName,
			)
		}
		if _, dup := seen[r.RuleName]; dup {
			return fmt.Errorf("%w: duplicate rule name %q", ErrValidation, r.RuleName)
		}
		seen[r.RuleName] = struct{}{}
		if r.Lifecycle != nil {
			if r.Lifecycle.DeleteAfterDays > 0 && r.Lifecycle.MoveToColdStorageAfterDays > 0 &&
				r.Lifecycle.DeleteAfterDays <= r.Lifecycle.MoveToColdStorageAfterDays {
				return fmt.Errorf(
					"%w: rule %q: DeleteAfterDays must be greater than MoveToColdStorageAfterDays",
					ErrValidation,
					r.RuleName,
				)
			}
		}
	}

	return nil
}

// ---- ListBackupJobs filtering + pagination ----

// ListBackupJobsFilter contains optional filter parameters for listing backup jobs.
type ListBackupJobsFilter struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	VaultName     string
	State         string
	ResourceArn   string
	ResourceType  string
	AccountID     string
	ParentJobID   string
	NextToken     string
	MaxResults    int
}

// inTimeRange returns false if t is outside the [after, before) window.
// Either bound may be nil (meaning "no bound").
func inTimeRange(t time.Time, after, before *time.Time) bool {
	if after != nil && !t.After(*after) {
		return false
	}
	if before != nil && !t.Before(*before) {
		return false
	}

	return true
}

// jobMatchesFilter reports whether j satisfies all active fields in f.
func jobMatchesFilter(j *Job, f ListBackupJobsFilter) bool {
	switch {
	case f.VaultName != "" && j.BackupVaultName != f.VaultName:
		return false
	case f.State != "" && j.State != f.State:
		return false
	case f.ResourceArn != "" && j.ResourceArn != f.ResourceArn:
		return false
	case f.ResourceType != "" && j.ResourceType != f.ResourceType:
		return false
	case f.AccountID != "" && j.AccountID != f.AccountID:
		return false
	case f.ParentJobID != "" && j.ParentJobID != f.ParentJobID:
		return false
	}

	return inTimeRange(j.CreationTime, f.CreatedAfter, f.CreatedBefore)
}

// ListBackupJobsFiltered returns backup jobs matching the filter, with pagination.
// Returns (jobs, nextToken).
func (b *InMemoryBackend) ListBackupJobsFiltered(f ListBackupJobsFilter) ([]*Job, string) {
	b.mu.RLock("ListBackupJobsFiltered")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		if !jobMatchesFilter(j, f) {
			continue
		}
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Job) int {
		if d := b.CreationTime.Compare(a.CreationTime); d != 0 {
			return d
		}

		return strings.Compare(a.BackupJobID, b.BackupJobID)
	})

	return paginateByID(
		list,
		func(j *Job) string { return j.BackupJobID },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- ListRecoveryPoints filtering + pagination ----

// ListRPFilter contains optional filter parameters for listing recovery points.
type ListRPFilter struct {
	CreatedAfter           *time.Time
	CreatedBefore          *time.Time
	ResourceArn            string
	ResourceType           string
	ParentRecoveryPointArn string
	NextToken              string
	MaxResults             int
}

// rpMatchesFilter reports whether rp satisfies all active fields in f.
func rpMatchesFilter(rp *RecoveryPoint, f ListRPFilter) bool {
	if f.ResourceArn != "" && rp.ResourceArn != f.ResourceArn {
		return false
	}
	if f.ResourceType != "" && rp.ResourceType != f.ResourceType {
		return false
	}
	if f.ParentRecoveryPointArn != "" && rp.ParentRecoveryPointArn != f.ParentRecoveryPointArn {
		return false
	}
	if f.CreatedAfter != nil && !rp.CreationDate.After(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && !rp.CreationDate.Before(*f.CreatedBefore) {
		return false
	}

	return true
}

// ListRecoveryPointsFiltered returns recovery points for a vault with optional filters and pagination.
func (b *InMemoryBackend) ListRecoveryPointsFiltered(
	vaultName string,
	f ListRPFilter,
) ([]*RecoveryPoint, string, error) {
	b.mu.RLock("ListRecoveryPointsFiltered")
	defer b.mu.RUnlock()

	if _, ok := b.vaults[vaultName]; !ok {
		return nil, "", fmt.Errorf("%w: vault %s not found", ErrNotFound, vaultName)
	}

	pts := b.recoveryPoints[vaultName]
	list := make([]*RecoveryPoint, 0, len(pts))
	for _, rp := range pts {
		if !rpMatchesFilter(rp, f) {
			continue
		}
		cp := *rp
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *RecoveryPoint) int {
		if d := b.CreationDate.Compare(a.CreationDate); d != 0 {
			return d
		}

		return strings.Compare(a.RecoveryPointArn, b.RecoveryPointArn)
	})

	page, token := paginateByID(
		list,
		func(rp *RecoveryPoint) string { return rp.RecoveryPointArn },
		f.MaxResults,
		f.NextToken,
	)

	return page, token, nil
}

// ---- ListCopyJobs filtering + pagination ----

// ListCopyJobsFilter contains optional filter parameters for listing copy jobs.
type ListCopyJobsFilter struct {
	CreatedAfter              *time.Time
	CreatedBefore             *time.Time
	State                     string
	ResourceArn               string
	ResourceType              string
	SourceBackupVaultArn      string
	DestinationBackupVaultArn string
	AccountID                 string
	NextToken                 string
	MaxResults                int
}

// copyJobMatchesFilter reports whether j satisfies all active fields in f.
func copyJobMatchesFilter(j *CopyJob, f ListCopyJobsFilter) bool {
	// Vault-specific filters checked before the common time-range check.
	if f.SourceBackupVaultArn != "" && j.SourceBackupVaultArn != f.SourceBackupVaultArn {
		return false
	}
	if f.DestinationBackupVaultArn != "" && j.DestinationBackupVaultArn != f.DestinationBackupVaultArn {
		return false
	}
	if f.State != "" && j.State != f.State {
		return false
	}
	if f.ResourceArn != "" && j.ResourceArn != f.ResourceArn {
		return false
	}
	if f.ResourceType != "" && j.ResourceType != f.ResourceType {
		return false
	}
	if f.AccountID != "" && j.AccountID != f.AccountID {
		return false
	}

	return inTimeRange(j.CreationDate, f.CreatedAfter, f.CreatedBefore)
}

// ListCopyJobsFiltered returns copy jobs matching the filter, with pagination.
func (b *InMemoryBackend) ListCopyJobsFiltered(f ListCopyJobsFilter) ([]*CopyJob, string) {
	b.mu.RLock("ListCopyJobsFiltered")
	defer b.mu.RUnlock()

	list := make([]*CopyJob, 0, len(b.copyJobs))
	for _, j := range b.copyJobs {
		if !copyJobMatchesFilter(j, f) {
			continue
		}
		cp := *j
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *CopyJob) int {
		if d := b.CreationDate.Compare(a.CreationDate); d != 0 {
			return d
		}

		return strings.Compare(a.CopyJobID, b.CopyJobID)
	})

	return paginateByID(
		list,
		func(j *CopyJob) string { return j.CopyJobID },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- ListBackupVaults filtering + pagination ----

// ListVaultsFilter contains optional filter parameters for listing backup vaults.
type ListVaultsFilter struct {
	VaultType  string
	NextToken  string
	MaxResults int
}

// ListBackupVaultsFiltered returns vaults with optional type filter and pagination.
func (b *InMemoryBackend) ListBackupVaultsFiltered(f ListVaultsFilter) ([]*Vault, string) {
	b.mu.RLock("ListBackupVaultsFiltered")
	defer b.mu.RUnlock()

	list := make([]*Vault, 0, len(b.vaults))
	for _, v := range b.vaults {
		// Filter by vault type: logically air-gapped vaults have MinRetentionDays > 0.
		if f.VaultType == "LOGICALLY_AIR_GAPPED_BACKUP_VAULT" && v.MinRetentionDays == 0 {
			continue
		}
		if f.VaultType == "BACKUP_VAULT" && v.MinRetentionDays > 0 {
			continue
		}
		cp := *v
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Vault) int {
		return strings.Compare(a.BackupVaultName, b.BackupVaultName)
	})

	return paginateByID(
		list,
		func(v *Vault) string { return v.BackupVaultName },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- ListBackupPlans pagination ----

// ListPlansFilter contains pagination parameters for listing backup plans.
type ListPlansFilter struct {
	NextToken  string
	MaxResults int
}

// ListBackupPlansPaged returns backup plans with pagination.
func (b *InMemoryBackend) ListBackupPlansPaged(f ListPlansFilter) ([]*Plan, string) {
	b.mu.RLock("ListBackupPlansPaged")
	defer b.mu.RUnlock()

	list := make([]*Plan, 0, len(b.plans))
	for _, p := range b.plans {
		cp := *p
		cp.Rules = make([]Rule, len(p.Rules))
		copy(cp.Rules, p.Rules)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Plan) int {
		return strings.Compare(a.BackupPlanName, b.BackupPlanName)
	})

	return paginateByID(
		list,
		func(p *Plan) string { return p.BackupPlanName },
		f.MaxResults,
		f.NextToken,
	)
}

// ---- DeleteBackupPlan with selection validation ----

// DeleteBackupPlanChecked deletes a backup plan, returning an error if selections exist.
func (b *InMemoryBackend) DeleteBackupPlanChecked(idOrName string) (*Plan, error) {
	b.mu.Lock("DeleteBackupPlanChecked")
	defer b.mu.Unlock()

	var planName string
	if _, ok := b.plans[idOrName]; ok {
		planName = idOrName
	} else if name, ok2 := b.planIDIndex[idOrName]; ok2 {
		planName = name
	} else {
		return nil, fmt.Errorf("%w: backup plan %s not found", ErrNotFound, idOrName)
	}

	p := b.plans[planName]

	// AWS requires all selections to be deleted before the plan can be deleted.
	if sels := b.selections[p.BackupPlanID]; len(sels) > 0 {
		return nil, fmt.Errorf(
			"%w: backup plan %s has %d active selection(s); delete them first",
			ErrValidation,
			planName,
			len(sels),
		)
	}

	delete(b.planARNIndex, p.BackupPlanArn)
	delete(b.planIDIndex, p.BackupPlanID)
	delete(b.plans, planName)
	delete(b.selections, p.BackupPlanID)
	cp := *p
	p.Tags.Close()

	return &cp, nil
}

// ---- DeleteBackupVault with lock enforcement ----

// IsVaultLocked reports whether the vault's lock date has passed (vault is now immutable).
func (b *InMemoryBackend) IsVaultLocked(vaultName string) bool {
	b.mu.RLock("IsVaultLocked")
	defer b.mu.RUnlock()

	cfg, ok := b.vaultLockConfigs[vaultName]
	if !ok {
		return false
	}

	return cfg.LockDate != nil && time.Now().UTC().After(*cfg.LockDate)
}

// DeleteBackupVaultChecked deletes a vault, enforcing lock and recovery point constraints.
func (b *InMemoryBackend) DeleteBackupVaultChecked(name string) error {
	b.mu.Lock("DeleteBackupVaultChecked")
	defer b.mu.Unlock()

	v, ok := b.vaults[name]
	if !ok {
		return fmt.Errorf("%w: vault %s not found", ErrNotFound, name)
	}

	if v.NumberOfRecoveryPoints > 0 {
		return fmt.Errorf(
			"%w: vault %s has %d recovery points; delete them first",
			ErrValidation, name, v.NumberOfRecoveryPoints,
		)
	}

	// Locked vaults cannot be deleted.
	if cfg, ok2 := b.vaultLockConfigs[name]; ok2 {
		if cfg.LockDate != nil && time.Now().UTC().After(*cfg.LockDate) {
			return fmt.Errorf(
				"%w: vault %s is locked and cannot be deleted",
				ErrValidation, name,
			)
		}
	}

	delete(b.vaultARNIndex, v.BackupVaultArn)
	delete(b.vaults, name)
	delete(b.vaultLockConfigs, name)
	delete(b.vaultAccessPolicies, name)
	delete(b.vaultNotifications, name)
	v.Tags.Close()

	return nil
}

// ---- CompleteBackupJob ----

// CompleteBackupJob transitions a job from CREATED to COMPLETED and creates a recovery point.
// This models AWS's asynchronous job completion in a synchronous way for the emulator.
func (b *InMemoryBackend) CompleteBackupJob(jobID string) error {
	b.mu.Lock("CompleteBackupJob")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: backup job %s not found", ErrNotFound, jobID)
	}
	if job.State != statusCreated {
		return nil // already done
	}

	now := time.Now().UTC()
	job.State = statusCompleted
	job.CompletionTime = &now
	job.PercentDone = "100.0"
	job.MessageCategory = "SUCCESS"
	job.BytesTransferred = completedJobBytes
	job.BackupSizeInBytes = completedJobBytes

	// Build a recovery point ARN.
	rpID := job.BackupJobID
	rpArn := "arn:aws:backup:" + b.region + ":" + b.accountID + ":recovery-point:" + rpID
	job.RecoveryPointArn = rpArn

	vault, ok2 := b.vaults[job.BackupVaultName]
	if !ok2 {
		return nil // vault deleted between job start and completion
	}

	if b.recoveryPoints[job.BackupVaultName] == nil {
		b.recoveryPoints[job.BackupVaultName] = make(map[string]*RecoveryPoint)
	}

	rp := &RecoveryPoint{
		RecoveryPointArn:  rpArn,
		BackupVaultName:   job.BackupVaultName,
		BackupVaultArn:    vault.BackupVaultArn,
		ResourceArn:       job.ResourceArn,
		ResourceType:      job.ResourceType,
		IAMRoleArn:        job.IAMRoleArn,
		Status:            statusCompleted,
		CreationDate:      now,
		CompletionDate:    &now,
		BackupSizeInBytes: completedJobBytes,
		StorageClass:      "WARM",
		IsEncrypted:       vault.EncryptionKeyArn != "",
		EncryptionKeyArn:  vault.EncryptionKeyArn,
	}
	b.recoveryPoints[job.BackupVaultName][rpArn] = rp
	vault.NumberOfRecoveryPoints++

	// Update protected resource record.
	b.protectedResources[job.ResourceArn] = &ProtectedResource{
		ResourceArn:     job.ResourceArn,
		ResourceType:    job.ResourceType,
		BackupVaultName: job.BackupVaultName,
		LastBackupTime:  now,
	}

	return nil
}

// ---- CreateBackupPlan / UpdateBackupPlan with rule validation ----

// CreateBackupPlanValidated creates a backup plan after validating its rules.
func (b *InMemoryBackend) CreateBackupPlanValidated(
	planName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
	kv map[string]string,
) (*Plan, error) {
	if planName == "" {
		return nil, fmt.Errorf("%w: BackupPlanName is required", ErrValidation)
	}
	if err := validateRules(rules); err != nil {
		return nil, err
	}

	return b.CreateBackupPlan(planName, rules, advancedSettings, kv)
}

// UpdateBackupPlanValidated updates a backup plan after validating rules.
func (b *InMemoryBackend) UpdateBackupPlanValidated(
	idOrName string,
	rules []Rule,
	advancedSettings []AdvancedBackupSetting,
) (*Plan, error) {
	if err := validateRules(rules); err != nil {
		return nil, err
	}

	return b.UpdateBackupPlan(idOrName, rules, advancedSettings)
}

// ---- Generic pagination helper ----

// paginateByID applies cursor-based pagination to a pre-sorted slice.
// keyFn extracts the string key for each item (used as the pagination cursor).
// Returns (page, nextToken). nextToken is "" when no more pages remain.
func paginateByID[T any](list []T, keyFn func(T) string, maxResults int, nextToken string) ([]T, string) {
	if maxResults <= 0 || maxResults > maxAllowedResults {
		maxResults = defaultMaxResults
	}

	// Advance to the cursor item.
	start := 0
	if nextToken != "" {
		found := false
		for i, item := range list {
			if keyFn(item) == nextToken {
				start = i
				found = true

				break
			}
		}
		if !found {
			return []T{}, ""
		}
	}

	list = list[start:]
	if len(list) <= maxResults {
		return list, ""
	}

	// NextToken is the key of the first item of the next page.
	return list[:maxResults], keyFn(list[maxResults])
}

// ParseTimeFilter parses an RFC3339 timestamp string into a *time.Time.
// Returns nil if the string is empty or invalid.
func ParseTimeFilter(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}

	return &t
}
