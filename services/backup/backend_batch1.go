package backup

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

var (
	errProtectedResourceNotFound = errors.New("protected resource not found")
	errRestoreJobNotFound        = errors.New("restore job not found")
	errReportJobNotFound         = errors.New("report job not found")
	errScanJobNotFound           = errors.New("scan job not found")
	errLegalHoldNotFound         = errors.New("legal hold not found")
	errRecoveryPointNotFound     = errors.New("recovery point not found")
	errVaultNotFoundB1           = errors.New("vault not found")
	errBackupJobNotFound         = errors.New("backup job not found")
	errBackupPlanNotFoundB1      = errors.New("backup plan not found")
	errTieringConfigNotFound     = errors.New("tiering configuration not found for vault")
)

const (
	keySummaryCount  = "Count"
	keySummaryRegion = "Region"
)

// ---- New types for batch-1 ops ----

// RestoreJob represents an AWS Backup restore job.
type RestoreJob struct {
	CompletionDate    *time.Time        `json:"completionDate,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	StartTime         time.Time         `json:"startTime"`
	RestoreJobID      string            `json:"restoreJobId"`
	RecoveryPointArn  string            `json:"recoveryPointArn"`
	IAMRoleArn        string            `json:"iamRoleArn"`
	ResourceArn       string            `json:"resourceArn,omitempty"`
	ResourceType      string            `json:"resourceType,omitempty"`
	BackupVaultName   string            `json:"backupVaultName,omitempty"`
	Status            string            `json:"status"`
	StatusMessage     string            `json:"statusMessage,omitempty"`
	PercentDone       string            `json:"percentDone,omitempty"`
	BackupSizeInBytes int64             `json:"backupSizeInBytes,omitempty"`
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

// TieringConfiguration holds tiering settings for a backup vault.
type TieringConfiguration struct {
	BackupVaultArn  string `json:"backupVaultArn"`
	BackupVaultName string `json:"backupVaultName"`
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

// DescribeGlobalSettings returns the account-level global backup settings.
func (b *InMemoryBackend) DescribeGlobalSettings() (map[string]string, time.Time) {
	b.mu.RLock("DescribeGlobalSettings")
	defer b.mu.RUnlock()

	cp := make(map[string]string, len(b.globalSettings))
	maps.Copy(cp, b.globalSettings)

	return cp, b.globalSettingsLastUpdate
}

// UpdateGlobalSettings updates the account-level global backup settings.
func (b *InMemoryBackend) UpdateGlobalSettings(settings map[string]string) {
	b.mu.Lock("UpdateGlobalSettings")
	defer b.mu.Unlock()

	maps.Copy(b.globalSettings, settings)
	b.globalSettingsLastUpdate = time.Now().UTC()
}

// DescribeRegionSettings returns the regional backup preferences.
func (b *InMemoryBackend) DescribeRegionSettings() *RegionSettings {
	b.mu.RLock("DescribeRegionSettings")
	defer b.mu.RUnlock()

	if b.regionSettings == nil {
		return &RegionSettings{
			ResourceTypeManagementPreference: map[string]bool{},
			ResourceTypeOptInPreference:      map[string]bool{},
		}
	}

	return b.regionSettings
}

// UpdateRegionSettings updates the regional backup preferences.
func (b *InMemoryBackend) UpdateRegionSettings(
	mgmtPref map[string]bool,
	optInPref map[string]bool,
) {
	b.mu.Lock("UpdateRegionSettings")
	defer b.mu.Unlock()

	if b.regionSettings == nil {
		b.regionSettings = &RegionSettings{
			ResourceTypeManagementPreference: make(map[string]bool),
			ResourceTypeOptInPreference:      make(map[string]bool),
		}
	}
	maps.Copy(b.regionSettings.ResourceTypeManagementPreference, mgmtPref)
	maps.Copy(b.regionSettings.ResourceTypeOptInPreference, optInPref)
}

// ---- Protected Resources ----

// PutProtectedResource adds or updates a protected resource record (called internally by StartBackupJob).
func (b *InMemoryBackend) PutProtectedResource(resourceArn, resourceType, vaultName string) {
	b.mu.Lock("PutProtectedResource")
	defer b.mu.Unlock()

	b.protectedResources[resourceArn] = &ProtectedResource{
		ResourceArn:     resourceArn,
		ResourceType:    resourceType,
		BackupVaultName: vaultName,
		LastBackupTime:  time.Now().UTC(),
	}
}

// DescribeProtectedResource returns a protected resource by ARN.
func (b *InMemoryBackend) DescribeProtectedResource(
	resourceArn string,
) (*ProtectedResource, error) {
	b.mu.RLock("DescribeProtectedResource")
	defer b.mu.RUnlock()

	pr, ok := b.protectedResources[resourceArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errProtectedResourceNotFound, resourceArn)
	}

	return pr, nil
}

// ListProtectedResources returns all protected resources.
func (b *InMemoryBackend) ListProtectedResources() []*ProtectedResource {
	b.mu.RLock("ListProtectedResources")
	defer b.mu.RUnlock()

	out := make([]*ProtectedResource, 0, len(b.protectedResources))
	for _, pr := range b.protectedResources {
		cp := *pr
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	return out
}

// ListProtectedResourcesByBackupVault returns protected resources for a vault.
func (b *InMemoryBackend) ListProtectedResourcesByBackupVault(
	vaultName string,
) []*ProtectedResource {
	b.mu.RLock("ListProtectedResourcesByBackupVault")
	defer b.mu.RUnlock()

	var out []*ProtectedResource
	for _, pr := range b.protectedResources {
		if pr.BackupVaultName == vaultName {
			cp := *pr
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	return out
}

// ---- Restore Jobs ----

// StartRestoreJob creates a new restore job.
func (b *InMemoryBackend) StartRestoreJob(
	recoveryPointArn, iamRoleArn, resourceType string,
	metadata map[string]string,
) *RestoreJob {
	b.mu.Lock("StartRestoreJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &RestoreJob{
		RestoreJobID:     "restore-job-" + uuid.New().String()[:8],
		RecoveryPointArn: recoveryPointArn,
		IAMRoleArn:       iamRoleArn,
		ResourceType:     resourceType,
		Metadata:         metadata,
		Status:           statusCompleted,
		PercentDone:      "100.0",
		StartTime:        now,
		CompletionDate:   &done,
	}
	b.restoreJobs[job.RestoreJobID] = job

	return job
}

// DescribeRestoreJob returns a restore job by ID.
func (b *InMemoryBackend) DescribeRestoreJob(restoreJobID string) (*RestoreJob, error) {
	b.mu.RLock("DescribeRestoreJob")
	defer b.mu.RUnlock()

	job, ok := b.restoreJobs[restoreJobID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errRestoreJobNotFound, restoreJobID)
	}

	return job, nil
}

// ListRestoreJobs returns all restore jobs.
func (b *InMemoryBackend) ListRestoreJobs() []*RestoreJob {
	b.mu.RLock("ListRestoreJobs")
	defer b.mu.RUnlock()

	out := make([]*RestoreJob, 0, len(b.restoreJobs))
	for _, j := range b.restoreJobs {
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RestoreJobID < out[j].RestoreJobID })

	return out
}

// ListRestoreJobsByProtectedResource returns restore jobs for a given resource ARN.
func (b *InMemoryBackend) ListRestoreJobsByProtectedResource(resourceArn string) []*RestoreJob {
	b.mu.RLock("ListRestoreJobsByProtectedResource")
	defer b.mu.RUnlock()

	var out []*RestoreJob
	for _, j := range b.restoreJobs {
		if j.ResourceArn == resourceArn || j.RecoveryPointArn == resourceArn {
			cp := *j
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RestoreJobID < out[j].RestoreJobID })

	return out
}

// PutRestoreValidationResult records validation for a restore job.
func (b *InMemoryBackend) PutRestoreValidationResult(restoreJobID, validationStatus string) {
	b.mu.Lock("PutRestoreValidationResult")
	defer b.mu.Unlock()

	b.restoreValidations[restoreJobID] = validationStatus
}

// ---- Report Jobs ----

// StartReportJob creates a new report job for a report plan.
func (b *InMemoryBackend) StartReportJob(reportPlanName string) *ReportJob {
	b.mu.Lock("StartReportJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &ReportJob{
		ReportJobID:    "report-job-" + uuid.New().String()[:8],
		ReportPlanArn:  "arn:aws:backup:" + b.region + ":" + b.accountID + ":report-plan:" + reportPlanName,
		Status:         statusCompleted,
		CreationTime:   now,
		CompletionTime: &done,
	}
	b.reportJobs[job.ReportJobID] = job

	return job
}

// DescribeReportJob returns a report job by ID.
func (b *InMemoryBackend) DescribeReportJob(reportJobID string) (*ReportJob, error) {
	b.mu.RLock("DescribeReportJob")
	defer b.mu.RUnlock()

	job, ok := b.reportJobs[reportJobID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errReportJobNotFound, reportJobID)
	}

	return job, nil
}

// ListReportJobs returns all report jobs, optionally filtered by report plan name.
func (b *InMemoryBackend) ListReportJobs(reportPlanName string) []*ReportJob {
	b.mu.RLock("ListReportJobs")
	defer b.mu.RUnlock()

	var out []*ReportJob
	for _, j := range b.reportJobs {
		if reportPlanName != "" &&
			j.ReportPlanArn != "arn:aws:backup:"+b.region+":"+b.accountID+":report-plan:"+reportPlanName {
			continue
		}
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReportJobID < out[j].ReportJobID })

	return out
}

// ---- Scan Jobs ----

// StartScanJob creates a new scan job for a backup vault.
func (b *InMemoryBackend) StartScanJob(backupVaultArn string) *ScanJob {
	b.mu.Lock("StartScanJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &ScanJob{
		ScanJobID:      "scan-job-" + uuid.New().String()[:8],
		BackupVaultArn: backupVaultArn,
		Status:         statusCompleted,
		CreationTime:   now,
		CompletionTime: &done,
	}
	b.scanJobs[job.ScanJobID] = job

	return job
}

// DescribeScanJob returns a scan job by ID.
func (b *InMemoryBackend) DescribeScanJob(scanJobID string) (*ScanJob, error) {
	b.mu.RLock("DescribeScanJob")
	defer b.mu.RUnlock()

	job, ok := b.scanJobs[scanJobID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errScanJobNotFound, scanJobID)
	}

	return job, nil
}

// ListScanJobs returns all scan jobs.
func (b *InMemoryBackend) ListScanJobs() []*ScanJob {
	b.mu.RLock("ListScanJobs")
	defer b.mu.RUnlock()

	out := make([]*ScanJob, 0, len(b.scanJobs))
	for _, j := range b.scanJobs {
		cp := *j
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ScanJobID < out[j].ScanJobID })

	return out
}

// ---- Legal Holds ----

// GetLegalHold returns a legal hold by ID.
func (b *InMemoryBackend) GetLegalHold(legalHoldID string) (*LegalHold, error) {
	b.mu.RLock("GetLegalHold")
	defer b.mu.RUnlock()

	lh, ok := b.legalHolds[legalHoldID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errLegalHoldNotFound, legalHoldID)
	}

	return lh, nil
}

// ListLegalHolds returns all legal holds.
func (b *InMemoryBackend) ListLegalHolds() []*LegalHold {
	b.mu.RLock("ListLegalHolds")
	defer b.mu.RUnlock()

	out := make([]*LegalHold, 0, len(b.legalHolds))
	for _, lh := range b.legalHolds {
		cp := *lh
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LegalHoldID < out[j].LegalHoldID })

	return out
}

// ListRecoveryPointsByLegalHold returns recovery points associated with a legal hold.
// In this implementation, returns all recovery points (legal hold → RP association not tracked).
func (b *InMemoryBackend) ListRecoveryPointsByLegalHold(legalHoldID string) []*RecoveryPoint {
	b.mu.RLock("ListRecoveryPointsByLegalHold")
	defer b.mu.RUnlock()

	_ = legalHoldID // association not tracked; return empty slice for accuracy

	return []*RecoveryPoint{}
}

// ListRecoveryPointsByResource returns recovery points for a given resource ARN across all vaults.
func (b *InMemoryBackend) ListRecoveryPointsByResource(resourceArn string) []*RecoveryPoint {
	b.mu.RLock("ListRecoveryPointsByResource")
	defer b.mu.RUnlock()

	var out []*RecoveryPoint
	for _, vaultRPs := range b.recoveryPoints {
		for _, rp := range vaultRPs {
			if rp.ResourceArn == resourceArn {
				cp := *rp
				out = append(out, &cp)
			}
		}
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].RecoveryPointArn < out[j].RecoveryPointArn },
	)

	return out
}

// ---- Recovery Point lifecycle ----

// UpdateRecoveryPointLifecycle updates the lifecycle of a recovery point.
func (b *InMemoryBackend) UpdateRecoveryPointLifecycle(
	vaultName, recoveryPointArn string,
	moveToColdStorageAfterDays, deleteAfterDays int64,
) error {
	b.mu.Lock("UpdateRecoveryPointLifecycle")
	defer b.mu.Unlock()

	vaultRPs, ok := b.recoveryPoints[vaultName]
	if !ok {
		return fmt.Errorf("%w: %s", errVaultNotFoundB1, vaultName)
	}
	if _, found := vaultRPs[recoveryPointArn]; !found {
		return fmt.Errorf("%w: %s", errRecoveryPointNotFound, recoveryPointArn)
	}
	// Store lifecycle settings in index map.
	key := vaultName + ":" + recoveryPointArn
	b.recoveryPointLifecycle[key] = fmt.Sprintf(
		"%d,%d",
		moveToColdStorageAfterDays,
		deleteAfterDays,
	)

	return nil
}

// GetRecoveryPointIndexDetails returns index details for a recovery point.
func (b *InMemoryBackend) GetRecoveryPointIndexDetails(
	vaultName, recoveryPointArn string,
) (string, error) {
	b.mu.RLock("GetRecoveryPointIndexDetails")
	defer b.mu.RUnlock()

	key := vaultName + ":" + recoveryPointArn
	status := b.recoveryPointIndexStatus[key]
	if status == "" {
		status = statusActive
	}

	return status, nil
}

// UpdateRecoveryPointIndexSettings updates indexing settings for a recovery point.
func (b *InMemoryBackend) UpdateRecoveryPointIndexSettings(
	vaultName, recoveryPointArn, indexStatus string,
) error {
	b.mu.Lock("UpdateRecoveryPointIndexSettings")
	defer b.mu.Unlock()

	key := vaultName + ":" + recoveryPointArn
	b.recoveryPointIndexStatus[key] = indexStatus

	return nil
}

// ListIndexedRecoveryPoints returns recovery points with an active index.
func (b *InMemoryBackend) ListIndexedRecoveryPoints() []*RecoveryPoint {
	b.mu.RLock("ListIndexedRecoveryPoints")
	defer b.mu.RUnlock()

	var out []*RecoveryPoint
	for _, vaultRPs := range b.recoveryPoints {
		for _, rp := range vaultRPs {
			cp := *rp
			out = append(out, &cp)
		}
	}
	sort.Slice(
		out,
		func(i, j int) bool { return out[i].RecoveryPointArn < out[j].RecoveryPointArn },
	)

	return out
}

// ---- Job operations ----

// StopBackupJob cancels a running backup job.
func (b *InMemoryBackend) StopBackupJob(jobID string) error {
	b.mu.Lock("StopBackupJob")
	defer b.mu.Unlock()

	job, ok := b.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: %s", errBackupJobNotFound, jobID)
	}
	job.State = "ABORTED"

	return nil
}

// ListBackupJobSummaries returns a summary of backup jobs by resource type and status.
func (b *InMemoryBackend) ListBackupJobSummaries() []map[string]any {
	b.mu.RLock("ListBackupJobSummaries")
	defer b.mu.RUnlock()

	// Group by state.
	counts := make(map[string]int)
	for _, j := range b.jobs {
		counts[j.State]++
	}

	summaries := make([]map[string]any, 0, len(counts))
	for status, count := range counts {
		summaries = append(summaries, map[string]any{
			"State":          status,
			keySummaryCount:  count,
			keySummaryRegion: b.region,
			"AccountId":      b.accountID,
		})
	}

	return summaries
}

// ListCopyJobSummaries returns a summary of copy jobs.
func (b *InMemoryBackend) ListCopyJobSummaries() []map[string]any {
	b.mu.RLock("ListCopyJobSummaries")
	defer b.mu.RUnlock()

	counts := make(map[string]int)
	for _, j := range b.copyJobs {
		counts[j.State]++
	}

	summaries := make([]map[string]any, 0, len(counts))
	for state, count := range counts {
		summaries = append(summaries, map[string]any{
			"State":          state,
			keySummaryCount:  count,
			keySummaryRegion: b.region,
		})
	}

	return summaries
}

// StartCopyJob creates a new copy job.
func (b *InMemoryBackend) StartCopyJob(
	recoveryPointArn, sourceVaultArn, destVaultArn, iamRoleArn string,
) *CopyJob {
	b.mu.Lock("StartCopyJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	done := now
	job := &CopyJob{
		CopyJobID:                 "copy-job-" + uuid.New().String()[:8],
		SourceBackupVaultArn:      sourceVaultArn,
		DestinationBackupVaultArn: destVaultArn,
		ResourceArn:               recoveryPointArn,
		IAMRoleArn:                iamRoleArn,
		State:                     statusCompleted,
		AccountID:                 b.accountID,
		Region:                    b.region,
		CreationDate:              now,
		CompletionDate:            &done,
	}
	b.copyJobs[job.CopyJobID] = job

	return job
}

// ---- Backup plan operations ----

// ListBackupPlanVersions returns all versions of a backup plan.
func (b *InMemoryBackend) ListBackupPlanVersions(planID string) ([]*Plan, error) {
	b.mu.RLock("ListBackupPlanVersions")
	defer b.mu.RUnlock()

	// Find plan by ID.
	name, ok := b.planIDIndex[planID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, planID)
	}
	plan, ok := b.plans[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, name)
	}

	cp := *plan
	// Return single version (versioning not tracked in depth).
	return []*Plan{&cp}, nil
}

// ExportBackupPlanTemplate exports a plan as a template JSON string.
func (b *InMemoryBackend) ExportBackupPlanTemplate(planID string) (string, error) {
	b.mu.RLock("ExportBackupPlanTemplate")
	defer b.mu.RUnlock()

	name, ok := b.planIDIndex[planID]
	if !ok {
		return "", fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, planID)
	}
	plan, ok := b.plans[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", errBackupPlanNotFoundB1, name)
	}

	return fmt.Sprintf(`{"BackupPlanName":"%s","Rules":[]}`, plan.BackupPlanName), nil
}

// ---- Tiering configurations ----

// CreateTieringConfiguration creates a tiering configuration for a vault.
func (b *InMemoryBackend) CreateTieringConfiguration(vaultName string) error {
	b.mu.Lock("CreateTieringConfiguration")
	defer b.mu.Unlock()

	vault, ok := b.vaults[vaultName]
	if !ok {
		return fmt.Errorf("%w: %s", errVaultNotFoundB1, vaultName)
	}
	b.tieringConfigs[vaultName] = &TieringConfiguration{
		BackupVaultName: vaultName,
		BackupVaultArn:  vault.BackupVaultArn,
	}

	return nil
}

// DeleteTieringConfiguration removes a tiering configuration.
func (b *InMemoryBackend) DeleteTieringConfiguration(vaultName string) error {
	b.mu.Lock("DeleteTieringConfiguration")
	defer b.mu.Unlock()

	delete(b.tieringConfigs, vaultName)

	return nil
}

// GetTieringConfiguration returns the tiering configuration for a vault.
func (b *InMemoryBackend) GetTieringConfiguration(vaultName string) (*TieringConfiguration, error) {
	b.mu.RLock("GetTieringConfiguration")
	defer b.mu.RUnlock()

	tc, ok := b.tieringConfigs[vaultName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errTieringConfigNotFound, vaultName)
	}

	return tc, nil
}

// ListTieringConfigurations returns all tiering configurations.
func (b *InMemoryBackend) ListTieringConfigurations() []*TieringConfiguration {
	b.mu.RLock("ListTieringConfigurations")
	defer b.mu.RUnlock()

	out := make([]*TieringConfiguration, 0, len(b.tieringConfigs))
	for _, tc := range b.tieringConfigs {
		cp := *tc
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BackupVaultName < out[j].BackupVaultName })

	return out
}

// UpdateTieringConfiguration updates a tiering configuration (no-op: config has no mutable fields in this mock).
func (b *InMemoryBackend) UpdateTieringConfiguration(vaultName string) error {
	b.mu.Lock("UpdateTieringConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.tieringConfigs[vaultName]; !ok {
		return fmt.Errorf("%w: %s", errTieringConfigNotFound, vaultName)
	}

	return nil
}

// ---- Restore Access Vaults ----

// ListRestoreAccessBackupVaults returns all restore access vaults.
func (b *InMemoryBackend) ListRestoreAccessBackupVaults() []*RestoreAccessVault {
	b.mu.RLock("ListRestoreAccessBackupVaults")
	defer b.mu.RUnlock()

	out := make([]*RestoreAccessVault, 0, len(b.restoreAccessVaults))
	for _, v := range b.restoreAccessVaults {
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RestoreAccessBackupVaultName < out[j].RestoreAccessBackupVaultName
	})

	return out
}

// RevokeRestoreAccessBackupVault removes a restore access vault.
func (b *InMemoryBackend) RevokeRestoreAccessBackupVault(vaultName string) error {
	b.mu.Lock("RevokeRestoreAccessBackupVault")
	defer b.mu.Unlock()

	delete(b.restoreAccessVaults, vaultName)

	return nil
}

// ---- MPA Approvals ----

// DisassociateBackupVaultMpaApprovalTeam removes the MPA approval team for a vault.
func (b *InMemoryBackend) DisassociateBackupVaultMpaApprovalTeam(vaultName string) error {
	b.mu.Lock("DisassociateBackupVaultMpaApprovalTeam")
	defer b.mu.Unlock()

	delete(b.mpaApprovals, vaultName)

	return nil
}
