package backup

import "github.com/blackbirdworks/gopherstack/pkgs/service"

const (
	opUnknown          = "Unknown"
	keyBackupVaultArn  = "BackupVaultArn"
	keyBackupVaultName = "BackupVaultName"
	keyCreationDate    = "CreationDate"
	keyBackupPlanArn   = "BackupPlanArn"
	keyBackupPlanID    = "BackupPlanId"
	keyVersionID       = "VersionId"
	keyBackupJobID     = "BackupJobId"
	keyCreationTime    = "CreationTime"
	keyVaultType       = "VaultType"
	keyAccountID       = "AccountId"
)

const (
	opAssociateBackupVaultMpaApprovalTeam = "AssociateBackupVaultMpaApprovalTeam"
	opCancelLegalHold                     = "CancelLegalHold"
	opCreateBackupPlan                    = "CreateBackupPlan"
	opCreateBackupSelection               = "CreateBackupSelection"
	opCreateBackupVault                   = "CreateBackupVault"
	opCreateFramework                     = "CreateFramework"
	opCreateLegalHold                     = "CreateLegalHold"
	opCreateLogicallyAirGappedBackupVault = "CreateLogicallyAirGappedBackupVault"
	opCreateReportPlan                    = "CreateReportPlan"
	opCreateRestoreAccessBackupVault      = "CreateRestoreAccessBackupVault"
	opCreateRestoreTestingPlan            = "CreateRestoreTestingPlan"
	opCreateRestoreTestingSelection       = "CreateRestoreTestingSelection"
	opDeleteBackupPlan                    = "DeleteBackupPlan"
	opDeleteBackupVault                   = "DeleteBackupVault"
	opDescribeBackupJob                   = "DescribeBackupJob"
	opDescribeBackupVault                 = "DescribeBackupVault"
	opGetBackupPlan                       = "GetBackupPlan"
	opListBackupJobs                      = "ListBackupJobs"
	opListBackupPlans                     = "ListBackupPlans"
	opListBackupVaults                    = "ListBackupVaults"
	opListTags                            = "ListTags"
	opStartBackupJob                      = "StartBackupJob"
	opTagResource                         = "TagResource"
	opUntagResource                       = "UntagResource"
	opUpdateBackupPlan                    = "UpdateBackupPlan"

	// Recovery point operations.
	opListRecoveryPointsByBackupVault     = "ListRecoveryPointsByBackupVault"
	opDescribeRecoveryPoint               = "DescribeRecoveryPoint"
	opGetRecoveryPointRestoreMetadata     = "GetRecoveryPointRestoreMetadata"
	opDeleteRecoveryPoint                 = "DeleteRecoveryPoint"
	opDisassociateRecoveryPoint           = "DisassociateRecoveryPoint"
	opDisassociateRecoveryPointFromParent = "DisassociateRecoveryPointFromParent"

	// Vault compliance operations.
	opPutBackupVaultAccessPolicy         = "PutBackupVaultAccessPolicy"
	opGetBackupVaultAccessPolicy         = "GetBackupVaultAccessPolicy"
	opDeleteBackupVaultAccessPolicy      = "DeleteBackupVaultAccessPolicy"
	opPutBackupVaultLockConfiguration    = "PutBackupVaultLockConfiguration"
	opDeleteBackupVaultLockConfiguration = "DeleteBackupVaultLockConfiguration"
	opPutBackupVaultNotifications        = "PutBackupVaultNotifications"
	opGetBackupVaultNotifications        = "GetBackupVaultNotifications"
	opDeleteBackupVaultNotifications     = "DeleteBackupVaultNotifications"

	// Backup selection read/delete operations.
	opGetBackupSelection    = "GetBackupSelection"
	opListBackupSelections  = "ListBackupSelections"
	opDeleteBackupSelection = "DeleteBackupSelection"

	// Copy job operations.
	opListCopyJobs    = "ListCopyJobs"
	opDescribeCopyJob = "DescribeCopyJob"

	// Restore testing read/update/delete operations.
	opGetRestoreTestingPlan         = "GetRestoreTestingPlan"
	opListRestoreTestingPlans       = "ListRestoreTestingPlans"
	opUpdateRestoreTestingPlan      = "UpdateRestoreTestingPlan"
	opDeleteRestoreTestingPlan      = "DeleteRestoreTestingPlan"
	opGetRestoreTestingSelection    = "GetRestoreTestingSelection"
	opListRestoreTestingSelections  = "ListRestoreTestingSelections"
	opUpdateRestoreTestingSelection = "UpdateRestoreTestingSelection"
	opDeleteRestoreTestingSelection = "DeleteRestoreTestingSelection"

	// Framework read/update/delete operations.
	opDescribeFramework = "DescribeFramework"
	opListFrameworks    = "ListFrameworks"
	opUpdateFramework   = "UpdateFramework"
	opDeleteFramework   = "DeleteFramework"

	// Report plan read/update/delete operations.
	opListReportPlans    = "ListReportPlans"
	opDescribeReportPlan = "DescribeReportPlan"
	opUpdateReportPlan   = "UpdateReportPlan"
	opDeleteReportPlan   = "DeleteReportPlan"

	// Extended operations (settings, protected resources, restore/report/scan
	// jobs, plan templates, tiering, restore-access vaults).
	opCreateTieringConfiguration             = "CreateTieringConfiguration"
	opDeleteTieringConfiguration             = "DeleteTieringConfiguration"
	opDescribeGlobalSettings                 = "DescribeGlobalSettings"
	opDescribeProtectedResource              = "DescribeProtectedResource"
	opDescribeRegionSettings                 = "DescribeRegionSettings"
	opDescribeReportJob                      = "DescribeReportJob"
	opDescribeRestoreJob                     = "DescribeRestoreJob"
	opDescribeScanJob                        = "DescribeScanJob"
	opDisassociateBackupVaultMpaApprovalTeam = "DisassociateBackupVaultMpaApprovalTeam"
	opExportBackupPlanTemplate               = "ExportBackupPlanTemplate"
	opGetBackupPlanFromJSON                  = "GetBackupPlanFromJSON"
	opGetBackupPlanFromTemplate              = "GetBackupPlanFromTemplate"
	opGetLegalHold                           = "GetLegalHold"
	opGetPITRMalwareScanResults              = "GetPITRMalwareScanResults"
	opGetRecoveryPointIndexDetails           = "GetRecoveryPointIndexDetails"
	opGetRestoreJobMetadata                  = "GetRestoreJobMetadata"
	opGetRestoreTestingInferredMetadata      = "GetRestoreTestingInferredMetadata"
	opGetSupportedResourceTypes              = "GetSupportedResourceTypes"
	opGetTieringConfiguration                = "GetTieringConfiguration"
	opListBackupJobSummaries                 = "ListBackupJobSummaries"
	opListBackupPlanTemplates                = "ListBackupPlanTemplates"
	opListBackupPlanVersions                 = "ListBackupPlanVersions"
	opListCopyJobSummaries                   = "ListCopyJobSummaries"
	opListIndexedRecoveryPoints              = "ListIndexedRecoveryPoints"
	opListLegalHolds                         = "ListLegalHolds"
	opListProtectedResources                 = "ListProtectedResources"
	opListProtectedResourcesByBackupVault    = "ListProtectedResourcesByBackupVault"
	opListRecoveryPointsByLegalHold          = "ListRecoveryPointsByLegalHold"
	opListRecoveryPointsByResource           = "ListRecoveryPointsByResource"
	opListReportJobs                         = "ListReportJobs"
	opListRestoreAccessBackupVaults          = "ListRestoreAccessBackupVaults"
	opListRestoreJobSummaries                = "ListRestoreJobSummaries"
	opListRestoreJobs                        = "ListRestoreJobs"
	opListRestoreJobsByProtectedResource     = "ListRestoreJobsByProtectedResource"
	opListScanJobSummaries                   = "ListScanJobSummaries"
	opListScanJobs                           = "ListScanJobs"
	opListTieringConfigurations              = "ListTieringConfigurations"
	opPutRestoreValidationResult             = "PutRestoreValidationResult"
	opRevokeRestoreAccessBackupVault         = "RevokeRestoreAccessBackupVault"
	opStartCopyJob                           = "StartCopyJob"
	opStartReportJob                         = "StartReportJob"
	opStartRestoreJob                        = "StartRestoreJob"
	opStartScanJob                           = "StartScanJob"
	opStopBackupJob                          = "StopBackupJob"
	opUpdateGlobalSettings                   = "UpdateGlobalSettings"
	opUpdateRecoveryPointIndexSettings       = "UpdateRecoveryPointIndexSettings"
	opUpdateRecoveryPointLifecycle           = "UpdateRecoveryPointLifecycle"
	opUpdateRegionSettings                   = "UpdateRegionSettings"
	opUpdateTieringConfiguration             = "UpdateTieringConfiguration"
)

const (
	backupMatchPriority = service.PriorityPathVersioned

	pathBackupVaults             = "/backup-vaults"
	pathBackupPlans              = "/backup/plans"
	pathBackupTemplate           = "/backup/template"
	pathBackupJobs               = "/backup-jobs"
	pathCopyJobs                 = "/copy-jobs"
	pathTags                     = "/tags/"
	pathUntag                    = "/untag/"
	pathLegalHolds               = "/legal-holds"
	pathAuditFrameworks          = "/audit/frameworks"
	pathAuditReportPlans         = "/audit/report-plans"
	pathAuditBackupJobSummaries  = "/audit/backup-job-summaries"
	pathAuditCopyJobSummaries    = "/audit/copy-job-summaries"
	pathAuditRestoreJobSummaries = "/audit/restore-job-summaries"
	pathAuditScanJobSummaries    = "/audit/scan-job-summaries"
	pathLogicallyAirGapped       = "/logically-air-gapped-backup-vaults"
	pathRestoreAccessVaults      = "/restore-access-backup-vaults"
	pathRestoreTestingPlans      = "/restore-testing/plans"
	pathGlobalSettings           = "/global-settings"
	// pathRegionSettings is AWS's actual wire path for region-settings
	// operations -- the API confusingly binds DescribeRegionSettings /
	// UpdateRegionSettings to /account-settings, not /region-settings.
	pathRegionSettings             = "/account-settings"
	pathSupportedTypes             = "/supported-resource-types"
	pathResources                  = "/resources"
	pathRestoreJobs                = "/restore-jobs"
	pathReportJobs                 = "/audit/report-jobs"
	pathScanJobs                   = "/scan/jobs"
	pathScanJobStart               = "/scan/job"
	pathTieringConf                = "/tiering-configurations"
	pathIndexedRecovery            = "/indexes/recovery-point"
	pathRestoreTestingInferredMeta = "/restore-testing/inferred-metadata"
	pathPITRMalwareScanResults     = "/scan/pitr-malware-scan-results"

	// splitTwo is the N argument for [strings.SplitN] to split into at most 2 parts.
	splitTwo = 2

	// JSON field name constants used in multiple handlers.
	keyState                       = "State"
	keySelectionID                 = "SelectionId"
	keyFrameworkArn                = "FrameworkArn"
	keyFrameworkName               = "FrameworkName"
	keyStatus                      = "Status"
	keyReportPlanArn               = "ReportPlanArn"
	keyReportPlanName              = "ReportPlanName"
	keyRestoreTestingPlanArn       = "RestoreTestingPlanArn"
	keyRestoreTestingPlanName      = "RestoreTestingPlanName"
	keyRestoreTestingSelectionName = "RestoreTestingSelectionName"
	keyRecoveryPointArn            = "RecoveryPointArn"
	keyBackupPlanName              = "BackupPlanName"
	keyRules                       = "Rules"
	keyRecoveryPoints              = "RecoveryPoints"
	keyCopyJobID                   = "CopyJobId"
	keyRestoreJobID                = "RestoreJobId"
	keyResourceArn                 = "ResourceArn"
	keyResourceType                = "ResourceType"
	keyLegalHoldID                 = "LegalHoldId"
	keyTitle                       = "Title"
	keyVaultState                  = "VaultState"
	keyReportJobID                 = "ReportJobId"
	keyScanJobID                   = "ScanJobId"
	keyTieringConfigurations       = "TieringConfigurations"

	// Status value constants.
	statusCompleted = "COMPLETED"
	statusCreated   = "CREATED"
	statusCreating  = "CREATING"
	statusActive    = "ACTIVE"
)

// The supportedOps* helpers partition the full set of operations returned by
// Handler.GetSupportedOperations by family, mirroring the section comments
// the flat list used to carry.

func supportedOpsCore() []string {
	return []string{
		opAssociateBackupVaultMpaApprovalTeam,
		opCancelLegalHold,
		opCreateBackupSelection,
		opCreateBackupVault,
		opCreateFramework,
		opCreateLegalHold,
		opCreateLogicallyAirGappedBackupVault,
		opCreateReportPlan,
		opCreateRestoreAccessBackupVault,
		opCreateRestoreTestingPlan,
		opCreateRestoreTestingSelection,
		opDescribeBackupVault,
		opListBackupVaults,
		opDeleteBackupVault,
		opCreateBackupPlan,
		opGetBackupPlan,
		opListBackupPlans,
		opUpdateBackupPlan,
		opDeleteBackupPlan,
		opStartBackupJob,
		opDescribeBackupJob,
		opListBackupJobs,
		opTagResource,
		opUntagResource,
		opListTags,
	}
}

func supportedOpsRecoveryPoints() []string {
	return []string{
		opListRecoveryPointsByBackupVault,
		opDescribeRecoveryPoint,
		opGetRecoveryPointRestoreMetadata,
		opDeleteRecoveryPoint,
		opDisassociateRecoveryPoint,
		opDisassociateRecoveryPointFromParent,
	}
}

func supportedOpsVaultCompliance() []string {
	return []string{
		opPutBackupVaultAccessPolicy,
		opGetBackupVaultAccessPolicy,
		opDeleteBackupVaultAccessPolicy,
		opPutBackupVaultLockConfiguration,
		opDeleteBackupVaultLockConfiguration,
		opPutBackupVaultNotifications,
		opGetBackupVaultNotifications,
		opDeleteBackupVaultNotifications,
	}
}

func supportedOpsSelections() []string {
	return []string{
		opGetBackupSelection,
		opListBackupSelections,
		opDeleteBackupSelection,
	}
}

func supportedOpsCopyJobs() []string {
	return []string{
		opListCopyJobs,
		opDescribeCopyJob,
	}
}

func supportedOpsRestoreTesting() []string {
	return []string{
		opGetRestoreTestingPlan,
		opListRestoreTestingPlans,
		opUpdateRestoreTestingPlan,
		opDeleteRestoreTestingPlan,
		opGetRestoreTestingSelection,
		opListRestoreTestingSelections,
		opUpdateRestoreTestingSelection,
		opDeleteRestoreTestingSelection,
	}
}

func supportedOpsFrameworks() []string {
	return []string{
		opDescribeFramework,
		opListFrameworks,
		opUpdateFramework,
		opDeleteFramework,
	}
}

func supportedOpsReportPlans() []string {
	return []string{
		opListReportPlans,
		opDescribeReportPlan,
		opUpdateReportPlan,
		opDeleteReportPlan,
	}
}

func supportedOpsExtended() []string {
	return []string{
		opCreateTieringConfiguration,
		opDeleteTieringConfiguration,
		opDescribeGlobalSettings,
		opDescribeProtectedResource,
		opDescribeRegionSettings,
		opDescribeReportJob,
		opDescribeRestoreJob,
		opDescribeScanJob,
		opDisassociateBackupVaultMpaApprovalTeam,
		opExportBackupPlanTemplate,
		opGetBackupPlanFromJSON,
		opGetBackupPlanFromTemplate,
		opGetLegalHold,
		opGetPITRMalwareScanResults,
		opGetRecoveryPointIndexDetails,
		opGetRestoreJobMetadata,
		opGetRestoreTestingInferredMetadata,
		opGetSupportedResourceTypes,
		opGetTieringConfiguration,
		opListBackupJobSummaries,
		opListBackupPlanTemplates,
		opListBackupPlanVersions,
		opListCopyJobSummaries,
		opListIndexedRecoveryPoints,
		opListLegalHolds,
		opListProtectedResources,
		opListProtectedResourcesByBackupVault,
		opListRecoveryPointsByLegalHold,
		opListRecoveryPointsByResource,
		opListReportJobs,
		opListRestoreAccessBackupVaults,
		opListRestoreJobSummaries,
		opListRestoreJobs,
		opListRestoreJobsByProtectedResource,
		opListScanJobSummaries,
		opListScanJobs,
		opListTieringConfigurations,
		opPutRestoreValidationResult,
		opRevokeRestoreAccessBackupVault,
		opStartCopyJob,
		opStartReportJob,
		opStartRestoreJob,
		opStartScanJob,
		opStopBackupJob,
		opUpdateGlobalSettings,
		opUpdateRecoveryPointIndexSettings,
		opUpdateRecoveryPointLifecycle,
		opUpdateRegionSettings,
		opUpdateTieringConfiguration,
	}
}
