package backup

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// matchesBackupPath returns true if the given path should be handled by the Backup handler.
func matchesBackupPath(path string) bool {
	prefixes := []string{
		pathBackupVaults + "/",
		pathBackupPlans + "/",
		pathBackupTemplate + "/",
		pathBackupJobs + "/",
		pathCopyJobs + "/",
		pathTags + "arn:aws:backup:",
		pathUntag,
		pathLegalHolds + "/",
		pathAuditFrameworks + "/",
		pathAuditReportPlans + "/",
		pathLogicallyAirGapped + "/",
		pathRestoreAccessVaults + "/",
		pathRestoreTestingPlans + "/",
		pathResources + "/",
		pathRestoreJobs + "/",
		pathReportJobs + "/",
		pathScanJobs + "/",
		pathTieringConf + "/",
	}

	exacts := []string{
		pathBackupVaults,
		pathBackupPlans,
		pathBackupJobs,
		pathCopyJobs,
		pathLegalHolds,
		pathAuditFrameworks,
		pathAuditReportPlans,
		pathLogicallyAirGapped,
		pathRestoreAccessVaults,
		pathRestoreTestingPlans,
		pathGlobalSettings,
		pathRegionSettings,
		pathSupportedTypes,
		pathResources,
		pathRestoreJobs,
		pathReportJobs,
		pathScanJobs,
		pathScanJobStart,
		pathTieringConf,
		pathIndexedRecovery,
		pathRestoreTestingInferredMeta,
		pathPITRMalwareScanResults,
	}

	if slices.Contains(exacts, path) {
		return true
	}

	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}

	return false
}

// backupRoute holds the parsed information from a Backup REST request path.
type backupRoute struct {
	resource  string // vault-name, plan-id, job-id, or resource-arn
	operation string
}

// parseBackupPath maps HTTP method + path to an operation name and resource ID.
func parseBackupPath(
	method, rawPath string,
) backupRoute {
	path, _ := url.PathUnescape(rawPath)

	switch {
	case strings.HasPrefix(path, pathBackupVaults):

		return parseVaultRoute(method, strings.TrimPrefix(path, pathBackupVaults))
	case strings.HasPrefix(path, pathBackupTemplate):

		return parseTemplateRoute(method, strings.TrimPrefix(path, pathBackupTemplate))
	case strings.HasPrefix(path, pathBackupPlans):

		return parsePlanRoute(method, strings.TrimPrefix(path, pathBackupPlans))
	case strings.HasPrefix(path, pathBackupJobs):

		return parseJobRoute(method, strings.TrimPrefix(path, pathBackupJobs))
	case strings.HasPrefix(path, pathCopyJobs):

		return parseCopyJobRoute(method, strings.TrimPrefix(path, pathCopyJobs))
	case strings.HasPrefix(path, pathUntag):
		if method == http.MethodPost {
			return backupRoute{
				operation: opUntagResource,
				resource:  strings.TrimPrefix(path, pathUntag),
			}
		}

		return backupRoute{operation: opUnknown}
	case strings.HasPrefix(path, pathTags):

		return parseTagsRoute(method, strings.TrimPrefix(path, pathTags))
	case strings.HasPrefix(path, pathLegalHolds):

		return parseLegalHoldRoute(method, strings.TrimPrefix(path, pathLegalHolds))
	case strings.HasPrefix(path, pathAuditFrameworks):

		return parseFrameworkRoute(method, strings.TrimPrefix(path, pathAuditFrameworks))
	case strings.HasPrefix(path, pathAuditReportPlans):

		return parseReportPlanRoute(method, strings.TrimPrefix(path, pathAuditReportPlans))
	case strings.HasPrefix(path, pathLogicallyAirGapped):

		return parseLogicallyAirGappedRoute(
			method,
			strings.TrimPrefix(path, pathLogicallyAirGapped),
		)
	case strings.HasPrefix(path, pathRestoreAccessVaults):

		return parseRestoreAccessVaultRoute(
			method,
			strings.TrimPrefix(path, pathRestoreAccessVaults),
		)
	case strings.HasPrefix(path, pathRestoreTestingPlans):

		return parseRestoreTestingRoute(method, strings.TrimPrefix(path, pathRestoreTestingPlans))
	}

	return parseBackupMiscPath(method, path)
}

// parseBackupMiscPath routes the remaining settings/jobs/resources path
// families that don't nest under a shared resource-collection prefix. Split
// out of parseBackupPath to keep both functions' cognitive complexity in check.
func parseBackupMiscPath(method, path string) backupRoute {
	if r := parseBackupSettingsPath(method, path); r.operation != opUnknown {
		return r
	}

	return parseBackupJobFamilyPath(method, path)
}

// parseBackupSettingsPath routes global/region settings, supported resource
// types, protected resources, and restore jobs.
func parseBackupSettingsPath(method, path string) backupRoute {
	switch {
	case path == pathRestoreTestingInferredMeta:
		if method == http.MethodGet {
			return backupRoute{operation: opGetRestoreTestingInferredMetadata}
		}
	case path == pathGlobalSettings:
		if method == http.MethodGet {
			return backupRoute{operation: opDescribeGlobalSettings}
		}

		return backupRoute{operation: opUpdateGlobalSettings}
	case path == pathRegionSettings:
		if method == http.MethodGet {
			return backupRoute{operation: opDescribeRegionSettings}
		}

		return backupRoute{operation: opUpdateRegionSettings}
	case path == pathSupportedTypes:

		return backupRoute{operation: opGetSupportedResourceTypes}
	case path == pathResources:

		return backupRoute{operation: opListProtectedResources}
	case strings.HasPrefix(path, pathResources+"/"):

		return parseResourceRoute(method, strings.TrimPrefix(path, pathResources+"/"))
	case path == pathRestoreJobs:
		if method == http.MethodGet {
			return backupRoute{operation: opListRestoreJobs}
		}

		return backupRoute{operation: opStartRestoreJob}
	case strings.HasPrefix(path, pathRestoreJobs+"/"):

		return parseRestoreJobSubRoute(strings.TrimPrefix(path, pathRestoreJobs+"/"))
	}

	return backupRoute{operation: opUnknown}
}

// parseBackupJobFamilyPath routes report jobs, scan jobs, indexed recovery
// points, and tiering configuration paths.
func parseBackupJobFamilyPath(method, path string) backupRoute {
	switch {
	case path == pathReportJobs:
		if method == http.MethodGet {
			return backupRoute{operation: opListReportJobs}
		}
	case strings.HasPrefix(path, pathReportJobs+"/"):

		return parseReportJobRoute(method, strings.TrimPrefix(path, pathReportJobs+"/"))
	case path == pathScanJobStart:
		if method == http.MethodPut {
			return backupRoute{operation: opStartScanJob}
		}
	case path == pathScanJobs:
		if method == http.MethodGet {
			return backupRoute{operation: opListScanJobs}
		}
	case strings.HasPrefix(path, pathScanJobs+"/"):
		if method == http.MethodGet {
			return backupRoute{
				operation: opDescribeScanJob,
				resource:  strings.TrimPrefix(path, pathScanJobs+"/"),
			}
		}
	case path == pathPITRMalwareScanResults:
		if method == http.MethodGet {
			return backupRoute{operation: opGetPITRMalwareScanResults}
		}
	case path == pathIndexedRecovery:
		if method == http.MethodGet {
			return backupRoute{operation: opListIndexedRecoveryPoints}
		}
	case strings.HasPrefix(path, pathTieringConf):

		return parseTieringRoute(method, strings.TrimPrefix(path, pathTieringConf))
	}

	return backupRoute{operation: opUnknown}
}

// parseRestoreJobSubRoute routes /restore-jobs/{id}[/metadata|/validations].
func parseRestoreJobSubRoute(suffix string) backupRoute {
	parts := strings.SplitN(suffix, "/", splitTwo)
	if len(parts) == splitTwo && parts[1] == "metadata" {
		return backupRoute{operation: opGetRestoreJobMetadata, resource: parts[0]}
	}
	if len(parts) == splitTwo && parts[1] == "validations" {
		return backupRoute{operation: opPutRestoreValidationResult, resource: parts[0]}
	}

	return backupRoute{operation: opDescribeRestoreJob, resource: parts[0]}
}

// parseResourceRoute routes /resources/{resourceArn}[/recovery-points|/restore-jobs].
// The resource ARN itself may contain slashes (e.g. "...:instance/i-0123"), so
// suffixes are matched from the end rather than by splitting on "/".
func parseResourceRoute(method, rest string) backupRoute {
	if arn, ok := strings.CutSuffix(rest, "/recovery-points"); ok {
		if method == http.MethodGet {
			return backupRoute{operation: opListRecoveryPointsByResource, resource: arn}
		}

		return backupRoute{operation: opUnknown}
	}

	if arn, ok := strings.CutSuffix(rest, "/restore-jobs"); ok {
		if method == http.MethodGet {
			return backupRoute{operation: opListRestoreJobsByProtectedResource, resource: arn}
		}

		return backupRoute{operation: opUnknown}
	}

	if method == http.MethodGet {
		return backupRoute{operation: opDescribeProtectedResource, resource: rest}
	}

	return backupRoute{operation: opUnknown}
}

// parseReportJobRoute routes /audit/report-jobs/{name}. AWS reuses the same
// path shape for both DescribeReportJob (name = ReportJobId, GET) and
// StartReportJob (name = ReportPlanName, POST).
func parseReportJobRoute(method, name string) backupRoute {
	switch method {
	case http.MethodGet:

		return backupRoute{operation: opDescribeReportJob, resource: name}
	case http.MethodPost:

		return backupRoute{operation: opStartReportJob, resource: name}
	}

	return backupRoute{operation: opUnknown}
}

// parseTieringRoute routes /tiering-configurations[/{TieringConfigurationName}].
// Real AWS: Create is PUT on the bare collection path (the configuration's
// name lives in the request body, not the URL); Get/Update/Delete address a
// specific configuration by name in the path (Update is also PUT, so it is
// the presence of a path suffix -- not the method -- that distinguishes
// Create from Update).
func parseTieringRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opListTieringConfigurations}
		case http.MethodPut:

			return backupRoute{operation: opCreateTieringConfiguration}
		}

		return backupRoute{operation: opUnknown}
	}
	switch method {
	case http.MethodGet:

		return backupRoute{operation: opGetTieringConfiguration, resource: name}
	case http.MethodPut:

		return backupRoute{operation: opUpdateTieringConfiguration, resource: name}
	case http.MethodDelete:

		return backupRoute{operation: opDeleteTieringConfiguration, resource: name}
	}

	return backupRoute{operation: opUnknown}
}

// vaultSubRoute tries to match a sub-resource suffix, returning the vault name and op suffix.
// Returns ("", "") if no recognized suffix is found.
func vaultSubRoute(name string) (string, string) {
	for _, sfx := range []string{
		"/mpaApprovalTeam",
		"/access-policy",
		"/vault-lock",
		"/notification-configuration",
	} {
		if v, ok := strings.CutSuffix(name, sfx); ok {
			return v, sfx
		}
	}

	return "", ""
}

func parseVaultRoute(method, suffix string) backupRoute {
	// suffix is either "" (collection) or "/{name}" or "/{name}/{subresource}"
	name := strings.TrimPrefix(suffix, "/")

	if name == "" {
		if method == http.MethodGet {
			return backupRoute{operation: opListBackupVaults}
		}

		return backupRoute{operation: opUnknown}
	}

	if strings.Contains(name, "/recovery-points") {
		return parseVaultRecoveryPointRoute(method, name)
	}

	vn, sub := vaultSubRoute(name)
	if sub != "" {
		return parseVaultSubResourceRoute(method, vn, sub)
	}

	if !strings.Contains(name, "/") {
		// /backup-vaults/{name}
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opCreateBackupVault, resource: name}
		case http.MethodGet:

			return backupRoute{operation: opDescribeBackupVault, resource: name}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteBackupVault, resource: name}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseVaultSubResourceRoute(method, vaultName, sub string) backupRoute {
	switch sub {
	case "/mpaApprovalTeam":
		switch method {
		case http.MethodPut:

			return backupRoute{
				operation: opAssociateBackupVaultMpaApprovalTeam,
				resource:  vaultName,
			}
		case http.MethodPost:
			// AWS uses POST .../mpaApprovalTeam?delete for the disassociate call
			// (same path as associate, distinguished by method + query string).
			return backupRoute{
				operation: opDisassociateBackupVaultMpaApprovalTeam,
				resource:  vaultName,
			}
		}
	case "/access-policy":
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opPutBackupVaultAccessPolicy, resource: vaultName}
		case http.MethodGet:

			return backupRoute{operation: opGetBackupVaultAccessPolicy, resource: vaultName}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteBackupVaultAccessPolicy, resource: vaultName}
		}
	case "/vault-lock":
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opPutBackupVaultLockConfiguration, resource: vaultName}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteBackupVaultLockConfiguration, resource: vaultName}
		}
	case "/notification-configuration":
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opPutBackupVaultNotifications, resource: vaultName}
		case http.MethodGet:

			return backupRoute{operation: opGetBackupVaultNotifications, resource: vaultName}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteBackupVaultNotifications, resource: vaultName}
		}
	}

	return backupRoute{operation: opUnknown}
}

// rpSubSuffix returns the recognized sub-resource suffixes for recovery points.
func rpSubSuffixes() []string {
	return []string{"/disassociate", "/parentAssociation", "/restore-metadata", "/index"}
}

// parseVaultRecoveryPointRoute handles /backup-vaults/{name}/recovery-points[/{arn}[/...]]
// The resource field is encoded as "vaultName|recoveryPointArn".
func parseVaultRecoveryPointRoute(method, name string) backupRoute {
	// name = "{vaultName}/recovery-points" or "{vaultName}/recovery-points/{arn}[/sub]"
	parts := strings.SplitN(name, "/recovery-points", splitTwo)
	vaultName := parts[0]
	rest := ""

	if len(parts) == splitTwo {
		rest = strings.TrimPrefix(parts[1], "/")
	}

	if rest == "" {
		if method == http.MethodGet {
			return backupRoute{operation: opListRecoveryPointsByBackupVault, resource: vaultName}
		}

		return backupRoute{operation: opUnknown}
	}

	// Check for known sub-resource suffixes.
	for _, sfx := range rpSubSuffixes() {
		if arn, ok := strings.CutSuffix(rest, sfx); ok {
			return parseRecoveryPointSubRoute(method, vaultName, arn, sfx)
		}
	}

	if !strings.Contains(rest, "/") {
		// /backup-vaults/{name}/recovery-points/{arn}
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opDescribeRecoveryPoint, resource: vaultName + "|" + rest}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteRecoveryPoint, resource: vaultName + "|" + rest}
		case http.MethodPost:

			return backupRoute{operation: opUpdateRecoveryPointLifecycle, resource: vaultName + "|" + rest}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseRecoveryPointSubRoute(method, vaultName, rpArn, sub string) backupRoute {
	res := vaultName + "|" + rpArn

	switch sub {
	case "/disassociate":
		if method == http.MethodPost {
			return backupRoute{operation: opDisassociateRecoveryPoint, resource: res}
		}
	case "/parentAssociation":
		if method == http.MethodDelete {
			return backupRoute{operation: opDisassociateRecoveryPointFromParent, resource: res}
		}
	case "/restore-metadata":
		if method == http.MethodGet {
			return backupRoute{operation: opGetRecoveryPointRestoreMetadata, resource: res}
		}
	case "/index":
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opGetRecoveryPointIndexDetails, resource: res}
		case http.MethodPost:

			return backupRoute{operation: opUpdateRecoveryPointIndexSettings, resource: res}
		}
	}

	return backupRoute{operation: opUnknown}
}

// parsePlanRoute routes backup plan and selection paths.
func parsePlanRoute(method, suffix string) backupRoute {
	// suffix is "" or "/{id}" or "/{id}/selections[/{selId}]"
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		// /backup/plans
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opCreateBackupPlan}
		case http.MethodGet:

			return backupRoute{operation: opListBackupPlans}
		}

		return backupRoute{operation: opUnknown}
	}

	if strings.Contains(id, "/") {
		// /backup/plans/{id}/selections[/{selId}]
		parts := strings.SplitN(id, "/", splitTwo)

		return parsePlanSelectionRoute(method, parts[0], parts[1])
	}

	// /backup/plans/{id}
	switch method {
	case http.MethodGet:

		return backupRoute{operation: opGetBackupPlan, resource: id}
	case http.MethodPost:

		return backupRoute{operation: opUpdateBackupPlan, resource: id}
	case http.MethodDelete:

		return backupRoute{operation: opDeleteBackupPlan, resource: id}
	}

	return backupRoute{operation: opUnknown}
}

// parsePlanSelectionRoute routes backup plan selection sub-paths.
func parsePlanSelectionRoute(method, planID, rest string) backupRoute {
	if rest == "versions" && method == http.MethodGet {
		return backupRoute{operation: opListBackupPlanVersions, resource: planID}
	}

	if rest == "toTemplate" && method == http.MethodGet {
		return backupRoute{operation: opExportBackupPlanTemplate, resource: planID}
	}

	if rest == "selections" {
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opCreateBackupSelection, resource: planID}
		case http.MethodGet:

			return backupRoute{operation: opListBackupSelections, resource: planID}
		}

		return backupRoute{operation: opUnknown}
	}

	if selID, ok := strings.CutPrefix(rest, "selections/"); ok {
		if !strings.Contains(selID, "/") {
			switch method {
			case http.MethodGet:

				return backupRoute{operation: opGetBackupSelection, resource: planID + "|" + selID}
			case http.MethodDelete:

				return backupRoute{
					operation: opDeleteBackupSelection,
					resource:  planID + "|" + selID,
				}
			}
		}
	}

	return backupRoute{operation: opUnknown}
}

// parseTemplateRoute routes backup plan template paths under /backup/template.
func parseTemplateRoute(method, suffix string) backupRoute {
	suffix = strings.TrimPrefix(suffix, "/")

	if suffix == "json/toPlan" && method == http.MethodPost {
		return backupRoute{operation: opGetBackupPlanFromJSON}
	}

	if suffix == "plans" && method == http.MethodGet {
		return backupRoute{operation: opListBackupPlanTemplates}
	}

	if rest, ok := strings.CutPrefix(suffix, "plans/"); ok {
		if id, cut := strings.CutSuffix(rest, "/toPlan"); cut && method == http.MethodGet {
			return backupRoute{operation: opGetBackupPlanFromTemplate, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseJobRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /backup-jobs
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opStartBackupJob}
		case http.MethodGet:

			return backupRoute{operation: opListBackupJobs}
		}
	} else if !strings.Contains(id, "/") {
		// /backup-jobs/{id}: GET describes, POST stops (AWS reuses the same
		// path for both -- there is no "/stop-backup-job" suffix on the wire).
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opDescribeBackupJob, resource: id}
		case http.MethodPost:

			return backupRoute{operation: opStopBackupJob, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

// parseTagsRoute routes /tags/{resourceArn}. AWS Backup's UntagResource lives
// on a separate /untag/{resourceArn} path (see pathUntag), not DELETE here.
func parseTagsRoute(method, resourceArn string) backupRoute {
	switch method {
	case http.MethodPost:

		return backupRoute{operation: opTagResource, resource: resourceArn}
	case http.MethodGet:

		return backupRoute{operation: opListTags, resource: resourceArn}
	}

	return backupRoute{operation: opUnknown}
}

func parseLegalHoldRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /legal-holds
		switch method {
		case http.MethodPost:

			return backupRoute{operation: opCreateLegalHold}
		case http.MethodGet:

			return backupRoute{operation: opListLegalHolds}
		}

		return backupRoute{operation: opUnknown}
	}

	if rpID, ok := strings.CutSuffix(id, "/recovery-points"); ok {
		if method == http.MethodGet {
			return backupRoute{operation: opListRecoveryPointsByLegalHold, resource: rpID}
		}

		return backupRoute{operation: opUnknown}
	}

	if !strings.Contains(id, "/") {
		// /legal-holds/{id}
		switch method {
		case http.MethodDelete:

			return backupRoute{operation: opCancelLegalHold, resource: id}
		case http.MethodGet:

			return backupRoute{operation: opGetLegalHold, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseCopyJobRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		switch method {
		case http.MethodPut:
			return backupRoute{operation: opStartCopyJob}
		case http.MethodGet:
			return backupRoute{operation: opListCopyJobs}
		}
	} else if !strings.Contains(id, "/") {
		if method == http.MethodGet {
			return backupRoute{operation: opDescribeCopyJob, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseFrameworkRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		// /audit/frameworks
		switch method {
		case http.MethodPost:

			return backupRoute{operation: opCreateFramework}
		case http.MethodGet:

			return backupRoute{operation: opListFrameworks}
		}
	} else if !strings.Contains(name, "/") {
		// /audit/frameworks/{name}
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opDescribeFramework, resource: name}
		case http.MethodPut:

			return backupRoute{operation: opUpdateFramework, resource: name}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteFramework, resource: name}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseReportPlanRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		// /audit/report-plans
		switch method {
		case http.MethodPost:

			return backupRoute{operation: opCreateReportPlan}
		case http.MethodGet:

			return backupRoute{operation: opListReportPlans}
		}
	} else if !strings.Contains(name, "/") {
		// /audit/report-plans/{name}
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opDescribeReportPlan, resource: name}
		case http.MethodPut:

			return backupRoute{operation: opUpdateReportPlan, resource: name}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteReportPlan, resource: name}
		}
	}

	return backupRoute{operation: opUnknown}
}

// parseLogicallyAirGappedRoute routes /logically-air-gapped-backup-vaults/{name}
// (CreateLogicallyAirGappedBackupVault) and the restore-access-vault ops
// nested under it. Real AWS addresses ListRestoreAccessBackupVaults and
// RevokeRestoreAccessBackupVault as sub-resources of the source air-gapped
// vault -- GET/DELETE
// /logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults[/{arn}] --
// not under the flat /restore-access-backup-vaults collection (which is only
// used by Create).
func parseLogicallyAirGappedRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		return backupRoute{operation: opUnknown}
	}

	if vaultName, rest, ok := strings.Cut(name, "/restore-access-backup-vaults"); ok {
		return parseRestoreAccessVaultSubRoute(method, vaultName, strings.TrimPrefix(rest, "/"))
	}

	if !strings.Contains(name, "/") && method == http.MethodPut {
		return backupRoute{operation: opCreateLogicallyAirGappedBackupVault, resource: name}
	}

	return backupRoute{operation: opUnknown}
}

// parseRestoreAccessVaultSubRoute routes
// /logically-air-gapped-backup-vaults/{vaultName}/restore-access-backup-vaults[/{arn}].
// The resource field for Revoke is encoded as "vaultName|restoreAccessVaultArn".
func parseRestoreAccessVaultSubRoute(method, vaultName, arnSuffix string) backupRoute {
	if arnSuffix == "" {
		if method == http.MethodGet {
			return backupRoute{operation: opListRestoreAccessBackupVaults, resource: vaultName}
		}

		return backupRoute{operation: opUnknown}
	}

	if method == http.MethodDelete {
		return backupRoute{
			operation: opRevokeRestoreAccessBackupVault,
			resource:  vaultName + "|" + arnSuffix,
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseRestoreAccessVaultRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /restore-access-backup-vaults (Create only -- List/Revoke are nested
		// under /logically-air-gapped-backup-vaults/{name}/..., see
		// parseRestoreAccessVaultSubRoute).
		if method == http.MethodPut {
			return backupRoute{operation: opCreateRestoreAccessBackupVault}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseRestoreTestingRoute(method, suffix string) backupRoute {
	// suffix is "" or "/{planName}" or "/{planName}/selections[/{selName}]"
	rest := strings.TrimPrefix(suffix, "/")

	switch {
	case rest == "":
		// /restore-testing/plans
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opCreateRestoreTestingPlan}
		case http.MethodGet:

			return backupRoute{operation: opListRestoreTestingPlans}
		}
	case strings.Contains(rest, "/"):

		return parseRestoreTestingSubRoute(method, rest)
	default:
		// /restore-testing/plans/{planName}
		switch method {
		case http.MethodGet:

			return backupRoute{operation: opGetRestoreTestingPlan, resource: rest}
		case http.MethodPut:

			return backupRoute{operation: opUpdateRestoreTestingPlan, resource: rest}
		case http.MethodDelete:

			return backupRoute{operation: opDeleteRestoreTestingPlan, resource: rest}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseRestoreTestingSubRoute(method, rest string) backupRoute {
	parts := strings.SplitN(rest, "/", splitTwo)
	planName := parts[0]
	sub := parts[1]

	switch {
	case sub == "selections":
		switch method {
		case http.MethodPut:

			return backupRoute{operation: opCreateRestoreTestingSelection, resource: planName}
		case http.MethodGet:

			return backupRoute{operation: opListRestoreTestingSelections, resource: planName}
		}
	case strings.HasPrefix(sub, "selections/"):
		selName := strings.TrimPrefix(sub, "selections/")
		if !strings.Contains(selName, "/") {
			switch method {
			case http.MethodGet:

				return backupRoute{
					operation: opGetRestoreTestingSelection,
					resource:  planName + "|" + selName,
				}
			case http.MethodPut:

				return backupRoute{
					operation: opUpdateRestoreTestingSelection,
					resource:  planName + "|" + selName,
				}
			case http.MethodDelete:

				return backupRoute{
					operation: opDeleteRestoreTestingSelection,
					resource:  planName + "|" + selName,
				}
			}
		}
	}

	return backupRoute{operation: opUnknown}
}
