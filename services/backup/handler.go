package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

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
)

const (
	backupMatchPriority = service.PriorityPathVersioned

	pathBackupVaults        = "/backup-vaults"
	pathBackupPlans         = "/backup/plans"
	pathBackupJobs          = "/backup-jobs"
	pathCopyJobs            = "/copy-jobs"
	pathTags                = "/tags/"
	pathLegalHolds          = "/legal-holds"
	pathAuditFrameworks     = "/audit/frameworks"
	pathAuditReportPlans    = "/audit/report-plans"
	pathLogicallyAirGapped  = "/logically-air-gapped-backup-vaults"
	pathRestoreAccessVaults = "/restore-access-backup-vaults"
	pathRestoreTestingPlans = "/restore-testing/plans"

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
)

var (
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Backup operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
	janitor *Janitor
}

// NewHandler creates a new Backup handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(
	interval, jobTTL time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
	j := NewJanitor(h.Backend, interval, jobTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "Backup" }

// GetSupportedOperations returns the list of supported Backup operations.
func (h *Handler) GetSupportedOperations() []string {
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
		// Recovery points.
		opListRecoveryPointsByBackupVault,
		opDescribeRecoveryPoint,
		opGetRecoveryPointRestoreMetadata,
		opDeleteRecoveryPoint,
		opDisassociateRecoveryPoint,
		opDisassociateRecoveryPointFromParent,
		// Vault compliance.
		opPutBackupVaultAccessPolicy,
		opGetBackupVaultAccessPolicy,
		opDeleteBackupVaultAccessPolicy,
		opPutBackupVaultLockConfiguration,
		opDeleteBackupVaultLockConfiguration,
		opPutBackupVaultNotifications,
		opGetBackupVaultNotifications,
		opDeleteBackupVaultNotifications,
		// Backup selections.
		opGetBackupSelection,
		opListBackupSelections,
		opDeleteBackupSelection,
		// Copy jobs.
		opListCopyJobs,
		opDescribeCopyJob,
		// Restore testing.
		opGetRestoreTestingPlan,
		opListRestoreTestingPlans,
		opUpdateRestoreTestingPlan,
		opDeleteRestoreTestingPlan,
		opGetRestoreTestingSelection,
		opListRestoreTestingSelections,
		opUpdateRestoreTestingSelection,
		opDeleteRestoreTestingSelection,
		// Frameworks.
		opDescribeFramework,
		opListFrameworks,
		opUpdateFramework,
		opDeleteFramework,
		// Report plans.
		opListReportPlans,
		opDescribeReportPlan,
		opUpdateReportPlan,
		opDeleteReportPlan,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "backup" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Backup instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS Backup REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return matchesBackupPath(c.Request().URL.Path)
	}
}

// matchesBackupPath returns true if the given path should be handled by the Backup handler.
func matchesBackupPath(path string) bool {
	prefixes := []string{
		pathBackupVaults + "/",
		pathBackupPlans + "/",
		pathBackupJobs + "/",
		pathCopyJobs + "/",
		pathTags + "arn:aws:backup:",
		pathLegalHolds + "/",
		pathAuditFrameworks + "/",
		pathAuditReportPlans + "/",
		pathLogicallyAirGapped + "/",
		pathRestoreAccessVaults + "/",
		pathRestoreTestingPlans + "/",
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

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return backupMatchPriority }

// backupRoute holds the parsed information from a Backup REST request path.
type backupRoute struct {
	resource  string // vault-name, plan-id, job-id, or resource-arn
	operation string
}

// parseBackupPath maps HTTP method + path to an operation name and resource ID.
func parseBackupPath(method, rawPath string) backupRoute {
	path, _ := url.PathUnescape(rawPath)

	switch {
	case strings.HasPrefix(path, pathBackupVaults):
		return parseVaultRoute(method, strings.TrimPrefix(path, pathBackupVaults))
	case strings.HasPrefix(path, pathBackupPlans):
		return parsePlanRoute(method, strings.TrimPrefix(path, pathBackupPlans))
	case strings.HasPrefix(path, pathBackupJobs):
		return parseJobRoute(method, strings.TrimPrefix(path, pathBackupJobs))
	case strings.HasPrefix(path, pathCopyJobs):
		return parseCopyJobRoute(method, strings.TrimPrefix(path, pathCopyJobs))
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
		if method == http.MethodPut {
			return backupRoute{operation: opAssociateBackupVaultMpaApprovalTeam, resource: vaultName}
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
	return []string{"/disassociate", "/parentAssociation", "/restore-metadata"}
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
	}

	return backupRoute{operation: opUnknown}
}

//nolint:cyclop // plan sub-resources require branching
func parsePlanRoute(method, suffix string) backupRoute {
	// suffix is "" or "/{id}" or "/{id}/selections[/{selId}]"
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		// /backup/plans
		switch method {
		case http.MethodPut:
			return backupRoute{operation: opCreateBackupPlan}
		case http.MethodGet:
			return backupRoute{operation: opListBackupPlans}
		}
	case strings.Contains(id, "/"):
		// /backup/plans/{id}/selections[/{selId}]
		parts := strings.SplitN(id, "/", splitTwo)
		planID := parts[0]
		rest := parts[1]

		switch {
		case rest == "selections":
			switch method {
			case http.MethodPut:
				return backupRoute{operation: opCreateBackupSelection, resource: planID}
			case http.MethodGet:
				return backupRoute{operation: opListBackupSelections, resource: planID}
			}
		case strings.HasPrefix(rest, "selections/"):
			selID := strings.TrimPrefix(rest, "selections/")
			if !strings.Contains(selID, "/") {
				switch method {
				case http.MethodGet:
					return backupRoute{
						operation: opGetBackupSelection,
						resource:  planID + "|" + selID,
					}
				case http.MethodDelete:
					return backupRoute{
						operation: opDeleteBackupSelection,
						resource:  planID + "|" + selID,
					}
				}
			}
		}
	default:
		// /backup/plans/{id}
		switch method {
		case http.MethodGet:
			return backupRoute{operation: opGetBackupPlan, resource: id}
		case http.MethodPost:
			return backupRoute{operation: opUpdateBackupPlan, resource: id}
		case http.MethodDelete:
			return backupRoute{operation: opDeleteBackupPlan, resource: id}
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
		// /backup-jobs/{id}
		if method == http.MethodGet {
			return backupRoute{operation: opDescribeBackupJob, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseTagsRoute(method, resourceArn string) backupRoute {
	switch method {
	case http.MethodPost:
		return backupRoute{operation: opTagResource, resource: resourceArn}
	case http.MethodGet:
		return backupRoute{operation: opListTags, resource: resourceArn}
	case http.MethodDelete:
		return backupRoute{operation: opUntagResource, resource: resourceArn}
	}

	return backupRoute{operation: opUnknown}
}

func parseLegalHoldRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /legal-holds
		if method == http.MethodPost {
			return backupRoute{operation: opCreateLegalHold}
		}
	} else if !strings.Contains(id, "/") {
		// /legal-holds/{id}
		if method == http.MethodDelete {
			return backupRoute{operation: opCancelLegalHold, resource: id}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseCopyJobRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		if method == http.MethodGet {
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

func parseLogicallyAirGappedRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name != "" && !strings.Contains(name, "/") {
		// /logically-air-gapped-backup-vaults/{name}
		if method == http.MethodPut {
			return backupRoute{operation: opCreateLogicallyAirGappedBackupVault, resource: name}
		}
	}

	return backupRoute{operation: opUnknown}
}

func parseRestoreAccessVaultRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /restore-access-backup-vaults
		if method == http.MethodPost {
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
				return backupRoute{operation: opGetRestoreTestingSelection, resource: planName + "|" + selName}
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

// ExtractOperation extracts the Backup operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseBackupPath(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := parseBackupPath(c.Request().Method, c.Request().URL.Path)

	return r.resource
}

// Handler returns the Echo handler function for Backup requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseBackupPath(c.Request().Method, c.Request().URL.Path)

		log.Debug("backup request", "operation", route.operation, "resource", route.resource)

		var body []byte
		if c.Request().Body != nil {
			decoder := json.NewDecoder(c.Request().Body)
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err == nil {
				body = raw
			}
		}

		return h.dispatch(c, route, body)
	}
}

//nolint:cyclop // dispatch table for 24 REST operations is inherently wide
func (h *Handler) dispatch(c *echo.Context, route backupRoute, body []byte) error {
	if ok, result := h.dispatchNewOps(c, route, body); ok {
		return result
	}

	switch route.operation {
	case opCreateBackupVault:
		return h.handleCreateBackupVault(c, route.resource, body)
	case opDescribeBackupVault:
		return h.handleDescribeBackupVault(c, route.resource)
	case opListBackupVaults:
		return h.handleListBackupVaults(c)
	case opDeleteBackupVault:
		return h.handleDeleteBackupVault(c, route.resource)
	case opCreateBackupPlan:
		return h.handleCreateBackupPlan(c, body)
	case opGetBackupPlan:
		return h.handleGetBackupPlan(c, route.resource)
	case opListBackupPlans:
		return h.handleListBackupPlans(c)
	case opUpdateBackupPlan:
		return h.handleUpdateBackupPlan(c, route.resource, body)
	case opDeleteBackupPlan:
		return h.handleDeleteBackupPlan(c, route.resource)
	case opStartBackupJob:
		return h.handleStartBackupJob(c, body)
	case opDescribeBackupJob:
		return h.handleDescribeBackupJob(c, route.resource)
	case opListBackupJobs:
		return h.handleListBackupJobs(c)
	case opTagResource:
		return h.handleTagResource(c, route.resource, body)
	case opUntagResource:
		return h.handleUntagResource(c, route.resource, body)
	case opListTags:
		return h.handleListTags(c, route.resource)
	default:
		return c.JSON(
			http.StatusNotFound,
			errResp("ResourceNotFoundException", "unknown operation: "+route.operation),
		)
	}
}

// dispatchNewOps dispatches additional Backup operations beyond the original set.
// It delegates to domain-specific sub-dispatchers. Returns (true, result) if handled.
func (h *Handler) dispatchNewOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	if ok, result := h.dispatchCreateOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchRecoveryPointOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchVaultComplianceOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchSelectionOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchCopyJobOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchRestoreTestingOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchFrameworkOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchReportPlanOps(c, route, body); ok {
		return true, result
	}

	return false, nil
}

func (h *Handler) dispatchCreateOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case opAssociateBackupVaultMpaApprovalTeam:
		return true, h.handleAssociateBackupVaultMpaApprovalTeam(c, route.resource, body)
	case opCancelLegalHold:
		return true, h.handleCancelLegalHold(c, route.resource)
	case opCreateBackupSelection:
		return true, h.handleCreateBackupSelection(c, route.resource, body)
	case opCreateFramework:
		return true, h.handleCreateFramework(c, body)
	case opCreateLegalHold:
		return true, h.handleCreateLegalHold(c, body)
	case opCreateLogicallyAirGappedBackupVault:
		return true, h.handleCreateLogicallyAirGappedBackupVault(c, route.resource, body)
	case opCreateReportPlan:
		return true, h.handleCreateReportPlan(c, body)
	case opCreateRestoreAccessBackupVault:
		return true, h.handleCreateRestoreAccessBackupVault(c, body)
	case opCreateRestoreTestingPlan:
		return true, h.handleCreateRestoreTestingPlan(c, body)
	case opCreateRestoreTestingSelection:
		return true, h.handleCreateRestoreTestingSelection(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) dispatchRecoveryPointOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opListRecoveryPointsByBackupVault:
		return true, h.handleListRecoveryPointsByBackupVault(c, route.resource)
	case opDescribeRecoveryPoint:
		return true, h.handleDescribeRecoveryPoint(c, route.resource)
	case opGetRecoveryPointRestoreMetadata:
		return true, h.handleGetRecoveryPointRestoreMetadata(c, route.resource)
	case opDeleteRecoveryPoint:
		return true, h.handleDeleteRecoveryPoint(c, route.resource)
	case opDisassociateRecoveryPoint:
		return true, h.handleDisassociateRecoveryPoint(c, route.resource)
	case opDisassociateRecoveryPointFromParent:
		return true, h.handleDisassociateRecoveryPointFromParent(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchVaultComplianceOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case opPutBackupVaultAccessPolicy:
		return true, h.handlePutBackupVaultAccessPolicy(c, route.resource, body)
	case opGetBackupVaultAccessPolicy:
		return true, h.handleGetBackupVaultAccessPolicy(c, route.resource)
	case opDeleteBackupVaultAccessPolicy:
		return true, h.handleDeleteBackupVaultAccessPolicy(c, route.resource)
	case opPutBackupVaultLockConfiguration:
		return true, h.handlePutBackupVaultLockConfiguration(c, route.resource, body)
	case opDeleteBackupVaultLockConfiguration:
		return true, h.handleDeleteBackupVaultLockConfiguration(c, route.resource)
	case opPutBackupVaultNotifications:
		return true, h.handlePutBackupVaultNotifications(c, route.resource, body)
	case opGetBackupVaultNotifications:
		return true, h.handleGetBackupVaultNotifications(c, route.resource)
	case opDeleteBackupVaultNotifications:
		return true, h.handleDeleteBackupVaultNotifications(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchSelectionOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opGetBackupSelection:
		return true, h.handleGetBackupSelection(c, route.resource)
	case opListBackupSelections:
		return true, h.handleListBackupSelections(c, route.resource)
	case opDeleteBackupSelection:
		return true, h.handleDeleteBackupSelection(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchCopyJobOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opListCopyJobs:
		return true, h.handleListCopyJobs(c)
	case opDescribeCopyJob:
		return true, h.handleDescribeCopyJob(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchRestoreTestingOps(
	c *echo.Context, route backupRoute, body []byte,
) (bool, error) {
	switch route.operation {
	case opGetRestoreTestingPlan:
		return true, h.handleGetRestoreTestingPlan(c, route.resource)
	case opListRestoreTestingPlans:
		return true, h.handleListRestoreTestingPlans(c)
	case opUpdateRestoreTestingPlan:
		return true, h.handleUpdateRestoreTestingPlan(c, route.resource, body)
	case opDeleteRestoreTestingPlan:
		return true, h.handleDeleteRestoreTestingPlan(c, route.resource)
	case opGetRestoreTestingSelection:
		return true, h.handleGetRestoreTestingSelection(c, route.resource)
	case opListRestoreTestingSelections:
		return true, h.handleListRestoreTestingSelections(c, route.resource)
	case opUpdateRestoreTestingSelection:
		return true, h.handleUpdateRestoreTestingSelection(c, route.resource, body)
	case opDeleteRestoreTestingSelection:
		return true, h.handleDeleteRestoreTestingSelection(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchFrameworkOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDescribeFramework:
		return true, h.handleDescribeFramework(c, route.resource)
	case opListFrameworks:
		return true, h.handleListFrameworks(c)
	case opUpdateFramework:
		return true, h.handleUpdateFramework(c, route.resource, body)
	case opDeleteFramework:
		return true, h.handleDeleteFramework(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchReportPlanOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case opListReportPlans:
		return true, h.handleListReportPlans(c)
	case opDescribeReportPlan:
		return true, h.handleDescribeReportPlan(c, route.resource)
	case opUpdateReportPlan:
		return true, h.handleUpdateReportPlan(c, route.resource, body)
	case opDeleteReportPlan:
		return true, h.handleDeleteReportPlan(c, route.resource)
	}

	return false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("AlreadyExistsException", err.Error()))
	case errors.Is(err, ErrValidation), errors.Is(err, errInvalidRequest):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"code": code, "message": msg}
}

// epochSeconds returns the Unix epoch timestamp as a float64 for JSON serialization.
// The AWS Backup SDK deserializes timestamps as JSON numbers (epoch seconds).
func epochSeconds(ts interface{ Unix() int64 }) float64 {
	return float64(ts.Unix())
}

// --- Vault handlers ---

type createBackupVaultBody struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags"`
	EncryptionKeyArn string            `json:"EncryptionKeyArn"`
	CreatorRequestID string            `json:"CreatorRequestId"`
}

func (h *Handler) handleCreateBackupVault(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in createBackupVaultBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	v, err := h.Backend.CreateBackupVault(
		name,
		in.EncryptionKeyArn,
		in.CreatorRequestID,
		in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  v.BackupVaultArn,
		keyBackupVaultName: v.BackupVaultName,
		keyCreationDate:    epochSeconds(v.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupVault(c *echo.Context, name string) error {
	v, err := h.Backend.DescribeBackupVault(name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyBackupVaultName:       v.BackupVaultName,
		keyBackupVaultArn:        v.BackupVaultArn,
		keyCreationDate:          epochSeconds(v.CreationTime),
		"NumberOfRecoveryPoints": v.NumberOfRecoveryPoints,
	}
	if v.EncryptionKeyArn != "" {
		resp["EncryptionKeyArn"] = v.EncryptionKeyArn
	}
	if v.Tags != nil {
		if t := v.Tags.Clone(); len(t) > 0 {
			resp["Tags"] = t
		}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupVaults(c *echo.Context) error {
	vaults := h.Backend.ListBackupVaults()
	items := make([]map[string]any, 0, len(vaults))

	for _, v := range vaults {
		items = append(items, map[string]any{
			keyBackupVaultName:       v.BackupVaultName,
			keyBackupVaultArn:        v.BackupVaultArn,
			keyCreationDate:          epochSeconds(v.CreationTime),
			"NumberOfRecoveryPoints": v.NumberOfRecoveryPoints,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupVaultList": items,
	})
}

func (h *Handler) handleDeleteBackupVault(c *echo.Context, name string) error {
	if err := h.Backend.DeleteBackupVault(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Plan handlers ---

type backupRuleJSON struct {
	RuleName                string `json:"RuleName"`
	TargetBackupVaultName   string `json:"TargetBackupVaultName"`
	ScheduleExpression      string `json:"ScheduleExpression,omitempty"`
	StartWindowMinutes      int64  `json:"StartWindowMinutes,omitempty"`
	CompletionWindowMinutes int64  `json:"CompletionWindowMinutes,omitempty"`
}

type backupPlanBodyDoc struct {
	BackupPlanName string           `json:"BackupPlanName"`
	Rules          []backupRuleJSON `json:"Rules"`
}

type createBackupPlanBody struct {
	BackupPlanTags map[string]string `json:"BackupPlanTags"`
	BackupPlan     backupPlanBodyDoc `json:"BackupPlan"`
}

func rulesFromJSON(in []backupRuleJSON) []Rule {
	rules := make([]Rule, 0, len(in))
	for _, r := range in {
		rules = append(rules, Rule{
			RuleName:                r.RuleName,
			TargetVaultName:         r.TargetBackupVaultName,
			ScheduleExpression:      r.ScheduleExpression,
			StartWindowMinutes:      r.StartWindowMinutes,
			CompletionWindowMinutes: r.CompletionWindowMinutes,
		})
	}

	return rules
}

func rulesToJSON(rules []Rule) []backupRuleJSON {
	out := make([]backupRuleJSON, 0, len(rules))
	for _, r := range rules {
		out = append(out, backupRuleJSON{
			RuleName:                r.RuleName,
			TargetBackupVaultName:   r.TargetVaultName,
			ScheduleExpression:      r.ScheduleExpression,
			StartWindowMinutes:      r.StartWindowMinutes,
			CompletionWindowMinutes: r.CompletionWindowMinutes,
		})
	}

	return out
}

func (h *Handler) handleCreateBackupPlan(c *echo.Context, body []byte) error {
	var in createBackupPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.BackupPlan.BackupPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp(
				"ValidationException",
				fmt.Sprintf("%s: BackupPlanName is required", errInvalidRequest),
			),
		)
	}

	p, err := h.Backend.CreateBackupPlan(
		in.BackupPlan.BackupPlanName,
		rulesFromJSON(in.BackupPlan.Rules),
		in.BackupPlanTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		keyCreationDate:  epochSeconds(p.CreationTime),
	})
}

func (h *Handler) handleGetBackupPlan(c *echo.Context, id string) error {
	p, err := h.Backend.GetBackupPlan(id)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		keyCreationDate:  epochSeconds(p.CreationTime),
		"BackupPlan": map[string]any{
			"BackupPlanName": p.BackupPlanName,
			"Rules":          rulesToJSON(p.Rules),
		},
	}
	if p.Tags != nil {
		if t := p.Tags.Clone(); len(t) > 0 {
			resp["Tags"] = t
		}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupPlans(c *echo.Context) error {
	plans := h.Backend.ListBackupPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, p := range plans {
		items = append(items, map[string]any{
			"BackupPlanName": p.BackupPlanName,
			keyBackupPlanArn: p.BackupPlanArn,
			keyBackupPlanID:  p.BackupPlanID,
			keyVersionID:     p.VersionID,
			keyCreationDate:  epochSeconds(p.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPlansList": items,
	})
}

type updateBackupPlanBody struct {
	BackupPlan backupPlanBodyDoc `json:"BackupPlan"`
}

func (h *Handler) handleUpdateBackupPlan(c *echo.Context, id string, body []byte) error {
	var in updateBackupPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	p, err := h.Backend.UpdateBackupPlan(id, rulesFromJSON(in.BackupPlan.Rules))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
	})
}

func (h *Handler) handleDeleteBackupPlan(c *echo.Context, id string) error {
	p, err := h.Backend.GetBackupPlan(id)
	if err != nil {
		return h.handleError(c, err)
	}

	if delErr := h.Backend.DeleteBackupPlan(id); delErr != nil {
		return h.handleError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanArn: p.BackupPlanArn,
		keyBackupPlanID:  p.BackupPlanID,
		keyVersionID:     p.VersionID,
		"DeletionDate":   epochSeconds(time.Now()),
	})
}

// --- Job handlers ---

type startBackupJobBody struct {
	BackupVaultName string `json:"BackupVaultName"`
	ResourceArn     string `json:"ResourceArn"`
	IamRoleArn      string `json:"IamRoleArn"`
	ResourceType    string `json:"ResourceType"`
}

func (h *Handler) handleStartBackupJob(c *echo.Context, body []byte) error {
	var in startBackupJobBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.BackupVaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp(
				"ValidationException",
				fmt.Sprintf("%s: BackupVaultName is required", errInvalidRequest),
			),
		)
	}

	j, err := h.Backend.StartBackupJob(
		in.BackupVaultName,
		in.ResourceArn,
		in.IamRoleArn,
		in.ResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupJobID:    j.BackupJobID,
		keyBackupVaultArn: j.BackupVaultArn,
		keyCreationDate:   epochSeconds(j.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupJob(c *echo.Context, jobID string) error {
	j, err := h.Backend.DescribeBackupJob(jobID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyBackupJobID:     j.BackupJobID,
		keyBackupVaultName: j.BackupVaultName,
		keyBackupVaultArn:  j.BackupVaultArn,
		keyState:           j.State,
		keyCreationDate:    epochSeconds(j.CreationTime),
	}
	if j.ResourceArn != "" {
		resp["ResourceArn"] = j.ResourceArn
	}
	if j.ResourceType != "" {
		resp["ResourceType"] = j.ResourceType
	}
	if j.IAMRoleArn != "" {
		resp["IamRoleArn"] = j.IAMRoleArn
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupJobs(c *echo.Context) error {
	vaultFilter := c.Request().URL.Query().Get("backupVaultName")
	jobs := h.Backend.ListBackupJobs(vaultFilter)
	items := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, map[string]any{
			keyBackupJobID:     j.BackupJobID,
			keyBackupVaultName: j.BackupVaultName,
			keyBackupVaultArn:  j.BackupVaultArn,
			"ResourceArn":      j.ResourceArn,
			keyState:           j.State,
			keyCreationDate:    epochSeconds(j.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupJobs": items,
	})
}

// --- Tag handlers ---

type tagResourceBody struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.Tags == nil {
		in.Tags = make(map[string]string)
	}

	if err := h.Backend.TagResource(resourceArn, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTags(c *echo.Context, resourceArn string) error {
	t, err := h.Backend.ListTags(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Tags": t,
	})
}

type untagResourceBody struct {
	TagKeyList []string `json:"TagKeyList"`
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string, body []byte) error {
	if resourceArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ResourceArn is required"),
		)
	}

	var in untagResourceBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.UntagResource(resourceArn, in.TagKeyList); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- New operation handlers ---

type associateMpaApprovalTeamBody struct {
	MpaApprovalTeamArn string `json:"MpaApprovalTeamArn"`
	RequesterComment   string `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleAssociateBackupVaultMpaApprovalTeam(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in associateMpaApprovalTeamBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.AssociateBackupVaultMpaApprovalTeam(vaultName, in.MpaApprovalTeamArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCancelLegalHold(c *echo.Context, legalHoldID string) error {
	if legalHoldID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "LegalHoldId is required"),
		)
	}

	if err := h.Backend.CancelLegalHold(legalHoldID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type createBackupSelectionBody struct {
	BackupSelection  backupSelectionDoc `json:"BackupSelection"`
	CreatorRequestID string             `json:"CreatorRequestId,omitempty"`
}

type backupSelectionDoc struct {
	SelectionName string `json:"SelectionName"`
	IamRoleArn    string `json:"IamRoleArn,omitempty"`
}

func (h *Handler) handleCreateBackupSelection(c *echo.Context, planID string, body []byte) error {
	if planID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupPlanId is required"),
		)
	}

	var in createBackupSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	sel, err := h.Backend.CreateBackupSelection(
		planID,
		in.BackupSelection.SelectionName,
		in.BackupSelection.IamRoleArn,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanID: sel.BackupPlanID,
		keySelectionID:  sel.SelectionID,
		keyCreationDate: epochSeconds(sel.CreationTime),
	})
}

type createFrameworkBody struct {
	FrameworkName        string `json:"FrameworkName"`
	FrameworkDescription string `json:"FrameworkDescription,omitempty"`
	IdempotencyToken     string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateFramework(c *echo.Context, body []byte) error {
	var in createFrameworkBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.FrameworkName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	f, err := h.Backend.CreateFramework(in.FrameworkName, in.FrameworkDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFrameworkArn:  f.FrameworkArn,
		keyFrameworkName: f.FrameworkName,
	})
}

type createLegalHoldBody struct {
	Title            string `json:"Title"`
	Description      string `json:"Description"`
	IdempotencyToken string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateLegalHold(c *echo.Context, body []byte) error {
	var in createLegalHoldBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.Title == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "Title is required"))
	}

	if in.Description == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "Description is required"),
		)
	}

	lh, err := h.Backend.CreateLegalHold(in.Title, in.Description)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LegalHoldId":   lh.LegalHoldID,
		"LegalHoldArn":  lh.LegalHoldArn,
		"Title":         lh.Title,
		"Description":   lh.Description,
		keyStatus:       lh.Status,
		keyCreationDate: epochSeconds(lh.CreationDate),
	})
}

type createLogicallyAirGappedBody struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID string            `json:"CreatorRequestId,omitempty"`
	MaxRetentionDays int64             `json:"MaxRetentionDays"`
	MinRetentionDays int64             `json:"MinRetentionDays"`
}

func (h *Handler) handleCreateLogicallyAirGappedBackupVault(
	c *echo.Context,
	name string,
	body []byte,
) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in createLogicallyAirGappedBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	v, err := h.Backend.CreateLogicallyAirGappedBackupVault(
		name, in.CreatorRequestID, in.MinRetentionDays, in.MaxRetentionDays, in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  v.BackupVaultArn,
		keyBackupVaultName: v.BackupVaultName,
		keyCreationDate:    epochSeconds(v.CreationTime),
		"VaultState":       "CREATING",
	})
}

type createReportPlanBody struct {
	ReportPlanName        string `json:"ReportPlanName"`
	ReportPlanDescription string `json:"ReportPlanDescription,omitempty"`
	IdempotencyToken      string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateReportPlan(c *echo.Context, body []byte) error {
	var in createReportPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.ReportPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	rp, err := h.Backend.CreateReportPlan(in.ReportPlanName, in.ReportPlanDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
		keyCreationTime:   epochSeconds(rp.CreationTime),
	})
}

type createRestoreAccessVaultBody struct {
	SourceBackupVaultArn string            `json:"SourceBackupVaultArn"`
	BackupVaultName      string            `json:"BackupVaultName,omitempty"`
	BackupVaultTags      map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID     string            `json:"CreatorRequestId,omitempty"`
	RequesterComment     string            `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleCreateRestoreAccessBackupVault(c *echo.Context, body []byte) error {
	var in createRestoreAccessVaultBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.SourceBackupVaultArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "SourceBackupVaultArn is required"),
		)
	}

	rav, err := h.Backend.CreateRestoreAccessBackupVault(
		in.SourceBackupVaultArn, in.BackupVaultName, in.CreatorRequestID, in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreAccessBackupVaultArn":  rav.RestoreAccessBackupVaultArn,
		"RestoreAccessBackupVaultName": rav.RestoreAccessBackupVaultName,
		keyCreationDate:                epochSeconds(rav.CreationDate),
		"VaultState":                   rav.VaultState,
	})
}

type restoreTestingPlanDoc struct {
	RestoreTestingPlanName string `json:"RestoreTestingPlanName"`
	ScheduleExpression     string `json:"ScheduleExpression,omitempty"`
}

type createRestoreTestingPlanBody struct {
	RestoreTestingPlan restoreTestingPlanDoc `json:"RestoreTestingPlan"`
	CreatorRequestID   string                `json:"CreatorRequestId,omitempty"`
}

func (h *Handler) handleCreateRestoreTestingPlan(c *echo.Context, body []byte) error {
	var in createRestoreTestingPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.RestoreTestingPlan.RestoreTestingPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	rtp, err := h.Backend.CreateRestoreTestingPlan(
		in.RestoreTestingPlan.RestoreTestingPlanName,
		in.RestoreTestingPlan.ScheduleExpression,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	})
}

type restoreTestingSelectionDoc struct {
	RestoreTestingSelectionName string `json:"RestoreTestingSelectionName"`
	ProtectedResourceType       string `json:"ProtectedResourceType,omitempty"`
}

type createRestoreTestingSelectionBody struct {
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
	CreatorRequestID        string                     `json:"CreatorRequestId,omitempty"`
}

func (h *Handler) handleCreateRestoreTestingSelection(
	c *echo.Context,
	planName string,
	body []byte,
) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	var in createRestoreTestingSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.RestoreTestingSelection.RestoreTestingSelectionName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingSelectionName is required"),
		)
	}

	sel, err := h.Backend.CreateRestoreTestingSelection(
		planName,
		in.RestoreTestingSelection.RestoreTestingSelectionName,
		in.RestoreTestingSelection.ProtectedResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:       sel.RestoreTestingPlanArn,
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	})
}

// splitVaultRP splits a "vaultName|recoveryPointArn" resource string.
// Returns ("", "", false) if the resource is not in the expected format.
func splitVaultRP(resource string) (string, string, bool) {
	parts := strings.SplitN(resource, "|", splitTwo)
	if len(parts) != splitTwo || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}

// splitPlanSel splits a "planID|selectionID" resource string.
// Returns ("", "", false) if the resource is not in the expected format.
func splitPlanSel(resource string) (string, string, bool) {
	parts := strings.SplitN(resource, "|", splitTwo)
	if len(parts) != splitTwo || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}

// --- Recovery point handlers ---

func (h *Handler) handleListRecoveryPointsByBackupVault(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	pts, err := h.Backend.ListRecoveryPointsByBackupVault(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(pts))
	for _, rp := range pts {
		item := map[string]any{
			keyRecoveryPointArn: rp.RecoveryPointArn,
			"BackupVaultName":   rp.BackupVaultName,
			keyBackupVaultArn:   rp.BackupVaultArn,
			keyStatus:           rp.Status,
			keyCreationDate:     epochSeconds(rp.CreationDate),
		}
		if rp.ResourceArn != "" {
			item["ResourceArn"] = rp.ResourceArn
		}
		if rp.ResourceType != "" {
			item["ResourceType"] = rp.ResourceType
		}
		items = append(items, item)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RecoveryPoints": items,
	})
}

func (h *Handler) handleDescribeRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	rp, err := h.Backend.DescribeRecoveryPoint(vaultName, rpArn)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyRecoveryPointArn: rp.RecoveryPointArn,
		"BackupVaultName":   rp.BackupVaultName,
		keyBackupVaultArn:   rp.BackupVaultArn,
		keyStatus:           rp.Status,
		keyCreationDate:     epochSeconds(rp.CreationDate),
	}
	if rp.ResourceArn != "" {
		resp["ResourceArn"] = rp.ResourceArn
	}
	if rp.ResourceType != "" {
		resp["ResourceType"] = rp.ResourceType
	}
	if rp.IAMRoleArn != "" {
		resp["IamRoleArn"] = rp.IAMRoleArn
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetRecoveryPointRestoreMetadata(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	metadata, err := h.Backend.GetRecoveryPointRestoreMetadata(vaultName, rpArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:   "",
		keyRecoveryPointArn: rpArn,
		"RestoreMetadata":   metadata,
	})
}

func (h *Handler) handleDeleteRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteRecoveryPoint(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisassociateRecoveryPoint(c *echo.Context, resource string) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DisassociateRecoveryPoint(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisassociateRecoveryPointFromParent(
	c *echo.Context,
	resource string,
) error {
	vaultName, rpArn, ok := splitVaultRP(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DisassociateRecoveryPointFromParent(vaultName, rpArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Vault compliance handlers ---

type putVaultAccessPolicyBody struct {
	Policy string `json:"Policy"`
}

func (h *Handler) handlePutBackupVaultAccessPolicy(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in putVaultAccessPolicyBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	if err := h.Backend.PutBackupVaultAccessPolicy(vaultName, in.Policy); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBackupVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	pol, err := h.Backend.GetBackupVaultAccessPolicy(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:  "",
		keyBackupVaultName: vaultName,
		"Policy":           pol.Policy,
	})
}

func (h *Handler) handleDeleteBackupVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultAccessPolicy(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type putVaultLockConfigBody struct {
	MinRetentionDays  int64 `json:"MinRetentionDays,omitempty"`
	MaxRetentionDays  int64 `json:"MaxRetentionDays,omitempty"`
	ChangeableForDays int64 `json:"ChangeableForDays,omitempty"`
}

func (h *Handler) handlePutBackupVaultLockConfiguration(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in putVaultLockConfigBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	cfg := &VaultLockConfig{
		MinRetentionDays:  in.MinRetentionDays,
		MaxRetentionDays:  in.MaxRetentionDays,
		ChangeableForDays: in.ChangeableForDays,
	}

	if err := h.Backend.PutBackupVaultLockConfiguration(vaultName, cfg); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteBackupVaultLockConfiguration(
	c *echo.Context,
	vaultName string,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultLockConfiguration(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type putVaultNotificationsBody struct {
	SNSTopicArn       string   `json:"SNSTopicArn"`
	BackupVaultEvents []string `json:"BackupVaultEvents"`
}

func (h *Handler) handlePutBackupVaultNotifications(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	var in putVaultNotificationsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	cfg := &VaultNotificationConfig{
		SNSTopicArn:       in.SNSTopicArn,
		BackupVaultEvents: in.BackupVaultEvents,
	}

	if err := h.Backend.PutBackupVaultNotifications(vaultName, cfg); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBackupVaultNotifications(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	cfg, err := h.Backend.GetBackupVaultNotifications(vaultName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupVaultArn:   "",
		keyBackupVaultName:  vaultName,
		"SNSTopicArn":       cfg.SNSTopicArn,
		"BackupVaultEvents": cfg.BackupVaultEvents,
	})
}

func (h *Handler) handleDeleteBackupVaultNotifications(c *echo.Context, vaultName string) error {
	if vaultName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupVaultName is required"),
		)
	}

	if err := h.Backend.DeleteBackupVaultNotifications(vaultName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Backup selection read/delete handlers ---

func (h *Handler) handleGetBackupSelection(c *echo.Context, resource string) error {
	planID, selID, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	sel, err := h.Backend.GetBackupSelection(planID, selID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBackupPlanID: sel.BackupPlanID,
		keySelectionID:  sel.SelectionID,
		keyCreationDate: epochSeconds(sel.CreationTime),
		"BackupSelection": map[string]any{
			"SelectionName": sel.SelectionName,
			"IamRoleArn":    sel.IAMRoleArn,
		},
	})
}

func (h *Handler) handleListBackupSelections(c *echo.Context, planID string) error {
	if planID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "BackupPlanId is required"),
		)
	}

	sels, err := h.Backend.ListBackupSelections(planID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		items = append(items, map[string]any{
			keyBackupPlanID: sel.BackupPlanID,
			keySelectionID:  sel.SelectionID,
			"SelectionName": sel.SelectionName,
			keyCreationDate: epochSeconds(sel.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupSelectionsList": items,
	})
}

func (h *Handler) handleDeleteBackupSelection(c *echo.Context, resource string) error {
	planID, selID, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteBackupSelection(planID, selID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Copy job handlers ---

func (h *Handler) handleListCopyJobs(c *echo.Context) error {
	jobs := h.Backend.ListCopyJobs()
	items := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		item := map[string]any{
			"CopyJobId":     j.CopyJobID,
			keyState:        j.State,
			keyCreationDate: epochSeconds(j.CreationDate),
		}
		if j.ResourceArn != "" {
			item["ResourceArn"] = j.ResourceArn
		}
		if j.SourceBackupVaultArn != "" {
			item["SourceBackupVaultArn"] = j.SourceBackupVaultArn
		}
		if j.DestinationBackupVaultArn != "" {
			item["DestinationBackupVaultArn"] = j.DestinationBackupVaultArn
		}
		items = append(items, item)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CopyJobs": items,
	})
}

func (h *Handler) handleDescribeCopyJob(c *echo.Context, copyJobID string) error {
	if copyJobID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "CopyJobId is required"),
		)
	}

	j, err := h.Backend.DescribeCopyJob(copyJobID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		"CopyJobId":     j.CopyJobID,
		keyState:        j.State,
		keyCreationDate: epochSeconds(j.CreationDate),
	}
	if j.ResourceArn != "" {
		resp["ResourceArn"] = j.ResourceArn
	}
	if j.SourceBackupVaultArn != "" {
		resp["SourceBackupVaultArn"] = j.SourceBackupVaultArn
	}
	if j.DestinationBackupVaultArn != "" {
		resp["DestinationBackupVaultArn"] = j.DestinationBackupVaultArn
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CopyJob": resp,
	})
}

// --- Restore testing read/update/delete handlers ---

func (h *Handler) handleGetRestoreTestingPlan(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	rtp, err := h.Backend.GetRestoreTestingPlan(planName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingPlan": map[string]any{
			keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
			keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
			"ScheduleExpression":      rtp.ScheduleExpression,
			keyCreationTime:           epochSeconds(rtp.CreationTime),
		},
	})
}

func (h *Handler) handleListRestoreTestingPlans(c *echo.Context) error {
	plans := h.Backend.ListRestoreTestingPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, rtp := range plans {
		items = append(items, map[string]any{
			keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
			keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
			"ScheduleExpression":      rtp.ScheduleExpression,
			keyCreationTime:           epochSeconds(rtp.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingPlans": items,
	})
}

type updateRestoreTestingPlanBody struct {
	RestoreTestingPlan restoreTestingPlanDoc `json:"RestoreTestingPlan"`
}

func (h *Handler) handleUpdateRestoreTestingPlan(
	c *echo.Context,
	planName string,
	body []byte,
) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	var in updateRestoreTestingPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	rtp, err := h.Backend.UpdateRestoreTestingPlan(
		planName,
		in.RestoreTestingPlan.ScheduleExpression,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:  rtp.RestoreTestingPlanArn,
		keyRestoreTestingPlanName: rtp.RestoreTestingPlanName,
		keyCreationTime:           epochSeconds(rtp.CreationTime),
	})
}

func (h *Handler) handleDeleteRestoreTestingPlan(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingPlan(planName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetRestoreTestingSelection(c *echo.Context, resource string) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	sel, err := h.Backend.GetRestoreTestingSelection(planName, selName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingSelection": map[string]any{
			keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
			keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
			"ProtectedResourceType":        sel.ProtectedResourceType,
			keyCreationTime:                epochSeconds(sel.CreationTime),
		},
	})
}

func (h *Handler) handleListRestoreTestingSelections(c *echo.Context, planName string) error {
	if planName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "RestoreTestingPlanName is required"),
		)
	}

	sels, err := h.Backend.ListRestoreTestingSelections(planName)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(sels))
	for _, sel := range sels {
		items = append(items, map[string]any{
			keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
			keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
			"ProtectedResourceType":        sel.ProtectedResourceType,
			keyCreationTime:                epochSeconds(sel.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingSelections": items,
	})
}

type updateRestoreTestingSelectionBody struct {
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
}

func (h *Handler) handleUpdateRestoreTestingSelection(
	c *echo.Context,
	resource string,
	body []byte,
) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	var in updateRestoreTestingSelectionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	sel, err := h.Backend.UpdateRestoreTestingSelection(
		planName,
		selName,
		in.RestoreTestingSelection.ProtectedResourceType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyRestoreTestingPlanArn:       sel.RestoreTestingPlanArn,
		keyRestoreTestingPlanName:      sel.RestoreTestingPlanName,
		keyRestoreTestingSelectionName: sel.RestoreTestingSelectionName,
		keyCreationTime:                epochSeconds(sel.CreationTime),
	})
}

func (h *Handler) handleDeleteRestoreTestingSelection(c *echo.Context, resource string) error {
	planName, selName, ok := splitPlanSel(resource)
	if !ok {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "invalid resource path"),
		)
	}

	if err := h.Backend.DeleteRestoreTestingSelection(planName, selName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Framework read/update/delete handlers ---

func (h *Handler) handleDescribeFramework(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	f, err := h.Backend.DescribeFramework(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFrameworkArn:        f.FrameworkArn,
		keyFrameworkName:       f.FrameworkName,
		"FrameworkDescription": f.FrameworkDescription,
		keyCreationTime:        epochSeconds(f.CreationTime),
	})
}

func (h *Handler) handleListFrameworks(c *echo.Context) error {
	frameworks := h.Backend.ListFrameworks()
	items := make([]map[string]any, 0, len(frameworks))

	for _, f := range frameworks {
		items = append(items, map[string]any{
			keyFrameworkArn:        f.FrameworkArn,
			keyFrameworkName:       f.FrameworkName,
			"FrameworkDescription": f.FrameworkDescription,
			keyCreationTime:        epochSeconds(f.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Frameworks": items,
	})
}

type updateFrameworkBody struct {
	FrameworkDescription string `json:"FrameworkDescription,omitempty"`
	IdempotencyToken     string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleUpdateFramework(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	var in updateFrameworkBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	f, err := h.Backend.UpdateFramework(name, in.FrameworkDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFrameworkArn:  f.FrameworkArn,
		keyFrameworkName: f.FrameworkName,
	})
}

func (h *Handler) handleDeleteFramework(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	if err := h.Backend.DeleteFramework(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Report plan read/update/delete handlers ---

func (h *Handler) handleListReportPlans(c *echo.Context) error {
	plans := h.Backend.ListReportPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, rp := range plans {
		items = append(items, map[string]any{
			keyReportPlanArn:        rp.ReportPlanArn,
			keyReportPlanName:       rp.ReportPlanName,
			"ReportPlanDescription": rp.ReportPlanDescription,
			keyCreationTime:         epochSeconds(rp.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReportPlans": items,
	})
}

func (h *Handler) handleDescribeReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	rp, err := h.Backend.DescribeReportPlan(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReportPlan": map[string]any{
			keyReportPlanArn:        rp.ReportPlanArn,
			keyReportPlanName:       rp.ReportPlanName,
			"ReportPlanDescription": rp.ReportPlanDescription,
			keyCreationTime:         epochSeconds(rp.CreationTime),
		},
	})
}

type updateReportPlanBody struct {
	ReportPlanDescription string `json:"ReportPlanDescription,omitempty"`
	IdempotencyToken      string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleUpdateReportPlan(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	var in updateReportPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	rp, err := h.Backend.UpdateReportPlan(name, in.ReportPlanDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
	})
}

func (h *Handler) handleDeleteReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	if err := h.Backend.DeleteReportPlan(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
