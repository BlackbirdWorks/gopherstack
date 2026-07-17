package backup

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
	groups := [][]string{
		supportedOpsCore(),
		supportedOpsRecoveryPoints(),
		supportedOpsVaultCompliance(),
		supportedOpsSelections(),
		supportedOpsCopyJobs(),
		supportedOpsRestoreTesting(),
		supportedOpsFrameworks(),
		supportedOpsReportPlans(),
		supportedOpsExtended(),
	}

	total := 0
	for _, g := range groups {
		total += len(g)
	}

	ops := make([]string, 0, total)
	for _, g := range groups {
		ops = append(ops, g...)
	}

	return ops
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

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return backupMatchPriority }

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

// dispatch routes a parsed Backup route to the appropriate handler.
func (h *Handler) dispatch(c *echo.Context, route backupRoute, body []byte) error {
	if ok, result := h.dispatchNewOps(c, route, body); ok {
		return result
	}

	if ok, result := h.dispatchVaultPlanOps(c, route, body); ok {
		return result
	}

	if ok, result := h.dispatchJobTagOps(c, route, body); ok {
		return result
	}

	return c.JSON(
		http.StatusNotFound,
		errResp("ResourceNotFoundException", "unknown operation: "+route.operation),
	)
}

// dispatchVaultPlanOps handles backup vault and backup plan operations.
func (h *Handler) dispatchVaultPlanOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
	switch route.operation {
	case opCreateBackupVault:

		return true, h.handleCreateBackupVault(c, route.resource, body)
	case opDescribeBackupVault:

		return true, h.handleDescribeBackupVault(c, route.resource)
	case opListBackupVaults:

		return true, h.handleListBackupVaults(c)
	case opDeleteBackupVault:

		return true, h.handleDeleteBackupVault(c, route.resource)
	case opCreateBackupPlan:

		return true, h.handleCreateBackupPlan(c, body)
	case opGetBackupPlan:

		return true, h.handleGetBackupPlan(c, route.resource)
	case opListBackupPlans:

		return true, h.handleListBackupPlans(c)
	case opUpdateBackupPlan:

		return true, h.handleUpdateBackupPlan(c, route.resource, body)
	case opDeleteBackupPlan:

		return true, h.handleDeleteBackupPlan(c, route.resource)
	}

	return false, nil
}

// dispatchJobTagOps handles backup job and tagging operations.
func (h *Handler) dispatchJobTagOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case opStartBackupJob:

		return true, h.handleStartBackupJob(c, body)
	case opDescribeBackupJob:

		return true, h.handleDescribeBackupJob(c, route.resource)
	case opListBackupJobs:

		return true, h.handleListBackupJobs(c)
	case opTagResource:

		return true, h.handleTagResource(c, route.resource, body)
	case opUntagResource:

		return true, h.handleUntagResource(c, route.resource, body)
	case opListTags:

		return true, h.handleListTags(c, route.resource)
	}

	return false, nil
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

	if ok, result := h.dispatchExtendedOps(c, route, body); ok {
		return true, result
	}

	return false, nil
}

// dispatchExtendedOps dispatches the remaining domain-specific operation
// families (settings, protected resources, restore/report/scan jobs, plan
// templates, tiering, restore-access vaults). Split out of dispatchNewOps to
// keep its cyclomatic complexity in check.
func (h *Handler) dispatchExtendedOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	if ok, result := h.dispatchSettingsOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchProtectedResourceOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchRestoreJobOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchReportJobOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchLegalHoldOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchRecoveryPointQueryOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchRecoveryPointIndexOps(c, route, body); ok {
		return true, result
	}

	return h.dispatchExtendedOpsContinued(c, route, body)
}

// dispatchExtendedOpsContinued continues dispatchExtendedOps: plan templates,
// tiering, restore-access vaults, and job summaries. Split purely to keep
// dispatchExtendedOps's cyclomatic complexity in check.
func (h *Handler) dispatchExtendedOpsContinued(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	if ok, result := h.dispatchPlanTemplateCatalogOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchBackupJobSummaryOps(c, route); ok {
		return true, result
	}

	if ok, result := h.dispatchCopyJobExtraOps(c, route, body); ok {
		return true, result
	}

	if ok, result := h.dispatchRestoreAccessVaultOps(c, route); ok {
		return true, result
	}

	return h.dispatchTieringOps(c, route, body)
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

func (h *Handler) dispatchVaultComplianceOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
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
	case opGetRestoreTestingInferredMetadata:

		return true, c.JSON(http.StatusOK, map[string]any{"InferredMetadata": map[string]string{}})
	}

	return false, nil
}

func (h *Handler) dispatchFrameworkOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
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

func (h *Handler) dispatchReportPlanOps(
	c *echo.Context,
	route backupRoute,
	body []byte,
) (bool, error) {
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

// parseInt parses a decimal integer string, returning 0 on error or empty input.
func parseInt(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return n
}

// --- Vault handlers ---

// setOptionalStr sets key in m if v is non-empty.
func setOptionalStr(m map[string]any, key, v string) {
	if v != "" {
		m[key] = v
	}
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
