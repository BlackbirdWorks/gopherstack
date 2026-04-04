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
	backupMatchPriority = service.PriorityPathVersioned

	pathBackupVaults        = "/backup-vaults"
	pathBackupPlans         = "/backup/plans"
	pathBackupJobs          = "/backup-jobs"
	pathTags                = "/tags/"
	pathLegalHolds          = "/legal-holds"
	pathAuditFrameworks     = "/audit/frameworks"
	pathAuditReportPlans    = "/audit/report-plans"
	pathLogicallyAirGapped  = "/logically-air-gapped-backup-vaults"
	pathRestoreAccessVaults = "/restore-access-backup-vaults"
	pathRestoreTestingPlans = "/restore-testing/plans"

	// splitTwo is the N argument for [strings.SplitN] to split into at most 2 parts.
	splitTwo = 2
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
func (h *Handler) WithJanitor(interval, jobTTL time.Duration, taskTimeout ...time.Duration) *Handler {
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
		"AssociateBackupVaultMpaApprovalTeam",
		"CancelLegalHold",
		"CreateBackupSelection",
		"CreateBackupVault",
		"CreateFramework",
		"CreateLegalHold",
		"CreateLogicallyAirGappedBackupVault",
		"CreateReportPlan",
		"CreateRestoreAccessBackupVault",
		"CreateRestoreTestingPlan",
		"CreateRestoreTestingSelection",
		"DescribeBackupVault",
		"ListBackupVaults",
		"DeleteBackupVault",
		"CreateBackupPlan",
		"GetBackupPlan",
		"ListBackupPlans",
		"UpdateBackupPlan",
		"DeleteBackupPlan",
		"StartBackupJob",
		"DescribeBackupJob",
		"ListBackupJobs",
		"TagResource",
		"ListTags",
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
	case strings.HasPrefix(path, pathTags):
		return parseTagsRoute(method, strings.TrimPrefix(path, pathTags))
	case strings.HasPrefix(path, pathLegalHolds):
		return parseLegalHoldRoute(method, strings.TrimPrefix(path, pathLegalHolds))
	case strings.HasPrefix(path, pathAuditFrameworks):
		return parseFrameworkRoute(method, strings.TrimPrefix(path, pathAuditFrameworks))
	case strings.HasPrefix(path, pathAuditReportPlans):
		return parseReportPlanRoute(method, strings.TrimPrefix(path, pathAuditReportPlans))
	case strings.HasPrefix(path, pathLogicallyAirGapped):
		return parseLogicallyAirGappedRoute(method, strings.TrimPrefix(path, pathLogicallyAirGapped))
	case strings.HasPrefix(path, pathRestoreAccessVaults):
		return parseRestoreAccessVaultRoute(method, strings.TrimPrefix(path, pathRestoreAccessVaults))
	case strings.HasPrefix(path, pathRestoreTestingPlans):
		return parseRestoreTestingRoute(method, strings.TrimPrefix(path, pathRestoreTestingPlans))
	}

	return backupRoute{operation: "Unknown"}
}

func parseVaultRoute(method, suffix string) backupRoute {
	// suffix is either "" (collection) or "/{name}" or "/{name}/mpaApprovalTeam"
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		// /backup-vaults
		if method == http.MethodGet {
			return backupRoute{operation: "ListBackupVaults"}
		}
	} else if before, ok := strings.CutSuffix(name, "/mpaApprovalTeam"); ok {
		// /backup-vaults/{name}/mpaApprovalTeam
		vaultName := before
		if method == http.MethodPut {
			return backupRoute{operation: "AssociateBackupVaultMpaApprovalTeam", resource: vaultName}
		}
	} else if !strings.Contains(name, "/") {
		// /backup-vaults/{name}
		switch method {
		case http.MethodPut:
			return backupRoute{operation: "CreateBackupVault", resource: name}
		case http.MethodGet:
			return backupRoute{operation: "DescribeBackupVault", resource: name}
		case http.MethodDelete:
			return backupRoute{operation: "DeleteBackupVault", resource: name}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parsePlanRoute(method, suffix string) backupRoute {
	// suffix is "" or "/{id}" or "/{id}/selections"
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		// /backup/plans
		switch method {
		case http.MethodPut:
			return backupRoute{operation: "CreateBackupPlan"}
		case http.MethodGet:
			return backupRoute{operation: "ListBackupPlans"}
		}
	case strings.Contains(id, "/"):
		// /backup/plans/{id}/selections
		parts := strings.SplitN(id, "/", splitTwo)
		planID := parts[0]
		rest := parts[1]

		if rest == "selections" && method == http.MethodPut {
			return backupRoute{operation: "CreateBackupSelection", resource: planID}
		}
	default:
		// /backup/plans/{id}
		switch method {
		case http.MethodGet:
			return backupRoute{operation: "GetBackupPlan", resource: id}
		case http.MethodPost:
			return backupRoute{operation: "UpdateBackupPlan", resource: id}
		case http.MethodDelete:
			return backupRoute{operation: "DeleteBackupPlan", resource: id}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseJobRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /backup-jobs
		switch method {
		case http.MethodPut:
			return backupRoute{operation: "StartBackupJob"}
		case http.MethodGet:
			return backupRoute{operation: "ListBackupJobs"}
		}
	} else if !strings.Contains(id, "/") {
		// /backup-jobs/{id}
		if method == http.MethodGet {
			return backupRoute{operation: "DescribeBackupJob", resource: id}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseTagsRoute(method, resourceArn string) backupRoute {
	switch method {
	case http.MethodPost:
		return backupRoute{operation: "TagResource", resource: resourceArn}
	case http.MethodGet:
		return backupRoute{operation: "ListTags", resource: resourceArn}
	}

	return backupRoute{operation: "Unknown"}
}

func parseLegalHoldRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /legal-holds
		if method == http.MethodPost {
			return backupRoute{operation: "CreateLegalHold"}
		}
	} else if !strings.Contains(id, "/") {
		// /legal-holds/{id}
		if method == http.MethodDelete {
			return backupRoute{operation: "CancelLegalHold", resource: id}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseFrameworkRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		// /audit/frameworks
		if method == http.MethodPost {
			return backupRoute{operation: "CreateFramework"}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseReportPlanRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name == "" {
		// /audit/report-plans
		if method == http.MethodPost {
			return backupRoute{operation: "CreateReportPlan"}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseLogicallyAirGappedRoute(method, suffix string) backupRoute {
	name := strings.TrimPrefix(suffix, "/")
	if name != "" && !strings.Contains(name, "/") {
		// /logically-air-gapped-backup-vaults/{name}
		if method == http.MethodPut {
			return backupRoute{operation: "CreateLogicallyAirGappedBackupVault", resource: name}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseRestoreAccessVaultRoute(method, suffix string) backupRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		// /restore-access-backup-vaults
		if method == http.MethodPost {
			return backupRoute{operation: "CreateRestoreAccessBackupVault"}
		}
	}

	return backupRoute{operation: "Unknown"}
}

func parseRestoreTestingRoute(method, suffix string) backupRoute {
	// suffix is "" or "/{planName}" or "/{planName}/selections"
	rest := strings.TrimPrefix(suffix, "/")
	if rest == "" {
		// /restore-testing/plans
		if method == http.MethodPut {
			return backupRoute{operation: "CreateRestoreTestingPlan"}
		}
	} else if strings.Contains(rest, "/") {
		// /restore-testing/plans/{planName}/selections
		parts := strings.SplitN(rest, "/", splitTwo)
		planName := parts[0]
		sub := parts[1]
		if sub == "selections" && method == http.MethodPut {
			return backupRoute{operation: "CreateRestoreTestingSelection", resource: planName}
		}
	}

	return backupRoute{operation: "Unknown"}
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
	case "CreateBackupVault":
		return h.handleCreateBackupVault(c, route.resource, body)
	case "DescribeBackupVault":
		return h.handleDescribeBackupVault(c, route.resource)
	case "ListBackupVaults":
		return h.handleListBackupVaults(c)
	case "DeleteBackupVault":
		return h.handleDeleteBackupVault(c, route.resource)
	case "CreateBackupPlan":
		return h.handleCreateBackupPlan(c, body)
	case "GetBackupPlan":
		return h.handleGetBackupPlan(c, route.resource)
	case "ListBackupPlans":
		return h.handleListBackupPlans(c)
	case "UpdateBackupPlan":
		return h.handleUpdateBackupPlan(c, route.resource, body)
	case "DeleteBackupPlan":
		return h.handleDeleteBackupPlan(c, route.resource)
	case "StartBackupJob":
		return h.handleStartBackupJob(c, body)
	case "DescribeBackupJob":
		return h.handleDescribeBackupJob(c, route.resource)
	case "ListBackupJobs":
		return h.handleListBackupJobs(c)
	case "TagResource":
		return h.handleTagResource(c, route.resource, body)
	case "ListTags":
		return h.handleListTags(c, route.resource)
	default:
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}
}

// dispatchNewOps dispatches the 10 newly added Backup operations.
// It returns (true, result) if the operation was handled, or (false, nil) otherwise.
func (h *Handler) dispatchNewOps(c *echo.Context, route backupRoute, body []byte) (bool, error) {
	switch route.operation {
	case "AssociateBackupVaultMpaApprovalTeam":
		return true, h.handleAssociateBackupVaultMpaApprovalTeam(c, route.resource, body)
	case "CancelLegalHold":
		return true, h.handleCancelLegalHold(c, route.resource)
	case "CreateBackupSelection":
		return true, h.handleCreateBackupSelection(c, route.resource, body)
	case "CreateFramework":
		return true, h.handleCreateFramework(c, body)
	case "CreateLegalHold":
		return true, h.handleCreateLegalHold(c, body)
	case "CreateLogicallyAirGappedBackupVault":
		return true, h.handleCreateLogicallyAirGappedBackupVault(c, route.resource, body)
	case "CreateReportPlan":
		return true, h.handleCreateReportPlan(c, body)
	case "CreateRestoreAccessBackupVault":
		return true, h.handleCreateRestoreAccessBackupVault(c, body)
	case "CreateRestoreTestingPlan":
		return true, h.handleCreateRestoreTestingPlan(c, body)
	case "CreateRestoreTestingSelection":
		return true, h.handleCreateRestoreTestingSelection(c, route.resource, body)
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
	CreatorRequestID string            `json:"CreatorRequestID"`
}

func (h *Handler) handleCreateBackupVault(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "BackupVaultName is required"))
	}

	var in createBackupVaultBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	v, err := h.Backend.CreateBackupVault(name, in.EncryptionKeyArn, in.CreatorRequestID, in.BackupVaultTags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupVaultArn":  v.BackupVaultArn,
		"BackupVaultName": v.BackupVaultName,
		"CreationDate":    epochSeconds(v.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupVault(c *echo.Context, name string) error {
	v, err := h.Backend.DescribeBackupVault(name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		"BackupVaultName":        v.BackupVaultName,
		"BackupVaultArn":         v.BackupVaultArn,
		"CreationDate":           epochSeconds(v.CreationTime),
		"NumberOfRecoveryPoints": v.NumberOfRecoveryPoints,
	}
	if v.EncryptionKeyArn != "" {
		resp["EncryptionKeyArn"] = v.EncryptionKeyArn
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListBackupVaults(c *echo.Context) error {
	vaults := h.Backend.ListBackupVaults()
	items := make([]map[string]any, 0, len(vaults))

	for _, v := range vaults {
		items = append(items, map[string]any{
			"BackupVaultName":        v.BackupVaultName,
			"BackupVaultArn":         v.BackupVaultArn,
			"CreationDate":           epochSeconds(v.CreationTime),
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
			errResp("ValidationException", fmt.Sprintf("%s: BackupPlanName is required", errInvalidRequest)),
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
		"BackupPlanArn": p.BackupPlanArn,
		"BackupPlanId":  p.BackupPlanID,
		"VersionId":     p.VersionID,
		"CreationDate":  epochSeconds(p.CreationTime),
	})
}

func (h *Handler) handleGetBackupPlan(c *echo.Context, id string) error {
	p, err := h.Backend.GetBackupPlan(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPlanArn": p.BackupPlanArn,
		"BackupPlanId":  p.BackupPlanID,
		"VersionId":     p.VersionID,
		"CreationDate":  epochSeconds(p.CreationTime),
		"BackupPlan": map[string]any{
			"BackupPlanName": p.BackupPlanName,
			"Rules":          rulesToJSON(p.Rules),
		},
	})
}

func (h *Handler) handleListBackupPlans(c *echo.Context) error {
	plans := h.Backend.ListBackupPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, p := range plans {
		items = append(items, map[string]any{
			"BackupPlanName": p.BackupPlanName,
			"BackupPlanArn":  p.BackupPlanArn,
			"BackupPlanId":   p.BackupPlanID,
			"VersionId":      p.VersionID,
			"CreationDate":   epochSeconds(p.CreationTime),
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
		"BackupPlanArn": p.BackupPlanArn,
		"BackupPlanId":  p.BackupPlanID,
		"VersionId":     p.VersionID,
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
		"BackupPlanArn": p.BackupPlanArn,
		"BackupPlanId":  p.BackupPlanID,
		"VersionId":     p.VersionID,
		"DeletionDate":  epochSeconds(p.CreationTime),
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
			errResp("ValidationException", fmt.Sprintf("%s: BackupVaultName is required", errInvalidRequest)),
		)
	}

	j, err := h.Backend.StartBackupJob(in.BackupVaultName, in.ResourceArn, in.IamRoleArn, in.ResourceType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupJobId":    j.BackupJobID,
		"BackupVaultArn": j.BackupVaultArn,
		"CreationDate":   epochSeconds(j.CreationTime),
	})
}

func (h *Handler) handleDescribeBackupJob(c *echo.Context, jobID string) error {
	j, err := h.Backend.DescribeBackupJob(jobID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		"BackupJobId":     j.BackupJobID,
		"BackupVaultName": j.BackupVaultName,
		"BackupVaultArn":  j.BackupVaultArn,
		"State":           j.State,
		"CreationDate":    epochSeconds(j.CreationTime),
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
			"BackupJobId":     j.BackupJobID,
			"BackupVaultName": j.BackupVaultName,
			"BackupVaultArn":  j.BackupVaultArn,
			"ResourceArn":     j.ResourceArn,
			"State":           j.State,
			"CreationDate":    epochSeconds(j.CreationTime),
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

// --- New operation handlers ---

type associateMpaApprovalTeamBody struct {
	MpaApprovalTeamArn string `json:"MpaApprovalTeamArn"`
	RequesterComment   string `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleAssociateBackupVaultMpaApprovalTeam(c *echo.Context, vaultName string, body []byte) error {
	if vaultName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "BackupVaultName is required"))
	}

	var in associateMpaApprovalTeamBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	if err := h.Backend.AssociateBackupVaultMpaApprovalTeam(vaultName, in.MpaApprovalTeamArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCancelLegalHold(c *echo.Context, legalHoldID string) error {
	if legalHoldID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "LegalHoldId is required"))
	}

	if err := h.Backend.CancelLegalHold(legalHoldID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type createBackupSelectionBody struct {
	BackupSelection  backupSelectionDoc `json:"BackupSelection"`
	CreatorRequestID string             `json:"CreatorRequestID,omitempty"`
}

type backupSelectionDoc struct {
	SelectionName string `json:"SelectionName"`
	IamRoleArn    string `json:"IamRoleArn,omitempty"`
}

func (h *Handler) handleCreateBackupSelection(c *echo.Context, planID string, body []byte) error {
	if planID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "BackupPlanId is required"))
	}

	var in createBackupSelectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	sel, err := h.Backend.CreateBackupSelection(planID, in.BackupSelection.SelectionName, in.BackupSelection.IamRoleArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPlanId": sel.BackupPlanID,
		"SelectionId":  sel.SelectionID,
		"CreationDate": epochSeconds(sel.CreationTime),
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
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "FrameworkName is required"))
	}

	f, err := h.Backend.CreateFramework(in.FrameworkName, in.FrameworkDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FrameworkArn":  f.FrameworkArn,
		"FrameworkName": f.FrameworkName,
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
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "Description is required"))
	}

	lh, err := h.Backend.CreateLegalHold(in.Title, in.Description)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LegalHoldId":  lh.LegalHoldID,
		"LegalHoldArn": lh.LegalHoldArn,
		"Title":        lh.Title,
		"Description":  lh.Description,
		"Status":       lh.Status,
		"CreationDate": epochSeconds(lh.CreationDate),
	})
}

type createLogicallyAirGappedBody struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID string            `json:"CreatorRequestID,omitempty"`
	MaxRetentionDays int64             `json:"MaxRetentionDays"`
	MinRetentionDays int64             `json:"MinRetentionDays"`
}

func (h *Handler) handleCreateLogicallyAirGappedBackupVault(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "BackupVaultName is required"))
	}

	var in createLogicallyAirGappedBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	v, err := h.Backend.CreateLogicallyAirGappedBackupVault(
		name, in.CreatorRequestID, in.MinRetentionDays, in.MaxRetentionDays, in.BackupVaultTags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupVaultArn":  v.BackupVaultArn,
		"BackupVaultName": v.BackupVaultName,
		"CreationDate":    epochSeconds(v.CreationTime),
		"VaultState":      "CREATING",
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
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "ReportPlanName is required"))
	}

	rp, err := h.Backend.CreateReportPlan(in.ReportPlanName, in.ReportPlanDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReportPlanArn":  rp.ReportPlanArn,
		"ReportPlanName": rp.ReportPlanName,
		"CreationTime":   epochSeconds(rp.CreationTime),
	})
}

type createRestoreAccessVaultBody struct {
	SourceBackupVaultArn string            `json:"SourceBackupVaultArn"`
	BackupVaultName      string            `json:"BackupVaultName,omitempty"`
	BackupVaultTags      map[string]string `json:"BackupVaultTags,omitempty"`
	CreatorRequestID     string            `json:"CreatorRequestID,omitempty"`
	RequesterComment     string            `json:"RequesterComment,omitempty"`
}

func (h *Handler) handleCreateRestoreAccessBackupVault(c *echo.Context, body []byte) error {
	var in createRestoreAccessVaultBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.SourceBackupVaultArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "SourceBackupVaultArn is required"))
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
		"CreationDate":                 epochSeconds(rav.CreationDate),
		"VaultState":                   rav.VaultState,
	})
}

type restoreTestingPlanDoc struct {
	RestoreTestingPlanName string `json:"RestoreTestingPlanName"`
	ScheduleExpression     string `json:"ScheduleExpression,omitempty"`
}

type createRestoreTestingPlanBody struct {
	RestoreTestingPlan restoreTestingPlanDoc `json:"RestoreTestingPlan"`
	CreatorRequestID   string                `json:"CreatorRequestID,omitempty"`
}

func (h *Handler) handleCreateRestoreTestingPlan(c *echo.Context, body []byte) error {
	var in createRestoreTestingPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.RestoreTestingPlan.RestoreTestingPlanName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "RestoreTestingPlanName is required"))
	}

	rtp, err := h.Backend.CreateRestoreTestingPlan(
		in.RestoreTestingPlan.RestoreTestingPlanName,
		in.RestoreTestingPlan.ScheduleExpression,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"RestoreTestingPlanArn":  rtp.RestoreTestingPlanArn,
		"RestoreTestingPlanName": rtp.RestoreTestingPlanName,
		"CreationTime":           epochSeconds(rtp.CreationTime),
	})
}

type restoreTestingSelectionDoc struct {
	RestoreTestingSelectionName string `json:"RestoreTestingSelectionName"`
	ProtectedResourceType       string `json:"ProtectedResourceType,omitempty"`
}

type createRestoreTestingSelectionBody struct {
	RestoreTestingSelection restoreTestingSelectionDoc `json:"RestoreTestingSelection"`
	CreatorRequestID        string                     `json:"CreatorRequestID,omitempty"`
}

func (h *Handler) handleCreateRestoreTestingSelection(c *echo.Context, planName string, body []byte) error {
	if planName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "RestoreTestingPlanName is required"))
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
		"RestoreTestingPlanArn":       sel.RestoreTestingPlanArn,
		"RestoreTestingPlanName":      sel.RestoreTestingPlanName,
		"RestoreTestingSelectionName": sel.RestoreTestingSelectionName,
		"CreationTime":                epochSeconds(sel.CreationTime),
	})
}
