package iot

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	keyTaskStatus = "taskStatus"
	keyTasksField = "tasks"
)

// ---------------------------------------------------------------------------
// Route resolvers: Device Defender audit + detect mitigation-action tasks,
// violations, and audit-finding related resources.
// ---------------------------------------------------------------------------

func resolveDeviceDefenderOps(path, method string) string {
	if op := resolveAuditMitigationTaskOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveDetectMitigationTaskOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveViolationOps(path, method); op != unknownOperation {
		return op
	}

	if path == "/audit/relatedResources" && method == http.MethodGet {
		return opListRelatedResourcesForAuditFinding
	}

	return unknownOperation
}

const pathAuditMitigationTasks = "/audit/mitigationactions/tasks"

func resolveAuditMitigationTaskOps(path, method string) string {
	switch {
	case path == pathAuditMitigationTasks && method == http.MethodGet:
		return opListAuditMitigationActionsTasks
	case path == "/audit/mitigationactions/executions" && method == http.MethodGet:
		return opListAuditMitigationActionsExecutions
	// The "/cancel" suffix is handled by resolveJobAndAuditOps
	// (opCancelAuditMitigationActionsTask); skip it here.
	case strings.HasPrefix(path, pathAuditMitigationTasks+"/") &&
		!strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPost:
		return opStartAuditMitigationActionsTask
	case strings.HasPrefix(path, pathAuditMitigationTasks+"/") &&
		!strings.HasSuffix(path, "/cancel") &&
		method == http.MethodGet:
		return opDescribeAuditMitigationActionsTask
	}

	return unknownOperation
}

const pathDetectMitigationTasks = "/detect/mitigationactions/tasks"

func resolveDetectMitigationTaskOps(path, method string) string {
	switch {
	case path == pathDetectMitigationTasks && method == http.MethodGet:
		return opListDetectMitigationActionsTasks
	case path == "/detect/mitigationactions/executions" && method == http.MethodGet:
		return opListDetectMitigationActionsExecutions
	case strings.HasPrefix(path, pathDetectMitigationTasks+"/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return opCancelDetectMitigationActionsTask
	case strings.HasPrefix(path, pathDetectMitigationTasks+"/") &&
		!strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return opStartDetectMitigationActionsTask
	case strings.HasPrefix(path, pathDetectMitigationTasks+"/") &&
		!strings.HasSuffix(path, "/cancel") &&
		method == http.MethodGet:
		return opDescribeDetectMitigationActionsTask
	}

	return unknownOperation
}

func resolveViolationOps(path, method string) string {
	switch {
	case path == "/active-violations" && method == http.MethodGet:
		return opListActiveViolations
	case path == "/violation-events" && method == http.MethodGet:
		return opListViolationEvents
	case strings.HasPrefix(path, "/violations/verification-state/") && method == http.MethodPost:
		return opPutVerificationStateOnViolation
	}

	return unknownOperation
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchDeviceDefenderOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opStartAuditMitigationActionsTask:
		return true, h.handleStartAuditMitigationActionsTask(c)
	case opDescribeAuditMitigationActionsTask:
		return true, h.handleDescribeAuditMitigationActionsTask(c)
	case opListAuditMitigationActionsTasks:
		return true, h.handleListAuditMitigationActionsTasks(c)
	case opListAuditMitigationActionsExecutions:
		return true, h.handleListAuditMitigationActionsExecutions(c)
	case opStartDetectMitigationActionsTask:
		return true, h.handleStartDetectMitigationActionsTask(c)
	case opDescribeDetectMitigationActionsTask:
		return true, h.handleDescribeDetectMitigationActionsTask(c)
	case opListDetectMitigationActionsTasks:
		return true, h.handleListDetectMitigationActionsTasks(c)
	case opListDetectMitigationActionsExecutions:
		return true, h.handleListDetectMitigationActionsExecutions(c)
	case opCancelDetectMitigationActionsTask:
		return true, h.handleCancelDetectMitigationActionsTask(c)
	case opListActiveViolations:
		return true, h.handleListActiveViolations(c)
	case opListViolationEvents:
		return true, h.handleListViolationEvents(c)
	case opPutVerificationStateOnViolation:
		return true, h.handlePutVerificationStateOnViolation(c)
	case opListRelatedResourcesForAuditFinding:
		return true, h.handleListRelatedResourcesForAuditFinding(c)
	}

	return false, nil
}

// ---------------------------------------------------------------------------
// Handlers: Audit mitigation-action tasks
// ---------------------------------------------------------------------------

func (h *Handler) handleStartAuditMitigationActionsTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, pathAuditMitigationTasks+"/")

	var req struct {
		Target                     *AuditMitigationActionsTaskTarget `json:"target"`
		AuditCheckToActionsMapping map[string][]string               `json:"auditCheckToActionsMapping"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	task, err := h.Backend.StartAuditMitigationActionsTask(&StartAuditMitigationActionsTaskInput{
		TaskID:                     taskID,
		Target:                     req.Target,
		AuditCheckToActionsMapping: req.AuditCheckToActionsMapping,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyTaskID: task.TaskID})
}

func (h *Handler) handleDescribeAuditMitigationActionsTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, pathAuditMitigationTasks+"/")

	task, err := h.Backend.DescribeAuditMitigationActionsTask(taskID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, task)
}

func (h *Handler) handleListAuditMitigationActionsTasks(c *echo.Context) error {
	auditTaskID := c.QueryParam("auditTaskId")
	taskStatus := c.QueryParam(keyTaskStatus)

	tasks := h.Backend.ListAuditMitigationActionsTasks(auditTaskID, taskStatus)

	summaries := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		summaries = append(summaries, map[string]any{
			keyTaskID:     t.TaskID,
			keyTaskStatus: t.TaskStatus,
			"startTime":   t.StartTime,
			"endTime":     t.EndTime,
		})
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(summaries, pageSize, start)

	resp := map[string]any{keyTasksField: page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListAuditMitigationActionsExecutions(c *echo.Context) error {
	taskID := c.QueryParam(keyTaskID)
	findingID := c.QueryParam("findingId")
	actionStatus := c.QueryParam("actionStatus")

	execs := h.Backend.ListAuditMitigationActionsExecutions(taskID, findingID, actionStatus)

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(execs, pageSize, start)

	resp := map[string]any{"actionsExecutions": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Handlers: Detect mitigation-action tasks
// ---------------------------------------------------------------------------

func (h *Handler) handleStartDetectMitigationActionsTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, pathDetectMitigationTasks+"/")

	var req struct {
		Target                      *DetectMitigationActionsTaskTarget `json:"target"`
		Actions                     []string                           `json:"actions"`
		IncludeOnlyActiveViolations bool                               `json:"includeOnlyActiveViolations"`
		IncludeSuppressedAlerts     bool                               `json:"includeSuppressedAlerts"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	task, err := h.Backend.StartDetectMitigationActionsTask(&StartDetectMitigationActionsTaskInput{
		TaskID:                      taskID,
		Target:                      req.Target,
		Actions:                     req.Actions,
		IncludeOnlyActiveViolations: req.IncludeOnlyActiveViolations,
		IncludeSuppressedAlerts:     req.IncludeSuppressedAlerts,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyTaskID: task.TaskID})
}

func (h *Handler) handleDescribeDetectMitigationActionsTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, pathDetectMitigationTasks+"/")

	task, err := h.Backend.DescribeDetectMitigationActionsTask(taskID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"taskSummary": task})
}

func (h *Handler) handleListDetectMitigationActionsTasks(c *echo.Context) error {
	startTime := parseIoTEpochQueryParam(c, "startTime")
	endTime := parseIoTEpochQueryParam(c, "endTime")

	tasks := h.Backend.ListDetectMitigationActionsTasks(startTime, endTime)

	summaries := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		summaries = append(summaries, map[string]any{
			keyTaskID:       t.TaskID,
			keyTaskStatus:   t.TaskStatus,
			"taskStartTime": t.StartTime,
			"taskEndTime":   t.EndTime,
		})
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(summaries, pageSize, start)

	resp := map[string]any{keyTasksField: page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCancelDetectMitigationActionsTask(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, pathDetectMitigationTasks+"/")
	taskID := strings.SplitN(after, "/", maxPathSegments)[0]

	if err := h.Backend.CancelDetectMitigationActionsTask(taskID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListDetectMitigationActionsExecutions(c *echo.Context) error {
	taskID := c.QueryParam(keyTaskID)
	violationID := c.QueryParam("violationId")
	thingName := c.QueryParam("thingName")

	execs := h.Backend.ListDetectMitigationActionsExecutions(taskID, violationID, thingName)

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(execs, pageSize, start)

	resp := map[string]any{"actionsExecutions": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Handlers: Violations
// ---------------------------------------------------------------------------

func (h *Handler) handleListActiveViolations(c *echo.Context) error {
	thingName := c.QueryParam("thingName")
	securityProfileName := c.QueryParam("securityProfileName")
	verificationState := c.QueryParam("verificationState")

	violations := h.Backend.ListActiveViolations(thingName, securityProfileName, verificationState)

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(violations, pageSize, start)

	resp := map[string]any{"activeViolations": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListViolationEvents(c *echo.Context) error {
	thingName := c.QueryParam("thingName")
	securityProfileName := c.QueryParam("securityProfileName")
	verificationState := c.QueryParam("verificationState")
	startTime := parseIoTEpochQueryParam(c, "startTime")
	endTime := parseIoTEpochQueryParam(c, "endTime")

	events := h.Backend.ListViolationEvents(thingName, securityProfileName, verificationState, startTime, endTime)

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(events, pageSize, start)

	resp := map[string]any{"violationEvents": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handlePutVerificationStateOnViolation(c *echo.Context) error {
	violationID := strings.TrimPrefix(c.Request().URL.Path, "/violations/verification-state/")

	var req struct {
		VerificationState            string `json:"verificationState"`
		VerificationStateDescription string `json:"verificationStateDescription"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	if req.VerificationState == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyError: "verificationState is required"},
		)
	}

	err := h.Backend.PutVerificationStateOnViolation(
		violationID,
		req.VerificationState,
		req.VerificationStateDescription,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---------------------------------------------------------------------------
// Handlers: Audit finding related resources
// ---------------------------------------------------------------------------

func (h *Handler) handleListRelatedResourcesForAuditFinding(c *echo.Context) error {
	findingID := c.QueryParam("findingId")

	resources, err := h.Backend.ListRelatedResourcesForAuditFinding(findingID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"relatedResources": resources})
}

// parseIoTEpochQueryParam parses a query parameter as a float64 epoch-seconds
// timestamp, returning 0 (unbounded) if absent or invalid.
func parseIoTEpochQueryParam(c *echo.Context, name string) float64 {
	v := c.QueryParam(name)
	if v == "" {
		return 0
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}

	return f
}

func (h *Handler) handleCancelAuditMitigationActionsTask(c *echo.Context) error {
	// Path: /audit/mitigationactions/tasks/{taskId}/cancel
	after := strings.TrimPrefix(c.Request().URL.Path, "/audit/mitigationactions/tasks/")
	taskID := strings.SplitN(after, "/", maxPathSegments)[0]

	if err := h.Backend.CancelAuditMitigationActionsTask(&CancelAuditMitigationActionsTaskInput{
		TaskID: taskID,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBehaviorModelTrainingSummaries(c *echo.Context) error {
	securityProfileName := c.QueryParam(keySecurityProfileName)
	maxResults := parseInt32QueryParam(c, "maxResults")
	nextToken := c.QueryParam("nextToken")

	summaries, next, err := h.Backend.GetBehaviorModelTrainingSummaries(securityProfileName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{"summaries": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}
