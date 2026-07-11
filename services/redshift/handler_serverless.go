package redshift

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---------------------------------------------------------------------------
// Serverless handler wiring
// ---------------------------------------------------------------------------

// ServerlessHandler is a separate Echo handler for Redshift Serverless APIs.
// The serverless API lives at a different hostname/path prefix than the classic
// Redshift API, so it is registered as its own Registerable.
type ServerlessHandler struct {
	Backend *InMemoryBackend
}

// NewServerlessHandler creates a new Redshift Serverless handler.
func NewServerlessHandler(backend *InMemoryBackend) *ServerlessHandler {
	return &ServerlessHandler{Backend: backend}
}

// Name returns the service name.
func (h *ServerlessHandler) Name() string { return "RedshiftServerless" }

// GetSupportedOperations returns the list of supported operations.
func (h *ServerlessHandler) GetSupportedOperations() []string {
	return []string{
		"CreateNamespace",
		"DeleteNamespace",
		"GetNamespace",
		"ListNamespaces",
		"UpdateNamespace",
		"CreateWorkgroup",
		"DeleteWorkgroup",
		"GetWorkgroup",
		"ListWorkgroups",
		"UpdateWorkgroup",
		"GetCredentials",
		"CreateSnapshot",
		"DeleteSnapshot",
		"GetSnapshot",
		"ListSnapshots",
		"CreateUsageLimit",
		"DeleteUsageLimit",
		"GetUsageLimit",
		"ListUsageLimits",
		"UpdateUsageLimit",
		"CreateScheduledAction",
		"DeleteScheduledAction",
		"GetScheduledAction",
		"ListScheduledActions",
		"UpdateScheduledAction",
	}
}

// ChaosServiceName returns the chaos service name.
func (h *ServerlessHandler) ChaosServiceName() string { return "redshift-serverless" }

// ChaosOperations returns all supported operations.
func (h *ServerlessHandler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions for chaos.
func (h *ServerlessHandler) ChaosRegions() []string { return []string{h.Backend.region} }

// Reset clears all backend state.
func (h *ServerlessHandler) Reset() {
	h.Backend.mu.Lock("ServerlessHandler.Reset")
	defer h.Backend.mu.Unlock()

	h.Backend.slNamespaces.Reset()
	h.Backend.slWorkgroups.Reset()
	h.Backend.slSnapshots.Reset()
	h.Backend.slUsageLimits.Reset()
	h.Backend.slScheduledActions.Reset()
}

const (
	slPrefix                 = "/redshift-serverless/namespaces"
	slWorkgroupsPrefix       = "/redshift-serverless/workgroups"
	slSnapshotsPrefix        = "/redshift-serverless/snapshots"
	slUsageLimitsPrefix      = "/redshift-serverless/usagelimits"
	slScheduledActionsPrefix = "/redshift-serverless/scheduledactions"
	slCredentialsPrefix      = "/redshift-serverless/workgroups/"
	slRespNamespace          = "namespace"
	slRespWorkgroup          = "workgroup"
	slRespSnapshot           = "snapshot"
	slRespUsageLimit         = "usageLimit"
	slRespScheduledAction    = "scheduledAction"
)

// RouteMatcher returns a function that matches Redshift Serverless requests.
func (h *ServerlessHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/redshift-serverless/")
	}
}

// MatchPriority returns the routing priority.
func (h *ServerlessHandler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
//
//nolint:gocognit,cyclop,gocyclo,funlen // large dispatch switch is inherently complex for serverless routing
func (h *ServerlessHandler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch {
	case strings.HasPrefix(path, slPrefix) && method == http.MethodPost && path == slPrefix:
		return "CreateNamespace"
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodGet &&
		!strings.Contains(strings.TrimPrefix(path, slPrefix+"/"), "/"):
		return "GetNamespace"
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodDelete:
		return "DeleteNamespace"
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodPatch:
		return "UpdateNamespace"
	case path == slPrefix && method == http.MethodGet:
		return "ListNamespaces"
	case path == slWorkgroupsPrefix && method == http.MethodPost:
		return "CreateWorkgroup"
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") &&
		method == http.MethodGet && strings.HasSuffix(path, "/credentials"):
		return "GetCredentials"
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodGet:
		return "GetWorkgroup"
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodDelete:
		return "DeleteWorkgroup"
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodPatch:
		return "UpdateWorkgroup"
	case path == slWorkgroupsPrefix && method == http.MethodGet:
		return "ListWorkgroups"
	case path == slSnapshotsPrefix && method == http.MethodPost:
		return "CreateSnapshot"
	case path == slSnapshotsPrefix && method == http.MethodGet:
		return "ListSnapshots"
	case strings.HasPrefix(path, slSnapshotsPrefix+"/") && method == http.MethodGet:
		return "GetSnapshot"
	case strings.HasPrefix(path, slSnapshotsPrefix+"/") && method == http.MethodDelete:
		return "DeleteSnapshot"
	case path == slUsageLimitsPrefix && method == http.MethodPost:
		return "CreateUsageLimit"
	case path == slUsageLimitsPrefix && method == http.MethodGet:
		return "ListUsageLimits"
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodGet:
		return "GetUsageLimit"
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodPatch:
		return "UpdateUsageLimit"
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodDelete:
		return "DeleteUsageLimit"
	case path == slScheduledActionsPrefix && method == http.MethodPost:
		return "CreateScheduledAction"
	case path == slScheduledActionsPrefix && method == http.MethodGet:
		return "ListScheduledActions"
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodGet:
		return "GetScheduledAction"
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodPatch:
		return "UpdateScheduledAction"
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodDelete:
		return "DeleteScheduledAction"
	}

	return opUnknown
}

// ExtractResource extracts a resource identifier from the request.
func (h *ServerlessHandler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *ServerlessHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method
		log := logger.Load(r.Context())

		var body []byte
		if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
			var err error

			body, err = httputils.ReadBody(r)
			if err != nil {
				log.ErrorContext(
					r.Context(),
					"redshift-serverless: failed to read body",
					"error",
					err,
				)

				return c.JSON(
					http.StatusInternalServerError,
					slErrorResponse("InternalFailure", "internal server error"),
				)
			}
		}

		return h.dispatch(c, path, method, body)
	}
}

//nolint:cyclop,funlen,gocognit,gocyclo // large dispatch table for serverless routing is inherently complex
func (h *ServerlessHandler) dispatch(
	c *echo.Context,
	path, method string,
	body []byte,
) error { //nolint:cyclop,funlen,gocognit,gocyclo // large dispatch table for serverless routing is inherently complex
	// Namespace routes
	switch {
	case path == slPrefix && method == http.MethodPost:
		return h.handleCreateNamespace(c, body)
	case path == slPrefix && method == http.MethodGet:
		return h.handleListNamespaces(c)
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodGet:
		name := strings.TrimPrefix(path, slPrefix+"/")

		return h.handleGetNamespace(c, name)
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodDelete:
		name := strings.TrimPrefix(path, slPrefix+"/")

		return h.handleDeleteNamespace(c, name)
	case strings.HasPrefix(path, slPrefix+"/") && method == http.MethodPatch:
		name := strings.TrimPrefix(path, slPrefix+"/")

		return h.handleUpdateNamespace(c, name, body)

	// Workgroup routes
	case path == slWorkgroupsPrefix && method == http.MethodPost:
		return h.handleCreateWorkgroup(c, body)
	case path == slWorkgroupsPrefix && method == http.MethodGet:
		return h.handleListWorkgroups(c)
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") &&
		strings.HasSuffix(path, "/credentials") && method == http.MethodGet:
		rest := strings.TrimPrefix(path, slWorkgroupsPrefix+"/")
		name := strings.TrimSuffix(rest, "/credentials")

		return h.handleGetCredentials(c, name)
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodGet:
		name := strings.TrimPrefix(path, slWorkgroupsPrefix+"/")

		return h.handleGetWorkgroup(c, name)
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodDelete:
		name := strings.TrimPrefix(path, slWorkgroupsPrefix+"/")

		return h.handleDeleteWorkgroup(c, name)
	case strings.HasPrefix(path, slWorkgroupsPrefix+"/") && method == http.MethodPatch:
		name := strings.TrimPrefix(path, slWorkgroupsPrefix+"/")

		return h.handleUpdateWorkgroup(c, name, body)

	// Snapshot routes
	case path == slSnapshotsPrefix && method == http.MethodPost:
		return h.handleCreateSnapshot(c, body)
	case path == slSnapshotsPrefix && method == http.MethodGet:
		return h.handleListSnapshots(c)
	case strings.HasPrefix(path, slSnapshotsPrefix+"/") && method == http.MethodGet:
		name := strings.TrimPrefix(path, slSnapshotsPrefix+"/")

		return h.handleGetSnapshot(c, name)
	case strings.HasPrefix(path, slSnapshotsPrefix+"/") && method == http.MethodDelete:
		name := strings.TrimPrefix(path, slSnapshotsPrefix+"/")

		return h.handleDeleteSnapshot(c, name)

	// Usage limit routes
	case path == slUsageLimitsPrefix && method == http.MethodPost:
		return h.handleCreateUsageLimit(c, body)
	case path == slUsageLimitsPrefix && method == http.MethodGet:
		return h.handleListUsageLimits(c)
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodGet:
		id := strings.TrimPrefix(path, slUsageLimitsPrefix+"/")

		return h.handleGetUsageLimit(c, id)
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodPatch:
		id := strings.TrimPrefix(path, slUsageLimitsPrefix+"/")

		return h.handleUpdateUsageLimit(c, id, body)
	case strings.HasPrefix(path, slUsageLimitsPrefix+"/") && method == http.MethodDelete:
		id := strings.TrimPrefix(path, slUsageLimitsPrefix+"/")

		return h.handleDeleteUsageLimit(c, id)

	// Scheduled action routes
	case path == slScheduledActionsPrefix && method == http.MethodPost:
		return h.handleCreateScheduledAction(c, body)
	case path == slScheduledActionsPrefix && method == http.MethodGet:
		return h.handleListScheduledActions(c)
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodGet:
		name := strings.TrimPrefix(path, slScheduledActionsPrefix+"/")

		return h.handleGetScheduledAction(c, name)
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodPatch:
		name := strings.TrimPrefix(path, slScheduledActionsPrefix+"/")

		return h.handleUpdateScheduledAction(c, name, body)
	case strings.HasPrefix(path, slScheduledActionsPrefix+"/") && method == http.MethodDelete:
		name := strings.TrimPrefix(path, slScheduledActionsPrefix+"/")

		return h.handleDeleteScheduledAction(c, name)

	default:

		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("UnknownOperationException", "unknown operation: "+path),
		)
	}
}

// ---------------------------------------------------------------------------
// Namespace handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateNamespace(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName string   `json:"namespaceName"`
		AdminUsername string   `json:"adminUsername"`
		DBName        string   `json:"dbName"`
		KmsKeyID      string   `json:"kmsKeyId"`
		IamRoles      []string `json:"iamRoles"`
		LogExports    []string `json:"logExports"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	ns, err := h.Backend.CreateNamespace(
		req.NamespaceName,
		req.AdminUsername,
		req.DBName,
		req.KmsKeyID,
		req.IamRoles,
		req.LogExports,
	)
	if err != nil {
		if errors.Is(err, ErrNamespaceAlreadyExists) {
			return c.JSON(http.StatusConflict, slErrorResponse("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, slErrorResponse("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleGetNamespace(c *echo.Context, name string) error {
	ns, err := h.Backend.GetNamespace(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleListNamespaces(c *echo.Context) error {
	maxResults := 0
	nextToken := c.QueryParam("nextToken")
	list, outToken := h.Backend.ListNamespaces(maxResults, nextToken)
	resp := map[string]any{"namespaces": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateNamespace(c *echo.Context, name string, body []byte) error {
	var req struct {
		AdminUsername string   `json:"adminUsername"`
		DBName        string   `json:"dbName"`
		KmsKeyID      string   `json:"kmsKeyId"`
		IamRoles      []string `json:"iamRoles"`
		LogExports    []string `json:"logExports"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	ns, err := h.Backend.UpdateNamespace(
		name,
		req.AdminUsername,
		req.DBName,
		req.KmsKeyID,
		req.IamRoles,
		req.LogExports,
	)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

func (h *ServerlessHandler) handleDeleteNamespace(c *echo.Context, name string) error {
	ns, err := h.Backend.DeleteNamespace(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespNamespace: ns})
}

// ---------------------------------------------------------------------------
// Workgroup handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateWorkgroup(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupName    string   `json:"workgroupName"`
		NamespaceName    string   `json:"namespaceName"`
		SubnetIDs        []string `json:"subnetIds"`
		SecurityGroupIDs []string `json:"securityGroupIds"`
		BaseCapacity     int      `json:"baseCapacity"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	wg, err := h.Backend.CreateWorkgroup(
		req.WorkgroupName,
		req.NamespaceName,
		req.BaseCapacity,
		req.SubnetIDs,
		req.SecurityGroupIDs,
	)
	if err != nil {
		if errors.Is(err, ErrWorkgroupAlreadyExists) {
			return c.JSON(http.StatusConflict, slErrorResponse("ConflictException", err.Error()))
		}

		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleGetWorkgroup(c *echo.Context, name string) error {
	wg, err := h.Backend.GetWorkgroup(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleListWorkgroups(c *echo.Context) error {
	maxResults := 0
	nextToken := c.QueryParam("nextToken")
	list, outToken := h.Backend.ListWorkgroups(maxResults, nextToken)
	resp := map[string]any{"workgroups": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateWorkgroup(c *echo.Context, name string, body []byte) error {
	var req struct {
		SubnetIDs        []string `json:"subnetIds"`
		SecurityGroupIDs []string `json:"securityGroupIds"`
		BaseCapacity     int      `json:"baseCapacity"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	wg, err := h.Backend.UpdateWorkgroup(
		name,
		req.BaseCapacity,
		req.SubnetIDs,
		req.SecurityGroupIDs,
	)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleDeleteWorkgroup(c *echo.Context, name string) error {
	wg, err := h.Backend.DeleteWorkgroup(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespWorkgroup: wg})
}

func (h *ServerlessHandler) handleGetCredentials(c *echo.Context, workgroupName string) error {
	dbName := c.QueryParam("dbName")
	accessKeyID, secretKey, expiry, err := h.Backend.GetCredentials(workgroupName, dbName)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dbPassword": secretKey,
		"dbUser":     accessKeyID, // backend already includes "IAMR:" prefix
		"expiration": expiry,
	})
}

// ---------------------------------------------------------------------------
// Snapshot handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateSnapshot(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotName  string `json:"snapshotName"`
		NamespaceName string `json:"namespaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	snap, err := h.Backend.CreateServerlessSnapshot(req.SnapshotName, req.NamespaceName)
	if err != nil {
		if errors.Is(err, ErrNamespaceNotFound) {
			return c.JSON(
				http.StatusNotFound,
				slErrorResponse("ResourceNotFoundException", err.Error()),
			)
		}

		return c.JSON(http.StatusConflict, slErrorResponse("ConflictException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

func (h *ServerlessHandler) handleGetSnapshot(c *echo.Context, name string) error {
	snap, err := h.Backend.GetServerlessSnapshot(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

func (h *ServerlessHandler) handleListSnapshots(c *echo.Context) error {
	namespaceName := c.QueryParam("namespaceName")
	nextToken := c.QueryParam("nextToken")
	list, outToken := h.Backend.ListServerlessSnapshots(namespaceName, 0, nextToken)
	resp := map[string]any{"snapshots": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleDeleteSnapshot(c *echo.Context, name string) error {
	snap, err := h.Backend.DeleteServerlessSnapshot(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshot: snap})
}

// ---------------------------------------------------------------------------
// Usage limit handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateUsageLimit(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn  string `json:"resourceArn"`
		UsageType    string `json:"usageType"`
		Period       string `json:"period"`
		BreachAction string `json:"breachAction"`
		Amount       int64  `json:"amount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	ul, err := h.Backend.CreateServerlessUsageLimit(
		req.ResourceArn,
		req.UsageType,
		req.Period,
		req.BreachAction,
		req.Amount,
	)
	if err != nil {
		return c.JSON(http.StatusBadRequest, slErrorResponse("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleGetUsageLimit(c *echo.Context, id string) error {
	ul, err := h.Backend.GetServerlessUsageLimit(id)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleListUsageLimits(c *echo.Context) error {
	resourceArn := c.QueryParam("resourceArn")
	nextToken := c.QueryParam("nextToken")
	list, outToken := h.Backend.ListServerlessUsageLimits(resourceArn, 0, nextToken)
	resp := map[string]any{"usageLimits": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateUsageLimit(c *echo.Context, id string, body []byte) error {
	var req struct {
		BreachAction string `json:"breachAction"`
		Amount       int64  `json:"amount"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	ul, err := h.Backend.UpdateServerlessUsageLimit(id, req.BreachAction, req.Amount)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

func (h *ServerlessHandler) handleDeleteUsageLimit(c *echo.Context, id string) error {
	ul, err := h.Backend.DeleteServerlessUsageLimit(id)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespUsageLimit: ul})
}

// ---------------------------------------------------------------------------
// Scheduled action handlers
// ---------------------------------------------------------------------------

func (h *ServerlessHandler) handleCreateScheduledAction(c *echo.Context, body []byte) error {
	var req struct {
		ScheduledActionName string `json:"scheduledActionName"`
		NamespaceName       string `json:"namespaceName"`
		Schedule            string `json:"schedule"`
		TargetAction        string `json:"targetAction"`
		StartTime           string `json:"startTime"`
		EndTime             string `json:"endTime"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	var startTime, endTime time.Time

	if req.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, req.StartTime); err == nil {
			startTime = t
		}
	}

	if req.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, req.EndTime); err == nil {
			endTime = t
		}
	}

	sa, err := h.Backend.CreateServerlessScheduledAction(
		req.ScheduledActionName,
		req.NamespaceName,
		req.Schedule,
		req.TargetAction,
		startTime,
		endTime,
	)
	if err != nil {
		return c.JSON(http.StatusConflict, slErrorResponse("ConflictException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: sa})
}

func (h *ServerlessHandler) handleGetScheduledAction(c *echo.Context, name string) error {
	sa, err := h.Backend.GetServerlessScheduledAction(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: sa})
}

func (h *ServerlessHandler) handleListScheduledActions(c *echo.Context) error {
	namespaceName := c.QueryParam("namespaceName")
	nextToken := c.QueryParam("nextToken")
	list, outToken := h.Backend.ListServerlessScheduledActions(namespaceName, 0, nextToken)
	resp := map[string]any{"scheduledActions": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateScheduledAction(
	c *echo.Context,
	name string,
	body []byte,
) error {
	var req struct {
		Schedule     string `json:"schedule"`
		TargetAction string `json:"targetAction"`
		StartTime    string `json:"startTime"`
		EndTime      string `json:"endTime"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			slErrorResponse("ValidationException", "invalid request body"),
		)
	}

	var startTime, endTime time.Time

	if req.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, req.StartTime); err == nil {
			startTime = t
		}
	}

	if req.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, req.EndTime); err == nil {
			endTime = t
		}
	}

	sa, err := h.Backend.UpdateServerlessScheduledAction(
		name,
		req.Schedule,
		req.TargetAction,
		startTime,
		endTime,
	)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: sa})
}

func (h *ServerlessHandler) handleDeleteScheduledAction(c *echo.Context, name string) error {
	sa, err := h.Backend.DeleteServerlessScheduledAction(name)
	if err != nil {
		return c.JSON(
			http.StatusNotFound,
			slErrorResponse("ResourceNotFoundException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespScheduledAction: sa})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func slErrorResponse(code, msg string) map[string]any {
	return map[string]any{
		"message": msg,
		"code":    code,
	}
}
