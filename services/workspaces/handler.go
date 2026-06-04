package workspaces

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityHeaderExact
	targetPrefix  = "WorkspacesService."

	opCreateWorkspaces                   = "CreateWorkspaces"
	opDescribeWorkspaces                 = "DescribeWorkspaces"
	opDescribeWorkspacesConnectionStatus = "DescribeWorkspacesConnectionStatus"
	opModifyWorkspaceProperties          = "ModifyWorkspaceProperties"
	opModifyWorkspaceState               = "ModifyWorkspaceState"
	opRebootWorkspaces                   = "RebootWorkspaces"
	opRebuildWorkspaces                  = "RebuildWorkspaces"
	opStartWorkspaces                    = "StartWorkspaces"
	opStopWorkspaces                     = "StopWorkspaces"
	opTerminateWorkspaces                = "TerminateWorkspaces"
	opCreateTags                         = "CreateTags"
	opDeleteTags                         = "DeleteTags"
	opDescribeTags                       = "DescribeTags"
	opDescribeWorkspaceBundles           = "DescribeWorkspaceBundles"
	opDescribeWorkspaceDirectories       = "DescribeWorkspaceDirectories"
)

// Handler handles WorkSpaces HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "WorkSpaces" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateWorkspaces,
		opDescribeWorkspaces,
		opDescribeWorkspacesConnectionStatus,
		opModifyWorkspaceProperties,
		opModifyWorkspaceState,
		opRebootWorkspaces,
		opRebuildWorkspaces,
		opStartWorkspaces,
		opStopWorkspaces,
		opTerminateWorkspaces,
		opCreateTags,
		opDeleteTags,
		opDescribeTags,
		opDescribeWorkspaceBundles,
		opDescribeWorkspaceDirectories,
	}
}

// RouteMatcher returns a matcher that accepts WorkSpaces target header requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, targetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "failed to read body"))
		}

		op := h.ExtractOperation(c)

		switch op {
		case opCreateWorkspaces:
			return h.handleCreateWorkspaces(c, body)
		case opDescribeWorkspaces:
			return h.handleDescribeWorkspaces(c, body)
		case opDescribeWorkspacesConnectionStatus:
			return h.handleDescribeWorkspacesConnectionStatus(c, body)
		case opModifyWorkspaceProperties:
			return h.handleModifyWorkspaceProperties(c, body)
		case opModifyWorkspaceState:
			return h.handleModifyWorkspaceState(c, body)
		case opRebootWorkspaces:
			return h.handleRebootWorkspaces(c, body)
		case opRebuildWorkspaces:
			return h.handleRebuildWorkspaces(c, body)
		case opStartWorkspaces:
			return h.handleStartWorkspaces(c, body)
		case opStopWorkspaces:
			return h.handleStopWorkspaces(c, body)
		case opTerminateWorkspaces:
			return h.handleTerminateWorkspaces(c, body)
		case opCreateTags:
			return h.handleCreateTags(c, body)
		case opDeleteTags:
			return h.handleDeleteTags(c, body)
		case opDescribeTags:
			return h.handleDescribeTags(c, body)
		case opDescribeWorkspaceBundles:
			return h.handleDescribeWorkspaceBundles(c, body)
		case opDescribeWorkspaceDirectories:
			return h.handleDescribeWorkspaceDirectories(c, body)
		default:
			log.Debug("workspaces: unknown operation", "op", op)

			return c.JSON(http.StatusNotImplemented, errBody("NotImplementedException", "operation not implemented"))
		}
	}
}

// tagItem represents a key/value tag pair in AWS API requests.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// createWorkspaceRequest is a single workspace spec within CreateWorkspaces.
type createWorkspaceRequest struct {
	Tags        []tagItem `json:"Tags"`
	UserName    string    `json:"UserName"`
	DirectoryId string    `json:"DirectoryId"`
	BundleId    string    `json:"BundleId"`
}

// workspaceIDRequest is used for bulk operations that take a WorkspaceId field.
type workspaceIDRequest struct {
	WorkspaceId string `json:"WorkspaceId"`
}

// workspacePropertiesResp is the JSON shape for WorkspaceProperties in responses.
type workspacePropertiesResp struct {
	ComputeTypeName                     string `json:"ComputeTypeName,omitempty"`
	RunningMode                         string `json:"RunningMode,omitempty"`
	RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib,omitempty"`
	RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes,omitempty"`
	UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib,omitempty"`
}

func (h *Handler) handleCreateWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		Workspaces []createWorkspaceRequest `json:"Workspaces"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	type pendingWorkspace struct {
		WorkspaceId string `json:"WorkspaceId"`
		DirectoryId string `json:"DirectoryId"`
		UserName    string `json:"UserName"`
		BundleId    string `json:"BundleId"`
		State       string `json:"State"`
	}

	pending := make([]pendingWorkspace, 0, len(req.Workspaces))

	for _, spec := range req.Workspaces {
		tags := make(map[string]string, len(spec.Tags))
		for _, t := range spec.Tags {
			tags[t.Key] = t.Value
		}

		ws, err := h.Backend.CreateWorkspace(spec.UserName, spec.DirectoryId, spec.BundleId, tags)
		if err != nil {
			return h.mapError(c, err)
		}

		pending = append(pending, pendingWorkspace{
			WorkspaceId: ws.WorkspaceID,
			DirectoryId: ws.DirectoryID,
			UserName:    ws.UserName,
			BundleId:    ws.BundleID,
			State:       ws.State,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests":  []any{},
		"PendingRequests": pending,
	})
}

func (h *Handler) handleDescribeWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		WorkspaceIds []string `json:"WorkspaceIds"`
		DirectoryId  string   `json:"DirectoryId"`
		UserName     string   `json:"UserName"`
		BundleId     string   `json:"BundleId"`
		Limit        int32    `json:"Limit"`
		NextToken    string   `json:"NextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
		}
	}

	var directoryIDs, userIDs, bundleIDs []string
	if req.DirectoryId != "" {
		directoryIDs = []string{req.DirectoryId}
	}

	if req.UserName != "" {
		userIDs = []string{req.UserName}
	}

	if req.BundleId != "" {
		bundleIDs = []string{req.BundleId}
	}

	workspaces, nextToken, err := h.Backend.DescribeWorkspaces(
		req.WorkspaceIds, directoryIDs, userIDs, bundleIDs, req.Limit, req.NextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	type workspaceResp struct {
		WorkspaceProperties *workspacePropertiesResp `json:"WorkspaceProperties,omitempty"`
		Tags                map[string]string        `json:"Tags,omitempty"`
		WorkspaceId         string                   `json:"WorkspaceId"`
		DirectoryId         string                   `json:"DirectoryId"`
		UserName            string                   `json:"UserName"`
		BundleId            string                   `json:"BundleId"`
		State               string                   `json:"State"`
	}

	items := make([]workspaceResp, 0, len(workspaces))
	for _, ws := range workspaces {
		item := workspaceResp{
			WorkspaceId: ws.WorkspaceID,
			DirectoryId: ws.DirectoryID,
			UserName:    ws.UserName,
			BundleId:    ws.BundleID,
			State:       ws.State,
			Tags:        ws.Tags,
		}

		if ws.Properties != nil {
			item.WorkspaceProperties = &workspacePropertiesResp{
				ComputeTypeName:                     ws.Properties.ComputeTypeName,
				RunningMode:                         ws.Properties.RunningMode,
				RootVolumeSizeGib:                   ws.Properties.RootVolumeSizeGib,
				RunningModeAutoStopTimeoutInMinutes:  ws.Properties.RunningModeAutoStopTimeoutInMinutes,
				UserVolumeSizeGib:                   ws.Properties.UserVolumeSizeGib,
			}
		}

		items = append(items, item)
	}

	resp := map[string]any{
		"Workspaces": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeWorkspacesConnectionStatus(c *echo.Context, body []byte) error {
	var req struct {
		WorkspaceIds []string `json:"WorkspaceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
		}
	}

	statuses, err := h.Backend.GetWorkspacesConnectionStatus(req.WorkspaceIds)
	if err != nil {
		return h.mapError(c, err)
	}

	type connStatusResp struct {
		WorkspaceId     string `json:"WorkspaceId"`
		ConnectionState string `json:"ConnectionState"`
	}

	items := make([]connStatusResp, 0, len(statuses))
	for _, s := range statuses {
		items = append(items, connStatusResp{
			WorkspaceId:     s.WorkspaceID,
			ConnectionState: s.ConnectionState,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"WorkspacesConnectionStatus": items,
	})
}

func (h *Handler) handleModifyWorkspaceProperties(c *echo.Context, body []byte) error {
	var req struct {
		WorkspaceProperties struct {
			ComputeTypeName                     string `json:"ComputeTypeName"`
			RunningMode                         string `json:"RunningMode"`
			RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib"`
			RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes"`
			UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib"`
		} `json:"WorkspaceProperties"`
		WorkspaceId string `json:"WorkspaceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	props := WorkspaceProperties{
		ComputeTypeName:                     req.WorkspaceProperties.ComputeTypeName,
		RunningMode:                         req.WorkspaceProperties.RunningMode,
		RootVolumeSizeGib:                   req.WorkspaceProperties.RootVolumeSizeGib,
		RunningModeAutoStopTimeoutInMinutes: req.WorkspaceProperties.RunningModeAutoStopTimeoutInMinutes,
		UserVolumeSizeGib:                   req.WorkspaceProperties.UserVolumeSizeGib,
	}

	if err := h.Backend.ModifyWorkspaceProperties(req.WorkspaceId, props); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleModifyWorkspaceState(c *echo.Context, body []byte) error {
	var req struct {
		WorkspaceId    string `json:"WorkspaceId"`
		WorkspaceState string `json:"WorkspaceState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	if err := h.Backend.ModifyWorkspaceState(req.WorkspaceId, req.WorkspaceState); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRebootWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		RebootWorkspaceRequests []workspaceIDRequest `json:"RebootWorkspaceRequests"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	ids := extractWorkspaceIDs(req.RebootWorkspaceRequests)

	failures, err := h.Backend.RebootWorkspaces(ids)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests": marshalFailedRequests(failures),
	})
}

func (h *Handler) handleRebuildWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		RebuildWorkspaceRequests []workspaceIDRequest `json:"RebuildWorkspaceRequests"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	ids := extractWorkspaceIDs(req.RebuildWorkspaceRequests)

	failures, err := h.Backend.RebuildWorkspaces(ids)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests": marshalFailedRequests(failures),
	})
}

func (h *Handler) handleStartWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		StartWorkspaceRequests []workspaceIDRequest `json:"StartWorkspaceRequests"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	ids := extractWorkspaceIDs(req.StartWorkspaceRequests)

	failures, err := h.Backend.StartWorkspaces(ids)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests": marshalFailedRequests(failures),
	})
}

func (h *Handler) handleStopWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		StopWorkspaceRequests []workspaceIDRequest `json:"StopWorkspaceRequests"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	ids := extractWorkspaceIDs(req.StopWorkspaceRequests)

	failures, err := h.Backend.StopWorkspaces(ids)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests": marshalFailedRequests(failures),
	})
}

func (h *Handler) handleTerminateWorkspaces(c *echo.Context, body []byte) error {
	var req struct {
		TerminateWorkspaceRequests []workspaceIDRequest `json:"TerminateWorkspaceRequests"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	ids := extractWorkspaceIDs(req.TerminateWorkspaceRequests)

	failures, err := h.Backend.TerminateWorkspaces(ids)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FailedRequests": marshalFailedRequests(failures),
	})
}

func (h *Handler) handleCreateTags(c *echo.Context, body []byte) error {
	var req struct {
		Tags       []tagItem `json:"Tags"`
		ResourceId string    `json:"ResourceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.CreateTags(req.ResourceId, tags); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteTags(c *echo.Context, body []byte) error {
	var req struct {
		TagKeys    []string `json:"TagKeys"`
		ResourceId string   `json:"ResourceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	if err := h.Backend.DeleteTags(req.ResourceId, req.TagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeTags(c *echo.Context, body []byte) error {
	var req struct {
		ResourceId string `json:"ResourceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
	}

	tags, err := h.Backend.DescribeTags(req.ResourceId)
	if err != nil {
		return h.mapError(c, err)
	}

	items := make([]tagItem, 0, len(tags))
	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TagList": items,
	})
}

func (h *Handler) handleDescribeWorkspaceBundles(c *echo.Context, body []byte) error {
	var req struct {
		BundleIds []string `json:"BundleIds"`
		Owner     string   `json:"Owner"`
		NextToken string   `json:"NextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
		}
	}

	bundles, nextToken, err := h.Backend.DescribeWorkspaceBundles(req.BundleIds, req.Owner, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	type bundleResp struct {
		BundleId    string `json:"BundleId"`
		Name        string `json:"Name"`
		Owner       string `json:"Owner"`
		Description string `json:"Description"`
	}

	items := make([]bundleResp, 0, len(bundles))
	for _, bun := range bundles {
		items = append(items, bundleResp{
			BundleId:    bun.BundleID,
			Name:        bun.Name,
			Owner:       bun.Owner,
			Description: bun.Description,
		})
	}

	resp := map[string]any{
		"Bundles": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeWorkspaceDirectories(c *echo.Context, body []byte) error {
	var req struct {
		DirectoryIds []string `json:"DirectoryIds"`
		NextToken    string   `json:"NextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, "invalid request body"))
		}
	}

	dirs, nextToken, err := h.Backend.DescribeWorkspaceDirectories(req.DirectoryIds, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	type dirResp struct {
		DirectoryId   string `json:"DirectoryId"`
		DirectoryName string `json:"DirectoryName"`
		DirectoryType string `json:"DirectoryType"`
		Alias         string `json:"Alias"`
		State         string `json:"State"`
	}

	items := make([]dirResp, 0, len(dirs))
	for _, d := range dirs {
		items = append(items, dirResp{
			DirectoryId:   d.DirectoryID,
			DirectoryName: d.DirectoryName,
			DirectoryType: d.DirectoryType,
			Alias:         d.Alias,
			State:         d.State,
		})
	}

	resp := map[string]any{
		"Directories": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) mapError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errBody(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errBody("InternalServerException", err.Error()))
	}
}

func errBody(code, msg string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": msg,
	}
}

// extractWorkspaceIDs pulls WorkspaceId strings from a slice of workspaceIDRequest.
func extractWorkspaceIDs(reqs []workspaceIDRequest) []string {
	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		ids = append(ids, r.WorkspaceId)
	}

	return ids
}

// failedRequestItem is the JSON representation of a FailedRequest.
type failedRequestItem struct {
	WorkspaceId  string `json:"WorkspaceId"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// marshalFailedRequests converts backend FailedRequests to JSON-serializable items.
func marshalFailedRequests(failures []FailedRequest) []failedRequestItem {
	if len(failures) == 0 {
		return []failedRequestItem{}
	}

	items := make([]failedRequestItem, 0, len(failures))
	for _, f := range failures {
		items = append(items, failedRequestItem{
			WorkspaceId:  f.WorkspaceID,
			ErrorCode:    f.ErrorCode,
			ErrorMessage: f.ErrorMessage,
		})
	}

	return items
}
