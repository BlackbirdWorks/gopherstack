package efs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	efsMatchPriority = service.PriorityPathVersioned

	pathFileSystems  = "/2015-02-01/file-systems"
	pathMountTargets = "/2015-02-01/mount-targets"
	pathAccessPoints = "/2015-02-01/access-points"
	pathTags         = "/2015-02-01/tags"

	// subresourcePathParts is the number of segments when splitting a path with a sub-resource.
	subresourcePathParts = 2
)

// Handler is the Echo HTTP handler for AWS EFS operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new EFS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "EFS" }

// GetSupportedOperations returns the list of supported EFS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateFileSystem",
		"DescribeFileSystems",
		"DeleteFileSystem",
		"CreateMountTarget",
		"DescribeMountTargets",
		"DeleteMountTarget",
		"CreateAccessPoint",
		"DescribeAccessPoints",
		"DeleteAccessPoint",
		"TagResource",
		"ListTagsForResource",
		"DescribeLifecycleConfiguration",
		"PutLifecycleConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "efs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this EFS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS EFS REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == pathFileSystems ||
			strings.HasPrefix(path, pathFileSystems+"/") ||
			path == pathMountTargets ||
			strings.HasPrefix(path, pathMountTargets+"/") ||
			path == pathAccessPoints ||
			strings.HasPrefix(path, pathAccessPoints+"/") ||
			strings.HasPrefix(path, pathTags+"/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return efsMatchPriority }

// efsRoute holds parsed information from an EFS REST request path.
type efsRoute struct {
	resource  string
	operation string
}

// parseEFSPath maps HTTP method + path to an operation name and resource ID.
func parseEFSPath(method, rawPath string) efsRoute {
	path, _ := url.PathUnescape(rawPath)

	switch {
	case strings.HasPrefix(path, pathFileSystems):
		return parseFileSystemRoute(method, strings.TrimPrefix(path, pathFileSystems))
	case strings.HasPrefix(path, pathMountTargets):
		return parseMountTargetRoute(method, strings.TrimPrefix(path, pathMountTargets))
	case strings.HasPrefix(path, pathAccessPoints):
		return parseAccessPointRoute(method, strings.TrimPrefix(path, pathAccessPoints))
	case strings.HasPrefix(path, pathTags+"/"):
		return parseTagsRoute(method, strings.TrimPrefix(path, pathTags+"/"))
	}

	return efsRoute{operation: "Unknown"}
}

func parseFileSystemRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		switch method {
		case http.MethodPost:
			return efsRoute{operation: "CreateFileSystem"}
		case http.MethodGet:
			return efsRoute{operation: "DescribeFileSystems"}
		}
	case !strings.Contains(id, "/"):
		switch method {
		case http.MethodGet:
			return efsRoute{operation: "DescribeFileSystems", resource: id}
		case http.MethodDelete:
			return efsRoute{operation: "DeleteFileSystem", resource: id}
		}
	default:
		// Sub-resource paths: /{fileSystemId}/{subresource}
		parts := strings.SplitN(id, "/", subresourcePathParts)
		if len(parts) < subresourcePathParts {
			break
		}

		fsID := parts[0]
		sub := parts[1]

		switch {
		case sub == "lifecycle-configuration" && method == http.MethodGet:
			return efsRoute{operation: "DescribeLifecycleConfiguration", resource: fsID}
		case sub == "lifecycle-configuration" && method == http.MethodPut:
			return efsRoute{operation: "PutLifecycleConfiguration", resource: fsID}
		}
	}

	return efsRoute{operation: "Unknown"}
}

func parseMountTargetRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		switch method {
		case http.MethodPost:
			return efsRoute{operation: "CreateMountTarget"}
		case http.MethodGet:
			return efsRoute{operation: "DescribeMountTargets"}
		}
	} else if !strings.Contains(id, "/") {
		switch method {
		case http.MethodGet:
			return efsRoute{operation: "DescribeMountTargets", resource: id}
		case http.MethodDelete:
			return efsRoute{operation: "DeleteMountTarget", resource: id}
		}
	}

	return efsRoute{operation: "Unknown"}
}

func parseAccessPointRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		switch method {
		case http.MethodPost:
			return efsRoute{operation: "CreateAccessPoint"}
		case http.MethodGet:
			return efsRoute{operation: "DescribeAccessPoints"}
		}
	} else if !strings.Contains(id, "/") {
		switch method {
		case http.MethodGet:
			return efsRoute{operation: "DescribeAccessPoints", resource: id}
		case http.MethodDelete:
			return efsRoute{operation: "DeleteAccessPoint", resource: id}
		}
	}

	return efsRoute{operation: "Unknown"}
}

func parseTagsRoute(method, resourceID string) efsRoute {
	switch method {
	case http.MethodPost:
		return efsRoute{operation: "TagResource", resource: resourceID}
	case http.MethodGet:
		return efsRoute{operation: "ListTagsForResource", resource: resourceID}
	}

	return efsRoute{operation: "Unknown"}
}

// ExtractOperation extracts the EFS operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseEFSPath(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := parseEFSPath(c.Request().Method, c.Request().URL.Path)

	return r.resource
}

// Handler returns the Echo handler function for EFS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseEFSPath(c.Request().Method, c.Request().URL.Path)

		log.Debug("efs request", "operation", route.operation, "resource", route.resource)

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

func (h *Handler) dispatch(c *echo.Context, route efsRoute, body []byte) error {
	switch route.operation {
	case "CreateFileSystem":
		return h.handleCreateFileSystem(c, body)
	case "DescribeFileSystems":
		return h.handleDescribeFileSystems(c, route.resource)
	case "DeleteFileSystem":
		return h.handleDeleteFileSystem(c, route.resource)
	case "CreateMountTarget":
		return h.handleCreateMountTarget(c, body)
	case "DescribeMountTargets":
		return h.handleDescribeMountTargets(c, route.resource)
	case "DeleteMountTarget":
		return h.handleDeleteMountTarget(c, route.resource)
	case "CreateAccessPoint":
		return h.handleCreateAccessPoint(c, body)
	case "DescribeAccessPoints":
		return h.handleDescribeAccessPoints(c, route.resource)
	case "DeleteAccessPoint":
		return h.handleDeleteAccessPoint(c, route.resource)
	case "TagResource":
		return h.handleTagResource(c, route.resource, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resource)
	case "DescribeLifecycleConfiguration":
		return h.handleDescribeLifecycleConfiguration(c, route.resource)
	case "PutLifecycleConfiguration":
		return h.handlePutLifecycleConfiguration(c, route.resource, body)
	default:
		return c.JSON(http.StatusNotFound, errResp("UnsupportedOperation", "unknown operation: "+route.operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("FileSystemNotFound", err.Error()))
	case errors.Is(err, ErrMountTargetNotFound):
		return c.JSON(http.StatusNotFound, errResp("MountTargetNotFound", err.Error()))
	case errors.Is(err, ErrAccessPointNotFound):
		return c.JSON(http.StatusNotFound, errResp("AccessPointNotFound", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("FileSystemAlreadyExists", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalServerError", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"ErrorCode": code, "Message": msg}
}

// --- FileSystem handlers ---

type createFileSystemBody struct {
	CreationToken   string     `json:"CreationToken"`
	PerformanceMode string     `json:"PerformanceMode"`
	ThroughputMode  string     `json:"ThroughputMode"`
	Tags            []tagEntry `json:"Tags"`
	Encrypted       bool       `json:"Encrypted"`
}

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsFromEntries(entries []tagEntry) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

func tagsToEntries(m map[string]string) []tagEntry {
	entries := make([]tagEntry, 0, len(m))
	for k, v := range m {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	return entries
}

func (h *Handler) handleCreateFileSystem(c *echo.Context, body []byte) error {
	var in createFileSystemBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.CreationToken == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "CreationToken is required"))
	}

	kv := tagsFromEntries(in.Tags)
	fs, err := h.Backend.CreateFileSystem(in.CreationToken, in.PerformanceMode, in.ThroughputMode, in.Encrypted, kv)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			// EFS returns 409 with the existing file system description.
			return c.JSON(http.StatusConflict, fsToResponse(fs))
		}

		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, fsToResponse(fs))
}

func (h *Handler) handleDescribeFileSystems(c *echo.Context, fileSystemID string) error {
	// Also accept ?FileSystemId= query param.
	if fileSystemID == "" {
		fileSystemID = c.Request().URL.Query().Get("FileSystemId")
	}

	fsList, err := h.Backend.DescribeFileSystems(fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(fsList))
	for _, fs := range fsList {
		items = append(items, fsToResponse(fs))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FileSystems": items,
	})
}

func (h *Handler) handleDeleteFileSystem(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteFileSystem(fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func fsToResponse(fs *FileSystem) map[string]any {
	resp := map[string]any{
		"FileSystemId":         fs.FileSystemID,
		"FileSystemArn":        fs.FileSystemArn,
		"CreationToken":        fs.CreationToken,
		"PerformanceMode":      fs.PerformanceMode,
		"ThroughputMode":       fs.ThroughputMode,
		"LifeCycleState":       fs.LifeCycleState,
		"Encrypted":            fs.Encrypted,
		"NumberOfMountTargets": fs.NumberOfMountTargets,
		"OwnerId":              fs.AccountID,
		"Tags":                 tagsToEntries(fs.Tags.Clone()),
		"CreationTime":         float64(fs.CreationTime.Unix()),
		"SizeInBytes": map[string]any{
			"Value":     0,
			"Timestamp": float64(fs.CreationTime.Unix()),
		},
	}
	if fs.Name != "" {
		resp["Name"] = fs.Name
	}

	return resp
}

// --- MountTarget handlers ---

type createMountTargetBody struct {
	FileSystemID string `json:"FileSystemId"`
	SubnetID     string `json:"SubnetId"`
	IPAddress    string `json:"IpAddress"`
}

func (h *Handler) handleCreateMountTarget(c *echo.Context, body []byte) error {
	var in createMountTargetBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	mt, err := h.Backend.CreateMountTarget(in.FileSystemID, in.SubnetID, in.IPAddress)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, mtToResponse(mt))
}

func (h *Handler) handleDescribeMountTargets(c *echo.Context, mountTargetID string) error {
	fsID := c.Request().URL.Query().Get("FileSystemId")
	if mountTargetID == "" {
		mountTargetID = c.Request().URL.Query().Get("MountTargetId")
	}

	mts, err := h.Backend.DescribeMountTargets(fsID, mountTargetID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(mts))
	for _, mt := range mts {
		items = append(items, mtToResponse(mt))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MountTargets": items,
	})
}

func (h *Handler) handleDeleteMountTarget(c *echo.Context, mountTargetID string) error {
	if err := h.Backend.DeleteMountTarget(mountTargetID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func mtToResponse(mt *MountTarget) map[string]any {
	return map[string]any{
		"MountTargetId":  mt.MountTargetID,
		"FileSystemId":   mt.FileSystemID,
		"SubnetId":       mt.SubnetID,
		"LifeCycleState": mt.LifeCycleState,
		"IpAddress":      mt.IPAddress,
		"OwnerId":        mt.OwnerID,
	}
}

// --- AccessPoint handlers ---

type createAccessPointBody struct {
	FileSystemID string     `json:"FileSystemId"`
	Tags         []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateAccessPoint(c *echo.Context, body []byte) error {
	var in createAccessPointBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	kv := tagsFromEntries(in.Tags)
	ap, err := h.Backend.CreateAccessPoint(in.FileSystemID, kv)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, apToResponse(ap))
}

func (h *Handler) handleDescribeAccessPoints(c *echo.Context, accessPointID string) error {
	fsID := c.Request().URL.Query().Get("FileSystemId")
	if accessPointID == "" {
		accessPointID = c.Request().URL.Query().Get("AccessPointId")
	}

	aps, err := h.Backend.DescribeAccessPoints(fsID, accessPointID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(aps))
	for _, ap := range aps {
		items = append(items, apToResponse(ap))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AccessPoints": items,
	})
}

func (h *Handler) handleDeleteAccessPoint(c *echo.Context, accessPointID string) error {
	if err := h.Backend.DeleteAccessPoint(accessPointID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func apToResponse(ap *AccessPoint) map[string]any {
	resp := map[string]any{
		"AccessPointId":  ap.AccessPointID,
		"AccessPointArn": ap.AccessPointArn,
		"FileSystemId":   ap.FileSystemID,
		"LifeCycleState": ap.LifeCycleState,
		"OwnerId":        ap.OwnerID,
		"Tags":           tagsToEntries(ap.Tags.Clone()),
	}
	if ap.Name != "" {
		resp["Name"] = ap.Name
	}

	return resp
}

// --- Tag handlers ---

type tagResourceBody struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceID string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	kv := tagsFromEntries(in.Tags)
	if err := h.Backend.TagResource(resourceID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceID string) error {
	t, err := h.Backend.ListTagsForResource(resourceID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Tags": tagsToEntries(t),
	})
}

// --- Lifecycle Configuration handlers ---

type putLifecycleConfigBody struct {
	LifecyclePolicies []LifecyclePolicy `json:"LifecyclePolicies"`
}

func (h *Handler) handleDescribeLifecycleConfiguration(c *echo.Context, fileSystemID string) error {
	policies, err := h.Backend.DescribeLifecycleConfiguration(fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LifecyclePolicies": policies,
	})
}

func (h *Handler) handlePutLifecycleConfiguration(c *echo.Context, fileSystemID string, body []byte) error {
	var in putLifecycleConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	stored, err := h.Backend.PutLifecycleConfiguration(fileSystemID, in.LifecyclePolicies)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"LifecyclePolicies": stored,
	})
}
