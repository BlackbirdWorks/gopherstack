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
	pathCreateTags   = "/2015-02-01/create-tags"
	pathDeleteTags   = "/2015-02-01/delete-tags"
	pathAccountPrefs = "/2015-02-01/account-preferences"

	// subresourcePathParts is the number of segments when splitting a path with a sub-resource.
	subresourcePathParts = 2
)

// Handler is the Echo HTTP handler for AWS EFS operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]struct{}
}

// NewHandler creates a new EFS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.buildOps()

	return h
}

// buildOps pre-builds the set of supported operation names for fast lookup.
func (h *Handler) buildOps() {
	supported := h.GetSupportedOperations()
	h.ops = make(map[string]struct{}, len(supported))

	for _, op := range supported {
		h.ops[op] = struct{}{}
	}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "EFS" }

// GetSupportedOperations returns the list of supported EFS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateFileSystem",
		"DescribeFileSystems",
		"DeleteFileSystem",
		"UpdateFileSystem",
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
		"CreateReplicationConfiguration",
		"CreateTags",
		"DeleteFileSystemPolicy",
		"DeleteReplicationConfiguration",
		"DeleteTags",
		"DescribeAccountPreferences",
		"DescribeBackupPolicy",
		"PutBackupPolicy",
		"DescribeFileSystemPolicy",
		"PutFileSystemPolicy",
		"DescribeMountTargetSecurityGroups",
		"DescribeReplicationConfigurations",
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
			strings.HasPrefix(path, pathTags+"/") ||
			strings.HasPrefix(path, pathCreateTags+"/") ||
			strings.HasPrefix(path, pathDeleteTags+"/") ||
			path == pathAccountPrefs
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
	case strings.HasPrefix(path, pathCreateTags+"/"):
		return parseCreateTagsRoute(method, strings.TrimPrefix(path, pathCreateTags+"/"))
	case strings.HasPrefix(path, pathDeleteTags+"/"):
		return parseDeleteTagsRoute(method, strings.TrimPrefix(path, pathDeleteTags+"/"))
	case path == pathAccountPrefs:
		return parseAccountPrefsRoute(method)
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
	case id == "replication-configurations":
		if method == http.MethodGet {
			return efsRoute{operation: "DescribeReplicationConfigurations"}
		}
	case !strings.Contains(id, "/"):
		// Treat the single segment as a file system ID.
		switch method {
		case http.MethodGet:
			return efsRoute{operation: "DescribeFileSystems", resource: id}
		case http.MethodDelete:
			return efsRoute{operation: "DeleteFileSystem", resource: id}
		case http.MethodPut:
			return efsRoute{operation: "UpdateFileSystem", resource: id}
		}
	default:
		return parseFileSystemSubRoute(method, id)
	}

	return efsRoute{operation: "Unknown"}
}

func parseFileSystemSubRoute(method, id string) efsRoute {
	// Sub-resource paths: /{fileSystemId}/{subresource}
	parts := strings.SplitN(id, "/", subresourcePathParts)
	if len(parts) < subresourcePathParts {
		return efsRoute{operation: "Unknown"}
	}

	fsID, sub := parts[0], parts[1]

	switch sub {
	case "lifecycle-configuration":
		return parseLifecycleConfigRoute(method, fsID)
	case "replication-configuration":
		return parseReplicationConfigRoute(method, fsID)
	case "policy":
		return parseFileSystemPolicyRoute(method, fsID)
	case "backup-policy":
		if method == http.MethodGet {
			return efsRoute{operation: "DescribeBackupPolicy", resource: fsID}
		}
		if method == http.MethodPut {
			return efsRoute{operation: "PutBackupPolicy", resource: fsID}
		}
	}

	return efsRoute{operation: "Unknown"}
}

func parseLifecycleConfigRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodGet:
		return efsRoute{operation: "DescribeLifecycleConfiguration", resource: fsID}
	case http.MethodPut:
		return efsRoute{operation: "PutLifecycleConfiguration", resource: fsID}
	}

	return efsRoute{operation: "Unknown"}
}

func parseReplicationConfigRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodPost:
		return efsRoute{operation: "CreateReplicationConfiguration", resource: fsID}
	case http.MethodDelete:
		return efsRoute{operation: "DeleteReplicationConfiguration", resource: fsID}
	}

	return efsRoute{operation: "Unknown"}
}

func parseFileSystemPolicyRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodGet:
		return efsRoute{operation: "DescribeFileSystemPolicy", resource: fsID}
	case http.MethodPut:
		return efsRoute{operation: "PutFileSystemPolicy", resource: fsID}
	case http.MethodDelete:
		return efsRoute{operation: "DeleteFileSystemPolicy", resource: fsID}
	}

	return efsRoute{operation: "Unknown"}
}

func parseMountTargetRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		switch method {
		case http.MethodPost:
			return efsRoute{operation: "CreateMountTarget"}
		case http.MethodGet:
			return efsRoute{operation: "DescribeMountTargets"}
		}
	case !strings.Contains(id, "/"):
		switch method {
		case http.MethodGet:
			return efsRoute{operation: "DescribeMountTargets", resource: id}
		case http.MethodDelete:
			return efsRoute{operation: "DeleteMountTarget", resource: id}
		}
	default:
		// Sub-resource paths: /{mountTargetId}/{subresource}
		parts := strings.SplitN(id, "/", subresourcePathParts)
		if len(parts) >= subresourcePathParts && parts[1] == "security-groups" && method == http.MethodGet {
			return efsRoute{operation: "DescribeMountTargetSecurityGroups", resource: parts[0]}
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

func parseCreateTagsRoute(method, fileSystemID string) efsRoute {
	if method == http.MethodPost {
		return efsRoute{operation: "CreateTags", resource: fileSystemID}
	}

	return efsRoute{operation: "Unknown"}
}

func parseDeleteTagsRoute(method, fileSystemID string) efsRoute {
	if method == http.MethodPost {
		return efsRoute{operation: "DeleteTags", resource: fileSystemID}
	}

	return efsRoute{operation: "Unknown"}
}

func parseAccountPrefsRoute(method string) efsRoute {
	if method == http.MethodGet {
		return efsRoute{operation: "DescribeAccountPreferences"}
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
	if ok, err := h.dispatchFileSystemOps(c, route, body); ok {
		return err
	}

	if ok, err := h.dispatchMountTargetAndAccessPointOps(c, route, body); ok {
		return err
	}

	if ok, err := h.dispatchTagAndMiscOps(c, route, body); ok {
		return err
	}

	return c.JSON(http.StatusNotFound, errResp("UnsupportedOperation", "unknown operation: "+route.operation))
}

func (h *Handler) dispatchFileSystemOps(c *echo.Context, route efsRoute, body []byte) (bool, error) {
	switch route.operation {
	case "CreateFileSystem":
		return true, h.handleCreateFileSystem(c, body)
	case "DescribeFileSystems":
		return true, h.handleDescribeFileSystems(c, route.resource)
	case "DeleteFileSystem":
		return true, h.handleDeleteFileSystem(c, route.resource)
	case "UpdateFileSystem":
		return true, h.handleUpdateFileSystem(c, route.resource, body)
	case "DescribeLifecycleConfiguration":
		return true, h.handleDescribeLifecycleConfiguration(c, route.resource)
	case "PutLifecycleConfiguration":
		return true, h.handlePutLifecycleConfiguration(c, route.resource, body)
	case "CreateReplicationConfiguration":
		return true, h.handleCreateReplicationConfiguration(c, route.resource, body)
	case "DeleteReplicationConfiguration":
		return true, h.handleDeleteReplicationConfiguration(c, route.resource)
	case "DescribeReplicationConfigurations":
		return true, h.handleDescribeReplicationConfigurations(c)
	case "DescribeFileSystemPolicy":
		return true, h.handleDescribeFileSystemPolicy(c, route.resource)
	case "PutFileSystemPolicy":
		return true, h.handlePutFileSystemPolicy(c, route.resource, body)
	case "DeleteFileSystemPolicy":
		return true, h.handleDeleteFileSystemPolicy(c, route.resource)
	case "DescribeBackupPolicy":
		return true, h.handleDescribeBackupPolicy(c, route.resource)
	case "PutBackupPolicy":
		return true, h.handlePutBackupPolicy(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) dispatchMountTargetAndAccessPointOps(c *echo.Context, route efsRoute, body []byte) (bool, error) {
	switch route.operation {
	case "CreateMountTarget":
		return true, h.handleCreateMountTarget(c, body)
	case "DescribeMountTargets":
		return true, h.handleDescribeMountTargets(c, route.resource)
	case "DeleteMountTarget":
		return true, h.handleDeleteMountTarget(c, route.resource)
	case "DescribeMountTargetSecurityGroups":
		return true, h.handleDescribeMountTargetSecurityGroups(c, route.resource)
	case "CreateAccessPoint":
		return true, h.handleCreateAccessPoint(c, body)
	case "DescribeAccessPoints":
		return true, h.handleDescribeAccessPoints(c, route.resource)
	case "DeleteAccessPoint":
		return true, h.handleDeleteAccessPoint(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchTagAndMiscOps(c *echo.Context, route efsRoute, body []byte) (bool, error) {
	switch route.operation {
	case "TagResource":
		return true, h.handleTagResource(c, route.resource, body)
	case "ListTagsForResource":
		return true, h.handleListTagsForResource(c, route.resource)
	case "CreateTags":
		return true, h.handleCreateTags(c, route.resource, body)
	case "DeleteTags":
		return true, h.handleDeleteTags(c, route.resource, body)
	case "DescribeAccountPreferences":
		return true, h.handleDescribeAccountPreferences(c)
	}

	return false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
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

// --- Replication Configuration handlers ---

type createReplicationConfigBody struct {
	Destinations []ReplicationDestination `json:"Destinations"`
}

func (h *Handler) handleCreateReplicationConfiguration(c *echo.Context, fileSystemID string, body []byte) error {
	var in createReplicationConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if fileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	rc, err := h.Backend.CreateReplicationConfiguration(fileSystemID, in.Destinations)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, rcToResponse(rc))
}

func (h *Handler) handleDeleteReplicationConfiguration(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteReplicationConfiguration(fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDescribeReplicationConfigurations(c *echo.Context) error {
	fsID := c.Request().URL.Query().Get("FileSystemId")

	rcs, err := h.Backend.DescribeReplicationConfigurations(fsID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(rcs))
	for _, rc := range rcs {
		items = append(items, rcToResponse(rc))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Replications": items,
	})
}

func rcToResponse(rc *ReplicationConfiguration) map[string]any {
	return map[string]any{
		"OriginalSourceFileSystemArn": rc.OriginalSourceFileSystemARN,
		"SourceFileSystemArn":         rc.SourceFileSystemARN,
		"SourceFileSystemId":          rc.SourceFileSystemID,
		"SourceFileSystemRegion":      rc.SourceFileSystemRegion,
		"CreationTime":                rc.CreationTime,
		"Destinations":                rc.Destinations,
	}
}

// --- Legacy CreateTags / DeleteTags handlers ---

type createTagsBody struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateTags(c *echo.Context, fileSystemID string, body []byte) error {
	var in createTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	kv := tagsFromEntries(in.Tags)
	if err := h.Backend.CreateTags(fileSystemID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type deleteTagsBody struct {
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleDeleteTags(c *echo.Context, fileSystemID string, body []byte) error {
	var in deleteTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.DeleteTags(fileSystemID, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- FileSystem Policy handlers ---

func (h *Handler) handleDescribeFileSystemPolicy(c *echo.Context, fileSystemID string) error {
	policy, err := h.Backend.DescribeFileSystemPolicy(fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FileSystemId": fileSystemID,
		"Policy":       policy,
	})
}

func (h *Handler) handleDeleteFileSystemPolicy(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteFileSystemPolicy(fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Account Preferences handler ---

func (h *Handler) handleDescribeAccountPreferences(c *echo.Context) error {
	prefs := h.Backend.DescribeAccountPreferences()

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceIdPreference": map[string]any{
			"ResourceIdType": prefs.ResourceIDType,
			"Resources":      []string{"FILE_SYSTEM", "MOUNT_TARGET"},
		},
	})
}

// --- Backup Policy handler ---

func (h *Handler) handleDescribeBackupPolicy(c *echo.Context, fileSystemID string) error {
	status, err := h.Backend.DescribeBackupPolicy(fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			"Status": status,
		},
	})
}

// --- Mount Target Security Groups handler ---

func (h *Handler) handleDescribeMountTargetSecurityGroups(c *echo.Context, mountTargetID string) error {
	groups, err := h.Backend.DescribeMountTargetSecurityGroups(mountTargetID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SecurityGroups": groups,
	})
}

// --- PutBackupPolicy handler ---

type putBackupPolicyBody struct {
	BackupPolicy struct {
		Status string `json:"Status"`
	} `json:"BackupPolicy"`
}

func (h *Handler) handlePutBackupPolicy(c *echo.Context, fileSystemID string, body []byte) error {
	var in putBackupPolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.PutBackupPolicy(fileSystemID, in.BackupPolicy.Status); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			"Status": in.BackupPolicy.Status,
		},
	})
}

// --- PutFileSystemPolicy handler ---

type putFileSystemPolicyBody struct {
	Policy string `json:"Policy"`
}

func (h *Handler) handlePutFileSystemPolicy(c *echo.Context, fileSystemID string, body []byte) error {
	var in putFileSystemPolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.PutFileSystemPolicy(fileSystemID, in.Policy); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FileSystemId": fileSystemID,
		"Policy":       in.Policy,
	})
}

// --- UpdateFileSystem handler ---

type updateFileSystemBody struct {
	ThroughputMode           string  `json:"ThroughputMode,omitempty"`
	ProvisionedThroughputMib float64 `json:"ProvisionedThroughputInMibps,omitempty"`
}

func (h *Handler) handleUpdateFileSystem(c *echo.Context, fileSystemID string, body []byte) error {
	var in updateFileSystemBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	req := UpdateFileSystemRequest(in)

	fs, err := h.Backend.UpdateFileSystem(fileSystemID, req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusAccepted, fsToResponse(fs))
}
