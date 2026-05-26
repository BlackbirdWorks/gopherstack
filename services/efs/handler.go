package efs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opUnknown = "Unknown"
	keyTags   = "Tags"
)

const (
	opUpdateFileSystem          = "UpdateFileSystem"
	opTagResource               = "TagResource"
	opPutLifecycleConfiguration = "PutLifecycleConfiguration"
	opPutBackupPolicy           = "PutBackupPolicy"
	opPutFileSystemPolicy       = "PutFileSystemPolicy"
)

const (
	keyFileSystemID   = "FileSystemId"
	keyLifeCycleState = "LifeCycleState"
	keyOwnerID        = "OwnerId"
)

const (
	opCreateFileSystem                  = "CreateFileSystem"
	opDescribeFileSystems               = "DescribeFileSystems"
	opDeleteFileSystem                  = "DeleteFileSystem"
	opCreateMountTarget                 = "CreateMountTarget"
	opDescribeMountTargets              = "DescribeMountTargets"
	opDeleteMountTarget                 = "DeleteMountTarget"
	opCreateAccessPoint                 = "CreateAccessPoint"
	opDescribeAccessPoints              = "DescribeAccessPoints"
	opDeleteAccessPoint                 = "DeleteAccessPoint"
	opListTagsForResource               = "ListTagsForResource"
	opDescribeLifecycleConfiguration    = "DescribeLifecycleConfiguration"
	opCreateReplicationConfiguration    = "CreateReplicationConfiguration"
	opCreateTags                        = "CreateTags"
	opDeleteFileSystemPolicy            = "DeleteFileSystemPolicy"
	opDeleteReplicationConfiguration    = "DeleteReplicationConfiguration"
	opDeleteTags                        = "DeleteTags"
	opDescribeAccountPreferences        = "DescribeAccountPreferences"
	opDescribeBackupPolicy              = "DescribeBackupPolicy"
	opDescribeFileSystemPolicy          = "DescribeFileSystemPolicy"
	opDescribeMountTargetSecurityGroups = "DescribeMountTargetSecurityGroups"
	opDescribeReplicationConfigurations = "DescribeReplicationConfigurations"
	opDescribeTags                      = "DescribeTags"
	opModifyMountTargetSecurityGroups   = "ModifyMountTargetSecurityGroups"
	opPutAccountPreferences             = "PutAccountPreferences"
	opUntagResource                     = "UntagResource"
	opUpdateFileSystemProtection        = "UpdateFileSystemProtection"
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

	defaultMaxItems = 10
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
		opCreateFileSystem,
		opDescribeFileSystems,
		opDeleteFileSystem,
		opUpdateFileSystem,
		opCreateMountTarget,
		opDescribeMountTargets,
		opDeleteMountTarget,
		opCreateAccessPoint,
		opDescribeAccessPoints,
		opDeleteAccessPoint,
		opTagResource,
		opListTagsForResource,
		opDescribeLifecycleConfiguration,
		opPutLifecycleConfiguration,
		opCreateReplicationConfiguration,
		opCreateTags,
		opDeleteFileSystemPolicy,
		opDeleteReplicationConfiguration,
		opDeleteTags,
		opDescribeAccountPreferences,
		opDescribeBackupPolicy,
		opPutBackupPolicy,
		opDescribeFileSystemPolicy,
		opPutFileSystemPolicy,
		opDescribeMountTargetSecurityGroups,
		opDescribeReplicationConfigurations,
		opDescribeTags,
		opModifyMountTargetSecurityGroups,
		opPutAccountPreferences,
		opUntagResource,
		opUpdateFileSystemProtection,
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

	return efsRoute{operation: opUnknown}
}

func parseFileSystemRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		switch method {
		case http.MethodPost:
			return efsRoute{operation: opCreateFileSystem}
		case http.MethodGet:
			return efsRoute{operation: opDescribeFileSystems}
		}
	case id == "replication-configurations":
		if method == http.MethodGet {
			return efsRoute{operation: opDescribeReplicationConfigurations}
		}
	case !strings.Contains(id, "/"):
		// Treat the single segment as a file system ID.
		switch method {
		case http.MethodGet:
			return efsRoute{operation: opDescribeFileSystems, resource: id}
		case http.MethodDelete:
			return efsRoute{operation: opDeleteFileSystem, resource: id}
		case http.MethodPut:
			return efsRoute{operation: opUpdateFileSystem, resource: id}
		}
	default:
		return parseFileSystemSubRoute(method, id)
	}

	return efsRoute{operation: opUnknown}
}

func parseFileSystemSubRoute(method, id string) efsRoute {
	// Sub-resource paths: /{fileSystemId}/{subresource}
	parts := strings.SplitN(id, "/", subresourcePathParts)
	if len(parts) < subresourcePathParts {
		return efsRoute{operation: opUnknown}
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
			return efsRoute{operation: opDescribeBackupPolicy, resource: fsID}
		}
		if method == http.MethodPut {
			return efsRoute{operation: opPutBackupPolicy, resource: fsID}
		}
	case "protection":
		if method == http.MethodPut {
			return efsRoute{operation: opUpdateFileSystemProtection, resource: fsID}
		}
	}

	return efsRoute{operation: opUnknown}
}

func parseLifecycleConfigRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodGet:
		return efsRoute{operation: opDescribeLifecycleConfiguration, resource: fsID}
	case http.MethodPut:
		return efsRoute{operation: opPutLifecycleConfiguration, resource: fsID}
	}

	return efsRoute{operation: opUnknown}
}

func parseReplicationConfigRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodPost:
		return efsRoute{operation: opCreateReplicationConfiguration, resource: fsID}
	case http.MethodDelete:
		return efsRoute{operation: opDeleteReplicationConfiguration, resource: fsID}
	}

	return efsRoute{operation: opUnknown}
}

func parseFileSystemPolicyRoute(method, fsID string) efsRoute {
	switch method {
	case http.MethodGet:
		return efsRoute{operation: opDescribeFileSystemPolicy, resource: fsID}
	case http.MethodPut:
		return efsRoute{operation: opPutFileSystemPolicy, resource: fsID}
	case http.MethodDelete:
		return efsRoute{operation: opDeleteFileSystemPolicy, resource: fsID}
	}

	return efsRoute{operation: opUnknown}
}

func parseMountTargetRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")

	switch {
	case id == "":
		switch method {
		case http.MethodPost:
			return efsRoute{operation: opCreateMountTarget}
		case http.MethodGet:
			return efsRoute{operation: opDescribeMountTargets}
		}
	case !strings.Contains(id, "/"):
		switch method {
		case http.MethodGet:
			return efsRoute{operation: opDescribeMountTargets, resource: id}
		case http.MethodDelete:
			return efsRoute{operation: opDeleteMountTarget, resource: id}
		}
	default:
		// Sub-resource paths: /{mountTargetId}/{subresource}
		parts := strings.SplitN(id, "/", subresourcePathParts)
		if len(parts) >= subresourcePathParts && parts[1] == "security-groups" {
			switch method {
			case http.MethodGet:
				return efsRoute{operation: opDescribeMountTargetSecurityGroups, resource: parts[0]}
			case http.MethodPut:
				return efsRoute{operation: opModifyMountTargetSecurityGroups, resource: parts[0]}
			}
		}
	}

	return efsRoute{operation: opUnknown}
}

func parseAccessPointRoute(method, suffix string) efsRoute {
	id := strings.TrimPrefix(suffix, "/")
	if id == "" {
		switch method {
		case http.MethodPost:
			return efsRoute{operation: opCreateAccessPoint}
		case http.MethodGet:
			return efsRoute{operation: opDescribeAccessPoints}
		}
	} else if !strings.Contains(id, "/") {
		switch method {
		case http.MethodGet:
			return efsRoute{operation: opDescribeAccessPoints, resource: id}
		case http.MethodDelete:
			return efsRoute{operation: opDeleteAccessPoint, resource: id}
		}
	}

	return efsRoute{operation: opUnknown}
}

func parseTagsRoute(method, resourceID string) efsRoute {
	switch method {
	case http.MethodPost:
		return efsRoute{operation: opTagResource, resource: resourceID}
	case http.MethodGet:
		return efsRoute{operation: opListTagsForResource, resource: resourceID}
	case http.MethodDelete:
		return efsRoute{operation: opUntagResource, resource: resourceID}
	}

	return efsRoute{operation: opUnknown}
}

func parseCreateTagsRoute(method, fileSystemID string) efsRoute {
	if method == http.MethodPost {
		return efsRoute{operation: opCreateTags, resource: fileSystemID}
	}

	return efsRoute{operation: opUnknown}
}

func parseDeleteTagsRoute(method, fileSystemID string) efsRoute {
	if method == http.MethodPost {
		return efsRoute{operation: opDeleteTags, resource: fileSystemID}
	}

	return efsRoute{operation: opUnknown}
}

func parseAccountPrefsRoute(method string) efsRoute {
	switch method {
	case http.MethodGet:
		return efsRoute{operation: opDescribeAccountPreferences}
	case http.MethodPut:
		return efsRoute{operation: opPutAccountPreferences}
	}

	return efsRoute{operation: opUnknown}
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
	case opCreateFileSystem:
		return true, h.handleCreateFileSystem(c, body)
	case opDescribeFileSystems:
		return true, h.handleDescribeFileSystems(c, route.resource)
	case opDeleteFileSystem:
		return true, h.handleDeleteFileSystem(c, route.resource)
	case opUpdateFileSystem:
		return true, h.handleUpdateFileSystem(c, route.resource, body)
	case opDescribeLifecycleConfiguration:
		return true, h.handleDescribeLifecycleConfiguration(c, route.resource)
	case opPutLifecycleConfiguration:
		return true, h.handlePutLifecycleConfiguration(c, route.resource, body)
	case opCreateReplicationConfiguration:
		return true, h.handleCreateReplicationConfiguration(c, route.resource, body)
	case opDeleteReplicationConfiguration:
		return true, h.handleDeleteReplicationConfiguration(c, route.resource)
	case opDescribeReplicationConfigurations:
		return true, h.handleDescribeReplicationConfigurations(c)
	case opDescribeFileSystemPolicy:
		return true, h.handleDescribeFileSystemPolicy(c, route.resource)
	case opPutFileSystemPolicy:
		return true, h.handlePutFileSystemPolicy(c, route.resource, body)
	case opDeleteFileSystemPolicy:
		return true, h.handleDeleteFileSystemPolicy(c, route.resource)
	case opDescribeBackupPolicy:
		return true, h.handleDescribeBackupPolicy(c, route.resource)
	case opPutBackupPolicy:
		return true, h.handlePutBackupPolicy(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) dispatchMountTargetAndAccessPointOps(c *echo.Context, route efsRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateMountTarget:
		return true, h.handleCreateMountTarget(c, body)
	case opDescribeMountTargets:
		return true, h.handleDescribeMountTargets(c, route.resource)
	case opDeleteMountTarget:
		return true, h.handleDeleteMountTarget(c, route.resource)
	case opDescribeMountTargetSecurityGroups:
		return true, h.handleDescribeMountTargetSecurityGroups(c, route.resource)
	case opModifyMountTargetSecurityGroups:
		return true, h.handleModifyMountTargetSecurityGroups(c, route.resource, body)
	case opCreateAccessPoint:
		return true, h.handleCreateAccessPoint(c, body)
	case opDescribeAccessPoints:
		return true, h.handleDescribeAccessPoints(c, route.resource)
	case opDeleteAccessPoint:
		return true, h.handleDeleteAccessPoint(c, route.resource)
	}

	return false, nil
}

func (h *Handler) dispatchTagAndMiscOps(c *echo.Context, route efsRoute, body []byte) (bool, error) {
	switch route.operation {
	case opTagResource:
		return true, h.handleTagResource(c, route.resource, body)
	case opListTagsForResource, opDescribeTags:
		return true, h.handleListTagsForResource(c, route.resource)
	case opUntagResource:
		return true, h.handleUntagResource(c, route.resource)
	case opCreateTags:
		return true, h.handleCreateTags(c, route.resource, body)
	case opDeleteTags:
		return true, h.handleDeleteTags(c, route.resource, body)
	case opDescribeAccountPreferences:
		return true, h.handleDescribeAccountPreferences(c)
	case opPutAccountPreferences:
		return true, h.handlePutAccountPreferences(c, body)
	case opUpdateFileSystemProtection:
		return true, h.handleUpdateFileSystemProtection(c, route.resource, body)
	}

	return false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrValidation):
		c.Response().Header().Set("x-amzn-ErrorType", "ValidationException")

		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	case errors.Is(err, ErrTooManyRequests):
		c.Response().Header().Set("x-amzn-ErrorType", "TooManyRequests")

		return c.JSON(http.StatusTooManyRequests, errResp("TooManyRequests", err.Error()))
	case errors.Is(err, ErrFileSystemInUse):
		c.Response().Header().Set("x-amzn-ErrorType", "FileSystemInUse")

		return c.JSON(http.StatusConflict, errResp("FileSystemInUse", err.Error()))
	case errors.Is(err, ErrMountTargetConflict):
		c.Response().Header().Set("x-amzn-ErrorType", "MountTargetConflict")

		return c.JSON(http.StatusConflict, errResp("MountTargetConflict", err.Error()))
	case errors.Is(err, ErrSecurityGroupLimitExceeded):
		c.Response().Header().Set("x-amzn-ErrorType", "SecurityGroupLimitExceeded")

		return c.JSON(http.StatusConflict, errResp("SecurityGroupLimitExceeded", err.Error()))
	case errors.Is(err, ErrNotFound):
		c.Response().Header().Set("x-amzn-ErrorType", "FileSystemNotFound")

		return c.JSON(http.StatusNotFound, errResp("FileSystemNotFound", err.Error()))
	case errors.Is(err, ErrMountTargetNotFound):
		c.Response().Header().Set("x-amzn-ErrorType", "MountTargetNotFound")

		return c.JSON(http.StatusNotFound, errResp("MountTargetNotFound", err.Error()))
	case errors.Is(err, ErrAccessPointNotFound):
		c.Response().Header().Set("x-amzn-ErrorType", "AccessPointNotFound")

		return c.JSON(http.StatusNotFound, errResp("AccessPointNotFound", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		c.Response().Header().Set("x-amzn-ErrorType", "FileSystemAlreadyExists")

		return c.JSON(http.StatusConflict, errResp("FileSystemAlreadyExists", err.Error()))
	default:
		c.Response().Header().Set("x-amzn-ErrorType", "InternalServerError")

		return c.JSON(http.StatusInternalServerError, errResp("InternalServerError", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"ErrorCode": code, "Message": msg}
}

// --- FileSystem handlers ---

type createFileSystemBody struct {
	CreationToken            string     `json:"CreationToken"`
	PerformanceMode          string     `json:"PerformanceMode"`
	ThroughputMode           string     `json:"ThroughputMode"`
	KmsKeyId                 string     `json:"KmsKeyId"`
	AvailabilityZoneName     string     `json:"AvailabilityZoneName"`
	Tags                     []tagEntry `json:"Tags"`
	ProvisionedThroughputMib float64    `json:"ProvisionedThroughputInMibps"`
	Encrypted                bool       `json:"Encrypted"`
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

// tagsToEntries converts a tag map to sorted entries for deterministic output.
func tagsToEntries(m map[string]string) []tagEntry {
	entries := make([]tagEntry, 0, len(m))
	for k, v := range m {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

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

	req := CreateFileSystemRequest{
		CreationToken:            in.CreationToken,
		PerformanceMode:          in.PerformanceMode,
		ThroughputMode:           in.ThroughputMode,
		KmsKeyId:                 in.KmsKeyId,
		AvailabilityZoneName:     in.AvailabilityZoneName,
		ProvisionedThroughputMib: in.ProvisionedThroughputMib,
		Encrypted:                in.Encrypted,
		Tags:                     tagsFromEntries(in.Tags),
	}

	fs, err := h.Backend.CreateFileSystem(req)
	if err != nil {
		if errors.Is(err, ErrCreationTokenExists) {
			// Identical token with identical args: return existing fs with 200 OK.
			return c.JSON(http.StatusOK, fsToResponse(fs))
		}
		if errors.Is(err, ErrAlreadyExists) {
			// Different args: return 409 with file system ID in body.
			c.Response().Header().Set("x-amzn-ErrorType", "FileSystemAlreadyExists")
			resp := map[string]string{
				"ErrorCode":    "FileSystemAlreadyExists",
				"Message":      err.Error(),
				"FileSystemId": fs.FileSystemID,
			}

			return c.JSON(http.StatusConflict, resp)
		}

		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, fsToResponse(fs))
}

func (h *Handler) handleDescribeFileSystems(c *echo.Context, fileSystemID string) error {
	// Also accept ?FileSystemId= query param.
	if fileSystemID == "" {
		fileSystemID = c.Request().URL.Query().Get(keyFileSystemID)
	}

	marker := c.Request().URL.Query().Get("Marker")
	maxItems := queryInt(c, "MaxItems", defaultMaxItems)

	fsList, nextMarker, err := h.Backend.DescribeFileSystems(fileSystemID, marker, maxItems)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(fsList))
	for _, fs := range fsList {
		items = append(items, fsToResponse(fs))
	}

	resp := map[string]any{
		"FileSystems": items,
	}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteFileSystem(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteFileSystem(fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func fsToResponse(fs *FileSystem) map[string]any {
	resp := map[string]any{
		keyFileSystemID:        fs.FileSystemID,
		"FileSystemArn":        fs.FileSystemArn,
		"CreationToken":        fs.CreationToken,
		"PerformanceMode":      fs.PerformanceMode,
		"ThroughputMode":       fs.ThroughputMode,
		keyLifeCycleState:      fs.LifeCycleState,
		"Encrypted":            fs.Encrypted,
		"NumberOfMountTargets": fs.NumberOfMountTargets,
		keyOwnerID:             fs.AccountID,
		keyTags:                tagsToEntries(fs.Tags.Clone()),
		"CreationTime":         float64(fs.CreationTime.Unix()),
		"SizeInBytes": map[string]any{
			"Value":     0,
			"Timestamp": float64(fs.CreationTime.Unix()),
		},
		"FileSystemProtection": map[string]any{
			"ReplicationOverwriteProtection": fs.ReplicationOverwriteProtection,
		},
	}
	if fs.Name != "" {
		resp["Name"] = fs.Name
	}
	if fs.KmsKeyId != "" {
		resp["KmsKeyId"] = fs.KmsKeyId
	}
	if fs.AvailabilityZoneName != "" {
		resp["AvailabilityZoneName"] = fs.AvailabilityZoneName
		resp["AvailabilityZoneId"] = fs.AvailabilityZoneId
	}
	if fs.ProvisionedThroughputMib > 0 {
		resp["ProvisionedThroughputInMibps"] = fs.ProvisionedThroughputMib
	}

	return resp
}

// --- MountTarget handlers ---

type createMountTargetBody struct {
	FileSystemID   string   `json:"FileSystemId"`
	SubnetID       string   `json:"SubnetId"`
	IPAddress      string   `json:"IpAddress"`
	SecurityGroups []string `json:"SecurityGroups"`
}

func (h *Handler) handleCreateMountTarget(c *echo.Context, body []byte) error {
	var in createMountTargetBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	req := CreateMountTargetRequest{
		FileSystemID:   in.FileSystemID,
		SubnetID:       in.SubnetID,
		IPAddress:      in.IPAddress,
		SecurityGroups: in.SecurityGroups,
	}

	mt, err := h.Backend.CreateMountTarget(req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, mtToResponse(mt))
}

func (h *Handler) handleDescribeMountTargets(c *echo.Context, mountTargetID string) error {
	fsID := c.Request().URL.Query().Get(keyFileSystemID)
	if mountTargetID == "" {
		mountTargetID = c.Request().URL.Query().Get("MountTargetId")
	}

	marker := c.Request().URL.Query().Get("Marker")
	maxItems := queryInt(c, "MaxItems", defaultMaxItems)

	mts, nextMarker, err := h.Backend.DescribeMountTargets(fsID, mountTargetID, marker, maxItems)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(mts))
	for _, mt := range mts {
		items = append(items, mtToResponse(mt))
	}

	resp := map[string]any{
		"MountTargets": items,
	}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteMountTarget(c *echo.Context, mountTargetID string) error {
	if err := h.Backend.DeleteMountTarget(mountTargetID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func mtToResponse(mt *MountTarget) map[string]any {
	resp := map[string]any{
		"MountTargetId":        mt.MountTargetID,
		keyFileSystemID:        mt.FileSystemID,
		"SubnetId":             mt.SubnetID,
		keyLifeCycleState:      mt.LifeCycleState,
		"IpAddress":            mt.IPAddress,
		keyOwnerID:             mt.OwnerID,
		"NetworkInterfaceId":   mt.NetworkInterfaceID,
		"VpcId":                mt.VpcID,
		"AvailabilityZoneName": mt.AvailabilityZoneName,
		"AvailabilityZoneId":   mt.AvailabilityZoneId,
	}
	if len(mt.SecurityGroups) > 0 {
		resp["SecurityGroups"] = mt.SecurityGroups
	}

	return resp
}

// --- AccessPoint handlers ---

type createAccessPointBody struct {
	FileSystemID  string         `json:"FileSystemId"`
	ClientToken   string         `json:"ClientToken"`
	Tags          []tagEntry     `json:"Tags"`
	PosixUser     *PosixUser     `json:"PosixUser"`
	RootDirectory *RootDirectory `json:"RootDirectory"`
}

func (h *Handler) handleCreateAccessPoint(c *echo.Context, body []byte) error {
	var in createAccessPointBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	req := CreateAccessPointRequest{
		FileSystemID:  in.FileSystemID,
		ClientToken:   in.ClientToken,
		Tags:          tagsFromEntries(in.Tags),
		PosixUser:     in.PosixUser,
		RootDirectory: in.RootDirectory,
	}

	ap, err := h.Backend.CreateAccessPoint(req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, apToResponse(ap))
}

func (h *Handler) handleDescribeAccessPoints(c *echo.Context, accessPointID string) error {
	fsID := c.Request().URL.Query().Get(keyFileSystemID)
	if accessPointID == "" {
		accessPointID = c.Request().URL.Query().Get("AccessPointId")
	}

	marker := c.Request().URL.Query().Get("NextToken")
	maxItems := queryInt(c, "MaxResults", defaultMaxItems)

	aps, nextToken, err := h.Backend.DescribeAccessPoints(fsID, accessPointID, marker, maxItems)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(aps))
	for _, ap := range aps {
		items = append(items, apToResponse(ap))
	}

	resp := map[string]any{
		"AccessPoints": items,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteAccessPoint(c *echo.Context, accessPointID string) error {
	if err := h.Backend.DeleteAccessPoint(accessPointID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func apToResponse(ap *AccessPoint) map[string]any {
	resp := map[string]any{
		"AccessPointId":   ap.AccessPointID,
		"AccessPointArn":  ap.AccessPointArn,
		keyFileSystemID:   ap.FileSystemID,
		keyLifeCycleState: ap.LifeCycleState,
		keyOwnerID:        ap.OwnerID,
		keyTags:           tagsToEntries(ap.Tags.Clone()),
	}
	if ap.Name != "" {
		resp["Name"] = ap.Name
	}
	if ap.ClientToken != "" {
		resp["ClientToken"] = ap.ClientToken
	}
	if ap.PosixUser != nil {
		resp["PosixUser"] = ap.PosixUser
	}
	if ap.RootDirectory != nil {
		resp["RootDirectory"] = ap.RootDirectory
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
		keyTags: tagsToEntries(t),
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
	fsID := c.Request().URL.Query().Get(keyFileSystemID)

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
		keyFileSystemID: fileSystemID,
		"Policy":        policy,
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
	Policy                          string `json:"Policy"`
	BypassPolicyLockoutSafetyCheck  bool   `json:"BypassPolicyLockoutSafetyCheck"`
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
		keyFileSystemID: fileSystemID,
		"Policy":        in.Policy,
	})
}

// --- UntagResource handler ---

func (h *Handler) handleUntagResource(c *echo.Context, resourceID string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if err := h.Backend.UntagResource(resourceID, tagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ModifyMountTargetSecurityGroups handler ---

type modifyMountTargetSGBody struct {
	SecurityGroups []string `json:"SecurityGroups"`
}

func (h *Handler) handleModifyMountTargetSecurityGroups(c *echo.Context, mountTargetID string, body []byte) error {
	var in modifyMountTargetSGBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.ModifyMountTargetSecurityGroups(mountTargetID, in.SecurityGroups); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- PutAccountPreferences handler ---

type putAccountPreferencesBody struct {
	ResourceIDPreference struct {
		ResourceIDType string `json:"ResourceIdType"`
	} `json:"ResourceIdPreference"`
}

func (h *Handler) handlePutAccountPreferences(c *echo.Context, body []byte) error {
	var in putAccountPreferencesBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	prefs, err := h.Backend.PutAccountPreferences(in.ResourceIDPreference.ResourceIDType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceIdPreference": map[string]any{
			"ResourceIdType": prefs.ResourceIDType,
			"Resources":      []string{"FILE_SYSTEM", "MOUNT_TARGET"},
		},
	})
}

// --- UpdateFileSystemProtection handler ---

type updateFileSystemProtectionBody struct {
	ReplicationOverwriteProtection string `json:"ReplicationOverwriteProtection"`
}

func (h *Handler) handleUpdateFileSystemProtection(c *echo.Context, fileSystemID string, body []byte) error {
	var in updateFileSystemProtectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.UpdateFileSystemProtection(fileSystemID, in.ReplicationOverwriteProtection); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReplicationOverwriteProtection": in.ReplicationOverwriteProtection,
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

	req := UpdateFileSystemRequest{
		ThroughputMode:           in.ThroughputMode,
		ProvisionedThroughputMib: in.ProvisionedThroughputMib,
	}

	fs, err := h.Backend.UpdateFileSystem(fileSystemID, req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusAccepted, fsToResponse(fs))
}

// queryInt reads a query parameter as an int, returning defaultVal if absent or invalid.
func queryInt(c *echo.Context, key string, defaultVal int) int {
	s := c.Request().URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return defaultVal
	}

	return v
}
