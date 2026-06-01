package directoryservice

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
	matchPriority = service.PriorityHeaderPartial

	targetPrefix = "DirectoryService_20150416."

	opCreateDirectory        = "CreateDirectory"
	opCreateMicrosoftAD      = "CreateMicrosoftAD"
	opDeleteDirectory        = "DeleteDirectory"
	opDescribeDirectories    = "DescribeDirectories"
	opCreateAlias            = "CreateAlias"
	opEnableSso              = "EnableSso"
	opDisableSso             = "DisableSso"
	opGetDirectoryLimits     = "GetDirectoryLimits"
	opCreateSnapshot         = "CreateSnapshot"
	opDeleteSnapshot         = "DeleteSnapshot"
	opDescribeSnapshots      = "DescribeSnapshots"
	opGetSnapshotLimits      = "GetSnapshotLimits"
	opRestoreFromSnapshot    = "RestoreFromSnapshot"
	opAddTagsToResource      = "AddTagsToResource"
	opRemoveTagsFromResource = "RemoveTagsFromResource"
	opListTagsForResource    = "ListTagsForResource"
	opUnknown                = "Unknown"
)

// Handler handles DirectoryService HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "DirectoryService" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateDirectory,
		opCreateMicrosoftAD,
		opDeleteDirectory,
		opDescribeDirectories,
		opCreateAlias,
		opEnableSso,
		opDisableSso,
		opGetDirectoryLimits,
		opCreateSnapshot,
		opDeleteSnapshot,
		opDescribeSnapshots,
		opGetSnapshotLimits,
		opRestoreFromSnapshot,
		opAddTagsToResource,
		opRemoveTagsFromResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a matcher that accepts DirectoryService requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	op, _ := strings.CutPrefix(target, targetPrefix)

	if op == "" {
		return opUnknown
	}

	return op
}

// ExtractResource extracts the directory ID from the request body when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"DirectoryId", "ResourceId"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.dispatch(c)
	}
}

func (h *Handler) dispatch(c *echo.Context) error {
	op := h.ExtractOperation(c)

	switch op {
	case opCreateDirectory:
		return h.handleCreateDirectory(c)
	case opCreateMicrosoftAD:
		return h.handleCreateMicrosoftAD(c)
	case opDeleteDirectory:
		return h.handleDeleteDirectory(c)
	case opDescribeDirectories:
		return h.handleDescribeDirectories(c)
	case opCreateAlias:
		return h.handleCreateAlias(c)
	case opEnableSso:
		return h.handleEnableSso(c)
	case opDisableSso:
		return h.handleDisableSso(c)
	case opGetDirectoryLimits:
		return h.handleGetDirectoryLimits(c)
	case opCreateSnapshot:
		return h.handleCreateSnapshot(c)
	case opDeleteSnapshot:
		return h.handleDeleteSnapshot(c)
	case opDescribeSnapshots:
		return h.handleDescribeSnapshots(c)
	case opGetSnapshotLimits:
		return h.handleGetSnapshotLimits(c)
	case opRestoreFromSnapshot:
		return h.handleRestoreFromSnapshot(c)
	case opAddTagsToResource:
		return h.handleAddTagsToResource(c)
	case opRemoveTagsFromResource:
		return h.handleRemoveTagsFromResource(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	default:
		return c.JSON(http.StatusNotImplemented, errResp("NotImplementedException", "operation not implemented: "+op))
	}
}

func (h *Handler) handleCreateDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Size        string `json:"Size"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Name is required"))
	}

	tags := reqTagsToTags(req.Tags)

	d, createErr := h.Backend.CreateDirectory(
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		DirectorySize(req.Size),
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryId": d.DirectoryID,
	})
}

func (h *Handler) handleCreateMicrosoftAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Edition     string `json:"Edition"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "Name is required"))
	}

	edition := DirectoryEdition(req.Edition)
	if edition == "" {
		edition = DirectoryEditionEnterprise
	}

	tags := reqTagsToTags(req.Tags)

	d, createErr := h.Backend.CreateMicrosoftAD(req.Name, req.ShortName, req.Description, req.Password, edition, tags)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryId": d.DirectoryID,
	})
}

func (h *Handler) handleDeleteDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if delErr := h.Backend.DeleteDirectory(req.DirectoryID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryId": req.DirectoryID,
	})
}

func (h *Handler) handleDescribeDirectories(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryIds []string `json:"DirectoryIds"`
		Limit        int32    `json:"Limit"`
		NextToken    string   `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	dirs, nextToken, listErr := h.Backend.DescribeDirectories(req.DirectoryIds, req.Limit, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	dirList := make([]map[string]any, 0, len(dirs))
	for _, d := range dirs {
		dirList = append(dirList, directoryToJSON(d))
	}

	resp := map[string]any{
		"DirectoryDescriptions": dirList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateAlias(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Alias       string `json:"Alias"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.Alias == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId and Alias are required"))
	}

	if aliasErr := h.Backend.CreateAlias(req.DirectoryID, req.Alias); aliasErr != nil {
		return h.mapError(c, aliasErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryId": req.DirectoryID,
		"Alias":       req.Alias,
	})
}

func (h *Handler) handleEnableSso(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if ssoErr := h.Backend.EnableSso(req.DirectoryID); ssoErr != nil {
		return h.mapError(c, ssoErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableSso(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if ssoErr := h.Backend.DisableSso(req.DirectoryID); ssoErr != nil {
		return h.mapError(c, ssoErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetDirectoryLimits(c *echo.Context) error {
	limits := h.Backend.GetDirectoryLimits()

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryLimits": map[string]any{
			"CloudOnlyDirectoriesCurrentCount": limits.CloudOnlyDirectoriesCurrentCount,
			"CloudOnlyDirectoriesLimit":        limits.CloudOnlyDirectoriesLimit,
			"CloudOnlyDirectoriesLimitReached": limits.CloudOnlyDirectoriesLimitReached,
			"CloudOnlyMicrosoftADCurrentCount": limits.CloudOnlyMicrosoftADCurrentCount,
			"CloudOnlyMicrosoftADLimit":        limits.CloudOnlyMicrosoftADLimit,
			"CloudOnlyMicrosoftADLimitReached": limits.CloudOnlyMicrosoftADLimitReached,
			"ConnectedDirectoriesCurrentCount": limits.ConnectedDirectoriesCurrentCount,
			"ConnectedDirectoriesLimit":        limits.ConnectedDirectoriesLimit,
			"ConnectedDirectoriesLimitReached": limits.ConnectedDirectoriesLimitReached,
		},
	})
}

func (h *Handler) handleCreateSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Name        string `json:"Name"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	snap, snapErr := h.Backend.CreateSnapshot(req.DirectoryID, req.Name)
	if snapErr != nil {
		return h.mapError(c, snapErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SnapshotId": snap.SnapshotID,
	})
}

func (h *Handler) handleDeleteSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SnapshotID string `json:"SnapshotId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SnapshotID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SnapshotId is required"))
	}

	if delErr := h.Backend.DeleteSnapshot(req.SnapshotID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SnapshotId": req.SnapshotID,
	})
}

func (h *Handler) handleDescribeSnapshots(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		SnapshotIds []string `json:"SnapshotIds"`
		Limit       int32    `json:"Limit"`
		NextToken   string   `json:"NextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	snaps, nextToken, listErr := h.Backend.DescribeSnapshots(req.DirectoryID, req.SnapshotIds, req.Limit, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	snapList := make([]map[string]any, 0, len(snaps))
	for _, s := range snaps {
		snapList = append(snapList, snapshotToJSON(s))
	}

	resp := map[string]any{
		"Snapshots": snapList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetSnapshotLimits(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	limits, limErr := h.Backend.GetSnapshotLimits(req.DirectoryID)
	if limErr != nil {
		return h.mapError(c, limErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SnapshotLimits": map[string]any{
			"ManualSnapshotsCurrentCount": limits.ManualSnapshotsCurrentCount,
			"ManualSnapshotsLimit":        limits.ManualSnapshotsLimit,
			"ManualSnapshotsLimitReached": limits.ManualSnapshotsLimitReached,
		},
	})
}

func (h *Handler) handleRestoreFromSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SnapshotID string `json:"SnapshotId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SnapshotID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SnapshotId is required"))
	}

	if restoreErr := h.Backend.RestoreFromSnapshot(req.SnapshotID); restoreErr != nil {
		return h.mapError(c, restoreErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleAddTagsToResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"ResourceId"`
		Tags       []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "ResourceId is required"))
	}

	tags := reqTagsToTags(req.Tags)

	if tagErr := h.Backend.AddTagsToResource(req.ResourceID, tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRemoveTagsFromResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string   `json:"ResourceId"`
		TagKeys    []string `json:"TagKeys"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "ResourceId is required"))
	}

	if untagErr := h.Backend.RemoveTagsFromResource(req.ResourceID, req.TagKeys); untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		ResourceID string `json:"ResourceId"`
		Limit      int32  `json:"Limit"`
		NextToken  string `json:"NextToken"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.ResourceID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "ResourceId is required"))
	}

	tags, nextToken, listErr := h.Backend.ListTagsForResource(req.ResourceID, req.Limit, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	tagList := make([]map[string]any, 0, len(tags))
	for _, t := range tags {
		tagList = append(tagList, map[string]any{
			"Key":   t.Key,
			"Value": t.Value,
		})
	}

	resp := map[string]any{
		"Tags": tagList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) mapError(c *echo.Context, err error) error {
	logger.Load(c.Request().Context()).Error("directoryservice error", "error", err)

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errResp("EntityDoesNotExistException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errResp("EntityAlreadyExistsException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errResp("ClientException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("ServiceException", err.Error()))
	}
}

func errResp(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

func directoryToJSON(d *Directory) map[string]any {
	return map[string]any{
		"DirectoryId": d.DirectoryID,
		"Name":        d.Name,
		"ShortName":   d.ShortName,
		"Description": d.Description,
		"Alias":       d.Alias,
		"AccessUrl":   d.AccessURL,
		"Type":        string(d.Type),
		"Stage":       string(d.Stage),
		"Size":        string(d.Size),
		"Edition":     string(d.Edition),
		"SsoEnabled":  d.SsoEnabled,
		"LaunchTime":  d.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snapshotToJSON(s *Snapshot) map[string]any {
	return map[string]any{
		"SnapshotId":  s.SnapshotID,
		"DirectoryId": s.DirectoryID,
		"Name":        s.Name,
		"Status":      string(s.Status),
		"Type":        string(s.Type),
		"StartTime":   s.StartTime.Format("2006-01-02T15:04:05.000Z"),
	}
}

func reqTagsToTags(raw []struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}) []Tag {
	tags := make([]Tag, 0, len(raw))
	for _, t := range raw {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}

	return tags
}
