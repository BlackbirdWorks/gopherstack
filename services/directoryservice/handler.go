package directoryservice

import (
	"encoding/json"
	"errors"
	"maps"
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

	keyDirectoryID = "DirectoryId"
	keySnapshotID  = "SnapshotId"
)

// Handler handles DirectoryService HTTP requests.
type Handler struct {
	Backend  StorageBackend
	dispatch map[string]echo.HandlerFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.dispatch = map[string]echo.HandlerFunc{
		opCreateDirectory:        h.handleCreateDirectory,
		opCreateMicrosoftAD:      h.handleCreateMicrosoftAD,
		opDeleteDirectory:        h.handleDeleteDirectory,
		opDescribeDirectories:    h.handleDescribeDirectories,
		opCreateAlias:            h.handleCreateAlias,
		opEnableSso:              h.handleEnableSso,
		opDisableSso:             h.handleDisableSso,
		opGetDirectoryLimits:     h.handleGetDirectoryLimits,
		opCreateSnapshot:         h.handleCreateSnapshot,
		opDeleteSnapshot:         h.handleDeleteSnapshot,
		opDescribeSnapshots:      h.handleDescribeSnapshots,
		opGetSnapshotLimits:      h.handleGetSnapshotLimits,
		opRestoreFromSnapshot:    h.handleRestoreFromSnapshot,
		opAddTagsToResource:      h.handleAddTagsToResource,
		opRemoveTagsFromResource: h.handleRemoveTagsFromResource,
		opListTagsForResource:    h.handleListTagsForResource,
	}

	maps.Copy(h.dispatch, h.appendixAOps())

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "DirectoryService" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	base := []string{ //nolint:prealloc // existing issue.
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

	return append(base, appendixAOpsNames()...)
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

	for _, key := range []string{keyDirectoryID, "ResourceId"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.doDispatch(c)
	}
}

func (h *Handler) doDispatch(c *echo.Context) error {
	op := h.ExtractOperation(c)
	if fn, ok := h.dispatch[op]; ok {
		return fn(c)
	}

	// AWS rejects an unrecognised operation with a 400 client error rather than 501.
	return c.JSON(http.StatusBadRequest, errResp("InvalidRequestException", "unrecognized operation: "+op))
}

func (h *Handler) handleCreateDirectory(c *echo.Context) error { //nolint:dupl // existing issue.
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
		VpcSettings *struct {
			VpcId     string   `json:"VpcId"`
			SubnetIds []string `json:"SubnetIds"`
		} `json:"VpcSettings"`
		Tags []struct {
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

	var vpcSettings *DirectoryVpcSettings
	if req.VpcSettings != nil {
		vpcSettings = &DirectoryVpcSettings{
			VpcID:     req.VpcSettings.VpcId,
			SubnetIDs: req.VpcSettings.SubnetIds,
		}
	}

	d, createErr := h.Backend.CreateDirectory(
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		DirectorySize(req.Size),
		vpcSettings,
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
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
		VpcSettings *struct {
			VpcId     string   `json:"VpcId"`
			SubnetIds []string `json:"SubnetIds"`
		} `json:"VpcSettings"`
		Tags []struct {
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

	var vpcSettings *DirectoryVpcSettings
	if req.VpcSettings != nil {
		vpcSettings = &DirectoryVpcSettings{
			VpcID:     req.VpcSettings.VpcId,
			SubnetIDs: req.VpcSettings.SubnetIds,
		}
	}

	d, createErr := h.Backend.CreateMicrosoftAD(req.Name, req.ShortName, req.Description, req.Password, edition, vpcSettings, tags)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
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
		keyDirectoryID: req.DirectoryID,
	})
}

func (h *Handler) handleDescribeDirectories(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		NextToken    string   `json:"NextToken"`
		DirectoryIDs []string `json:"DirectoryIds"`
		Limit        int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	dirs, nextToken, listErr := h.Backend.DescribeDirectories(req.DirectoryIDs, req.Limit, req.NextToken)
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
		keyDirectoryID: req.DirectoryID,
		"Alias":        req.Alias,
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
		keySnapshotID: snap.SnapshotID,
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
		keySnapshotID: req.SnapshotID,
	})
}

func (h *Handler) handleDescribeSnapshots(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		NextToken   string   `json:"NextToken"`
		SnapshotIDs []string `json:"SnapshotIds"`
		Limit       int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	snaps, nextToken, listErr := h.Backend.DescribeSnapshots(req.DirectoryID, req.SnapshotIDs, req.Limit, req.NextToken)
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
		NextToken  string `json:"NextToken"`
		Limit      int32  `json:"Limit"`
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
	out := map[string]any{
		keyDirectoryID: d.DirectoryID,
		"Name":         d.Name, //nolint:goconst // existing issue.
		"ShortName":    d.ShortName,
		"Description":  d.Description, //nolint:goconst // existing issue.
		"Alias":        d.Alias,
		"AccessUrl":    d.AccessURL,
		"Type":         string(d.Type), //nolint:goconst // existing issue.
		"Stage":        string(d.Stage),
		"Size":         string(d.Size),
		"Edition":      string(d.Edition),
		"SsoEnabled":   d.SsoEnabled,
		"LaunchTime":   float64(d.LaunchTime.Unix()),
	}
	if d.VpcSettings != nil {
		secGroups := d.VpcSettings.SecurityGroupIDs
		if secGroups == nil {
			secGroups = []string{}
		}
		azs := d.VpcSettings.AvailabilityZones
		if azs == nil {
			azs = []string{}
		}
		out["VpcSettings"] = map[string]any{
			"VpcId":            d.VpcSettings.VpcID,
			"SubnetIds":        d.VpcSettings.SubnetIDs,
			"SecurityGroupIds": secGroups,
			"AvailabilityZones": azs,
		}
	}
	return out
}

func snapshotToJSON(s *Snapshot) map[string]any {
	return map[string]any{
		keySnapshotID:  s.SnapshotID,
		keyDirectoryID: s.DirectoryID,
		"Name":         s.Name,
		"Status":       string(s.Status), //nolint:goconst // existing issue.
		"Type":         string(s.Type),
		"StartTime":    float64(s.StartTime.Unix()),
	}
}

func reqTagsToTags(raw []struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
},
) []Tag {
	tags := make([]Tag, 0, len(raw))
	for _, t := range raw {
		tags = append(tags, Tag{Key: t.Key, Value: t.Value})
	}

	return tags
}
