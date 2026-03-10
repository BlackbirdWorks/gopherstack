package amplify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	amplifyAppsPrefix        = "/apps"
	amplifyTagsPrefix        = "/tags/"
	amplifyServiceIdentifier = ":amplify"

	// Path segment counts for Amplify routes.
	pathSegsApps      = 1 // ["apps"]
	pathSegsAppID     = 2 // ["apps", "{appId}"]
	pathSegsAppSub    = 3 // ["apps", "{appId}", "branches"]
	pathSegsAppBranch = 4 // ["apps", "{appId}", "branches", "{branchName}"]

	opUnknown = "Unknown"
)

// Handler is the Echo HTTP handler for Amplify operations.
type Handler struct {
	Backend       StorageBackend
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Amplify handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Amplify" }

// GetSupportedOperations returns the list of supported Amplify operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApp",
		"GetApp",
		"ListApps",
		"DeleteApp",
		"CreateBranch",
		"GetBranch",
		"ListBranches",
		"DeleteBranch",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "amplify" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Amplify instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Amplify API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, amplifyAppsPrefix) {
			return true
		}

		// Only claim /tags/{arn} when the ARN belongs to Amplify.
		// Other services (e.g. FIS) also expose a /tags/{arn} endpoint at the
		// same path prefix; we must not steal their requests.
		if strings.HasPrefix(path, amplifyTagsPrefix) {
			arn := path[len(amplifyTagsPrefix):]

			return strings.Contains(arn, amplifyServiceIdentifier)
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the Amplify operation from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseAmplifyOperation(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the app ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := splitAmplifyPath(c.Request().URL.Path)
	// Path: /apps/{appId}/...
	const appIDIndex = 1
	if len(segs) > appIDIndex && segs[0] == "apps" {
		return segs[appIDIndex]
	}

	return ""
}

// parseAmplifyOperation derives an operation name from the HTTP method and path.
func parseAmplifyOperation(method, path string) string {
	if strings.HasPrefix(path, amplifyTagsPrefix) {
		return parseTagsOperation(method)
	}

	segs := splitAmplifyPath(path)

	switch len(segs) {
	case pathSegsApps:
		// /apps
		return parseAppsOperation(method)
	case pathSegsAppID:
		// /apps/{appId}
		return parseAppIDOperation(method)
	case pathSegsAppSub:
		// /apps/{appId}/branches
		if segs[2] == arnResourceBranches {
			return parseBranchesOperation(method)
		}
	case pathSegsAppBranch:
		// /apps/{appId}/branches/{branchName}
		if segs[2] == arnResourceBranches {
			return parseBranchOperation(method)
		}
	}

	return opUnknown
}

func parseAppsOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApp"
	case http.MethodGet:
		return "ListApps"
	default:
		return opUnknown
	}
}

func parseTagsOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodPost:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	default:
		return opUnknown
	}
}

func parseAppIDOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApp"
	case http.MethodDelete:
		return "DeleteApp"
	default:
		return opUnknown
	}
}

func parseBranchesOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateBranch"
	case http.MethodGet:
		return "ListBranches"
	default:
		return opUnknown
	}
}

func parseBranchOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetBranch"
	case http.MethodDelete:
		return "DeleteBranch"
	default:
		return opUnknown
	}
}

// splitAmplifyPath splits a URL path into non-empty segments.
func splitAmplifyPath(path string) []string {
	var segs []string

	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// extractResourceARN extracts and URL-decodes the resource ARN from a /tags/{arn} path.
func extractResourceARN(rawPath, decodedPath string) string {
	if rawPath == "" {
		rawPath = decodedPath
	}

	const tagsPrefix = "/tags/"
	encoded := strings.TrimPrefix(rawPath, tagsPrefix)

	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}

	return decoded
}

// Handler returns the Echo handler function for Amplify requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		method := c.Request().Method
		path := c.Request().URL.Path
		log := logger.Load(ctx)

		log.DebugContext(ctx, "Amplify request", "method", method, "path", path)

		if strings.HasPrefix(path, amplifyTagsPrefix) {
			return h.handleTags(ctx, c)
		}

		segs := splitAmplifyPath(path)

		if len(segs) == 0 || segs[0] != "apps" {
			return c.JSON(http.StatusNotFound, amplifyError("not found"))
		}

		switch len(segs) {
		case pathSegsApps:
			return h.handleApps(ctx, c)
		case pathSegsAppID:
			return h.handleAppID(ctx, c, segs[1])
		case pathSegsAppSub:
			if segs[2] == arnResourceBranches {
				return h.handleBranches(ctx, c, segs[1])
			}

			return c.JSON(http.StatusNotFound, amplifyError("not found"))
		case pathSegsAppBranch:
			if segs[2] == arnResourceBranches {
				return h.handleBranchName(ctx, c, segs[1], segs[3])
			}

			return c.JSON(http.StatusNotFound, amplifyError("not found"))
		default:
			return c.JSON(http.StatusNotFound, amplifyError("not found"))
		}
	}
}

// handleApps handles POST/GET /apps.
func (h *Handler) handleApps(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createApp(ctx, c)
	case http.MethodGet:
		return h.listApps(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, amplifyError("method not allowed"))
	}
}

// handleAppID handles GET/DELETE /apps/{appId}.
func (h *Handler) handleAppID(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getApp(ctx, c, appID)
	case http.MethodDelete:
		return h.deleteApp(ctx, c, appID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, amplifyError("method not allowed"))
	}
}

// handleBranches handles POST/GET /apps/{appId}/branches.
func (h *Handler) handleBranches(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createBranch(ctx, c, appID)
	case http.MethodGet:
		return h.listBranches(ctx, c, appID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, amplifyError("method not allowed"))
	}
}

// handleBranchName handles GET/DELETE /apps/{appId}/branches/{branchName}.
func (h *Handler) handleBranchName(ctx context.Context, c *echo.Context, appID, branchName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getBranch(ctx, c, appID, branchName)
	case http.MethodDelete:
		return h.deleteBranch(ctx, c, appID, branchName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, amplifyError("method not allowed"))
	}
}

// handleTags dispatches GET/POST/DELETE /tags/{resourceArn}.
func (h *Handler) handleTags(ctx context.Context, c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.RawPath, c.Request().URL.Path)

	switch c.Request().Method {
	case http.MethodGet:
		return h.listTagsForResource(ctx, c, resourceARN)
	case http.MethodPost:
		return h.tagResource(ctx, c, resourceARN)
	case http.MethodDelete:
		return h.untagResource(ctx, c, resourceARN)
	default:
		return c.JSON(http.StatusMethodNotAllowed, amplifyError("method not allowed"))
	}
}

// createApp handles POST /apps.
func (h *Handler) createApp(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, amplifyError(err.Error()))
	}

	var input struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Repository  string            `json:"repository"`
		Platform    string            `json:"platform"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, amplifyError("invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, amplifyError("name is required"))
	}

	app, createErr := h.Backend.CreateApp(input.Name, input.Description, input.Repository, input.Platform, input.Tags)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateApp", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"app": toAppView(app)})
}

// getApp handles GET /apps/{appId}.
func (h *Handler) getApp(ctx context.Context, c *echo.Context, appID string) error {
	app, err := h.Backend.GetApp(appID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetApp", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"app": toAppView(app)})
}

// listApps handles GET /apps.
func (h *Handler) listApps(ctx context.Context, c *echo.Context) error {
	apps, err := h.Backend.ListApps()
	if err != nil {
		return h.handleBackendError(ctx, c, "ListApps", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"apps": toAppViews(apps)})
}

// deleteApp handles DELETE /apps/{appId}.
func (h *Handler) deleteApp(ctx context.Context, c *echo.Context, appID string) error {
	if err := h.Backend.DeleteApp(appID); err != nil {
		return h.handleBackendError(ctx, c, "DeleteApp", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// createBranch handles POST /apps/{appId}/branches.
func (h *Handler) createBranch(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, amplifyError(err.Error()))
	}

	var input struct {
		Tags            map[string]string `json:"tags"`
		BranchName      string            `json:"branchName"`
		Description     string            `json:"description"`
		Stage           string            `json:"stage"`
		EnableAutoBuild bool              `json:"enableAutoBuild"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, amplifyError("invalid request body"))
	}

	if input.BranchName == "" {
		return c.JSON(http.StatusBadRequest, amplifyError("branchName is required"))
	}

	branch, createErr := h.Backend.CreateBranch(
		appID,
		input.BranchName,
		input.Description,
		input.Stage,
		input.EnableAutoBuild,
		input.Tags,
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateBranch", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"branch": toBranchView(branch)})
}

// getBranch handles GET /apps/{appId}/branches/{branchName}.
func (h *Handler) getBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	branch, err := h.Backend.GetBranch(appID, branchName)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetBranch", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"branch": toBranchView(branch)})
}

// listBranches handles GET /apps/{appId}/branches.
func (h *Handler) listBranches(ctx context.Context, c *echo.Context, appID string) error {
	branches, err := h.Backend.ListBranches(appID)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListBranches", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"branches": toBranchViews(branches)})
}

// deleteBranch handles DELETE /apps/{appId}/branches/{branchName}.
func (h *Handler) deleteBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if err := h.Backend.DeleteBranch(appID, branchName); err != nil {
		return h.handleBackendError(ctx, c, "DeleteBranch", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// listTagsForResource handles GET /tags/{resourceArn}.
func (h *Handler) listTagsForResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tagMap, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListTagsForResource", err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagMap})
}

// tagResource handles POST /tags/{resourceArn}.
func (h *Handler) tagResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, amplifyError(err.Error()))
	}

	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, amplifyError("invalid request body"))
	}

	if tagErr := h.Backend.TagResource(resourceARN, input.Tags); tagErr != nil {
		return h.handleBackendError(ctx, c, "TagResource", tagErr)
	}

	return c.NoContent(http.StatusOK)
}

// untagResource handles DELETE /tags/{resourceArn}?tagKeys=key1&tagKeys=key2.
func (h *Handler) untagResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.handleBackendError(ctx, c, "UntagResource", untagErr)
	}

	return c.NoContent(http.StatusOK)
}

// handleBackendError maps backend errors to appropriate HTTP responses.
func (h *Handler) handleBackendError(ctx context.Context, c *echo.Context, op string, err error) error {
	log := logger.Load(ctx)
	log.ErrorContext(ctx, "Amplify operation failed", "operation", op, "error", err)

	if errors.Is(err, awserr.ErrNotFound) {
		return c.JSON(http.StatusNotFound, amplifyError(err.Error()))
	}

	if errors.Is(err, awserr.ErrAlreadyExists) || errors.Is(err, awserr.ErrConflict) {
		return c.JSON(http.StatusBadRequest, amplifyError(err.Error()))
	}

	return c.JSON(http.StatusInternalServerError, amplifyError(fmt.Sprintf("internal error: %s", err.Error())))
}

// appView is the JSON representation of an App with timestamps as Unix epoch
// float64 values, as required by the AWS SDK v2 deserialiser.
type appView struct {
	Tags          map[string]string `json:"tags,omitempty"`
	AppID         string            `json:"appId"`
	ARN           string            `json:"appArn"`
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	Repository    string            `json:"repository,omitempty"`
	DefaultDomain string            `json:"defaultDomain,omitempty"`
	Platform      Platform          `json:"platform"`
	CreateTime    float64           `json:"createTime"`
	UpdateTime    float64           `json:"updateTime"`
}

// branchView is the JSON representation of a Branch with timestamps as Unix
// epoch float64 values, as required by the AWS SDK v2 deserialiser.
type branchView struct {
	Tags            map[string]string `json:"tags,omitempty"`
	AppID           string            `json:"appId"`
	BranchARN       string            `json:"branchArn"`
	BranchName      string            `json:"branchName"`
	Description     string            `json:"description,omitempty"`
	Stage           Stage             `json:"stage,omitempty"`
	CreateTime      float64           `json:"createTime"`
	UpdateTime      float64           `json:"updateTime"`
	EnableAutoBuild bool              `json:"enableAutoBuild"`
}

func toAppView(a *App) appView {
	var tagMap map[string]string
	if a.Tags != nil {
		tagMap = a.Tags.Clone()
	}

	return appView{
		Tags:          tagMap,
		CreateTime:    float64(a.CreateTime.Unix()),
		UpdateTime:    float64(a.UpdateTime.Unix()),
		AppID:         a.AppID,
		ARN:           a.ARN,
		Name:          a.Name,
		Description:   a.Description,
		Repository:    a.Repository,
		DefaultDomain: a.DefaultDomain,
		Platform:      a.Platform,
	}
}

func toBranchView(b *Branch) branchView {
	var tagMap map[string]string
	if b.Tags != nil {
		tagMap = b.Tags.Clone()
	}

	return branchView{
		Tags:            tagMap,
		CreateTime:      float64(b.CreateTime.Unix()),
		UpdateTime:      float64(b.UpdateTime.Unix()),
		AppID:           b.AppID,
		BranchARN:       b.BranchARN,
		BranchName:      b.BranchName,
		Description:     b.Description,
		Stage:           b.Stage,
		EnableAutoBuild: b.EnableAutoBuild,
	}
}

func toAppViews(apps []*App) []appView {
	views := make([]appView, len(apps))
	for i, a := range apps {
		views[i] = toAppView(a)
	}

	return views
}

func toBranchViews(branches []*Branch) []branchView {
	views := make([]branchView, len(branches))
	for i, b := range branches {
		views[i] = toBranchView(b)
	}

	return views
}

// amplifyError builds a standard Amplify error response body.
func amplifyError(message string) map[string]any {
	return map[string]any{"message": message}
}
