package resourcegroups

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrUnknownOperation is returned when the requested Resource Groups operation is not supported.
	ErrUnknownOperation = errors.New("UnknownOperationException")
	errInvalidRequest   = errors.New("invalid request")
)

const resourceGroupsTargetPrefix = "ResourceGroups."

// extractResourceNameInput is used to parse the group name from various Resource Groups request bodies.
type extractResourceNameInput struct {
	Name      string `json:"Name"`
	GroupName string `json:"GroupName"`
	Group     string `json:"Group"`
}

// tagResourceInput is the JSON request body for tagging a resource.
type tagResourceInput struct {
	Tags map[string]string `json:"Tags"`
}

// untagResourceInput is the JSON request body for untagging a resource.
type untagResourceInput struct {
	Keys []string `json:"Keys"`
}

// rgRESTPathOps is the static mapping of REST API paths to Resource Groups operation names.
var rgRESTPathOps = map[string]string{ //nolint:gochecknoglobals // lookup table for REST path routing
	"/groups":                  "CreateGroup",
	"/get-group":               "GetGroup",
	"/delete-group":            "DeleteGroup",
	"/groups-list":             "ListGroups",
	"/get-group-query":         "GetGroupQuery",
	"/get-group-configuration": "GetGroupConfiguration",
}

type groupNameInput struct {
	// Group accepts the REST API "Group" field. The value is used as-is as the group
	// name. When set and GroupName is empty, its value is used to look up the group.
	Group     string `json:"Group"`
	GroupName string `json:"GroupName"`
}

// resolvedName returns the group name to use for backend operations.
// GroupName takes precedence over Group to match the preferred lookup field.
func (g *groupNameInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

// Handler is the Echo HTTP handler for Resource Groups operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Resource Groups handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResourceGroups" }

// GetSupportedOperations returns the list of supported Resource Groups operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGroup",
		"DeleteGroup",
		"ListGroups",
		"GetGroup",
		"GetGroupQuery",
		"GetGroupConfiguration",
		"GetTags",
		"Tag",
		"Untag",
	}
}

// isResourceTagsPath reports whether path matches the pattern /resources/{Arn}/tags.
// The ARN segment must be non-empty, so the path must be longer than "/resources/" + "/tags".
func isResourceTagsPath(path string) bool {
	const prefix = "/resources/"
	const suffix = "/tags"

	return strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix) && len(path) > len(prefix)+len(suffix)
}

// arnFromResourceTagsPath extracts the ARN from a /resources/{Arn}/tags path.
func arnFromResourceTagsPath(path string) string {
	return path[len("/resources/") : len(path)-len("/tags")]
}

// RouteMatcher returns a function that matches Resource Groups requests.
// It matches both X-Amz-Target (JSON protocol) and REST API paths used by the AWS SDK.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), resourceGroupsTargetPrefix) {
			return true
		}

		_, isREST := rgRESTPathOps[c.Request().URL.Path]

		return isREST || isResourceTagsPath(c.Request().URL.Path)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Resource Groups action from the X-Amz-Target header or REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, resourceGroupsTargetPrefix)
	if action != "" && action != target {
		return action
	}

	if op, ok := rgRESTPathOps[c.Request().URL.Path]; ok {
		return op
	}

	if isResourceTagsPath(c.Request().URL.Path) {
		switch c.Request().Method {
		case http.MethodGet:
			return "GetTags"
		case http.MethodPut:
			return "Tag"
		case http.MethodPatch:
			return "Untag"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the group name from the request body, checking
// Name (CreateGroup), GroupName, and Group (REST API) fields.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractResourceNameInput
	_ = json.Unmarshal(body, &req)

	if req.Name != "" {
		return req.Name
	}

	if req.GroupName != "" {
		return req.GroupName
	}

	return req.Group
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Dynamic REST paths: GET|PUT|PATCH /resources/{Arn}/tags
		if isResourceTagsPath(c.Request().URL.Path) {
			return h.handleResourceTags(c)
		}

		// Static REST API paths: POST /groups, /get-group, /delete-group, etc.
		// Only POST is accepted; other methods get 405 to avoid misrouting.
		if op, ok := rgRESTPathOps[c.Request().URL.Path]; ok {
			if c.Request().Method != http.MethodPost {
				return c.NoContent(http.StatusMethodNotAllowed)
			}

			return h.handleREST(c, op)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ResourceGroups", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// handleREST handles Resource Groups REST API calls routed by path.
func (h *Handler) handleREST(c *echo.Context, action string) error {
	ctx := c.Request().Context()

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	response, dispErr := h.dispatch(ctx, action, body)
	if dispErr != nil {
		return h.handleError(ctx, c, action, dispErr)
	}

	return c.JSONBlob(http.StatusOK, response)
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateGroup":           service.WrapOp(h.handleCreateGroup),
		"DeleteGroup":           service.WrapOp(h.handleDeleteGroup),
		"ListGroups":            service.WrapOp(h.handleListGroups),
		"GetGroup":              service.WrapOp(h.handleGetGroup),
		"GetGroupQuery":         service.WrapOp(h.handleGetGroupQuery),
		"GetGroupConfiguration": service.WrapOp(h.handleGetGroupConfiguration),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, ErrUnknownOperation
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, errInvalidRequest), errors.Is(err, ErrUnknownOperation),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code = http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

type handleCreateGroupInput struct {
	Tags          *tags.Tags     `json:"Tags"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	Name          string         `json:"Name"`
	Description   string         `json:"Description"`
}

type createGroupOutput struct {
	Group         *Group         `json:"Group"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
}

func (h *Handler) handleCreateGroup(_ context.Context, in *handleCreateGroupInput) (*createGroupOutput, error) {
	g, err := h.Backend.CreateGroup(in.Name, in.Description, in.ResourceQuery, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createGroupOutput{Group: g, ResourceQuery: g.ResourceQuery}, nil
}

type deleteGroupOutput struct{}

func (h *Handler) handleDeleteGroup(_ context.Context, in *groupNameInput) (*deleteGroupOutput, error) {
	if err := h.Backend.DeleteGroup(in.resolvedName()); err != nil {
		return nil, err
	}

	return &deleteGroupOutput{}, nil
}

type listGroupsInput struct{}

type listGroupsOutput struct {
	GroupIdentifiers []Group `json:"GroupIdentifiers"`
}

func (h *Handler) handleListGroups(_ context.Context, _ *listGroupsInput) (*listGroupsOutput, error) {
	groups := h.Backend.ListGroups()

	return &listGroupsOutput{GroupIdentifiers: groups}, nil
}

type getGroupOutput struct {
	Group *Group `json:"Group"`
}

func (h *Handler) handleGetGroup(_ context.Context, in *groupNameInput) (*getGroupOutput, error) {
	g, err := h.Backend.GetGroup(in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupOutput{Group: g}, nil
}

type getGroupQueryOutput struct {
	GroupQuery *groupQueryOutput `json:"GroupQuery"`
}

type groupQueryOutput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	GroupName     string         `json:"GroupName"`
}

func (h *Handler) handleGetGroupQuery(_ context.Context, in *groupNameInput) (*getGroupQueryOutput, error) {
	g, err := h.Backend.GetGroup(in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupQueryOutput{GroupQuery: &groupQueryOutput{
		GroupName:     g.Name,
		ResourceQuery: g.ResourceQuery,
	}}, nil
}

type getGroupConfigurationOutput struct {
	GroupConfiguration *groupConfigurationOutput `json:"GroupConfiguration"`
}

type groupConfigurationOutput struct {
	GroupName     string            `json:"GroupName"`
	Configuration []json.RawMessage `json:"Configuration"`
}

func (h *Handler) handleGetGroupConfiguration(
	_ context.Context,
	in *groupNameInput,
) (*getGroupConfigurationOutput, error) {
	g, err := h.Backend.GetGroup(in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupConfigurationOutput{GroupConfiguration: &groupConfigurationOutput{
		GroupName:     g.Name,
		Configuration: []json.RawMessage{},
	}}, nil
}

// handleResourceTags routes GET/PUT/PATCH /resources/{Arn}/tags to the
// GetTags, Tag, and Untag operations respectively.
func (h *Handler) handleResourceTags(c *echo.Context) error {
	ctx := c.Request().Context()
	resourceARN := arnFromResourceTagsPath(c.Request().URL.Path)
	log := logger.Load(ctx)

	switch c.Request().Method {
	case http.MethodGet:
		tagMap, err := h.Backend.GetTagsByARN(resourceARN)
		if err != nil {
			return h.handleError(ctx, c, "GetTags", err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			"Arn":  resourceARN,
			"Tags": tagMap,
		})

	case http.MethodPut:
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read Tag request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		var in tagResourceInput

		if err = json.Unmarshal(body, &in); err != nil {
			return h.handleError(ctx, c, "Tag", errInvalidRequest)
		}

		tagMap, err := h.Backend.AddTagsByARN(resourceARN, in.Tags)
		if err != nil {
			return h.handleError(ctx, c, "Tag", err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			"Arn":  resourceARN,
			"Tags": tagMap,
		})

	case http.MethodPatch:
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read Untag request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		var in untagResourceInput

		if err = json.Unmarshal(body, &in); err != nil {
			return h.handleError(ctx, c, "Untag", errInvalidRequest)
		}

		if err = h.Backend.RemoveTagsByARN(resourceARN, in.Keys); err != nil {
			return h.handleError(ctx, c, "Untag", err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			"Arn":  resourceARN,
			"Keys": in.Keys,
		})

	default:
		return c.NoContent(http.StatusMethodNotAllowed)
	}
}
