package resourcegroups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	keyArn = "Arn"
)

const (
	opCreateGroup           = "CreateGroup"
	opGetGroup              = "GetGroup"
	opDeleteGroup           = "DeleteGroup"
	opListGroups            = "ListGroups"
	opGetGroupQuery         = "GetGroupQuery"
	opGetGroupConfiguration = "GetGroupConfiguration"
	opUpdateGroup           = "UpdateGroup"
	opUpdateGroupQuery      = "UpdateGroupQuery"
	opCancelTagSyncTask     = "CancelTagSyncTask"
	opGetAccountSettings    = "GetAccountSettings"
	opGetTagSyncTask        = "GetTagSyncTask"
	opGroupResources        = "GroupResources"
	opListGroupResources    = "ListGroupResources"
	opListGroupingStatuses  = "ListGroupingStatuses"
	opListTagSyncTasks      = "ListTagSyncTasks"
	opPutGroupConfiguration = "PutGroupConfiguration"
	opSearchResources       = "SearchResources"
	opStartTagSyncTask      = "StartTagSyncTask"
	opUpdateAccountSettings = "UpdateAccountSettings"
	opUngroupResources      = "UngroupResources"
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
	"/groups":                  opCreateGroup,
	"/get-group":               opGetGroup,
	"/delete-group":            opDeleteGroup,
	"/groups-list":             opListGroups,
	"/get-group-query":         opGetGroupQuery,
	"/get-group-configuration": opGetGroupConfiguration,
	"/update-group":            opUpdateGroup,
	"/update-group-query":      opUpdateGroupQuery,
	// New operations
	"/cancel-tag-sync-task":    opCancelTagSyncTask,
	"/get-account-settings":    opGetAccountSettings,
	"/get-tag-sync-task":       opGetTagSyncTask,
	"/group-resources":         opGroupResources,
	"/list-group-resources":    opListGroupResources,
	"/list-grouping-statuses":  opListGroupingStatuses,
	"/list-tag-sync-tasks":     opListTagSyncTasks,
	"/put-group-configuration": opPutGroupConfiguration,
	"/resources/search":        opSearchResources,
	"/start-tag-sync-task":     opStartTagSyncTask,
	"/update-account-settings": opUpdateAccountSettings,
	"/ungroup-resources":       opUngroupResources,
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
	ops     map[string]service.JSONOpFunc
	Backend StorageBackend
}

// NewHandler creates a new Resource Groups handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps constructs the static dispatch table once at handler creation.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateGroup:           service.WrapOp(h.handleCreateGroup),
		opDeleteGroup:           service.WrapOp(h.handleDeleteGroup),
		opListGroups:            service.WrapOp(h.handleListGroups),
		opGetGroup:              service.WrapOp(h.handleGetGroup),
		opGetGroupQuery:         service.WrapOp(h.handleGetGroupQuery),
		opGetGroupConfiguration: service.WrapOp(h.handleGetGroupConfiguration),
		opUpdateGroup:           service.WrapOp(h.handleUpdateGroup),
		opUpdateGroupQuery:      service.WrapOp(h.handleUpdateGroupQuery),
		// New operations
		opCancelTagSyncTask:     service.WrapOp(h.handleCancelTagSyncTask),
		opGetAccountSettings:    service.WrapOp(h.handleGetAccountSettings),
		opGetTagSyncTask:        service.WrapOp(h.handleGetTagSyncTask),
		opGroupResources:        service.WrapOp(h.handleGroupResources),
		opListGroupResources:    service.WrapOp(h.handleListGroupResources),
		opListGroupingStatuses:  service.WrapOp(h.handleListGroupingStatuses),
		opListTagSyncTasks:      service.WrapOp(h.handleListTagSyncTasks),
		opPutGroupConfiguration: service.WrapOp(h.handlePutGroupConfiguration),
		opSearchResources:       service.WrapOp(h.handleSearchResources),
		opStartTagSyncTask:      service.WrapOp(h.handleStartTagSyncTask),
		opUpdateAccountSettings: service.WrapOp(h.handleUpdateAccountSettings),
		opUngroupResources:      service.WrapOp(h.handleUngroupResources),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResourceGroups" }

// GetSupportedOperations returns the list of supported Resource Groups operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCancelTagSyncTask,
		opCreateGroup,
		opDeleteGroup,
		opGetAccountSettings,
		opGetGroup,
		opGetGroupConfiguration,
		opGetGroupQuery,
		opGetTagSyncTask,
		"GetTags",
		opGroupResources,
		opListGroupResources,
		opListGroupingStatuses,
		opListGroups,
		opListTagSyncTasks,
		opPutGroupConfiguration,
		opSearchResources,
		opStartTagSyncTask,
		"Tag",
		opUngroupResources,
		"Untag",
		opUpdateAccountSettings,
		opUpdateGroup,
		opUpdateGroupQuery,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "resource-groups" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Resource Groups instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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
		case http.MethodPatch, http.MethodDelete:
			return "Untag"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the group name from the request body, checking
// Name (CreateGroup), GroupName, and Group (REST API) fields.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
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
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)

		// Dynamic REST paths: GET|PUT|PATCH /resources/{Arn}/tags
		if isResourceTagsPath(c.Request().URL.Path) {
			return h.handleResourceTags(ctx, c)
		}

		// Static REST API paths: POST /groups, /get-group, /delete-group, etc.
		// Only POST is accepted; other methods get 405 to avoid misrouting.
		if op, ok := rgRESTPathOps[c.Request().URL.Path]; ok {
			if c.Request().Method != http.MethodPost {
				return c.NoContent(http.StatusMethodNotAllowed)
			}

			return h.handleREST(ctx, c, op)
		}

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ResourceGroups", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(innerCtx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(innerCtx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

// handleREST handles Resource Groups REST API calls routed by path.
func (h *Handler) handleREST(ctx context.Context, c *echo.Context, action string) error {
	body, err := httputils.ReadBody(c.Request())
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

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	case errors.Is(err, ErrValidation):
		code = http.StatusBadRequest
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrTagSyncTaskNotFound):
		code = http.StatusNotFound
	}

	return c.JSON(code, map[string]string{"message": err.Error()})
}

type handleCreateGroupInput struct {
	Name          string                   `json:"Name"`
	Description   string                   `json:"Description"`
	Tags          *tags.Tags               `json:"Tags"`
	ResourceQuery *ResourceQuery           `json:"ResourceQuery"`
	Configuration []GroupConfigurationItem `json:"Configuration"`
}

type groupConfigurationBody struct {
	Status        string                   `json:"Status,omitempty"`
	Configuration []GroupConfigurationItem `json:"Configuration,omitempty"`
}

// createGroupOutput mirrors CreateGroupOutput: Tags is a top-level sibling of
// Group (types.Group itself carries no Tags member), matching the real API.
type createGroupOutput struct {
	Group              *getGroupBody           `json:"Group"`
	ResourceQuery      *ResourceQuery          `json:"ResourceQuery,omitempty"`
	GroupConfiguration *groupConfigurationBody `json:"GroupConfiguration,omitempty"`
	Tags               map[string]string       `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateGroup(ctx context.Context, in *handleCreateGroupInput) (*createGroupOutput, error) {
	g, err := h.Backend.CreateGroup(ctx, in.Name, in.Description, in.ResourceQuery, in.Tags, in.Configuration)
	if err != nil {
		return nil, err
	}

	out := &createGroupOutput{
		Group:         groupBodyFromGroup(g),
		ResourceQuery: g.ResourceQuery,
	}

	if g.Tags != nil {
		if tagMap := g.Tags.Clone(); len(tagMap) > 0 {
			out.Tags = tagMap
		}
	}

	if len(in.Configuration) > 0 {
		out.GroupConfiguration = &groupConfigurationBody{
			Configuration: in.Configuration,
			Status:        "UPDATE_COMPLETE",
		}
	}

	return out, nil
}

// deleteGroupOutput mirrors DeleteGroupOutput: AWS echoes back the deleted
// group's description in the response.
type deleteGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleDeleteGroup(ctx context.Context, in *groupNameInput) (*deleteGroupOutput, error) {
	g, err := h.Backend.DeleteGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &deleteGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type listGroupsInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListGroupsFilter `json:"Filters"`
	NextToken  string             `json:"NextToken"`
	MaxResults int                `json:"MaxResults"`
}

type listGroupIdentifierOutput struct {
	GroupName   string `json:"GroupName"`
	GroupArn    string `json:"GroupArn"`
	Description string `json:"Description,omitempty"`
	Owner       string `json:"Owner,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Criticality int    `json:"Criticality,omitempty"`
}

type listGroupsGroupOutput struct {
	GroupArn    string `json:"GroupArn"`
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
	Owner       string `json:"Owner,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Criticality int    `json:"Criticality,omitempty"`
}

type listGroupsOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Groups           []listGroupsGroupOutput     `json:"Groups"`
	GroupIdentifiers []listGroupIdentifierOutput `json:"GroupIdentifiers"`
	NextToken        string                      `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroups(ctx context.Context, in *listGroupsInput) (*listGroupsOutput, error) {
	groups, nextToken := h.Backend.ListGroups(ctx, in.Filters, in.NextToken, in.MaxResults)
	identifiers := make([]listGroupIdentifierOutput, 0, len(groups))
	groupsList := make([]listGroupsGroupOutput, 0, len(groups))

	for _, group := range groups {
		identifiers = append(identifiers, listGroupIdentifierOutput{
			GroupName:   group.Name,
			GroupArn:    group.ARN,
			Description: group.Description,
			Owner:       group.Owner,
			DisplayName: group.DisplayName,
			Criticality: group.Criticality,
		})
		groupsList = append(groupsList, listGroupsGroupOutput{
			GroupArn:    group.ARN,
			Name:        group.Name,
			Description: group.Description,
			Owner:       group.Owner,
			DisplayName: group.DisplayName,
			Criticality: group.Criticality,
		})
	}

	return &listGroupsOutput{Groups: groupsList, GroupIdentifiers: identifiers, NextToken: nextToken}, nil
}

// getGroupBody is the AWS-accurate wire shape of types.Group: it deliberately
// excludes Tags and ResourceQuery, which travel as separate top-level
// response fields (see createGroupOutput and getGroupQueryOutput) and are not
// members of the Group shape itself.
type getGroupBody struct {
	ApplicationTag map[string]string `json:"ApplicationTag,omitempty"`
	GroupArn       string            `json:"GroupArn"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
	Owner          string            `json:"Owner,omitempty"`
	DisplayName    string            `json:"DisplayName,omitempty"`
	Criticality    int               `json:"Criticality,omitempty"`
}

// groupBodyFromGroup builds the AWS wire-shaped Group body from the backend's
// internal representation.
func groupBodyFromGroup(g *Group) *getGroupBody {
	return &getGroupBody{
		GroupArn:       g.ARN,
		Name:           g.Name,
		Description:    g.Description,
		Owner:          g.Owner,
		DisplayName:    g.DisplayName,
		Criticality:    g.Criticality,
		ApplicationTag: g.ApplicationTag,
	}
}

type getGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleGetGroup(ctx context.Context, in *groupNameInput) (*getGroupOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type getGroupQueryOutput struct {
	GroupQuery *groupQueryOutput `json:"GroupQuery"`
}

type groupQueryOutput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	GroupName     string         `json:"GroupName"`
}

func (h *Handler) handleGetGroupQuery(ctx context.Context, in *groupNameInput) (*getGroupQueryOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	return &getGroupQueryOutput{GroupQuery: &groupQueryOutput{
		GroupName:     g.Name,
		ResourceQuery: g.ResourceQuery,
	}}, nil
}

// getGroupConfigurationOutput mirrors GetGroupConfigurationOutput. The real
// GroupConfiguration shape has no GroupName member (unlike GroupQuery); it
// only carries Configuration, FailureReason, ProposedConfiguration, and
// Status, so it reuses groupConfigurationBody rather than a bespoke type.
type getGroupConfigurationOutput struct {
	GroupConfiguration *groupConfigurationBody `json:"GroupConfiguration"`
}

func (h *Handler) handleGetGroupConfiguration(
	ctx context.Context,
	in *groupNameInput,
) (*getGroupConfigurationOutput, error) {
	g, err := h.Backend.GetGroup(ctx, in.resolvedName())
	if err != nil {
		return nil, err
	}

	items, err := h.Backend.GetGroupConfigurationItems(ctx, g.Name)
	if err != nil {
		return nil, err
	}

	body := &groupConfigurationBody{Configuration: items}
	if len(items) > 0 {
		body.Status = "UPDATE_COMPLETE"
	}

	return &getGroupConfigurationOutput{GroupConfiguration: body}, nil
}

type updateGroupInput struct {
	Group       string `json:"Group"`
	GroupName   string `json:"GroupName"`
	Description string `json:"Description"`
	DisplayName string `json:"DisplayName"`
	Criticality int    `json:"Criticality"`
}

func (g *updateGroupInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type updateGroupOutput struct {
	Group *getGroupBody `json:"Group"`
}

func (h *Handler) handleUpdateGroup(ctx context.Context, in *updateGroupInput) (*updateGroupOutput, error) {
	name := in.resolvedName()
	if name == "" {
		return nil, fmt.Errorf("%w: Group or GroupName is required", ErrValidation)
	}

	g, err := h.Backend.UpdateGroup(ctx, name, in.Description, in.DisplayName, in.Criticality)
	if err != nil {
		return nil, err
	}

	return &updateGroupOutput{Group: groupBodyFromGroup(g)}, nil
}

type updateGroupQueryInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	Group         string         `json:"Group"`
	GroupName     string         `json:"GroupName"`
}

func (g *updateGroupQueryInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type updateGroupQueryOutput struct {
	GroupQuery *groupQueryOutput `json:"GroupQuery"`
}

func (h *Handler) handleUpdateGroupQuery(
	ctx context.Context,
	in *updateGroupQueryInput,
) (*updateGroupQueryOutput, error) {
	name := in.resolvedName()
	if name == "" {
		return nil, fmt.Errorf("%w: Group or GroupName is required", ErrValidation)
	}

	g, err := h.Backend.UpdateGroupQuery(ctx, name, in.ResourceQuery)
	if err != nil {
		return nil, err
	}

	return &updateGroupQueryOutput{GroupQuery: &groupQueryOutput{
		GroupName:     g.Name,
		ResourceQuery: g.ResourceQuery,
	}}, nil
}

// handleTagRequest handles PUT /resources/{Arn}/tags (Tag operation).
func (h *Handler) handleTagRequest(ctx context.Context, c *echo.Context, log *slog.Logger, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "failed to read Tag request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var in tagResourceInput

	if err = json.Unmarshal(body, &in); err != nil {
		return h.handleError(ctx, c, "Tag", errInvalidRequest)
	}

	tagMap, err := h.Backend.AddTagsByARN(ctx, resourceARN, in.Tags)
	if err != nil {
		return h.handleError(ctx, c, "Tag", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyArn: resourceARN,
		"Tags": tagMap,
	})
}

// handleUntagRequest handles DELETE /resources/{Arn}/tags (Untag operation).
// Keys may come from query params or request body.
func (h *Handler) handleUntagRequest(ctx context.Context, c *echo.Context, log *slog.Logger, resourceARN string) error {
	keys, err := h.extractUntagKeys(ctx, c, log)
	if err != nil {
		return err
	}

	if err = h.Backend.RemoveTagsByARN(ctx, resourceARN, keys); err != nil {
		return h.handleError(ctx, c, "Untag", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyArn: resourceARN,
		"Keys": keys,
	})
}

// extractUntagKeys parses tag keys from query params or body for the Untag operation.
func (h *Handler) extractUntagKeys(ctx context.Context, c *echo.Context, log *slog.Logger) ([]string, error) {
	keysParam := c.Request().URL.Query().Get("keys")
	if keysParam != "" {
		return strings.Split(keysParam, ","), nil
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "failed to read Untag request body", "error", err)

		return nil, c.String(http.StatusInternalServerError, "internal server error")
	}

	if len(body) == 0 {
		return nil, nil
	}

	var in untagResourceInput
	if err = json.Unmarshal(body, &in); err != nil {
		return nil, h.handleError(ctx, c, "Untag", errInvalidRequest)
	}

	return in.Keys, nil
}

// handleResourceTags routes GET/PUT/DELETE/PATCH /resources/{Arn}/tags to the
// GetTags, Tag, and Untag operations respectively.
func (h *Handler) handleResourceTags(ctx context.Context, c *echo.Context) error {
	resourceARN := arnFromResourceTagsPath(c.Request().URL.Path)
	log := logger.Load(ctx)

	switch c.Request().Method {
	case http.MethodGet:
		tagMap, err := h.Backend.GetTagsByARN(ctx, resourceARN)
		if err != nil {
			return h.handleError(ctx, c, "GetTags", err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyArn: resourceARN,
			"Tags": tagMap,
		})

	case http.MethodPut:
		return h.handleTagRequest(ctx, c, log, resourceARN)

	case http.MethodDelete:
		return h.handleUntagRequest(ctx, c, log, resourceARN)

	case http.MethodPatch:
		// PATCH kept as compat alias for existing tests; AWS uses DELETE.
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read Untag request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		var in untagResourceInput

		if err = json.Unmarshal(body, &in); err != nil {
			return h.handleError(ctx, c, "Untag", errInvalidRequest)
		}

		if err = h.Backend.RemoveTagsByARN(ctx, resourceARN, in.Keys); err != nil {
			return h.handleError(ctx, c, "Untag", err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyArn: resourceARN,
			"Keys": in.Keys,
		})

	default:
		return c.NoContent(http.StatusMethodNotAllowed)
	}
}

// --- New operations ---

// handleGetAccountSettings returns the account-level Resource Groups settings.
type getAccountSettingsInput struct{}

type getAccountSettingsOutput struct {
	AccountSettings AccountSettings `json:"AccountSettings"`
}

func (h *Handler) handleGetAccountSettings(
	_ context.Context,
	_ *getAccountSettingsInput,
) (*getAccountSettingsOutput, error) {
	settings := h.Backend.GetAccountSettings()

	return &getAccountSettingsOutput{AccountSettings: settings}, nil
}

// handlePutGroupConfiguration stores a configuration for a group.
type putGroupConfigurationInput struct {
	Group         string                   `json:"Group"`
	GroupName     string                   `json:"GroupName"`
	Configuration []GroupConfigurationItem `json:"Configuration"`
}

func (g *putGroupConfigurationInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type putGroupConfigurationOutput struct{}

func (h *Handler) handlePutGroupConfiguration(
	ctx context.Context,
	in *putGroupConfigurationInput,
) (*putGroupConfigurationOutput, error) {
	if err := h.Backend.PutGroupConfiguration(ctx, in.resolvedName(), in.Configuration); err != nil {
		return nil, err
	}

	return &putGroupConfigurationOutput{}, nil
}

// handleGroupResources adds resources to a group.
type groupResourcesInput struct {
	Group        string   `json:"Group"`
	ResourceArns []string `json:"ResourceArns"`
}

type groupResourcesOutput struct {
	Failed    []GroupingFailedItem `json:"Failed,omitempty"`
	Pending   []GroupingFailedItem `json:"Pending,omitempty"`
	Succeeded []string             `json:"Succeeded"`
}

// isValidResourceARN reports whether s is a syntactically valid AWS ARN.
// A valid ARN starts with "arn:" and contains at least five colon separators
// (six colon-delimited segments).
func isValidResourceARN(s string) bool {
	return strings.HasPrefix(s, "arn:") && strings.Count(s, ":") >= 5
}

func (h *Handler) handleGroupResources(ctx context.Context, in *groupResourcesInput) (*groupResourcesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	valid := make([]string, 0, len(in.ResourceArns))
	failed := make([]GroupingFailedItem, 0)

	for _, a := range in.ResourceArns {
		if !isValidResourceARN(a) {
			failed = append(failed, GroupingFailedItem{
				ResourceArn:  a,
				ErrorCode:    groupingErrInvalidARN,
				ErrorMessage: fmt.Sprintf("invalid ARN: %q", a),
			})

			continue
		}

		valid = append(valid, a)
	}

	succeeded, err := h.Backend.GroupResources(ctx, in.Group, valid)
	if err != nil {
		return nil, err
	}

	return &groupResourcesOutput{
		Succeeded: succeeded,
		Failed:    failed,
		Pending:   []GroupingFailedItem{},
	}, nil
}

// handleListGroupResources lists the resources associated with a group.
type listGroupResourcesInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListGroupResourcesFilter `json:"Filters"`
	Group      string                     `json:"Group"`
	GroupName  string                     `json:"GroupName"`
	NextToken  string                     `json:"NextToken"`
	MaxResults int                        `json:"MaxResults"`
}

func (g *listGroupResourcesInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type listGroupResourcesItem struct {
	Identifier ResourceIdentifier `json:"Identifier"`
}

type listGroupResourcesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Resources []listGroupResourcesItem `json:"Resources"`
	NextToken string                   `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroupResources(
	ctx context.Context,
	in *listGroupResourcesInput,
) (*listGroupResourcesOutput, error) {
	identifiers, nextToken, err := h.Backend.ListGroupResources(
		ctx, in.resolvedName(), in.Filters, in.NextToken, in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	items := make([]listGroupResourcesItem, 0, len(identifiers))

	for _, id := range identifiers {
		items = append(items, listGroupResourcesItem{Identifier: id})
	}

	return &listGroupResourcesOutput{Resources: items, NextToken: nextToken}, nil
}

// handleListGroupingStatuses lists the grouping/ungrouping statuses for a group.
type listGroupingStatusesInput struct {
	Group      string `json:"Group"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// groupingStatusItemWire is the AWS wire shape of a GroupingStatusesItem.
// UpdatedAt is serialized as a JSON number of seconds since the Unix epoch
// (the unixTimestamp format used by the rest-json protocol), not an
// RFC3339/ISO8601 string -- see pkgs/awstime.
type groupingStatusItemWire struct {
	ResourceArn  string  `json:"ResourceArn,omitempty"`
	Action       string  `json:"Action,omitempty"`
	Status       string  `json:"Status,omitempty"`
	ErrorCode    string  `json:"ErrorCode,omitempty"`
	ErrorMessage string  `json:"ErrorMessage,omitempty"`
	UpdatedAt    float64 `json:"UpdatedAt,omitempty"`
}

type listGroupingStatusesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Group            string                   `json:"Group"`
	GroupingStatuses []groupingStatusItemWire `json:"GroupingStatuses"`
	NextToken        string                   `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroupingStatuses(
	ctx context.Context,
	in *listGroupingStatusesInput,
) (*listGroupingStatusesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	statuses, nextToken, err := h.Backend.ListGroupingStatuses(ctx, in.Group, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]groupingStatusItemWire, 0, len(statuses))
	for i := range statuses {
		items = append(items, groupingStatusItemWire{
			ResourceArn:  statuses[i].ResourceArn,
			Action:       statuses[i].Action,
			Status:       statuses[i].Status,
			ErrorCode:    statuses[i].ErrorCode,
			ErrorMessage: statuses[i].ErrorMessage,
			UpdatedAt:    awstime.Epoch(statuses[i].UpdatedAt),
		})
	}

	return &listGroupingStatusesOutput{
		Group:            in.Group,
		GroupingStatuses: items,
		NextToken:        nextToken,
	}, nil
}

// handleSearchResources searches for resources matching a query.
type searchResourcesInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery"`
	NextToken     string         `json:"NextToken"`
	MaxResults    int            `json:"MaxResults"`
}

type searchResourcesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ResourceIdentifiers []ResourceIdentifier `json:"ResourceIdentifiers"`
	NextToken           string               `json:"NextToken,omitempty"`
}

func (h *Handler) handleSearchResources(ctx context.Context, in *searchResourcesInput) (*searchResourcesOutput, error) {
	identifiers, nextToken, err := h.Backend.SearchResources(ctx, in.ResourceQuery, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &searchResourcesOutput{ResourceIdentifiers: identifiers, NextToken: nextToken}, nil
}

// handleStartTagSyncTask creates a new tag-sync task.
type startTagSyncTaskInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	Group         string         `json:"Group"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
}

type startTagSyncTaskOutput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
}

func (h *Handler) handleStartTagSyncTask(
	ctx context.Context,
	in *startTagSyncTaskInput,
) (*startTagSyncTaskOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	task, err := h.Backend.StartTagSyncTask(ctx, in.Group, in.RoleArn, in.TagKey, in.TagValue, in.ResourceQuery)
	if err != nil {
		return nil, err
	}

	return &startTagSyncTaskOutput{
		GroupArn:      task.GroupArn,
		GroupName:     task.GroupName,
		RoleArn:       task.RoleArn,
		TagKey:        task.TagKey,
		TagValue:      task.TagValue,
		TaskArn:       task.TaskArn,
		ResourceQuery: task.ResourceQuery,
	}, nil
}

// handleCancelTagSyncTask cancels a tag-sync task.
type cancelTagSyncTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type cancelTagSyncTaskOutput struct{}

func (h *Handler) handleCancelTagSyncTask(
	ctx context.Context,
	in *cancelTagSyncTaskInput,
) (*cancelTagSyncTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", ErrValidation)
	}

	if err := h.Backend.CancelTagSyncTask(ctx, in.TaskArn); err != nil {
		return nil, err
	}

	return &cancelTagSyncTaskOutput{}, nil
}

// tagSyncTaskItem is the AWS wire shape shared by GetTagSyncTask's response
// body and each element of ListTagSyncTasks' TagSyncTasks array. CreatedAt is
// serialized as a JSON number of seconds since the Unix epoch (the
// unixTimestamp format used by the rest-json protocol), not an RFC3339/ISO8601
// string -- see pkgs/awstime.
type tagSyncTaskItem struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
	CreatedAt     float64        `json:"CreatedAt,omitempty"`
}

// tagSyncTaskItemFromTask builds the wire-shaped item from the backend's
// internal representation.
func tagSyncTaskItemFromTask(t *TagSyncTask) tagSyncTaskItem {
	return tagSyncTaskItem{
		TaskArn:       t.TaskArn,
		GroupArn:      t.GroupArn,
		GroupName:     t.GroupName,
		RoleArn:       t.RoleArn,
		TagKey:        t.TagKey,
		TagValue:      t.TagValue,
		ResourceQuery: t.ResourceQuery,
		Status:        t.Status,
		ErrorMessage:  t.ErrorMessage,
		CreatedAt:     awstime.Epoch(t.CreatedAt),
	}
}

// handleGetTagSyncTask returns the details of a tag-sync task.
type getTagSyncTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

func (h *Handler) handleGetTagSyncTask(ctx context.Context, in *getTagSyncTaskInput) (*tagSyncTaskItem, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", ErrValidation)
	}

	task, err := h.Backend.GetTagSyncTask(ctx, in.TaskArn)
	if err != nil {
		return nil, err
	}

	out := tagSyncTaskItemFromTask(task)

	return &out, nil
}

// handleListTagSyncTasks lists tag-sync tasks.
type listTagSyncTasksInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListTagSyncTasksFilter `json:"Filters,omitempty"`
	NextToken  string                   `json:"NextToken"`
	MaxResults int                      `json:"MaxResults"`
}

type listTagSyncTasksOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	TagSyncTasks []tagSyncTaskItem `json:"TagSyncTasks"`
	NextToken    string            `json:"NextToken,omitempty"`
}

func (h *Handler) handleListTagSyncTasks(
	ctx context.Context,
	in *listTagSyncTasksInput,
) (*listTagSyncTasksOutput, error) {
	tasks, nextToken, err := h.Backend.ListTagSyncTasks(ctx, in.Filters, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]tagSyncTaskItem, 0, len(tasks))
	for i := range tasks {
		items = append(items, tagSyncTaskItemFromTask(&tasks[i]))
	}

	return &listTagSyncTasksOutput{TagSyncTasks: items, NextToken: nextToken}, nil
}

// handleUngroupResources removes resources from a group.
type ungroupResourcesInput struct {
	Group        string   `json:"Group"`
	ResourceArns []string `json:"ResourceArns"`
}

type ungroupResourcesOutput struct {
	Failed    []GroupingFailedItem `json:"Failed,omitempty"`
	Pending   []GroupingFailedItem `json:"Pending,omitempty"`
	Succeeded []string             `json:"Succeeded"`
}

func (h *Handler) handleUngroupResources(
	ctx context.Context,
	in *ungroupResourcesInput,
) (*ungroupResourcesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	result, err := h.Backend.UngroupResources(ctx, in.Group, in.ResourceArns)
	if err != nil {
		return nil, err
	}

	return &ungroupResourcesOutput{
		Succeeded: result.Succeeded,
		Failed:    result.Failed,
		Pending:   []GroupingFailedItem{},
	}, nil
}

// handleUpdateAccountSettings updates account-level lifecycle event settings.
type updateAccountSettingsInput struct {
	GroupLifecycleEventsDesiredStatus string `json:"GroupLifecycleEventsDesiredStatus"`
}

type updateAccountSettingsOutput struct {
	AccountSettings AccountSettings `json:"AccountSettings"`
}

func (h *Handler) handleUpdateAccountSettings(
	_ context.Context,
	in *updateAccountSettingsInput,
) (*updateAccountSettingsOutput, error) {
	if err := h.Backend.UpdateAccountSettings(in.GroupLifecycleEventsDesiredStatus); err != nil {
		return nil, err
	}

	settings := h.Backend.GetAccountSettings()

	return &updateAccountSettingsOutput{AccountSettings: settings}, nil
}
