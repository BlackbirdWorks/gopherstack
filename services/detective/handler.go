package detective

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
	matchPriority = service.PriorityPathVersioned

	pathGraph          = "/graph"
	pathGraphRemoval   = "/graph/removal"
	pathGraphsList     = "/graphs/list"
	pathGraphMembers   = "/graph/members"
	pathMembersRemoval = "/graph/members/removal"
	pathMembersGet     = "/graph/members/get"
	pathMembersList    = "/graph/members/list"
	pathTagsPrefix     = "/tags/"

	opCreateGraph         = "CreateGraph"
	opDeleteGraph         = "DeleteGraph"
	opListGraphs          = "ListGraphs"
	opCreateMembers       = "CreateMembers"
	opDeleteMembers       = "DeleteMembers"
	opGetMembers          = "GetMembers"
	opListMembers         = "ListMembers"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
	opUnknown             = "Unknown"

	keyUnprocessedAccounts = "UnprocessedAccounts"
)

// Handler handles Detective HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Detective" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateGraph,
		opDeleteGraph,
		opListGraphs,
		opCreateMembers,
		opDeleteMembers,
		opGetMembers,
		opListMembers,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a matcher that accepts Detective REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, pathTagsPrefix) {
			return strings.HasPrefix(path[len(pathTagsPrefix):], "arn:aws:detective:")
		}

		return path == pathGraph ||
			path == pathGraphRemoval ||
			path == pathGraphsList ||
			path == pathGraphMembers ||
			path == pathMembersRemoval ||
			path == pathMembersGet ||
			path == pathMembersList
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return classifyPath(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	if resource, ok := strings.CutPrefix(path, pathTagsPrefix); ok {
		return resource
	}

	return path
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch classifyPath(method, path) {
	case opCreateGraph:
		return h.handleCreateGraph(c)
	case opDeleteGraph:
		return h.handleDeleteGraph(c)
	case opListGraphs:
		return h.handleListGraphs(c)
	case opCreateMembers:
		return h.handleCreateMembers(c)
	case opDeleteMembers:
		return h.handleDeleteMembers(c)
	case opGetMembers:
		return h.handleGetMembers(c)
	case opListMembers:
		return h.handleListMembers(c)
	case opTagResource:
		return h.handleTagResource(c)
	case opUntagResource:
		return h.handleUntagResource(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	default:
		return c.JSON(http.StatusNotImplemented, errorResponse("NotImplementedException", "operation not implemented"))
	}
}

func classifyGraphPath(method, path string) string {
	if method != http.MethodPost {
		return opUnknown
	}

	postPathOps := map[string]string{
		pathGraph:          opCreateGraph,
		pathGraphRemoval:   opDeleteGraph,
		pathGraphsList:     opListGraphs,
		pathGraphMembers:   opCreateMembers,
		pathMembersRemoval: opDeleteMembers,
		pathMembersGet:     opGetMembers,
		pathMembersList:    opListMembers,
	}

	if op, ok := postPathOps[path]; ok {
		return op
	}

	return opUnknown
}

func classifyTagPath(method string) string {
	switch method {
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	case http.MethodGet:
		return opListTagsForResource
	default:
		return opUnknown
	}
}

func classifyPath(method, path string) string {
	if strings.HasPrefix(path, pathTagsPrefix) {
		return classifyTagPath(method)
	}

	return classifyGraphPath(method, path)
}

func (h *Handler) handleCreateGraph(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags map[string]string `json:"Tags"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	g, createErr := h.Backend.CreateGraph(req.Tags)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"GraphArn": g.Arn,
	})
}

func (h *Handler) handleDeleteGraph(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if delErr := h.Backend.DeleteGraph(req.GraphArn); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListGraphs(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	graphs, nextToken, listErr := h.Backend.ListGraphs(req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	graphList := make([]map[string]any, 0, len(graphs))
	for _, g := range graphs {
		graphList = append(graphList, map[string]any{
			"Arn":         g.Arn,
			"CreatedTime": g.CreatedTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{
		"GraphList": graphList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn string `json:"GraphArn"`
		Message  string `json:"Message"`
		Accounts []struct {
			AccountID    string `json:"AccountId"`
			EmailAddress string `json:"EmailAddress"`
		} `json:"Accounts"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	accounts := make([]Account, 0, len(req.Accounts))
	for _, a := range req.Accounts {
		accounts = append(accounts, Account{
			AccountID:    a.AccountID,
			EmailAddress: a.EmailAddress,
		})
	}

	members, unprocessed, createErr := h.Backend.CreateMembers(req.GraphArn, accounts, req.Message)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Members":              memberDetailsToJSON(members),
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleDeleteMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIDs []string `json:"AccountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	deleted, unprocessed, delErr := h.Backend.DeleteMembers(req.GraphArn, req.AccountIDs)
	if delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AccountIds":           deleted,
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleGetMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIDs []string `json:"AccountIds"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	members, unprocessed, getErr := h.Backend.GetMembers(req.GraphArn, req.AccountIDs)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MemberDetails":        memberDetailsToJSON(members),
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleListMembers(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		NextToken  string `json:"NextToken"`
		GraphArn   string `json:"GraphArn"`
		MaxResults int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	members, nextToken, listErr := h.Backend.ListMembers(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{
		"MemberDetails": memberDetailsToJSON(members),
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags map[string]string `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN, ok := extractTagARN(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid resource ARN"))
	}

	tags, listErr := h.Backend.ListTagsForResource(resourceARN)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Tags": tags,
	})
}

func extractTagARN(path string) (string, bool) {
	arn, ok := strings.CutPrefix(path, pathTagsPrefix)

	return arn, ok && arn != ""
}

func memberDetailsToJSON(members []*MemberDetail) []map[string]any {
	result := make([]map[string]any, 0, len(members))
	for _, m := range members {
		result = append(result, map[string]any{
			"AccountId":       m.AccountID,
			"AdministratorId": m.AdministratorID,
			"EmailAddress":    m.EmailAddress,
			"GraphArn":        m.GraphARN,
			"InvitedTime":     m.InvitedTime.Format("2006-01-02T15:04:05.000Z"),
			"MasterId":        m.AdministratorID,
			"Status":          m.Status,
			"UpdatedTime":     m.UpdatedTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return result
}

func unprocessedToJSON(accounts []UnprocessedAccount) []map[string]any {
	result := make([]map[string]any, 0, len(accounts))
	for _, a := range accounts {
		result = append(result, map[string]any{
			"AccountId": a.AccountID,
			"Reason":    a.Reason,
		})
	}

	return result
}

func (h *Handler) mapError(c *echo.Context, err error) error {
	logger.Load(c.Request().Context()).Error("detective error", "error", err)

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", err.Error()))
	}
}

func errorResponse(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}
