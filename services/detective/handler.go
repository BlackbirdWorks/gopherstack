package detective

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	pathGraph                            = "/graph"
	pathGraphRemoval                     = "/graph/removal"
	pathGraphsList                       = "/graphs/list"
	pathGraphMembers                     = "/graph/members"
	pathMembersRemoval                   = "/graph/members/removal"
	pathMembersGet                       = "/graph/members/get"
	pathMembersList                      = "/graph/members/list"
	pathTagsPrefix                       = "/tags/"
	pathInvitation                       = "/invitation"
	pathInvitationRemoval                = "/invitation/removal"
	pathInvitationsList                  = "/invitations/list"
	pathMembershipRemoval                = "/membership/removal"
	pathGraphDatasourcesGet              = "/graph/datasources/get"
	pathGraphDatasourcesList             = "/graph/datasources/list"
	pathGraphDatasourcesUpdate           = "/graph/datasources/update"
	pathMembershipDatasourcesGet         = "/membership/datasources/get"
	pathGraphMemberMonitoringstate       = "/graph/member/monitoringstate"
	pathInvestigationsGetInvestigation   = "/investigations/getInvestigation"
	pathInvestigationsListIndicators     = "/investigations/listIndicators"
	pathInvestigationsListInvestigations = "/investigations/listInvestigations"
	pathInvestigationsStartInvestigation = "/investigations/startInvestigation"
	pathInvestigationsUpdateState        = "/investigations/updateInvestigationState"
	pathOrgsDescribeOrgConfig            = "/orgs/describeOrganizationConfiguration"
	pathOrgsDisableAdminAccount          = "/orgs/disableAdminAccount"
	pathOrgsEnableAdminAccount           = "/orgs/enableAdminAccount"
	pathOrgsAdminAccountsList            = "/orgs/adminAccountslist"
	pathOrgsUpdateOrgConfig              = "/orgs/updateOrganizationConfiguration"

	opCreateGraph                       = "CreateGraph"
	opDeleteGraph                       = "DeleteGraph"
	opListGraphs                        = "ListGraphs"
	opCreateMembers                     = "CreateMembers"
	opDeleteMembers                     = "DeleteMembers"
	opGetMembers                        = "GetMembers"
	opListMembers                       = "ListMembers"
	opTagResource                       = "TagResource"
	opUntagResource                     = "UntagResource"
	opListTagsForResource               = "ListTagsForResource"
	opAcceptInvitation                  = "AcceptInvitation"
	opRejectInvitation                  = "RejectInvitation"
	opListInvitations                   = "ListInvitations"
	opDisassociateMembership            = "DisassociateMembership"
	opBatchGetGraphMemberDatasources    = "BatchGetGraphMemberDatasources"
	opBatchGetMembershipDatasources     = "BatchGetMembershipDatasources"
	opListDatasourcePackages            = "ListDatasourcePackages"
	opUpdateDatasourcePackages          = "UpdateDatasourcePackages"
	opStartMonitoringMember             = "StartMonitoringMember"
	opGetInvestigation                  = "GetInvestigation"
	opListIndicators                    = "ListIndicators"
	opListInvestigations                = "ListInvestigations"
	opStartInvestigation                = "StartInvestigation"
	opUpdateInvestigationState          = "UpdateInvestigationState"
	opDescribeOrganizationConfiguration = "DescribeOrganizationConfiguration"
	opDisableOrganizationAdminAccount   = "DisableOrganizationAdminAccount"
	opEnableOrganizationAdminAccount    = "EnableOrganizationAdminAccount"
	opListOrganizationAdminAccounts     = "ListOrganizationAdminAccounts"
	opUpdateOrganizationConfiguration   = "UpdateOrganizationConfiguration"
	opUnknown                           = "Unknown"

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
		opAcceptInvitation,
		opRejectInvitation,
		opListInvitations,
		opDisassociateMembership,
		opBatchGetGraphMemberDatasources,
		opBatchGetMembershipDatasources,
		opListDatasourcePackages,
		opUpdateDatasourcePackages,
		opStartMonitoringMember,
		opGetInvestigation,
		opListIndicators,
		opListInvestigations,
		opStartInvestigation,
		opUpdateInvestigationState,
		opDescribeOrganizationConfiguration,
		opDisableOrganizationAdminAccount,
		opEnableOrganizationAdminAccount,
		opListOrganizationAdminAccounts,
		opUpdateOrganizationConfiguration,
	}
}

// RouteMatcher returns a matcher that accepts Detective REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, pathTagsPrefix) {
			return strings.HasPrefix(path[len(pathTagsPrefix):], "arn:aws:detective:")
		}

		switch path {
		case pathGraph,
			pathGraphRemoval,
			pathGraphsList,
			pathGraphMembers,
			pathMembersRemoval,
			pathMembersGet,
			pathMembersList,
			pathInvitation,
			pathInvitationRemoval,
			pathInvitationsList,
			pathMembershipRemoval,
			pathGraphDatasourcesGet,
			pathGraphDatasourcesList,
			pathGraphDatasourcesUpdate,
			pathMembershipDatasourcesGet,
			pathGraphMemberMonitoringstate,
			pathInvestigationsGetInvestigation,
			pathInvestigationsListIndicators,
			pathInvestigationsListInvestigations,
			pathInvestigationsStartInvestigation,
			pathInvestigationsUpdateState,
			pathOrgsDescribeOrgConfig,
			pathOrgsDisableAdminAccount,
			pathOrgsEnableAdminAccount,
			pathOrgsAdminAccountsList,
			pathOrgsUpdateOrgConfig:
			return true
		}

		return false
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

func (h *Handler) handleREST(c *echo.Context) error { //nolint:cyclop,funlen // existing issue.
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
	case opAcceptInvitation:
		return h.handleAcceptInvitation(c)
	case opRejectInvitation:
		return h.handleRejectInvitation(c)
	case opListInvitations:
		return h.handleListInvitations(c)
	case opDisassociateMembership:
		return h.handleDisassociateMembership(c)
	case opBatchGetGraphMemberDatasources:
		return h.handleBatchGetGraphMemberDatasources(c)
	case opBatchGetMembershipDatasources:
		return h.handleBatchGetMembershipDatasources(c)
	case opListDatasourcePackages:
		return h.handleListDatasourcePackages(c)
	case opUpdateDatasourcePackages:
		return h.handleUpdateDatasourcePackages(c)
	case opStartMonitoringMember:
		return h.handleStartMonitoringMember(c)
	case opGetInvestigation:
		return h.handleGetInvestigation(c)
	case opListIndicators:
		return h.handleListIndicators(c)
	case opListInvestigations:
		return h.handleListInvestigations(c)
	case opStartInvestigation:
		return h.handleStartInvestigation(c)
	case opUpdateInvestigationState:
		return h.handleUpdateInvestigationState(c)
	case opDescribeOrganizationConfiguration:
		return h.handleDescribeOrganizationConfiguration(c)
	case opDisableOrganizationAdminAccount:
		return h.handleDisableOrganizationAdminAccount(c)
	case opEnableOrganizationAdminAccount:
		return h.handleEnableOrganizationAdminAccount(c)
	case opListOrganizationAdminAccounts:
		return h.handleListOrganizationAdminAccounts(c)
	case opUpdateOrganizationConfiguration:
		return h.handleUpdateOrganizationConfiguration(c)
	default:
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidInputException", "unknown operation"))
	}
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

	if method == http.MethodPut && path == pathInvitation {
		return opAcceptInvitation
	}

	if method != http.MethodPost {
		return opUnknown
	}

	postPathOps := map[string]string{
		pathGraph:                            opCreateGraph,
		pathGraphRemoval:                     opDeleteGraph,
		pathGraphsList:                       opListGraphs,
		pathGraphMembers:                     opCreateMembers,
		pathMembersRemoval:                   opDeleteMembers,
		pathMembersGet:                       opGetMembers,
		pathMembersList:                      opListMembers,
		pathInvitationRemoval:                opRejectInvitation,
		pathInvitationsList:                  opListInvitations,
		pathMembershipRemoval:                opDisassociateMembership,
		pathGraphDatasourcesGet:              opBatchGetGraphMemberDatasources,
		pathGraphDatasourcesList:             opListDatasourcePackages,
		pathGraphDatasourcesUpdate:           opUpdateDatasourcePackages,
		pathMembershipDatasourcesGet:         opBatchGetMembershipDatasources,
		pathGraphMemberMonitoringstate:       opStartMonitoringMember,
		pathInvestigationsGetInvestigation:   opGetInvestigation,
		pathInvestigationsListIndicators:     opListIndicators,
		pathInvestigationsListInvestigations: opListInvestigations,
		pathInvestigationsStartInvestigation: opStartInvestigation,
		pathInvestigationsUpdateState:        opUpdateInvestigationState,
		pathOrgsDescribeOrgConfig:            opDescribeOrganizationConfiguration,
		pathOrgsDisableAdminAccount:          opDisableOrganizationAdminAccount,
		pathOrgsEnableAdminAccount:           opEnableOrganizationAdminAccount,
		pathOrgsAdminAccountsList:            opListOrganizationAdminAccounts,
		pathOrgsUpdateOrgConfig:              opUpdateOrganizationConfiguration,
	}

	if op, ok := postPathOps[path]; ok {
		return op
	}

	return opUnknown
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
		"GraphArn": g.Arn, //nolint:goconst // existing issue.
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
			"CreatedTime": g.CreatedTime.Format("2006-01-02T15:04:05.000Z"), //nolint:goconst // existing issue.
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
			"AccountId":       m.AccountID, //nolint:goconst // existing issue.
			"AdministratorId": m.AdministratorID,
			"EmailAddress":    m.EmailAddress,
			"GraphArn":        m.GraphARN,
			"InvitedTime":     m.InvitedTime.Format("2006-01-02T15:04:05.000Z"),
			"MasterId":        m.AdministratorID,
			"Status":          m.Status, //nolint:goconst // existing issue.
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

func (h *Handler) handleAcceptInvitation(c *echo.Context) error {
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

	if acceptErr := h.Backend.AcceptInvitation(req.GraphArn); acceptErr != nil {
		return h.mapError(c, acceptErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRejectInvitation(c *echo.Context) error {
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

	if rejectErr := h.Backend.RejectInvitation(req.GraphArn); rejectErr != nil {
		return h.mapError(c, rejectErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInvitations(c *echo.Context) error {
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

	invitations, nextToken, listErr := h.Backend.ListInvitations(req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	resp := map[string]any{
		"Invitations": memberDetailsToJSON(invitations),
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDisassociateMembership(c *echo.Context) error {
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

	if disErr := h.Backend.DisassociateMembership(req.GraphArn); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleBatchGetGraphMemberDatasources(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string   `json:"GraphArn"`
		AccountIds []string `json:"AccountIds"` //nolint:revive // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	results, unprocessed, getErr := h.Backend.BatchGetGraphMemberDatasources(req.GraphArn, req.AccountIds)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	memberDatasources := make([]map[string]any, 0, len(results))
	for _, r := range results {
		memberDatasources = append(memberDatasources, map[string]any{
			"AccountId":                     r.AccountID,
			"GraphArn":                      r.GraphARN,
			"DatasourcePackageIngestStates": r.DatasourcePackageIngestStates,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MemberDatasources":    memberDatasources,
		keyUnprocessedAccounts: unprocessedToJSON(unprocessed),
	})
}

func (h *Handler) handleBatchGetMembershipDatasources(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArns []string `json:"GraphArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	results, unprocessed, getErr := h.Backend.BatchGetMembershipDatasources(req.GraphArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	membershipDatasources := make([]map[string]any, 0, len(results))
	for _, r := range results {
		membershipDatasources = append(membershipDatasources, map[string]any{
			"AccountId":                     r.AccountID,
			"GraphArn":                      r.GraphARN,
			"DatasourcePackageIngestStates": r.DatasourcePackageIngestStates,
		})
	}

	unprocessedGraphs := make([]map[string]any, 0, len(unprocessed))
	for _, g := range unprocessed {
		unprocessedGraphs = append(unprocessedGraphs, map[string]any{
			"GraphArn": g.GraphArn,
			"Reason":   g.Reason,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"MembershipDatasources": membershipDatasources,
		"UnprocessedGraphs":     unprocessedGraphs,
	})
}

func (h *Handler) handleListDatasourcePackages(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string `json:"GraphArn"`
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	packages, nextToken, listErr := h.Backend.ListDatasourcePackages(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	pkgDetails := make(map[string]any, len(packages))
	for k, v := range packages {
		pkgDetails[k] = map[string]any{
			"DatasourcePackageIngestState": v.IngestState,
		}
	}

	resp := map[string]any{
		"DatasourcePackages": pkgDetails,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDatasourcePackages(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn           string   `json:"GraphArn"`
		DatasourcePackages []string `json:"DatasourcePackages"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if updateErr := h.Backend.UpdateDatasourcePackages(req.GraphArn, req.DatasourcePackages); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartMonitoringMember(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn  string `json:"GraphArn"`
		AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.AccountId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "AccountId is required"))
	}

	if startErr := h.Backend.StartMonitoringMember(req.GraphArn, req.AccountId); startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartInvestigation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn       string `json:"GraphArn"`
		EntityArn      string `json:"EntityArn"`
		EntityType     string `json:"EntityType"`
		ScopeStartTime string `json:"ScopeStartTime"`
		ScopeEndTime   string `json:"ScopeEndTime"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.EntityArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "EntityArn is required"))
	}

	scopeStart, parseErr := parseTime(req.ScopeStartTime)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid ScopeStartTime"))
	}

	scopeEnd, parseErr := parseTime(req.ScopeEndTime)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid ScopeEndTime"))
	}

	id, startErr := h.Backend.StartInvestigation(req.GraphArn, req.EntityArn, req.EntityType, scopeStart, scopeEnd)
	if startErr != nil {
		return h.mapError(c, startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"InvestigationId": id, //nolint:goconst // existing issue.
	})
}

func (h *Handler) handleGetInvestigation(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	inv, getErr := h.Backend.GetInvestigation(req.GraphArn, req.InvestigationId)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"CreatedTime":     inv.CreatedTime.Format("2006-01-02T15:04:05.000Z"),
		"EntityArn":       inv.EntityARN,
		"EntityType":      inv.EntityType,
		"GraphArn":        inv.GraphARN,
		"InvestigationId": inv.InvestigationID,
		"ScopeEndTime":    inv.ScopeEndTime.Format("2006-01-02T15:04:05.000Z"),
		"ScopeStartTime":  inv.ScopeStartTime.Format("2006-01-02T15:04:05.000Z"),
		"Severity":        inv.Severity,
		"State":           inv.State,
		"Status":          inv.Status,
	})
}

func (h *Handler) handleListInvestigations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string `json:"GraphArn"`
		NextToken  string `json:"NextToken"`
		MaxResults int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	investigations, nextToken, listErr := h.Backend.ListInvestigations(req.GraphArn, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	details := make([]map[string]any, 0, len(investigations))
	for _, inv := range investigations {
		details = append(details, map[string]any{
			"CreatedTime":     inv.CreatedTime.Format("2006-01-02T15:04:05.000Z"),
			"EntityArn":       inv.EntityARN,
			"EntityType":      inv.EntityType,
			"InvestigationId": inv.InvestigationID,
			"Severity":        inv.Severity,
			"State":           inv.State,
			"Status":          inv.Status,
		})
	}

	resp := map[string]any{
		"InvestigationDetails": details,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateInvestigationState(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
		State           string `json:"State"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	if req.State == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "State is required"))
	}

	if updateErr := h.Backend.UpdateInvestigationState(req.GraphArn, req.InvestigationId, req.State); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListIndicators(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn        string `json:"GraphArn"`
		InvestigationId string `json:"InvestigationId"` //nolint:revive,staticcheck // existing issue.
		IndicatorType   string `json:"IndicatorType"`
		NextToken       string `json:"NextToken"`
		MaxResults      int32  `json:"MaxResults"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if req.InvestigationId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "InvestigationId is required"))
	}

	indicators, nextToken, listErr := h.Backend.ListIndicators(
		req.GraphArn, req.InvestigationId, req.IndicatorType, req.MaxResults, req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	indicatorList := make([]map[string]any, 0, len(indicators))
	for _, ind := range indicators {
		indicatorList = append(indicatorList, map[string]any{
			"IndicatorType": ind.IndicatorType,
			"Title":         ind.Title,
		})
	}

	resp := map[string]any{
		"GraphArn":        req.GraphArn,
		"InvestigationId": req.InvestigationId,
		"Indicators":      indicatorList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeOrganizationConfiguration(c *echo.Context) error {
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

	autoEnable, descErr := h.Backend.DescribeOrganizationConfiguration(req.GraphArn)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"AutoEnable": autoEnable,
	})
}

func (h *Handler) handleDisableOrganizationAdminAccount(c *echo.Context) error {
	if disErr := h.Backend.DisableOrganizationAdminAccount(); disErr != nil {
		return h.mapError(c, disErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleEnableOrganizationAdminAccount(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.AccountId == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "AccountId is required"))
	}

	if enableErr := h.Backend.EnableOrganizationAdminAccount(req.AccountId); enableErr != nil {
		return h.mapError(c, enableErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListOrganizationAdminAccounts(c *echo.Context) error {
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

	admins, nextToken, listErr := h.Backend.ListOrganizationAdminAccounts(req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	adminList := make([]map[string]any, 0, len(admins))
	for _, a := range admins {
		adminList = append(adminList, map[string]any{
			"AccountId":      a.AccountID,
			"DelegationTime": a.DelegationTime.Format("2006-01-02T15:04:05.000Z"),
			"GraphArn":       a.GraphARN,
		})
	}

	resp := map[string]any{
		"Administrators": adminList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateOrganizationConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		GraphArn   string `json:"GraphArn"`
		AutoEnable bool   `json:"AutoEnable"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.GraphArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "GraphArn is required"))
	}

	if updateErr := h.Backend.UpdateOrganizationConfiguration(req.GraphArn, req.AutoEnable); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}

	formats := []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000000000Z",
	}

	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}

	return time.Parse(time.RFC3339Nano, s)
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
