package detective

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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

	// JSON response key names repeated across several handler_*.go files.
	// Named here (rather than left as inline literals in each file) so that
	// splitting the former monolithic handler.go into per-family files does
	// not trip goconst in files that individually re-use the same key.
	keyGraphArn                       = "GraphArn"
	keyAccountID                      = "AccountId"
	keyCreatedTime                    = "CreatedTime"
	keyStatusField                    = "Status"
	keyReason                         = "Reason"
	keyIPAddress                      = "IpAddress"
	keyIsNewForEntireAccount          = "IsNewForEntireAccount"
	keyDatasourcePackageIngestStates  = "DatasourcePackageIngestStates"
	keyDatasourcePackageIngestHistory = "DatasourcePackageIngestHistory"
)

// Handler handles Detective HTTP requests.
type Handler struct {
	Backend StorageBackend

	// dispatch maps each supported operation to its handler method. Keeping
	// this as a plain map (rather than a switch) avoids the funlen/cyclop
	// complexity of a large switch statement while remaining a simple,
	// idiomatic lookup.
	dispatch map[string]func(*Handler, *echo.Context) error
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.dispatch = map[string]func(*Handler, *echo.Context) error{
		opCreateGraph:                       (*Handler).handleCreateGraph,
		opDeleteGraph:                       (*Handler).handleDeleteGraph,
		opListGraphs:                        (*Handler).handleListGraphs,
		opCreateMembers:                     (*Handler).handleCreateMembers,
		opDeleteMembers:                     (*Handler).handleDeleteMembers,
		opGetMembers:                        (*Handler).handleGetMembers,
		opListMembers:                       (*Handler).handleListMembers,
		opTagResource:                       (*Handler).handleTagResource,
		opUntagResource:                     (*Handler).handleUntagResource,
		opListTagsForResource:               (*Handler).handleListTagsForResource,
		opAcceptInvitation:                  (*Handler).handleAcceptInvitation,
		opRejectInvitation:                  (*Handler).handleRejectInvitation,
		opListInvitations:                   (*Handler).handleListInvitations,
		opDisassociateMembership:            (*Handler).handleDisassociateMembership,
		opBatchGetGraphMemberDatasources:    (*Handler).handleBatchGetGraphMemberDatasources,
		opBatchGetMembershipDatasources:     (*Handler).handleBatchGetMembershipDatasources,
		opListDatasourcePackages:            (*Handler).handleListDatasourcePackages,
		opUpdateDatasourcePackages:          (*Handler).handleUpdateDatasourcePackages,
		opStartMonitoringMember:             (*Handler).handleStartMonitoringMember,
		opGetInvestigation:                  (*Handler).handleGetInvestigation,
		opListIndicators:                    (*Handler).handleListIndicators,
		opListInvestigations:                (*Handler).handleListInvestigations,
		opStartInvestigation:                (*Handler).handleStartInvestigation,
		opUpdateInvestigationState:          (*Handler).handleUpdateInvestigationState,
		opDescribeOrganizationConfiguration: (*Handler).handleDescribeOrganizationConfiguration,
		opDisableOrganizationAdminAccount:   (*Handler).handleDisableOrganizationAdminAccount,
		opEnableOrganizationAdminAccount:    (*Handler).handleEnableOrganizationAdminAccount,
		opListOrganizationAdminAccounts:     (*Handler).handleListOrganizationAdminAccounts,
		opUpdateOrganizationConfiguration:   (*Handler).handleUpdateOrganizationConfiguration,
	}

	return h
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

func (h *Handler) handleREST(c *echo.Context) error {
	op := classifyPath(c.Request().Method, c.Request().URL.Path)
	if fn, ok := h.dispatch[op]; ok {
		return fn(h, c)
	}

	return c.JSON(http.StatusBadRequest, errorResponse("InvalidInputException", "unknown operation"))
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
