package guardduty

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	guardDutyService = "guardduty"
	matchPriority    = service.PriorityPathVersioned

	pathDetector       = "detector"
	pathFilter         = "filter"
	pathFindings       = "findings"
	pathIPSet          = "ipset"
	pathThreatIntelSet = "threatintelset"
	pathTags           = "tags"

	keyName            = "name"
	keyStatus          = "status"
	keyTags            = "tags"
	keyCreatedAt       = "createdAt"
	keyUpdatedAt       = "updatedAt"
	keyInvestigationID = "investigationId"

	opCreateDetector         = "CreateDetector"
	opGetDetector            = "GetDetector"
	opUpdateDetector         = "UpdateDetector"
	opDeleteDetector         = "DeleteDetector"
	opListDetectors          = "ListDetectors"
	opCreateFilter           = "CreateFilter"
	opGetFilter              = "GetFilter"
	opUpdateFilter           = "UpdateFilter"
	opDeleteFilter           = "DeleteFilter"
	opListFilters            = "ListFilters"
	opGetFindings            = "GetFindings"
	opListFindings           = "ListFindings"
	opArchiveFindings        = "ArchiveFindings"
	opUnarchiveFindings      = "UnarchiveFindings"
	opCreateSampleFindings   = "CreateSampleFindings"
	opGetFindingsStatistics  = "GetFindingsStatistics"
	opUpdateFindingsFeedback = "UpdateFindingsFeedback"
	opCreateIPSet            = "CreateIPSet"
	opGetIPSet               = "GetIPSet"
	opUpdateIPSet            = "UpdateIPSet"
	opDeleteIPSet            = "DeleteIPSet"
	opListIPSets             = "ListIPSets"
	opCreateThreatIntelSet   = "CreateThreatIntelSet"
	opGetThreatIntelSet      = "GetThreatIntelSet"
	opUpdateThreatIntelSet   = "UpdateThreatIntelSet"
	opDeleteThreatIntelSet   = "DeleteThreatIntelSet"
	opListThreatIntelSets    = "ListThreatIntelSets"
	opTagResource            = "TagResource"
	opUntagResource          = "UntagResource"
	opListTagsForResource    = "ListTagsForResource"
	opUnknown                = "Unknown"

	// Appendix A ops — member management.
	opCreateMembers          = "CreateMembers"
	opDeleteMembers          = "DeleteMembers"
	opGetMembers             = "GetMembers"
	opInviteMembers          = "InviteMembers"
	opListMembers            = "ListMembers"
	opStartMonitoringMembers = "StartMonitoringMembers"
	opStopMonitoringMembers  = "StopMonitoringMembers"
	opDisassociateMembers    = "DisassociateMembers"
	opGetMemberDetectors     = "GetMemberDetectors"
	opUpdateMemberDetectors  = "UpdateMemberDetectors"

	// Appendix A ops — invitation / admin account.
	opAcceptAdministratorInvitation        = "AcceptAdministratorInvitation"
	opAcceptInvitation                     = "AcceptInvitation"
	opGetAdministratorAccount              = "GetAdministratorAccount"
	opGetMasterAccount                     = "GetMasterAccount"
	opDisassociateFromAdministratorAccount = "DisassociateFromAdministratorAccount"
	opDisassociateFromMasterAccount        = "DisassociateFromMasterAccount"
	opDeclineInvitations                   = "DeclineInvitations"
	opDeleteInvitations                    = "DeleteInvitations"
	opGetInvitationsCount                  = "GetInvitationsCount"
	opListInvitations                      = "ListInvitations"

	// Appendix A ops — organization.
	opEnableOrganizationAdminAccount    = "EnableOrganizationAdminAccount"
	opDisableOrganizationAdminAccount   = "DisableOrganizationAdminAccount"
	opListOrganizationAdminAccounts     = "ListOrganizationAdminAccounts"
	opDescribeOrganizationConfiguration = "DescribeOrganizationConfiguration"
	opUpdateOrganizationConfiguration   = "UpdateOrganizationConfiguration"
	opGetOrganizationStatistics         = "GetOrganizationStatistics"

	// Appendix A ops — publishing destinations.
	opCreatePublishingDestination   = "CreatePublishingDestination"
	opDeletePublishingDestination   = "DeletePublishingDestination"
	opDescribePublishingDestination = "DescribePublishingDestination"
	opListPublishingDestinations    = "ListPublishingDestinations"
	opUpdatePublishingDestination   = "UpdatePublishingDestination"

	// Appendix A ops — malware scanning.
	opDescribeMalwareScans      = "DescribeMalwareScans"
	opListMalwareScans          = "ListMalwareScans"
	opStartMalwareScan          = "StartMalwareScan"
	opGetMalwareScan            = "GetMalwareScan"
	opGetMalwareScanSettings    = "GetMalwareScanSettings"
	opUpdateMalwareScanSettings = "UpdateMalwareScanSettings"
	opGetUsageStatistics        = "GetUsageStatistics"
	opGetRemainingFreeTrialDays = "GetRemainingFreeTrialDays"
	opGetCoverageStatistics     = "GetCoverageStatistics"
	opListCoverage              = "ListCoverage"

	// Appendix A ops — malware protection plans.
	opCreateMalwareProtectionPlan = "CreateMalwareProtectionPlan"
	opDeleteMalwareProtectionPlan = "DeleteMalwareProtectionPlan"
	opGetMalwareProtectionPlan    = "GetMalwareProtectionPlan"
	opListMalwareProtectionPlans  = "ListMalwareProtectionPlans"
	opUpdateMalwareProtectionPlan = "UpdateMalwareProtectionPlan"
	opSendObjectMalwareScan       = "SendObjectMalwareScan"

	// Appendix A ops — threat / trusted entity sets.
	opCreateThreatEntitySet  = "CreateThreatEntitySet"
	opGetThreatEntitySet     = "GetThreatEntitySet"
	opListThreatEntitySets   = "ListThreatEntitySets"
	opUpdateThreatEntitySet  = "UpdateThreatEntitySet"
	opDeleteThreatEntitySet  = "DeleteThreatEntitySet"
	opCreateTrustedEntitySet = "CreateTrustedEntitySet"
	opGetTrustedEntitySet    = "GetTrustedEntitySet"
	opListTrustedEntitySets  = "ListTrustedEntitySets"
	opUpdateTrustedEntitySet = "UpdateTrustedEntitySet"
	opDeleteTrustedEntitySet = "DeleteTrustedEntitySet"

	// Appendix A ops — investigations (GuardDuty Extended Threat Detection).
	opCreateInvestigation = "CreateInvestigation"
	opGetInvestigation    = "GetInvestigation"
	opListInvestigations  = "ListInvestigations"

	// URL depth constants for path parsing.
	depthRoot       = 1 // /detector
	depthResource   = 2 // /detector/{id}
	depthCollection = 3 // /detector/{id}/filter
	depthItem       = 4 // /detector/{id}/filter/{name}
	depthAction     = 4 // /detector/{id}/findings/archive
	depthDeep       = 5 // /detector/{id}/member/detector/get

	// Path constants for new resources.
	pathMember                = "member"
	pathAdmin                 = "admin"
	pathAdministrator         = "administrator"
	pathMaster                = "master"
	pathOrganization          = "organization"
	pathInvitation            = "invitation"
	pathPublishingDestination = "publishingDestination"
	pathMalwareProtectionPlan = "malware-protection-plan"
	pathMalwareScan           = "malware-scan"
	pathMalwareScans          = "malware-scans"
	pathMalwareScanSettings   = "malware-scan-settings"
	pathObjectMalwareScan     = "object-malware-scan"
	pathThreatEntitySet       = "threatentityset"
	pathTrustedEntitySet      = "trustedentityset"
	pathInvestigation         = "investigation"
	actionList                = "list"
	pathCoverage              = "coverage"
	pathFreeTrial             = "freeTrial"
	pathUsage                 = "usage"

	minTagPathParts       = 2
	minDetectorSubIDParts = 4

	// actionGet is the shared "get" sub-action segment used by several
	// distinct POST-only item routers (parseDetectorDeepPath,
	// parseFindingsItem, parseMemberItem).
	actionGet = "get"
)

// Handler handles GuardDuty HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GuardDuty" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateDetector,
		opGetDetector,
		opUpdateDetector,
		opDeleteDetector,
		opListDetectors,
		opCreateFilter,
		opGetFilter,
		opUpdateFilter,
		opDeleteFilter,
		opListFilters,
		opGetFindings,
		opListFindings,
		opArchiveFindings,
		opUnarchiveFindings,
		opCreateSampleFindings,
		opGetFindingsStatistics,
		opUpdateFindingsFeedback,
		opCreateIPSet,
		opGetIPSet,
		opUpdateIPSet,
		opDeleteIPSet,
		opListIPSets,
		opCreateThreatIntelSet,
		opGetThreatIntelSet,
		opUpdateThreatIntelSet,
		opDeleteThreatIntelSet,
		opListThreatIntelSets,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Appendix A
		opAcceptAdministratorInvitation,
		opAcceptInvitation,
		opCreateMalwareProtectionPlan,
		opCreateMembers,
		opCreatePublishingDestination,
		opCreateInvestigation,
		opCreateThreatEntitySet,
		opCreateTrustedEntitySet,
		opDeclineInvitations,
		opDeleteInvitations,
		opDeleteMalwareProtectionPlan,
		opDeleteMembers,
		opDeletePublishingDestination,
		opDeleteThreatEntitySet,
		opDeleteTrustedEntitySet,
		opDescribeMalwareScans,
		opDescribeOrganizationConfiguration,
		opDescribePublishingDestination,
		opDisableOrganizationAdminAccount,
		opDisassociateFromAdministratorAccount,
		opDisassociateFromMasterAccount,
		opDisassociateMembers,
		opEnableOrganizationAdminAccount,
		opGetAdministratorAccount,
		opGetCoverageStatistics,
		opGetInvestigation,
		opGetInvitationsCount,
		opGetMalwareProtectionPlan,
		opGetMalwareScan,
		opGetMalwareScanSettings,
		opGetMasterAccount,
		opGetMemberDetectors,
		opGetMembers,
		opGetOrganizationStatistics,
		opGetRemainingFreeTrialDays,
		opGetThreatEntitySet,
		opGetTrustedEntitySet,
		opInviteMembers,
		opListCoverage,
		opListInvestigations,
		opListInvitations,
		opListMalwareProtectionPlans,
		opListMalwareScans,
		opListMembers,
		opListOrganizationAdminAccounts,
		opListPublishingDestinations,
		opListThreatEntitySets,
		opListTrustedEntitySets,
		opSendObjectMalwareScan,
		opStartMalwareScan,
		opStartMonitoringMembers,
		opStopMonitoringMembers,
		opGetUsageStatistics,
		opUpdateMalwareProtectionPlan,
		opUpdateMalwareScanSettings,
		opUpdateMemberDetectors,
		opUpdateOrganizationConfiguration,
		opUpdatePublishingDestination,
		opUpdateThreatEntitySet,
		opUpdateTrustedEntitySet,
	}
}

// RouteMatcher returns a function that matches GuardDuty requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/"+pathDetector) ||
			isGuardDutyTagsPath(path) ||
			strings.HasPrefix(path, "/"+pathAdmin) ||
			strings.HasPrefix(path, "/"+pathInvitation) ||
			strings.HasPrefix(path, "/"+pathMalwareProtectionPlan) ||
			strings.HasPrefix(path, "/"+pathMalwareScan) ||
			strings.HasPrefix(path, "/"+pathObjectMalwareScan) ||
			strings.HasPrefix(path, "/"+pathOrganization)
	}
}

// isGuardDutyTagsPath reports whether path is a /tags/{resourceArn} request
// for a GuardDuty resource ARN. It checks for the "guardduty" service
// segment rather than hardcoding the "aws" partition: a hardcoded
// "arn:aws:guardduty:" prefix would reject a well-formed GuardDuty ARN from
// any non-standard partition (arn:aws-us-gov:guardduty:..., arn:aws-cn:
// guardduty:..., arn:aws-iso*:guardduty:...), silently making
// TagResource/UntagResource/ListTagsForResource unroutable for those
// accounts even though pkgs/arn.PartitionForRegion already produces such
// ARNs for GovCloud/China/ISO regions.
func isGuardDutyTagsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, "/"+pathTags+"/arn:")
	if !ok {
		return false
	}

	return strings.Contains(rest, ":guardduty:")
}

// restRouter returns the shared REST-path routing/dispatch wiring for this
// handler. See service.RESTRouter: GuardDuty's routing reduces entirely to
// parsing an operation out of (method, path) and dispatching on it, so the
// HTTP glue lives in pkgs/service instead of being duplicated here.
func (h *Handler) restRouter() service.RESTRouter {
	return service.RESTRouter{
		ServiceName: guardDutyService,
		Priority:    matchPriority,
		Parse:       parseRESTPath,
		OpUnknown:   opUnknown,
		NotFoundBody: func() any {
			return errBody(errResourceNotFound, "not found")
		},
		BadRequestBody: func() any {
			return errBody("BadRequestException", "failed to read body")
		},
		InternalErrorBody: func() any {
			return errBody("InternalFailure", "serialization failed")
		},
		Dispatch:    h.dispatch,
		HandleError: h.handleError,
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return h.restRouter().MatchPriority() }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string { return h.restRouter().ExtractOperation(c) }

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string { return h.restRouter().ExtractResource(c) }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc { return h.restRouter().Handler() }

func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, ok, err := h.dispatchDetectorOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFilterOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchIPSetOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchThreatIntelSetOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMemberOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchInvitationOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchOrgOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchPublishingDestOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMalwareOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchEntitySetOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchInvestigationOps(op, path, body); ok {
		return result, code, err
	}

	return h.dispatchTagOps(op, path, query, body)
}

// parseRESTPath maps (method, path) → (operation, resource).
//
// GuardDuty REST paths:
//
//	/detector                              → CreateDetector (POST) / ListDetectors (GET)
//	/detector/{id}                         → GetDetector (GET) / UpdateDetector (POST) / DeleteDetector (DELETE)
//	/detector/{id}/filter                  → CreateFilter (POST) / ListFilters (GET)
//	/detector/{id}/filter/{name}           → GetFilter (GET) / UpdateFilter (POST) / DeleteFilter (DELETE)
//	/detector/{id}/findings                → ListFindings (POST)
//	/detector/{id}/findings/get            → GetFindings (POST)
//	/detector/{id}/findings/archive        → ArchiveFindings (POST)
//	/detector/{id}/findings/unarchive      → UnarchiveFindings (POST)
//	/detector/{id}/findings/create         → CreateSampleFindings (POST)
//	/detector/{id}/findings/statistics     → GetFindingsStatistics (POST)
//	/detector/{id}/findings/feedback       → UpdateFindingsFeedback (POST)
//	/detector/{id}/ipset                   → CreateIPSet (POST) / ListIPSets (GET)
//	/detector/{id}/ipset/{ipSetId}         → GetIPSet (GET) / UpdateIPSet (POST) / DeleteIPSet (DELETE)
//	/detector/{id}/threatintelset          → CreateThreatIntelSet (POST) / ListThreatIntelSets (GET)
//	/detector/{id}/threatintelset/{setId}  → GetThreatIntelSet (GET) / UpdateThreatIntelSet (POST) /
//	                                          DeleteThreatIntelSet (DELETE)
//	/detector/{id}/investigation           → CreateInvestigation (POST)
//	/detector/{id}/investigation/{invId}   → GetInvestigation (GET)
//	/detector/{id}/investigation/list      → ListInvestigations (POST)
//	/tags/{resourceArn}                    → ListTagsForResource (GET) / TagResource (POST) / UntagResource (DELETE)
func parseRESTPath(method, path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) == 0 {
		return opUnknown, ""
	}

	if fn, ok := topLevelPathParsers()[parts[0]]; ok {
		return fn(method, parts)
	}

	return opUnknown, ""
}

// pathParser parses the (method, path-segments) remainder of a REST path
// into (operation, resource).
type pathParser func(method string, parts []string) (string, string)

// topLevelPathParsers maps the first path segment to the parser responsible
// for the rest of the path. Built once via sync.OnceValue (apigatewayv2-style
// route-table pattern) so parseRESTPath itself stays a trivial map lookup
// instead of a large branching switch.
//
//nolint:gochecknoglobals // route table, not mutable state
var topLevelPathParsers = sync.OnceValue(func() map[string]pathParser {
	return map[string]pathParser{
		pathDetector:              parseDetectorPath,
		pathTags:                  parseTagPath,
		pathAdmin:                 parseAdminPath,
		pathInvitation:            parseInvitationPath,
		pathMalwareProtectionPlan: parseMalwareProtectionPlanPath,
		pathMalwareScan:           parseMalwareScanPath,
		pathObjectMalwareScan:     parseObjectMalwareScanPath,
		pathOrganization:          parseOrganizationStatisticsPath,
	}
})

func parseObjectMalwareScanPath(method string, parts []string) (string, string) {
	if method == http.MethodPost && len(parts) == 2 && parts[1] == "send" {
		return opSendObjectMalwareScan, ""
	}

	return opUnknown, ""
}

func parseOrganizationStatisticsPath(method string, parts []string) (string, string) {
	if method == http.MethodGet && len(parts) == 2 && parts[1] == "statistics" {
		return opGetOrganizationStatistics, ""
	}

	return opUnknown, ""
}

func parseDetectorRootPath(method string) (string, string) {
	switch method {
	case http.MethodPost:
		return opCreateDetector, ""
	case http.MethodGet:
		return opListDetectors, ""
	}

	return opUnknown, ""
}

func parseDetectorResourcePath(method, detectorID string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetDetector, detectorID
	case http.MethodPost:
		return opUpdateDetector, detectorID
	case http.MethodDelete:
		return opDeleteDetector, detectorID
	}

	return opUnknown, ""
}

// parseDetectorDeepPath routes the one 5-segment detector path this API has:
// /detector/{id}/member/detector/{get|update}.
func parseDetectorDeepPath(method, detectorID, collection, sub, action string) (string, string) {
	if collection != pathMember || sub != "detector" || method != http.MethodPost {
		return opUnknown, ""
	}

	switch action {
	case actionGet:
		return opGetMemberDetectors, detectorID
	case "update":
		return opUpdateMemberDetectors, detectorID
	}

	return opUnknown, ""
}

func parseDetectorPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /detector
		return parseDetectorRootPath(method)
	case depthResource: // /detector/{id}
		return parseDetectorResourcePath(method, parts[1])
	case depthCollection: // /detector/{id}/{collection}
		return parseDetectorCollection(method, parts[1], parts[2])
	case depthItem: // /detector/{id}/{collection}/{item}
		return parseDetectorItem(method, parts[1], parts[2], parts[3])
	case depthDeep: // /detector/{id}/{collection}/{sub}/{action}
		return parseDetectorDeepPath(method, parts[1], parts[2], parts[3], parts[4])
	}

	return opUnknown, ""
}

// detectorCollectionRouteKey builds the lookup key for
// detectorCollectionRoutes: the collection segment and HTTP method
// unambiguously select an op with no further branching, so the whole
// parseDetectorCollection switch collapses to one map lookup.
func detectorCollectionRouteKey(collection, method string) string {
	return collection + " " + method
}

// detectorCollectionRoutes maps (collection, method) pairs reachable from
// /detector/{id}/{collection} to their operation name. Built once via
// sync.OnceValue (apigatewayv2-style route-table pattern).
//
//nolint:gochecknoglobals // route table, not mutable state
var detectorCollectionRoutes = sync.OnceValue(func() map[string]string {
	return map[string]string{
		detectorCollectionRouteKey(pathFilter, http.MethodPost):                opCreateFilter,
		detectorCollectionRouteKey(pathFilter, http.MethodGet):                 opListFilters,
		detectorCollectionRouteKey(pathFindings, http.MethodPost):              opListFindings,
		detectorCollectionRouteKey(pathIPSet, http.MethodPost):                 opCreateIPSet,
		detectorCollectionRouteKey(pathIPSet, http.MethodGet):                  opListIPSets,
		detectorCollectionRouteKey(pathThreatIntelSet, http.MethodPost):        opCreateThreatIntelSet,
		detectorCollectionRouteKey(pathThreatIntelSet, http.MethodGet):         opListThreatIntelSets,
		detectorCollectionRouteKey(pathMember, http.MethodPost):                opCreateMembers,
		detectorCollectionRouteKey(pathMember, http.MethodGet):                 opListMembers,
		detectorCollectionRouteKey(pathAdministrator, http.MethodPost):         opAcceptAdministratorInvitation,
		detectorCollectionRouteKey(pathAdministrator, http.MethodGet):          opGetAdministratorAccount,
		detectorCollectionRouteKey(pathMaster, http.MethodPost):                opAcceptInvitation,
		detectorCollectionRouteKey(pathMaster, http.MethodGet):                 opGetMasterAccount,
		detectorCollectionRouteKey(pathAdmin, http.MethodGet):                  opDescribeOrganizationConfiguration,
		detectorCollectionRouteKey(pathAdmin, http.MethodPost):                 opUpdateOrganizationConfiguration,
		detectorCollectionRouteKey(pathPublishingDestination, http.MethodPost): opCreatePublishingDestination,
		detectorCollectionRouteKey(pathPublishingDestination, http.MethodGet):  opListPublishingDestinations,
		detectorCollectionRouteKey(pathMalwareScans, http.MethodPost):          opDescribeMalwareScans,
		detectorCollectionRouteKey(pathMalwareScanSettings, http.MethodGet):    opGetMalwareScanSettings,
		detectorCollectionRouteKey(pathMalwareScanSettings, http.MethodPost):   opUpdateMalwareScanSettings,
		detectorCollectionRouteKey(pathThreatEntitySet, http.MethodPost):       opCreateThreatEntitySet,
		detectorCollectionRouteKey(pathThreatEntitySet, http.MethodGet):        opListThreatEntitySets,
		detectorCollectionRouteKey(pathTrustedEntitySet, http.MethodPost):      opCreateTrustedEntitySet,
		detectorCollectionRouteKey(pathTrustedEntitySet, http.MethodGet):       opListTrustedEntitySets,
		detectorCollectionRouteKey(pathCoverage, http.MethodPost):              opListCoverage,
		detectorCollectionRouteKey(pathInvestigation, http.MethodPost):         opCreateInvestigation,
	}
})

func parseDetectorCollection(method, detectorID, collection string) (string, string) {
	op, ok := detectorCollectionRoutes()[detectorCollectionRouteKey(collection, method)]
	if !ok {
		return opUnknown, ""
	}

	return op, detectorID
}

// parseItemCRUD routes the common Get/Post(update)/Delete-by-item pattern
// shared by several detector item collections (filters, IP sets, threat
// intel sets, publishing destinations, threat/trusted entity sets): the
// resource is addressed by id (typically detectorID + "/" + item) and only
// the HTTP verb selects the op.
func parseItemCRUD(method, id, getOp, updateOp, deleteOp string) (string, string) {
	switch method {
	case http.MethodGet:
		return getOp, id
	case http.MethodPost:
		return updateOp, id
	case http.MethodDelete:
		return deleteOp, id
	}

	return opUnknown, ""
}

// parseItemAction matches a single POST action addressed by item, used by the
// several detector-item routes that support exactly one named action
// (disassociate, statistics, daysRemaining, ...).
func parseItemAction(method, detectorID, item, wantItem, op string) (string, string) {
	if method == http.MethodPost && item == wantItem {
		return op, detectorID
	}

	return opUnknown, ""
}

// parseFindingsItem routes the POST-only findings item actions (get, archive,
// unarchive, create, statistics, feedback).
func parseFindingsItem(method, detectorID, item string) (string, string) {
	if method != http.MethodPost {
		return opUnknown, ""
	}

	switch item {
	case actionGet:
		return opGetFindings, detectorID
	case "archive":
		return opArchiveFindings, detectorID
	case "unarchive":
		return opUnarchiveFindings, detectorID
	case "create":
		return opCreateSampleFindings, detectorID
	case "statistics":
		return opGetFindingsStatistics, detectorID
	case "feedback":
		return opUpdateFindingsFeedback, detectorID
	}

	return opUnknown, ""
}

// parseMemberItem routes the POST-only member item actions (get, delete,
// start, stop, invite, disassociate).
func parseMemberItem(method, detectorID, item string) (string, string) {
	if method != http.MethodPost {
		return opUnknown, ""
	}

	switch item {
	case actionGet:
		return opGetMembers, detectorID
	case "delete":
		return opDeleteMembers, detectorID
	case "start":
		return opStartMonitoringMembers, detectorID
	case "stop":
		return opStopMonitoringMembers, detectorID
	case "invite":
		return opInviteMembers, detectorID
	case "disassociate":
		return opDisassociateMembers, detectorID
	}

	return opUnknown, ""
}

// parseInvestigationItem routes the two item-addressed investigation paths:
// GET /detector/{id}/investigation/{investigationId} (GetInvestigation) and
// POST /detector/{id}/investigation/list (ListInvestigations -- the one
// "item" segment here is the literal string "list", not a real
// investigation ID, matching the real serializer's
// "/detector/{DetectorId}/investigation/list" path).
func parseInvestigationItem(method, detectorID, item string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetInvestigation, detectorID + "/" + item
	case http.MethodPost:
		if item == actionList {
			return opListInvestigations, detectorID
		}
	}

	return opUnknown, ""
}

func parseDetectorItem(method, detectorID, collection, item string) (string, string) {
	switch collection {
	case pathInvestigation:
		return parseInvestigationItem(method, detectorID, item)
	case pathFilter:
		return parseItemCRUD(method, detectorID+"/"+item, opGetFilter, opUpdateFilter, opDeleteFilter)
	case pathFindings:
		return parseFindingsItem(method, detectorID, item)
	case pathIPSet:
		return parseItemCRUD(method, detectorID+"/"+item, opGetIPSet, opUpdateIPSet, opDeleteIPSet)
	case pathThreatIntelSet:
		return parseItemCRUD(
			method, detectorID+"/"+item,
			opGetThreatIntelSet, opUpdateThreatIntelSet, opDeleteThreatIntelSet,
		)
	case pathMember:
		return parseMemberItem(method, detectorID, item)
	case pathAdministrator:
		return parseItemAction(method, detectorID, item, "disassociate", opDisassociateFromAdministratorAccount)
	case pathMaster:
		return parseItemAction(method, detectorID, item, "disassociate", opDisassociateFromMasterAccount)
	case pathPublishingDestination:
		return parseItemCRUD(
			method, detectorID+"/"+item,
			opDescribePublishingDestination, opUpdatePublishingDestination, opDeletePublishingDestination,
		)
	case pathThreatEntitySet:
		return parseItemCRUD(
			method, detectorID+"/"+item,
			opGetThreatEntitySet, opUpdateThreatEntitySet, opDeleteThreatEntitySet,
		)
	case pathTrustedEntitySet:
		return parseItemCRUD(
			method, detectorID+"/"+item,
			opGetTrustedEntitySet, opUpdateTrustedEntitySet, opDeleteTrustedEntitySet,
		)
	case pathCoverage:
		return parseItemAction(method, detectorID, item, "statistics", opGetCoverageStatistics)
	case pathUsage:
		return parseItemAction(method, detectorID, item, "statistics", opGetUsageStatistics)
	case pathFreeTrial:
		return parseItemAction(method, detectorID, item, "daysRemaining", opGetRemainingFreeTrialDays)
	}

	return opUnknown, ""
}

func parseTagPath(method string, parts []string) (string, string) {
	if len(parts) < minTagPathParts {
		return opUnknown, ""
	}

	// /tags/{resourceArn} — the ARN may have slashes, rejoin from index 1.
	resourceARN := strings.Join(parts[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return opUnknown, ""
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	code := http.StatusInternalServerError
	msg := err.Error()

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, awserr.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, awserr.ErrInvalidParameter):
		code = http.StatusBadRequest
	}

	return c.JSON(code, errBody(msg, msg))
}

func tagsOrEmpty(m map[string]string) map[string]string {
	if m != nil {
		return m
	}

	return map[string]string{}
}

func errBody(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

func errorf(code string) error {
	return awserr.New(code, awserr.ErrInvalidParameter)
}

// extractID extracts a resource ID from a path like /prefix/{id}/...
//
//nolint:unparam // prefix is always pathDetector by design
func extractID(path, prefix string) string {
	stripped := strings.TrimPrefix(path, "/"+prefix+"/")
	before, _, found := strings.Cut(stripped, "/")

	if !found {
		return stripped
	}

	return before
}

// extractDetectorAndSubID extracts detectorID and the sub-resource ID from
// paths like /detector/{id}/{collection}/{subID}.
func extractDetectorAndSubID(path string) (string, string) {
	// path: /detector/{detectorID}/{collection}/{subID}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) < minDetectorSubIDParts {
		return "", ""
	}

	return parts[1], parts[3]
}

func orEmptyAny[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}
