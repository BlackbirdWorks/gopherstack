package guardduty

import (
	"context"
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
	guardDutyService = "guardduty"
	matchPriority    = service.PriorityPathVersioned

	pathDetector       = "detector"
	pathFilter         = "filter"
	pathFindings       = "findings"
	pathIPSet          = "ipset"
	pathThreatIntelSet = "threatintelset"
	pathTags           = "tags"

	keyName      = "name"
	keyStatus    = "status"
	keyTags      = "tags"
	keyCreatedAt = "createdAt"
	keyUpdatedAt = "updatedAt"

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
	pathCoverage              = "coverage"
	pathFreeTrial             = "freeTrial"
	pathUsage                 = "usage"

	minTagPathParts       = 2
	minDetectorSubIDParts = 4
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

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	if op == opUnknown {
		return c.JSON(http.StatusNotFound, errBody(errResourceNotFound, "not found"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errBody("BadRequestException", "failed to read body"))
	}

	result, statusCode, opErr := h.dispatch(ctx, op, c.Request().URL.Path, c.Request().URL.RawQuery, body)
	if opErr != nil {
		log.Error("guardduty operation error", "op", op, "err", opErr)

		return h.handleError(c, opErr)
	}

	if result == nil {
		return c.JSON(statusCode, struct{}{})
	}

	data, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		return c.JSON(http.StatusInternalServerError, errBody("InternalFailure", "serialization failed"))
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, data)
}

func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, ok, err := h.dispatchDetectorOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFilterOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchIPSetOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchThreatIntelSetOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMemberOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchInvitationOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchOrgOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchPublishingDestOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMalwareOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchEntitySetOps(op, path, body); ok {
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
//	/tags/{resourceArn}                    → ListTagsForResource (GET) / TagResource (POST) / UntagResource (DELETE)
func parseRESTPath(method, path string) (string, string) { //nolint:cyclop // existing issue.
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) == 0 {
		return opUnknown, ""
	}

	switch parts[0] {
	case pathDetector:
		return parseDetectorPath(method, parts)
	case pathTags:
		return parseTagPath(method, parts)
	case pathAdmin:
		return parseAdminPath(method, parts)
	case pathInvitation:
		return parseInvitationPath(method, parts)
	case pathMalwareProtectionPlan:
		return parseMalwareProtectionPlanPath(method, parts)
	case pathMalwareScan:
		return parseMalwareScanPath(method, parts)
	case pathObjectMalwareScan:
		if method == http.MethodPost && len(parts) == 2 && parts[1] == "send" {
			return opSendObjectMalwareScan, ""
		}
	case pathOrganization:
		if method == http.MethodGet && len(parts) == 2 && parts[1] == "statistics" {
			return opGetOrganizationStatistics, ""
		}
	}

	return opUnknown, ""
}

func parseDetectorPath(method string, parts []string) (string, string) { //nolint:gocognit,cyclop // existing issue.
	switch len(parts) {
	case depthRoot: // /detector
		switch method {
		case http.MethodPost:
			return opCreateDetector, ""
		case http.MethodGet:
			return opListDetectors, ""
		}

	case depthResource: // /detector/{id}
		detectorID := parts[1]
		switch method {
		case http.MethodGet:
			return opGetDetector, detectorID
		case http.MethodPost:
			return opUpdateDetector, detectorID
		case http.MethodDelete:
			return opDeleteDetector, detectorID
		}

	case depthCollection: // /detector/{id}/{collection}
		detectorID := parts[1]
		collection := parts[2]
		if op, res := parseDetectorCollection(method, detectorID, collection); op != opUnknown {
			return op, res
		}

	case depthItem: // /detector/{id}/{collection}/{item}
		detectorID := parts[1]
		collection := parts[2]
		item := parts[3]
		if op, res := parseDetectorItem(method, detectorID, collection, item); op != opUnknown {
			return op, res
		}

	case depthDeep: // /detector/{id}/{collection}/{sub}/{action}
		detectorID := parts[1]
		collection := parts[2]
		sub := parts[3]
		action := parts[4]
		if collection == pathMember && sub == "detector" {
			switch action {
			case "get": //nolint:goconst // existing issue.
				if method == http.MethodPost {
					return opGetMemberDetectors, detectorID
				}
			case "update":
				if method == http.MethodPost {
					return opUpdateMemberDetectors, detectorID
				}
			}
		}
	}

	return opUnknown, ""
}

func parseDetectorCollection( //nolint:gocognit,gocyclo,cyclop,funlen // existing issue.
	method, detectorID, collection string,
) (string, string) {
	switch collection {
	case pathFilter:
		switch method {
		case http.MethodPost:
			return opCreateFilter, detectorID
		case http.MethodGet:
			return opListFilters, detectorID
		}
	case pathFindings:
		if method == http.MethodPost {
			return opListFindings, detectorID
		}
	case pathIPSet:
		switch method {
		case http.MethodPost:
			return opCreateIPSet, detectorID
		case http.MethodGet:
			return opListIPSets, detectorID
		}
	case pathThreatIntelSet:
		switch method {
		case http.MethodPost:
			return opCreateThreatIntelSet, detectorID
		case http.MethodGet:
			return opListThreatIntelSets, detectorID
		}
	case pathMember:
		switch method {
		case http.MethodPost:
			return opCreateMembers, detectorID
		case http.MethodGet:
			return opListMembers, detectorID
		}
	case pathAdministrator:
		switch method {
		case http.MethodPost:
			return opAcceptAdministratorInvitation, detectorID
		case http.MethodGet:
			return opGetAdministratorAccount, detectorID
		}
	case pathMaster:
		switch method {
		case http.MethodPost:
			return opAcceptInvitation, detectorID
		case http.MethodGet:
			return opGetMasterAccount, detectorID
		}
	case pathAdmin:
		switch method {
		case http.MethodGet:
			return opDescribeOrganizationConfiguration, detectorID
		case http.MethodPost:
			return opUpdateOrganizationConfiguration, detectorID
		}
	case pathPublishingDestination:
		switch method {
		case http.MethodPost:
			return opCreatePublishingDestination, detectorID
		case http.MethodGet:
			return opListPublishingDestinations, detectorID
		}
	case pathMalwareScans:
		if method == http.MethodPost {
			return opDescribeMalwareScans, detectorID
		}
	case pathMalwareScanSettings:
		switch method {
		case http.MethodGet:
			return opGetMalwareScanSettings, detectorID
		case http.MethodPost:
			return opUpdateMalwareScanSettings, detectorID
		}
	case pathThreatEntitySet:
		switch method {
		case http.MethodPost:
			return opCreateThreatEntitySet, detectorID
		case http.MethodGet:
			return opListThreatEntitySets, detectorID
		}
	case pathTrustedEntitySet:
		switch method {
		case http.MethodPost:
			return opCreateTrustedEntitySet, detectorID
		case http.MethodGet:
			return opListTrustedEntitySets, detectorID
		}
	case pathCoverage:
		if method == http.MethodPost {
			return opListCoverage, detectorID
		}
	}

	return opUnknown, ""
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
	case "get":
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
	case "get":
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

func parseDetectorItem(method, detectorID, collection, item string) (string, string) {
	switch collection {
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
