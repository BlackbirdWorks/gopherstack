package macie2

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	macie2Service = "macie2"
	matchPriority = service.PriorityPathVersioned

	pathMacie          = "macie"
	pathAllowLists     = "allow-lists"
	pathCustomDataIDs  = "custom-data-identifiers"
	pathFindingsFilter = "findingsfilters"
	pathFindings       = "findings"
	pathTags           = "tags"
	// Appendix A paths.
	pathAutomatedDiscovery = "automated-discovery"
	pathJobs               = "jobs"
	pathMembers            = "members"
	pathInvitations        = "invitations"
	pathAdministrator      = "administrator"
	pathMaster             = "master"
	pathAdmin              = "admin"
	pathDatasources        = "datasources"
	pathClassExportCfg     = "classification-export-configuration"
	pathClassScopes        = "classification-scopes"
	pathFindingsPubCfg     = "findings-publication-configuration"
	pathResourceProf       = "resource-profiles"
	pathRevealCfg          = "reveal-configuration"
	pathUsage              = "usage"
	pathManagedDataIDs     = "managed-data-identifiers"
	pathTemplates          = "templates"

	opEnableMacie          = "EnableMacie"
	opDisableMacie         = "DisableMacie"
	opGetMacieSession      = "GetMacieSession"
	opUpdateMacieSession   = "UpdateMacieSession"
	opCreateAllowList      = "CreateAllowList"
	opGetAllowList         = "GetAllowList"
	opUpdateAllowList      = "UpdateAllowList"
	opDeleteAllowList      = "DeleteAllowList"
	opListAllowLists       = "ListAllowLists"
	opCreateCustomDataID   = "CreateCustomDataIdentifier"
	opGetCustomDataID      = "GetCustomDataIdentifier"
	opDeleteCustomDataID   = "DeleteCustomDataIdentifier"
	opListCustomDataIDs    = "ListCustomDataIdentifiers"
	opTestCustomDataID     = "TestCustomDataIdentifier"
	opCreateFindingsFilter = "CreateFindingsFilter"
	opGetFindingsFilter    = "GetFindingsFilter"
	opUpdateFindingsFilter = "UpdateFindingsFilter"
	opDeleteFindingsFilter = "DeleteFindingsFilter"
	opListFindingsFilters  = "ListFindingsFilters"
	opGetFindings          = "GetFindings"
	opListFindings         = "ListFindings"
	opCreateSampleFindings = "CreateSampleFindings"
	opGetFindingStatistics = "GetFindingStatistics"
	opTagResource          = "TagResource"
	opUntagResource        = "UntagResource"
	opListTagsForResource  = "ListTagsForResource"
	opUnknown              = "Unknown"
	// Appendix A operations.
	opCreateClassificationJob                 = "CreateClassificationJob"
	opDescribeClassificationJob               = "DescribeClassificationJob"
	opListClassificationJobs                  = "ListClassificationJobs"
	opUpdateClassificationJob                 = "UpdateClassificationJob"
	opCreateMember                            = "CreateMember"
	opGetMember                               = "GetMember"
	opDeleteMember                            = "DeleteMember"
	opListMembers                             = "ListMembers"
	opDisassociateMember                      = "DisassociateMember"
	opUpdateMemberSession                     = "UpdateMemberSession"
	opCreateInvitations                       = "CreateInvitations"
	opAcceptInvitation                        = "AcceptInvitation"
	opDeclineInvitations                      = "DeclineInvitations"
	opDeleteInvitations                       = "DeleteInvitations"
	opGetInvitationsCount                     = "GetInvitationsCount"
	opListInvitations                         = "ListInvitations"
	opGetAdministratorAccount                 = "GetAdministratorAccount"
	opDisassociateFromAdministratorAccount    = "DisassociateFromAdministratorAccount"
	opGetMasterAccount                        = "GetMasterAccount"
	opDisassociateFromMasterAccount           = "DisassociateFromMasterAccount"
	opEnableOrganizationAdminAccount          = "EnableOrganizationAdminAccount"
	opDisableOrganizationAdminAccount         = "DisableOrganizationAdminAccount"
	opListOrganizationAdminAccounts           = "ListOrganizationAdminAccounts"
	opDescribeOrganizationConfiguration       = "DescribeOrganizationConfiguration"
	opUpdateOrganizationConfiguration         = "UpdateOrganizationConfiguration"
	opGetAutomatedDiscoveryConfiguration      = "GetAutomatedDiscoveryConfiguration"
	opUpdateAutomatedDiscoveryConfiguration   = "UpdateAutomatedDiscoveryConfiguration"
	opListAutomatedDiscoveryAccounts          = "ListAutomatedDiscoveryAccounts"
	opBatchUpdateAutomatedDiscoveryAccounts   = "BatchUpdateAutomatedDiscoveryAccounts"
	opDescribeBuckets                         = "DescribeBuckets"
	opGetBucketStatistics                     = "GetBucketStatistics"
	opBatchGetCustomDataIdentifiers           = "BatchGetCustomDataIdentifiers"
	opGetClassificationExportConfiguration    = "GetClassificationExportConfiguration"
	opPutClassificationExportConfiguration    = "PutClassificationExportConfiguration"
	opGetClassificationScope                  = "GetClassificationScope"
	opListClassificationScopes                = "ListClassificationScopes"
	opUpdateClassificationScope               = "UpdateClassificationScope"
	opGetFindingsPublicationConfiguration     = "GetFindingsPublicationConfiguration"
	opPutFindingsPublicationConfiguration     = "PutFindingsPublicationConfiguration"
	opGetResourceProfile                      = "GetResourceProfile"
	opUpdateResourceProfile                   = "UpdateResourceProfile"
	opListResourceProfileArtifacts            = "ListResourceProfileArtifacts"
	opListResourceProfileDetections           = "ListResourceProfileDetections"
	opUpdateResourceProfileDetections         = "UpdateResourceProfileDetections"
	opGetRevealConfiguration                  = "GetRevealConfiguration"
	opUpdateRevealConfiguration               = "UpdateRevealConfiguration"
	opGetSensitiveDataOccurrences             = "GetSensitiveDataOccurrences"
	opGetSensitiveDataOccurrencesAvailability = "GetSensitiveDataOccurrencesAvailability"
	opGetSensitivityInspectionTemplate        = "GetSensitivityInspectionTemplate"
	opListSensitivityInspectionTemplates      = "ListSensitivityInspectionTemplates"
	opUpdateSensitivityInspectionTemplate     = "UpdateSensitivityInspectionTemplate"
	opGetUsageStatistics                      = "GetUsageStatistics"
	opGetUsageTotals                          = "GetUsageTotals"
	opListManagedDataIdentifiers              = "ListManagedDataIdentifiers"
	opSearchResources                         = "SearchResources"

	minTagPathParts = 2

	keyArn        = "arn"
	depthRoot     = 1
	depthResource = 2
	splitTwo      = 2

	// Shared sub-path/response-key literals (repeated across family files).
	segDisassociate  = "disassociate"
	keyConfiguration = "configuration"
	segStatistics    = "statistics"
	keyItems         = "items"
)

// Handler handles Macie2 HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Macie2" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opEnableMacie,
		opDisableMacie,
		opGetMacieSession,
		opUpdateMacieSession,
		opCreateAllowList,
		opGetAllowList,
		opUpdateAllowList,
		opDeleteAllowList,
		opListAllowLists,
		opCreateCustomDataID,
		opGetCustomDataID,
		opDeleteCustomDataID,
		opListCustomDataIDs,
		opTestCustomDataID,
		opCreateFindingsFilter,
		opGetFindingsFilter,
		opUpdateFindingsFilter,
		opDeleteFindingsFilter,
		opListFindingsFilters,
		opGetFindings,
		opListFindings,
		opCreateSampleFindings,
		opGetFindingStatistics,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Appendix A
		opCreateClassificationJob,
		opDescribeClassificationJob,
		opListClassificationJobs,
		opUpdateClassificationJob,
		opCreateMember,
		opGetMember,
		opDeleteMember,
		opListMembers,
		opDisassociateMember,
		opUpdateMemberSession,
		opCreateInvitations,
		opAcceptInvitation,
		opDeclineInvitations,
		opDeleteInvitations,
		opGetInvitationsCount,
		opListInvitations,
		opGetAdministratorAccount,
		opDisassociateFromAdministratorAccount,
		opGetMasterAccount,
		opDisassociateFromMasterAccount,
		opEnableOrganizationAdminAccount,
		opDisableOrganizationAdminAccount,
		opListOrganizationAdminAccounts,
		opDescribeOrganizationConfiguration,
		opUpdateOrganizationConfiguration,
		opGetAutomatedDiscoveryConfiguration,
		opUpdateAutomatedDiscoveryConfiguration,
		opListAutomatedDiscoveryAccounts,
		opBatchUpdateAutomatedDiscoveryAccounts,
		opDescribeBuckets,
		opGetBucketStatistics,
		opBatchGetCustomDataIdentifiers,
		opGetClassificationExportConfiguration,
		opPutClassificationExportConfiguration,
		opGetClassificationScope,
		opListClassificationScopes,
		opUpdateClassificationScope,
		opGetFindingsPublicationConfiguration,
		opPutFindingsPublicationConfiguration,
		opGetResourceProfile,
		opUpdateResourceProfile,
		opListResourceProfileArtifacts,
		opListResourceProfileDetections,
		opUpdateResourceProfileDetections,
		opGetRevealConfiguration,
		opUpdateRevealConfiguration,
		opGetSensitiveDataOccurrences,
		opGetSensitiveDataOccurrencesAvailability,
		opGetSensitivityInspectionTemplate,
		opListSensitivityInspectionTemplates,
		opUpdateSensitivityInspectionTemplate,
		opGetUsageStatistics,
		opGetUsageTotals,
		opListManagedDataIdentifiers,
		opSearchResources,
	}
}

// RouteMatcher returns a function that matches Macie2 requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher { //nolint:cyclop // existing issue.
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/"+pathMacie) ||
			strings.HasPrefix(path, "/"+pathAllowLists) ||
			strings.HasPrefix(path, "/"+pathCustomDataIDs) ||
			strings.HasPrefix(path, "/"+pathFindingsFilter) ||
			strings.HasPrefix(path, "/"+pathFindings) ||
			strings.HasPrefix(path, "/"+pathTags+"/arn:aws:macie2:") ||
			strings.HasPrefix(path, "/"+pathJobs) ||
			strings.HasPrefix(path, "/"+pathMembers) ||
			strings.HasPrefix(path, "/"+pathInvitations) ||
			strings.HasPrefix(path, "/"+pathAdministrator) ||
			strings.HasPrefix(path, "/"+pathMaster) ||
			strings.HasPrefix(path, "/"+pathAdmin) ||
			strings.HasPrefix(path, "/"+pathAutomatedDiscovery) ||
			strings.HasPrefix(path, "/"+pathDatasources) ||
			strings.HasPrefix(path, "/"+pathClassExportCfg) ||
			strings.HasPrefix(path, "/"+pathClassScopes) ||
			strings.HasPrefix(path, "/"+pathFindingsPubCfg) ||
			strings.HasPrefix(path, "/"+pathResourceProf) ||
			strings.HasPrefix(path, "/"+pathRevealCfg) ||
			strings.HasPrefix(path, "/"+pathUsage) ||
			strings.HasPrefix(path, "/"+pathManagedDataIDs) ||
			strings.HasPrefix(path, "/"+pathTemplates)
	}
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
		log.Error("macie2 operation error", "op", op, "err", opErr)

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

func (h *Handler) dispatch( //nolint:cyclop // existing issue.
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, ok, err := h.dispatchSessionOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchAllowListOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchCustomDataIDOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingsFilterOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingRevealOps(op, path); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchClassificationJobOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMemberOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchInvitationOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchAdministratorOps(op); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchOrganizationOps(op, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchAutomatedDiscoveryOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchBucketOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchClassificationExportConfigOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingsPublicationConfigOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchScopeOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchResourceProfileOps(op, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchRevealOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchSensitivityTemplateOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchUsageOps(op, query, body); ok {
		return result, code, err
	}

	return h.dispatchTagOps(op, path, query, body)
}

// parseRESTPath maps (method, path) → (operation, resource).
func parseRESTPath(method, path string) (string, string) { //nolint:cyclop // existing issue.
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 {
		return opUnknown, ""
	}

	switch parts[0] {
	case pathMacie:
		return parseMaciePath(method, parts)
	case pathAllowLists:
		return parseAllowListPath(method, parts)
	case pathCustomDataIDs:
		return parseCustomDataIDPath(method, parts)
	case pathFindingsFilter:
		return parseFindingsFilterPath(method, parts)
	case pathFindings:
		return parseFindingsPath(method, parts)
	case pathTags:
		return parseTagPath(method, parts)
	case pathJobs:
		return parseJobPath(method, parts)
	case pathMembers:
		return parseMembersPath(method, parts)
	case pathInvitations:
		return parseInvitationsPath(method, parts)
	case pathAdministrator:
		return parseAdministratorPath(method, parts)
	case pathMaster:
		return parseMasterPath(method, parts)
	case pathAdmin:
		return parseAdminPath(method, parts)
	case pathAutomatedDiscovery:
		return parseAutomatedDiscoveryPath(method, parts)
	case pathDatasources:
		return parseDatasourcesPath(method, parts)
	case pathClassExportCfg:
		return parseClassExportCfgPath(method, parts)
	case pathClassScopes:
		return parseClassScopesPath(method, parts)
	case pathFindingsPubCfg:
		return parseFindingsPubCfgPath(method, parts)
	case pathResourceProf:
		return parseResourceProfilesPath(method, parts)
	case pathRevealCfg:
		return parseRevealCfgPath(method, parts)
	case pathUsage:
		return parseUsagePath(method, parts)
	case pathManagedDataIDs:
		return parseManagedDataIDsPath(method, parts)
	case pathTemplates:
		return parseTemplatesPath(method, parts)
	}

	return opUnknown, ""
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	msg := err.Error()

	var code int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, awserr.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, awserr.ErrInvalidParameter):
		code = http.StatusBadRequest
	default:
		code = http.StatusInternalServerError
	}

	return c.JSON(code, errBody(msg, msg))
}

func errBody(code, message string) map[string]string {
	return map[string]string{"__type": code, "message": message}
}

func extractID(path, prefix string) string {
	trimmed := strings.TrimPrefix(path, "/"+prefix+"/")
	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

func extractQueryParam(query, key string) string {
	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, key+"="); ok {
			return v
		}
	}

	return ""
}
