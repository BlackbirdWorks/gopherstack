package macie2

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

	if result, code, ok, err := h.dispatchAllowListOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchCustomDataIDOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingsFilterOps(op, path, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchFindingOps(op, path, body); ok {
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

	if result, code, ok, err := h.dispatchAdminOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchAutomatedDiscoveryOps(op, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchBucketOps(op, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchConfigOps(op, body); ok {
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

	if result, code, ok, err := h.dispatchSearchOps(op, body); ok {
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

func parseMaciePath(method string, parts []string) (string, string) {
	// /macie/members/{id} → UpdateMemberSession
	if len(parts) == 3 && parts[1] == pathMembers && method == http.MethodPatch {
		return opUpdateMemberSession, parts[2]
	}

	switch method {
	case http.MethodGet:
		return opGetMacieSession, ""
	case http.MethodPost:
		return opEnableMacie, ""
	case http.MethodDelete:
		return opDisableMacie, ""
	case http.MethodPatch:
		return opUpdateMacieSession, ""
	}

	return opUnknown, ""
}

func parseAllowListPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /allow-lists
		switch method {
		case http.MethodPost:
			return opCreateAllowList, ""
		case http.MethodGet:
			return opListAllowLists, ""
		}
	case depthResource: // /allow-lists/{id}
		id := parts[1]
		switch method {
		case http.MethodGet:
			return opGetAllowList, id
		case http.MethodPatch:
			return opUpdateAllowList, id
		case http.MethodDelete:
			return opDeleteAllowList, id
		}
	}

	return opUnknown, ""
}

func parseCustomDataIDPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /custom-data-identifiers
		if method == http.MethodPost {
			return opCreateCustomDataID, ""
		}
	case depthResource: // /custom-data-identifiers/{id|list|test}
		sub := parts[1]
		switch sub {
		case "list": //nolint:goconst // existing issue.
			if method == http.MethodPost {
				return opListCustomDataIDs, ""
			}
		case "test":
			if method == http.MethodPost {
				return opTestCustomDataID, ""
			}
		case "get":
			if method == http.MethodPost {
				return opBatchGetCustomDataIdentifiers, ""
			}
		default:
			switch method {
			case http.MethodGet:
				return opGetCustomDataID, sub
			case http.MethodDelete:
				return opDeleteCustomDataID, sub
			}
		}
	}

	return opUnknown, ""
}

func parseFindingsFilterPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /findingsfilters
		switch method {
		case http.MethodPost:
			return opCreateFindingsFilter, ""
		case http.MethodGet:
			return opListFindingsFilters, ""
		}
	case depthResource: // /findingsfilters/{id}
		id := parts[1]
		switch method {
		case http.MethodGet:
			return opGetFindingsFilter, id
		case http.MethodPatch:
			return opUpdateFindingsFilter, id
		case http.MethodDelete:
			return opDeleteFindingsFilter, id
		}
	}

	return opUnknown, ""
}

func parseFindingsPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /findings
		if method == http.MethodPost {
			return opListFindings, ""
		}
	case depthResource: // /findings/{action}
		if method == http.MethodPost {
			switch parts[1] {
			case "describe":
				return opGetFindings, ""
			case "sample":
				return opCreateSampleFindings, ""
			case "statistics": //nolint:goconst // existing issue.
				return opGetFindingStatistics, ""
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[2] == "reveal" && method == http.MethodGet {
			return opGetSensitiveDataOccurrences, parts[1]
		}
	case 4: //nolint:mnd // existing issue.
		if parts[2] == "reveal" && parts[3] == "availability" && method == http.MethodGet {
			return opGetSensitiveDataOccurrencesAvailability, parts[1]
		}
	}

	return opUnknown, ""
}

func parseTagPath(method string, parts []string) (string, string) {
	if len(parts) < minTagPathParts {
		return opUnknown, ""
	}

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

func (h *Handler) dispatchSessionOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetMacieSession:
		result, code := h.handleGetMacieSession()

		return result, code, true, nil

	case opEnableMacie:
		code, err := h.handleEnableMacie(body)

		return nil, code, true, err

	case opDisableMacie:
		code, err := h.handleDisableMacie()

		return nil, code, true, err

	case opUpdateMacieSession:
		result, code, err := h.handleUpdateMacieSession(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchAllowListOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateAllowList:
		result, code, err := h.handleCreateAllowList(body)

		return result, code, true, err

	case opGetAllowList:
		id := extractID(path, pathAllowLists)
		result, code, err := h.handleGetAllowList(id)

		return result, code, true, err

	case opUpdateAllowList:
		id := extractID(path, pathAllowLists)
		result, code, err := h.handleUpdateAllowList(id, body)

		return result, code, true, err

	case opDeleteAllowList:
		id := extractID(path, pathAllowLists)
		code, err := h.handleDeleteAllowList(id)

		return nil, code, true, err

	case opListAllowLists:
		result, code := h.handleListAllowLists()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchCustomDataIDOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateCustomDataID:
		result, code, err := h.handleCreateCustomDataID(body)

		return result, code, true, err

	case opGetCustomDataID:
		id := extractID(path, pathCustomDataIDs)
		result, code, err := h.handleGetCustomDataID(id)

		return result, code, true, err

	case opDeleteCustomDataID:
		id := extractID(path, pathCustomDataIDs)
		code, err := h.handleDeleteCustomDataID(id)

		return nil, code, true, err

	case opListCustomDataIDs:
		result, code, err := h.handleListCustomDataIDs()

		return result, code, true, err

	case opTestCustomDataID:
		result, code, err := h.handleTestCustomDataID(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchFindingsFilterOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateFindingsFilter:
		result, code, err := h.handleCreateFindingsFilter(body)

		return result, code, true, err

	case opGetFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		result, code, err := h.handleGetFindingsFilter(id)

		return result, code, true, err

	case opUpdateFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		result, code, err := h.handleUpdateFindingsFilter(id, body)

		return result, code, true, err

	case opDeleteFindingsFilter:
		id := extractID(path, pathFindingsFilter)
		code, err := h.handleDeleteFindingsFilter(id)

		return nil, code, true, err

	case opListFindingsFilters:
		result, code := h.handleListFindingsFilters()

		return result, code, true, nil
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchFindingOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetFindings:
		result, code, err := h.handleGetFindings(body)

		return result, code, true, err

	case opListFindings:
		result, code := h.handleListFindings()

		return result, code, true, nil

	case opCreateSampleFindings:
		code, err := h.handleCreateSampleFindings(body)

		return nil, code, true, err

	case opGetFindingStatistics:
		result, code, err := h.handleGetFindingStatistics(body)

		return result, code, true, err

	case opGetSensitiveDataOccurrences:
		findingID := extractFindingRevealID(path)
		result, code, err := h.handleGetSensitiveDataOccurrences(findingID)

		return result, code, true, err

	case opGetSensitiveDataOccurrencesAvailability:
		findingID := extractFindingRevealID(path)
		result, code, err := h.handleGetSensitiveDataOccurrencesAvailability(findingID)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	switch op {
	case opListTagsForResource:
		resourceARN := extractTagARN(path)
		result, code, err := h.handleListTagsForResource(resourceARN)

		return result, code, err

	case opTagResource:
		resourceARN := extractTagARN(path)
		code, err := h.handleTagResource(resourceARN, body)

		return nil, code, err

	case opUntagResource:
		resourceARN := extractTagARN(path)
		code, err := h.handleUntagResource(resourceARN, query)

		return nil, code, err
	}

	return nil, http.StatusNotFound, nil
}

// Session handlers

func (h *Handler) handleGetMacieSession() (any, int) {
	session := h.Backend.GetSession()
	if session == nil {
		return errBody(errMacieNotEnabled, "Macie is not enabled for this account"), http.StatusForbidden
	}

	return map[string]any{
		"createdAt":                  session.CreatedAt,
		"findingPublishingFrequency": session.FindingPublishingFrequency,
		"serviceRole":                session.ServiceRole,
		"status":                     session.Status,
		"updatedAt":                  session.UpdatedAt,
	}, http.StatusOK
}

func (h *Handler) handleEnableMacie(body []byte) (int, error) {
	var req struct {
		ClientToken                string `json:"clientToken"`
		FindingPublishingFrequency string `json:"findingPublishingFrequency"`
		Status                     string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if req.FindingPublishingFrequency != "" && !validFrequency(req.FindingPublishingFrequency) {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableMacie(req.ClientToken, req.FindingPublishingFrequency, req.Status); err != nil {
		if errors.Is(err, awserr.ErrConflict) {
			return http.StatusConflict, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableMacie() (int, error) {
	if err := h.Backend.DisableMacie(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateMacieSession(body []byte) (any, int, error) {
	if h.Backend.GetSession() == nil {
		return errBody(errMacieNotEnabled, "Macie is not currently enabled for this account"), http.StatusForbidden, nil
	}

	var req struct {
		FindingPublishingFrequency string `json:"findingPublishingFrequency"`
		Status                     string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	if req.FindingPublishingFrequency != "" && !validFrequency(req.FindingPublishingFrequency) {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateMacieSession(req.FindingPublishingFrequency, req.Status); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return nil, http.StatusOK, nil
}

// Allow list handlers

func (h *Handler) handleCreateAllowList(body []byte) (any, int, error) {
	var req struct {
		Criteria    *AllowListCriteria `json:"criteria"`
		Tags        map[string]string  `json:"tags"`
		ClientToken string             `json:"clientToken"`
		Description string             `json:"description"`
		Name        string             `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	criteria := AllowListCriteria{}
	if req.Criteria != nil {
		criteria = *req.Criteria
	}

	al, err := h.Backend.CreateAllowList(req.Name, req.Description, criteria, req.Tags)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: al.Arn, "id": al.ID}, http.StatusOK, nil
}

func (h *Handler) handleGetAllowList(id string) (any, int, error) {
	al, err := h.Backend.GetAllowList(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return al, http.StatusOK, nil
}

func (h *Handler) handleUpdateAllowList(id string, body []byte) (any, int, error) {
	var req struct {
		Criteria    *AllowListCriteria `json:"criteria"`
		Description string             `json:"description"`
		Name        string             `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	criteria := AllowListCriteria{}
	if req.Criteria != nil {
		criteria = *req.Criteria
	}

	al, err := h.Backend.UpdateAllowList(id, req.Name, req.Description, criteria)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: al.Arn, "id": al.ID}, http.StatusOK, nil
}

func (h *Handler) handleDeleteAllowList(id string) (int, error) {
	if err := h.Backend.DeleteAllowList(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListAllowLists() (any, int) {
	lists, _ := h.Backend.ListAllowLists()

	return map[string]any{"allowLists": lists}, http.StatusOK
}

// Custom data identifier handlers

func (h *Handler) handleCreateCustomDataID(body []byte) (any, int, error) {
	var req struct {
		MaximumMatchDistance *int32            `json:"maximumMatchDistance"`
		Tags                 map[string]string `json:"tags"`
		ClientToken          string            `json:"clientToken"`
		Description          string            `json:"description"`
		Name                 string            `json:"name"`
		Regex                string            `json:"regex"`
		IgnoreWords          []string          `json:"ignoreWords"`
		Keywords             []string          `json:"keywords"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Regex == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	id, err := h.Backend.CreateCustomDataIdentifier(
		req.Name, req.Description, req.Regex,
		req.IgnoreWords, req.Keywords, req.MaximumMatchDistance,
		req.Tags,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return nil, http.StatusBadRequest, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{"customDataIdentifierId": id}, http.StatusOK, nil
}

func (h *Handler) handleGetCustomDataID(id string) (any, int, error) {
	cdi, err := h.Backend.GetCustomDataIdentifier(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return cdi, http.StatusOK, nil
}

func (h *Handler) handleDeleteCustomDataID(id string) (int, error) {
	if err := h.Backend.DeleteCustomDataIdentifier(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListCustomDataIDs() (any, int, error) {
	items, err := h.Backend.ListCustomDataIdentifiers()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"items": items}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleTestCustomDataID(body []byte) (any, int, error) {
	var req struct {
		MaximumMatchDistance *int32   `json:"maximumMatchDistance"`
		Regex                string   `json:"regex"`
		SampleText           string   `json:"sampleText"`
		IgnoreWords          []string `json:"ignoreWords"`
		Keywords             []string `json:"keywords"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	count, err := h.Backend.TestCustomDataIdentifier(
		req.Regex, req.IgnoreWords, req.Keywords,
		req.MaximumMatchDistance, req.SampleText,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return nil, http.StatusBadRequest, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]int32{"matchCount": count}, http.StatusOK, nil
}

// Findings filter handlers

func (h *Handler) handleCreateFindingsFilter(body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any    `json:"findingCriteria"`
		Position        *int32            `json:"position"`
		Tags            map[string]string `json:"tags"`
		Action          string            `json:"action"`
		ClientToken     string            `json:"clientToken"`
		Description     string            `json:"description"`
		Name            string            `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.Action == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	ff, err := h.Backend.CreateFindingsFilter(
		req.Name, req.Description, req.Action,
		req.Position, req.FindingCriteria, req.Tags,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: ff.Arn, "id": ff.ID}, http.StatusOK, nil
}

func (h *Handler) handleGetFindingsFilter(id string) (any, int, error) {
	ff, err := h.Backend.GetFindingsFilter(id)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return ff, http.StatusOK, nil
}

func (h *Handler) handleUpdateFindingsFilter(id string, body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		Position        *int32         `json:"position"`
		Action          string         `json:"action"`
		Description     string         `json:"description"`
		Name            string         `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	ff, err := h.Backend.UpdateFindingsFilter(
		id, req.Name, req.Description, req.Action,
		req.Position, req.FindingCriteria,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{keyArn: ff.Arn, "id": ff.ID}, http.StatusOK, nil
}

func (h *Handler) handleDeleteFindingsFilter(id string) (int, error) {
	if err := h.Backend.DeleteFindingsFilter(id); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListFindingsFilters() (any, int) {
	filters, _ := h.Backend.ListFindingsFilters()

	return map[string]any{"findingsFilterListItems": filters}, http.StatusOK
}

// Finding handlers

func (h *Handler) handleGetFindings(body []byte) (any, int, error) {
	var req struct {
		FindingIDs []string `json:"findingIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	findings, err := h.Backend.GetFindings(req.FindingIDs)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"findings": findings}, http.StatusOK, nil
}

func (h *Handler) handleListFindings() (any, int) {
	ids, _, _ := h.Backend.ListFindings(nil, 0, "")

	return map[string]any{"findingIds": ids}, http.StatusOK
}

func (h *Handler) handleCreateSampleFindings(body []byte) (int, error) {
	var req struct {
		FindingTypes []string `json:"findingTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.CreateSampleFindings(req.FindingTypes); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingStatistics(body []byte) (any, int, error) {
	var req struct {
		FindingCriteria map[string]any `json:"findingCriteria"`
		GroupBy         string         `json:"groupBy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	groups, err := h.Backend.GetFindingStatistics(req.GroupBy, req.FindingCriteria)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"countsByGroup": groups}, http.StatusOK, nil
}

// Tag handlers

func (h *Handler) handleListTagsForResource(resourceARN string) (any, int, error) {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"tags": tags}, http.StatusOK, nil
}

func (h *Handler) handleTagResource(resourceARN string, body []byte) (int, error) {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUntagResource(resourceARN, query string) (int, error) {
	tagKeys := parseTagKeys(query)
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

// Error handling

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

// Helper utilities

func validFrequency(f string) bool {
	return f == "FIFTEEN_MINUTES" || f == "ONE_HOUR" || f == "SIX_HOURS"
}

func errBody(code, message string) map[string]string {
	return map[string]string{"__type": code, "message": message}
}

func extractID(path, prefix string) string {
	trimmed := strings.TrimPrefix(path, "/"+prefix+"/")
	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

func extractTagARN(path string) string {
	trimmed := strings.TrimPrefix(path, "/"+pathTags+"/")

	return trimmed
}

func extractFindingRevealID(path string) string {
	// /findings/{findingId}/reveal[/availability]
	trimmed := strings.TrimPrefix(path, "/"+pathFindings+"/")
	parts := strings.SplitN(trimmed, "/", splitTwo)

	return parts[0]
}

func parseTagKeys(query string) []string {
	var keys []string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "tagKeys="); ok {
			keys = append(keys, v)
		}
	}

	return keys
}
