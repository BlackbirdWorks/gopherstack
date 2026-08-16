package macie2

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

// routedPathPrefixes lists every top-level path segment RouteMatcher
// recognizes as belonging to Macie2, one entry per pathXxx constant. Kept as
// a table (rather than a long ||-chain) so RouteMatcher stays a flat loop
// regardless of how many route families this service grows to cover.
var routedPathPrefixes = []string{ //nolint:gochecknoglobals // static route table, apigatewayv2 style.
	pathMacie, pathAllowLists, pathCustomDataIDs, pathFindingsFilter, pathFindings,
	pathJobs, pathMembers, pathInvitations, pathAdministrator, pathMaster, pathAdmin,
	pathAutomatedDiscovery, pathDatasources, pathClassExportCfg, pathClassScopes,
	pathFindingsPubCfg, pathResourceProf, pathRevealCfg, pathUsage, pathManagedDataIDs,
	pathTemplates,
}

// ambiguousRoutedPathPrefixes are routedPathPrefixes entries that also
// prefix-match another registered service's real paths -- SecurityHub's
// GetFindings/BatchImportFindings live under /findings* and its
// CreateMembers family under /members*, both of which this handler's plain
// prefix check would otherwise swallow before SecurityHub's own
// (lower-registration-order) matcher ever runs. Gated by isMacie2Request
// instead of narrowing the prefix, since real Macie2 also uses these exact
// prefixes for its own findings/members ops.
var ambiguousRoutedPathPrefixes = map[string]bool{ //nolint:gochecknoglobals // read-only lookup data
	pathFindings: true,
	pathMembers:  true,
}

// RouteMatcher returns a function that matches Macie2 requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, "/"+pathTags+"/arn:aws:macie2:") {
			return true
		}

		for _, prefix := range routedPathPrefixes {
			if !strings.HasPrefix(path, "/"+prefix) {
				continue
			}

			if ambiguousRoutedPathPrefixes[prefix] && !isMacie2Request(c) {
				continue
			}

			return true
		}

		return false
	}
}

// isMacie2Request checks the Authorization header for the macie2 signing service.
func isMacie2Request(c *echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")

	return strings.Contains(auth, "/"+macie2Service+"/")
}

// restRouter returns the shared REST-path routing/dispatch wiring for this
// handler. See service.RESTRouter: Macie2's routing reduces entirely to
// parsing an operation out of (method, path) and dispatching on it, so the
// HTTP glue lives in pkgs/service instead of being duplicated here.
func (h *Handler) restRouter() service.RESTRouter {
	return service.RESTRouter{
		ServiceName: macie2Service,
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

// opFamilyDispatcher tries to handle op against one op family. ok is false
// when op doesn't belong to that family, signaling dispatch to try the next
// one in the table.
type opFamilyDispatcher func() (result any, code int, ok bool, err error)

// opFamilyDispatchers returns every op-family dispatcher, tried in order
// against a fixed (op, path, query, body) request -- replacing what used to
// be a long chain of `if result, code, ok, err := h.dispatchXxx(...); ok`
// checks in dispatch. Kept as a table so dispatch itself stays a flat loop
// regardless of how many op families this service grows to cover.
func (h *Handler) opFamilyDispatchers(op, path, query string, body []byte) []opFamilyDispatcher {
	return []opFamilyDispatcher{
		func() (any, int, bool, error) { return h.dispatchSessionOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchAllowListOps(op, path, query, body) },
		func() (any, int, bool, error) { return h.dispatchCustomDataIDOps(op, path, body) },
		func() (any, int, bool, error) { return h.dispatchFindingsFilterOps(op, path, query, body) },
		func() (any, int, bool, error) { return h.dispatchFindingOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchFindingRevealOps(op, path) },
		func() (any, int, bool, error) { return h.dispatchClassificationJobOps(op, path, body) },
		func() (any, int, bool, error) { return h.dispatchMemberOps(op, path, query, body) },
		func() (any, int, bool, error) { return h.dispatchInvitationOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchAdministratorOps(op) },
		func() (any, int, bool, error) { return h.dispatchOrganizationOps(op, query, body) },
		func() (any, int, bool, error) { return h.dispatchAutomatedDiscoveryOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchBucketOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchClassificationExportConfigOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchFindingsPublicationConfigOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchScopeOps(op, path, body) },
		func() (any, int, bool, error) { return h.dispatchResourceProfileOps(op, query, body) },
		func() (any, int, bool, error) { return h.dispatchRevealOps(op, body) },
		func() (any, int, bool, error) { return h.dispatchSensitivityTemplateOps(op, path, body) },
		func() (any, int, bool, error) { return h.dispatchUsageOps(op, query, body) },
	}
}

func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	for _, tryFamily := range h.opFamilyDispatchers(op, path, query, body) {
		if result, code, ok, err := tryFamily(); ok {
			return result, code, err
		}
	}

	return h.dispatchTagOps(op, path, query, body)
}

// pathParser maps (method, pathParts) → (operation, resource) for one
// top-level path segment. Every parseXxxPath function in this package has
// this exact signature.
type pathParser func(method string, parts []string) (string, string)

// pathParsers is the (lazily built, then memoized) table from top-level path
// segment to its parser, replacing what used to be a 20+ case switch in
// parseRESTPath -- kept as a sync.OnceValue-backed map (apigatewayv2 style)
// so parseRESTPath itself stays a flat lookup regardless of how many route
// families this service grows to cover.
//
//nolint:gochecknoglobals // static route table, apigatewayv2 style.
var pathParsers = sync.OnceValue(func() map[string]pathParser {
	return map[string]pathParser{
		pathMacie:              parseMaciePath,
		pathAllowLists:         parseAllowListPath,
		pathCustomDataIDs:      parseCustomDataIDPath,
		pathFindingsFilter:     parseFindingsFilterPath,
		pathFindings:           parseFindingsPath,
		pathTags:               parseTagPath,
		pathJobs:               parseJobPath,
		pathMembers:            parseMembersPath,
		pathInvitations:        parseInvitationsPath,
		pathAdministrator:      parseAdministratorPath,
		pathMaster:             parseMasterPath,
		pathAdmin:              parseAdminPath,
		pathAutomatedDiscovery: parseAutomatedDiscoveryPath,
		pathDatasources:        parseDatasourcesPath,
		pathClassExportCfg:     parseClassExportCfgPath,
		pathClassScopes:        parseClassScopesPath,
		pathFindingsPubCfg:     parseFindingsPubCfgPath,
		pathResourceProf:       parseResourceProfilesPath,
		pathRevealCfg:          parseRevealCfgPath,
		pathUsage:              parseUsagePath,
		pathManagedDataIDs:     parseManagedDataIDsPath,
		pathTemplates:          parseTemplatesPath,
	}
})

// parseRESTPath maps (method, path) → (operation, resource).
func parseRESTPath(method, path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 {
		return opUnknown, ""
	}

	parser, ok := pathParsers()[parts[0]]
	if !ok {
		return opUnknown, ""
	}

	return parser(method, parts)
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
