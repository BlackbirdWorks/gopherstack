package cleanrooms

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Path sub-resource name constants (goconst).
const (
	subAnalysisTemplates  = "analysistemplates"
	subCAMAAssociations   = "configuredaudiencemodelassociations"
	subIDNamespaceAssocs  = "idnamespaceassociations"
	subPrivacyBudgetTmpls = "privacybudgettemplates"
	subSchemas            = "schemas"
	subAnalysisRule       = "analysisRule"
	subProtectedJobs      = "protectedJobs"
	subProtectedQueries   = "protectedQueries"
	subTags               = "tags"
)

// Response key constants (goconst).
const (
	keyCollaboration              = "collaboration"
	keyAnalysisTemplate           = "analysisTemplate"
	keyErrors                     = "errors"
	keyCollaborationChangeRequest = "collaborationChangeRequest"
	keyCAMAAssociation            = "configuredAudienceModelAssociation"
	keyIDNamespaceAssociation     = "idNamespaceAssociation"
	keyPrivacyBudgetTemplate      = "privacyBudgetTemplate"
	keyMembership                 = "membership"
	keyConfiguredTable            = "configuredTable"
	keyConfiguredTableAssociation = "configuredTableAssociation"
	keyProtectedQuery             = "protectedQuery"
	keyProtectedJob               = "protectedJob"
	keyIDMappingTable             = "idMappingTable"
	keyAnalysisRule               = "analysisRule"
)

// Path segment count constants (mnd).
const (
	segsRoot         = 1 // just the resource name
	segsWithID       = 2 // resource + ID
	segsWithSub      = 3 // resource + ID + sub
	segsWithSubID    = 4 // resource + ID + sub + subID
	segsWithSubSub   = 5 // 5 segments
	segsWithSubSubID = 6 // 6 segments
)

const (
	cleanroomsHostPrefix = "cleanrooms."

	opBatchGetCollaborationAnalysisTemplate                = "BatchGetCollaborationAnalysisTemplate"
	opBatchGetSchema                                       = "BatchGetSchema"
	opBatchGetSchemaAnalysisRule                           = "BatchGetSchemaAnalysisRule"
	opCreateAnalysisTemplate                               = "CreateAnalysisTemplate"
	opCreateCollaboration                                  = "CreateCollaboration"
	opCreateCollaborationChangeRequest                     = "CreateCollaborationChangeRequest"
	opCreateConfiguredAudienceModelAssociation             = "CreateConfiguredAudienceModelAssociation"
	opCreateConfiguredTable                                = "CreateConfiguredTable"
	opCreateConfiguredTableAnalysisRule                    = "CreateConfiguredTableAnalysisRule"
	opCreateConfiguredTableAssociation                     = "CreateConfiguredTableAssociation"
	opCreateConfiguredTableAssociationAnalysisRule         = "CreateConfiguredTableAssociationAnalysisRule"
	opCreateIDMappingTable                                 = "CreateIdMappingTable"
	opCreateIDNamespaceAssociation                         = "CreateIdNamespaceAssociation"
	opCreateMembership                                     = "CreateMembership"
	opCreatePrivacyBudgetTemplate                          = "CreatePrivacyBudgetTemplate"
	opDeleteAnalysisTemplate                               = "DeleteAnalysisTemplate"
	opDeleteCollaboration                                  = "DeleteCollaboration"
	opDeleteConfiguredAudienceModelAssociation             = "DeleteConfiguredAudienceModelAssociation"
	opDeleteConfiguredTable                                = "DeleteConfiguredTable"
	opDeleteConfiguredTableAnalysisRule                    = "DeleteConfiguredTableAnalysisRule"
	opDeleteConfiguredTableAssociation                     = "DeleteConfiguredTableAssociation"
	opDeleteConfiguredTableAssociationAnalysisRule         = "DeleteConfiguredTableAssociationAnalysisRule"
	opDeleteIDMappingTable                                 = "DeleteIdMappingTable"
	opDeleteIDNamespaceAssociation                         = "DeleteIdNamespaceAssociation"
	opDeleteMember                                         = "DeleteMember"
	opDeleteMembership                                     = "DeleteMembership"
	opDeletePrivacyBudgetTemplate                          = "DeletePrivacyBudgetTemplate"
	opGetAnalysisTemplate                                  = "GetAnalysisTemplate"
	opGetCollaboration                                     = "GetCollaboration"
	opGetCollaborationAnalysisTemplate                     = "GetCollaborationAnalysisTemplate"
	opGetCollaborationChangeRequest                        = "GetCollaborationChangeRequest"
	opGetCollaborationConfiguredAudienceModelAssociation   = "GetCollaborationConfiguredAudienceModelAssociation"
	opGetCollaborationIDNamespaceAssociation               = "GetCollaborationIdNamespaceAssociation"
	opGetCollaborationPrivacyBudgetTemplate                = "GetCollaborationPrivacyBudgetTemplate"
	opGetConfiguredAudienceModelAssociation                = "GetConfiguredAudienceModelAssociation"
	opGetConfiguredTable                                   = "GetConfiguredTable"
	opGetConfiguredTableAnalysisRule                       = "GetConfiguredTableAnalysisRule"
	opGetConfiguredTableAssociation                        = "GetConfiguredTableAssociation"
	opGetConfiguredTableAssociationAnalysisRule            = "GetConfiguredTableAssociationAnalysisRule"
	opGetIDMappingTable                                    = "GetIdMappingTable"
	opGetIDNamespaceAssociation                            = "GetIdNamespaceAssociation"
	opGetMembership                                        = "GetMembership"
	opGetPrivacyBudgetTemplate                             = "GetPrivacyBudgetTemplate"
	opGetProtectedJob                                      = "GetProtectedJob"
	opGetProtectedQuery                                    = "GetProtectedQuery"
	opGetSchema                                            = "GetSchema"
	opGetSchemaAnalysisRule                                = "GetSchemaAnalysisRule"
	opListAnalysisTemplates                                = "ListAnalysisTemplates"
	opListCollaborationAnalysisTemplates                   = "ListCollaborationAnalysisTemplates"
	opListCollaborationChangeRequests                      = "ListCollaborationChangeRequests"
	opListCollaborationConfiguredAudienceModelAssociations = "ListCollaborationConfiguredAudienceModelAssociations"
	opListCollaborationIDNamespaceAssociations             = "ListCollaborationIdNamespaceAssociations"
	opListCollaborationPrivacyBudgets                      = "ListCollaborationPrivacyBudgets"
	opListCollaborationPrivacyBudgetTemplates              = "ListCollaborationPrivacyBudgetTemplates"
	opListCollaborations                                   = "ListCollaborations"
	opListConfiguredAudienceModelAssociations              = "ListConfiguredAudienceModelAssociations"
	opListConfiguredTableAssociations                      = "ListConfiguredTableAssociations"
	opListConfiguredTables                                 = "ListConfiguredTables"
	opListIDMappingTables                                  = "ListIdMappingTables"
	opListIDNamespaceAssociations                          = "ListIdNamespaceAssociations"
	opListMembers                                          = "ListMembers"
	opListMemberships                                      = "ListMemberships"
	opListPrivacyBudgets                                   = "ListPrivacyBudgets"
	opListPrivacyBudgetTemplates                           = "ListPrivacyBudgetTemplates"
	opListProtectedJobs                                    = "ListProtectedJobs"
	opListProtectedQueries                                 = "ListProtectedQueries"
	opListSchemas                                          = "ListSchemas"
	opListTagsForResource                                  = "ListTagsForResource"
	opPopulateIDMappingTable                               = "PopulateIdMappingTable"
	opPreviewPrivacyImpact                                 = "PreviewPrivacyImpact"
	opStartProtectedJob                                    = "StartProtectedJob"
	opStartProtectedQuery                                  = "StartProtectedQuery"
	opTagResource                                          = "TagResource"
	opUntagResource                                        = "UntagResource"
	opUpdateAnalysisTemplate                               = "UpdateAnalysisTemplate"
	opUpdateCollaboration                                  = "UpdateCollaboration"
	opUpdateCollaborationChangeRequest                     = "UpdateCollaborationChangeRequest"
	opUpdateConfiguredAudienceModelAssociation             = "UpdateConfiguredAudienceModelAssociation"
	opUpdateConfiguredTable                                = "UpdateConfiguredTable"
	opUpdateConfiguredTableAnalysisRule                    = "UpdateConfiguredTableAnalysisRule"
	opUpdateConfiguredTableAssociation                     = "UpdateConfiguredTableAssociation"
	opUpdateConfiguredTableAssociationAnalysisRule         = "UpdateConfiguredTableAssociationAnalysisRule"
	opUpdateIDMappingTable                                 = "UpdateIdMappingTable"
	opUpdateIDNamespaceAssociation                         = "UpdateIdNamespaceAssociation"
	opUpdateMembership                                     = "UpdateMembership"
	opUpdatePrivacyBudgetTemplate                          = "UpdatePrivacyBudgetTemplate"
	opUpdateProtectedJob                                   = "UpdateProtectedJob"
	opUpdateProtectedQuery                                 = "UpdateProtectedQuery"
	opUnknown                                              = ""
)

var errUnknownAction = errors.New("unknown action")

// Handler handles AWS Clean Rooms HTTP requests.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Clean Rooms handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

func (h *Handler) Name() string                        { return "CleanRooms" }
func (h *Handler) Reset()                              { h.Backend.Reset() }
func (h *Handler) StartWorker(_ context.Context) error { return nil }

func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchGetCollaborationAnalysisTemplate,
		opBatchGetSchema,
		opBatchGetSchemaAnalysisRule,
		opCreateAnalysisTemplate,
		opCreateCollaboration,
		opCreateCollaborationChangeRequest,
		opCreateConfiguredAudienceModelAssociation,
		opCreateConfiguredTable,
		opCreateConfiguredTableAnalysisRule,
		opCreateConfiguredTableAssociation,
		opCreateConfiguredTableAssociationAnalysisRule,
		opCreateIDMappingTable,
		opCreateIDNamespaceAssociation,
		opCreateMembership,
		opCreatePrivacyBudgetTemplate,
		opDeleteAnalysisTemplate,
		opDeleteCollaboration,
		opDeleteConfiguredAudienceModelAssociation,
		opDeleteConfiguredTable,
		opDeleteConfiguredTableAnalysisRule,
		opDeleteConfiguredTableAssociation,
		opDeleteConfiguredTableAssociationAnalysisRule,
		opDeleteIDMappingTable,
		opDeleteIDNamespaceAssociation,
		opDeleteMember,
		opDeleteMembership,
		opDeletePrivacyBudgetTemplate,
		opGetAnalysisTemplate,
		opGetCollaboration,
		opGetCollaborationAnalysisTemplate,
		opGetCollaborationChangeRequest,
		opGetCollaborationConfiguredAudienceModelAssociation,
		opGetCollaborationIDNamespaceAssociation,
		opGetCollaborationPrivacyBudgetTemplate,
		opGetConfiguredAudienceModelAssociation,
		opGetConfiguredTable,
		opGetConfiguredTableAnalysisRule,
		opGetConfiguredTableAssociation,
		opGetConfiguredTableAssociationAnalysisRule,
		opGetIDMappingTable,
		opGetIDNamespaceAssociation,
		opGetMembership,
		opGetPrivacyBudgetTemplate,
		opGetProtectedJob,
		opGetProtectedQuery,
		opGetSchema,
		opGetSchemaAnalysisRule,
		opListAnalysisTemplates,
		opListCollaborationAnalysisTemplates,
		opListCollaborationChangeRequests,
		opListCollaborationConfiguredAudienceModelAssociations,
		opListCollaborationIDNamespaceAssociations,
		opListCollaborationPrivacyBudgets,
		opListCollaborationPrivacyBudgetTemplates,
		opListCollaborations,
		opListConfiguredAudienceModelAssociations,
		opListConfiguredTableAssociations,
		opListConfiguredTables,
		opListIDMappingTables,
		opListIDNamespaceAssociations,
		opListMembers,
		opListMemberships,
		opListPrivacyBudgets,
		opListPrivacyBudgetTemplates,
		opListProtectedJobs,
		opListProtectedQueries,
		opListSchemas,
		opListTagsForResource,
		opPopulateIDMappingTable,
		opPreviewPrivacyImpact,
		opStartProtectedJob,
		opStartProtectedQuery,
		opTagResource,
		opUntagResource,
		opUpdateAnalysisTemplate,
		opUpdateCollaboration,
		opUpdateCollaborationChangeRequest,
		opUpdateConfiguredAudienceModelAssociation,
		opUpdateConfiguredTable,
		opUpdateConfiguredTableAnalysisRule,
		opUpdateConfiguredTableAssociation,
		opUpdateConfiguredTableAssociationAnalysisRule,
		opUpdateIDMappingTable,
		opUpdateIDNamespaceAssociation,
		opUpdateMembership,
		opUpdatePrivacyBudgetTemplate,
		opUpdateProtectedJob,
		opUpdateProtectedQuery,
	}
}

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		host := c.Request().Host
		path := c.Request().URL.Path

		return strings.HasPrefix(host, cleanroomsHostPrefix) ||
			strings.HasPrefix(path, "/collaborations") ||
			strings.HasPrefix(path, "/configuredTables") ||
			strings.HasPrefix(path, "/memberships") ||
			strings.HasPrefix(path, "/tags/")
	}
}

func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)
		if op == opUnknown {
			return c.String(http.StatusNotFound, "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "cleanrooms: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		// Inject path parameters into body for handlers.
		body = injectPathParams(c.Request().URL.Path, op, body)

		result, dispErr := h.dispatch(ctx, op, body, c)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}
		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	type errResp struct {
		Message string `json:"message"`
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp{err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp{err.Error()})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errResp{err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, errResp{err.Error()})
	}
}

// classifyPath maps (method, path) to an operation name and primary resource.
func classifyPath(method, path string) (string, string) {
	// Trim leading slash and split
	path = strings.TrimPrefix(path, "/")
	segs := strings.Split(path, "/")
	if len(segs) == 0 {
		return opUnknown, ""
	}

	root := segs[0]

	switch root {
	case "collaborations":
		return classifyCollaborations(method, segs)
	case "configuredTables":
		return classifyConfiguredTables(method, segs)
	case "memberships":
		return classifyMemberships(method, segs)
	case "tags":
		return classifyTags(method, segs)
	}

	return opUnknown, ""
}

func classifyCollaborations(method string, segs []string) (string, string) {
	// /collaborations
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateCollaboration, ""
		case http.MethodGet:
			return opListCollaborations, ""
		}
	}
	// /collaborations/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetCollaboration, id
		case http.MethodDelete:
			return opDeleteCollaboration, id
		case http.MethodPatch:
			return opUpdateCollaboration, id
		}
	}
	// /collaborations/{id}/{sub}[/...]
	if len(segs) >= segsWithSub {
		id := segs[1]
		sub := segs[2]

		return classifyCollaboration(method, id, sub, segs)
	}

	return opUnknown, ""
}

// classifyCollaboration handles sub-resource routing for /collaborations/{id}/{sub}[/...].
func classifyCollaboration(method, id, sub string, segs []string) (string, string) {
	switch sub {
	case subAnalysisTemplates:
		return classifyCollabAnalysisTemplates(method, id, segs)
	case "batch-analysistemplates", "batch-schema", "batch-schema-analysis-rule":
		return classifyCollabBatchPost(method, id, sub)
	case "changeRequests":
		return classifyCollabChangeRequests(method, id, segs)
	case subCAMAAssociations:
		return classifyCollabCAMAAssocs(method, id, segs)
	case subIDNamespaceAssocs:
		return classifyCollabIDNamespaceAssocs(method, id, segs)
	case "member":
		return classifyCollabMember(method, id, segs)
	case "members":
		if method == http.MethodGet {
			return opListMembers, id
		}
	case subPrivacyBudgetTmpls:
		return classifyCollabPrivacyBudgetTmpls(method, id, segs)
	case "privacybudgets":
		if method == http.MethodGet {
			return opListCollaborationPrivacyBudgets, id
		}
	case subSchemas:
		return classifyCollabSchemas(method, id, segs)
	}

	return opUnknown, ""
}

func classifyCollabBatchPost(method, id, sub string) (string, string) {
	if method != http.MethodPost {
		return opUnknown, ""
	}
	switch sub {
	case "batch-analysistemplates":
		return opBatchGetCollaborationAnalysisTemplate, id
	case "batch-schema":
		return opBatchGetSchema, id
	case "batch-schema-analysis-rule":
		return opBatchGetSchemaAnalysisRule, id
	}

	return opUnknown, ""
}

func classifyCollabMember(method, id string, segs []string) (string, string) {
	// /collaborations/{id}/member/{accountId}
	if len(segs) == segsWithSubID && method == http.MethodDelete {
		return opDeleteMember, id
	}

	return opUnknown, ""
}

func classifyCollabAnalysisTemplates(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationAnalysisTemplates, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationAnalysisTemplate, id
	}

	return opUnknown, ""
}

func classifyCollabChangeRequests(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateCollaborationChangeRequest, id
		case http.MethodGet:
			return opListCollaborationChangeRequests, id
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetCollaborationChangeRequest, id
		case http.MethodPatch:
			return opUpdateCollaborationChangeRequest, id
		}
	}

	return opUnknown, ""
}

func classifyCollabCAMAAssocs(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationConfiguredAudienceModelAssociations, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationConfiguredAudienceModelAssociation, id
	}

	return opUnknown, ""
}

func classifyCollabIDNamespaceAssocs(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationIDNamespaceAssociations, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationIDNamespaceAssociation, id
	}

	return opUnknown, ""
}

func classifyCollabPrivacyBudgetTmpls(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationPrivacyBudgetTemplates, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationPrivacyBudgetTemplate, id
	}

	return opUnknown, ""
}

func classifyCollabSchemas(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListSchemas, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetSchema, id
	}
	// /collaborations/{id}/schemas/{name}/analysisRule/{type}
	if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule && method == http.MethodGet {
		return opGetSchemaAnalysisRule, id
	}

	return opUnknown, ""
}

func classifyConfiguredTables(method string, segs []string) (string, string) {
	// /configuredTables
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredTable, ""
		case http.MethodGet:
			return opListConfiguredTables, ""
		}
	}
	// /configuredTables/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetConfiguredTable, id
		case http.MethodDelete:
			return opDeleteConfiguredTable, id
		case http.MethodPatch:
			return opUpdateConfiguredTable, id
		}
	}
	// /configuredTables/{id}/analysisRule[/{type}]
	if len(segs) >= segsWithSub && segs[2] == subAnalysisRule {
		return classifyConfiguredTableAnalysisRule(method, segs)
	}

	return opUnknown, ""
}

func classifyConfiguredTableAnalysisRule(method string, segs []string) (string, string) {
	id := segs[1]
	if len(segs) == segsWithSub && method == http.MethodPost {
		return opCreateConfiguredTableAnalysisRule, id
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAnalysisRule, id
		case http.MethodDelete:
			return opDeleteConfiguredTableAnalysisRule, id
		case http.MethodPatch:
			return opUpdateConfiguredTableAnalysisRule, id
		}
	}

	return opUnknown, ""
}

func classifyMemberships(method string, segs []string) (string, string) {
	// /memberships
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateMembership, ""
		case http.MethodGet:
			return opListMemberships, ""
		}
	}
	// /memberships/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetMembership, id
		case http.MethodDelete:
			return opDeleteMembership, id
		case http.MethodPatch:
			return opUpdateMembership, id
		}
	}
	if len(segs) < segsWithSub {
		return opUnknown, ""
	}
	membershipID := segs[1]
	sub := segs[2]

	return classifyMembership(method, membershipID, sub, segs)
}

// classifyMembership handles sub-resource routing for /memberships/{id}/{sub}[/...].
func classifyMembership(method, membershipID, sub string, segs []string) (string, string) {
	switch sub {
	case subAnalysisTemplates:
		return classifyMemAnalysisTemplates(method, membershipID, segs)
	case "configuredTableAssociations":
		return classifyMemCTAssociations(method, membershipID, segs)
	case subCAMAAssociations:
		return classifyMemCAMAAssocs(method, membershipID, segs)
	case "idmappingtables":
		return classifyMemIDMappingTables(method, membershipID, segs)
	case subIDNamespaceAssocs:
		return classifyMemIDNamespaceAssocs(method, membershipID, segs)
	case "previewprivacyimpact":
		if method == http.MethodPost {
			return opPreviewPrivacyImpact, membershipID
		}
	case "privacybudgets":
		if method == http.MethodGet {
			return opListPrivacyBudgets, membershipID
		}
	case subPrivacyBudgetTmpls:
		return classifyMemPrivacyBudgetTmpls(method, membershipID, segs)
	case subProtectedJobs:
		return classifyMemProtectedJobs(method, membershipID, segs)
	case subProtectedQueries:
		return classifyMemProtectedQueries(method, membershipID, segs)
	}

	return opUnknown, ""
}

func classifyMemAnalysisTemplates(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateAnalysisTemplate, membershipID
		case http.MethodGet:
			return opListAnalysisTemplates, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetAnalysisTemplate, membershipID
		case http.MethodDelete:
			return opDeleteAnalysisTemplate, membershipID
		case http.MethodPatch:
			return opUpdateAnalysisTemplate, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemCTAssociations(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredTableAssociation, membershipID
		case http.MethodGet:
			return opListConfiguredTableAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAssociation, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredTableAssociation, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredTableAssociation, membershipID
		}
	}
	if len(segs) >= segsWithSubSub && segs[4] == subAnalysisRule {
		return classifyMemCTAssocAnalysisRule(method, membershipID, segs)
	}

	return opUnknown, ""
}

func classifyMemCTAssocAnalysisRule(method, membershipID string, segs []string) (string, string) {
	// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule
	if len(segs) == segsWithSubSub && method == http.MethodPost {
		return opCreateConfiguredTableAssociationAnalysisRule, membershipID
	}
	// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule/{type}
	if len(segs) == segsWithSubSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAssociationAnalysisRule, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredTableAssociationAnalysisRule, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredTableAssociationAnalysisRule, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemCAMAAssocs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredAudienceModelAssociation, membershipID
		case http.MethodGet:
			return opListConfiguredAudienceModelAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredAudienceModelAssociation, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredAudienceModelAssociation, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredAudienceModelAssociation, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemIDMappingTables(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateIDMappingTable, membershipID
		case http.MethodGet:
			return opListIDMappingTables, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetIDMappingTable, membershipID
		case http.MethodDelete:
			return opDeleteIDMappingTable, membershipID
		case http.MethodPatch:
			return opUpdateIDMappingTable, membershipID
		}
	}
	// /memberships/{id}/idmappingtables/{tableId}/populate
	if len(segs) == segsWithSubSub && segs[4] == "populate" && method == http.MethodPost {
		return opPopulateIDMappingTable, membershipID
	}

	return opUnknown, ""
}

func classifyMemIDNamespaceAssocs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateIDNamespaceAssociation, membershipID
		case http.MethodGet:
			return opListIDNamespaceAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetIDNamespaceAssociation, membershipID
		case http.MethodDelete:
			return opDeleteIDNamespaceAssociation, membershipID
		case http.MethodPatch:
			return opUpdateIDNamespaceAssociation, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemPrivacyBudgetTmpls(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreatePrivacyBudgetTemplate, membershipID
		case http.MethodGet:
			return opListPrivacyBudgetTemplates, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetPrivacyBudgetTemplate, membershipID
		case http.MethodDelete:
			return opDeletePrivacyBudgetTemplate, membershipID
		case http.MethodPatch:
			return opUpdatePrivacyBudgetTemplate, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemProtectedJobs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opStartProtectedJob, membershipID
		case http.MethodGet:
			return opListProtectedJobs, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetProtectedJob, membershipID
		case http.MethodPatch:
			return opUpdateProtectedJob, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemProtectedQueries(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opStartProtectedQuery, membershipID
		case http.MethodGet:
			return opListProtectedQueries, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetProtectedQuery, membershipID
		case http.MethodPatch:
			return opUpdateProtectedQuery, membershipID
		}
	}

	return opUnknown, ""
}

func classifyTags(method string, segs []string) (string, string) {
	if len(segs) < segsWithID {
		return opUnknown, ""
	}
	resourceArn := strings.Join(segs[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceArn
	case http.MethodPost:
		return opTagResource, resourceArn
	case http.MethodDelete:
		return opUntagResource, resourceArn
	}

	return opUnknown, ""
}

// injectPathParams merges URL path segments into the request body JSON.
func injectPathParams(path, _ string, body []byte) []byte {
	path = strings.TrimPrefix(path, "/")
	segs := strings.Split(path, "/")

	var m map[string]json.RawMessage
	if len(body) > 0 {
		_ = json.Unmarshal(body, &m)
	}
	if m == nil {
		m = make(map[string]json.RawMessage)
	}

	setStr := func(key, val string) {
		if val != "" {
			b, _ := json.Marshal(val)
			m[key] = b
		}
	}

	switch {
	case len(segs) >= segsWithID && segs[0] == "collaborations":
		injectCollaborationParams(segs, setStr)
	case len(segs) >= segsWithID && segs[0] == "configuredTables":
		setStr("configuredTableIdentifier", segs[1])
		if len(segs) == segsWithSubID && segs[2] == subAnalysisRule {
			setStr("analysisRuleType", segs[3])
		}
	case len(segs) >= segsWithID && segs[0] == "memberships":
		injectMembershipParams(segs, setStr)
	case len(segs) >= segsWithID && segs[0] == subTags:
		arnVal := strings.Join(segs[1:], "/")
		setStr("resourceArn", arnVal)
	}

	out, _ := json.Marshal(m)

	return out
}

// injectCollaborationParams injects path parameters for /collaborations/... routes.
func injectCollaborationParams(segs []string, setStr func(string, string)) {
	setStr("collaborationIdentifier", segs[1])
	if len(segs) >= segsWithSubID {
		switch segs[2] {
		case subAnalysisTemplates:
			setStr("analysisTemplateArn", segs[3])
		case "changeRequests":
			setStr("changeRequestIdentifier", segs[3])
		case subCAMAAssociations:
			setStr("configuredAudienceModelAssociationIdentifier", segs[3])
		case subIDNamespaceAssocs:
			setStr("idNamespaceAssociationIdentifier", segs[3])
		case "member":
			setStr("accountId", segs[3])
		case subPrivacyBudgetTmpls:
			setStr("privacyBudgetTemplateIdentifier", segs[3])
		case subSchemas:
			setStr("name", segs[3])
			if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule {
				setStr("type", segs[5])
			}
		}
	}
}

// injectMembershipParams injects path parameters for /memberships/... routes.
func injectMembershipParams(segs []string, setStr func(string, string)) {
	setStr("membershipIdentifier", segs[1])
	if len(segs) >= segsWithSubID {
		switch segs[2] {
		case subAnalysisTemplates:
			setStr("analysisTemplateIdentifier", segs[3])
		case "configuredTableAssociations":
			setStr("configuredTableAssociationIdentifier", segs[3])
			if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule {
				setStr("analysisRuleType", segs[5])
			}
		case subCAMAAssociations:
			setStr("configuredAudienceModelAssociationIdentifier", segs[3])
		case "idmappingtables":
			setStr("idMappingTableIdentifier", segs[3])
		case subIDNamespaceAssocs:
			setStr("idNamespaceAssociationIdentifier", segs[3])
		case subPrivacyBudgetTmpls:
			setStr("privacyBudgetTemplateIdentifier", segs[3])
		case subProtectedJobs:
			setStr("protectedJobIdentifier", segs[3])
		case subProtectedQueries:
			setStr("protectedQueryIdentifier", segs[3])
		}
	}
}

// ---- dispatch ----

// opHandlerFn is the unified type for operation handlers.
type opHandlerFn func(ctx context.Context, body []byte, c *echo.Context) ([]byte, error)

// buildOpHandlers returns a map from operation name to handler function.
func (h *Handler) buildOpHandlers(_ *echo.Context) map[string]opHandlerFn {
	out := h.buildCollaborationHandlers()
	maps.Copy(out, h.buildMembershipHandlers())
	maps.Copy(out, h.buildConfiguredTableHandlers())
	maps.Copy(out, h.buildResourceHandlers())

	return out
}

func (h *Handler) buildCollaborationHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// Collaboration
		opCreateCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateCollaboration(ctx, body)
		},
		opGetCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaboration(ctx, body)
		},
		opListCollaborations: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborations(ctx, ec)
		},
		opUpdateCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateCollaboration(ctx, body)
		},
		opDeleteCollaboration: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteCollaboration(ctx, body)
		},
		opListMembers: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListMembers(ctx, body, ec)
		},
		opDeleteMember: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteMember(ctx, body)
		},
		// Collaboration sub-resources
		opGetCollaborationAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationAnalysisTemplate(ctx, body)
		},
		opListCollaborationAnalysisTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationAnalysisTemplates(ctx, body, ec)
		},
		opBatchGetCollaborationAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetCollaborationAnalysisTemplate(ctx, body)
		},
		opBatchGetSchema: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetSchema(ctx, body)
		},
		opBatchGetSchemaAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleBatchGetSchemaAnalysisRule(ctx, body)
		},
		opGetSchema: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetSchema(ctx, body)
		},
		opListSchemas: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListSchemas(ctx, body, ec)
		},
		opGetSchemaAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetSchemaAnalysisRule(ctx, body)
		},
		opCreateCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateCollaborationChangeRequest(ctx, body)
		},
		opGetCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationChangeRequest(ctx, body)
		},
		opListCollaborationChangeRequests: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationChangeRequests(ctx, body, ec)
		},
		opUpdateCollaborationChangeRequest: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateCollaborationChangeRequest(ctx, body)
		},
		opGetCollaborationConfiguredAudienceModelAssociation: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleGetCollaborationConfiguredAudienceModelAssociation(ctx, body)
		},
		opListCollaborationConfiguredAudienceModelAssociations: func(
			ctx context.Context, body []byte, ec *echo.Context,
		) ([]byte, error) {
			return h.handleListCollaborationConfiguredAudienceModelAssociations(ctx, body, ec)
		},
		opGetCollaborationIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationIDNamespaceAssociation(ctx, body)
		},
		opListCollaborationIDNamespaceAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationIDNamespaceAssociations(ctx, body, ec)
		},
		opGetCollaborationPrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetCollaborationPrivacyBudgetTemplate(ctx, body)
		},
		opListCollaborationPrivacyBudgetTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationPrivacyBudgetTemplates(ctx, body, ec)
		},
		opListCollaborationPrivacyBudgets: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListCollaborationPrivacyBudgets(ctx, body, ec)
		},
	}
}

func (h *Handler) buildMembershipHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateMembership(ctx, body)
		},
		opGetMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetMembership(ctx, body)
		},
		opListMemberships: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListMemberships(ctx, ec)
		},
		opUpdateMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateMembership(ctx, body)
		},
		opDeleteMembership: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteMembership(ctx, body)
		},
		opCreateAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateAnalysisTemplate(ctx, body)
		},
		opGetAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetAnalysisTemplate(ctx, body)
		},
		opListAnalysisTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListAnalysisTemplates(ctx, body, ec)
		},
		opUpdateAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateAnalysisTemplate(ctx, body)
		},
		opDeleteAnalysisTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteAnalysisTemplate(ctx, body)
		},
		opStartProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleStartProtectedQuery(ctx, body)
		},
		opGetProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetProtectedQuery(ctx, body)
		},
		opListProtectedQueries: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListProtectedQueries(ctx, body, ec)
		},
		opUpdateProtectedQuery: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateProtectedQuery(ctx, body)
		},
		opStartProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleStartProtectedJob(ctx, body)
		},
		opGetProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetProtectedJob(ctx, body)
		},
		opListProtectedJobs: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListProtectedJobs(ctx, body, ec)
		},
		opUpdateProtectedJob: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateProtectedJob(ctx, body)
		},
	}
}

func (h *Handler) buildConfiguredTableHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		opCreateConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTable(ctx, body)
		},
		opGetConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTable(ctx, body)
		},
		opListConfiguredTables: func(ctx context.Context, _ []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredTables(ctx, ec)
		},
		opUpdateConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTable(ctx, body)
		},
		opDeleteConfiguredTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTable(ctx, body)
		},
		opCreateConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTableAnalysisRule(ctx, body)
		},
		opGetConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAnalysisRule(ctx, body)
		},
		opUpdateConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTableAnalysisRule(ctx, body)
		},
		opDeleteConfiguredTableAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTableAnalysisRule(ctx, body)
		},
		opCreateConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredTableAssociation(ctx, body)
		},
		opGetConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAssociation(ctx, body)
		},
		opListConfiguredTableAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredTableAssociations(ctx, body, ec)
		},
		opUpdateConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredTableAssociation(ctx, body)
		},
		opDeleteConfiguredTableAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredTableAssociation(ctx, body)
		},
		opCreateConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleCreateConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opGetConfiguredTableAssociationAnalysisRule: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opUpdateConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleUpdateConfiguredTableAssociationAnalysisRule(ctx, body)
		},
		opDeleteConfiguredTableAssociationAnalysisRule: func(
			ctx context.Context, body []byte, _ *echo.Context,
		) ([]byte, error) {
			return h.handleDeleteConfiguredTableAssociationAnalysisRule(ctx, body)
		},
	}
}

func (h *Handler) buildResourceHandlers() map[string]opHandlerFn {
	return map[string]opHandlerFn{
		// IDMappingTable
		opCreateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIDMappingTable(ctx, body)
		},
		opGetIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIDMappingTable(ctx, body)
		},
		opListIDMappingTables: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIDMappingTables(ctx, body, ec)
		},
		opUpdateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIDMappingTable(ctx, body)
		},
		opDeleteIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIDMappingTable(ctx, body)
		},
		opPopulateIDMappingTable: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handlePopulateIDMappingTable(ctx, body)
		},
		// IDNamespaceAssociation
		opCreateIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateIDNamespaceAssociation(ctx, body)
		},
		opGetIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetIDNamespaceAssociation(ctx, body)
		},
		opListIDNamespaceAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListIDNamespaceAssociations(ctx, body, ec)
		},
		opUpdateIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateIDNamespaceAssociation(ctx, body)
		},
		opDeleteIDNamespaceAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteIDNamespaceAssociation(ctx, body)
		},
		// ConfiguredAudienceModelAssociation
		opCreateConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreateConfiguredAudienceModelAssociation(ctx, body)
		},
		opGetConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetConfiguredAudienceModelAssociation(ctx, body)
		},
		opListConfiguredAudienceModelAssociations: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListConfiguredAudienceModelAssociations(ctx, body, ec)
		},
		opUpdateConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdateConfiguredAudienceModelAssociation(ctx, body)
		},
		opDeleteConfiguredAudienceModelAssociation: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeleteConfiguredAudienceModelAssociation(ctx, body)
		},
		// PrivacyBudget
		opCreatePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleCreatePrivacyBudgetTemplate(ctx, body)
		},
		opGetPrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleGetPrivacyBudgetTemplate(ctx, body)
		},
		opListPrivacyBudgetTemplates: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListPrivacyBudgetTemplates(ctx, body, ec)
		},
		opUpdatePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleUpdatePrivacyBudgetTemplate(ctx, body)
		},
		opDeletePrivacyBudgetTemplate: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleDeletePrivacyBudgetTemplate(ctx, body)
		},
		opListPrivacyBudgets: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleListPrivacyBudgets(ctx, body, ec)
		},
		opPreviewPrivacyImpact: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handlePreviewPrivacyImpact(ctx, body)
		},
		// Tags
		opListTagsForResource: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleListTagsForResource(ctx, body)
		},
		opTagResource: func(ctx context.Context, body []byte, _ *echo.Context) ([]byte, error) {
			return h.handleTagResource(ctx, body)
		},
		opUntagResource: func(ctx context.Context, body []byte, ec *echo.Context) ([]byte, error) {
			return h.handleUntagResource(ctx, body, ec)
		},
	}
}

func (h *Handler) dispatch(
	ctx context.Context,
	op string,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	handlers := h.buildOpHandlers(c)
	if fn, ok := handlers[op]; ok {
		return fn(ctx, body, c)
	}

	return nil, errUnknownAction
}

// ---- handler helpers ----

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)

	return b
}

func qp(c *echo.Context, key string) string {
	return c.QueryParam(key)
}

// ---- Collaboration handlers ----

func (h *Handler) handleCreateCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                   map[string]string `json:"tags"`
		Name                   string            `json:"name"`
		Description            string            `json:"description"`
		CreatorDisplayName     string            `json:"creatorDisplayName"`
		QueryLogStatus         string            `json:"queryLogStatus"`
		CreatorMemberAbilities []string          `json:"creatorMemberAbilities"`
		Members                []MemberSpec      `json:"members"`
	}
	_ = json.Unmarshal(body, &req)
	c, err := h.Backend.CreateCollaboration(
		req.Name,
		req.Description,
		req.CreatorDisplayName,
		req.CreatorMemberAbilities,
		req.Members,
		req.QueryLogStatus,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: c}), nil
}

func (h *Handler) handleGetCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	c, err := h.Backend.GetCollaboration(req.CollaborationIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: c}), nil
}

func (h *Handler) handleListCollaborations(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListCollaborations(
		qp(c, "memberStatus"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	resp := map[string]any{"collaborationList": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
		Description             string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	col, err := h.Backend.UpdateCollaboration(
		req.CollaborationIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaboration: col}), nil
}

func (h *Handler) handleDeleteCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteCollaboration(req.CollaborationIdentifier)
}

func (h *Handler) handleListMembers(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListMembers(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"memberList": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleDeleteMember(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		AccountID               string `json:"accountId"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteMember(req.CollaborationIdentifier, req.AccountID)
}

func (h *Handler) handleGetCollaborationAnalysisTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		AnalysisTemplateArn     string `json:"analysisTemplateArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationAnalysisTemplate(
		req.CollaborationIdentifier,
		req.AnalysisTemplateArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleListCollaborationAnalysisTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationAnalysisTemplates(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"analysisTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleBatchGetCollaborationAnalysisTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		AnalysisTemplateArns    []string `json:"analysisTemplateArns"`
	}
	_ = json.Unmarshal(body, &req)
	items, errs, err := h.Backend.BatchGetCollaborationAnalysisTemplate(
		req.CollaborationIdentifier,
		req.AnalysisTemplateArns,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"analysisTemplates": items, keyErrors: errs}), nil
}

func (h *Handler) handleBatchGetSchema(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		Names                   []string `json:"names"`
	}
	_ = json.Unmarshal(body, &req)
	items, errs, err := h.Backend.BatchGetSchema(req.CollaborationIdentifier, req.Names)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"schemas": items, keyErrors: errs}), nil
}

func (h *Handler) handleBatchGetSchemaAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier    string `json:"collaborationIdentifier"`
		SchemaAnalysisRuleRequests []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"schemaAnalysisRuleRequests"`
	}
	_ = json.Unmarshal(body, &req)
	names := make([]string, 0, len(req.SchemaAnalysisRuleRequests))
	var ruleType string
	for _, r := range req.SchemaAnalysisRuleRequests {
		names = append(names, r.Name)
		if ruleType == "" {
			ruleType = r.Type
		}
	}
	items, errs, err := h.Backend.BatchGetSchemaAnalysisRule(
		req.CollaborationIdentifier,
		names,
		ruleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"analysisRules": items, keyErrors: errs}), nil
}

func (h *Handler) handleGetSchema(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
	}
	_ = json.Unmarshal(body, &req)
	s, err := h.Backend.GetSchema(req.CollaborationIdentifier, req.Name)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"schema": s}), nil
}

func (h *Handler) handleListSchemas(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListSchemas(
		req.CollaborationIdentifier,
		qp(c, "schemaType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"schemaSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleGetSchemaAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		Name                    string `json:"name"`
		Type                    string `json:"type"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetSchemaAnalysisRule(req.CollaborationIdentifier, req.Name, req.Type)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleCreateCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Details                 map[string]any `json:"details"`
		CollaborationIdentifier string         `json:"collaborationIdentifier"`
		Type                    string         `json:"type"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.Type,
		req.Details,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) handleGetCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.ChangeRequestIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) handleListCollaborationChangeRequests(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationChangeRequests(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collaborationChangeRequests": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateCollaborationChangeRequest(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
		Status                  string `json:"status"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateCollaborationChangeRequest(
		req.CollaborationIdentifier,
		req.ChangeRequestIdentifier,
		req.Status,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCollaborationChangeRequest: r}), nil
}

func (h *Handler) handleGetCollaborationConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier                      string `json:"collaborationIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationConfiguredAudienceModelAssociation(
		req.CollaborationIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleListCollaborationConfiguredAudienceModelAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationConfiguredAudienceModelAssociations(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleGetCollaborationIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier          string `json:"collaborationIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationIDNamespaceAssociation(
		req.CollaborationIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleListCollaborationIDNamespaceAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationIDNamespaceAssociations(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleGetCollaborationPrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier         string `json:"collaborationIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationPrivacyBudgetTemplate(
		req.CollaborationIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgetTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgetTemplates(
		req.CollaborationIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgets(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgets(
		req.CollaborationIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

// ---- Membership handlers ----

func (h *Handler) handleCreateMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DefaultResultConfiguration map[string]any    `json:"defaultResultConfiguration"`
		PaymentConfiguration       map[string]any    `json:"paymentConfiguration"`
		Tags                       map[string]string `json:"tags"`
		CollaborationIdentifier    string            `json:"collaborationIdentifier"`
		QueryLogStatus             string            `json:"queryLogStatus"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.CreateMembership(
		req.CollaborationIdentifier,
		req.QueryLogStatus,
		req.DefaultResultConfiguration,
		req.PaymentConfiguration,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleGetMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.GetMembership(req.MembershipIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleListMemberships(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListMemberships(
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	resp := map[string]any{"membershipSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		DefaultResultConfiguration map[string]any `json:"defaultResultConfiguration"`
		MembershipIdentifier       string         `json:"membershipIdentifier"`
		QueryLogStatus             string         `json:"queryLogStatus"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.UpdateMembership(
		req.MembershipIdentifier,
		req.QueryLogStatus,
		req.DefaultResultConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyMembership: m}), nil
}

func (h *Handler) handleDeleteMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteMembership(req.MembershipIdentifier)
}

// ---- ConfiguredTable handlers ----

func (h *Handler) handleCreateConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TableReference map[string]any    `json:"tableReference"`
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		AnalysisMethod string            `json:"analysisMethod"`
		AllowedColumns []string          `json:"allowedColumns"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.CreateConfiguredTable(
		req.Name,
		req.Description,
		req.TableReference,
		req.AllowedColumns,
		req.AnalysisMethod,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleGetConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.GetConfiguredTable(req.ConfiguredTableIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleListConfiguredTables(
	_ context.Context,
	c *echo.Context,
) ([]byte, error) {
	items, next := h.Backend.ListConfiguredTables(qp(c, "maxResults"), qp(c, "nextToken"))
	resp := map[string]any{"configuredTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		Name                      string `json:"name"`
		Description               string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.UpdateConfiguredTable(
		req.ConfiguredTableIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTable: ct}), nil
}

func (h *Handler) handleDeleteConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTable(req.ConfiguredTableIdentifier)
}

// ---- ConfiguredTableAnalysisRule handlers ----

func (h *Handler) handleCreateConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleGetConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAnalysisRule(
		req.ConfiguredTableIdentifier,
		req.AnalysisRuleType,
	)
}

// ---- ConfiguredTableAssociation handlers ----

func (h *Handler) handleCreateConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Tags                      map[string]string `json:"tags"`
		MembershipIdentifier      string            `json:"membershipIdentifier"`
		Name                      string            `json:"name"`
		Description               string            `json:"description"`
		ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
		RoleArn                   string            `json:"roleArn"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.ConfiguredTableIdentifier,
		req.RoleArn,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleGetConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleListConfiguredTableAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredTableAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredTableAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		Description                          string `json:"description"`
		RoleArn                              string `json:"roleArn"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.Description,
		req.RoleArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyConfiguredTableAssociation: a}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAssociation(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
	)
}

// ---- ConfiguredTableAssociationAnalysisRule handlers ----

func (h *Handler) handleCreateConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleGetConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
		req.AnalysisRulePolicy,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{subAnalysisRule: r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociationAnalysisRule(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredTableAssociationAnalysisRule(
		req.MembershipIdentifier,
		req.ConfiguredTableAssociationIdentifier,
		req.AnalysisRuleType,
	)
}

// ---- AnalysisTemplate handlers ----

func (h *Handler) handleCreateAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Source               map[string]any    `json:"source"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		Format               string            `json:"format"`
		AnalysisParameters   []map[string]any  `json:"analysisParameters"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateAnalysisTemplate(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.Format,
		req.Source,
		req.AnalysisParameters,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleGetAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleListAnalysisTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListAnalysisTemplates(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"analysisTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
		Description                string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdateAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyAnalysisTemplate: t}), nil
}

func (h *Handler) handleDeleteAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteAnalysisTemplate(
		req.MembershipIdentifier,
		req.AnalysisTemplateIdentifier,
	)
}

// ---- ProtectedQuery handlers ----

func (h *Handler) handleStartProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		SQLParameters        map[string]any `json:"sqlParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		ComputeConfiguration map[string]any `json:"computeConfiguration"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	var sqlText string
	if req.SQLParameters != nil {
		if v, ok := req.SQLParameters["queryString"].(string); ok {
			sqlText = v
		}
	}
	q, err := h.Backend.StartProtectedQuery(
		req.MembershipIdentifier,
		sqlText,
		req.ResultConfiguration,
		req.ComputeConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

func (h *Handler) handleGetProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.GetProtectedQuery(req.MembershipIdentifier, req.ProtectedQueryIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

func (h *Handler) handleListProtectedQueries(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedQueries(
		req.MembershipIdentifier,
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"protectedQueries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
		TargetStatus             string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.UpdateProtectedQuery(
		req.MembershipIdentifier,
		req.ProtectedQueryIdentifier,
		req.TargetStatus,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedQuery: q}), nil
}

// ---- ProtectedJob handlers ----

func (h *Handler) handleStartProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobParameters        map[string]any `json:"jobParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
		Type                 string         `json:"type"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.StartProtectedJob(
		req.MembershipIdentifier,
		req.Type,
		req.JobParameters,
		req.ResultConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

func (h *Handler) handleGetProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.GetProtectedJob(req.MembershipIdentifier, req.ProtectedJobIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

func (h *Handler) handleListProtectedJobs(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedJobs(
		req.MembershipIdentifier,
		qp(c, "status"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"protectedJobs": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
		TargetStatus           string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.UpdateProtectedJob(
		req.MembershipIdentifier,
		req.ProtectedJobIdentifier,
		req.TargetStatus,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyProtectedJob: j}), nil
}

// ---- PrivacyBudgetTemplate handlers ----

func (h *Handler) handleCreatePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Parameters           map[string]any    `json:"parameters"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		PrivacyBudgetType    string            `json:"privacyBudgetType"`
		AutoRefresh          string            `json:"autoRefresh"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreatePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetType,
		req.AutoRefresh,
		req.Parameters,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleGetPrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetPrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleListPrivacyBudgetTemplates(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgetTemplates(
		req.MembershipIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdatePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Parameters                      map[string]any `json:"parameters"`
		MembershipIdentifier            string         `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
		AutoRefresh                     string         `json:"autoRefresh"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdatePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
		req.AutoRefresh,
		req.Parameters,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyPrivacyBudgetTemplate: t}), nil
}

func (h *Handler) handleDeletePrivacyBudgetTemplate(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeletePrivacyBudgetTemplate(
		req.MembershipIdentifier,
		req.PrivacyBudgetTemplateIdentifier,
	)
}

func (h *Handler) handleListPrivacyBudgets(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgets(
		req.MembershipIdentifier,
		qp(c, "privacyBudgetType"),
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handlePreviewPrivacyImpact(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Parameters           map[string]any `json:"parameters"`
		MembershipIdentifier string         `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PreviewPrivacyImpact(req.MembershipIdentifier, req.Parameters)
	if err != nil {
		return nil, err
	}

	return mustJSON(result), nil
}

// ---- IdMappingTable handlers ----

func (h *Handler) handleCreateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		KmsKeyArn            string            `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateIDMappingTable(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.InputReferenceConfig,
		req.KmsKeyArn,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleGetIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetIDMappingTable(req.MembershipIdentifier, req.IDMappingTableIdentifier)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleListIDMappingTables(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIDMappingTables(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idMappingTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
		Description              string `json:"description"`
		KmsKeyArn                string `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdateIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
		req.Description,
		req.KmsKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDMappingTable: t}), nil
}

func (h *Handler) handleDeleteIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
	)
}

func (h *Handler) handlePopulateIDMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IDMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PopulateIDMappingTable(
		req.MembershipIdentifier,
		req.IDMappingTableIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(result), nil
}

// ---- IdNamespaceAssociation handlers ----

func (h *Handler) handleCreateIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		IDMappingConfig      map[string]any    `json:"idMappingConfig"`
		Tags                 map[string]string `json:"tags"`
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.Name,
		req.Description,
		req.InputReferenceConfig,
		req.IDMappingConfig,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleGetIDNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleListIDNamespaceAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIDNamespaceAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		IDMappingConfig                  map[string]any `json:"idMappingConfig"`
		MembershipIdentifier             string         `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string         `json:"idNamespaceAssociationIdentifier"`
		Description                      string         `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
		req.Description,
		req.IDMappingConfig,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyIDNamespaceAssociation: a}), nil
}

func (h *Handler) handleDeleteIDNamespaceAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IDNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteIDNamespaceAssociation(
		req.MembershipIdentifier,
		req.IDNamespaceAssociationIdentifier,
	)
}

// ---- ConfiguredAudienceModelAssociation handlers ----

func (h *Handler) handleCreateConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		Tags                       map[string]string `json:"tags"`
		MembershipIdentifier       string            `json:"membershipIdentifier"`
		ConfiguredAudienceModelArn string            `json:"configuredAudienceModelArn"`
		Name                       string            `json:"name"`
		Description                string            `json:"description"`
		ManageResourcePolicies     bool              `json:"manageResourcePolicies"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelArn,
		req.Name,
		req.Description,
		req.ManageResourcePolicies,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleGetConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleListConfiguredAudienceModelAssociations(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredAudienceModelAssociations(
		req.MembershipIdentifier,
		qp(c, "maxResults"),
		qp(c, "nextToken"),
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
		Name                                         string `json:"name"`
		Description                                  string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
		req.Name,
		req.Description,
	)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{keyCAMAAssociation: a}), nil
}

func (h *Handler) handleDeleteConfiguredAudienceModelAssociation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.DeleteConfiguredAudienceModelAssociation(
		req.MembershipIdentifier,
		req.ConfiguredAudienceModelAssociationIdentifier,
	)
}

// ---- Tag handlers ----

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}
	_ = json.Unmarshal(body, &req)
	tags, err := h.Backend.ListTagsForResource(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return mustJSON(map[string]any{"tags": tags}), nil
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"tags"`
		ResourceArn string            `json:"resourceArn"`
	}
	_ = json.Unmarshal(body, &req)

	return nil, h.Backend.TagResource(req.ResourceArn, req.Tags)
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	body []byte,
	c *echo.Context,
) ([]byte, error) {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	_ = json.Unmarshal(body, &req)
	// tagKeys can also come from query params
	if len(req.TagKeys) == 0 {
		req.TagKeys = c.Request().URL.Query()["tagKeys"]
	}

	return nil, h.Backend.UntagResource(req.ResourceArn, req.TagKeys)
}
