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
	subIntermediateTables = "intermediateTables"
	subDisallowIT         = "disallowIntermediateTable"
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
	keyIntermediateTable          = "intermediateTable"
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
	cleanroomsHostPrefix  = "cleanrooms."
	cleanroomsServiceName = "cleanrooms"

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
	opCreateIntermediateTable                              = "CreateIntermediateTable"
	opCreateIntermediateTableAnalysisRule                  = "CreateIntermediateTableAnalysisRule"
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
	opDeleteIntermediateTable                              = "DeleteIntermediateTable"
	opDeleteIntermediateTableAnalysisRule                  = "DeleteIntermediateTableAnalysisRule"
	opDeleteMember                                         = "DeleteMember"
	opDeleteMembership                                     = "DeleteMembership"
	opDeletePrivacyBudgetTemplate                          = "DeletePrivacyBudgetTemplate"
	opDisallowIntermediateTable                            = "DisallowIntermediateTable"
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
	opGetIntermediateTable                                 = "GetIntermediateTable"
	opGetIntermediateTableAnalysisRule                     = "GetIntermediateTableAnalysisRule"
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
	opListIntermediateTableVersions                        = "ListIntermediateTableVersions"
	opListIntermediateTables                               = "ListIntermediateTables"
	opListMembers                                          = "ListMembers"
	opListMemberships                                      = "ListMemberships"
	opListPrivacyBudgets                                   = "ListPrivacyBudgets"
	opListPrivacyBudgetTemplates                           = "ListPrivacyBudgetTemplates"
	opListProtectedJobs                                    = "ListProtectedJobs"
	opListProtectedQueries                                 = "ListProtectedQueries"
	opListSchemas                                          = "ListSchemas"
	opListTagsForResource                                  = "ListTagsForResource"
	opPopulateIDMappingTable                               = "PopulateIdMappingTable"
	opPopulateIntermediateTable                            = "PopulateIntermediateTable"
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
	opUpdateIntermediateTable                              = "UpdateIntermediateTable"
	opUpdateIntermediateTableAnalysisRule                  = "UpdateIntermediateTableAnalysisRule"
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

func (h *Handler) Name() string { return "CleanRooms" }

func (h *Handler) Reset() { h.Backend.Reset() }

func (h *Handler) StartWorker(_ context.Context) error { return nil }

func (h *Handler) GetSupportedOperations() []string {
	return append(createDeleteOperations(), readWriteOperations()...)
}

// createDeleteOperations returns every Batch/Create/Delete/Disallow
// operation, factored out of GetSupportedOperations to keep it under the
// funlen line limit.
func createDeleteOperations() []string {
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
		opCreateIntermediateTable,
		opCreateIntermediateTableAnalysisRule,
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
		opDeleteIntermediateTable,
		opDeleteIntermediateTableAnalysisRule,
		opDeleteMember,
		opDeleteMembership,
		opDeletePrivacyBudgetTemplate,
		opDisallowIntermediateTable,
	}
}

// readWriteOperations returns every Get/List/Populate/Preview/Start/Tag/
// Update operation, factored out of GetSupportedOperations to keep it under
// the funlen line limit.
func readWriteOperations() []string {
	return []string{
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
		opGetIntermediateTable,
		opGetIntermediateTableAnalysisRule,
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
		opListIntermediateTableVersions,
		opListIntermediateTables,
		opListMembers,
		opListMemberships,
		opListPrivacyBudgets,
		opListPrivacyBudgetTemplates,
		opListProtectedJobs,
		opListProtectedQueries,
		opListSchemas,
		opListTagsForResource,
		opPopulateIDMappingTable,
		opPopulateIntermediateTable,
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
		opUpdateIntermediateTable,
		opUpdateIntermediateTableAnalysisRule,
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

		if strings.HasPrefix(host, cleanroomsHostPrefix) ||
			strings.HasPrefix(path, "/collaborations") ||
			strings.HasPrefix(path, "/configuredTables") ||
			strings.HasPrefix(path, "/memberships") {
			return true
		}

		return httputils.MatchesTaggedResourceARN(path, cleanroomsServiceName)
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
	var status int
	var code string
	switch {
	case errors.Is(err, ErrNotFound):
		status, code = http.StatusNotFound, "ResourceNotFoundException"
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrConflict):
		status, code = http.StatusConflict, "ConflictException"
	case errors.Is(err, ErrValidation):
		status, code = http.StatusBadRequest, "ValidationException"
	default:
		status, code = http.StatusInternalServerError, "InternalServerException"
	}

	// restJson1 clients (aws-sdk-go-v2 / Terraform provider) classify the error
	// by the x-amzn-errortype header or the "__type" body field. The Terraform
	// delete-waiter polls GetCollaboration after delete and must recognize
	// ResourceNotFoundException; emit both so the error is classified correctly.
	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, map[string]string{"__type": code, "message": err.Error()})
}

// opHandlerFn is the unified type for operation handlers.
type opHandlerFn func(ctx context.Context, body []byte, c *echo.Context) ([]byte, error)

// buildOpHandlers returns a map from operation name to handler function.
func (h *Handler) buildOpHandlers(_ *echo.Context) map[string]opHandlerFn {
	out := h.buildCollaborationHandlers()
	maps.Copy(out, h.buildMembershipHandlers())
	maps.Copy(out, h.buildConfiguredTableHandlers())
	maps.Copy(out, h.buildConfiguredTableAssociationHandlers())
	maps.Copy(out, h.buildAnalysisTemplateHandlers())
	maps.Copy(out, h.buildSchemaHandlers())
	maps.Copy(out, h.buildProtectedComputeHandlers())
	maps.Copy(out, h.buildPrivacyBudgetHandlers())
	maps.Copy(out, h.buildIDMappingTableHandlers())
	maps.Copy(out, h.buildCAMAAndIDNamespaceHandlers())
	maps.Copy(out, h.buildIntermediateTableHandlers())
	maps.Copy(out, h.buildTagHandlers())

	return out
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

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)

	return b
}

func qp(c *echo.Context, key string) string {
	return c.QueryParam(key)
}
