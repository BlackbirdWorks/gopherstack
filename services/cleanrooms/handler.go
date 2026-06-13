package cleanrooms

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	cleanroomsHostPrefix = "cleanrooms."

	opBatchGetCollaborationAnalysisTemplate         = "BatchGetCollaborationAnalysisTemplate"
	opBatchGetSchema                                = "BatchGetSchema"
	opBatchGetSchemaAnalysisRule                    = "BatchGetSchemaAnalysisRule"
	opCreateAnalysisTemplate                        = "CreateAnalysisTemplate"
	opCreateCollaboration                           = "CreateCollaboration"
	opCreateCollaborationChangeRequest              = "CreateCollaborationChangeRequest"
	opCreateConfiguredAudienceModelAssociation      = "CreateConfiguredAudienceModelAssociation"
	opCreateConfiguredTable                         = "CreateConfiguredTable"
	opCreateConfiguredTableAnalysisRule             = "CreateConfiguredTableAnalysisRule"
	opCreateConfiguredTableAssociation              = "CreateConfiguredTableAssociation"
	opCreateConfiguredTableAssociationAnalysisRule  = "CreateConfiguredTableAssociationAnalysisRule"
	opCreateIdMappingTable                          = "CreateIdMappingTable"
	opCreateIdNamespaceAssociation                  = "CreateIdNamespaceAssociation"
	opCreateMembership                              = "CreateMembership"
	opCreatePrivacyBudgetTemplate                   = "CreatePrivacyBudgetTemplate"
	opDeleteAnalysisTemplate                        = "DeleteAnalysisTemplate"
	opDeleteCollaboration                           = "DeleteCollaboration"
	opDeleteConfiguredAudienceModelAssociation      = "DeleteConfiguredAudienceModelAssociation"
	opDeleteConfiguredTable                         = "DeleteConfiguredTable"
	opDeleteConfiguredTableAnalysisRule             = "DeleteConfiguredTableAnalysisRule"
	opDeleteConfiguredTableAssociation              = "DeleteConfiguredTableAssociation"
	opDeleteConfiguredTableAssociationAnalysisRule  = "DeleteConfiguredTableAssociationAnalysisRule"
	opDeleteIdMappingTable                          = "DeleteIdMappingTable"
	opDeleteIdNamespaceAssociation                  = "DeleteIdNamespaceAssociation"
	opDeleteMember                                  = "DeleteMember"
	opDeleteMembership                              = "DeleteMembership"
	opDeletePrivacyBudgetTemplate                   = "DeletePrivacyBudgetTemplate"
	opGetAnalysisTemplate                           = "GetAnalysisTemplate"
	opGetCollaboration                              = "GetCollaboration"
	opGetCollaborationAnalysisTemplate              = "GetCollaborationAnalysisTemplate"
	opGetCollaborationChangeRequest                 = "GetCollaborationChangeRequest"
	opGetCollaborationConfiguredAudienceModelAssociation = "GetCollaborationConfiguredAudienceModelAssociation"
	opGetCollaborationIdNamespaceAssociation        = "GetCollaborationIdNamespaceAssociation"
	opGetCollaborationPrivacyBudgetTemplate         = "GetCollaborationPrivacyBudgetTemplate"
	opGetConfiguredAudienceModelAssociation         = "GetConfiguredAudienceModelAssociation"
	opGetConfiguredTable                            = "GetConfiguredTable"
	opGetConfiguredTableAnalysisRule                = "GetConfiguredTableAnalysisRule"
	opGetConfiguredTableAssociation                 = "GetConfiguredTableAssociation"
	opGetConfiguredTableAssociationAnalysisRule     = "GetConfiguredTableAssociationAnalysisRule"
	opGetIdMappingTable                             = "GetIdMappingTable"
	opGetIdNamespaceAssociation                     = "GetIdNamespaceAssociation"
	opGetMembership                                 = "GetMembership"
	opGetPrivacyBudgetTemplate                      = "GetPrivacyBudgetTemplate"
	opGetProtectedJob                               = "GetProtectedJob"
	opGetProtectedQuery                             = "GetProtectedQuery"
	opGetSchema                                     = "GetSchema"
	opGetSchemaAnalysisRule                         = "GetSchemaAnalysisRule"
	opListAnalysisTemplates                         = "ListAnalysisTemplates"
	opListCollaborationAnalysisTemplates            = "ListCollaborationAnalysisTemplates"
	opListCollaborationChangeRequests               = "ListCollaborationChangeRequests"
	opListCollaborationConfiguredAudienceModelAssociations = "ListCollaborationConfiguredAudienceModelAssociations"
	opListCollaborationIdNamespaceAssociations      = "ListCollaborationIdNamespaceAssociations"
	opListCollaborationPrivacyBudgets               = "ListCollaborationPrivacyBudgets"
	opListCollaborationPrivacyBudgetTemplates       = "ListCollaborationPrivacyBudgetTemplates"
	opListCollaborations                            = "ListCollaborations"
	opListConfiguredAudienceModelAssociations       = "ListConfiguredAudienceModelAssociations"
	opListConfiguredTableAssociations               = "ListConfiguredTableAssociations"
	opListConfiguredTables                          = "ListConfiguredTables"
	opListIdMappingTables                           = "ListIdMappingTables"
	opListIdNamespaceAssociations                   = "ListIdNamespaceAssociations"
	opListMembers                                   = "ListMembers"
	opListMemberships                               = "ListMemberships"
	opListPrivacyBudgets                            = "ListPrivacyBudgets"
	opListPrivacyBudgetTemplates                    = "ListPrivacyBudgetTemplates"
	opListProtectedJobs                             = "ListProtectedJobs"
	opListProtectedQueries                          = "ListProtectedQueries"
	opListSchemas                                   = "ListSchemas"
	opListTagsForResource                           = "ListTagsForResource"
	opPopulateIdMappingTable                        = "PopulateIdMappingTable"
	opPreviewPrivacyImpact                          = "PreviewPrivacyImpact"
	opStartProtectedJob                             = "StartProtectedJob"
	opStartProtectedQuery                           = "StartProtectedQuery"
	opTagResource                                   = "TagResource"
	opUntagResource                                 = "UntagResource"
	opUpdateAnalysisTemplate                        = "UpdateAnalysisTemplate"
	opUpdateCollaboration                           = "UpdateCollaboration"
	opUpdateCollaborationChangeRequest              = "UpdateCollaborationChangeRequest"
	opUpdateConfiguredAudienceModelAssociation      = "UpdateConfiguredAudienceModelAssociation"
	opUpdateConfiguredTable                         = "UpdateConfiguredTable"
	opUpdateConfiguredTableAnalysisRule             = "UpdateConfiguredTableAnalysisRule"
	opUpdateConfiguredTableAssociation              = "UpdateConfiguredTableAssociation"
	opUpdateConfiguredTableAssociationAnalysisRule  = "UpdateConfiguredTableAssociationAnalysisRule"
	opUpdateIdMappingTable                          = "UpdateIdMappingTable"
	opUpdateIdNamespaceAssociation                  = "UpdateIdNamespaceAssociation"
	opUpdateMembership                              = "UpdateMembership"
	opUpdatePrivacyBudgetTemplate                   = "UpdatePrivacyBudgetTemplate"
	opUpdateProtectedJob                            = "UpdateProtectedJob"
	opUpdateProtectedQuery                          = "UpdateProtectedQuery"
	opUnknown                                       = ""
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
		opCreateIdMappingTable,
		opCreateIdNamespaceAssociation,
		opCreateMembership,
		opCreatePrivacyBudgetTemplate,
		opDeleteAnalysisTemplate,
		opDeleteCollaboration,
		opDeleteConfiguredAudienceModelAssociation,
		opDeleteConfiguredTable,
		opDeleteConfiguredTableAnalysisRule,
		opDeleteConfiguredTableAssociation,
		opDeleteConfiguredTableAssociationAnalysisRule,
		opDeleteIdMappingTable,
		opDeleteIdNamespaceAssociation,
		opDeleteMember,
		opDeleteMembership,
		opDeletePrivacyBudgetTemplate,
		opGetAnalysisTemplate,
		opGetCollaboration,
		opGetCollaborationAnalysisTemplate,
		opGetCollaborationChangeRequest,
		opGetCollaborationConfiguredAudienceModelAssociation,
		opGetCollaborationIdNamespaceAssociation,
		opGetCollaborationPrivacyBudgetTemplate,
		opGetConfiguredAudienceModelAssociation,
		opGetConfiguredTable,
		opGetConfiguredTableAnalysisRule,
		opGetConfiguredTableAssociation,
		opGetConfiguredTableAssociationAnalysisRule,
		opGetIdMappingTable,
		opGetIdNamespaceAssociation,
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
		opListCollaborationIdNamespaceAssociations,
		opListCollaborationPrivacyBudgets,
		opListCollaborationPrivacyBudgetTemplates,
		opListCollaborations,
		opListConfiguredAudienceModelAssociations,
		opListConfiguredTableAssociations,
		opListConfiguredTables,
		opListIdMappingTables,
		opListIdNamespaceAssociations,
		opListMembers,
		opListMemberships,
		opListPrivacyBudgets,
		opListPrivacyBudgetTemplates,
		opListProtectedJobs,
		opListProtectedQueries,
		opListSchemas,
		opListTagsForResource,
		opPopulateIdMappingTable,
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
		opUpdateIdMappingTable,
		opUpdateIdNamespaceAssociation,
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

// pathRouteEntry holds a parsed path classification.
type pathRouteEntry struct {
	op      string
	seg     []string
}

// classifyPath maps (method, path) to an operation name and primary resource.
func classifyPath(method, path string) (string, string) {
	// Trim leading slash and split
	path = strings.TrimPrefix(path, "/")
	segs := strings.SplitN(path, "/", -1)
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
	if len(segs) == 1 {
		switch method {
		case http.MethodPost:
			return opCreateCollaboration, ""
		case http.MethodGet:
			return opListCollaborations, ""
		}
	}
	// /collaborations/{id}
	if len(segs) == 2 {
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
	// /collaborations/{id}/{sub}
	if len(segs) >= 3 {
		id := segs[1]
		sub := segs[2]
		switch sub {
		case "analysistemplates":
			if len(segs) == 3 {
				if method == http.MethodGet {
					return opListCollaborationAnalysisTemplates, id
				}
			}
			if len(segs) == 4 {
				if method == http.MethodGet {
					return opGetCollaborationAnalysisTemplate, id
				}
			}
		case "batch-analysistemplates":
			if method == http.MethodPost {
				return opBatchGetCollaborationAnalysisTemplate, id
			}
		case "batch-schema":
			if method == http.MethodPost {
				return opBatchGetSchema, id
			}
		case "batch-schema-analysis-rule":
			if method == http.MethodPost {
				return opBatchGetSchemaAnalysisRule, id
			}
		case "changeRequests":
			if len(segs) == 3 {
				switch method {
				case http.MethodPost:
					return opCreateCollaborationChangeRequest, id
				case http.MethodGet:
					return opListCollaborationChangeRequests, id
				}
			}
			if len(segs) == 4 {
				switch method {
				case http.MethodGet:
					return opGetCollaborationChangeRequest, id
				case http.MethodPatch:
					return opUpdateCollaborationChangeRequest, id
				}
			}
		case "configuredaudiencemodelassociations":
			if len(segs) == 3 {
				if method == http.MethodGet {
					return opListCollaborationConfiguredAudienceModelAssociations, id
				}
			}
			if len(segs) == 4 {
				if method == http.MethodGet {
					return opGetCollaborationConfiguredAudienceModelAssociation, id
				}
			}
		case "idnamespaceassociations":
			if len(segs) == 3 {
				if method == http.MethodGet {
					return opListCollaborationIdNamespaceAssociations, id
				}
			}
			if len(segs) == 4 {
				if method == http.MethodGet {
					return opGetCollaborationIdNamespaceAssociation, id
				}
			}
		case "member":
			// /collaborations/{id}/member/{accountId}
			if len(segs) == 4 && method == http.MethodDelete {
				return opDeleteMember, id
			}
		case "members":
			if method == http.MethodGet {
				return opListMembers, id
			}
		case "privacybudgettemplates":
			if len(segs) == 3 {
				if method == http.MethodGet {
					return opListCollaborationPrivacyBudgetTemplates, id
				}
			}
			if len(segs) == 4 {
				if method == http.MethodGet {
					return opGetCollaborationPrivacyBudgetTemplate, id
				}
			}
		case "privacybudgets":
			if method == http.MethodGet {
				return opListCollaborationPrivacyBudgets, id
			}
		case "schemas":
			if len(segs) == 3 {
				if method == http.MethodGet {
					return opListSchemas, id
				}
			}
			if len(segs) == 4 {
				if method == http.MethodGet {
					return opGetSchema, id
				}
			}
			// /collaborations/{id}/schemas/{name}/analysisRule/{type}
			if len(segs) == 6 && segs[4] == "analysisRule" {
				if method == http.MethodGet {
					return opGetSchemaAnalysisRule, id
				}
			}
		}
	}
	return opUnknown, ""
}

func classifyConfiguredTables(method string, segs []string) (string, string) {
	// /configuredTables
	if len(segs) == 1 {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredTable, ""
		case http.MethodGet:
			return opListConfiguredTables, ""
		}
	}
	// /configuredTables/{id}
	if len(segs) == 2 {
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
	// /configuredTables/{id}/analysisRule
	if len(segs) == 3 && segs[2] == "analysisRule" {
		id := segs[1]
		if method == http.MethodPost {
			return opCreateConfiguredTableAnalysisRule, id
		}
	}
	// /configuredTables/{id}/analysisRule/{type}
	if len(segs) == 4 && segs[2] == "analysisRule" {
		id := segs[1]
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
	if len(segs) == 1 {
		switch method {
		case http.MethodPost:
			return opCreateMembership, ""
		case http.MethodGet:
			return opListMemberships, ""
		}
	}
	// /memberships/{id}
	if len(segs) == 2 {
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
	if len(segs) < 3 {
		return opUnknown, ""
	}
	membershipID := segs[1]
	sub := segs[2]
	switch sub {
	case "analysistemplates":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreateAnalysisTemplate, membershipID
			case http.MethodGet:
				return opListAnalysisTemplates, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetAnalysisTemplate, membershipID
			case http.MethodDelete:
				return opDeleteAnalysisTemplate, membershipID
			case http.MethodPatch:
				return opUpdateAnalysisTemplate, membershipID
			}
		}
	case "configuredTableAssociations":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreateConfiguredTableAssociation, membershipID
			case http.MethodGet:
				return opListConfiguredTableAssociations, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetConfiguredTableAssociation, membershipID
			case http.MethodDelete:
				return opDeleteConfiguredTableAssociation, membershipID
			case http.MethodPatch:
				return opUpdateConfiguredTableAssociation, membershipID
			}
		}
		// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule
		if len(segs) == 5 && segs[4] == "analysisRule" {
			if method == http.MethodPost {
				return opCreateConfiguredTableAssociationAnalysisRule, membershipID
			}
		}
		// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule/{type}
		if len(segs) == 6 && segs[4] == "analysisRule" {
			switch method {
			case http.MethodGet:
				return opGetConfiguredTableAssociationAnalysisRule, membershipID
			case http.MethodDelete:
				return opDeleteConfiguredTableAssociationAnalysisRule, membershipID
			case http.MethodPatch:
				return opUpdateConfiguredTableAssociationAnalysisRule, membershipID
			}
		}
	case "configuredaudiencemodelassociations":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreateConfiguredAudienceModelAssociation, membershipID
			case http.MethodGet:
				return opListConfiguredAudienceModelAssociations, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetConfiguredAudienceModelAssociation, membershipID
			case http.MethodDelete:
				return opDeleteConfiguredAudienceModelAssociation, membershipID
			case http.MethodPatch:
				return opUpdateConfiguredAudienceModelAssociation, membershipID
			}
		}
	case "idmappingtables":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreateIdMappingTable, membershipID
			case http.MethodGet:
				return opListIdMappingTables, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetIdMappingTable, membershipID
			case http.MethodDelete:
				return opDeleteIdMappingTable, membershipID
			case http.MethodPatch:
				return opUpdateIdMappingTable, membershipID
			}
		}
		// /memberships/{id}/idmappingtables/{tableId}/populate
		if len(segs) == 5 && segs[4] == "populate" {
			if method == http.MethodPost {
				return opPopulateIdMappingTable, membershipID
			}
		}
	case "idnamespaceassociations":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreateIdNamespaceAssociation, membershipID
			case http.MethodGet:
				return opListIdNamespaceAssociations, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetIdNamespaceAssociation, membershipID
			case http.MethodDelete:
				return opDeleteIdNamespaceAssociation, membershipID
			case http.MethodPatch:
				return opUpdateIdNamespaceAssociation, membershipID
			}
		}
	case "previewprivacyimpact":
		if method == http.MethodPost {
			return opPreviewPrivacyImpact, membershipID
		}
	case "privacybudgets":
		if method == http.MethodGet {
			return opListPrivacyBudgets, membershipID
		}
	case "privacybudgettemplates":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opCreatePrivacyBudgetTemplate, membershipID
			case http.MethodGet:
				return opListPrivacyBudgetTemplates, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetPrivacyBudgetTemplate, membershipID
			case http.MethodDelete:
				return opDeletePrivacyBudgetTemplate, membershipID
			case http.MethodPatch:
				return opUpdatePrivacyBudgetTemplate, membershipID
			}
		}
	case "protectedJobs":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opStartProtectedJob, membershipID
			case http.MethodGet:
				return opListProtectedJobs, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetProtectedJob, membershipID
			case http.MethodPatch:
				return opUpdateProtectedJob, membershipID
			}
		}
	case "protectedQueries":
		if len(segs) == 3 {
			switch method {
			case http.MethodPost:
				return opStartProtectedQuery, membershipID
			case http.MethodGet:
				return opListProtectedQueries, membershipID
			}
		}
		if len(segs) == 4 {
			switch method {
			case http.MethodGet:
				return opGetProtectedQuery, membershipID
			case http.MethodPatch:
				return opUpdateProtectedQuery, membershipID
			}
		}
	}
	return opUnknown, ""
}

func classifyTags(method string, segs []string) (string, string) {
	if len(segs) < 2 {
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
func injectPathParams(path, op string, body []byte) []byte {
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
	case len(segs) >= 2 && segs[0] == "collaborations":
		setStr("collaborationIdentifier", segs[1])
		if len(segs) >= 4 {
			switch segs[2] {
			case "analysistemplates":
				setStr("analysisTemplateArn", segs[3])
			case "changeRequests":
				setStr("changeRequestIdentifier", segs[3])
			case "configuredaudiencemodelassociations":
				setStr("configuredAudienceModelAssociationIdentifier", segs[3])
			case "idnamespaceassociations":
				setStr("idNamespaceAssociationIdentifier", segs[3])
			case "member":
				setStr("accountId", segs[3])
			case "privacybudgettemplates":
				setStr("privacyBudgetTemplateIdentifier", segs[3])
			case "schemas":
				setStr("name", segs[3])
				if len(segs) == 6 && segs[4] == "analysisRule" {
					setStr("type", segs[5])
				}
			}
		}
	case len(segs) >= 2 && segs[0] == "configuredTables":
		setStr("configuredTableIdentifier", segs[1])
		if len(segs) == 4 && segs[2] == "analysisRule" {
			setStr("analysisRuleType", segs[3])
		}
	case len(segs) >= 2 && segs[0] == "memberships":
		setStr("membershipIdentifier", segs[1])
		if len(segs) >= 4 {
			switch segs[2] {
			case "analysistemplates":
				setStr("analysisTemplateIdentifier", segs[3])
			case "configuredTableAssociations":
				setStr("configuredTableAssociationIdentifier", segs[3])
				if len(segs) == 6 && segs[4] == "analysisRule" {
					setStr("analysisRuleType", segs[5])
				}
			case "configuredaudiencemodelassociations":
				setStr("configuredAudienceModelAssociationIdentifier", segs[3])
			case "idmappingtables":
				setStr("idMappingTableIdentifier", segs[3])
			case "idnamespaceassociations":
				setStr("idNamespaceAssociationIdentifier", segs[3])
			case "privacybudgettemplates":
				setStr("privacyBudgetTemplateIdentifier", segs[3])
			case "protectedJobs":
				setStr("protectedJobIdentifier", segs[3])
			case "protectedQueries":
				setStr("protectedQueryIdentifier", segs[3])
			}
		}
	case len(segs) >= 2 && segs[0] == "tags":
		arnVal := strings.Join(segs[1:], "/")
		setStr("resourceArn", arnVal)
	}

	out, _ := json.Marshal(m)
	return out
}

// ---- dispatch ----

func (h *Handler) dispatch(ctx context.Context, op string, body []byte, c *echo.Context) ([]byte, error) {
	switch op {
	// Collaboration
	case opCreateCollaboration:
		return h.handleCreateCollaboration(ctx, body)
	case opGetCollaboration:
		return h.handleGetCollaboration(ctx, body)
	case opListCollaborations:
		return h.handleListCollaborations(ctx, body, c)
	case opUpdateCollaboration:
		return h.handleUpdateCollaboration(ctx, body)
	case opDeleteCollaboration:
		return h.handleDeleteCollaboration(ctx, body)
	case opListMembers:
		return h.handleListMembers(ctx, body, c)
	case opDeleteMember:
		return h.handleDeleteMember(ctx, body)
	// Collaboration sub-resources
	case opGetCollaborationAnalysisTemplate:
		return h.handleGetCollaborationAnalysisTemplate(ctx, body)
	case opListCollaborationAnalysisTemplates:
		return h.handleListCollaborationAnalysisTemplates(ctx, body, c)
	case opBatchGetCollaborationAnalysisTemplate:
		return h.handleBatchGetCollaborationAnalysisTemplate(ctx, body)
	case opBatchGetSchema:
		return h.handleBatchGetSchema(ctx, body)
	case opBatchGetSchemaAnalysisRule:
		return h.handleBatchGetSchemaAnalysisRule(ctx, body)
	case opGetSchema:
		return h.handleGetSchema(ctx, body)
	case opListSchemas:
		return h.handleListSchemas(ctx, body, c)
	case opGetSchemaAnalysisRule:
		return h.handleGetSchemaAnalysisRule(ctx, body)
	case opCreateCollaborationChangeRequest:
		return h.handleCreateCollaborationChangeRequest(ctx, body)
	case opGetCollaborationChangeRequest:
		return h.handleGetCollaborationChangeRequest(ctx, body)
	case opListCollaborationChangeRequests:
		return h.handleListCollaborationChangeRequests(ctx, body, c)
	case opUpdateCollaborationChangeRequest:
		return h.handleUpdateCollaborationChangeRequest(ctx, body)
	case opGetCollaborationConfiguredAudienceModelAssociation:
		return h.handleGetCollaborationConfiguredAudienceModelAssociation(ctx, body)
	case opListCollaborationConfiguredAudienceModelAssociations:
		return h.handleListCollaborationConfiguredAudienceModelAssociations(ctx, body, c)
	case opGetCollaborationIdNamespaceAssociation:
		return h.handleGetCollaborationIdNamespaceAssociation(ctx, body)
	case opListCollaborationIdNamespaceAssociations:
		return h.handleListCollaborationIdNamespaceAssociations(ctx, body, c)
	case opGetCollaborationPrivacyBudgetTemplate:
		return h.handleGetCollaborationPrivacyBudgetTemplate(ctx, body)
	case opListCollaborationPrivacyBudgetTemplates:
		return h.handleListCollaborationPrivacyBudgetTemplates(ctx, body, c)
	case opListCollaborationPrivacyBudgets:
		return h.handleListCollaborationPrivacyBudgets(ctx, body, c)
	// Membership
	case opCreateMembership:
		return h.handleCreateMembership(ctx, body)
	case opGetMembership:
		return h.handleGetMembership(ctx, body)
	case opListMemberships:
		return h.handleListMemberships(ctx, body, c)
	case opUpdateMembership:
		return h.handleUpdateMembership(ctx, body)
	case opDeleteMembership:
		return h.handleDeleteMembership(ctx, body)
	// ConfiguredTable
	case opCreateConfiguredTable:
		return h.handleCreateConfiguredTable(ctx, body)
	case opGetConfiguredTable:
		return h.handleGetConfiguredTable(ctx, body)
	case opListConfiguredTables:
		return h.handleListConfiguredTables(ctx, body, c)
	case opUpdateConfiguredTable:
		return h.handleUpdateConfiguredTable(ctx, body)
	case opDeleteConfiguredTable:
		return h.handleDeleteConfiguredTable(ctx, body)
	// ConfiguredTableAnalysisRule
	case opCreateConfiguredTableAnalysisRule:
		return h.handleCreateConfiguredTableAnalysisRule(ctx, body)
	case opGetConfiguredTableAnalysisRule:
		return h.handleGetConfiguredTableAnalysisRule(ctx, body)
	case opUpdateConfiguredTableAnalysisRule:
		return h.handleUpdateConfiguredTableAnalysisRule(ctx, body)
	case opDeleteConfiguredTableAnalysisRule:
		return h.handleDeleteConfiguredTableAnalysisRule(ctx, body)
	// ConfiguredTableAssociation
	case opCreateConfiguredTableAssociation:
		return h.handleCreateConfiguredTableAssociation(ctx, body)
	case opGetConfiguredTableAssociation:
		return h.handleGetConfiguredTableAssociation(ctx, body)
	case opListConfiguredTableAssociations:
		return h.handleListConfiguredTableAssociations(ctx, body, c)
	case opUpdateConfiguredTableAssociation:
		return h.handleUpdateConfiguredTableAssociation(ctx, body)
	case opDeleteConfiguredTableAssociation:
		return h.handleDeleteConfiguredTableAssociation(ctx, body)
	// ConfiguredTableAssociationAnalysisRule
	case opCreateConfiguredTableAssociationAnalysisRule:
		return h.handleCreateConfiguredTableAssociationAnalysisRule(ctx, body)
	case opGetConfiguredTableAssociationAnalysisRule:
		return h.handleGetConfiguredTableAssociationAnalysisRule(ctx, body)
	case opUpdateConfiguredTableAssociationAnalysisRule:
		return h.handleUpdateConfiguredTableAssociationAnalysisRule(ctx, body)
	case opDeleteConfiguredTableAssociationAnalysisRule:
		return h.handleDeleteConfiguredTableAssociationAnalysisRule(ctx, body)
	// AnalysisTemplate
	case opCreateAnalysisTemplate:
		return h.handleCreateAnalysisTemplate(ctx, body)
	case opGetAnalysisTemplate:
		return h.handleGetAnalysisTemplate(ctx, body)
	case opListAnalysisTemplates:
		return h.handleListAnalysisTemplates(ctx, body, c)
	case opUpdateAnalysisTemplate:
		return h.handleUpdateAnalysisTemplate(ctx, body)
	case opDeleteAnalysisTemplate:
		return h.handleDeleteAnalysisTemplate(ctx, body)
	// ProtectedQuery
	case opStartProtectedQuery:
		return h.handleStartProtectedQuery(ctx, body)
	case opGetProtectedQuery:
		return h.handleGetProtectedQuery(ctx, body)
	case opListProtectedQueries:
		return h.handleListProtectedQueries(ctx, body, c)
	case opUpdateProtectedQuery:
		return h.handleUpdateProtectedQuery(ctx, body)
	// ProtectedJob
	case opStartProtectedJob:
		return h.handleStartProtectedJob(ctx, body)
	case opGetProtectedJob:
		return h.handleGetProtectedJob(ctx, body)
	case opListProtectedJobs:
		return h.handleListProtectedJobs(ctx, body, c)
	case opUpdateProtectedJob:
		return h.handleUpdateProtectedJob(ctx, body)
	// PrivacyBudgetTemplate
	case opCreatePrivacyBudgetTemplate:
		return h.handleCreatePrivacyBudgetTemplate(ctx, body)
	case opGetPrivacyBudgetTemplate:
		return h.handleGetPrivacyBudgetTemplate(ctx, body)
	case opListPrivacyBudgetTemplates:
		return h.handleListPrivacyBudgetTemplates(ctx, body, c)
	case opUpdatePrivacyBudgetTemplate:
		return h.handleUpdatePrivacyBudgetTemplate(ctx, body)
	case opDeletePrivacyBudgetTemplate:
		return h.handleDeletePrivacyBudgetTemplate(ctx, body)
	case opListPrivacyBudgets:
		return h.handleListPrivacyBudgets(ctx, body, c)
	case opPreviewPrivacyImpact:
		return h.handlePreviewPrivacyImpact(ctx, body)
	// IdMappingTable
	case opCreateIdMappingTable:
		return h.handleCreateIdMappingTable(ctx, body)
	case opGetIdMappingTable:
		return h.handleGetIdMappingTable(ctx, body)
	case opListIdMappingTables:
		return h.handleListIdMappingTables(ctx, body, c)
	case opUpdateIdMappingTable:
		return h.handleUpdateIdMappingTable(ctx, body)
	case opDeleteIdMappingTable:
		return h.handleDeleteIdMappingTable(ctx, body)
	case opPopulateIdMappingTable:
		return h.handlePopulateIdMappingTable(ctx, body)
	// IdNamespaceAssociation
	case opCreateIdNamespaceAssociation:
		return h.handleCreateIdNamespaceAssociation(ctx, body)
	case opGetIdNamespaceAssociation:
		return h.handleGetIdNamespaceAssociation(ctx, body)
	case opListIdNamespaceAssociations:
		return h.handleListIdNamespaceAssociations(ctx, body, c)
	case opUpdateIdNamespaceAssociation:
		return h.handleUpdateIdNamespaceAssociation(ctx, body)
	case opDeleteIdNamespaceAssociation:
		return h.handleDeleteIdNamespaceAssociation(ctx, body)
	// ConfiguredAudienceModelAssociation
	case opCreateConfiguredAudienceModelAssociation:
		return h.handleCreateConfiguredAudienceModelAssociation(ctx, body)
	case opGetConfiguredAudienceModelAssociation:
		return h.handleGetConfiguredAudienceModelAssociation(ctx, body)
	case opListConfiguredAudienceModelAssociations:
		return h.handleListConfiguredAudienceModelAssociations(ctx, body, c)
	case opUpdateConfiguredAudienceModelAssociation:
		return h.handleUpdateConfiguredAudienceModelAssociation(ctx, body)
	case opDeleteConfiguredAudienceModelAssociation:
		return h.handleDeleteConfiguredAudienceModelAssociation(ctx, body)
	// Tags
	case opListTagsForResource:
		return h.handleListTagsForResource(ctx, body)
	case opTagResource:
		return h.handleTagResource(ctx, body)
	case opUntagResource:
		return h.handleUntagResource(ctx, body, c)
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
		Name                   string            `json:"name"`
		Description            string            `json:"description"`
		CreatorDisplayName     string            `json:"creatorDisplayName"`
		CreatorMemberAbilities []string          `json:"creatorMemberAbilities"`
		Members                []MemberSpec      `json:"members"`
		QueryLogStatus         string            `json:"queryLogStatus"`
		Tags                   map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	c, err := h.Backend.CreateCollaboration(req.Name, req.Description, req.CreatorDisplayName, req.CreatorMemberAbilities, req.Members, req.QueryLogStatus, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"collaboration": c}), nil
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
	return mustJSON(map[string]any{"collaboration": c}), nil
}

func (h *Handler) handleListCollaborations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	items, next := h.Backend.ListCollaborations(qp(c, "memberStatus"), qp(c, "maxResults"), qp(c, "nextToken"))
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
	col, err := h.Backend.UpdateCollaboration(req.CollaborationIdentifier, req.Name, req.Description)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"collaboration": col}), nil
}

func (h *Handler) handleDeleteCollaboration(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteCollaboration(req.CollaborationIdentifier)
}

func (h *Handler) handleListMembers(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListMembers(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
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
		AccountId               string `json:"accountId"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteMember(req.CollaborationIdentifier, req.AccountId)
}

func (h *Handler) handleGetCollaborationAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		AnalysisTemplateArn     string `json:"analysisTemplateArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationAnalysisTemplate(req.CollaborationIdentifier, req.AnalysisTemplateArn)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisTemplate": t}), nil
}

func (h *Handler) handleListCollaborationAnalysisTemplates(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationAnalysisTemplates(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"analysisTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleBatchGetCollaborationAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		AnalysisTemplateArns    []string `json:"analysisTemplateArns"`
	}
	_ = json.Unmarshal(body, &req)
	items, errs, err := h.Backend.BatchGetCollaborationAnalysisTemplate(req.CollaborationIdentifier, req.AnalysisTemplateArns)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisTemplates": items, "errors": errs}), nil
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
	return mustJSON(map[string]any{"schemas": items, "errors": errs}), nil
}

func (h *Handler) handleBatchGetSchemaAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string   `json:"collaborationIdentifier"`
		SchemaAnalysisRuleRequests []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"schemaAnalysisRuleRequests"`
	}
	_ = json.Unmarshal(body, &req)
	var names []string
	var ruleType string
	for _, r := range req.SchemaAnalysisRuleRequests {
		names = append(names, r.Name)
		if ruleType == "" {
			ruleType = r.Type
		}
	}
	items, errs, err := h.Backend.BatchGetSchemaAnalysisRule(req.CollaborationIdentifier, names, ruleType)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRules": items, "errors": errs}), nil
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

func (h *Handler) handleListSchemas(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListSchemas(req.CollaborationIdentifier, qp(c, "schemaType"), qp(c, "maxResults"), qp(c, "nextToken"))
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
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleCreateCollaborationChangeRequest(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string         `json:"collaborationIdentifier"`
		Type                    string         `json:"type"`
		Details                 map[string]any `json:"details"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateCollaborationChangeRequest(req.CollaborationIdentifier, req.Type, req.Details)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"collaborationChangeRequest": r}), nil
}

func (h *Handler) handleGetCollaborationChangeRequest(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetCollaborationChangeRequest(req.CollaborationIdentifier, req.ChangeRequestIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"collaborationChangeRequest": r}), nil
}

func (h *Handler) handleListCollaborationChangeRequests(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationChangeRequests(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collaborationChangeRequests": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateCollaborationChangeRequest(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
		ChangeRequestIdentifier string `json:"changeRequestIdentifier"`
		Status                  string `json:"status"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateCollaborationChangeRequest(req.CollaborationIdentifier, req.ChangeRequestIdentifier, req.Status)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"collaborationChangeRequest": r}), nil
}

func (h *Handler) handleGetCollaborationConfiguredAudienceModelAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier                      string `json:"collaborationIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationConfiguredAudienceModelAssociation(req.CollaborationIdentifier, req.ConfiguredAudienceModelAssociationIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredAudienceModelAssociation": a}), nil
}

func (h *Handler) handleListCollaborationConfiguredAudienceModelAssociations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationConfiguredAudienceModelAssociations(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleGetCollaborationIdNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier          string `json:"collaborationIdentifier"`
		IdNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetCollaborationIdNamespaceAssociation(req.CollaborationIdentifier, req.IdNamespaceAssociationIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idNamespaceAssociation": a}), nil
}

func (h *Handler) handleListCollaborationIdNamespaceAssociations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationIdNamespaceAssociations(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleGetCollaborationPrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		CollaborationIdentifier         string `json:"collaborationIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetCollaborationPrivacyBudgetTemplate(req.CollaborationIdentifier, req.PrivacyBudgetTemplateIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"privacyBudgetTemplate": t}), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgetTemplates(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgetTemplates(req.CollaborationIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleListCollaborationPrivacyBudgets(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		CollaborationIdentifier string `json:"collaborationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListCollaborationPrivacyBudgets(req.CollaborationIdentifier, qp(c, "privacyBudgetType"), qp(c, "maxResults"), qp(c, "nextToken"))
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
		CollaborationIdentifier    string            `json:"collaborationIdentifier"`
		QueryLogStatus             string            `json:"queryLogStatus"`
		DefaultResultConfiguration map[string]any    `json:"defaultResultConfiguration"`
		PaymentConfiguration       map[string]any    `json:"paymentConfiguration"`
		Tags                       map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.CreateMembership(req.CollaborationIdentifier, req.QueryLogStatus, req.DefaultResultConfiguration, req.PaymentConfiguration, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"membership": m}), nil
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
	return mustJSON(map[string]any{"membership": m}), nil
}

func (h *Handler) handleListMemberships(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	items, next := h.Backend.ListMemberships(qp(c, "status"), qp(c, "maxResults"), qp(c, "nextToken"))
	resp := map[string]any{"membershipSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateMembership(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string         `json:"membershipIdentifier"`
		QueryLogStatus             string         `json:"queryLogStatus"`
		DefaultResultConfiguration map[string]any `json:"defaultResultConfiguration"`
	}
	_ = json.Unmarshal(body, &req)
	m, err := h.Backend.UpdateMembership(req.MembershipIdentifier, req.QueryLogStatus, req.DefaultResultConfiguration)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"membership": m}), nil
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
		Name             string            `json:"name"`
		Description      string            `json:"description"`
		TableReference   map[string]any    `json:"tableReference"`
		AllowedColumns   []string          `json:"allowedColumns"`
		AnalysisMethod   string            `json:"analysisMethod"`
		Tags             map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	ct, err := h.Backend.CreateConfiguredTable(req.Name, req.Description, req.TableReference, req.AllowedColumns, req.AnalysisMethod, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredTable": ct}), nil
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
	return mustJSON(map[string]any{"configuredTable": ct}), nil
}

func (h *Handler) handleListConfiguredTables(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
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
	ct, err := h.Backend.UpdateConfiguredTable(req.ConfiguredTableIdentifier, req.Name, req.Description)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredTable": ct}), nil
}

func (h *Handler) handleDeleteConfiguredTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteConfiguredTable(req.ConfiguredTableIdentifier)
}

// ---- ConfiguredTableAnalysisRule handlers ----

func (h *Handler) handleCreateConfiguredTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAnalysisRule(req.ConfiguredTableIdentifier, req.AnalysisRuleType, req.AnalysisRulePolicy)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleGetConfiguredTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAnalysisRule(req.ConfiguredTableIdentifier, req.AnalysisRuleType)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
		AnalysisRuleType          string         `json:"analysisRuleType"`
		AnalysisRulePolicy        map[string]any `json:"analysisRulePolicy"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAnalysisRule(req.ConfiguredTableIdentifier, req.AnalysisRuleType, req.AnalysisRulePolicy)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ConfiguredTableIdentifier string `json:"configuredTableIdentifier"`
		AnalysisRuleType          string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteConfiguredTableAnalysisRule(req.ConfiguredTableIdentifier, req.AnalysisRuleType)
}

// ---- ConfiguredTableAssociation handlers ----

func (h *Handler) handleCreateConfiguredTableAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier      string            `json:"membershipIdentifier"`
		Name                      string            `json:"name"`
		Description               string            `json:"description"`
		ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
		RoleArn                   string            `json:"roleArn"`
		Tags                      map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredTableAssociation(req.MembershipIdentifier, req.Name, req.Description, req.ConfiguredTableIdentifier, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredTableAssociation": a}), nil
}

func (h *Handler) handleGetConfiguredTableAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredTableAssociation(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredTableAssociation": a}), nil
}

func (h *Handler) handleListConfiguredTableAssociations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredTableAssociations(req.MembershipIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredTableAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		Description                          string `json:"description"`
		RoleArn                              string `json:"roleArn"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredTableAssociation(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier, req.Description, req.RoleArn)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredTableAssociation": a}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteConfiguredTableAssociation(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier)
}

// ---- ConfiguredTableAssociationAnalysisRule handlers ----

func (h *Handler) handleCreateConfiguredTableAssociationAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.CreateConfiguredTableAssociationAnalysisRule(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier, req.AnalysisRuleType, req.AnalysisRulePolicy)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleGetConfiguredTableAssociationAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.GetConfiguredTableAssociationAnalysisRule(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier, req.AnalysisRuleType)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleUpdateConfiguredTableAssociationAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string         `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string         `json:"analysisRuleType"`
		AnalysisRulePolicy                   map[string]any `json:"analysisRulePolicy"`
	}
	_ = json.Unmarshal(body, &req)
	r, err := h.Backend.UpdateConfiguredTableAssociationAnalysisRule(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier, req.AnalysisRuleType, req.AnalysisRulePolicy)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisRule": r}), nil
}

func (h *Handler) handleDeleteConfiguredTableAssociationAnalysisRule(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                 string `json:"membershipIdentifier"`
		ConfiguredTableAssociationIdentifier string `json:"configuredTableAssociationIdentifier"`
		AnalysisRuleType                     string `json:"analysisRuleType"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteConfiguredTableAssociationAnalysisRule(req.MembershipIdentifier, req.ConfiguredTableAssociationIdentifier, req.AnalysisRuleType)
}

// ---- AnalysisTemplate handlers ----

func (h *Handler) handleCreateAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		Format               string            `json:"format"`
		Source               map[string]any    `json:"source"`
		AnalysisParameters   []map[string]any  `json:"analysisParameters"`
		Tags                 map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateAnalysisTemplate(req.MembershipIdentifier, req.Name, req.Description, req.Format, req.Source, req.AnalysisParameters, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisTemplate": t}), nil
}

func (h *Handler) handleGetAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetAnalysisTemplate(req.MembershipIdentifier, req.AnalysisTemplateIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisTemplate": t}), nil
}

func (h *Handler) handleListAnalysisTemplates(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListAnalysisTemplates(req.MembershipIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
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
	t, err := h.Backend.UpdateAnalysisTemplate(req.MembershipIdentifier, req.AnalysisTemplateIdentifier, req.Description)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"analysisTemplate": t}), nil
}

func (h *Handler) handleDeleteAnalysisTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier       string `json:"membershipIdentifier"`
		AnalysisTemplateIdentifier string `json:"analysisTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteAnalysisTemplate(req.MembershipIdentifier, req.AnalysisTemplateIdentifier)
}

// ---- ProtectedQuery handlers ----

func (h *Handler) handleStartProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string         `json:"membershipIdentifier"`
		SqlParameters        map[string]any `json:"sqlParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
		ComputeConfiguration map[string]any `json:"computeConfiguration"`
	}
	_ = json.Unmarshal(body, &req)
	var sqlText string
	if req.SqlParameters != nil {
		if v, ok := req.SqlParameters["queryString"].(string); ok {
			sqlText = v
		}
	}
	q, err := h.Backend.StartProtectedQuery(req.MembershipIdentifier, sqlText, req.ResultConfiguration, req.ComputeConfiguration)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedQuery": q}), nil
}

func (h *Handler) handleGetProtectedQuery(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.GetProtectedQuery(req.MembershipIdentifier, req.ProtectedQueryIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedQuery": q}), nil
}

func (h *Handler) handleListProtectedQueries(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedQueries(req.MembershipIdentifier, qp(c, "status"), qp(c, "maxResults"), qp(c, "nextToken"))
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
		MembershipIdentifier   string `json:"membershipIdentifier"`
		ProtectedQueryIdentifier string `json:"protectedQueryIdentifier"`
		TargetStatus           string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	q, err := h.Backend.UpdateProtectedQuery(req.MembershipIdentifier, req.ProtectedQueryIdentifier, req.TargetStatus)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedQuery": q}), nil
}

// ---- ProtectedJob handlers ----

func (h *Handler) handleStartProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string         `json:"membershipIdentifier"`
		Type                 string         `json:"type"`
		JobParameters        map[string]any `json:"jobParameters"`
		ResultConfiguration  map[string]any `json:"resultConfiguration"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.StartProtectedJob(req.MembershipIdentifier, req.Type, req.JobParameters, req.ResultConfiguration)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedJob": j}), nil
}

func (h *Handler) handleGetProtectedJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier  string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.GetProtectedJob(req.MembershipIdentifier, req.ProtectedJobIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedJob": j}), nil
}

func (h *Handler) handleListProtectedJobs(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListProtectedJobs(req.MembershipIdentifier, qp(c, "status"), qp(c, "maxResults"), qp(c, "nextToken"))
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
		MembershipIdentifier  string `json:"membershipIdentifier"`
		ProtectedJobIdentifier string `json:"protectedJobIdentifier"`
		TargetStatus          string `json:"targetStatus"`
	}
	_ = json.Unmarshal(body, &req)
	j, err := h.Backend.UpdateProtectedJob(req.MembershipIdentifier, req.ProtectedJobIdentifier, req.TargetStatus)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"protectedJob": j}), nil
}

// ---- PrivacyBudgetTemplate handlers ----

func (h *Handler) handleCreatePrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string            `json:"membershipIdentifier"`
		PrivacyBudgetType    string            `json:"privacyBudgetType"`
		AutoRefresh          string            `json:"autoRefresh"`
		Parameters           map[string]any    `json:"parameters"`
		Tags                 map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreatePrivacyBudgetTemplate(req.MembershipIdentifier, req.PrivacyBudgetType, req.AutoRefresh, req.Parameters, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"privacyBudgetTemplate": t}), nil
}

func (h *Handler) handleGetPrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetPrivacyBudgetTemplate(req.MembershipIdentifier, req.PrivacyBudgetTemplateIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"privacyBudgetTemplate": t}), nil
}

func (h *Handler) handleListPrivacyBudgetTemplates(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgetTemplates(req.MembershipIdentifier, qp(c, "privacyBudgetType"), qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"privacyBudgetTemplateSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdatePrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string         `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
		AutoRefresh                     string         `json:"autoRefresh"`
		Parameters                      map[string]any `json:"parameters"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdatePrivacyBudgetTemplate(req.MembershipIdentifier, req.PrivacyBudgetTemplateIdentifier, req.AutoRefresh, req.Parameters)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"privacyBudgetTemplate": t}), nil
}

func (h *Handler) handleDeletePrivacyBudgetTemplate(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier            string `json:"membershipIdentifier"`
		PrivacyBudgetTemplateIdentifier string `json:"privacyBudgetTemplateIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeletePrivacyBudgetTemplate(req.MembershipIdentifier, req.PrivacyBudgetTemplateIdentifier)
}

func (h *Handler) handleListPrivacyBudgets(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListPrivacyBudgets(req.MembershipIdentifier, qp(c, "privacyBudgetType"), qp(c, "maxResults"), qp(c, "nextToken"))
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
		MembershipIdentifier string         `json:"membershipIdentifier"`
		Parameters           map[string]any `json:"parameters"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PreviewPrivacyImpact(req.MembershipIdentifier, req.Parameters)
	if err != nil {
		return nil, err
	}
	return mustJSON(result), nil
}

// ---- IdMappingTable handlers ----

func (h *Handler) handleCreateIdMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		KmsKeyArn            string            `json:"kmsKeyArn"`
		Tags                 map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.CreateIdMappingTable(req.MembershipIdentifier, req.Name, req.Description, req.InputReferenceConfig, req.KmsKeyArn, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idMappingTable": t}), nil
}

func (h *Handler) handleGetIdMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IdMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.GetIdMappingTable(req.MembershipIdentifier, req.IdMappingTableIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idMappingTable": t}), nil
}

func (h *Handler) handleListIdMappingTables(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIdMappingTables(req.MembershipIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idMappingTableSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIdMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IdMappingTableIdentifier string `json:"idMappingTableIdentifier"`
		Description              string `json:"description"`
		KmsKeyArn                string `json:"kmsKeyArn"`
	}
	_ = json.Unmarshal(body, &req)
	t, err := h.Backend.UpdateIdMappingTable(req.MembershipIdentifier, req.IdMappingTableIdentifier, req.Description, req.KmsKeyArn)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idMappingTable": t}), nil
}

func (h *Handler) handleDeleteIdMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IdMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteIdMappingTable(req.MembershipIdentifier, req.IdMappingTableIdentifier)
}

func (h *Handler) handlePopulateIdMappingTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier     string `json:"membershipIdentifier"`
		IdMappingTableIdentifier string `json:"idMappingTableIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	result, err := h.Backend.PopulateIdMappingTable(req.MembershipIdentifier, req.IdMappingTableIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(result), nil
}

// ---- IdNamespaceAssociation handlers ----

func (h *Handler) handleCreateIdNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier string            `json:"membershipIdentifier"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		InputReferenceConfig map[string]any    `json:"inputReferenceConfig"`
		IdMappingConfig      map[string]any    `json:"idMappingConfig"`
		Tags                 map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateIdNamespaceAssociation(req.MembershipIdentifier, req.Name, req.Description, req.InputReferenceConfig, req.IdMappingConfig, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idNamespaceAssociation": a}), nil
}

func (h *Handler) handleGetIdNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IdNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetIdNamespaceAssociation(req.MembershipIdentifier, req.IdNamespaceAssociationIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idNamespaceAssociation": a}), nil
}

func (h *Handler) handleListIdNamespaceAssociations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListIdNamespaceAssociations(req.MembershipIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"idNamespaceAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateIdNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string         `json:"membershipIdentifier"`
		IdNamespaceAssociationIdentifier string         `json:"idNamespaceAssociationIdentifier"`
		Description                      string         `json:"description"`
		IdMappingConfig                  map[string]any `json:"idMappingConfig"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateIdNamespaceAssociation(req.MembershipIdentifier, req.IdNamespaceAssociationIdentifier, req.Description, req.IdMappingConfig)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"idNamespaceAssociation": a}), nil
}

func (h *Handler) handleDeleteIdNamespaceAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier             string `json:"membershipIdentifier"`
		IdNamespaceAssociationIdentifier string `json:"idNamespaceAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteIdNamespaceAssociation(req.MembershipIdentifier, req.IdNamespaceAssociationIdentifier)
}

// ---- ConfiguredAudienceModelAssociation handlers ----

func (h *Handler) handleCreateConfiguredAudienceModelAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier           string            `json:"membershipIdentifier"`
		ConfiguredAudienceModelArn     string            `json:"configuredAudienceModelArn"`
		Name                           string            `json:"name"`
		Description                    string            `json:"description"`
		ManageResourcePolicies         bool              `json:"manageResourcePolicies"`
		Tags                           map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.CreateConfiguredAudienceModelAssociation(req.MembershipIdentifier, req.ConfiguredAudienceModelArn, req.Name, req.Description, req.ManageResourcePolicies, req.Tags)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredAudienceModelAssociation": a}), nil
}

func (h *Handler) handleGetConfiguredAudienceModelAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.GetConfiguredAudienceModelAssociation(req.MembershipIdentifier, req.ConfiguredAudienceModelAssociationIdentifier)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredAudienceModelAssociation": a}), nil
}

func (h *Handler) handleListConfiguredAudienceModelAssociations(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
	var req struct {
		MembershipIdentifier string `json:"membershipIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	items, next, err := h.Backend.ListConfiguredAudienceModelAssociations(req.MembershipIdentifier, qp(c, "maxResults"), qp(c, "nextToken"))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"configuredAudienceModelAssociationSummaries": items}
	if next != "" {
		resp["nextToken"] = next
	}
	return mustJSON(resp), nil
}

func (h *Handler) handleUpdateConfiguredAudienceModelAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
		Name                                         string `json:"name"`
		Description                                  string `json:"description"`
	}
	_ = json.Unmarshal(body, &req)
	a, err := h.Backend.UpdateConfiguredAudienceModelAssociation(req.MembershipIdentifier, req.ConfiguredAudienceModelAssociationIdentifier, req.Name, req.Description)
	if err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"configuredAudienceModelAssociation": a}), nil
}

func (h *Handler) handleDeleteConfiguredAudienceModelAssociation(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		MembershipIdentifier                         string `json:"membershipIdentifier"`
		ConfiguredAudienceModelAssociationIdentifier string `json:"configuredAudienceModelAssociationIdentifier"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.DeleteConfiguredAudienceModelAssociation(req.MembershipIdentifier, req.ConfiguredAudienceModelAssociationIdentifier)
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
		ResourceArn string            `json:"resourceArn"`
		Tags        map[string]string `json:"tags"`
	}
	_ = json.Unmarshal(body, &req)
	return nil, h.Backend.TagResource(req.ResourceArn, req.Tags)
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte, c *echo.Context) ([]byte, error) {
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
