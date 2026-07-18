package appsync

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	keySourceAPIAssociations = "sourceApiAssociations"
	keySourceAPIAssociation  = "sourceApiAssociation"
	keyEnvironmentVariables  = "environmentVariables"
	keyGraphqlAPI            = "graphqlApi"
	keyDataSource            = "dataSource"
	keyResolver              = "resolver"
	keyAPICache              = "apiCache"
	keyFunctionConfiguration = "functionConfiguration"
	keyType                  = "type"
	keyDomainNameConfig      = "domainNameConfig"
	keyAPI                   = "api"
	keyChannelNamespace      = "channelNamespace"
	keyStatus                = "status"
	keyCode                  = "code"
)

const (
	appsyncPathPrefix       = "/v1/apis"
	appsyncV2PathPrefix     = "/v2/apis"
	appsyncDomainPrefix     = "/v1/domainnames"
	appsyncSourcePrefix     = "/v1/sourceApis"
	appsyncMergedPrefix     = "/v1/mergedApis"
	appsyncIntrospectPrefix = "/v1/dataSource-introspections"
	// appsyncEvalCodePrefix and appsyncEvalTemplatePrefix match the real AWS SDK
	// endpoints ("/v1/dataplane-evaluatecode" and "/v1/dataplane-evaluatetemplate" —
	// two distinct top-level paths, not "/v1/dataplane-evaluations/{code,template}").
	appsyncEvalCodePrefix     = "/v1/dataplane-evaluatecode"
	appsyncEvalTemplatePrefix = "/v1/dataplane-evaluatetemplate"
	// appsyncTagsPrefix matches the real AWS SDK tagging endpoint
	// "/v1/tags/{resourceArn}" (tagging is NOT nested under /v1/apis/{apiId}/tags).
	appsyncTagsPrefix = "/v1/tags"

	// Path segment names.
	pathSegV1          = "v1"
	pathSegV2          = "v2"
	pathSegAPIs        = "apis"
	pathSegDomainNames = "domainnames"

	// Path segment counts for AppSync routes.
	pathSegsAPIs           = 2
	pathSegsAPIID          = 3
	pathSegsAPISubresource = 4
	pathSegsNamedResource  = 5
	pathSegsTypeResolvers  = 6
	pathSegsResolver       = 7

	// Resource segment names.
	pathSegDatasources       = "datasources"
	pathSegTypes             = "types"
	pathSegResolvers         = "resolvers"
	pathSegAPIKeys           = "apikeys"
	pathSegAPICaches         = "ApiCaches"
	pathSegFlushCache        = "FlushCache"
	pathSegTags              = "tags"
	pathSegFunctions         = "functions"
	pathSegChannelNamespaces = "channelNamespaces"

	// opUnknown is the operation name for unrecognized paths.
	opUnknown = "Unknown"
)

// Operation names referenced from more than one place (route labeling, dispatch,
// error logging, GetSupportedOperations) — deduplicated per goconst.
const (
	opListSourceAPIAssociations = "ListSourceApiAssociations"
	opFlushAPICache             = "FlushApiCache"
	opUpdateAPICache            = "UpdateApiCache"
	opListTagsForResource       = "ListTagsForResource"
	opTagResource               = "TagResource"
	opUntagResource             = "UntagResource"
)

// Handler is the Echo HTTP handler for AppSync operations.
type Handler struct {
	Backend       StorageBackend
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new AppSync handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// StartWorker starts the AppSync background workers.
func (h *Handler) StartWorker(ctx context.Context) error {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		go NewJanitor(b).Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppSync" }

// GetSupportedOperations returns the list of supported AppSync operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGraphqlApi",
		"GetGraphqlApi",
		"UpdateGraphqlApi",
		"ListGraphqlApis",
		"DeleteGraphqlApi",
		"StartSchemaCreation",
		"GetSchemaCreationStatus",
		"GetIntrospectionSchema",
		"CreateDataSource",
		"GetDataSource",
		"UpdateDataSource",
		"ListDataSources",
		"DeleteDataSource",
		"CreateResolver",
		"GetResolver",
		"UpdateResolver",
		"ListResolvers",
		"DeleteResolver",
		"ListResolversByFunction",
		"ExecuteGraphQL",
		"AssociateApi",
		"DisassociateApi",
		"AssociateMergedGraphqlApi",
		"AssociateSourceGraphqlApi",
		"DisassociateMergedGraphqlApi",
		"DisassociateSourceGraphqlApi",
		"GetSourceApiAssociation",
		opListSourceAPIAssociations,
		"CreateApi",
		"GetApi",
		"ListApis",
		"UpdateApi",
		"DeleteApi",
		"CreateApiCache",
		"DeleteApiCache",
		opFlushAPICache,
		"GetApiCache",
		opUpdateAPICache,
		"CreateApiKey",
		"DeleteApiKey",
		"ListApiKeys",
		"UpdateApiKey",
		"CreateChannelNamespace",
		"DeleteChannelNamespace",
		"GetChannelNamespace",
		"ListChannelNamespaces",
		"UpdateChannelNamespace",
		"CreateDomainName",
		"DeleteDomainName",
		"GetApiAssociation",
		"GetDomainName",
		"ListDomainNames",
		"UpdateDomainName",
		"CreateFunction",
		"DeleteFunction",
		"GetFunction",
		"ListFunctions",
		"UpdateFunction",
		"CreateType",
		"DeleteType",
		"GetType",
		"ListTypes",
		"UpdateType",
		"GetGraphqlApiEnvironmentVariables",
		"PutGraphqlApiEnvironmentVariables",
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		"EvaluateCode",
		"EvaluateMappingTemplate",
		"GetDataSourceIntrospection",
		"ListTypesByAssociation",
		"StartDataSourceIntrospection",
		"StartSchemaMerge",
		"UpdateSourceApiAssociation",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "appsync" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this AppSync instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches AppSync management API and GraphQL requests.
//
// The /v2/apis path prefix is shared with API Gateway V2. Both services use the same
// URL path but send distinct User-Agent values: the AppSync SDK includes "api/appsync/"
// while the API Gateway V2 SDK includes "api/apigatewayv2/". When the path matches
// /v2/apis, we only claim the request if the User-Agent indicates AppSync.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, appsyncV2PathPrefix) {
			ua := c.Request().Header.Get("User-Agent")

			return strings.Contains(ua, "api/appsync")
		}

		return strings.HasPrefix(path, appsyncPathPrefix) ||
			strings.HasPrefix(path, appsyncDomainPrefix) ||
			strings.HasPrefix(path, appsyncSourcePrefix) ||
			strings.HasPrefix(path, appsyncMergedPrefix) ||
			strings.HasPrefix(path, appsyncIntrospectPrefix) ||
			strings.HasPrefix(path, appsyncEvalCodePrefix) ||
			strings.HasPrefix(path, appsyncEvalTemplatePrefix) ||
			strings.HasPrefix(path, appsyncTagsPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the AppSync operation from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseOperation(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the API ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := splitPath(c.Request().URL.Path)
	// Path: /v1/apis/{apiId}/... or /v2/apis/{apiId}/...
	const apiIDIndex = 2
	if len(segs) > apiIDIndex && (segs[0] == pathSegV1 || segs[0] == pathSegV2) && segs[1] == pathSegAPIs {
		return segs[apiIDIndex]
	}

	// Path: /v1/domainnames/{domainName}/...
	if len(segs) > apiIDIndex && segs[0] == pathSegV1 && segs[1] == pathSegDomainNames {
		return segs[apiIDIndex]
	}

	return ""
}

// parseOperation derives an operation name from the HTTP method and path.
func parseOperation(method, path string) string {
	segs := splitPath(path)

	if len(segs) == 0 {
		return opUnknown
	}

	version := segs[0]

	if len(segs) < pathSegsAPIs {
		return opUnknown
	}

	switch segs[1] {
	case pathSegDomainNames:
		return parseOperationDomainNames(method, segs)
	case "sourceApis":
		return parseOperationSourceAPIs(method, segs)
	case "mergedApis":
		return parseOperationMergedAPIs(method, segs)
	case "dataSource-introspections":
		return parseOperationDataSourceIntrospections(method, segs)
	case "dataplane-evaluatecode":
		return parseOperationEvaluate(method, len(segs), "EvaluateCode")
	case "dataplane-evaluatetemplate":
		return parseOperationEvaluate(method, len(segs), "EvaluateMappingTemplate")
	case pathSegTags:
		return parseOperationTags(method, segs)
	case pathSegAPIs:
		if version == pathSegV2 {
			return parseOperationV2APIs(method, segs)
		}

		return parseOperationV1APIs(method, segs)
	}

	return opUnknown
}

func parseOperationDomainNames(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		// /v1/domainnames
		switch method {
		case http.MethodPost:
			return "CreateDomainName"
		case http.MethodGet:
			return "ListDomainNames"
		}

		return opUnknown
	case pathSegsAPIID:
		// /v1/domainnames/{domainName}
		switch method {
		case http.MethodGet:
			return "GetDomainName"
		case http.MethodDelete:
			return "DeleteDomainName"
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			return "UpdateDomainName"
		}

		return opUnknown
	case pathSegsAPISubresource:
		// /v1/domainnames/{domainName}/apiassociation
		if segs[3] == "apiassociation" {
			switch method {
			case http.MethodPost:
				return "AssociateApi"
			case http.MethodGet:
				return "GetApiAssociation"
			case http.MethodDelete:
				return "DisassociateApi"
			}
		}

		return opUnknown
	}

	return opUnknown
}

func parseOperationSourceAPIs(method string, segs []string) string {
	// /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations
	if segs[3] != "mergedApiAssociations" {
		return opUnknown
	}

	switch len(segs) {
	case pathSegsAPISubresource:
		if method == http.MethodPost {
			return "AssociateMergedGraphqlApi"
		}
	case pathSegsNamedResource:
		if method == http.MethodDelete {
			return "DisassociateMergedGraphqlApi"
		}
	}

	return opUnknown
}

func parseOperationMergedAPIs(method string, segs []string) string {
	// /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations
	if len(segs) < pathSegsAPISubresource || segs[3] != keySourceAPIAssociations {
		return opUnknown
	}

	switch len(segs) {
	case pathSegsAPISubresource:
		switch method {
		case http.MethodPost:
			return "AssociateSourceGraphqlApi"
		case http.MethodGet:
			return opListSourceAPIAssociations
		}
	case pathSegsNamedResource:
		switch method {
		case http.MethodGet:
			return "GetSourceApiAssociation"
		case http.MethodDelete:
			return "DisassociateSourceGraphqlApi"
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			return "UpdateSourceApiAssociation"
		}
	case pathSegsTypeResolvers:
		// /v1/mergedApis/{id}/sourceApiAssociations/{assocId}/types
		if segs[5] == pathSegTypes {
			return "ListTypesByAssociation"
		}
	}

	return opUnknown
}

func parseOperationDataSourceIntrospections(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		// /v1/dataSource-introspections
		if method == http.MethodPost {
			return "StartDataSourceIntrospection"
		}
	case pathSegsAPIID:
		// /v1/dataSource-introspections/{introspectionId}
		if method == http.MethodGet {
			return "GetDataSourceIntrospection"
		}
	}

	return opUnknown
}

// parseOperationEvaluate maps POST requests on the single-segment dataplane-evaluation
// paths ("/v1/dataplane-evaluatecode", "/v1/dataplane-evaluatetemplate" — each is a
// standalone top-level path, not a subresource) to their operation name.
func parseOperationEvaluate(method string, numSegs int, opName string) string {
	if numSegs != pathSegsAPIs || method != http.MethodPost {
		return opUnknown
	}

	return opName
}

// parseOperationTags maps requests on "/v1/tags/{resourceArn}". resourceArn may itself
// contain "/" (e.g. "arn:aws:appsync:region:account:apis/{apiId}"), which splitPath
// breaks into additional path segments, so any length at or beyond the ARN's first
// segment qualifies.
func parseOperationTags(method string, segs []string) string {
	if len(segs) < pathSegsAPIID {
		return opUnknown
	}

	switch method {
	case http.MethodPost:
		return opTagResource
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
}

func parseOperationV2APIs(method string, segs []string) string {
	switch len(segs) {
	case pathSegsAPIs:
		return parseOpV2APIsCollection(method)
	case pathSegsAPIID:
		return parseOpV2APIsItem(method)
	case pathSegsAPISubresource:
		return parseOpV2APIsSubresource(method, segs)
	case pathSegsNamedResource:
		return parseOpV2APIsNamedResource(method, segs)
	}

	return opUnknown
}

func parseOpV2APIsCollection(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApi"
	case http.MethodGet:
		return "ListApis"
	}

	return opUnknown
}

func parseOpV2APIsItem(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApi"
	case http.MethodDelete:
		return "DeleteApi"
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return "UpdateApi"
	}

	return opUnknown
}

func parseOpV2APIsSubresource(method string, segs []string) string {
	// /v2/apis/{apiId}/channelNamespaces
	if segs[3] != pathSegChannelNamespaces {
		return opUnknown
	}

	switch method {
	case http.MethodPost:
		return "CreateChannelNamespace"
	case http.MethodGet:
		return "ListChannelNamespaces"
	}

	return opUnknown
}

func parseOpV2APIsNamedResource(method string, segs []string) string {
	// /v2/apis/{apiId}/channelNamespaces/{name}
	if segs[3] != pathSegChannelNamespaces {
		return opUnknown
	}

	switch method {
	case http.MethodGet:
		return "GetChannelNamespace"
	case http.MethodDelete:
		return "DeleteChannelNamespace"
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return "UpdateChannelNamespace"
	}

	return opUnknown
}

func parseOperationV1APIs(method string, segs []string) string {
	// segs[0] = "v1", segs[1] = "apis", segs[2] = apiId (if present)
	switch len(segs) {
	case pathSegsAPIs:
		return parseOperationAPIs(method)
	case pathSegsAPIID:
		return parseOperationAPIID(method)
	case pathSegsAPISubresource:
		return parseOperationSub(method, segs[3])
	case pathSegsNamedResource:
		if segs[3] == pathSegAPICaches {
			return parseOperationAPICachesNamed(method, segs[4])
		}

		return parseOperationNamed(method, segs[3])
	case pathSegsTypeResolvers:
		return parseOperationTypeResolvers(method, segs[3], segs[5])
	case pathSegsResolver:
		return parseOperationResolver(method, segs[3], segs[5])
	}

	return opUnknown
}

// parseOperationAPICachesNamed maps the two 5-segment ApiCaches subpaths:
// "/v1/apis/{apiId}/ApiCaches/update" (real AWS SDK endpoint for UpdateApiCache) and
// "/v1/apis/{apiId}/ApiCaches/entries" (legacy convenience alias for FlushApiCache; the
// real SDK uses "/v1/apis/{apiId}/FlushCache" instead).
func parseOperationAPICachesNamed(method, seg4 string) string {
	switch seg4 {
	case "update":
		if method == http.MethodPost {
			return opUpdateAPICache
		}
	case "entries":
		if method == http.MethodDelete {
			return opFlushAPICache
		}
	}

	return opUnknown
}

func parseOperationAPIs(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateGraphqlApi"
	case http.MethodGet:
		return "ListGraphqlApis"
	default:
		return opUnknown
	}
}

func parseOperationAPIID(method string) string {
	switch method {
	case http.MethodDelete:
		return "DeleteGraphqlApi"
	case http.MethodPost, http.MethodPatch, http.MethodPut:
		return "UpdateGraphqlApi"
	default:
		return "GetGraphqlApi"
	}
}

// parseOpIfMethod returns matchOp if method == wantMethod, else fallbackOp. Collapses
// the common single-method-check branch shape so callers with several such cases stay
// under the cyclomatic complexity budget.
func parseOpIfMethod(method, wantMethod, matchOp, fallbackOp string) string {
	if method == wantMethod {
		return matchOp
	}

	return fallbackOp
}

func parseOperationSub(method, seg string) string {
	switch seg {
	case "schemacreation":
		return parseOpIfMethod(method, http.MethodPost, "StartSchemaCreation", "GetSchemaCreationStatus")
	case "schema":
		return "GetIntrospectionSchema"
	case pathSegDatasources:
		return parseOperationSubDatasources(method)
	case pathSegAPIKeys:
		return parseOperationSubAPIKeys(method)
	case pathSegAPICaches:
		return parseOperationSubAPICaches(method)
	case pathSegFlushCache:
		return parseOpIfMethod(method, http.MethodDelete, opFlushAPICache, opUnknown)
	case pathSegFunctions:
		return parseOperationSubFunctions(method)
	case pathSegTypes:
		return parseOperationSubTypes(method)
	case keyEnvironmentVariables:
		return parseOpIfMethod(method, http.MethodPut,
			"PutGraphqlApiEnvironmentVariables", "GetGraphqlApiEnvironmentVariables")
	case "graphql":
		return "ExecuteGraphQL"
	case "schemaMerge":
		return "StartSchemaMerge"
	case pathSegTags:
		// Legacy convenience alias: the real AWS SDK sends tag ops to
		// "/v1/tags/{resourceArn}" instead (see parseOperationTags), but this
		// apiId-scoped alias is kept working for non-SDK/manual callers.
		return parseOperationSubTags(method)
	case keySourceAPIAssociations:
		return parseOpIfMethod(method, http.MethodGet, opListSourceAPIAssociations, opUnknown)
	}

	return opUnknown
}

func parseOperationSubDatasources(method string) string {
	if method == http.MethodPost {
		return "CreateDataSource"
	}

	return "ListDataSources"
}

func parseOperationSubAPIKeys(method string) string {
	if method == http.MethodPost {
		return "CreateApiKey"
	}

	return "ListApiKeys"
}

func parseOperationSubAPICaches(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApiCache"
	case http.MethodPut:
		return opUpdateAPICache
	case http.MethodDelete:
		return "DeleteApiCache"
	}

	return "GetApiCache"
}

func parseOperationSubFunctions(method string) string {
	if method == http.MethodPost {
		return "CreateFunction"
	}

	return "ListFunctions"
}

func parseOperationSubTypes(method string) string {
	if method == http.MethodPost {
		return "CreateType"
	}

	return "ListTypes"
}

func parseOperationSubTags(method string) string {
	switch method {
	case http.MethodPost:
		return opTagResource
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
}

// parseOperationNamed maps methods on named (single-item) subresources. The real AWS
// SDK sends Update* operations as POST to the same path as Get (restjson1 uses POST,
// not PUT/PATCH, for AppSync updates); PUT/PATCH are accepted too for non-SDK callers.
func parseOperationNamed(method, seg3 string) string {
	switch seg3 {
	case pathSegDatasources:
		switch method {
		case http.MethodDelete:
			return "DeleteDataSource"
		case http.MethodPost, http.MethodPut:
			return "UpdateDataSource"
		}

		return "GetDataSource"
	case pathSegAPIKeys:
		switch method {
		case http.MethodDelete:
			return "DeleteApiKey"
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			return "UpdateApiKey"
		}

		return opUnknown
	case pathSegFunctions:
		switch method {
		case http.MethodDelete:
			return "DeleteFunction"
		case http.MethodPost, http.MethodPut:
			return "UpdateFunction"
		}

		return "GetFunction"
	case pathSegTypes:
		switch method {
		case http.MethodDelete:
			return "DeleteType"
		case http.MethodPost, http.MethodPut:
			return "UpdateType"
		}

		return "GetType"
	}

	return opUnknown
}

func parseOperationResolver(method, seg3, seg5 string) string {
	if seg3 != pathSegTypes || seg5 != pathSegResolvers {
		return opUnknown
	}

	switch method {
	case http.MethodDelete:
		return "DeleteResolver"
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return "UpdateResolver"
	}

	return "GetResolver"
}

func parseOperationTypeResolvers(method, seg3, seg5 string) string {
	if seg5 != pathSegResolvers {
		return opUnknown
	}

	if seg3 == pathSegTypes {
		if method == http.MethodPost {
			return "CreateResolver"
		}

		return "ListResolvers"
	}

	if seg3 == pathSegFunctions {
		return "ListResolversByFunction"
	}

	return opUnknown
}

// splitPath splits a URL path into non-empty segments.
func splitPath(path string) []string {
	var segs []string

	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// Handler returns the Echo handler function for AppSync requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		method := c.Request().Method
		path := c.Request().URL.Path
		segs := splitPath(path)
		log := logger.Load(ctx)

		log.DebugContext(ctx, "AppSync request", "method", method, "path", path)

		if len(segs) < pathSegsAPIs {
			return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
		}

		if handled, err := h.dispatchTopLevel(ctx, c, segs); handled {
			return err
		}

		switch {
		case segs[0] == pathSegV2 && segs[1] == pathSegAPIs:
			return h.handleV2APIs(ctx, c, segs)
		case len(segs) == pathSegsAPIs && segs[1] == pathSegAPIs:
			return h.handleAPIs(ctx, c)
		case len(segs) >= pathSegsAPIID && segs[1] == pathSegAPIs:
			return h.handleAPIResource(ctx, c, segs)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
		}
	}
}

// dispatchTopLevel handles every top-level path family keyed by segs[1] that isn't
// "/v1/apis" or "/v2/apis" (those fall through to the caller's second switch). Returns
// handled=false when segs[1] matched none of these families.
func (h *Handler) dispatchTopLevel(ctx context.Context, c *echo.Context, segs []string) (bool, error) {
	switch segs[1] {
	case pathSegDomainNames:
		return true, h.handleDomainNames(ctx, c, segs)
	case "sourceApis":
		return true, h.handleSourceAPIs(ctx, c, segs)
	case "mergedApis":
		return true, h.handleMergedAPIs(ctx, c, segs)
	case "dataSource-introspections":
		return true, h.handleDataSourceIntrospections(ctx, c, segs)
	case "dataplane-evaluations":
		// Legacy convenience alias; the real AWS SDK sends these to the two
		// standalone paths below instead (see appsyncEvalCodePrefix/
		// appsyncEvalTemplatePrefix).
		return true, h.handleDataplaneEvaluations(ctx, c, segs)
	case "dataplane-evaluatecode":
		return true, h.dispatchEvaluate(ctx, c, h.evaluateCode)
	case "dataplane-evaluatetemplate":
		return true, h.dispatchEvaluate(ctx, c, h.evaluateMappingTemplate)
	case pathSegTags:
		return true, h.handleTagsByARN(ctx, c, segs)
	}

	return false, nil
}

// dispatchEvaluate is the shared POST-only guard for the two standalone
// dataplane-evaluate* paths.
func (h *Handler) dispatchEvaluate(
	ctx context.Context, c *echo.Context, fn func(context.Context, *echo.Context) error,
) error {
	return h.requireMethod(c, http.MethodPost, func() error { return fn(ctx, c) })
}

// requireMethod runs fn if the request method matches want, else responds 405. Shared
// by single-method subresource paths (e.g. FlushCache, sourceApiAssociations) to keep
// their callers' cyclomatic complexity down.
func (h *Handler) requireMethod(c *echo.Context, want string, fn func() error) error {
	if c.Request().Method != want {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return fn()
}

// handleAPIs handles /v1/apis.
func (h *Handler) handleAPIs(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createGraphqlAPI(ctx, c)
	case http.MethodGet:
		return h.listGraphqlAPIs(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleAPIResource handles /v1/apis/{apiId}/...
func (h *Handler) handleAPIResource(ctx context.Context, c *echo.Context, segs []string) error {
	apiID := segs[2]

	if len(segs) == pathSegsAPIID {
		return h.handleAPIByID(ctx, c, apiID)
	}

	if len(segs) < pathSegsAPISubresource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	switch segs[3] {
	case "schemacreation":
		return h.handleSchemaCreation(ctx, c, apiID)
	case "schema":
		return h.getIntrospectionSchema(ctx, c, apiID)
	case pathSegDatasources:
		return h.handleDataSources(ctx, c, apiID, segs)
	case pathSegTypes:
		return h.handleTypes(ctx, c, apiID, segs)
	case "graphql":
		return h.handleGraphQL(ctx, c, apiID)
	case pathSegAPIKeys:
		return h.handleAPIKeys(ctx, c, apiID, segs)
	case pathSegAPICaches:
		return h.handleAPICaches(ctx, c, apiID, segs)
	}

	return h.handleAPIResourceExtra(ctx, c, apiID, segs)
}

// handleAPIResourceExtra continues handleAPIResource's segs[3] dispatch for the
// remaining subresources. Split out to stay under the cyclomatic complexity budget.
func (h *Handler) handleAPIResourceExtra(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	switch segs[3] {
	case pathSegFlushCache:
		// /v1/apis/{apiId}/FlushCache — the real AWS SDK endpoint for FlushApiCache.
		return h.requireMethod(c, http.MethodDelete, func() error {
			return h.flushAPICache(ctx, c, apiID)
		})
	case pathSegFunctions:
		return h.handleFunctions(ctx, c, apiID, segs)
	case pathSegTags:
		return h.handleTags(ctx, c, apiID)
	case keyEnvironmentVariables:
		return h.handleEnvironmentVariables(ctx, c, apiID)
	case "schemaMerge":
		return h.handleSchemaMerge(ctx, c, apiID)
	case keySourceAPIAssociations:
		// /v1/apis/{apiId}/sourceApiAssociations — the real AWS SDK endpoint for
		// ListSourceApiAssociations (distinct from the /v1/mergedApis/{id}/... path
		// used by Associate/Get/Update/DisassociateSourceGraphqlApi).
		return h.requireMethod(c, http.MethodGet, func() error {
			return h.listSourceAPIAssociations(ctx, c, apiID)
		})
	default:
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}
}

// handleAPIByID handles GET/DELETE/POST on /v1/apis/{apiId}.
//
// The real AWS SDK sends UpdateGraphqlApi as POST to this same path (restjson1 uses
// POST, not PUT/PATCH, for AppSync updates); PUT/PATCH are accepted too for
// non-SDK/manual callers.
func (h *Handler) handleAPIByID(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getGraphqlAPI(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteGraphqlAPI(ctx, c, apiID)
	case http.MethodPost, http.MethodPatch, http.MethodPut:
		return h.updateGraphqlAPI(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}
