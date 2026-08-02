package resiliencehub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const resiliencehubService = "resiliencehub"

var errUnknownPath = errors.New("unknown path")

// Handler is the HTTP handler for the AWS Resilience Hub API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Resilience Hub handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ResilienceHub" }

// GetSupportedOperations returns every operation this handler routes -- all
// 63 operations on the aws-sdk-go-v2/service/resiliencehub client, per
// sdk_completeness_test.go's empty exception list.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptResourceGroupingRecommendations",
		"AddDraftAppVersionResourceMappings",
		"BatchUpdateRecommendationStatus",
		"CreateApp",
		"CreateAppVersionAppComponent",
		"CreateAppVersionResource",
		"CreateRecommendationTemplate",
		"CreateResiliencyPolicy",
		"DeleteApp",
		"DeleteAppAssessment",
		"DeleteAppInputSource",
		"DeleteAppVersionAppComponent",
		"DeleteAppVersionResource",
		"DeleteRecommendationTemplate",
		"DeleteResiliencyPolicy",
		"DescribeApp",
		"DescribeAppAssessment",
		"DescribeAppVersion",
		"DescribeAppVersionAppComponent",
		"DescribeAppVersionResource",
		"DescribeAppVersionResourcesResolutionStatus",
		"DescribeAppVersionTemplate",
		"DescribeDraftAppVersionResourcesImportStatus",
		"DescribeMetricsExport",
		"DescribeResiliencyPolicy",
		"DescribeResourceGroupingRecommendationTask",
		"ImportResourcesToDraftAppVersion",
		"ListAlarmRecommendations",
		"ListAppAssessmentComplianceDrifts",
		"ListAppAssessmentResourceDrifts",
		"ListAppAssessments",
		"ListAppComponentCompliances",
		"ListAppComponentRecommendations",
		"ListAppInputSources",
		"ListAppVersionAppComponents",
		"ListAppVersionResourceMappings",
		"ListAppVersionResources",
		"ListAppVersions",
		"ListApps",
		"ListMetrics",
		"ListRecommendationTemplates",
		"ListResiliencyPolicies",
		"ListResourceGroupingRecommendations",
		"ListSopRecommendations",
		"ListSuggestedResiliencyPolicies",
		"ListTagsForResource",
		"ListTestRecommendations",
		"ListUnsupportedAppVersionResources",
		"PublishAppVersion",
		"PutDraftAppVersionTemplate",
		"RejectResourceGroupingRecommendations",
		"RemoveDraftAppVersionResourceMappings",
		"ResolveAppVersionResources",
		"StartAppAssessment",
		"StartMetricsExport",
		"StartResourceGroupingRecommendationTask",
		"TagResource",
		"UntagResource",
		"UpdateApp",
		"UpdateAppVersion",
		"UpdateAppVersionAppComponent",
		"UpdateAppVersionResource",
		"UpdateResiliencyPolicy",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return resiliencehubService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a matcher that reports true iff the request's method
// plus first path segment (e.g. "create-app" for POST /create-app, or
// "tags" for the /tags/{resourceArn} trio) is a key in routes(). Every
// operation but the tags trio is a fixed, literal, kebab-case action path
// with NO path parameters at all (confirmed by reading every op's own
// httpbinding.SplitURI(...) literal in the SDK's serializers.go -- see
// PARITY.md's routing section).
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		segs := rawPathSegments(c.Request())
		if len(segs) == 0 {
			return false
		}

		_, ok := h.routes()[routeKey(c.Request().Method, segs)]

		return ok
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	entry, _ := h.dispatch(c.Request())

	return entry.op
}

// ExtractResource extracts the primary resource identifier from the
// request -- the ARN in the /tags/{resourceArn} path, or empty for every
// other (body-addressed) operation.
func (h *Handler) ExtractResource(c *echo.Context) string {
	const tagsPathSegs = 2

	segs := rawPathSegments(c.Request())
	if len(segs) >= tagsPathSegs && segs[0] == "tags" {
		return segs[1]
	}

	return ""
}

type dispatchFunc func(ctx context.Context, r *http.Request, body []byte) ([]byte, error)

type routeEntry struct {
	fn dispatchFunc
	op string
}

// routeKey builds the flat lookup key routes() is keyed by: the HTTP method
// plus the raw first path segment (fixed, literal, kebab-case action name --
// see RouteMatcher's doc comment), except the tags trio which is keyed by
// method + "tags" regardless of the ARN segment that follows.
func routeKey(method string, segs []string) string {
	return method + " " + segs[0]
}

// Handler returns the Echo handler function for Resilience Hub requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "resiliencehub: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		entry, ok := h.dispatch(c.Request())
		if !ok {
			return h.handleError(c, fmt.Errorf("%w: %s %s", errUnknownPath, c.Request().Method, c.Request().URL.Path))
		}

		result, dispErr := entry.fn(ctx, c.Request(), body)
		if dispErr != nil {
			log.ErrorContext(ctx, "resiliencehub: operation failed", "op", entry.op, "error", dispErr)

			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

// dispatch resolves r to its routeEntry via the flat routes() table.
func (h *Handler) dispatch(r *http.Request) (routeEntry, bool) {
	segs := rawPathSegments(r)
	if len(segs) == 0 {
		return routeEntry{}, false
	}

	entry, ok := h.routes()[routeKey(r.Method, segs)]

	return entry, ok
}

// handleError renders err as one of Resilience Hub's restjson1 error
// envelopes: {"message": "..."} with an X-Amzn-Errortype header, plus
// ResourceId/ResourceType when the error carries them (ConflictException and
// ResourceNotFoundException both do -- confirmed from types/errors.go,
// unlike services/outposts' NotFoundException which carries neither).
func (h *Handler) handleError(c *echo.Context, err error) error {
	status := http.StatusInternalServerError
	errType := "InternalServerException"

	var apiErr *apiError

	body := map[string]any{"message": err.Error()}

	switch {
	case errors.As(err, &apiErr) && errors.Is(err, errNotFoundSentinel):
		status, errType = http.StatusNotFound, "ResourceNotFoundException"
		addResourceFields(body, apiErr)
	case errors.As(err, &apiErr) && errors.Is(err, errConflictSentinel):
		status, errType = http.StatusConflict, "ConflictException"
		addResourceFields(body, apiErr)
	case errors.Is(err, errValidationSentinel):
		status, errType = http.StatusBadRequest, "ValidationException"
	case errors.Is(err, errQuotaExceeded):
		status, errType = http.StatusPaymentRequired, "ServiceQuotaExceededException"
	case errors.Is(err, errAccessDenied):
		status, errType = http.StatusForbidden, "AccessDeniedException"
	case errors.Is(err, errUnknownPath):
		status, errType = http.StatusNotFound, "ResourceNotFoundException"
	}

	payload, _ := json.Marshal(body)

	c.Response().Header().Set("X-Amzn-Errortype", errType)

	return c.JSONBlob(status, payload)
}

func addResourceFields(body map[string]any, apiErr *apiError) {
	if apiErr == nil {
		return
	}

	if apiErr.resourceID != "" {
		body["resourceId"] = apiErr.resourceID
	}

	if apiErr.resourceType != "" {
		body["resourceType"] = apiErr.resourceType
	}
}

// rawPathSegments splits the raw (or decoded) URL path into non-empty
// segments, URL-decoding each segment individually so that an encoded slash
// in the /tags/{resourceArn} path parameter is preserved as a single segment
// -- matches services/outposts and services/grafana's helper of the same
// name (the house fix for the ARN-in-path route-matching trap).
func rawPathSegments(r *http.Request) []string {
	rawPath := r.URL.RawPath
	if rawPath == "" {
		rawPath = r.URL.Path
	}

	rawPath = strings.TrimPrefix(rawPath, "/")
	parts := strings.Split(rawPath, "/")

	segments := make([]string, 0, len(parts))

	for _, p := range parts {
		if p == "" {
			continue
		}

		decoded, err := url.PathUnescape(p)
		if err != nil {
			decoded = p
		}

		segments = append(segments, decoded)
	}

	return segments
}

// queryMaxResults parses the "maxResults" query parameter (lowerCamel --
// this service's convention throughout, unlike services/outposts'
// PascalCase).
func queryMaxResults(q url.Values) int {
	raw := q.Get("maxResults")
	if raw == "" {
		return 0
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}

	return n
}

// decodeJSONBody unmarshals body into v, treating an empty body as a no-op
// (leaving v at its zero value) rather than a JSON error -- several POST
// operations in this service (e.g. StartResourceGroupingRecommendationTask
// with only an appArn) have minimal or optional bodies.
func decodeJSONBody(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return validationError("malformed request body: " + err.Error())
	}

	return nil
}

// marshalResponse marshals v, wrapping any error as an internal failure
// (marshaling our own well-typed wire structs should never fail).
func marshalResponse(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("resiliencehub: marshal response: %w", err)
	}

	return data, nil
}
