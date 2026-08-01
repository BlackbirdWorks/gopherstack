package outposts

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

const outpostsService = "outposts"

var errUnknownPath = errors.New("unknown path")

// Handler is the HTTP handler for the AWS Outposts API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Outposts handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Outposts" }

// GetSupportedOperations returns every operation this handler routes -- all
// 43 operations on the aws-sdk-go-v2/service/outposts client, per
// sdk_completeness_test.go's empty exception list.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CancelCapacityTask",
		"CancelOrder",
		"CreateOrder",
		"CreateOutpost",
		"CreateQuote",
		"CreateRenewal",
		"CreateSite",
		"DeleteOutpost",
		"DeleteQuote",
		"DeleteSite",
		"GetCapacityTask",
		"GetCatalogItem",
		"GetConnection",
		"GetOrder",
		"GetOutpost",
		"GetOutpostBillingInformation",
		"GetOutpostInstanceTypes",
		"GetOutpostSupportedInstanceTypes",
		"GetQuote",
		"GetRenewalPricing",
		"GetSite",
		"GetSiteAddress",
		"ListAssetInstances",
		"ListAssets",
		"ListBlockingInstancesForCapacityTask",
		"ListCapacityTasks",
		"ListCatalogItems",
		"ListOrderableInstanceTypes",
		"ListOrders",
		"ListOutposts",
		"ListQuotes",
		"ListSites",
		"ListTagsForResource",
		"StartCapacityTask",
		"StartConnection",
		"StartOutpostDecommission",
		"TagResource",
		"UntagResource",
		"UpdateOutpost",
		"UpdateQuote",
		"UpdateSite",
		"UpdateSiteAddress",
		"UpdateSiteRackPhysicalProperties",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return outpostsService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Outposts API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, prefix := range []string{
			"/outposts", "/outpost/", "/orders", "/list-orders", "/quotes",
			"/renewals", "/sites", "/catalog/", "/instanceTypes", "/connections",
			"/capacity/", "/tags/",
		} {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.routeRequest(c.Request())

	return op
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := rawPathSegments(c.Request())
	if len(segs) < 2 { //nolint:mnd // need at least a top-level segment + identifier
		return ""
	}

	return segs[1]
}

type dispatchFunc func(ctx context.Context, r *http.Request, body []byte) ([]byte, error)

// Handler returns the Echo handler function for Outposts requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "outposts: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op, dispatchFn := h.routeRequest(c.Request())
		if dispatchFn == nil {
			return h.handleError(c, fmt.Errorf("%w: %s %s", errUnknownPath, c.Request().Method, c.Request().URL.Path))
		}

		result, dispErr := dispatchFn(ctx, c.Request(), body)
		if dispErr != nil {
			log.ErrorContext(ctx, "outposts: operation failed", "op", op, "error", dispErr)

			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

// topLevelRouteFunc routes every request under one first path segment (e.g.
// every /outposts... request).
type topLevelRouteFunc func(segs []string, method string) (string, dispatchFunc)

// topLevelRouters maps a request's first path segment to the function that
// routes everything under it. A map keeps routeRequest itself a flat lookup
// (cyclomatic complexity 2) instead of one large switch over all 12
// top-level segments -- see PARITY.md for the full method+path verification
// trail behind every one of these routes.
func (h *Handler) topLevelRouters() map[string]topLevelRouteFunc {
	return map[string]topLevelRouteFunc{
		segOutposts:     h.routeOutposts,
		segOutpost:      h.routeOutpostSingular,
		segOrders:       h.routeOrders,
		"list-orders":   h.routeListOrdersTop,
		segQuotes:       h.routeQuotes,
		"renewals":      h.routeRenewalsTop,
		segSites:        h.routeSites,
		segCatalog:      h.routeCatalog,
		"instanceTypes": h.routeInstanceTypesTop,
		segConnections:  h.routeConnections,
		segCapacity:     h.routeCapacityTop,
		segTags:         h.routeTags,
	}
}

// routeRequest maps HTTP method + path to operation name and dispatch
// function.
func (h *Handler) routeRequest(r *http.Request) (string, dispatchFunc) {
	segs := rawPathSegments(r)
	if len(segs) == 0 {
		return "", nil
	}

	fn, ok := h.topLevelRouters()[segs[0]]
	if !ok {
		return "", nil
	}

	return fn(segs, r.Method)
}

func (h *Handler) routeListOrdersTop(_ []string, method string) (string, dispatchFunc) {
	if method == http.MethodGet {
		return "ListOrders", h.handleListOrders
	}

	return "", nil
}

func (h *Handler) routeRenewalsTop(_ []string, method string) (string, dispatchFunc) {
	if method == http.MethodPost {
		return "CreateRenewal", h.handleCreateRenewal
	}

	return "", nil
}

func (h *Handler) routeInstanceTypesTop(_ []string, method string) (string, dispatchFunc) {
	if method == http.MethodGet {
		return "ListOrderableInstanceTypes", h.handleListOrderableInstanceTypes
	}

	return "", nil
}

const (
	twoSegs   = 2
	threeSegs = 3
	fourSegs  = 4
	fiveSegs  = 5
)

// routeOutposts routes every /outposts... path.
func (h *Handler) routeOutposts(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		return h.routeOutpostsRoot(method)
	case twoSegs:
		return h.routeOutpostByID(method)
	case threeSegs:
		return h.routeOutpostSubResource(segs[2], method)
	case fourSegs:
		if segs[2] == segCapacity {
			return h.routeCapacityTaskByID(method)
		}
	case fiveSegs:
		if segs[2] == segCapacity && segs[4] == "blockingInstances" && method == http.MethodGet {
			return "ListBlockingInstancesForCapacityTask", h.handleListBlockingInstancesForCapacityTask
		}
	}

	return "", nil
}

func (h *Handler) routeOutpostsRoot(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodPost:
		return "CreateOutpost", h.handleCreateOutpost
	case http.MethodGet:
		return "ListOutposts", h.handleListOutposts
	}

	return "", nil
}

func (h *Handler) routeOutpostByID(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "GetOutpost", h.handleGetOutpost
	case http.MethodDelete:
		return "DeleteOutpost", h.handleDeleteOutpost
	case http.MethodPatch:
		return "UpdateOutpost", h.handleUpdateOutpost
	}

	return "", nil
}

func (h *Handler) routeOutpostSubResource(sub, method string) (string, dispatchFunc) {
	switch sub {
	case "instanceTypes":
		if method == http.MethodGet {
			return "GetOutpostInstanceTypes", h.handleGetOutpostInstanceTypes
		}
	case "supportedInstanceTypes":
		if method == http.MethodGet {
			return "GetOutpostSupportedInstanceTypes", h.handleGetOutpostSupportedInstanceTypes
		}
	case "decommission":
		if method == http.MethodPost {
			return "StartOutpostDecommission", h.handleStartOutpostDecommission
		}
	case "assets":
		if method == http.MethodGet {
			return "ListAssets", h.handleListAssets
		}
	case "assetInstances":
		if method == http.MethodGet {
			return "ListAssetInstances", h.handleListAssetInstances
		}
	case segCapacity:
		if method == http.MethodPost {
			return "StartCapacityTask", h.handleStartCapacityTask
		}
	}

	return "", nil
}

func (h *Handler) routeCapacityTaskByID(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodPost:
		return "CancelCapacityTask", h.handleCancelCapacityTask
	case http.MethodGet:
		return "GetCapacityTask", h.handleGetCapacityTask
	}

	return "", nil
}

// routeOutpostSingular routes /outpost/... (the two singular-path ops).
func (h *Handler) routeOutpostSingular(segs []string, method string) (string, dispatchFunc) {
	if len(segs) != threeSegs || method != http.MethodGet {
		return "", nil
	}

	switch segs[2] {
	case "billing-information":
		return "GetOutpostBillingInformation", h.handleGetOutpostBillingInformation
	case "renewal-pricing":
		return "GetRenewalPricing", h.handleGetRenewalPricing
	}

	return "", nil
}

// routeOrders routes every /orders... path.
func (h *Handler) routeOrders(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		if method == http.MethodPost {
			return "CreateOrder", h.handleCreateOrder
		}
	case twoSegs:
		if method == http.MethodGet {
			return "GetOrder", h.handleGetOrder
		}
	case threeSegs:
		if segs[2] == "cancel" && method == http.MethodPost {
			return "CancelOrder", h.handleCancelOrder
		}
	}

	return "", nil
}

// routeQuotes routes every /quotes... path.
func (h *Handler) routeQuotes(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		switch method {
		case http.MethodPost:
			return "CreateQuote", h.handleCreateQuote
		case http.MethodGet:
			return "ListQuotes", h.handleListQuotes
		}
	case twoSegs:
		switch method {
		case http.MethodGet:
			return "GetQuote", h.handleGetQuote
		case http.MethodDelete:
			return "DeleteQuote", h.handleDeleteQuote
		case http.MethodPatch:
			return "UpdateQuote", h.handleUpdateQuote
		}
	}

	return "", nil
}

// routeSites routes every /sites... path.
func (h *Handler) routeSites(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		return h.routeSitesRoot(method)
	case twoSegs:
		return h.routeSiteByID(method)
	case threeSegs:
		return h.routeSiteSubResource(segs[2], method)
	}

	return "", nil
}

func (h *Handler) routeSitesRoot(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodPost:
		return "CreateSite", h.handleCreateSite
	case http.MethodGet:
		return "ListSites", h.handleListSites
	}

	return "", nil
}

func (h *Handler) routeSiteByID(method string) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "GetSite", h.handleGetSite
	case http.MethodPatch:
		return "UpdateSite", h.handleUpdateSite
	case http.MethodDelete:
		return "DeleteSite", h.handleDeleteSite
	}

	return "", nil
}

func (h *Handler) routeSiteSubResource(sub, method string) (string, dispatchFunc) {
	switch sub {
	case "address":
		switch method {
		case http.MethodGet:
			return "GetSiteAddress", h.handleGetSiteAddress
		case http.MethodPut:
			return "UpdateSiteAddress", h.handleUpdateSiteAddress
		}
	case "rackPhysicalProperties":
		if method == http.MethodPatch {
			return "UpdateSiteRackPhysicalProperties", h.handleUpdateSiteRackPhysicalProperties
		}
	}

	return "", nil
}

// routeCatalog routes /catalog/item/{id} and /catalog/items.
func (h *Handler) routeCatalog(segs []string, method string) (string, dispatchFunc) {
	if method != http.MethodGet {
		return "", nil
	}

	switch {
	case len(segs) == threeSegs && segs[1] == "item":
		return "GetCatalogItem", h.handleGetCatalogItem
	case len(segs) == twoSegs && segs[1] == "items":
		return "ListCatalogItems", h.handleListCatalogItems
	}

	return "", nil
}

// routeConnections routes /connections and /connections/{id}.
func (h *Handler) routeConnections(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		if method == http.MethodPost {
			return "StartConnection", h.handleStartConnection
		}
	case twoSegs:
		if method == http.MethodGet {
			return "GetConnection", h.handleGetConnection
		}
	}

	return "", nil
}

// routeCapacityTop routes /capacity/tasks (the top-level ListCapacityTasks
// collection, distinct from /outposts/{id}/capacity/...).
func (h *Handler) routeCapacityTop(segs []string, method string) (string, dispatchFunc) {
	if len(segs) == twoSegs && segs[1] == "tasks" && method == http.MethodGet {
		return "ListCapacityTasks", h.handleListCapacityTasks
	}

	return "", nil
}

// routeTags routes /tags/{resourceArn}.
func (h *Handler) routeTags(segs []string, method string) (string, dispatchFunc) {
	if len(segs) < twoSegs {
		return "", nil
	}

	switch method {
	case http.MethodGet:
		return "ListTagsForResource", h.handleListTagsForResource
	case http.MethodPost:
		return "TagResource", h.handleTagResource
	case http.MethodDelete:
		return "UntagResource", h.handleUntagResource
	}

	return "", nil
}

// handleError renders err as one of Outposts' restjson1 error envelopes:
// {"message": "..."} with an X-Amzn-Errortype header, plus ResourceId/
// ResourceType when the error carries them (ConflictException only -- see
// errors.go's apiError).
func (h *Handler) handleError(c *echo.Context, err error) error {
	status := http.StatusInternalServerError
	errType := "InternalServerException"

	var apiErr *apiError

	body := map[string]any{"message": err.Error()}

	switch {
	case errors.As(err, &apiErr) && errors.Is(err, errNotFoundSentinel):
		status, errType = http.StatusNotFound, "NotFoundException"
	case errors.As(err, &apiErr) && errors.Is(err, errConflictSentinel):
		status, errType = http.StatusConflict, "ConflictException"
		addResourceFields(body, apiErr)
	case errors.Is(err, errValidationSentinel):
		status, errType = http.StatusBadRequest, "ValidationException"
	case errors.Is(err, errQuotaExceeded):
		status, errType = http.StatusPaymentRequired, "ServiceQuotaExceededException"
	case errors.Is(err, errUnknownPath):
		status, errType = http.StatusNotFound, "NotFoundException"
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
		body["ResourceId"] = apiErr.resourceID
	}

	if apiErr.resourceType != "" {
		body["ResourceType"] = apiErr.resourceType
	}
}

// rawPathSegments splits the raw (or decoded) URL path into non-empty
// segments, URL-decoding each segment individually so that encoded slashes
// in path params (e.g. the ARN in /tags/{resourceArn}) are preserved as a
// single segment -- matches services/grafana and services/s3tables's helper
// of the same name (the house fix for the ARN-in-path route-matching trap).
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

// queryMaxResults parses the "MaxResults" query parameter (PascalCase --
// see wire.go's doc comment on this service's query-string casing).
func queryMaxResults(q url.Values) int {
	raw := q.Get("MaxResults")
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
// operations in this service (e.g. CancelOrder) have no request body at
// all.
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
		return nil, fmt.Errorf("outposts: marshal response: %w", err)
	}

	return data, nil
}
