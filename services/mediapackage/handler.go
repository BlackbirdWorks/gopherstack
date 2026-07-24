package mediapackage

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	pathChannels        = "/channels"
	pathOriginEndpoints = "/origin_endpoints"
	pathHarvestJobs     = "/harvest_jobs"
	pathTags            = "/tags/"

	// sigV4Service is the SigV4 signing name MediaPackage SDK clients use. The
	// "/channels" REST path is shared with IoT Analytics and MediaTailor, so we
	// disambiguate the shared path by the request's SigV4 service name.
	sigV4Service = "mediapackage"

	keyMessage = "Message"

	opCreateChannel     = "CreateChannel"
	opDescribeChannel   = "DescribeChannel"
	opUpdateChannel     = "UpdateChannel"
	opDeleteChannel     = "DeleteChannel"
	opListChannels      = "ListChannels"
	opConfigureLogs     = "ConfigureLogs"
	opRotateChannelCred = "RotateChannelCredentials"

	opCreateOriginEndpoint   = "CreateOriginEndpoint"
	opDescribeOriginEndpoint = "DescribeOriginEndpoint"
	opUpdateOriginEndpoint   = "UpdateOriginEndpoint"
	opDeleteOriginEndpoint   = "DeleteOriginEndpoint"
	opListOriginEndpoints    = "ListOriginEndpoints"

	opCreateHarvestJob   = "CreateHarvestJob"
	opDescribeHarvestJob = "DescribeHarvestJob"
	opListHarvestJobs    = "ListHarvestJobs"

	opRotateIngestEndpointCred = "RotateIngestEndpointCredentials"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"

	opUnknown = "Unknown"
)

// Handler handles MediaPackage HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaPackage" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateChannel,
		opDescribeChannel,
		opUpdateChannel,
		opDeleteChannel,
		opListChannels,
		opConfigureLogs,
		opRotateChannelCred,
		opRotateIngestEndpointCred,
		opCreateOriginEndpoint,
		opDescribeOriginEndpoint,
		opUpdateOriginEndpoint,
		opDeleteOriginEndpoint,
		opListOriginEndpoints,
		opCreateHarvestJob,
		opDescribeHarvestJob,
		opListHarvestJobs,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a function that matches MediaPackage requests by path and
// Authorization header. IoTAnalytics shares /channels paths, so we must
// distinguish by the service name embedded in the AWS Signature header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		// The "/channels" path (bare and sub-paths) is shared with IoT Analytics
		// and MediaTailor, which register matchers at the same priority. Claim it
		// only when the request is SigV4-signed for the mediapackage service so
		// routing is deterministic regardless of service registration order.
		if path == pathChannels || strings.HasPrefix(path, pathChannels+"/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == sigV4Service
		}

		pathMatch := path == pathOriginEndpoints ||
			strings.HasPrefix(path, pathOriginEndpoints+"/") ||
			path == pathHarvestJobs ||
			strings.HasPrefix(path, pathHarvestJobs+"/") ||
			isMediaPackageTagPath(path)

		return pathMatch
	}
}

// isMediaPackageTagPath reports whether path is a /tags/{arn} path for a MediaPackage ARN.
// Other services (e.g. FIS) also expose /tags/{arn} at the same path prefix; we must not
// steal their requests. MediaPackage ARNs always contain ":mediapackage:".
func isMediaPackageTagPath(path string) bool {
	arn, ok := strings.CutPrefix(path, pathTags)

	return ok && strings.Contains(arn, ":mediapackage:")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	op, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "invalid JSON body"})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	handlers := map[string]func() error{
		opCreateChannel:     func() error { return h.handleCreateChannel(c, body) },
		opDescribeChannel:   func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:     func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:     func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:      func() error { return h.handleListChannels(c) },
		opConfigureLogs:     func() error { return h.handleConfigureLogs(c, resource, body) },
		opRotateChannelCred: func() error { return h.handleRotateChannelCredentials(c, resource) },
		opRotateIngestEndpointCred: func() error {
			return h.handleRotateIngestEndpointCredentials(c, c.Request().URL.Path)
		},
		opCreateOriginEndpoint:   func() error { return h.handleCreateOriginEndpoint(c, body) },
		opDescribeOriginEndpoint: func() error { return h.handleDescribeOriginEndpoint(c, resource) },
		opUpdateOriginEndpoint:   func() error { return h.handleUpdateOriginEndpoint(c, resource, body) },
		opDeleteOriginEndpoint:   func() error { return h.handleDeleteOriginEndpoint(c, resource) },
		opListOriginEndpoints:    func() error { return h.handleListOriginEndpoints(c) },
		opCreateHarvestJob:       func() error { return h.handleCreateHarvestJob(c, body) },
		opDescribeHarvestJob:     func() error { return h.handleDescribeHarvestJob(c, resource) },
		opListHarvestJobs:        func() error { return h.handleListHarvestJobs(c) },
		opTagResource:            func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:          func() error { return h.handleUntagResource(c, resource) },
		opListTagsForResource:    func() error { return h.handleListTagsForResource(c, resource) },
	}

	if fn, ok := handlers[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
}

func classifyPath(method, path string) (string, string) {
	if op, res, ok := classifyChannelPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyOriginEndpointPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyHarvestJobPath(method, path); ok {
		return op, res
	}

	if strings.HasPrefix(path, pathTags) {
		return classifyTagPath(method, path)
	}

	return opUnknown, ""
}

func classifyChannelPath(method, path string) (string, string, bool) {
	const prefix = pathChannels + "/"

	switch {
	case path == pathChannels && method == http.MethodGet:
		return opListChannels, "", true
	case path == pathChannels && method == http.MethodPost:
		return opCreateChannel, "", true
	}

	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}

	rest := strings.TrimPrefix(path, prefix)
	id, sub, hasSub := strings.Cut(rest, "/")

	if !hasSub {
		return classifyChannelRootOp(method), id, true
	}

	return classifyChannelSubOp(method, sub), id, true
}

// classifyChannelRootOp classifies a request to the bare /channels/{id}
// path (no sub-resource) by HTTP method.
func classifyChannelRootOp(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeChannel
	case http.MethodPut:
		return opUpdateChannel
	case http.MethodDelete:
		return opDeleteChannel
	default:
		return opUnknown
	}
}

// classifyChannelSubOp classifies a request to a /channels/{id}/{sub...}
// path by sub-resource and HTTP method.
func classifyChannelSubOp(method, sub string) string {
	switch {
	case sub == "credentials" && method == http.MethodPut:
		return opRotateChannelCred
	case sub == "configure_logs" && method == http.MethodPut:
		return opConfigureLogs
	}

	// PUT /channels/{id}/ingest_endpoints/{ingestEndpointId}/credentials
	if method == http.MethodPut &&
		strings.HasPrefix(sub, "ingest_endpoints/") &&
		strings.HasSuffix(sub, "/credentials") {
		return opRotateIngestEndpointCred
	}

	return opUnknown
}

func classifyOriginEndpointPath(method, path string) (string, string, bool) {
	const prefix = pathOriginEndpoints + "/"

	switch {
	case path == pathOriginEndpoints && method == http.MethodGet:
		return opListOriginEndpoints, "", true
	case path == pathOriginEndpoints && method == http.MethodPost:
		return opCreateOriginEndpoint, "", true
	}

	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}

	rest := strings.TrimPrefix(path, prefix)
	id, _, _ := strings.Cut(rest, "/")

	switch method {
	case http.MethodGet:
		return opDescribeOriginEndpoint, id, true
	case http.MethodPut:
		return opUpdateOriginEndpoint, id, true
	case http.MethodDelete:
		return opDeleteOriginEndpoint, id, true
	}

	return opUnknown, id, true
}

func classifyHarvestJobPath(method, path string) (string, string, bool) {
	const prefix = pathHarvestJobs + "/"

	switch {
	case path == pathHarvestJobs && method == http.MethodGet:
		return opListHarvestJobs, "", true
	case path == pathHarvestJobs && method == http.MethodPost:
		return opCreateHarvestJob, "", true
	}

	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}

	id := strings.TrimPrefix(path, prefix)

	if method == http.MethodGet {
		return opDescribeHarvestJob, id, true
	}

	return opUnknown, id, true
}

func classifyTagPath(method, path string) (string, string) {
	resourceARN := strings.TrimPrefix(path, pathTags)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return opUnknown, resourceARN
}

func (h *Handler) jsonError(c *echo.Context, status int, err error) error {
	return c.JSON(status, map[string]any{keyMessage: err.Error()})
}

func (h *Handler) jsonErrorTyped(c *echo.Context, status int, errType string, err error) error {
	return c.JSON(status, map[string]any{keyMessage: err.Error(), "__type": errType})
}

func (h *Handler) mapError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		// Include __type so the AWS SDK can identify the NotFoundException
		// and terraform destroy-wait converges correctly.
		return h.jsonErrorTyped(c, http.StatusNotFound, ErrNotFound.Error(), err)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.jsonError(c, http.StatusUnprocessableEntity, err)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.jsonError(c, http.StatusUnprocessableEntity, err)
	default:
		return h.jsonError(c, http.StatusInternalServerError, err)
	}
}

// --- body helpers ---

func extractTags(body map[string]any) map[string]string {
	raw, ok := body["tags"].(map[string]any)
	if !ok {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		s, isStr := v.(string)
		if isStr {
			tags[k] = s
		}
	}

	return tags
}

func intFromBody(body map[string]any, key string) int {
	v, ok := body[key]
	if !ok {
		return -1
	}

	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}

	return -1
}

func stringsFromBody(body map[string]any, key string) []string {
	raw, ok := body[key].([]any)
	if !ok {
		return nil
	}

	result := make([]string, 0, len(raw))
	for _, v := range raw {
		s, isStr := v.(string)
		if isStr {
			result = append(result, s)
		}
	}

	return result
}

func mapFromBody(body map[string]any, key string) map[string]any {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	return raw
}

// parseMediaPkgMaxResults converts a query-parameter string to a non-negative int.
func parseMediaPkgMaxResults(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}

	return n
}
