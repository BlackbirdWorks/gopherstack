package mediapackage

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
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
		opCreateChannel:            func() error { return h.handleCreateChannel(c, body) },
		opDescribeChannel:          func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:            func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:            func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:             func() error { return h.handleListChannels(c) },
		opConfigureLogs:            func() error { return h.handleConfigureLogs(c, resource, body) },
		opRotateChannelCred:        func() error { return h.handleRotateChannelCredentials(c, resource) },
		opRotateIngestEndpointCred: func() error { return h.handleRotateIngestEndpointCredentials(c, c.Request().URL.Path) },
		opCreateOriginEndpoint:     func() error { return h.handleCreateOriginEndpoint(c, body) },
		opDescribeOriginEndpoint:   func() error { return h.handleDescribeOriginEndpoint(c, resource) },
		opUpdateOriginEndpoint:     func() error { return h.handleUpdateOriginEndpoint(c, resource, body) },
		opDeleteOriginEndpoint:     func() error { return h.handleDeleteOriginEndpoint(c, resource) },
		opListOriginEndpoints:      func() error { return h.handleListOriginEndpoints(c) },
		opCreateHarvestJob:         func() error { return h.handleCreateHarvestJob(c, body) },
		opDescribeHarvestJob:       func() error { return h.handleDescribeHarvestJob(c, resource) },
		opListHarvestJobs:          func() error { return h.handleListHarvestJobs(c) },
		opTagResource:              func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:            func() error { return h.handleUntagResource(c, resource) },
		opListTagsForResource:      func() error { return h.handleListTagsForResource(c, resource) },
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

func classifyChannelPath(method, path string) (string, string, bool) { //nolint:cyclop // existing issue.
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
		switch method {
		case http.MethodGet:
			return opDescribeChannel, id, true
		case http.MethodPut:
			return opUpdateChannel, id, true
		case http.MethodDelete:
			return opDeleteChannel, id, true
		}

		return opUnknown, id, true
	}

	switch {
	case sub == "ingest_endpoints/credentials" && method == http.MethodPost:
		return opRotateChannelCred, id, true
	case sub == "configure_logs" && method == http.MethodPut:
		return opConfigureLogs, id, true
	}

	// PUT /channels/{id}/ingest_endpoints/{ingestEndpointId}/credentials
	if method == http.MethodPut &&
		strings.HasPrefix(sub, "ingest_endpoints/") &&
		strings.HasSuffix(sub, "/credentials") {
		return opRotateIngestEndpointCred, id, true
	}

	return opUnknown, id, true
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

func (h *Handler) mapError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.jsonError(c, http.StatusNotFound, err)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.jsonError(c, http.StatusUnprocessableEntity, err)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.jsonError(c, http.StatusUnprocessableEntity, err)
	default:
		return h.jsonError(c, http.StatusInternalServerError, err)
	}
}

// --- channel output helpers ---

type ingestEndpointOutput struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type hlsIngestOutput struct {
	IngestEndpoints []ingestEndpointOutput `json:"ingestEndpoints"`
}

type channelOutput struct {
	Tags        map[string]any  `json:"tags"`
	Arn         string          `json:"arn"`
	ID          string          `json:"id"`
	Description string          `json:"description"`
	HlsIngest   hlsIngestOutput `json:"hlsIngest"`
}

func toChannelOutput(ch *Channel) channelOutput {
	endpoints := make([]ingestEndpointOutput, 0, len(ch.HlsIngest.IngestEndpoints))
	for _, ep := range ch.HlsIngest.IngestEndpoints {
		endpoints = append(endpoints, ingestEndpointOutput{
			ID:       ep.ID,
			URL:      ep.URL,
			Username: ep.Username,
			Password: ep.Password,
		})
	}

	tags := make(map[string]any, len(ch.Tags))
	for k, v := range ch.Tags {
		tags[k] = v
	}

	return channelOutput{
		Arn:         ch.ARN,
		ID:          ch.ID,
		Description: ch.Description,
		HlsIngest:   hlsIngestOutput{IngestEndpoints: endpoints},
		Tags:        tags,
	}
}

// --- origin endpoint output helper ---

type originEndpointOutput struct {
	Tags                   map[string]any `json:"tags"`
	Arn                    string         `json:"arn"`
	ChannelID              string         `json:"channelId"`
	ID                     string         `json:"id"`
	Description            string         `json:"description"`
	ManifestName           string         `json:"manifestName"`
	URL                    string         `json:"url"`
	Origination            string         `json:"origination"`
	Whitelist              []string       `json:"whitelist"`
	StartoverWindowSeconds int            `json:"startoverWindowSeconds"`
	TimeDelaySeconds       int            `json:"timeDelaySeconds"`
}

func toOriginEndpointOutput(ep *OriginEndpoint) originEndpointOutput {
	tags := make(map[string]any, len(ep.Tags))
	for k, v := range ep.Tags {
		tags[k] = v
	}

	whitelist := ep.Whitelist
	if whitelist == nil {
		whitelist = []string{}
	}

	return originEndpointOutput{
		Arn:                    ep.ARN,
		ChannelID:              ep.ChannelID,
		ID:                     ep.ID,
		Description:            ep.Description,
		ManifestName:           ep.ManifestName,
		URL:                    ep.URL,
		Origination:            ep.Origination,
		StartoverWindowSeconds: ep.StartoverWindowSeconds,
		TimeDelaySeconds:       ep.TimeDelaySeconds,
		Whitelist:              whitelist,
		Tags:                   tags,
	}
}

// --- channel handlers ---

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	id, _ := body["id"].(string)
	description, _ := body["description"].(string)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(id, description, tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toChannelOutput(ch))
}

func (h *Handler) handleDescribeChannel(c *echo.Context, id string) error {
	ch, err := h.Backend.DescribeChannel(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(c *echo.Context, id string, body map[string]any) error {
	description, _ := body["description"].(string)

	ch, err := h.Backend.UpdateChannel(id, description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDeleteChannel(c *echo.Context, id string) error {
	_, err := h.Backend.DeleteChannel(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	channels, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]channelOutput, 0, len(channels))
	for _, ch := range channels {
		out = append(out, toChannelOutput(ch))
	}

	resp := map[string]any{"channels": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleConfigureLogs(c *echo.Context, id string, body map[string]any) error {
	var egressLogGroup, ingressLogGroup string

	if egress, ok := body["egressAccessLogs"].(map[string]any); ok {
		egressLogGroup, _ = egress["logGroupName"].(string)
	}

	if ingress, ok := body["ingressAccessLogs"].(map[string]any); ok {
		ingressLogGroup, _ = ingress["logGroupName"].(string)
	}

	ch, err := h.Backend.ConfigureLogs(id, egressLogGroup, ingressLogGroup)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleRotateChannelCredentials(c *echo.Context, id string) error {
	ch, err := h.Backend.RotateChannelCredentials(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// --- origin endpoint handlers ---

func (h *Handler) handleCreateOriginEndpoint(c *echo.Context, body map[string]any) error {
	channelID, _ := body["channelId"].(string)
	id, _ := body["id"].(string)
	description, _ := body["description"].(string)
	manifestName, _ := body["manifestName"].(string)
	origination, _ := body["origination"].(string)
	startover := intFromBody(body, "startoverWindowSeconds")
	timeDelay := intFromBody(body, "timeDelaySeconds")
	whitelist := stringsFromBody(body, "whitelist")
	tags := extractTags(body)

	ep, err := h.Backend.CreateOriginEndpoint(
		channelID,
		id,
		description,
		manifestName,
		startover,
		timeDelay,
		origination,
		whitelist,
		tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toOriginEndpointOutput(ep))
}

func (h *Handler) handleDescribeOriginEndpoint(c *echo.Context, id string) error {
	ep, err := h.Backend.DescribeOriginEndpoint(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toOriginEndpointOutput(ep))
}

func (h *Handler) handleUpdateOriginEndpoint(c *echo.Context, id string, body map[string]any) error {
	description, _ := body["description"].(string)
	manifestName, _ := body["manifestName"].(string)
	origination, _ := body["origination"].(string)
	startover := intFromBody(body, "startoverWindowSeconds")
	timeDelay := intFromBody(body, "timeDelaySeconds")
	whitelist := stringsFromBody(body, "whitelist")

	ep, err := h.Backend.UpdateOriginEndpoint(
		id,
		description,
		manifestName,
		startover,
		timeDelay,
		origination,
		whitelist,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toOriginEndpointOutput(ep))
}

func (h *Handler) handleDeleteOriginEndpoint(c *echo.Context, id string) error {
	_, err := h.Backend.DeleteOriginEndpoint(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListOriginEndpoints(c *echo.Context) error {
	channelID := c.QueryParam("channelId")

	endpoints, nextToken, err := h.Backend.ListOriginEndpoints(channelID, 0, "")
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]originEndpointOutput, 0, len(endpoints))
	for _, ep := range endpoints {
		out = append(out, toOriginEndpointOutput(ep))
	}

	resp := map[string]any{"originEndpoints": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- tag handlers ---

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.QueryParams()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.mapError(c, err)
	}

	// Sort for stable output.
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make(map[string]any, len(tags))
	for _, k := range keys {
		out[k] = tags[k]
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": out})
}

// --- harvest job handlers ---

type s3DestinationOutput struct {
	BucketName  string `json:"bucketName"`
	ManifestKey string `json:"manifestKey"`
	RoleArn     string `json:"roleArn"`
}

type harvestJobOutput struct {
	S3Destination    *s3DestinationOutput `json:"s3Destination"`
	Arn              string               `json:"arn"`
	ChannelID        string               `json:"channelId"`
	CreatedAt        string               `json:"createdAt"`
	EndTime          string               `json:"endTime"`
	ID               string               `json:"id"`
	OriginEndpointID string               `json:"originEndpointId"`
	StartTime        string               `json:"startTime"`
	Status           string               `json:"status"`
}

func toHarvestJobOutput(j *HarvestJob) harvestJobOutput {
	out := harvestJobOutput{
		Arn:              j.ARN,
		ChannelID:        j.ChannelID,
		CreatedAt:        j.CreatedAt,
		EndTime:          j.EndTime,
		ID:               j.ID,
		OriginEndpointID: j.OriginEndpointID,
		StartTime:        j.StartTime,
		Status:           j.Status,
	}

	if j.S3Destination != nil {
		out.S3Destination = &s3DestinationOutput{
			BucketName:  j.S3Destination.BucketName,
			ManifestKey: j.S3Destination.ManifestKey,
			RoleArn:     j.S3Destination.RoleArn,
		}
	}

	return out
}

func (h *Handler) handleCreateHarvestJob(c *echo.Context, body map[string]any) error {
	id, _ := body["id"].(string)
	originEndpointID, _ := body["originEndpointId"].(string)
	startTime, _ := body["startTime"].(string)
	endTime, _ := body["endTime"].(string)

	var s3Dest S3Destination

	if raw, ok := body["s3Destination"].(map[string]any); ok {
		s3Dest.BucketName, _ = raw["bucketName"].(string)
		s3Dest.ManifestKey, _ = raw["manifestKey"].(string)
		s3Dest.RoleArn, _ = raw["roleArn"].(string)
	}

	job, err := h.Backend.CreateHarvestJob(id, originEndpointID, startTime, endTime, s3Dest)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toHarvestJobOutput(job))
}

func (h *Handler) handleDescribeHarvestJob(c *echo.Context, id string) error {
	job, err := h.Backend.DescribeHarvestJob(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toHarvestJobOutput(job))
}

func (h *Handler) handleListHarvestJobs(c *echo.Context) error {
	includeChannelID := c.QueryParam("includeChannelId")
	includeStatus := c.QueryParam("includeStatus")

	jobs, nextToken, err := h.Backend.ListHarvestJobs(includeChannelID, includeStatus, 0, "")
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]harvestJobOutput, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toHarvestJobOutput(j))
	}

	resp := map[string]any{"harvestJobs": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleRotateIngestEndpointCredentials(c *echo.Context, path string) error {
	// path: /channels/{channelId}/ingest_endpoints/{ingestEndpointId}/credentials
	rest := strings.TrimPrefix(path, pathChannels+"/")
	channelID, sub, _ := strings.Cut(rest, "/")
	// sub: ingest_endpoints/{ingestEndpointId}/credentials
	sub = strings.TrimPrefix(sub, "ingest_endpoints/")
	ingestEndpointID := strings.TrimSuffix(sub, "/credentials")

	ch, err := h.Backend.RotateIngestEndpointCredentials(channelID, ingestEndpointID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
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
