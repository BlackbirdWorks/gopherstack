package iotanalytics

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opBatchPutMessage            = "BatchPutMessage"
	opCancelPipelineReprocessing = "CancelPipelineReprocessing"
	opCreateChannel              = "CreateChannel"
	opCreateDataset              = "CreateDataset"
	opCreateDatasetContent       = "CreateDatasetContent"
	opCreateDatastore            = "CreateDatastore"
	opCreatePipeline             = "CreatePipeline"
	opDeleteChannel              = "DeleteChannel"
	opDeleteDataset              = "DeleteDataset"
	opDeleteDatasetContent       = "DeleteDatasetContent"
	opDeleteDatastore            = "DeleteDatastore"
	opDeletePipeline             = "DeletePipeline"
	opDescribeChannel            = "DescribeChannel"
	opDescribeDataset            = "DescribeDataset"
	opDescribeDatastore          = "DescribeDatastore"
	opDescribeLoggingOptions     = "DescribeLoggingOptions"
	opDescribePipeline           = "DescribePipeline"
	opGetDatasetContent          = "GetDatasetContent"
	opListChannels               = "ListChannels"
	opListDatasetContents        = "ListDatasetContents"
	opListDatasets               = "ListDatasets"
	opListDatastores             = "ListDatastores"
	opListPipelines              = "ListPipelines"
	opListTagsForResource        = "ListTagsForResource"
	opPutLoggingOptions          = "PutLoggingOptions"
	opRunPipelineActivity        = "RunPipelineActivity"
	opSampleChannelData          = "SampleChannelData"
	opStartPipelineReprocessing  = "StartPipelineReprocessing"
	opTagResource                = "TagResource"
	opUntagResource              = "UntagResource"
	opUpdateChannel              = "UpdateChannel"
	opUpdateDataset              = "UpdateDataset"
	opUpdateDatastore            = "UpdateDatastore"
	opUpdatePipeline             = "UpdatePipeline"
)

const (
	// iotAnalyticsService is the SigV4 service name for IoT Analytics.
	iotAnalyticsService = "iotanalytics"
	// pathChannels is the route prefix for channels.
	pathChannels = "/channels"
	// pathDatastores is the route prefix for datastores.
	pathDatastores = "/datastores"
	// pathDatasets is the route prefix for datasets.
	pathDatasets = "/datasets"
	// pathPipelines is the route prefix for pipelines.
	pathPipelines = "/pipelines"
	// pathTags is the route for tags operations.
	pathTags = "/tags"
	// pathLogging is the route for logging options.
	pathLogging = "/logging"
	// pathMessages is the route for batch message ingestion.
	pathMessages = "/messages"
	// pathPipelineActivities is the route for pipeline activity execution.
	pathPipelineActivities = "/pipelineactivities"
	// minNameSegments is the minimum path segments to extract a resource name.
	minNameSegments = 2
	// maxSubPathSegments limits SplitN to extract up to 3 sub-path components.
	maxSubPathSegments = 3
	// defaultMaxResults is the default page size for list operations.
	defaultMaxResults = 100
	// absoluteMaxResults is the maximum allowed page size for list operations.
	absoluteMaxResults = 250
)

// handlerFunc is the uniform signature for all dispatch operations.
type handlerFunc func(*echo.Context, string, []byte) error

// Handler is the HTTP handler for the IoT Analytics REST API.
type Handler struct {
	Backend StorageBackend
	ops     map[string]handlerFunc
}

// NewHandler creates a new IoT Analytics handler with a pre-built dispatch table.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = buildOps(h)

	return h
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// buildChannelOps returns the channel-related entries for the dispatch map.
func buildChannelOps(h *Handler) map[string]handlerFunc {
	return map[string]handlerFunc{
		opCreateChannel: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateChannel(c, body)
		},
		opListChannels: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListChannels(c)
		},
		opDescribeChannel: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeChannel(c, resource)
		},
		opUpdateChannel: func(c *echo.Context, resource string, body []byte) error {
			return h.handleUpdateChannel(c, resource, body)
		},
		opDeleteChannel: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteChannel(c, resource)
		},
		opSampleChannelData: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleSampleChannelData(c, resource)
		},
	}
}

// buildDatastoreOps returns the datastore-related entries for the dispatch map.
func buildDatastoreOps(h *Handler) map[string]handlerFunc {
	return map[string]handlerFunc{
		opCreateDatastore: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateDatastore(c, body)
		},
		opListDatastores: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListDatastores(c)
		},
		opDescribeDatastore: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeDatastore(c, resource)
		},
		opUpdateDatastore: func(c *echo.Context, resource string, body []byte) error {
			return h.handleUpdateDatastore(c, resource, body)
		},
		opDeleteDatastore: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteDatastore(c, resource)
		},
	}
}

// buildDatasetOps returns the dataset-related entries for the dispatch map.
func buildDatasetOps(h *Handler) map[string]handlerFunc {
	return map[string]handlerFunc{
		opCreateDataset: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateDataset(c, body)
		},
		opListDatasets: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListDatasets(c)
		},
		opDescribeDataset: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeDataset(c, resource)
		},
		opUpdateDataset: func(c *echo.Context, resource string, body []byte) error {
			return h.handleUpdateDataset(c, resource, body)
		},
		opDeleteDataset: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteDataset(c, resource)
		},
		opCreateDatasetContent: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleCreateDatasetContent(c, resource)
		},
		opGetDatasetContent: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleGetDatasetContent(c, resource)
		},
		opListDatasetContents: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleListDatasetContents(c, resource)
		},
		opDeleteDatasetContent: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteDatasetContent(c, resource)
		},
	}
}

// buildPipelineOps returns the pipeline-related entries for the dispatch map.
func buildPipelineOps(h *Handler) map[string]handlerFunc {
	return map[string]handlerFunc{
		opCreatePipeline: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreatePipeline(c, body)
		},
		opListPipelines: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListPipelines(c)
		},
		opDescribePipeline: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribePipeline(c, resource)
		},
		opUpdatePipeline: func(c *echo.Context, resource string, body []byte) error {
			return h.handleUpdatePipeline(c, resource, body)
		},
		opDeletePipeline: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeletePipeline(c, resource)
		},
		opStartPipelineReprocessing: func(c *echo.Context, resource string, body []byte) error {
			return h.handleStartPipelineReprocessing(c, resource, body)
		},
		opCancelPipelineReprocessing: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleCancelPipelineReprocessing(c, resource)
		},
	}
}

// buildMiscOps returns the miscellaneous entries (tags, logging, messages) for the dispatch map.
func buildMiscOps(h *Handler) map[string]handlerFunc {
	return map[string]handlerFunc{
		opListTagsForResource: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListTagsForResource(c)
		},
		opTagResource: func(c *echo.Context, _ string, body []byte) error {
			return h.handleTagResource(c, body)
		},
		opUntagResource: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleUntagResource(c)
		},
		opDescribeLoggingOptions: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleDescribeLoggingOptions(c)
		},
		opPutLoggingOptions: func(c *echo.Context, _ string, body []byte) error {
			return h.handlePutLoggingOptions(c, body)
		},
		opBatchPutMessage: func(c *echo.Context, _ string, body []byte) error {
			return h.handleBatchPutMessage(c, body)
		},
		opRunPipelineActivity: func(c *echo.Context, _ string, body []byte) error {
			return h.handleRunPipelineActivity(c, body)
		},
	}
}

// buildOps constructs the operation dispatch map by merging all sub-group maps.
func buildOps(h *Handler) map[string]handlerFunc {
	ops := make(map[string]handlerFunc)

	maps.Copy(ops, buildChannelOps(h))
	maps.Copy(ops, buildDatastoreOps(h))
	maps.Copy(ops, buildDatasetOps(h))
	maps.Copy(ops, buildPipelineOps(h))
	maps.Copy(ops, buildMiscOps(h))

	return ops
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTAnalytics" }

// GetSupportedOperations returns the list of supported IoT Analytics operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchPutMessage,
		opCancelPipelineReprocessing,
		opCreateChannel,
		opCreateDataset,
		opCreateDatasetContent,
		opCreateDatastore,
		opCreatePipeline,
		opDeleteChannel,
		opDeleteDataset,
		opDeleteDatasetContent,
		opDeleteDatastore,
		opDeletePipeline,
		opDescribeChannel,
		opDescribeDataset,
		opDescribeDatastore,
		opDescribeLoggingOptions,
		opDescribePipeline,
		opGetDatasetContent,
		opListChannels,
		opListDatasetContents,
		opListDatasets,
		opListDatastores,
		opListPipelines,
		opListTagsForResource,
		opPutLoggingOptions,
		opRunPipelineActivity,
		opSampleChannelData,
		opStartPipelineReprocessing,
		opTagResource,
		opUntagResource,
		opUpdateChannel,
		opUpdateDataset,
		opUpdateDatastore,
		opUpdatePipeline,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return iotAnalyticsService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

// RouteMatcher returns a function that matches IoT Analytics REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		// The "/channels" path is shared with MediaPackage and MediaTailor, which
		// register matchers at the same priority. Claim it only for SigV4-signed
		// iotanalytics requests so routing is deterministic regardless of service
		// registration order.
		if strings.HasPrefix(path, pathChannels) {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotAnalyticsService
		}

		if strings.HasPrefix(path, pathDatastores) ||
			strings.HasPrefix(path, pathDatasets) ||
			strings.HasPrefix(path, pathPipelines) {
			svc := httputils.ExtractServiceFromRequest(c.Request())

			return svc == "" || svc == iotAnalyticsService
		}

		if path == pathTags || strings.HasPrefix(path, pathTags+"?") || strings.HasPrefix(path, pathTags+"/") {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotAnalyticsService
		}

		if path == pathLogging || strings.HasPrefix(path, pathLogging+"?") {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotAnalyticsService
		}

		if strings.HasPrefix(path, pathMessages) {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotAnalyticsService
		}

		if strings.HasPrefix(path, pathPipelineActivities) {
			return httputils.ExtractServiceFromRequest(c.Request()) == iotAnalyticsService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the IoT Analytics operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseIoTAnalyticsPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource name from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseIoTAnalyticsPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function for IoT Analytics requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		method := c.Request().Method
		path := c.Request().URL.Path

		op, resource := parseIoTAnalyticsPath(method, path)
		if op == "" {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "iotanalytics: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalFailureException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "iotanalytics request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body)
	}
}

// parseIoTAnalyticsPath parses an HTTP method + path into an operation name and resource key.
func parseIoTAnalyticsPath(method, path string) (string, string) {
	switch {
	case strings.HasPrefix(path, pathChannels):
		return parseChannelPath(method, path)
	case strings.HasPrefix(path, pathDatastores):
		return parseResourcePath(method, path, pathDatastores, "Datastore", "datastoreName")
	case strings.HasPrefix(path, pathDatasets):
		return parseDatasetPath(method, path)
	case strings.HasPrefix(path, pathPipelines):
		return parsePipelinePath(method, path)
	case path == pathTags || strings.HasPrefix(path, pathTags+"?"):
		return parseTagsPath(method)
	case path == pathLogging || strings.HasPrefix(path, pathLogging+"?"):
		return parseLoggingPath(method)
	case path == pathMessages+"/batch":
		if method == http.MethodPost {
			return opBatchPutMessage, ""
		}
	case path == pathPipelineActivities+"/run":
		if method == http.MethodPost {
			return opRunPipelineActivity, ""
		}
	}

	return "", ""
}

// parseChannelPath handles channel routes including /channels/{name}/sample.
func parseChannelPath(method, path string) (string, string) {
	rest := strings.TrimPrefix(path, pathChannels)

	if rest == "" || rest == "/" {
		switch method {
		case http.MethodPost:
			return opCreateChannel, ""
		case http.MethodGet:
			return opListChannels, ""
		}

		return "", ""
	}

	segs := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", maxSubPathSegments)
	if len(segs) == 0 {
		return "", ""
	}

	name := segs[0]

	if len(segs) == minNameSegments && segs[1] == "sample" && method == http.MethodGet {
		return opSampleChannelData, name
	}

	switch method {
	case http.MethodGet:
		return opDescribeChannel, name
	case http.MethodPut:
		return opUpdateChannel, name
	case http.MethodDelete:
		return opDeleteChannel, name
	}

	return "", ""
}

// parseDatasetPath handles dataset routes including content sub-paths.
func parseDatasetPath(method, path string) (string, string) {
	rest := strings.TrimPrefix(path, pathDatasets)

	if rest == "" || rest == "/" {
		switch method {
		case http.MethodPost:
			return opCreateDataset, ""
		case http.MethodGet:
			return opListDatasets, ""
		}

		return "", ""
	}

	segs := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", maxSubPathSegments)
	if len(segs) == 0 {
		return "", ""
	}

	name := segs[0]

	if len(segs) >= minNameSegments {
		return parseDatasetSubPath(method, name, segs[1])
	}

	switch method {
	case http.MethodGet:
		return opDescribeDataset, name
	case http.MethodPut:
		return opUpdateDataset, name
	case http.MethodDelete:
		return opDeleteDataset, name
	}

	return "", ""
}

// parseDatasetSubPath handles the content/contents sub-paths for datasets.
func parseDatasetSubPath(method, name, sub string) (string, string) {
	switch sub {
	case "content":
		switch method {
		case http.MethodPost:
			return opCreateDatasetContent, name
		case http.MethodGet:
			return opGetDatasetContent, name
		case http.MethodDelete:
			return opDeleteDatasetContent, name
		}
	case "contents":
		if method == http.MethodGet {
			return opListDatasetContents, name
		}
	}

	return "", ""
}

// parsePipelinePath handles pipeline routes including reprocessing sub-paths.
func parsePipelinePath(method, path string) (string, string) {
	rest := strings.TrimPrefix(path, pathPipelines)

	if rest == "" || rest == "/" {
		switch method {
		case http.MethodPost:
			return opCreatePipeline, ""
		case http.MethodGet:
			return opListPipelines, ""
		}

		return "", ""
	}

	segs := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", maxSubPathSegments)
	if len(segs) == 0 {
		return "", ""
	}

	name := segs[0]

	if len(segs) >= minNameSegments && segs[1] == "reprocessing" {
		if len(segs) == maxSubPathSegments && segs[2] != "" {
			if method == http.MethodDelete {
				return opCancelPipelineReprocessing, name + "/" + segs[2]
			}

			return "", ""
		}

		if method == http.MethodPost {
			return opStartPipelineReprocessing, name
		}

		return "", ""
	}

	switch method {
	case http.MethodGet:
		return opDescribePipeline, name
	case http.MethodPut:
		return opUpdatePipeline, name
	case http.MethodDelete:
		return opDeletePipeline, name
	}

	return "", ""
}

// parseLoggingPath maps method to logging options operation.
func parseLoggingPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opDescribeLoggingOptions, ""
	case http.MethodPut:
		return opPutLoggingOptions, ""
	}

	return "", ""
}

// parseResourcePath handles CRUD for a resource type (channels, datastores, datasets, pipelines).
func parseResourcePath(method, path, prefix, typeName, _ string) (string, string) {
	rest := strings.TrimPrefix(path, prefix)

	if rest == "" || rest == "/" {
		switch method {
		case http.MethodPost:
			return "Create" + typeName, ""
		case http.MethodGet:
			return "List" + typeName + "s", ""
		}

		return "", ""
	}

	segs := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", minNameSegments)
	if len(segs) == 0 {
		return "", ""
	}

	name := segs[0]

	switch method {
	case http.MethodGet:
		return "Describe" + typeName, name
	case http.MethodPut:
		return "Update" + typeName, name
	case http.MethodDelete:
		return "Delete" + typeName, name
	}

	return "", ""
}

// parseTagsPath maps method to tags operation.
func parseTagsPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListTagsForResource, ""
	case http.MethodPost:
		return opTagResource, ""
	case http.MethodDelete:
		return opUntagResource, ""
	}

	return "", ""
}

// dispatch routes the request to the appropriate handler using the pre-built ops map.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	fn, ok := h.ops[op]
	if !ok {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "unknown operation: "+op)
	}

	return fn(c, resource, body)
}

// parsePagination extracts and validates maxResults and nextToken from query params.
// Returns clamped maxResults and the decoded cursor (name to start after).
func parsePagination(c *echo.Context) (int, string) {
	maxResults := defaultMaxResults

	if s := c.Request().URL.Query().Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxResults = min(n, absoluteMaxResults)
		}
	}

	cursor := ""

	if tok := c.Request().URL.Query().Get("nextToken"); tok != "" {
		if decoded, err := base64.StdEncoding.DecodeString(tok); err == nil {
			cursor = string(decoded)
		}
	}

	return maxResults, cursor
}

// encodeNextToken base64-encodes a cursor name for use as a pagination token.
func encodeNextToken(name string) string {
	return base64.StdEncoding.EncodeToString([]byte(name))
}

// ----------------------------------------
// Channel handlers
// ----------------------------------------

func (h *Handler) handleCreateChannel(c *echo.Context, body []byte) error {
	var req createChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "channelName is required")
	}

	tags := tagsToMap(req.Tags)

	ch, err := h.Backend.CreateChannel(
		c.Request().Context(), req.ChannelName, tags, req.ChannelStorage, req.RetentionPeriod)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createChannelResponse{
		ChannelName: ch.Name,
		ChannelARN:  ch.ARN,
	})
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	channels := h.Backend.ListChannels()

	summaries := make([]channelSummary, 0, len(channels))
	var nextToken *string

	count := 0

	for _, ch := range channels {
		if cursor != "" && ch.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].ChannelName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, channelSummary{
			ChannelName:            ch.Name,
			ChannelARN:             ch.ARN,
			Status:                 ch.Status,
			CreationTime:           ch.CreationTime,
			LastUpdateTime:         ch.LastUpdate,
			LastMessageArrivalTime: ch.LastMessageArrivalTime,
		})
		count++
	}

	return c.JSON(http.StatusOK, listChannelsResponse{
		ChannelSummaries: summaries,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, name string) error {
	ch, err := h.Backend.DescribeChannel(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeChannelResponse{
		Channel: channelDetail{
			Storage:                ch.Storage,
			RetentionPeriod:        ch.RetentionPeriod,
			Tags:                   mapToTagsSorted(ch.Tags),
			Name:                   ch.Name,
			ARN:                    ch.ARN,
			Status:                 ch.Status,
			CreationTime:           ch.CreationTime,
			LastUpdateTime:         ch.LastUpdate,
			LastMessageArrivalTime: ch.LastMessageArrivalTime,
		},
	})
}

func (h *Handler) handleUpdateChannel(c *echo.Context, name string, body []byte) error {
	var req updateChannelRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	if err := h.Backend.UpdateChannel(name, req.ChannelStorage, req.RetentionPeriod); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteChannel(c *echo.Context, name string) error {
	if err := h.Backend.DeleteChannel(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Datastore handlers
// ----------------------------------------

func (h *Handler) handleCreateDatastore(c *echo.Context, body []byte) error {
	var req createDatastoreRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.DatastoreName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "datastoreName is required")
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDatastore(
		c.Request().Context(),
		req.DatastoreName,
		tags,
		req.DatastoreStorage,
		req.RetentionPeriod,
		req.FileFormatConfiguration,
		req.Partitions,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatastoreResponse{
		DatastoreName: ds.Name,
		DatastoreARN:  ds.ARN,
	})
}

func (h *Handler) handleListDatastores(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	datastores := h.Backend.ListDatastores()

	summaries := make([]datastoreSummary, 0, len(datastores))
	var nextToken *string

	count := 0

	for _, ds := range datastores {
		if cursor != "" && ds.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].DatastoreName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, datastoreSummary{
			DatastoreName:  ds.Name,
			DatastoreARN:   ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		})
		count++
	}

	return c.JSON(http.StatusOK, listDatastoresResponse{
		DatastoreSummaries: summaries,
		NextToken:          nextToken,
	})
}

func (h *Handler) handleDescribeDatastore(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDatastore(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeDatastoreResponse{
		Datastore: datastoreDetail{
			Storage:                 ds.Storage,
			RetentionPeriod:         ds.RetentionPeriod,
			FileFormatConfiguration: ds.FileFormatConfiguration,
			Partitions:              ds.Partitions,
			Tags:                    mapToTagsSorted(ds.Tags),
			Name:                    ds.Name,
			ARN:                     ds.ARN,
			Status:                  ds.Status,
			CreationTime:            ds.CreationTime,
			LastUpdateTime:          ds.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateDatastore(c *echo.Context, name string, body []byte) error {
	var req updateDatastoreRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	err := h.Backend.UpdateDatastore(
		name, req.DatastoreStorage, req.RetentionPeriod, req.FileFormatConfiguration, req.Partitions,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteDatastore(c *echo.Context, name string) error {
	if err := h.Backend.DeleteDatastore(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Dataset handlers
// ----------------------------------------

func (h *Handler) handleCreateDataset(c *echo.Context, body []byte) error {
	var req createDatasetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.DatasetName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "datasetName is required")
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDataset(
		c.Request().Context(),
		req.DatasetName,
		tags,
		req.Actions,
		req.Triggers,
		req.ContentDeliveryRules,
		req.VersioningConfiguration,
		req.LateDataRules,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatasetResponse{
		DatasetName: ds.Name,
		DatasetARN:  ds.ARN,
	})
}

func (h *Handler) handleListDatasets(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	datasets := h.Backend.ListDatasets()

	summaries := make([]datasetSummary, 0, len(datasets))
	var nextToken *string

	count := 0

	for _, ds := range datasets {
		if cursor != "" && ds.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].DatasetName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, datasetSummary{
			DatasetName:    ds.Name,
			DatasetARN:     ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		})
		count++
	}

	return c.JSON(http.StatusOK, listDatasetsResponse{
		DatasetSummaries: summaries,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleDescribeDataset(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDataset(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeDatasetResponse{
		Dataset: datasetDetail{
			Actions:                 ds.Actions,
			Triggers:                ds.Triggers,
			ContentDeliveryRules:    ds.ContentDeliveryRules,
			LateDataRules:           ds.LateDataRules,
			VersioningConfiguration: ds.VersioningConfiguration,
			Tags:                    mapToTagsSorted(ds.Tags),
			Name:                    ds.Name,
			ARN:                     ds.ARN,
			Status:                  ds.Status,
			CreationTime:            ds.CreationTime,
			LastUpdateTime:          ds.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateDataset(c *echo.Context, name string, body []byte) error {
	var req updateDatasetRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	err := h.Backend.UpdateDataset(
		name, req.Actions, req.Triggers, req.ContentDeliveryRules, req.VersioningConfiguration, req.LateDataRules,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteDataset(c *echo.Context, name string) error {
	if err := h.Backend.DeleteDataset(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Pipeline handlers
// ----------------------------------------

func (h *Handler) handleCreatePipeline(c *echo.Context, body []byte) error {
	var req createPipelineRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.PipelineName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "pipelineName is required")
	}

	tags := tagsToMap(req.Tags)

	p, err := h.Backend.CreatePipeline(c.Request().Context(), req.PipelineName, tags, req.PipelineActivities)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createPipelineResponse{
		PipelineName: p.Name,
		PipelineARN:  p.ARN,
	})
}

func (h *Handler) handleListPipelines(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	pipelines := h.Backend.ListPipelines()

	summaries := make([]pipelineSummary, 0, len(pipelines))
	var nextToken *string

	count := 0

	for _, p := range pipelines {
		if cursor != "" && p.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].PipelineName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, pipelineSummary{
			PipelineName:          p.Name,
			PipelineARN:           p.ARN,
			ReprocessingSummaries: reprocessingSummariesSorted(p.Reprocessings),
			CreationTime:          p.CreationTime,
			LastUpdateTime:        p.LastUpdate,
		})
		count++
	}

	return c.JSON(http.StatusOK, listPipelinesResponse{
		PipelineSummaries: summaries,
		NextToken:         nextToken,
	})
}

func (h *Handler) handleDescribePipeline(c *echo.Context, name string) error {
	p, err := h.Backend.DescribePipeline(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describePipelineResponse{
		Pipeline: pipelineDetail{
			Activities:            p.Activities,
			ReprocessingSummaries: reprocessingSummariesSorted(p.Reprocessings),
			Tags:                  mapToTagsSorted(p.Tags),
			Name:                  p.Name,
			ARN:                   p.ARN,
			CreationTime:          p.CreationTime,
			LastUpdateTime:        p.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdatePipeline(c *echo.Context, name string, body []byte) error {
	var req updatePipelineRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	if err := h.Backend.UpdatePipeline(name, req.PipelineActivities); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePipeline(c *echo.Context, name string) error {
	if err := h.Backend.DeletePipeline(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Tags handlers
// ----------------------------------------

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "tagKeys query parameter is required")
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// SampleChannelData handler
// ----------------------------------------

func (h *Handler) handleSampleChannelData(c *echo.Context, channelName string) error {
	maxMessages := defaultSampleMessages

	if s := c.Request().URL.Query().Get("maxMessages"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxMessages = n
		}
	}

	payloads, err := h.Backend.SampleChannelData(channelName, maxMessages)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, sampleChannelDataResponse{Payloads: payloads})
}

// ----------------------------------------
// BatchPutMessage handler
// ----------------------------------------

func (h *Handler) handleBatchPutMessage(c *echo.Context, body []byte) error {
	var req batchPutMessageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "channelName is required")
	}

	if len(req.Messages) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "messages must not be empty")
	}

	if len(req.Messages) > maxBatchMessages {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException",
			"messages count exceeds limit of "+strconv.Itoa(maxBatchMessages))
	}

	for i, msg := range req.Messages {
		if msg.MessageID == "" {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"messageId is required for message at index "+strconv.Itoa(i),
			)
		}

		if len(msg.Payload) == 0 {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"payload must not be empty for message "+msg.MessageID,
			)
		}
	}

	errs, err := h.Backend.BatchPutMessage(req.ChannelName, req.Messages)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
	}

	return c.JSON(http.StatusOK, batchPutMessageResponse{BatchPutMessageErrorEntries: errs})
}

// ----------------------------------------
// Pipeline reprocessing handlers
// ----------------------------------------

func (h *Handler) handleStartPipelineReprocessing(c *echo.Context, pipelineName string, body []byte) error {
	var req startPipelineReprocessingRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	reprocessingID, err := h.Backend.StartPipelineReprocessing(pipelineName, req.StartTime, req.EndTime)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusCreated, startPipelineReprocessingResponse{ReprocessingID: reprocessingID})
}

func (h *Handler) handleCancelPipelineReprocessing(c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, "/", minNameSegments)
	if len(parts) != minNameSegments {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	pipelineName := parts[0]
	reprocessingID := parts[1]

	if err := h.Backend.CancelPipelineReprocessing(pipelineName, reprocessingID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// DatasetContent handlers
// ----------------------------------------

func (h *Handler) handleCreateDatasetContent(c *echo.Context, datasetName string) error {
	content, err := h.Backend.CreateDatasetContent(datasetName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createDatasetContentResponse{
		VersionID: content.VersionID,
	})
}

func (h *Handler) handleGetDatasetContent(c *echo.Context, datasetName string) error {
	versionID := c.Request().URL.Query().Get("versionId")

	content, err := h.Backend.GetDatasetContent(datasetName, versionID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getDatasetContentResponse{
		Status:    &datasetContentStatusDTO{State: content.Status},
		Timestamp: content.CreationTime,
	})
}

func (h *Handler) handleListDatasetContents(c *echo.Context, datasetName string) error {
	contents, err := h.Backend.ListDatasetContents(datasetName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]datasetContentSummary, 0, len(contents))

	for _, content := range contents {
		summaries = append(summaries, datasetContentSummary{
			Version:        content.VersionID,
			Status:         &datasetContentStatusDTO{State: content.Status},
			CreationTime:   content.CreationTime,
			CompletionTime: content.CompletionTime,
		})
	}

	return c.JSON(http.StatusOK, listDatasetContentsResponse{DatasetContentSummaries: summaries})
}

func (h *Handler) handleDeleteDatasetContent(c *echo.Context, datasetName string) error {
	versionID := c.Request().URL.Query().Get("versionId")

	if err := h.Backend.DeleteDatasetContent(datasetName, versionID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// LoggingOptions handlers
// ----------------------------------------

func (h *Handler) handleDescribeLoggingOptions(c *echo.Context) error {
	opts, err := h.Backend.DescribeLoggingOptions()
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeLoggingOptionsResponse{
		LoggingOptions: loggingOptionsDTO{
			RoleARN: opts.RoleARN,
			Level:   opts.Level,
			Enabled: opts.Enabled,
		},
	})
}

func (h *Handler) handlePutLoggingOptions(c *echo.Context, body []byte) error {
	var req putLoggingOptionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	opts := &LoggingOptions{
		RoleARN: req.LoggingOptions.RoleARN,
		Level:   req.LoggingOptions.Level,
		Enabled: req.LoggingOptions.Enabled,
	}

	if err := h.Backend.PutLoggingOptions(opts); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// RunPipelineActivity handler
// ----------------------------------------

func (h *Handler) handleRunPipelineActivity(c *echo.Context, body []byte) error {
	var req runPipelineActivityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	payloads, err := h.Backend.RunPipelineActivity(req.Payloads)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
	}

	return c.JSON(http.StatusOK, runPipelineActivityResponse{
		Payloads:  payloads,
		LogResult: "",
	})
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

// writeError writes an IoT Analytics JSON error response with AWS __type field.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// writeBackendError maps a backend error to an HTTP error response with appropriate AWS error type.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	if isNotFound(err) {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	}

	if errors.Is(err, ErrAlreadyExists) {
		return h.writeError(c, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
	}

	if errors.Is(err, ErrValidation) {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
}

// ci trigger
