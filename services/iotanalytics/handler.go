package iotanalytics

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
	// minNameSegments is the minimum path segments to extract a resource name.
	minNameSegments = 2
)

// Handler is the HTTP handler for the IoT Analytics REST API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new IoT Analytics handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "IoTAnalytics" }

// GetSupportedOperations returns the list of supported IoT Analytics operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateChannel",
		"ListChannels",
		"DescribeChannel",
		"UpdateChannel",
		"DeleteChannel",
		"CreateDatastore",
		"ListDatastores",
		"DescribeDatastore",
		"UpdateDatastore",
		"DeleteDatastore",
		"CreateDataset",
		"ListDatasets",
		"DescribeDataset",
		"UpdateDataset",
		"DeleteDataset",
		"CreatePipeline",
		"ListPipelines",
		"DescribePipeline",
		"UpdatePipeline",
		"DeletePipeline",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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

		if strings.HasPrefix(path, pathChannels) ||
			strings.HasPrefix(path, pathDatastores) ||
			strings.HasPrefix(path, pathDatasets) ||
			strings.HasPrefix(path, pathPipelines) {
			return true
		}

		if path == pathTags || strings.HasPrefix(path, pathTags+"?") || strings.HasPrefix(path, pathTags+"/") {
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
			return h.writeError(c, http.StatusNotFound, "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "iotanalytics: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
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
		return parseResourcePath(method, path, pathChannels, "Channel", "channelName")
	case strings.HasPrefix(path, pathDatastores):
		return parseResourcePath(method, path, pathDatastores, "Datastore", "datastoreName")
	case strings.HasPrefix(path, pathDatasets):
		return parseResourcePath(method, path, pathDatasets, "Dataset", "datasetName")
	case strings.HasPrefix(path, pathPipelines):
		return parseResourcePath(method, path, pathPipelines, "Pipeline", "pipelineName")
	case path == pathTags || strings.HasPrefix(path, pathTags+"?"):
		return parseTagsPath(method)
	}

	return "", ""
}

// parseResourcePath handles CRUD for a resource type (channels, datastores, datasets, pipelines).
func parseResourcePath(method, path, prefix, typeName, _ string) (string, string) {
	rest := strings.TrimPrefix(path, prefix)

	// /channels  or /channels/
	if rest == "" || rest == "/" {
		switch method {
		case http.MethodPost:
			return "Create" + typeName, ""
		case http.MethodGet:
			return "List" + typeName + "s", ""
		}

		return "", ""
	}

	// /channels/{name}
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
		return "ListTagsForResource", ""
	case http.MethodPost:
		return "TagResource", ""
	case http.MethodDelete:
		return "UntagResource", ""
	}

	return "", ""
}

// dispatch routes the request to the appropriate handler.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	if handled, err := h.dispatchChannel(c, op, resource, body); handled {
		return err
	}

	if handled, err := h.dispatchDatastore(c, op, resource, body); handled {
		return err
	}

	if handled, err := h.dispatchDataset(c, op, resource, body); handled {
		return err
	}

	if handled, err := h.dispatchPipeline(c, op, resource, body); handled {
		return err
	}

	switch op {
	case "ListTagsForResource":
		return h.handleListTagsForResource(c)
	case "TagResource":
		return h.handleTagResource(c, body)
	case "UntagResource":
		return h.handleUntagResource(c)
	}

	return h.writeError(c, http.StatusNotFound, "unknown operation: "+op)
}

// dispatchChannel routes channel operations. Returns (true, err) if op was handled.
func (h *Handler) dispatchChannel(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateChannel":
		return true, h.handleCreateChannel(c, body)
	case "ListChannels":
		return true, h.handleListChannels(c)
	case "DescribeChannel":
		return true, h.handleDescribeChannel(c, resource)
	case "UpdateChannel":
		return true, h.handleUpdateChannel(c, resource)
	case "DeleteChannel":
		return true, h.handleDeleteChannel(c, resource)
	}

	return false, nil
}

// dispatchDatastore routes datastore operations. Returns (true, err) if op was handled.
func (h *Handler) dispatchDatastore(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateDatastore":
		return true, h.handleCreateDatastore(c, body)
	case "ListDatastores":
		return true, h.handleListDatastores(c)
	case "DescribeDatastore":
		return true, h.handleDescribeDatastore(c, resource)
	case "UpdateDatastore":
		return true, h.handleUpdateDatastore(c, resource)
	case "DeleteDatastore":
		return true, h.handleDeleteDatastore(c, resource)
	}

	return false, nil
}

// dispatchDataset routes dataset operations. Returns (true, err) if op was handled.
func (h *Handler) dispatchDataset(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreateDataset":
		return true, h.handleCreateDataset(c, body)
	case "ListDatasets":
		return true, h.handleListDatasets(c)
	case "DescribeDataset":
		return true, h.handleDescribeDataset(c, resource)
	case "UpdateDataset":
		return true, h.handleUpdateDataset(c, resource)
	case "DeleteDataset":
		return true, h.handleDeleteDataset(c, resource)
	}

	return false, nil
}

// dispatchPipeline routes pipeline operations. Returns (true, err) if op was handled.
func (h *Handler) dispatchPipeline(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "CreatePipeline":
		return true, h.handleCreatePipeline(c, body)
	case "ListPipelines":
		return true, h.handleListPipelines(c)
	case "DescribePipeline":
		return true, h.handleDescribePipeline(c, resource)
	case "UpdatePipeline":
		return true, h.handleUpdatePipeline(c, resource)
	case "DeletePipeline":
		return true, h.handleDeletePipeline(c, resource)
	}

	return false, nil
}

// ----------------------------------------
// Channel handlers
// ----------------------------------------

func (h *Handler) handleCreateChannel(c *echo.Context, body []byte) error {
	var req createChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "channelName is required")
	}

	tags := tagsToMap(req.Tags)

	ch, err := h.Backend.CreateChannel(req.ChannelName, tags)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, createChannelResponse{
		ChannelName: ch.Name,
		ChannelARN:  ch.ARN,
	})
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	channels := h.Backend.ListChannels()
	summaries := make([]channelSummary, 0, len(channels))

	for _, ch := range channels {
		summaries = append(summaries, channelSummary{
			ChannelName:    ch.Name,
			ChannelARN:     ch.ARN,
			Status:         ch.Status,
			CreationTime:   ch.CreationTime,
			LastUpdateTime: ch.LastUpdate,
		})
	}

	return c.JSON(http.StatusOK, listChannelsResponse{ChannelSummaries: summaries})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, name string) error {
	ch, err := h.Backend.DescribeChannel(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeChannelResponse{
		Channel: channelDetail{
			Name:           ch.Name,
			ARN:            ch.ARN,
			Status:         ch.Status,
			CreationTime:   ch.CreationTime,
			LastUpdateTime: ch.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateChannel(c *echo.Context, name string) error {
	if err := h.Backend.UpdateChannel(name); err != nil {
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
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
	}

	if req.DatastoreName == "" {
		return h.writeError(c, http.StatusBadRequest, "datastoreName is required")
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDatastore(req.DatastoreName, tags)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, createDatastoreResponse{
		DatastoreName: ds.Name,
		DatastoreARN:  ds.ARN,
	})
}

func (h *Handler) handleListDatastores(c *echo.Context) error {
	datastores := h.Backend.ListDatastores()
	summaries := make([]datastoreSummary, 0, len(datastores))

	for _, ds := range datastores {
		summaries = append(summaries, datastoreSummary{
			DatastoreName:  ds.Name,
			DatastoreARN:   ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		})
	}

	return c.JSON(http.StatusOK, listDatastoresResponse{DatastoreSummaries: summaries})
}

func (h *Handler) handleDescribeDatastore(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDatastore(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeDatastoreResponse{
		Datastore: datastoreDetail{
			Name:           ds.Name,
			ARN:            ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateDatastore(c *echo.Context, name string) error {
	if err := h.Backend.UpdateDatastore(name); err != nil {
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
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
	}

	if req.DatasetName == "" {
		return h.writeError(c, http.StatusBadRequest, "datasetName is required")
	}

	tags := tagsToMap(req.Tags)

	ds, err := h.Backend.CreateDataset(req.DatasetName, tags)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, createDatasetResponse{
		DatasetName: ds.Name,
		DatasetARN:  ds.ARN,
	})
}

func (h *Handler) handleListDatasets(c *echo.Context) error {
	datasets := h.Backend.ListDatasets()
	summaries := make([]datasetSummary, 0, len(datasets))

	for _, ds := range datasets {
		summaries = append(summaries, datasetSummary{
			DatasetName:    ds.Name,
			DatasetARN:     ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		})
	}

	return c.JSON(http.StatusOK, listDatasetsResponse{DatasetSummaries: summaries})
}

func (h *Handler) handleDescribeDataset(c *echo.Context, name string) error {
	ds, err := h.Backend.DescribeDataset(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeDatasetResponse{
		Dataset: datasetDetail{
			Name:           ds.Name,
			ARN:            ds.ARN,
			Status:         ds.Status,
			CreationTime:   ds.CreationTime,
			LastUpdateTime: ds.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdateDataset(c *echo.Context, name string) error {
	if err := h.Backend.UpdateDataset(name); err != nil {
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
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
	}

	if req.PipelineName == "" {
		return h.writeError(c, http.StatusBadRequest, "pipelineName is required")
	}

	tags := tagsToMap(req.Tags)

	p, err := h.Backend.CreatePipeline(req.PipelineName, tags)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, createPipelineResponse{
		PipelineName: p.Name,
		PipelineARN:  p.ARN,
	})
}

func (h *Handler) handleListPipelines(c *echo.Context) error {
	pipelines := h.Backend.ListPipelines()
	summaries := make([]pipelineSummary, 0, len(pipelines))

	for _, p := range pipelines {
		summaries = append(summaries, pipelineSummary{
			PipelineName:   p.Name,
			PipelineARN:    p.ARN,
			CreationTime:   p.CreationTime,
			LastUpdateTime: p.LastUpdate,
		})
	}

	return c.JSON(http.StatusOK, listPipelinesResponse{PipelineSummaries: summaries})
}

func (h *Handler) handleDescribePipeline(c *echo.Context, name string) error {
	p, err := h.Backend.DescribePipeline(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describePipelineResponse{
		Pipeline: pipelineDetail{
			Name:           p.Name,
			ARN:            p.ARN,
			CreationTime:   p.CreationTime,
			LastUpdateTime: p.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdatePipeline(c *echo.Context, name string) error {
	if err := h.Backend.UpdatePipeline(name); err != nil {
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
		return h.writeError(c, http.StatusBadRequest, "resourceArn query parameter is required")
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
		return h.writeError(c, http.StatusBadRequest, "resourceArn query parameter is required")
	}

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(c, http.StatusBadRequest, "resourceArn query parameter is required")
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		return h.writeError(c, http.StatusBadRequest, "tagKeys query parameter is required")
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

// writeError writes an IoT Analytics JSON error response.
func (h *Handler) writeError(c *echo.Context, status int, message string) error {
	return c.JSON(status, errorResponse{Message: message})
}

// writeBackendError maps a backend error to an HTTP error response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	if isNotFound(err) {
		return h.writeError(c, http.StatusNotFound, err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, err.Error())
}
