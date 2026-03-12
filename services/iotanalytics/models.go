package iotanalytics

import "time"

// Sentinel errors for IoT Analytics backend operations.
var (
	// ErrChannelNotFound is returned when a channel does not exist.
	ErrChannelNotFound = newNotFoundError("channel not found")
	// ErrDatastoreNotFound is returned when a datastore does not exist.
	ErrDatastoreNotFound = newNotFoundError("datastore not found")
	// ErrDatasetNotFound is returned when a dataset does not exist.
	ErrDatasetNotFound = newNotFoundError("dataset not found")
	// ErrPipelineNotFound is returned when a pipeline does not exist.
	ErrPipelineNotFound = newNotFoundError("pipeline not found")
)

// notFoundError represents a resource-not-found error.
type notFoundError struct {
	msg string
}

func (e *notFoundError) Error() string { return e.msg }

// newNotFoundError creates a new notFoundError.
func newNotFoundError(msg string) *notFoundError {
	return &notFoundError{msg: msg}
}

// isNotFound returns true if the error is a notFoundError.
func isNotFound(err error) bool {
	_, ok := err.(*notFoundError) //nolint:errorlint // direct type check for sentinel

	return ok
}

// Channel stores all metadata and state for a single IoT Analytics channel.
type Channel struct {
	Tags         map[string]string
	Name         string
	ARN          string
	Status       string
	CreationTime float64
	LastUpdate   float64
}

// Datastore stores all metadata and state for a single IoT Analytics datastore.
type Datastore struct {
	Tags         map[string]string
	Name         string
	ARN          string
	Status       string
	CreationTime float64
	LastUpdate   float64
}

// Dataset stores all metadata and state for a single IoT Analytics dataset.
type Dataset struct {
	Tags         map[string]string
	Name         string
	ARN          string
	Status       string
	CreationTime float64
	LastUpdate   float64
}

// Pipeline stores all metadata and state for a single IoT Analytics pipeline.
type Pipeline struct {
	Tags                  map[string]string
	Name                  string
	ARN                   string
	ReprocessingSummaries []string
	CreationTime          float64
	LastUpdate            float64
}

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// DTO types for request/response serialization.

// createChannelRequest is the request body for CreateChannel.
type createChannelRequest struct {
	ChannelName string   `json:"channelName"`
	Tags        []tagDTO `json:"tags,omitempty"`
}

// createChannelResponse is the response body for CreateChannel.
type createChannelResponse struct {
	ChannelName string `json:"channelName"`
	ChannelARN  string `json:"channelArn"`
}

// channelSummary is a summary of a channel for list operations.
type channelSummary struct {
	ChannelName    string  `json:"channelName"`
	ChannelARN     string  `json:"channelArn,omitempty"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// listChannelsResponse is the response body for ListChannels.
type listChannelsResponse struct {
	NextToken        *string          `json:"nextToken,omitempty"`
	ChannelSummaries []channelSummary `json:"channelSummaries"`
}

// describeChannelResponse is the response body for DescribeChannel.
type describeChannelResponse struct {
	Channel channelDetail `json:"channel"`
}

// channelDetail is a detailed view of a channel.
type channelDetail struct {
	Name           string  `json:"name"`
	ARN            string  `json:"arn"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// createDatastoreRequest is the request body for CreateDatastore.
type createDatastoreRequest struct {
	DatastoreName string   `json:"datastoreName"`
	Tags          []tagDTO `json:"tags,omitempty"`
}

// createDatastoreResponse is the response body for CreateDatastore.
type createDatastoreResponse struct {
	DatastoreName string `json:"datastoreName"`
	DatastoreARN  string `json:"datastoreArn"`
}

// datastoreSummary is a summary of a datastore for list operations.
type datastoreSummary struct {
	DatastoreName  string  `json:"datastoreName"`
	DatastoreARN   string  `json:"datastoreArn,omitempty"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// listDatastoresResponse is the response body for ListDatastores.
type listDatastoresResponse struct {
	NextToken          *string            `json:"nextToken,omitempty"`
	DatastoreSummaries []datastoreSummary `json:"datastoreSummaries"`
}

// describeDatastoreResponse is the response body for DescribeDatastore.
type describeDatastoreResponse struct {
	Datastore datastoreDetail `json:"datastore"`
}

// datastoreDetail is a detailed view of a datastore.
type datastoreDetail struct {
	Name           string  `json:"name"`
	ARN            string  `json:"arn"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// createDatasetRequest is the request body for CreateDataset.
type createDatasetRequest struct {
	DatasetName string   `json:"datasetName"`
	Actions     []any    `json:"actions,omitempty"`
	Tags        []tagDTO `json:"tags,omitempty"`
}

// createDatasetResponse is the response body for CreateDataset.
type createDatasetResponse struct {
	DatasetName string `json:"datasetName"`
	DatasetARN  string `json:"datasetArn"`
}

// datasetSummary is a summary of a dataset for list operations.
type datasetSummary struct {
	DatasetName    string  `json:"datasetName"`
	DatasetARN     string  `json:"datasetArn,omitempty"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// listDatasetsResponse is the response body for ListDatasets.
type listDatasetsResponse struct {
	NextToken        *string          `json:"nextToken,omitempty"`
	DatasetSummaries []datasetSummary `json:"datasetSummaries"`
}

// describeDatasetResponse is the response body for DescribeDataset.
type describeDatasetResponse struct {
	Dataset datasetDetail `json:"dataset"`
}

// datasetDetail is a detailed view of a dataset.
type datasetDetail struct {
	Name           string  `json:"name"`
	ARN            string  `json:"arn"`
	Status         string  `json:"status"`
	CreationTime   float64 `json:"creationTime"`
	LastUpdateTime float64 `json:"lastUpdateTime,omitempty"`
}

// createPipelineRequest is the request body for CreatePipeline.
type createPipelineRequest struct {
	PipelineName       string   `json:"pipelineName"`
	PipelineActivities []any    `json:"pipelineActivities,omitempty"`
	Tags               []tagDTO `json:"tags,omitempty"`
}

// createPipelineResponse is the response body for CreatePipeline.
type createPipelineResponse struct {
	PipelineName string `json:"pipelineName"`
	PipelineARN  string `json:"pipelineArn"`
}

// pipelineSummary is a summary of a pipeline for list operations.
type pipelineSummary struct {
	PipelineName          string   `json:"pipelineName"`
	PipelineARN           string   `json:"pipelineArn,omitempty"`
	ReprocessingSummaries []string `json:"reprocessingSummaries,omitempty"`
	CreationTime          float64  `json:"creationTime"`
	LastUpdateTime        float64  `json:"lastUpdateTime,omitempty"`
}

// listPipelinesResponse is the response body for ListPipelines.
type listPipelinesResponse struct {
	NextToken         *string           `json:"nextToken,omitempty"`
	PipelineSummaries []pipelineSummary `json:"pipelineSummaries"`
}

// describePipelineResponse is the response body for DescribePipeline.
type describePipelineResponse struct {
	Pipeline pipelineDetail `json:"pipeline"`
}

// pipelineDetail is a detailed view of a pipeline.
type pipelineDetail struct {
	Name                  string   `json:"name"`
	ARN                   string   `json:"arn"`
	ReprocessingSummaries []string `json:"reprocessingSummaries,omitempty"`
	CreationTime          float64  `json:"creationTime"`
	LastUpdateTime        float64  `json:"lastUpdateTime,omitempty"`
}

// TagDTO is a key-value tag for IoT Analytics resources.
type TagDTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// tagDTO is an alias for TagDTO for internal use.
type tagDTO = TagDTO

// listTagsResponse is the response body for ListTagsForResource.
type listTagsResponse struct {
	Tags []tagDTO `json:"tags"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags []tagDTO `json:"tags"`
}

// errorResponse is the standard IoT Analytics error response.
type errorResponse struct {
	Message string `json:"message"`
}
