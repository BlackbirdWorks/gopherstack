package kinesisanalytics

import "time"

// Application represents a Kinesis Analytics v1 application.
type Application struct {
	CreateTimestamp        *time.Time
	LastUpdateTimestamp    *time.Time
	Tags                   map[string]string
	ApplicationName        string
	ApplicationARN         string
	ApplicationCode        string
	ApplicationDescription string
	ApplicationStatus      string
	ApplicationVersionID   int64
}

// applicationSummary is the short form returned by ListApplications.
type applicationSummary struct {
	ApplicationARN    string `json:"ApplicationARN"`
	ApplicationName   string `json:"ApplicationName"`
	ApplicationStatus string `json:"ApplicationStatus"`
}

// createApplicationInput is the request body for CreateApplication.
type createApplicationInput struct {
	ApplicationName        string     `json:"ApplicationName"`
	ApplicationDescription string     `json:"ApplicationDescription"`
	ApplicationCode        string     `json:"ApplicationCode"`
	Tags                   []tagEntry `json:"Tags"`
}

// createApplicationOutput is the response body for CreateApplication.
type createApplicationOutput struct {
	ApplicationSummary applicationSummary `json:"ApplicationSummary"`
}

// deleteApplicationInput is the request body for DeleteApplication.
type deleteApplicationInput struct {
	ApplicationName string  `json:"ApplicationName"`
	CreateTimestamp float64 `json:"CreateTimestamp"`
}

// describeApplicationInput is the request body for DescribeApplication.
type describeApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// applicationDetail is the full application detail returned by DescribeApplication.
type applicationDetail struct {
	ApplicationARN         string  `json:"ApplicationARN"`
	ApplicationName        string  `json:"ApplicationName"`
	ApplicationStatus      string  `json:"ApplicationStatus"`
	ApplicationCode        string  `json:"ApplicationCode,omitempty"`
	ApplicationDescription string  `json:"ApplicationDescription,omitempty"`
	ApplicationVersionID   int64   `json:"ApplicationVersionId"`
	CreateTimestamp        float64 `json:"CreateTimestamp,omitempty"`
	LastUpdateTimestamp    float64 `json:"LastUpdateTimestamp,omitempty"`
}

// describeApplicationOutput is the response body for DescribeApplication.
type describeApplicationOutput struct {
	ApplicationDetail applicationDetail `json:"ApplicationDetail"`
}

// listApplicationsInput is the request body for ListApplications.
type listApplicationsInput struct {
	ExclusiveStartApplicationName string `json:"ExclusiveStartApplicationName"`
	Limit                         int    `json:"Limit"`
}

// listApplicationsOutput is the response body for ListApplications.
type listApplicationsOutput struct {
	ApplicationSummaries []applicationSummary `json:"ApplicationSummaries"`
	HasMoreApplications  bool                 `json:"HasMoreApplications"`
}

// startApplicationInput is the request body for StartApplication.
type startApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// stopApplicationInput is the request body for StopApplication.
type stopApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// updateApplicationInput is the request body for UpdateApplication.
type updateApplicationInput struct {
	ApplicationUpdate           *applicationUpdate `json:"ApplicationUpdate"`
	ApplicationName             string             `json:"ApplicationName"`
	CurrentApplicationVersionID int64              `json:"CurrentApplicationVersionId"`
}

// applicationUpdate holds optional update fields.
type applicationUpdate struct {
	ApplicationCodeUpdate string `json:"ApplicationCodeUpdate,omitempty"`
}

// tagEntry is a key-value tag pair.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// listTagsForResourceInput is the request body for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

// listTagsForResourceOutput is the response body for ListTagsForResource.
type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
}

// tagResourceInput is the request body for TagResource.
type tagResourceInput struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagEntry `json:"Tags"`
}

// untagResourceInput is the request body for UntagResource.
type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

// errorResponse is the standard Kinesis Analytics error response body.
type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
