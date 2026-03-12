package mediastore

import "time"

// Container represents an AWS Elemental MediaStore container.
type Container struct {
	CreationTime         *time.Time
	Tags                 map[string]string
	ARN                  string
	ContainerPolicy      string
	CorsPolicy           string
	LifecyclePolicy      string
	MetricPolicy         string
	Endpoint             string
	Name                 string
	Status               string
	AccessLoggingEnabled bool
}

// -- Request / Response bodies ------------------------------------------------

// createContainerRequest is the request body for CreateContainer.
type createContainerRequest struct {
	ContainerName string     `json:"ContainerName"`
	Tags          []tagEntry `json:"Tags,omitempty"`
}

// tagEntry represents a key-value tag in the Mediastore API.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// deleteContainerRequest is the request body for DeleteContainer.
type deleteContainerRequest struct {
	ContainerName string `json:"ContainerName"`
}

// describeContainerRequest is the request body for DescribeContainer.
type describeContainerRequest struct {
	ContainerName string `json:"ContainerName"`
}

// putContainerPolicyRequest is the request body for PutContainerPolicy.
type putContainerPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
	Policy        string `json:"Policy"`
}

// getContainerPolicyRequest is the request body for GetContainerPolicy.
type getContainerPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// deleteContainerPolicyRequest is the request body for DeleteContainerPolicy.
type deleteContainerPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// putCorsPolicyRequest is the request body for PutCorsPolicy.
type putCorsPolicyRequest struct {
	ContainerName string     `json:"ContainerName"`
	CorsPolicy    []CorsRule `json:"CorsPolicy"`
}

// CorsRule represents a single CORS rule.
type CorsRule struct {
	AllowedHeaders []string `json:"AllowedHeaders"`
	AllowedMethods []string `json:"AllowedMethods,omitempty"`
	AllowedOrigins []string `json:"AllowedOrigins"`
	ExposeHeaders  []string `json:"ExposeHeaders,omitempty"`
	MaxAgeSeconds  int      `json:"MaxAgeSeconds,omitempty"`
}

// getCorsPolicyRequest is the request body for GetCorsPolicy.
type getCorsPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// deleteCorsPolicyRequest is the request body for DeleteCorsPolicy.
type deleteCorsPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// putLifecyclePolicyRequest is the request body for PutLifecyclePolicy.
type putLifecyclePolicyRequest struct {
	ContainerName   string `json:"ContainerName"`
	LifecyclePolicy string `json:"LifecyclePolicy"`
}

// getLifecyclePolicyRequest is the request body for GetLifecyclePolicy.
type getLifecyclePolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// deleteLifecyclePolicyRequest is the request body for DeleteLifecyclePolicy.
type deleteLifecyclePolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// putMetricPolicyRequest is the request body for PutMetricPolicy.
type putMetricPolicyRequest struct {
	ContainerName string       `json:"ContainerName"`
	MetricPolicy  MetricPolicy `json:"MetricPolicy"`
}

// MetricPolicy represents a metric policy.
type MetricPolicy struct {
	ContainerLevelMetrics string             `json:"ContainerLevelMetrics"`
	MetricPolicyRules     []MetricPolicyRule `json:"MetricPolicyRules,omitempty"`
}

// MetricPolicyRule represents a metric policy rule.
type MetricPolicyRule struct {
	ObjectGroup     string `json:"ObjectGroup"`
	ObjectGroupName string `json:"ObjectGroupName"`
}

// getMetricPolicyRequest is the request body for GetMetricPolicy.
type getMetricPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// deleteMetricPolicyRequest is the request body for DeleteMetricPolicy.
type deleteMetricPolicyRequest struct {
	ContainerName string `json:"ContainerName"`
}

// startAccessLoggingRequest is the request body for StartAccessLogging.
type startAccessLoggingRequest struct {
	ContainerName string `json:"ContainerName"`
}

// stopAccessLoggingRequest is the request body for StopAccessLogging.
type stopAccessLoggingRequest struct {
	ContainerName string `json:"ContainerName"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Resource string     `json:"Resource"`
	Tags     []tagEntry `json:"Tags"`
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	Resource string   `json:"Resource"`
	TagKeys  []string `json:"TagKeys"`
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	Resource string `json:"Resource"`
}

// containerObject is the JSON representation of a container.
type containerObject struct {
	CreationTime         *time.Time `json:"CreationTime,omitempty"`
	ARN                  string     `json:"ARN,omitempty"`
	Endpoint             string     `json:"Endpoint,omitempty"`
	Name                 string     `json:"Name,omitempty"`
	Status               string     `json:"Status,omitempty"`
	AccessLoggingEnabled bool       `json:"AccessLoggingEnabled"`
}

// createContainerResponse is the response for CreateContainer.
type createContainerResponse struct {
	Container containerObject `json:"Container"`
}

// describeContainerResponse is the response for DescribeContainer.
type describeContainerResponse struct {
	Container containerObject `json:"Container"`
}

// listContainersResponse is the response for ListContainers.
type listContainersResponse struct {
	NextToken  *string           `json:"NextToken,omitempty"`
	Containers []containerObject `json:"Containers"`
}

// getContainerPolicyResponse is the response for GetContainerPolicy.
type getContainerPolicyResponse struct {
	Policy string `json:"Policy"`
}

// getCorsPolicyResponse is the response for GetCorsPolicy.
type getCorsPolicyResponse struct {
	CorsPolicy []CorsRule `json:"CorsPolicy"`
}

// getLifecyclePolicyResponse is the response for GetLifecyclePolicy.
type getLifecyclePolicyResponse struct {
	LifecyclePolicy string `json:"LifecyclePolicy"`
}

// getMetricPolicyResponse is the response for GetMetricPolicy.
type getMetricPolicyResponse struct {
	MetricPolicy MetricPolicy `json:"MetricPolicy"`
}

// listTagsForResourceResponse is the response for ListTagsForResource.
type listTagsForResourceResponse struct {
	Tags []tagEntry `json:"Tags"`
}

// errorResponse is the standard JSON error response body.
type errorResponse struct {
	Message string `json:"Message"`
}
