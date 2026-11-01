package lambdaweb

// RevisionInput is the user-supplied part of a revision.
type RevisionInput struct {
	S3            S3Object
	Description   string
	KmsKeyArn     string
	Runtime       string
	ServiceConfig ServiceConfig
}

// ScalingInput carries an endpoint's scalingConfig; a nil MaxEnvironments clears it.
type ScalingInput struct {
	MaxEnvironments *int32
}

// ThrottleInput carries an endpoint's throttleConfig; a nil RateLimit clears it.
type ThrottleInput struct {
	RateLimit *int32
}

// EndpointInput is the user-supplied part of an endpoint.
type EndpointInput struct {
	Scaling            *ScalingInput
	Throttle           *ThrottleInput
	Name               string
	Description        string
	Type               string
	AuthType           string
	AutoDeploymentMode string
	Regions            []string
	Weights            []RevisionWeight
}

// EndpointUpdate is a partial endpoint update; nil/empty fields keep the current value.
type EndpointUpdate struct {
	Description        *string
	Scaling            *ScalingInput
	Throttle           *ThrottleInput
	AuthType           string
	AutoDeploymentMode string
	Weights            []RevisionWeight
	WeightsSet         bool
}

// CreateFunctionInput is CreateWebFunction's request.
type CreateFunctionInput struct {
	Revision *RevisionInput
	Endpoint *EndpointInput
	Tags     map[string]string
	Name     string
}

// CreatedFunction is CreateWebFunction's result.
type CreatedFunction struct {
	Function *Function
	Revision *Revision
	Endpoint *Endpoint
}

// Filter is a list filter.
type Filter struct {
	Name   string
	Values []string
}

// ListOptions carries list pagination and filters.
type ListOptions struct {
	NextToken  string
	Filters    []Filter
	MaxResults int
}
