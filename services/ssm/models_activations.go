package ssm

type CreateResourceDataSyncOutput struct{}

// DeleteActivationOutput is the response for DeleteActivation.
type DeleteActivationOutput struct{}

// DeleteResourceDataSyncOutput is the response for DeleteResourceDataSync.
type DeleteResourceDataSyncOutput struct{}

// DeregisterManagedInstanceOutput is the response for DeregisterManagedInstance.
type DeregisterManagedInstanceOutput struct{}

// UpdateManagedInstanceRoleOutput is the response for UpdateManagedInstanceRole.
type UpdateManagedInstanceRoleOutput struct{}

// UpdateResourceDataSyncOutput is the response for UpdateResourceDataSync.
type UpdateResourceDataSyncOutput struct{}

// CreateResourceDataSyncInput is the request for CreateResourceDataSync.
type CreateResourceDataSyncInput struct {
	S3Destination *ResourceDataSyncS3Destination `json:"S3Destination,omitempty"`
	SyncSource    *ResourceDataSyncSource        `json:"SyncSource,omitempty"`
	SyncName      string                         `json:"SyncName"`
	SyncType      string                         `json:"SyncType,omitempty"`
}

// ResourceDataSyncS3Destination mirrors types.ResourceDataSyncS3Destination
// (types.go:5561). DestinationDataSharing (Organizations cross-account
// prefix sharing) is deliberately not modeled, matching the same
// shallow-scalar convention ResourceDataSyncSource already documents for its
// own AwsOrganizationsSource member.
type ResourceDataSyncS3Destination struct {
	BucketName   string `json:"BucketName"`
	Region       string `json:"Region"`
	SyncFormat   string `json:"SyncFormat"`
	AWSKMSKeyARN string `json:"AWSKMSKeyARN,omitempty"`
	Prefix       string `json:"Prefix,omitempty"`
}

// DeleteActivationInput is the request for DeleteActivation.
type DeleteActivationInput struct {
	ActivationID string `json:"ActivationId"`
}

// DeleteResourceDataSyncInput is the request for DeleteResourceDataSync.
type DeleteResourceDataSyncInput struct {
	SyncName string `json:"SyncName"`
}

// DeregisterManagedInstanceInput is the request for DeregisterManagedInstance.
type DeregisterManagedInstanceInput struct {
	InstanceID string `json:"InstanceId"`
}

// DescribeActivationsFilter filters DescribeActivations results by
// ActivationIds, DefaultInstanceName or IamRole (api_op_DescribeActivations.go,
// types.DescribeActivationsFilterKeys).
type DescribeActivationsFilter struct {
	FilterKey    string   `json:"FilterKey,omitempty"`
	FilterValues []string `json:"FilterValues,omitempty"`
}

// DescribeActivationsInput is the request for DescribeActivations.
type DescribeActivationsInput struct {
	MaxResults *int32                      `json:"MaxResults,omitempty"`
	NextToken  string                      `json:"NextToken,omitempty"`
	Filters    []DescribeActivationsFilter `json:"Filters,omitempty"`
}

// DescribeActivationsOutput is the response for DescribeActivations.
type DescribeActivationsOutput struct {
	NextToken      string       `json:"NextToken,omitempty"`
	ActivationList []Activation `json:"ActivationList"`
}

// ListResourceDataSyncInput is the request payload.
type ListResourceDataSyncInput struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	SyncType   string `json:"SyncType,omitempty"`
}

// ListResourceDataSyncOutput is the response payload.
type ListResourceDataSyncOutput struct{}

// UpdateManagedInstanceRoleInput is the request payload.
type UpdateManagedInstanceRoleInput struct {
	InstanceID string `json:"InstanceId"`
	IamRole    string `json:"IamRole"`
}

// ResourceDataSyncSource mirrors types.ResourceDataSyncSource (types.go:5593)
// on the request side and types.ResourceDataSyncSourceWithState (types.go:5641)
// on the response side -- both wire shapes share the same field set here.
// AwsOrganizationsSource is deliberately not modeled, matching this
// backend's established shallow-scalar convention for optional deep-nested
// sync-source config (same simplification Runbook/StartAutomationExecutionInput
// already make for their own optional nested types).
type ResourceDataSyncSource struct {
	SourceType              string   `json:"SourceType"`
	SourceRegions           []string `json:"SourceRegions"`
	EnableAllOpsDataSources bool     `json:"EnableAllOpsDataSources,omitempty"`
	IncludeFutureRegions    bool     `json:"IncludeFutureRegions,omitempty"`
}

// UpdateResourceDataSyncInput is the request payload.
type UpdateResourceDataSyncInput struct {
	SyncSource *ResourceDataSyncSource `json:"SyncSource"`
	SyncName   string                  `json:"SyncName"`
	SyncType   string                  `json:"SyncType"`
}

// Activation represents an SSM activation for managed instances.
type Activation struct {
	ActivationID        string  `json:"ActivationId"`
	ActivationCode      string  `json:"ActivationCode"`
	Description         string  `json:"Description,omitempty"`
	DefaultInstanceName string  `json:"DefaultInstanceName,omitempty"`
	IamRole             string  `json:"IamRole"`
	Tags                []Tag   `json:"Tags,omitempty"`
	ExpirationDate      float64 `json:"ExpirationDate"`
	CreatedDate         float64 `json:"CreatedDate"`
	RegistrationLimit   int32   `json:"RegistrationLimit"`
	RegistrationsCount  int32   `json:"RegistrationsCount"`
	Expired             bool    `json:"Expired"`
}

// CreateActivationInput is the request payload for CreateActivation.
type CreateActivationInput struct {
	DefaultInstanceName string  `json:"DefaultInstanceName,omitempty"`
	Description         string  `json:"Description,omitempty"`
	IamRole             string  `json:"IamRole"`
	Tags                []Tag   `json:"Tags,omitempty"`
	ExpirationDate      float64 `json:"ExpirationDate,omitempty"`
	RegistrationLimit   int32   `json:"RegistrationLimit,omitempty"`
}

// CreateActivationOutput is the response payload for CreateActivation.
type CreateActivationOutput struct {
	ActivationCode string `json:"ActivationCode"`
	ActivationID   string `json:"ActivationId"`
}

// ResourceDataSync represents a resource data sync configuration.
type ResourceDataSync struct {
	SyncSource      *ResourceDataSyncSource        `json:"SyncSource,omitempty"`
	S3Destination   *ResourceDataSyncS3Destination `json:"S3Destination,omitempty"`
	SyncName        string                         `json:"SyncName"`
	SyncType        string                         `json:"SyncType"`
	LastStatus      string                         `json:"LastStatus"`
	SyncCreatedTime float64                        `json:"SyncCreatedTime"`
	LastSyncTime    float64                        `json:"LastSyncTime,omitempty"`
}

// ListResourceDataSyncOutputFull extends the empty stub output.
type ListResourceDataSyncOutputFull struct {
	NextToken             string             `json:"NextToken,omitempty"`
	ResourceDataSyncItems []ResourceDataSync `json:"ResourceDataSyncItems"`
}
