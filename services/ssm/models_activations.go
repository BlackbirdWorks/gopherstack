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
	SyncName string `json:"SyncName"`
	SyncType string `json:"SyncType,omitempty"`
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

// DescribeActivationsInput is the request for DescribeActivations.
type DescribeActivationsInput struct{}

// DescribeActivationsOutput is the response for DescribeActivations.
type DescribeActivationsOutput struct {
	ActivationList []Activation `json:"ActivationList"`
}

// ListResourceDataSyncInput is the request payload.
type ListResourceDataSyncInput struct{}

// ListResourceDataSyncOutput is the response payload.
type ListResourceDataSyncOutput struct{}

// UpdateManagedInstanceRoleInput is the request payload.
type UpdateManagedInstanceRoleInput struct {
	InstanceID string `json:"InstanceId"`
	IamRole    string `json:"IamRole"`
}

// UpdateResourceDataSyncInput is the request payload.
type UpdateResourceDataSyncInput struct {
	SyncName string `json:"SyncName"`
}

// Activation represents an SSM activation for managed instances.
type Activation struct {
	ActivationID        string  `json:"ActivationId"`
	ActivationCode      string  `json:"ActivationCode"`
	Description         string  `json:"Description,omitempty"`
	DefaultInstanceName string  `json:"DefaultInstanceName,omitempty"`
	IamRole             string  `json:"IamRole"`
	RegistrationLimit   int32   `json:"RegistrationLimit"`
	RegistrationsCount  int32   `json:"RegistrationsCount"`
	ExpirationDate      float64 `json:"ExpirationDate"`
	Expired             bool    `json:"Expired"`
	CreatedDate         float64 `json:"CreatedDate"`
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
	SyncName        string  `json:"SyncName"`
	SyncType        string  `json:"SyncType"`
	LastStatus      string  `json:"LastStatus"`
	SyncCreatedTime float64 `json:"SyncCreatedTime"`
	LastSyncTime    float64 `json:"LastSyncTime,omitempty"`
}

// CreateResourceDataSyncInputFull replaces the empty stub for CreateResourceDataSync.
type CreateResourceDataSyncInputFull struct {
	SyncName string `json:"SyncName"`
	SyncType string `json:"SyncType,omitempty"`
}

// ListResourceDataSyncOutputFull extends the empty stub output.
type ListResourceDataSyncOutputFull struct {
	NextToken             string             `json:"NextToken,omitempty"`
	ResourceDataSyncItems []ResourceDataSync `json:"ResourceDataSyncItems"`
}
