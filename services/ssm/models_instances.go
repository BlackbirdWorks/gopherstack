package ssm

import (
	"time"
)

// DescribeEffectiveInstanceAssociationsInput is the request for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsInput struct {
	InstanceID string `json:"InstanceId"`
}

// DescribeEffectiveInstanceAssociationsOutput is the response for DescribeEffectiveInstanceAssociations.
type DescribeEffectiveInstanceAssociationsOutput struct{}

// DescribeInstanceAssociationsStatusInput is the request for DescribeInstanceAssociationsStatus.
type DescribeInstanceAssociationsStatusInput struct {
	InstanceID string `json:"InstanceId"`
}

// DescribeInstanceAssociationsStatusOutput is the response for DescribeInstanceAssociationsStatus.
type DescribeInstanceAssociationsStatusOutput struct{}

// DescribeInstanceInformationInput is the request for DescribeInstanceInformation.
type DescribeInstanceInformationInput struct{}

// DescribeInstanceInformationOutput is the response for DescribeInstanceInformation.
type DescribeInstanceInformationOutput struct{}

// DescribeInstancePatchStatesInput is the request for DescribeInstancePatchStates.
type DescribeInstancePatchStatesInput struct {
	MaxResults  *int64   `json:"MaxResults,omitempty"`
	NextToken   string   `json:"NextToken,omitempty"`
	InstanceIDs []string `json:"InstanceIds"`
}

// DescribeInstancePatchStatesOutput is the response for DescribeInstancePatchStates.
type DescribeInstancePatchStatesOutput struct{}

// InstancePatchStateFilter filters patch states by field.
type InstancePatchStateFilter struct {
	Key    string   `json:"Key"`
	Type   string   `json:"Type,omitempty"`
	Values []string `json:"Values"`
}

// DescribeInstancePatchStatesForPatchGroupInput is the request for DescribeInstancePatchStatesForPatchGroup.
type DescribeInstancePatchStatesForPatchGroupInput struct {
	MaxResults *int64                     `json:"MaxResults,omitempty"`
	NextToken  string                     `json:"NextToken,omitempty"`
	PatchGroup string                     `json:"PatchGroup"`
	Filters    []InstancePatchStateFilter `json:"Filters,omitempty"`
}

// DescribeInstancePatchStatesForPatchGroupOutput is the response for DescribeInstancePatchStatesForPatchGroup.
type DescribeInstancePatchStatesForPatchGroupOutput struct {
	NextToken           string               `json:"NextToken,omitempty"`
	InstancePatchStates []InstancePatchState `json:"InstancePatchStates"`
}

// PatchComplianceData holds the patch compliance data for a single patch.
type PatchComplianceData struct {
	Classification string     `json:"Classification"`
	InstalledTime  *time.Time `json:"InstalledTime,omitempty"`
	KBId           string     `json:"KBId,omitempty"`
	Severity       string     `json:"Severity"`
	State          string     `json:"State"`
	Title          string     `json:"Title"`
}

// DescribeInstancePatchesInput is the request for DescribeInstancePatches.
type DescribeInstancePatchesInput struct {
	MaxResults *int64                    `json:"MaxResults,omitempty"`
	InstanceID string                    `json:"InstanceId"`
	NextToken  string                    `json:"NextToken,omitempty"`
	Filters    []PatchOrchestratorFilter `json:"Filters,omitempty"`
}

// DescribeInstancePatchesOutput is the response for DescribeInstancePatches.
type DescribeInstancePatchesOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Patches   []PatchComplianceData `json:"Patches"`
}

// InstancePropertyStringFilter filters instance properties by string field.
type InstancePropertyStringFilter struct {
	Key      string   `json:"Key"`
	Operator string   `json:"Operator,omitempty"`
	Values   []string `json:"Values"`
}

// InstancePropertyFilter filters instance properties.
type InstancePropertyFilter struct {
	Key      string   `json:"Key"`
	ValueSet []string `json:"ValueSet"`
}

// InstanceProperty represents properties of a managed instance.
type InstanceProperty struct {
	InstanceID      string `json:"InstanceId"`
	Name            string `json:"Name,omitempty"`
	PlatformType    string `json:"PlatformType,omitempty"`
	PlatformName    string `json:"PlatformName,omitempty"`
	PlatformVersion string `json:"PlatformVersion,omitempty"`
	PingStatus      string `json:"PingStatus,omitempty"`
	AgentVersion    string `json:"AgentVersion,omitempty"`
	ActivationID    string `json:"ActivationId,omitempty"`
}

// DescribeInstancePropertiesInput is the request for DescribeInstanceProperties.
type DescribeInstancePropertiesInput struct {
	MaxResults                 *int64                         `json:"MaxResults,omitempty"`
	NextToken                  string                         `json:"NextToken,omitempty"`
	FiltersWithOperator        []InstancePropertyStringFilter `json:"FiltersWithOperator,omitempty"`
	InstancePropertyFilterList []InstancePropertyFilter       `json:"InstancePropertyFilterList,omitempty"`
}

// DescribeInstancePropertiesOutput is the response for DescribeInstanceProperties.
type DescribeInstancePropertiesOutput struct {
	NextToken          string             `json:"NextToken,omitempty"`
	InstanceProperties []InstanceProperty `json:"InstanceProperties"`
}

// ListNodesInput is the request payload.
type ListNodesInput struct{}

// ListNodesOutput is the response payload.
type ListNodesOutput struct{}

// ListNodesSummaryInput is the request payload.
type ListNodesSummaryInput struct{}

// ListNodesSummaryOutput is the response payload.
type ListNodesSummaryOutput struct{}

// NodeInfo represents an SSM managed node (instance).
type NodeInfo struct {
	RegistrationDate time.Time `json:"RegistrationDate"`
	InstanceID       string    `json:"InstanceId"`
	PlatformType     string    `json:"PlatformType"`
	AgentVersion     string    `json:"AgentVersion"`
}

// ListNodesOutputFull has nodes list.
type ListNodesOutputFull struct {
	NextToken string     `json:"NextToken,omitempty"`
	Nodes     []NodeInfo `json:"Nodes"`
}

// ListNodesSummaryOutputFull has summary.
type ListNodesSummaryOutputFull struct {
	NextToken string              `json:"NextToken,omitempty"`
	Summary   []map[string]string `json:"Summary"`
}

// DescribeEffectiveInstanceAssociationsOutputFull has effective associations.
type DescribeEffectiveInstanceAssociationsOutputFull struct {
	NextToken    string                    `json:"NextToken,omitempty"`
	Associations []InstanceAssociationInfo `json:"Associations"`
}

// InstanceAssociationInfo is a minimal association info for an instance.
type InstanceAssociationInfo struct {
	AssociationID      string `json:"AssociationId"`
	Name               string `json:"Name"`
	DocumentVersion    string `json:"DocumentVersion"`
	AssociationVersion string `json:"AssociationVersion"`
}

// DescribeInstanceAssociationsStatusOutputFull has status info.
type DescribeInstanceAssociationsStatusOutputFull struct {
	NextToken                      string                          `json:"NextToken,omitempty"`
	InstanceAssociationStatusInfos []InstanceAssociationStatusInfo `json:"InstanceAssociationStatusInfos"`
}

// InstanceAssociationStatusInfo has status of an association on an instance.
type InstanceAssociationStatusInfo struct {
	ExecutionDate time.Time `json:"ExecutionDate"`
	AssociationID string    `json:"AssociationId"`
	Name          string    `json:"Name"`
	Status        string    `json:"Status"`
}

// DescribeInstanceInformationOutputFull extends the empty stub.
type DescribeInstanceInformationOutputFull struct {
	NextToken               string                `json:"NextToken,omitempty"`
	InstanceInformationList []InstanceInformation `json:"InstanceInformationList"`
}

// InstanceInformation represents info about a managed instance.
type InstanceInformation struct {
	RegistrationDate time.Time `json:"RegistrationDate"`
	InstanceID       string    `json:"InstanceId"`
	PingStatus       string    `json:"PingStatus"`
	AgentVersion     string    `json:"AgentVersion"`
	PlatformType     string    `json:"PlatformType"`
}

// DescribeInstancePatchStatesOutputFull extends the empty stub.
type DescribeInstancePatchStatesOutputFull struct {
	NextToken           string               `json:"NextToken,omitempty"`
	InstancePatchStates []InstancePatchState `json:"InstancePatchStates"`
}

// InstancePatchState represents patch compliance state for an instance.
type InstancePatchState struct {
	OperationStartTime time.Time `json:"OperationStartTime"`
	InstanceID         string    `json:"InstanceId"`
	PatchGroup         string    `json:"PatchGroup"`
	BaselineID         string    `json:"BaselineId"`
	Operation          string    `json:"Operation"`
	FailedCount        int       `json:"FailedCount"`
	InstalledCount     int       `json:"InstalledCount"`
	MissingCount       int       `json:"MissingCount"`
}
