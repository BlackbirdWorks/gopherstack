package ssm

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

// InstanceInformationStringFilterEntry filters DescribeInstanceInformation
// results by a free-form key (api_op_DescribeInstanceInformation.go Filters
// member, types.InstanceInformationStringFilter).
type InstanceInformationStringFilterEntry struct {
	Key    string   `json:"Key,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// InstanceInformationFilterEntry filters DescribeInstanceInformation results
// by the legacy InstanceInformationFilterList member (deprecated by the real
// API in favor of Filters/InstanceInformationStringFilter above, but still a
// live request field -- types.InstanceInformationFilter).
type InstanceInformationFilterEntry struct {
	Key      string   `json:"key,omitempty"`
	ValueSet []string `json:"valueSet,omitempty"`
}

// DescribeInstanceInformationInput is the request for DescribeInstanceInformation.
type DescribeInstanceInformationInput struct {
	MaxResults                    *int32                                 `json:"MaxResults,omitempty"`
	NextToken                     string                                 `json:"NextToken,omitempty"`
	Filters                       []InstanceInformationStringFilterEntry `json:"Filters,omitempty"`
	InstanceInformationFilterList []InstanceInformationFilterEntry       `json:"InstanceInformationFilterList,omitempty"`
}

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
	Classification string  `json:"Classification"`
	KBId           string  `json:"KBId,omitempty"`
	Severity       string  `json:"Severity"`
	State          string  `json:"State"`
	Title          string  `json:"Title"`
	InstalledTime  float64 `json:"InstalledTime,omitempty"`
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

// ListNodesInput is the request payload. Matches ListNodesInput in the
// pinned SDK (api_op_ListNodes.go:31-53): Filters, MaxResults, NextToken and
// SyncName are all optional -- unlike ListNodesSummaryInput's Aggregators,
// nothing here is required, so an empty body is genuinely valid. The
// previous literal struct{} still had a real bug: it silently discarded
// Filters/MaxResults/NextToken/SyncName from every real request instead of
// declining to bind them because none were required.
type ListNodesInput struct {
	MaxResults *int32       `json:"MaxResults,omitempty"`
	NextToken  string       `json:"NextToken,omitempty"`
	SyncName   string       `json:"SyncName,omitempty"`
	Filters    []NodeFilter `json:"Filters,omitempty"`
}

// ListNodesOutput is the response payload.
type ListNodesOutput struct{}

// NodeInstanceInfo mirrors types.InstanceInfo (types/types.go:2693-2747,
// ssm@v1.73.4), the payload of a Node's NodeType.Instance member. Only
// AgentVersion and PlatformType have backing state in this in-memory
// implementation (see nodeAttributeValue); every other member (AgentType,
// AvailabilityZone, ComputerName, InstanceStatus, IpAddress, ManagedStatus,
// Name, PlatformName, PlatformVersion, ResourceType, SourceId,
// SourceLocation, SourceType) has none and is left absent rather than
// fabricated.
type NodeInstanceInfo struct {
	AgentVersion string `json:"AgentVersion,omitempty"`
	PlatformType string `json:"PlatformType,omitempty"`
}

// NodeType mirrors the types.NodeType tagged union (types/types.go:4172-
// 4189, ssm@v1.73.4); Instance is its only known member
// (NodeTypeMemberInstance), wire key "Instance"
// (deserializers.go:36943).
type NodeType struct {
	Instance *NodeInstanceInfo `json:"Instance,omitempty"`
}

// NodeOwnerInfo mirrors types.NodeOwnerInfo (types/types.go:4155-4171,
// ssm@v1.73.4). This backend has no account/OU tracking, so Node.Owner is
// always nil rather than fabricated.
type NodeOwnerInfo struct {
	AccountID              string `json:"AccountId,omitempty"`
	OrganizationalUnitID   string `json:"OrganizationalUnitId,omitempty"`
	OrganizationalUnitPath string `json:"OrganizationalUnitPath,omitempty"`
}

// Node is the real ListNodesOutput element, types.Node (types/types.go:4087-
// 4106, ssm@v1.73.4): CaptureTime, Id, NodeType, Owner and Region, all
// optional. This backend previously serialized NodeInfo directly under
// top-level InstanceId/PlatformType/AgentVersion/RegistrationDate keys, none
// of which exist on the real wire -- PlatformType and AgentVersion are real
// fields, but nested three levels down inside NodeType.Instance, and
// RegistrationDate does not exist at all (the real field is CaptureTime,
// deserializers.go:36710, an epoch-seconds number like every other
// timestamp in this service).
type Node struct {
	NodeType    *NodeType      `json:"NodeType,omitempty"`
	Owner       *NodeOwnerInfo `json:"Owner,omitempty"`
	ID          string         `json:"Id,omitempty"`
	Region      string         `json:"Region,omitempty"`
	CaptureTime float64        `json:"CaptureTime,omitempty"`
}

// NodeAggregator mirrors types.NodeAggregator in the pinned SDK
// (types/types.go:4109-4132): AggregatorType, AttributeName and TypeName are
// all required. Nested Aggregators (multi-level grouping) are accepted on
// the wire but not applied -- this backend only groups by the top-level
// AttributeName.
type NodeAggregator struct {
	AggregatorType string           `json:"AggregatorType"`
	AttributeName  string           `json:"AttributeName"`
	TypeName       string           `json:"TypeName"`
	Aggregators    []NodeAggregator `json:"Aggregators,omitempty"`
}

// NodeFilter mirrors types.NodeFilter (types/types.go:4135-4152).
type NodeFilter struct {
	Key    string   `json:"Key"`
	Type   string   `json:"Type,omitempty"`
	Values []string `json:"Values"`
}

// ListNodesSummaryInput is the request payload. Field set matches
// ListNodesSummaryInput in the pinned SDK (api_op_ListNodesSummary.go:31-62):
// Aggregators is required.
type ListNodesSummaryInput struct {
	MaxResults  *int32           `json:"MaxResults,omitempty"`
	NextToken   string           `json:"NextToken,omitempty"`
	SyncName    string           `json:"SyncName,omitempty"`
	Aggregators []NodeAggregator `json:"Aggregators"`
	Filters     []NodeFilter     `json:"Filters,omitempty"`
}

// ListNodesSummaryOutput is the response payload.
type ListNodesSummaryOutput struct{}

// NodeInfo is this backend's internal representation of a managed node,
// used for filtering/aggregation (nodeAttributeValue, matchesNodeFilter,
// aggregateNodes) and converted to the real wire shape Node by nodeToWire
// before a ListNodes response is built. It is not itself marshaled.
type NodeInfo struct {
	InstanceID       string
	PlatformType     string
	AgentVersion     string
	RegistrationDate float64
}

// ListNodesOutputFull has nodes list.
type ListNodesOutputFull struct {
	NextToken string `json:"NextToken,omitempty"`
	Nodes     []Node `json:"Nodes"`
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
	AssociationID string  `json:"AssociationId"`
	Name          string  `json:"Name"`
	Status        string  `json:"Status"`
	ExecutionDate float64 `json:"ExecutionDate"`
}

// DescribeInstanceInformationOutputFull extends the empty stub.
type DescribeInstanceInformationOutputFull struct {
	NextToken               string                `json:"NextToken,omitempty"`
	InstanceInformationList []InstanceInformation `json:"InstanceInformationList"`
}

// InstanceInformation represents info about a managed instance.
type InstanceInformation struct {
	InstanceID       string  `json:"InstanceId"`
	PingStatus       string  `json:"PingStatus"`
	AgentVersion     string  `json:"AgentVersion"`
	PlatformType     string  `json:"PlatformType"`
	RegistrationDate float64 `json:"RegistrationDate"`
}

// DescribeInstancePatchStatesOutputFull extends the empty stub.
type DescribeInstancePatchStatesOutputFull struct {
	NextToken           string               `json:"NextToken,omitempty"`
	InstancePatchStates []InstancePatchState `json:"InstancePatchStates"`
}

// InstancePatchState represents patch compliance state for an instance.
type InstancePatchState struct {
	InstanceID         string  `json:"InstanceId"`
	PatchGroup         string  `json:"PatchGroup"`
	BaselineID         string  `json:"BaselineId"`
	Operation          string  `json:"Operation"`
	OperationStartTime float64 `json:"OperationStartTime"`
	FailedCount        int     `json:"FailedCount"`
	InstalledCount     int     `json:"InstalledCount"`
	MissingCount       int     `json:"MissingCount"`
}
