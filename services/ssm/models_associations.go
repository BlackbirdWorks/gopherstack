package ssm

// DeleteAssociationOutput is the response for DeleteAssociation.
type DeleteAssociationOutput struct{}

// StartAssociationsOnceOutput is the response for StartAssociationsOnce.
type StartAssociationsOnceOutput struct{}

// DeleteAssociationInput is the request for DeleteAssociation.
type DeleteAssociationInput struct {
	AssociationID string `json:"AssociationId,omitempty"`
	Name          string `json:"Name,omitempty"`
	InstanceID    string `json:"InstanceId,omitempty"`
}

// DescribeAssociationInput is the request for DescribeAssociation.
type DescribeAssociationInput struct {
	AssociationID string `json:"AssociationId,omitempty"`
	Name          string `json:"Name,omitempty"`
	InstanceID    string `json:"InstanceId,omitempty"`
}

// DescribeAssociationOutput is the response for DescribeAssociation.
type DescribeAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// DescribeAssociationExecutionTargetsInput is the request for DescribeAssociationExecutionTargets.
type DescribeAssociationExecutionTargetsInput struct {
	AssociationID string `json:"AssociationId"`
	ExecutionID   string `json:"ExecutionId,omitempty"`
}

// DescribeAssociationExecutionTargetsOutput is the response for DescribeAssociationExecutionTargets.
type DescribeAssociationExecutionTargetsOutput struct{}

// DescribeAssociationExecutionsInput is the request for DescribeAssociationExecutions.
type DescribeAssociationExecutionsInput struct {
	AssociationID string `json:"AssociationId"`
}

// DescribeAssociationExecutionsOutput is the response for DescribeAssociationExecutions.
type DescribeAssociationExecutionsOutput struct{}

// ListAssociationVersionsInput is the request payload.
type ListAssociationVersionsInput struct {
	AssociationID string `json:"AssociationId"`
}

// ListAssociationVersionsOutput is the response payload.
type ListAssociationVersionsOutput struct{}

// ListAssociationsInput is the request payload.
type ListAssociationsInput struct{}

// ListAssociationsOutput is the response payload.
type ListAssociationsOutput struct {
	Associations []Association `json:"Associations"`
}

// StartAssociationsOnceInput is the request payload.
type StartAssociationsOnceInput struct {
	AssociationIDs []string `json:"AssociationIds"`
}

// UpdateAssociationInput is the request payload.
type UpdateAssociationInput struct {
	AssociationID   string              `json:"AssociationId"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// UpdateAssociationOutput is the response payload.
type UpdateAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// UpdateAssociationStatusInput is the request payload.
type UpdateAssociationStatusInput struct {
	InstanceID        string                 `json:"InstanceId"`
	Name              string                 `json:"Name"`
	AssociationStatus AssociationStatusValue `json:"AssociationStatus"`
}

// UpdateAssociationStatusOutput is the response payload.
type UpdateAssociationStatusOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// AssociationTarget is a target for an association (key/values).
type AssociationTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// Association represents an SSM association between a document and targets.
type Association struct {
	AssociationID             string               `json:"AssociationId"`
	AssociationName           string               `json:"AssociationName,omitempty"`
	DocumentVersion           string               `json:"DocumentVersion,omitempty"`
	InstanceID                string               `json:"InstanceId,omitempty"`
	Name                      string               `json:"Name"`
	Overview                  *AssociationOverview `json:"Overview,omitempty"`
	Parameters                map[string][]string  `json:"Parameters,omitempty"`
	Targets                   []AssociationTarget  `json:"Targets,omitempty"`
	LastUpdateAssociationDate float64              `json:"LastUpdateAssociationDate"`
}

// AssociationOverview is a summary of an association.
type AssociationOverview struct {
	Status string `json:"Status"`
}

// CreateAssociationInput is the request payload for CreateAssociation.
type CreateAssociationInput struct {
	Name            string              `json:"Name"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	InstanceID      string              `json:"InstanceId,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// CreateAssociationOutput is the response payload for CreateAssociation.
type CreateAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// CreateAssociationBatchRequestEntry is a single entry in a batch create association request.
type CreateAssociationBatchRequestEntry struct {
	Name            string              `json:"Name"`
	AssociationName string              `json:"AssociationName,omitempty"`
	DocumentVersion string              `json:"DocumentVersion,omitempty"`
	InstanceID      string              `json:"InstanceId,omitempty"`
	Parameters      map[string][]string `json:"Parameters,omitempty"`
	Targets         []AssociationTarget `json:"Targets,omitempty"`
}

// FailedCreateAssociation represents a failed association entry in a batch.
type FailedCreateAssociation struct {
	Message string                             `json:"Message"`
	Fault   string                             `json:"Fault"`
	Entry   CreateAssociationBatchRequestEntry `json:"Entry"`
}

// CreateAssociationBatchInput is the request payload for CreateAssociationBatch.
type CreateAssociationBatchInput struct {
	Entries []CreateAssociationBatchRequestEntry `json:"Entries"`
}

// CreateAssociationBatchOutput is the response payload for CreateAssociationBatch.
type CreateAssociationBatchOutput struct {
	Failed     []FailedCreateAssociation `json:"Failed"`
	Successful []Association             `json:"Successful"`
}

// AssociationStatusValue is the status payload in UpdateAssociationStatus.
type AssociationStatusValue struct {
	Name             string `json:"Name"`
	ExecutionSummary string `json:"ExecutionSummary,omitempty"`
}

// ListAssociationsOutputFull extends the stub list output.
type ListAssociationsOutputFull struct {
	NextToken    string        `json:"NextToken,omitempty"`
	Associations []Association `json:"Associations"`
}

// ListAssociationVersionsOutputFull extends the empty output.
type ListAssociationVersionsOutputFull struct {
	NextToken           string        `json:"NextToken,omitempty"`
	AssociationVersions []Association `json:"AssociationVersions"`
}

// DescribeAssociationExecutionsOutputFull extends the empty output.
type DescribeAssociationExecutionsOutputFull struct {
	NextToken             string                 `json:"NextToken,omitempty"`
	AssociationExecutions []AssociationExecution `json:"AssociationExecutions"`
}

// AssociationExecution represents a single execution record of an association.
type AssociationExecution struct {
	AssociationID string  `json:"AssociationId"`
	ExecutionID   string  `json:"ExecutionId"`
	Status        string  `json:"Status"`
	ExecutionDate float64 `json:"ExecutionDate"`
}

// DescribeAssociationExecutionTargetsOutputFull extends the empty output.
type DescribeAssociationExecutionTargetsOutputFull struct {
	NextToken                   string                       `json:"NextToken,omitempty"`
	AssociationExecutionTargets []AssociationExecutionTarget `json:"AssociationExecutionTargets"`
}

// AssociationExecutionTarget represents a single target of an association execution.
type AssociationExecutionTarget struct {
	AssociationID string `json:"AssociationId"`
	ExecutionID   string `json:"ExecutionId"`
	ResourceID    string `json:"ResourceId"`
	ResourceType  string `json:"ResourceType"`
	Status        string `json:"Status"`
}

// UpdateAssociationStatusOutputFull extends the empty stub.
type UpdateAssociationStatusOutputFull struct {
	AssociationDescription Association `json:"AssociationDescription"`
}
