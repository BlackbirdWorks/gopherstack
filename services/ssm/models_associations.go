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

// AssociationFilterEntry filters ListAssociations results by InstanceId,
// Name (document name), AssociationId or AssociationName
// (api_op_ListAssociations.go AssociationFilterList member,
// types.AssociationFilterKey).
type AssociationFilterEntry struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ListAssociationsInput is the request payload.
type ListAssociationsInput struct {
	MaxResults            *int32                   `json:"MaxResults,omitempty"`
	NextToken             string                   `json:"NextToken,omitempty"`
	AssociationFilterList []AssociationFilterEntry `json:"AssociationFilterList,omitempty"`
}

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
	Parameters                    map[string][]string                `json:"Parameters,omitempty"`
	Duration                      *int32                             `json:"Duration,omitempty"`
	OutputLocation                *InstanceAssociationOutputLocation `json:"OutputLocation,omitempty"`
	AssociationDispatchAssumeRole string                             `json:"AssociationDispatchAssumeRole,omitempty"`
	AssociationID                 string                             `json:"AssociationId"`
	SyncCompliance                string                             `json:"SyncCompliance,omitempty"`
	DocumentVersion               string                             `json:"DocumentVersion,omitempty"`
	AutomationTargetParameterName string                             `json:"AutomationTargetParameterName,omitempty"`
	ScheduleExpression            string                             `json:"ScheduleExpression,omitempty"`
	ComplianceSeverity            string                             `json:"ComplianceSeverity,omitempty"`
	AssociationName               string                             `json:"AssociationName,omitempty"`
	MaxConcurrency                string                             `json:"MaxConcurrency,omitempty"`
	MaxErrors                     string                             `json:"MaxErrors,omitempty"`
	Targets                       []AssociationTarget                `json:"Targets,omitempty"`
	CalendarNames                 []string                           `json:"CalendarNames,omitempty"`
	ApplyOnlyAtCronInterval       bool                               `json:"ApplyOnlyAtCronInterval,omitempty"`
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

// AssociationStatusInfo is the Status member of Association/AssociationDescription
// (real SDK types.AssociationStatus, api_op_UpdateAssociationStatus.go /
// deserializers.go awsAwsjson11_deserializeDocumentAssociationDescription
// case "Status"). Date/Message/Name are all required on the real type.
type AssociationStatusInfo struct {
	Message        string  `json:"Message"`
	Name           string  `json:"Name"`
	AdditionalInfo string  `json:"AdditionalInfo,omitempty"`
	Date           float64 `json:"Date"`
}

// UpdateAssociationStatusOutput is the response payload.
type UpdateAssociationStatusOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// AssociationStatusValue is the request-side status payload for
// UpdateAssociationStatus (real SDK types.AssociationStatus,
// serializers.go awsAwsjson11_serializeDocumentAssociationStatus). Date and
// Message are both required on the real type -- there is no
// "ExecutionSummary" member anywhere in that shape.
type AssociationStatusValue struct {
	Message        string  `json:"Message"`
	Name           string  `json:"Name"`
	AdditionalInfo string  `json:"AdditionalInfo,omitempty"`
	Date           float64 `json:"Date"`
}

// AssociationTarget is a target for an association (key/values).
type AssociationTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// S3OutputLocation identifies the S3 bucket/prefix/region an association's
// execution results are stored to.
type S3OutputLocation struct {
	OutputS3BucketName string `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix  string `json:"OutputS3KeyPrefix,omitempty"`
	OutputS3Region     string `json:"OutputS3Region,omitempty"`
}

// InstanceAssociationOutputLocation is an S3 bucket where an association's
// execution results are stored (CreateAssociationInput.OutputLocation /
// AssociationDescription.OutputLocation).
type InstanceAssociationOutputLocation struct {
	S3Location *S3OutputLocation `json:"S3Location,omitempty"`
}

// copyAssocOutputLocation deep copies an OutputLocation for an association.
func copyAssocOutputLocation(src *InstanceAssociationOutputLocation) *InstanceAssociationOutputLocation {
	if src == nil {
		return nil
	}

	dst := &InstanceAssociationOutputLocation{}
	if src.S3Location != nil {
		s3loc := *src.S3Location
		dst.S3Location = &s3loc
	}

	return dst
}

// Association represents an SSM association between a document and targets.
type Association struct {
	Overview                      *AssociationOverview               `json:"Overview,omitempty"`
	OutputLocation                *InstanceAssociationOutputLocation `json:"OutputLocation,omitempty"`
	Duration                      *int32                             `json:"Duration,omitempty"`
	Parameters                    map[string][]string                `json:"Parameters,omitempty"`
	Status                        *AssociationStatusInfo             `json:"Status,omitempty"`
	AssociationID                 string                             `json:"AssociationId"`
	Name                          string                             `json:"Name"`
	SyncCompliance                string                             `json:"SyncCompliance,omitempty"`
	ScheduleExpression            string                             `json:"ScheduleExpression,omitempty"`
	AssociationName               string                             `json:"AssociationName,omitempty"`
	DocumentVersion               string                             `json:"DocumentVersion,omitempty"`
	AutomationTargetParameterName string                             `json:"AutomationTargetParameterName,omitempty"`
	MaxErrors                     string                             `json:"MaxErrors,omitempty"`
	ComplianceSeverity            string                             `json:"ComplianceSeverity,omitempty"`
	InstanceID                    string                             `json:"InstanceId,omitempty"`
	MaxConcurrency                string                             `json:"MaxConcurrency,omitempty"`
	AssociationDispatchAssumeRole string                             `json:"AssociationDispatchAssumeRole,omitempty"`
	AssociationVersion            string                             `json:"AssociationVersion,omitempty"`
	Targets                       []AssociationTarget                `json:"Targets,omitempty"`
	CalendarNames                 []string                           `json:"CalendarNames,omitempty"`
	LastUpdateAssociationDate     float64                            `json:"LastUpdateAssociationDate"`
	ApplyOnlyAtCronInterval       bool                               `json:"ApplyOnlyAtCronInterval,omitempty"`
}

// AssociationOverview is a summary of an association.
type AssociationOverview struct {
	Status string `json:"Status"`
}

// CreateAssociationInput is the request payload for CreateAssociation.
type CreateAssociationInput struct {
	Parameters                    map[string][]string                `json:"Parameters,omitempty"`
	OutputLocation                *InstanceAssociationOutputLocation `json:"OutputLocation,omitempty"`
	Duration                      *int32                             `json:"Duration,omitempty"`
	AutomationTargetParameterName string                             `json:"AutomationTargetParameterName,omitempty"`
	ComplianceSeverity            string                             `json:"ComplianceSeverity,omitempty"`
	SyncCompliance                string                             `json:"SyncCompliance,omitempty"`
	ScheduleExpression            string                             `json:"ScheduleExpression,omitempty"`
	AssociationDispatchAssumeRole string                             `json:"AssociationDispatchAssumeRole,omitempty"`
	Name                          string                             `json:"Name"`
	AssociationName               string                             `json:"AssociationName,omitempty"`
	InstanceID                    string                             `json:"InstanceId,omitempty"`
	DocumentVersion               string                             `json:"DocumentVersion,omitempty"`
	MaxConcurrency                string                             `json:"MaxConcurrency,omitempty"`
	MaxErrors                     string                             `json:"MaxErrors,omitempty"`
	CalendarNames                 []string                           `json:"CalendarNames,omitempty"`
	Targets                       []AssociationTarget                `json:"Targets,omitempty"`
	Tags                          []Tag                              `json:"Tags,omitempty"`
	ApplyOnlyAtCronInterval       bool                               `json:"ApplyOnlyAtCronInterval,omitempty"`
}

// CreateAssociationOutput is the response payload for CreateAssociation.
type CreateAssociationOutput struct {
	AssociationDescription Association `json:"AssociationDescription"`
}

// CreateAssociationBatchRequestEntry is a single entry in a batch create association request.
type CreateAssociationBatchRequestEntry struct {
	Parameters                    map[string][]string                `json:"Parameters,omitempty"`
	OutputLocation                *InstanceAssociationOutputLocation `json:"OutputLocation,omitempty"`
	Duration                      *int32                             `json:"Duration,omitempty"`
	AutomationTargetParameterName string                             `json:"AutomationTargetParameterName,omitempty"`
	ComplianceSeverity            string                             `json:"ComplianceSeverity,omitempty"`
	SyncCompliance                string                             `json:"SyncCompliance,omitempty"`
	ScheduleExpression            string                             `json:"ScheduleExpression,omitempty"`
	AssociationDispatchAssumeRole string                             `json:"AssociationDispatchAssumeRole,omitempty"`
	Name                          string                             `json:"Name"`
	AssociationName               string                             `json:"AssociationName,omitempty"`
	InstanceID                    string                             `json:"InstanceId,omitempty"`
	DocumentVersion               string                             `json:"DocumentVersion,omitempty"`
	MaxConcurrency                string                             `json:"MaxConcurrency,omitempty"`
	MaxErrors                     string                             `json:"MaxErrors,omitempty"`
	CalendarNames                 []string                           `json:"CalendarNames,omitempty"`
	Targets                       []AssociationTarget                `json:"Targets,omitempty"`
	ApplyOnlyAtCronInterval       bool                               `json:"ApplyOnlyAtCronInterval,omitempty"`
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

// ListAssociationsOutputFull extends the stub list output.
type ListAssociationsOutputFull struct {
	NextToken    string        `json:"NextToken,omitempty"`
	Associations []Association `json:"Associations"`
}

// AssociationVersionInfo is the narrow element type for ListAssociationVersionsOutput.
// Real AWS SDK v2 types.AssociationVersionInfo (api_op_ListAssociationVersions.go).
type AssociationVersionInfo struct {
	OutputLocation                *InstanceAssociationOutputLocation `json:"OutputLocation,omitempty"`
	Duration                      *int32                             `json:"Duration,omitempty"`
	Parameters                    map[string][]string                `json:"Parameters,omitempty"`
	MaxErrors                     string                             `json:"MaxErrors,omitempty"`
	DocumentVersion               string                             `json:"DocumentVersion,omitempty"`
	AssociationDispatchAssumeRole string                             `json:"AssociationDispatchAssumeRole,omitempty"`
	AssociationID                 string                             `json:"AssociationId,omitempty"`
	AssociationName               string                             `json:"AssociationName,omitempty"`
	AssociationVersion            string                             `json:"AssociationVersion,omitempty"`
	ComplianceSeverity            string                             `json:"ComplianceSeverity,omitempty"`
	SyncCompliance                string                             `json:"SyncCompliance,omitempty"`
	MaxConcurrency                string                             `json:"MaxConcurrency,omitempty"`
	ScheduleExpression            string                             `json:"ScheduleExpression,omitempty"`
	Name                          string                             `json:"Name,omitempty"`
	CalendarNames                 []string                           `json:"CalendarNames,omitempty"`
	Targets                       []AssociationTarget                `json:"Targets,omitempty"`
	CreatedDate                   float64                            `json:"CreatedDate,omitempty"`
	ApplyOnlyAtCronInterval       bool                               `json:"ApplyOnlyAtCronInterval,omitempty"`
}

// ListAssociationVersionsOutputFull extends the empty output.
type ListAssociationVersionsOutputFull struct {
	NextToken           string                   `json:"NextToken,omitempty"`
	AssociationVersions []AssociationVersionInfo `json:"AssociationVersions"`
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
