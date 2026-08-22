package scheduler

import "context"

// scheduleTargetRetryPolicy mirrors RetryPolicy for handler input/output.
type scheduleTargetRetryPolicy struct {
	MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     int `json:"MaximumRetryAttempts,omitempty"`
}

// scheduleTargetDeadLetterConfig mirrors DeadLetterConfig for handler input/output.
type scheduleTargetDeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// scheduleTargetEventBridgeParameters mirrors EventBridgeParameters for handler input/output.
type scheduleTargetEventBridgeParameters struct {
	DetailType string `json:"DetailType,omitempty"`
	Source     string `json:"Source,omitempty"`
}

// scheduleTargetKinesisParameters mirrors KinesisParameters for handler input/output.
type scheduleTargetKinesisParameters struct {
	PartitionKey string `json:"PartitionKey,omitempty"`
}

// scheduleTargetSqsParameters mirrors SqsParameters for handler input/output.
type scheduleTargetSqsParameters struct {
	MessageGroupID string `json:"MessageGroupId,omitempty"`
}

// scheduleTargetSageMakerPipelineParam mirrors a pipeline parameter name/value.
type scheduleTargetSageMakerPipelineParam struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// scheduleTargetSageMakerPipelineParameters mirrors SageMakerPipelineParameters for handler.
type scheduleTargetSageMakerPipelineParameters struct {
	PipelineParameterList []scheduleTargetSageMakerPipelineParam `json:"PipelineParameterList,omitempty"`
}

// scheduleTargetEcsAwsvpcConfiguration mirrors EcsAwsvpcConfiguration for handler input/output.
type scheduleTargetEcsAwsvpcConfiguration struct {
	AssignPublicIP string   `json:"AssignPublicIp,omitempty"`
	SecurityGroups []string `json:"SecurityGroups,omitempty"`
	Subnets        []string `json:"Subnets,omitempty"`
}

// scheduleTargetEcsNetworkConfiguration mirrors EcsNetworkConfiguration for handler
// input/output. The wrapped field uses a lower-camel key -- unlike every other member
// name in this file -- because that's what the real SDK's
// awsRestjson1_(de)serializeDocumentNetworkConfiguration actually emits/expects.
type scheduleTargetEcsNetworkConfiguration struct {
	AwsvpcConfiguration *scheduleTargetEcsAwsvpcConfiguration `json:"awsvpcConfiguration,omitempty"`
}

// scheduleTargetEcsCapacityProviderStrategyItem mirrors EcsCapacityProviderStrategyItem.
// Real SDK's serializer/deserializer for this shape use lower-camel keys
// ("capacityProvider"/"base"/"weight"), unlike the rest of EcsParameters.
type scheduleTargetEcsCapacityProviderStrategyItem struct {
	CapacityProvider string `json:"capacityProvider"`
	Base             int    `json:"base,omitempty"`
	Weight           int    `json:"weight,omitempty"`
}

// scheduleTargetEcsPlacementConstraint mirrors EcsPlacementConstraint for handler
// input/output. Real SDK uses lower-camel keys ("expression"/"type") for this shape.
type scheduleTargetEcsPlacementConstraint struct {
	Expression string `json:"expression,omitempty"`
	Type       string `json:"type,omitempty"`
}

// scheduleTargetEcsPlacementStrategy mirrors EcsPlacementStrategy for handler
// input/output. Real SDK uses lower-camel keys ("field"/"type") for this shape.
type scheduleTargetEcsPlacementStrategy struct {
	Field string `json:"field,omitempty"`
	Type  string `json:"type,omitempty"`
}

// scheduleTargetEcsParameters mirrors EcsParameters for handler input/output.
//
// Tags is []map[string]string, not a list of {Key,Value} objects -- real SDK's
// EcsParameters.Tags is a list of free-form single-entry maps (e.g.
// [{"env":"prod"}]), serialized by awsRestjson1_(de)serializeDocumentTags /
// ...TagMap, which round-trip an arbitrary map[string]string per element.
type scheduleTargetEcsParameters struct {
	NetworkConfiguration     *scheduleTargetEcsNetworkConfiguration          `json:"NetworkConfiguration,omitempty"`
	PropagateTags            string                                          `json:"PropagateTags,omitempty"`
	TaskDefinitionArn        string                                          `json:"TaskDefinitionArn,omitempty"`
	LaunchType               string                                          `json:"LaunchType,omitempty"`
	PlatformVersion          string                                          `json:"PlatformVersion,omitempty"`
	Group                    string                                          `json:"Group,omitempty"`
	ReferenceID              string                                          `json:"ReferenceId,omitempty"`
	PlacementConstraints     []scheduleTargetEcsPlacementConstraint          `json:"PlacementConstraints,omitempty"`
	PlacementStrategy        []scheduleTargetEcsPlacementStrategy            `json:"PlacementStrategy,omitempty"`
	Tags                     []map[string]string                             `json:"Tags,omitempty"`
	CapacityProviderStrategy []scheduleTargetEcsCapacityProviderStrategyItem `json:"CapacityProviderStrategy,omitempty"`
	TaskCount                int                                             `json:"TaskCount,omitempty"`
	EnableECSManagedTags     bool                                            `json:"EnableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                                            `json:"EnableExecuteCommand,omitempty"`
}

// scheduleTarget holds the ARN, IAM role, and optional custom input for a schedule target.
type scheduleTarget struct {
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
}

// scheduleFlexibleTimeWindow holds the flexible time window configuration for a schedule.
type scheduleFlexibleTimeWindow struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes"`
}

type scheduleInput struct {
	EndDate                    *float64                   `json:"EndDate,omitempty"`
	StartDate                  *float64                   `json:"StartDate,omitempty"`
	Target                     scheduleTarget             `json:"Target"`
	ScheduleExpressionTimezone string                     `json:"ScheduleExpressionTimezone"`
	Description                string                     `json:"Description"`
	Name                       string                     `json:"Name"`
	State                      string                     `json:"State"`
	ScheduleExpression         string                     `json:"ScheduleExpression"`
	GroupName                  string                     `json:"GroupName"`
	ActionAfterCompletion      string                     `json:"ActionAfterCompletion,omitempty"`
	KmsKeyArn                  string                     `json:"KmsKeyArn,omitempty"`
	ClientToken                string                     `json:"ClientToken,omitempty"`
	FlexibleTimeWindow         scheduleFlexibleTimeWindow `json:"FlexibleTimeWindow"`
}

type createScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleCreateSchedule(ctx context.Context, in *scheduleInput) (*createScheduleOutput, error) {
	state := in.State
	if state == "" {
		state = scheduleStateEnabled
	}

	if err := validateActionAfterCompletion(in.ActionAfterCompletion); err != nil {
		return nil, err
	}

	groupName := in.GroupName
	if groupName == "" {
		groupName = defaultGroupName
	}

	tokenKey := clientTokenKey("schedule", groupName, in.Name, in.ClientToken)
	if arn, ok := h.lookupIdempotent(tokenKey); ok {
		return &createScheduleOutput{ScheduleArn: arn}, nil
	}

	var opts []ScheduleOption
	if in.StartDate != nil {
		opts = append(opts, WithStartDate(epochSecondsToTime(*in.StartDate)))
	}

	if in.EndDate != nil {
		opts = append(opts, WithEndDate(epochSecondsToTime(*in.EndDate)))
	}

	if in.ActionAfterCompletion != "" {
		opts = append(opts, WithActionAfterCompletion(in.ActionAfterCompletion))
	}

	if in.KmsKeyArn != "" {
		opts = append(opts, WithKmsKeyArn(in.KmsKeyArn))
	}

	s, err := h.Backend.CreateSchedule(
		ctx,
		in.Name,
		in.GroupName,
		in.ScheduleExpression,
		in.Description,
		in.ScheduleExpressionTimezone,
		targetFromInput(in.Target),
		state,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	h.storeIdempotent(tokenKey, s.ARN)

	return &createScheduleOutput{ScheduleArn: s.ARN}, nil
}

// retryPolicyFromInput converts a handler retry policy to the backend type.
func retryPolicyFromInput(in *scheduleTargetRetryPolicy) *RetryPolicy {
	if in == nil {
		return nil
	}

	return &RetryPolicy{
		MaximumEventAgeInSeconds: in.MaximumEventAgeInSeconds,
		MaximumRetryAttempts:     in.MaximumRetryAttempts,
	}
}

// deadLetterConfigFromInput converts a handler DLQ config to the backend type.
func deadLetterConfigFromInput(in *scheduleTargetDeadLetterConfig) *DeadLetterConfig {
	if in == nil {
		return nil
	}

	return &DeadLetterConfig{Arn: in.Arn}
}

// eventBridgeParamsFromInput converts handler EventBridge parameters to the backend type.
func eventBridgeParamsFromInput(in *scheduleTargetEventBridgeParameters) *EventBridgeParameters {
	if in == nil {
		return nil
	}

	return &EventBridgeParameters{DetailType: in.DetailType, Source: in.Source}
}

// kinesisParamsFromInput converts handler Kinesis parameters to the backend type.
func kinesisParamsFromInput(in *scheduleTargetKinesisParameters) *KinesisParameters {
	if in == nil {
		return nil
	}

	return &KinesisParameters{PartitionKey: in.PartitionKey}
}

// sqsParamsFromInput converts handler SQS parameters to the backend type.
func sqsParamsFromInput(in *scheduleTargetSqsParameters) *SqsParameters {
	if in == nil {
		return nil
	}

	return &SqsParameters{MessageGroupID: in.MessageGroupID}
}

// sageMakerParamsFromInput converts handler SageMaker parameters to the backend type.
func sageMakerParamsFromInput(in *scheduleTargetSageMakerPipelineParameters) *SageMakerPipelineParameters {
	if in == nil {
		return nil
	}

	params := make([]SageMakerPipelineParameter, len(in.PipelineParameterList))
	for i, p := range in.PipelineParameterList {
		params[i] = SageMakerPipelineParameter(p)
	}

	return &SageMakerPipelineParameters{PipelineParameterList: params}
}

// ecsNetworkConfigFromInput converts handler network configuration to the backend type.
func ecsNetworkConfigFromInput(in *scheduleTargetEcsNetworkConfiguration) *EcsNetworkConfiguration {
	if in == nil {
		return nil
	}

	out := &EcsNetworkConfiguration{}

	if in.AwsvpcConfiguration != nil {
		out.AwsvpcConfiguration = &EcsAwsvpcConfiguration{
			Subnets:        in.AwsvpcConfiguration.Subnets,
			SecurityGroups: in.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: in.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return out
}

// ecsCapacityStrategyFromInput converts handler capacity provider strategy to the backend type.
func ecsCapacityStrategyFromInput(
	in []scheduleTargetEcsCapacityProviderStrategyItem,
) []EcsCapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsCapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = EcsCapacityProviderStrategyItem(item)
	}

	return out
}

// ecsPlacementConstraintsFromInput converts handler placement constraints to the backend type.
func ecsPlacementConstraintsFromInput(in []scheduleTargetEcsPlacementConstraint) []EcsPlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsPlacementConstraint, len(in))
	for i, c := range in {
		out[i] = EcsPlacementConstraint(c)
	}

	return out
}

// ecsPlacementStrategyFromInput converts handler placement strategy to the backend type.
func ecsPlacementStrategyFromInput(in []scheduleTargetEcsPlacementStrategy) []EcsPlacementStrategy {
	if len(in) == 0 {
		return nil
	}

	out := make([]EcsPlacementStrategy, len(in))
	for i, s := range in {
		out[i] = EcsPlacementStrategy(s)
	}

	return out
}

// ecsParamsFromInput converts handler ECS parameters to the backend type.
func ecsParamsFromInput(in *scheduleTargetEcsParameters) *EcsParameters {
	if in == nil {
		return nil
	}

	return &EcsParameters{
		TaskDefinitionArn:        in.TaskDefinitionArn,
		LaunchType:               in.LaunchType,
		TaskCount:                in.TaskCount,
		PlatformVersion:          in.PlatformVersion,
		Group:                    in.Group,
		PropagateTags:            in.PropagateTags,
		ReferenceID:              in.ReferenceID,
		EnableECSManagedTags:     in.EnableECSManagedTags,
		EnableExecuteCommand:     in.EnableExecuteCommand,
		NetworkConfiguration:     ecsNetworkConfigFromInput(in.NetworkConfiguration),
		CapacityProviderStrategy: ecsCapacityStrategyFromInput(in.CapacityProviderStrategy),
		PlacementConstraints:     ecsPlacementConstraintsFromInput(in.PlacementConstraints),
		PlacementStrategy:        ecsPlacementStrategyFromInput(in.PlacementStrategy),
		Tags:                     in.Tags,
	}
}

// targetFromInput converts a handler scheduleTarget into the backend Target type.
func targetFromInput(in scheduleTarget) Target {
	return Target{
		ARN:                         in.Arn,
		RoleARN:                     in.RoleArn,
		Input:                       in.Input,
		RetryPolicy:                 retryPolicyFromInput(in.RetryPolicy),
		DeadLetterConfig:            deadLetterConfigFromInput(in.DeadLetterConfig),
		EventBridgeParameters:       eventBridgeParamsFromInput(in.EventBridgeParameters),
		KinesisParameters:           kinesisParamsFromInput(in.KinesisParameters),
		SqsParameters:               sqsParamsFromInput(in.SqsParameters),
		SageMakerPipelineParameters: sageMakerParamsFromInput(in.SageMakerPipelineParameters),
		EcsParameters:               ecsParamsFromInput(in.EcsParameters),
	}
}

// retryPolicyToOutput converts a backend retry policy to the handler output type.
func retryPolicyToOutput(r *RetryPolicy) *scheduleTargetRetryPolicy {
	if r == nil {
		return nil
	}

	return &scheduleTargetRetryPolicy{
		MaximumEventAgeInSeconds: r.MaximumEventAgeInSeconds,
		MaximumRetryAttempts:     r.MaximumRetryAttempts,
	}
}

// deadLetterConfigToOutput converts a backend DLQ config to the handler output type.
func deadLetterConfigToOutput(d *DeadLetterConfig) *scheduleTargetDeadLetterConfig {
	if d == nil {
		return nil
	}

	return &scheduleTargetDeadLetterConfig{Arn: d.Arn}
}

// eventBridgeParamsToOutput converts backend EventBridge parameters to the handler output type.
func eventBridgeParamsToOutput(e *EventBridgeParameters) *scheduleTargetEventBridgeParameters {
	if e == nil {
		return nil
	}

	return &scheduleTargetEventBridgeParameters{DetailType: e.DetailType, Source: e.Source}
}

// kinesisParamsToOutput converts backend Kinesis parameters to the handler output type.
func kinesisParamsToOutput(k *KinesisParameters) *scheduleTargetKinesisParameters {
	if k == nil {
		return nil
	}

	return &scheduleTargetKinesisParameters{PartitionKey: k.PartitionKey}
}

// sqsParamsToOutput converts backend SQS parameters to the handler output type.
func sqsParamsToOutput(s *SqsParameters) *scheduleTargetSqsParameters {
	if s == nil {
		return nil
	}

	return &scheduleTargetSqsParameters{MessageGroupID: s.MessageGroupID}
}

// sageMakerParamsToOutput converts backend SageMaker parameters to the handler output type.
func sageMakerParamsToOutput(s *SageMakerPipelineParameters) *scheduleTargetSageMakerPipelineParameters {
	if s == nil {
		return nil
	}

	params := make([]scheduleTargetSageMakerPipelineParam, len(s.PipelineParameterList))
	for i, p := range s.PipelineParameterList {
		params[i] = scheduleTargetSageMakerPipelineParam(p)
	}

	return &scheduleTargetSageMakerPipelineParameters{PipelineParameterList: params}
}

// ecsNetworkConfigToOutput converts backend network configuration to the handler output type.
func ecsNetworkConfigToOutput(in *EcsNetworkConfiguration) *scheduleTargetEcsNetworkConfiguration {
	if in == nil {
		return nil
	}

	out := &scheduleTargetEcsNetworkConfiguration{}

	if in.AwsvpcConfiguration != nil {
		out.AwsvpcConfiguration = &scheduleTargetEcsAwsvpcConfiguration{
			Subnets:        in.AwsvpcConfiguration.Subnets,
			SecurityGroups: in.AwsvpcConfiguration.SecurityGroups,
			AssignPublicIP: in.AwsvpcConfiguration.AssignPublicIP,
		}
	}

	return out
}

// ecsCapacityStrategyToOutput converts backend capacity provider strategy to the handler output type.
func ecsCapacityStrategyToOutput(in []EcsCapacityProviderStrategyItem) []scheduleTargetEcsCapacityProviderStrategyItem {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsCapacityProviderStrategyItem, len(in))
	for i, item := range in {
		out[i] = scheduleTargetEcsCapacityProviderStrategyItem(item)
	}

	return out
}

// ecsPlacementConstraintsToOutput converts backend placement constraints to the handler output type.
func ecsPlacementConstraintsToOutput(in []EcsPlacementConstraint) []scheduleTargetEcsPlacementConstraint {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsPlacementConstraint, len(in))
	for i, c := range in {
		out[i] = scheduleTargetEcsPlacementConstraint(c)
	}

	return out
}

// ecsPlacementStrategyToOutput converts backend placement strategy to the handler output type.
func ecsPlacementStrategyToOutput(in []EcsPlacementStrategy) []scheduleTargetEcsPlacementStrategy {
	if len(in) == 0 {
		return nil
	}

	out := make([]scheduleTargetEcsPlacementStrategy, len(in))
	for i, s := range in {
		out[i] = scheduleTargetEcsPlacementStrategy(s)
	}

	return out
}

// ecsParamsToOutput converts backend ECS parameters to the handler output type.
func ecsParamsToOutput(e *EcsParameters) *scheduleTargetEcsParameters {
	if e == nil {
		return nil
	}

	return &scheduleTargetEcsParameters{
		TaskDefinitionArn:        e.TaskDefinitionArn,
		LaunchType:               e.LaunchType,
		TaskCount:                e.TaskCount,
		PlatformVersion:          e.PlatformVersion,
		Group:                    e.Group,
		PropagateTags:            e.PropagateTags,
		ReferenceID:              e.ReferenceID,
		EnableECSManagedTags:     e.EnableECSManagedTags,
		EnableExecuteCommand:     e.EnableExecuteCommand,
		NetworkConfiguration:     ecsNetworkConfigToOutput(e.NetworkConfiguration),
		CapacityProviderStrategy: ecsCapacityStrategyToOutput(e.CapacityProviderStrategy),
		PlacementConstraints:     ecsPlacementConstraintsToOutput(e.PlacementConstraints),
		PlacementStrategy:        ecsPlacementStrategyToOutput(e.PlacementStrategy),
		Tags:                     e.Tags,
	}
}

// targetToOutput converts a backend Target into the handler output type.
func targetToOutput(t Target) scheduleTargetOutput {
	return scheduleTargetOutput{
		Arn:                         t.ARN,
		RoleArn:                     t.RoleARN,
		Input:                       t.Input,
		RetryPolicy:                 retryPolicyToOutput(t.RetryPolicy),
		DeadLetterConfig:            deadLetterConfigToOutput(t.DeadLetterConfig),
		EventBridgeParameters:       eventBridgeParamsToOutput(t.EventBridgeParameters),
		KinesisParameters:           kinesisParamsToOutput(t.KinesisParameters),
		SqsParameters:               sqsParamsToOutput(t.SqsParameters),
		SageMakerPipelineParameters: sageMakerParamsToOutput(t.SageMakerPipelineParameters),
		EcsParameters:               ecsParamsToOutput(t.EcsParameters),
	}
}

type scheduleTargetOutput struct {
	RetryPolicy                 *scheduleTargetRetryPolicy                 `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            *scheduleTargetDeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	EventBridgeParameters       *scheduleTargetEventBridgeParameters       `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           *scheduleTargetKinesisParameters           `json:"KinesisParameters,omitempty"`
	SqsParameters               *scheduleTargetSqsParameters               `json:"SqsParameters,omitempty"`
	SageMakerPipelineParameters *scheduleTargetSageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	EcsParameters               *scheduleTargetEcsParameters               `json:"EcsParameters,omitempty"`
	Arn                         string                                     `json:"Arn"`
	RoleArn                     string                                     `json:"RoleArn"`
	Input                       string                                     `json:"Input,omitempty"`
}

type flexibleTimeWindowOutput struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes int    `json:"MaximumWindowInMinutes,omitempty"`
}

type getScheduleOutput struct {
	EndDate                    *float64                 `json:"EndDate,omitempty"`
	StartDate                  *float64                 `json:"StartDate,omitempty"`
	Target                     scheduleTargetOutput     `json:"Target"`
	ScheduleExpression         string                   `json:"ScheduleExpression"`
	Name                       string                   `json:"Name"`
	Arn                        string                   `json:"Arn"`
	GroupName                  string                   `json:"GroupName"`
	ScheduleExpressionTimezone string                   `json:"ScheduleExpressionTimezone,omitempty"`
	Description                string                   `json:"Description,omitempty"`
	State                      string                   `json:"State"`
	ActionAfterCompletion      string                   `json:"ActionAfterCompletion,omitempty"`
	KmsKeyArn                  string                   `json:"KmsKeyArn,omitempty"`
	FlexibleTimeWindow         flexibleTimeWindowOutput `json:"FlexibleTimeWindow"`
	LastModificationDate       float64                  `json:"LastModificationDate"`
	CreationDate               float64                  `json:"CreationDate"`
}

func (h *Handler) handleGetSchedule(ctx context.Context, in *scheduleNameInput) (*getScheduleOutput, error) {
	s, err := h.Backend.GetSchedule(ctx, in.Name, in.GroupName)
	if err != nil {
		return nil, err
	}

	out := &getScheduleOutput{
		Name:                       s.Name,
		Arn:                        s.ARN,
		GroupName:                  s.GroupName,
		ScheduleExpression:         s.ScheduleExpression,
		ScheduleExpressionTimezone: s.ScheduleExpressionTimezone,
		Description:                s.Description,
		State:                      s.State,
		ActionAfterCompletion:      s.ActionAfterCompletion,
		KmsKeyArn:                  s.KmsKeyArn,
		CreationDate:               float64(s.CreationDate.Unix()),
		LastModificationDate:       float64(s.LastModificationDate.Unix()),
		Target:                     targetToOutput(s.Target),
		FlexibleTimeWindow: flexibleTimeWindowOutput{
			Mode:                   s.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: s.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
	}

	if s.StartDate != nil {
		v := float64(s.StartDate.Unix())
		out.StartDate = &v
	}

	if s.EndDate != nil {
		v := float64(s.EndDate.Unix())
		out.EndDate = &v
	}

	return out, nil
}

type listSchedulesInput struct {
	GroupName  string `json:"GroupName"`
	NamePrefix string `json:"NamePrefix"`
	State      string `json:"State"`
	NextToken  string `json:"NextToken"`
	MaxResults string `json:"MaxResults"`
}

// scheduleSummaryTarget holds the target summary included in ListSchedules items.
// Real AWS's TargetSummary type (see aws-sdk-go-v2/service/scheduler/types) has
// only an Arn field -- no RoleArn -- so this must not add one.
type scheduleSummaryTarget struct {
	Arn string `json:"Arn"`
}

type scheduleSummary struct {
	Target               scheduleSummaryTarget `json:"Target"`
	Name                 string                `json:"Name"`
	Arn                  string                `json:"Arn"`
	GroupName            string                `json:"GroupName"`
	ScheduleExpression   string                `json:"ScheduleExpression"`
	State                string                `json:"State"`
	CreationDate         float64               `json:"CreationDate"`
	LastModificationDate float64               `json:"LastModificationDate"`
}

type listSchedulesOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Schedules []scheduleSummary `json:"Schedules"`
}

func (h *Handler) handleListSchedules(ctx context.Context, in *listSchedulesInput) (*listSchedulesOutput, error) {
	maxResults := parseMaxResults(in.MaxResults)
	schedules, nextToken := h.Backend.ListSchedules(
		ctx,
		in.GroupName,
		in.NamePrefix,
		in.State,
		in.NextToken,
		maxResults,
	)
	items := make([]scheduleSummary, 0, len(schedules))

	for _, s := range schedules {
		items = append(items, scheduleSummary{
			Name:                 s.Name,
			Arn:                  s.ARN,
			GroupName:            s.GroupName,
			ScheduleExpression:   s.ScheduleExpression,
			State:                s.State,
			CreationDate:         float64(s.CreationDate.Unix()),
			LastModificationDate: float64(s.LastModificationDate.Unix()),
			Target: scheduleSummaryTarget{
				Arn: s.Target.ARN,
			},
		})
	}

	return &listSchedulesOutput{Schedules: items, NextToken: nextToken}, nil
}

func (h *Handler) handleDeleteSchedule(ctx context.Context, in *scheduleNameInput) (*emptyOutput, error) {
	return voidOp(func() error { return h.Backend.DeleteSchedule(ctx, in.Name, in.GroupName) })
}

type updateScheduleOutput struct {
	ScheduleArn string `json:"ScheduleArn"`
}

func (h *Handler) handleUpdateSchedule(ctx context.Context, in *scheduleInput) (*updateScheduleOutput, error) {
	if err := validateActionAfterCompletion(in.ActionAfterCompletion); err != nil {
		return nil, err
	}

	var opts []ScheduleOption
	if in.StartDate != nil {
		opts = append(opts, WithStartDate(epochSecondsToTime(*in.StartDate)))
	}

	if in.EndDate != nil {
		opts = append(opts, WithEndDate(epochSecondsToTime(*in.EndDate)))
	}

	if in.ActionAfterCompletion != "" {
		opts = append(opts, WithActionAfterCompletion(in.ActionAfterCompletion))
	}

	if in.KmsKeyArn != "" {
		opts = append(opts, WithKmsKeyArn(in.KmsKeyArn))
	}

	s, err := h.Backend.UpdateSchedule(
		ctx,
		in.Name,
		in.GroupName,
		in.ScheduleExpression,
		in.Description,
		in.ScheduleExpressionTimezone,
		targetFromInput(in.Target),
		in.State,
		FlexibleTimeWindow{
			Mode:                   in.FlexibleTimeWindow.Mode,
			MaximumWindowInMinutes: in.FlexibleTimeWindow.MaximumWindowInMinutes,
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return &updateScheduleOutput{ScheduleArn: s.ARN}, nil
}
