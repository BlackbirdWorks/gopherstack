// Package fis provides an in-memory implementation of the AWS Fault Injection
// Service (FIS) API. It supports experiment templates, experiment lifecycle
// management, and auto-discovered FIS actions from other registered services.
package fis

import (
	"context"
	"time"
)

// ----------------------------------------
// Experiment Template models
// ----------------------------------------

// ExperimentTemplate is the in-memory representation of a FIS experiment template.
type ExperimentTemplate struct {
	CreationTime      time.Time
	LastUpdateTime    time.Time
	Tags              map[string]string
	Targets           map[string]ExperimentTemplateTarget
	Actions           map[string]ExperimentTemplateAction
	LogConfiguration  *ExperimentTemplateLogConfiguration
	ExperimentOptions *ExperimentTemplateExperimentOptions
	ID                string
	Arn               string
	Description       string
	RoleArn           string
	StopConditions    []ExperimentTemplateStopCondition
}

// ExperimentTemplateTarget defines how resources are selected for a fault action.
type ExperimentTemplateTarget struct {
	ResourceTags  map[string]string
	Parameters    map[string]string
	ResourceType  string
	SelectionMode string
	ResourceArns  []string
	Filters       []ExperimentTemplateTargetFilter
}

// ExperimentTemplateTargetFilter narrows the set of matching resources.
type ExperimentTemplateTargetFilter struct {
	Path   string
	Values []string
}

// ExperimentTemplateAction describes a fault action within a template.
type ExperimentTemplateAction struct {
	Parameters  map[string]string
	Targets     map[string]string
	ActionID    string
	Description string
	StartAfter  []string
}

// ExperimentTemplateStopCondition defines when an experiment should automatically stop.
type ExperimentTemplateStopCondition struct {
	Source string
	Value  string
}

// ExperimentTemplateLogConfiguration specifies where experiment logs are sent.
type ExperimentTemplateLogConfiguration struct {
	CloudWatchLogsConfiguration *ExperimentTemplateCloudWatchLogsConfiguration
	S3Configuration             *ExperimentTemplateS3Configuration
	LogSchemaVersion            int
}

// ExperimentTemplateCloudWatchLogsConfiguration holds the CloudWatch log group ARN.
type ExperimentTemplateCloudWatchLogsConfiguration struct {
	LogGroupArn string
}

// ExperimentTemplateS3Configuration holds the S3 bucket for experiment logs.
type ExperimentTemplateS3Configuration struct {
	BucketName string
	Prefix     string
}

// ExperimentTemplateExperimentOptions controls account and target resolution behaviour.
type ExperimentTemplateExperimentOptions struct {
	AccountTargeting          string
	EmptyTargetResolutionMode string
}

// ----------------------------------------
// Experiment models
// ----------------------------------------

// Experiment is the in-memory representation of a running FIS experiment.
type Experiment struct {
	StartTime            time.Time
	ExperimentOptions    *ExperimentExperimentOptions
	Targets              map[string]ExperimentTarget
	Actions              map[string]ExperimentAction
	LogConfiguration     *ExperimentLogConfiguration
	Tags                 map[string]string
	EndTime              *time.Time
	cancel               context.CancelFunc
	Status               ExperimentStatus
	ExperimentTemplateID string
	RoleArn              string
	ID                   string
	Arn                  string
	StopConditions       []ExperimentStopCondition
}

// ExperimentStatus holds the status string and an optional human-readable reason.
type ExperimentStatus struct {
	Status string
	Reason string
}

// ExperimentTarget holds resolved resource ARNs for a target group.
type ExperimentTarget struct {
	Parameters   map[string]string
	ResourceType string
	ResourceArns []string
}

// ExperimentAction tracks the state of an individual experiment action.
type ExperimentAction struct {
	Parameters map[string]string
	Targets    map[string]string
	StartTime  *time.Time
	EndTime    *time.Time
	Status     ExperimentActionStatus
	ActionID   string
}

// ExperimentActionStatus holds the status and reason for a single action.
type ExperimentActionStatus struct {
	Status string
	Reason string
}

// ExperimentStopCondition mirrors ExperimentTemplateStopCondition for running experiments.
type ExperimentStopCondition struct {
	Source string
	Value  string
}

// ExperimentLogConfiguration holds resolved log configuration for an experiment.
type ExperimentLogConfiguration struct {
	CloudWatchLogsConfiguration *ExperimentCloudWatchLogsConfiguration
	S3Configuration             *ExperimentS3Configuration
	LogSchemaVersion            int
}

// ExperimentCloudWatchLogsConfiguration holds the CloudWatch log group ARN.
type ExperimentCloudWatchLogsConfiguration struct {
	LogGroupArn string
}

// ExperimentS3Configuration holds the S3 bucket for experiment logs.
type ExperimentS3Configuration struct {
	BucketName string
	Prefix     string
}

// ExperimentExperimentOptions controls account and target resolution behaviour.
type ExperimentExperimentOptions struct {
	AccountTargeting          string
	EmptyTargetResolutionMode string
}

// ----------------------------------------
// Action & Target Resource Type discovery
// ----------------------------------------

// ActionSummary is the response model for GetAction / ListActions.
type ActionSummary struct {
	Targets     map[string]ActionTarget
	Parameters  map[string]ActionParameter
	Tags        map[string]string
	ID          string
	Arn         string
	Description string
}

// ActionTarget describes the target resource type required by an action.
type ActionTarget struct {
	ResourceType string
}

// ActionParameter describes a parameter accepted by an action.
type ActionParameter struct {
	Description string
	Required    bool
}

// TargetResourceTypeSummary is the response model for GetTargetResourceType / ListTargetResourceTypes.
type TargetResourceTypeSummary struct {
	Parameters   map[string]TargetResourceTypeParameter
	ResourceType string
	Description  string
}

// TargetResourceTypeParameter describes a parameter accepted when targeting a resource type.
type TargetResourceTypeParameter struct {
	Description string
	Required    bool
}

// ----------------------------------------
// JSON request / response DTOs
// ----------------------------------------

// createExperimentTemplateRequest is the JSON body for POST /experimentTemplates.
type createExperimentTemplateRequest struct {
	Tags              map[string]string                       `json:"tags"`
	Targets           map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions           map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration  *experimentTemplateLogConfigurationDTO  `json:"logConfiguration"`
	ExperimentOptions *experimentTemplateExperimentOptionsDTO `json:"experimentOptions"`
	ClientToken       string                                  `json:"clientToken"`
	Description       string                                  `json:"description"`
	RoleArn           string                                  `json:"roleArn"`
	StopConditions    []experimentTemplateStopConditionDTO    `json:"stopConditions"`
}

// updateExperimentTemplateRequest is the JSON body for PATCH /experimentTemplates/{id}.
type updateExperimentTemplateRequest struct {
	Targets           map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions           map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration  *experimentTemplateLogConfigurationDTO  `json:"logConfiguration"`
	ExperimentOptions *experimentTemplateExperimentOptionsDTO `json:"experimentOptions"`
	Description       string                                  `json:"description"`
	RoleArn           string                                  `json:"roleArn"`
	StopConditions    []experimentTemplateStopConditionDTO    `json:"stopConditions"`
}

// startExperimentRequest is the JSON body for POST /experiments.
type startExperimentRequest struct {
	Tags                 map[string]string `json:"tags"`
	ClientToken          string            `json:"clientToken"`
	ExperimentTemplateID string            `json:"experimentTemplateId"`
}

// experimentTemplateTargetDTO is the JSON representation of a template target.
type experimentTemplateTargetDTO struct {
	ResourceTags  map[string]string                   `json:"resourceTags,omitempty"`
	Parameters    map[string]string                   `json:"parameters,omitempty"`
	ResourceType  string                              `json:"resourceType"`
	SelectionMode string                              `json:"selectionMode"`
	ResourceArns  []string                            `json:"resourceArns,omitempty"`
	Filters       []experimentTemplateTargetFilterDTO `json:"filters,omitempty"`
}

// experimentTemplateTargetFilterDTO is the JSON representation of a target filter.
type experimentTemplateTargetFilterDTO struct {
	Path   string   `json:"path"`
	Values []string `json:"values"`
}

// experimentTemplateActionDTO is the JSON representation of a template action.
type experimentTemplateActionDTO struct {
	Parameters  map[string]string `json:"parameters,omitempty"`
	Targets     map[string]string `json:"targets,omitempty"`
	ActionID    string            `json:"actionId"`
	Description string            `json:"description,omitempty"`
	StartAfter  []string          `json:"startAfter,omitempty"`
}

// experimentTemplateStopConditionDTO is the JSON representation of a stop condition.
type experimentTemplateStopConditionDTO struct {
	Source string `json:"source"`
	Value  string `json:"value,omitempty"`
}

// experimentTemplateLogConfigurationDTO is the JSON representation of log configuration.
type experimentTemplateLogConfigurationDTO struct {
	//nolint:lll // struct tag for CloudWatch config is necessarily long
	CloudWatchLogsConfiguration *experimentTemplateCloudWatchLogsConfigurationDTO `json:"cloudWatchLogsConfiguration,omitempty"`
	S3Configuration             *experimentTemplateS3ConfigurationDTO             `json:"s3Configuration,omitempty"`
	LogSchemaVersion            int                                               `json:"logSchemaVersion"`
}

// experimentTemplateCloudWatchLogsConfigurationDTO holds the CloudWatch log group ARN.
type experimentTemplateCloudWatchLogsConfigurationDTO struct {
	LogGroupArn string `json:"logGroupArn"`
}

// experimentTemplateS3ConfigurationDTO holds the S3 bucket for experiment logs.
type experimentTemplateS3ConfigurationDTO struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix,omitempty"`
}

// experimentTemplateExperimentOptionsDTO holds account targeting and resolution options.
type experimentTemplateExperimentOptionsDTO struct {
	AccountTargeting          string `json:"accountTargeting,omitempty"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode,omitempty"`
}

// experimentTemplateResponseDTO is the outer envelope for experiment-template responses.
type experimentTemplateResponseDTO struct {
	ExperimentTemplate experimentTemplateDTO `json:"experimentTemplate"`
}

// listExperimentTemplatesResponseDTO is the outer envelope for list responses.
type listExperimentTemplatesResponseDTO struct {
	NextToken           string                  `json:"nextToken,omitempty"`
	ExperimentTemplates []experimentTemplateDTO `json:"experimentTemplates"`
}

// experimentTemplateDTO is the JSON representation of an experiment template.
type experimentTemplateDTO struct {
	Tags              map[string]string                       `json:"tags"`
	Targets           map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions           map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration  *experimentTemplateLogConfigurationDTO  `json:"logConfiguration,omitempty"`
	ExperimentOptions *experimentTemplateExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	ID                string                                  `json:"id"`
	Arn               string                                  `json:"arn"`
	Description       string                                  `json:"description,omitempty"`
	RoleArn           string                                  `json:"roleArn,omitempty"`
	StopConditions    []experimentTemplateStopConditionDTO    `json:"stopConditions"`
	CreationTime      float64                                 `json:"creationTime"`
	LastUpdateTime    float64                                 `json:"lastUpdateTime"`
}

// experimentResponseDTO is the outer envelope for experiment responses.
type experimentResponseDTO struct {
	Experiment experimentDTO `json:"experiment"`
}

// listExperimentsResponseDTO is the outer envelope for list experiments responses.
type listExperimentsResponseDTO struct {
	NextToken   string          `json:"nextToken,omitempty"`
	Experiments []experimentDTO `json:"experiments"`
}

// experimentDTO is the JSON representation of a running experiment.
type experimentDTO struct {
	ExperimentOptions    *experimentExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	Targets              map[string]experimentTargetDTO  `json:"targets"`
	Actions              map[string]experimentActionDTO  `json:"actions"`
	LogConfiguration     *experimentLogConfigurationDTO  `json:"logConfiguration,omitempty"`
	Tags                 map[string]string               `json:"tags"`
	EndTime              *float64                        `json:"endTime,omitempty"`
	Status               experimentStatusDTO             `json:"status"`
	Arn                  string                          `json:"arn"`
	ExperimentTemplateID string                          `json:"experimentTemplateId"`
	RoleArn              string                          `json:"roleArn,omitempty"`
	ID                   string                          `json:"id"`
	StopConditions       []experimentStopConditionDTO    `json:"stopConditions"`
	StartTime            float64                         `json:"startTime"`
}

// experimentStatusDTO is the JSON representation of an experiment status.
type experimentStatusDTO struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// experimentTargetDTO is the JSON representation of a resolved target.
type experimentTargetDTO struct {
	Parameters   map[string]string `json:"parameters,omitempty"`
	ResourceType string            `json:"resourceType"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
}

// experimentActionDTO is the JSON representation of a running experiment action.
type experimentActionDTO struct {
	Parameters map[string]string          `json:"parameters,omitempty"`
	Targets    map[string]string          `json:"targets,omitempty"`
	Status     *experimentActionStatusDTO `json:"status,omitempty"`
	StartTime  *float64                   `json:"startTime,omitempty"`
	EndTime    *float64                   `json:"endTime,omitempty"`
	ActionID   string                     `json:"actionId"`
}

// experimentActionStatusDTO is the JSON representation of an action status.
type experimentActionStatusDTO struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// experimentStopConditionDTO is the JSON representation of a stop condition.
type experimentStopConditionDTO struct {
	Source string `json:"source"`
	Value  string `json:"value,omitempty"`
}

// experimentLogConfigurationDTO is the JSON representation of log configuration.
type experimentLogConfigurationDTO struct {
	CloudWatchLogsConfiguration *experimentCloudWatchLogsConfigurationDTO `json:"cloudWatchLogsConfiguration,omitempty"`
	S3Configuration             *experimentS3ConfigurationDTO             `json:"s3Configuration,omitempty"`
	LogSchemaVersion            int                                       `json:"logSchemaVersion"`
}

// experimentCloudWatchLogsConfigurationDTO holds the CloudWatch log group ARN.
type experimentCloudWatchLogsConfigurationDTO struct {
	LogGroupArn string `json:"logGroupArn"`
}

// experimentS3ConfigurationDTO holds the S3 bucket for experiment logs.
type experimentS3ConfigurationDTO struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix,omitempty"`
}

// experimentExperimentOptionsDTO holds account targeting and resolution options.
type experimentExperimentOptionsDTO struct {
	AccountTargeting          string `json:"accountTargeting,omitempty"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode,omitempty"`
}

// listActionsResponseDTO is the outer envelope for list actions responses.
type listActionsResponseDTO struct {
	NextToken string      `json:"nextToken,omitempty"`
	Actions   []actionDTO `json:"actions"`
}

// actionResponseDTO is the outer envelope for a single action.
type actionResponseDTO struct {
	Action actionDTO `json:"action"`
}

// actionDTO is the JSON representation of a FIS action.
type actionDTO struct {
	Targets     map[string]actionTargetDTO `json:"targets,omitempty"`
	Parameters  map[string]actionParamDTO  `json:"parameters,omitempty"`
	Tags        map[string]string          `json:"tags"`
	ID          string                     `json:"id"`
	Arn         string                     `json:"arn"`
	Description string                     `json:"description,omitempty"`
}

// actionTargetDTO is the JSON representation of an action's target specification.
type actionTargetDTO struct {
	ResourceType string `json:"resourceType"`
}

// actionParamDTO is the JSON representation of an action parameter.
type actionParamDTO struct {
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required"`
}

// listTargetResourceTypesResponseDTO is the outer envelope for list target resource types.
type listTargetResourceTypesResponseDTO struct {
	NextToken           string                  `json:"nextToken,omitempty"`
	TargetResourceTypes []targetResourceTypeDTO `json:"targetResourceTypes"`
}

// targetResourceTypeResponseDTO is the outer envelope for a single target resource type.
type targetResourceTypeResponseDTO struct {
	TargetResourceType targetResourceTypeDTO `json:"targetResourceType"`
}

// targetResourceTypeDTO is the JSON representation of a target resource type.
type targetResourceTypeDTO struct {
	Parameters   map[string]targetRTParamDTO `json:"parameters,omitempty"`
	ResourceType string                      `json:"resourceType"`
	Description  string                      `json:"description,omitempty"`
}

// targetRTParamDTO is the JSON representation of a target resource type parameter.
type targetRTParamDTO struct {
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required"`
}

// tagsResponseDTO is the outer envelope for ListTagsForResource responses.
type tagsResponseDTO struct {
	Tags map[string]string `json:"tags"`
}

// errorResponseDTO is the standard FIS JSON error response.
type errorResponseDTO struct {
	Message    string `json:"message"`
	ResourceID string `json:"resourceId,omitempty"`
}
