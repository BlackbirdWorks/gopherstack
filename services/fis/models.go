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
	CreationTime      time.Time                            `json:"creationTime"`
	LastUpdateTime    time.Time                            `json:"lastUpdateTime"`
	Tags              map[string]string                    `json:"tags"`
	Targets           map[string]ExperimentTemplateTarget  `json:"targets"`
	Actions           map[string]ExperimentTemplateAction  `json:"actions"`
	LogConfiguration  *ExperimentTemplateLogConfiguration  `json:"logConfiguration"`
	ExperimentOptions *ExperimentTemplateExperimentOptions `json:"experimentOptions"`
	ID                string                               `json:"id"`
	Arn               string                               `json:"arn"`
	Description       string                               `json:"description"`
	RoleArn           string                               `json:"roleArn"`
	StopConditions    []ExperimentTemplateStopCondition    `json:"stopConditions"`
}

// ExperimentTemplateTarget defines how resources are selected for a fault action.
type ExperimentTemplateTarget struct {
	ResourceTags  map[string]string                `json:"resourceTags"`
	Parameters    map[string]string                `json:"parameters"`
	ResourceType  string                           `json:"resourceType"`
	SelectionMode string                           `json:"selectionMode"`
	ResourceArns  []string                         `json:"resourceArns"`
	Filters       []ExperimentTemplateTargetFilter `json:"filters"`
}

// ExperimentTemplateTargetFilter narrows the set of matching resources.
type ExperimentTemplateTargetFilter struct {
	Path   string   `json:"path"`
	Values []string `json:"values"`
}

// ExperimentTemplateAction describes a fault action within a template.
type ExperimentTemplateAction struct {
	Parameters  map[string]string `json:"parameters"`
	Targets     map[string]string `json:"targets"`
	ActionID    string            `json:"actionID"`
	Description string            `json:"description"`
	StartAfter  []string          `json:"startAfter"`
}

// ExperimentTemplateStopCondition defines when an experiment should automatically stop.
type ExperimentTemplateStopCondition struct {
	Source string `json:"source"`
	Value  string `json:"value"`
}

// ExperimentTemplateLogConfiguration specifies where experiment logs are sent.
type ExperimentTemplateLogConfiguration struct {
	CloudWatchLogsConfiguration *ExperimentTemplateCloudWatchLogsConfiguration `json:"cloudWatchLogsConfiguration"`
	S3Configuration             *ExperimentTemplateS3Configuration             `json:"s3Configuration"`
	LogSchemaVersion            int                                            `json:"logSchemaVersion"`
}

// ExperimentTemplateCloudWatchLogsConfiguration holds the CloudWatch log group ARN.
type ExperimentTemplateCloudWatchLogsConfiguration struct {
	LogGroupArn string `json:"logGroupArn"`
}

// ExperimentTemplateS3Configuration holds the S3 bucket for experiment logs.
type ExperimentTemplateS3Configuration struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ExperimentTemplateExperimentOptions controls account and target resolution behaviour.
type ExperimentTemplateExperimentOptions struct {
	AccountTargeting          string `json:"accountTargeting"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode"`
}

// ----------------------------------------
// Experiment models
// ----------------------------------------

// Experiment is the in-memory representation of a running FIS experiment.
type Experiment struct {
	CreationTime                     time.Time                    `json:"creationTime"`
	StartTime                        time.Time                    `json:"startTime"`
	ExperimentOptions                *ExperimentExperimentOptions `json:"experimentOptions"`
	Targets                          map[string]ExperimentTarget  `json:"targets"`
	Actions                          map[string]ExperimentAction  `json:"actions"`
	LogConfiguration                 *ExperimentLogConfiguration  `json:"logConfiguration"`
	Tags                             map[string]string            `json:"tags"`
	EndTime                          *time.Time                   `json:"endTime"`
	cancel                           context.CancelFunc           `json:"-"`
	Status                           ExperimentStatus             `json:"status"`
	ExperimentTemplateID             string                       `json:"experimentTemplateID"`
	RoleArn                          string                       `json:"roleArn"`
	ID                               string                       `json:"id"`
	Arn                              string                       `json:"arn"`
	StopConditions                   []ExperimentStopCondition    `json:"stopConditions"`
	TargetAccountConfigurationsCount int                          `json:"targetAccountConfigurationsCount,omitempty"`
}

// ExperimentStatus holds the status string, an optional human-readable reason, and structured error info.
type ExperimentStatus struct {
	Error  *ExperimentStatusError `json:"error,omitempty"`
	Status string                 `json:"status"`
	Reason string                 `json:"reason,omitempty"`
}

// ExperimentStatusError holds structured error info for failed experiments.
type ExperimentStatusError struct {
	ExceptionName string `json:"exceptionName,omitempty"`
	AccountID     string `json:"accountId,omitempty"`
}

// ExperimentTarget holds resolved resource ARNs for a target group.
type ExperimentTarget struct {
	Parameters   map[string]string `json:"parameters"`
	ResourceType string            `json:"resourceType"`
	ResourceArns []string          `json:"resourceArns"`
}

// ExperimentAction tracks the state of an individual experiment action.
type ExperimentAction struct {
	Parameters map[string]string      `json:"parameters"`
	Targets    map[string]string      `json:"targets"`
	StartTime  *time.Time             `json:"startTime"`
	EndTime    *time.Time             `json:"endTime"`
	Status     ExperimentActionStatus `json:"status"`
	ActionID   string                 `json:"actionID"`
}

// ExperimentActionStatus holds the status and reason for a single action.
type ExperimentActionStatus struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// ExperimentStopCondition mirrors ExperimentTemplateStopCondition for running experiments.
type ExperimentStopCondition struct {
	Source string `json:"source"`
	Value  string `json:"value"`
}

// ExperimentLogConfiguration holds resolved log configuration for an experiment.
type ExperimentLogConfiguration struct {
	CloudWatchLogsConfiguration *ExperimentCloudWatchLogsConfiguration `json:"cloudWatchLogsConfiguration"`
	S3Configuration             *ExperimentS3Configuration             `json:"s3Configuration"`
	LogSchemaVersion            int                                    `json:"logSchemaVersion"`
}

// ExperimentCloudWatchLogsConfiguration holds the CloudWatch log group ARN.
type ExperimentCloudWatchLogsConfiguration struct {
	LogGroupArn string `json:"logGroupArn"`
}

// ExperimentS3Configuration holds the S3 bucket for experiment logs.
type ExperimentS3Configuration struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ExperimentExperimentOptions controls account and target resolution behaviour.
type ExperimentExperimentOptions struct {
	AccountTargeting          string `json:"accountTargeting"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode"`
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
	CloudWatchLogsConfiguration *cwLogsConfigurationDTO               `json:"cloudWatchLogsConfiguration,omitempty"`
	S3Configuration             *experimentTemplateS3ConfigurationDTO `json:"s3Configuration,omitempty"`
	LogSchemaVersion            int                                   `json:"logSchemaVersion"`
}

// cwLogsConfigurationDTO holds the CloudWatch log group ARN.
type cwLogsConfigurationDTO struct {
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
	ExperimentOptions                *experimentExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	Targets                          map[string]experimentTargetDTO  `json:"targets"`
	Actions                          map[string]experimentActionDTO  `json:"actions"`
	LogConfiguration                 *experimentLogConfigurationDTO  `json:"logConfiguration,omitempty"`
	Tags                             map[string]string               `json:"tags"`
	EndTime                          *float64                        `json:"endTime,omitempty"`
	State                            experimentStatusDTO             `json:"state"`
	Status                           experimentStatusDTO             `json:"status"`
	Arn                              string                          `json:"arn"`
	ExperimentTemplateID             string                          `json:"experimentTemplateId"`
	RoleArn                          string                          `json:"roleArn,omitempty"`
	ID                               string                          `json:"id"`
	StopConditions                   []experimentStopConditionDTO    `json:"stopConditions"`
	CreationTime                     float64                         `json:"creationTime"`
	StartTime                        float64                         `json:"startTime"`
	TargetAccountConfigurationsCount int                             `json:"targetAccountConfigurationsCount,omitempty"`
}

// experimentStatusDTO is the JSON representation of an experiment status.
type experimentStatusDTO struct {
	Error  *experimentStatusErrorDTO `json:"error,omitempty"`
	Status string                    `json:"status"`
	Reason string                    `json:"reason,omitempty"`
}

// experimentStatusErrorDTO is the JSON representation of structured error info on failed experiments.
type experimentStatusErrorDTO struct {
	ExceptionName string `json:"exceptionName,omitempty"`
	AccountID     string `json:"accountId,omitempty"`
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
	State      *experimentActionStatusDTO `json:"state,omitempty"`
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
// __type enables AWS SDKs to deserialize specific exception types.
type errorResponseDTO struct {
	Type       string `json:"__type,omitempty"`
	Message    string `json:"message"`
	ResourceID string `json:"resourceId,omitempty"`
}

// ----------------------------------------
// Target Account Configuration models
// ----------------------------------------

// TargetAccountConfiguration is the in-memory representation of a FIS target account configuration
// associated with an experiment template.
type TargetAccountConfiguration struct {
	ExperimentTemplateID string `json:"experimentTemplateId"`
	AccountID            string `json:"accountId"`
	Description          string `json:"description"`
	RoleArn              string `json:"roleArn"`
}

// ExperimentTargetAccountConfiguration is the in-memory representation of a FIS target account
// configuration associated with a running experiment.
type ExperimentTargetAccountConfiguration struct {
	ExperimentID string `json:"experimentId"`
	AccountID    string `json:"accountId"`
	Description  string `json:"description"`
	RoleArn      string `json:"roleArn"`
}

// createTargetAccountConfigurationRequest is the JSON body for
// POST /experimentTemplates/{id}/targetAccountConfigurations/{accountId}.
type createTargetAccountConfigurationRequest struct {
	ClientToken string `json:"clientToken"`
	Description string `json:"description"`
	RoleArn     string `json:"roleArn"`
}

// updateTargetAccountConfigurationRequest is the JSON body for
// PATCH /experimentTemplates/{id}/targetAccountConfigurations/{accountId}.
type updateTargetAccountConfigurationRequest struct {
	Description *string `json:"description,omitempty"`
	RoleArn     *string `json:"roleArn,omitempty"`
}

// targetAccountConfigurationDTO is the JSON representation of a target account configuration.
type targetAccountConfigurationDTO struct {
	AccountID   string `json:"accountId"`
	Description string `json:"description,omitempty"`
	RoleArn     string `json:"roleArn,omitempty"`
}

// targetAccountConfigurationResponseDTO is the outer envelope for single target account configuration responses.
type targetAccountConfigurationResponseDTO struct {
	TargetAccountConfiguration targetAccountConfigurationDTO `json:"targetAccountConfiguration"`
}

// listTargetAccountConfigurationsResponseDTO is the outer envelope for list target account configuration responses.
type listTargetAccountConfigurationsResponseDTO struct {
	NextToken                   string                          `json:"nextToken,omitempty"`
	TargetAccountConfigurations []targetAccountConfigurationDTO `json:"targetAccountConfigurations"`
}

// experimentTargetAccountConfigurationDTO is the JSON representation of an experiment target account configuration.
type experimentTargetAccountConfigurationDTO struct {
	AccountID   string `json:"accountId"`
	Description string `json:"description,omitempty"`
	RoleArn     string `json:"roleArn,omitempty"`
}

// experimentTargetAccountConfigurationResponseDTO is the outer envelope for single experiment target account
// configuration responses.
type experimentTargetAccountConfigurationResponseDTO struct {
	TargetAccountConfiguration experimentTargetAccountConfigurationDTO `json:"targetAccountConfiguration"`
}

// listExperimentTargetAccountConfigurationsResponseDTO is the outer envelope for list experiment target account
// configuration responses.
type listExperimentTargetAccountConfigurationsResponseDTO struct {
	NextToken                   string                                    `json:"nextToken,omitempty"`
	TargetAccountConfigurations []experimentTargetAccountConfigurationDTO `json:"targetAccountConfigurations"`
}

// ----------------------------------------
// Phase 3 — Resolved Targets models
// ----------------------------------------

// ExperimentResolvedTarget holds the resolved resources for a single target group.
type ExperimentResolvedTarget struct {
	ResourceType         string
	TargetName           string
	ResolvedArns         []string
	TargetResourcesCount int
}

// resolvedTargetDTO is the JSON representation of a resolved target.
type resolvedTargetDTO struct {
	ResourceType         string   `json:"resourceType"`
	TargetName           string   `json:"targetName"`
	ResolvedArns         []string `json:"resolvedArns,omitempty"`
	TargetResourcesCount int      `json:"targetResourcesCount"`
}

// listExperimentResolvedTargetsResponseDTO is the outer envelope for ListExperimentResolvedTargets.
type listExperimentResolvedTargetsResponseDTO struct {
	NextToken       string              `json:"nextToken,omitempty"`
	ResolvedTargets []resolvedTargetDTO `json:"resolvedTargets"`
}

// ----------------------------------------
// Phase 3 — Safety Lever models
// ----------------------------------------

// SafetyLever is the model for the FIS account-level safety lever.
type SafetyLever struct {
	Tags  map[string]string `json:"tags"`
	State SafetyLeverState  `json:"state"`
	ID    string            `json:"id"`
	Arn   string            `json:"arn"`
}

// SafetyLeverState holds the status and optional human-readable reason.
type SafetyLeverState struct {
	Reason string `json:"reason"`
	Status string `json:"status"` // "disengaged" | "engaged"
}

// safetyLeverResponseDTO is the outer envelope for safety lever responses.
type safetyLeverResponseDTO struct {
	SafetyLever safetyLeverDTO `json:"safetyLever"`
}

// safetyLeverDTO is the JSON representation of a safety lever.
type safetyLeverDTO struct {
	Tags  map[string]string   `json:"tags,omitempty"`
	State safetyLeverStateDTO `json:"state"`
	ID    string              `json:"id"`
	Arn   string              `json:"arn"`
}

// safetyLeverStateDTO is the JSON representation of a safety lever state.
type safetyLeverStateDTO struct {
	Reason string `json:"reason,omitempty"`
	Status string `json:"status"`
}

// updateSafetyLeverStateRequest is the JSON body for PATCH /safetyLevers/{id}.
type updateSafetyLeverStateRequest struct {
	UpdateSafetyLeverStateInput updateSafetyLeverStateInputDTO `json:"updateSafetyLeverStateInput"`
}

// updateSafetyLeverStateInputDTO is the nested input for UpdateSafetyLeverState.
type updateSafetyLeverStateInputDTO struct {
	Reason string `json:"reason,omitempty"`
	Status string `json:"status"`
}
