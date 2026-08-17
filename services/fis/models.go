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
	CreationTime                  time.Time                              `json:"creationTime"`
	LastUpdateTime                time.Time                              `json:"lastUpdateTime"`
	Tags                          map[string]string                      `json:"tags"`
	Targets                       map[string]ExperimentTemplateTarget    `json:"targets"`
	Actions                       map[string]ExperimentTemplateAction    `json:"actions"`
	LogConfiguration              *ExperimentTemplateLogConfiguration    `json:"logConfiguration"`
	ExperimentOptions             *ExperimentTemplateExperimentOptions   `json:"experimentOptions"`
	ExperimentReportConfiguration *ExperimentTemplateReportConfiguration `json:"experimentReportConfiguration"`
	ID                            string                                 `json:"id"`
	Arn                           string                                 `json:"arn"`
	Description                   string                                 `json:"description"`
	RoleArn                       string                                 `json:"roleArn"`
	StopConditions                []ExperimentTemplateStopCondition      `json:"stopConditions"`
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

// ExperimentTemplateReportConfiguration describes the experiment report generation
// settings for an experiment template: which CloudWatch dashboards to snapshot,
// where to write the generated report, and how much time around the experiment's
// start/end to include in the report's data sources.
type ExperimentTemplateReportConfiguration struct {
	DataSources            *ExperimentTemplateReportConfigurationDataSources `json:"dataSources"`
	Outputs                *ExperimentTemplateReportConfigurationOutputs     `json:"outputs"`
	PreExperimentDuration  string                                            `json:"preExperimentDuration"`
	PostExperimentDuration string                                            `json:"postExperimentDuration"`
}

// ExperimentTemplateReportConfigurationDataSources lists the data sources for an
// experiment report.
type ExperimentTemplateReportConfigurationDataSources struct {
	CloudWatchDashboards []ExperimentTemplateReportConfigurationCloudWatchDashboard `json:"cloudWatchDashboards"`
}

// ExperimentTemplateReportConfigurationCloudWatchDashboard identifies a CloudWatch
// dashboard whose widgets are captured as snapshot graphs in the experiment report.
type ExperimentTemplateReportConfigurationCloudWatchDashboard struct {
	DashboardIdentifier string `json:"dashboardIdentifier"`
}

// ExperimentTemplateReportConfigurationOutputs holds the output destinations for
// an experiment report.
type ExperimentTemplateReportConfigurationOutputs struct {
	S3Configuration *ExperimentTemplateReportConfigurationOutputsS3Configuration `json:"s3Configuration"`
}

// ExperimentTemplateReportConfigurationOutputsS3Configuration is the S3
// destination for a generated experiment report.
type ExperimentTemplateReportConfigurationOutputsS3Configuration struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ----------------------------------------
// Experiment models
// ----------------------------------------

// Experiment is the in-memory representation of a running FIS experiment.
type Experiment struct {
	CreationTime                     time.Time                      `json:"creationTime"`
	StartTime                        time.Time                      `json:"startTime"`
	ExperimentOptions                *ExperimentExperimentOptions   `json:"experimentOptions"`
	ExperimentReportConfiguration    *ExperimentReportConfiguration `json:"experimentReportConfiguration"`
	ExperimentReport                 *ExperimentReport              `json:"experimentReport"`
	Targets                          map[string]ExperimentTarget    `json:"targets"`
	Actions                          map[string]ExperimentAction    `json:"actions"`
	LogConfiguration                 *ExperimentLogConfiguration    `json:"logConfiguration"`
	Tags                             map[string]string              `json:"tags"`
	EndTime                          *time.Time                     `json:"endTime"`
	cancel                           context.CancelFunc             `json:"-"`
	Status                           ExperimentStatus               `json:"status"`
	ExperimentTemplateID             string                         `json:"experimentTemplateID"`
	RoleArn                          string                         `json:"roleArn"`
	ID                               string                         `json:"id"`
	Arn                              string                         `json:"arn"`
	StopConditions                   []ExperimentStopCondition      `json:"stopConditions"`
	TargetAccountConfigurationsCount int                            `json:"targetAccountConfigurationsCount,omitempty"`
}

// ExperimentStatus holds the status string, an optional human-readable reason, and structured error info.
type ExperimentStatus struct {
	Error  *ExperimentStatusError `json:"error,omitempty"`
	Status string                 `json:"status"`
	Reason string                 `json:"reason,omitempty"`
}

// ExperimentStatusError holds structured error info for failed experiments.
// Field names mirror the real AWS FIS wire shape (types.ExperimentError): "code" and
// "location", not "exceptionName" — the real deserializer discards unknown keys, so a
// mismatched field name silently reaches SDK callers as an always-nil Code.
type ExperimentStatusError struct {
	Code      string `json:"code,omitempty"`
	Location  string `json:"location,omitempty"`
	AccountID string `json:"accountId,omitempty"`
}

// ExperimentTarget holds resolved resource ARNs for a target group. Filters,
// ResourceTags, and SelectionMode mirror the target's original template
// definition and are carried through as informational metadata alongside the
// resolved ResourceArns, matching the real AWS FIS wire shape (types.ExperimentTarget).
type ExperimentTarget struct {
	Parameters    map[string]string                `json:"parameters"`
	ResourceTags  map[string]string                `json:"resourceTags"`
	Filters       []ExperimentTemplateTargetFilter `json:"filters"`
	ResourceType  string                           `json:"resourceType"`
	SelectionMode string                           `json:"selectionMode"`
	ResourceArns  []string                         `json:"resourceArns"`
}

// ExperimentAction tracks the state of an individual experiment action.
type ExperimentAction struct {
	Parameters  map[string]string      `json:"parameters"`
	Targets     map[string]string      `json:"targets"`
	StartTime   *time.Time             `json:"startTime"`
	EndTime     *time.Time             `json:"endTime"`
	Status      ExperimentActionStatus `json:"status"`
	ActionID    string                 `json:"actionID"`
	Description string                 `json:"description"`
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

// ExperimentExperimentOptions controls account and target resolution behaviour,
// plus the resolved actions mode ("run-all" or "skip-all") the experiment was
// started with.
type ExperimentExperimentOptions struct {
	AccountTargeting          string `json:"accountTargeting"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode"`
	ActionsMode               string `json:"actionsMode"`
}

// ExperimentReportConfiguration describes the report generation settings
// resolved for a running experiment (copied from its template at StartExperiment
// time). Shape mirrors ExperimentTemplateReportConfiguration; kept as a distinct
// Go type to match the real AWS FIS SDK's ExperimentTemplateReportConfiguration /
// ExperimentReportConfiguration split.
type ExperimentReportConfiguration struct {
	DataSources            *ExperimentReportConfigurationDataSources `json:"dataSources"`
	Outputs                *ExperimentReportConfigurationOutputs     `json:"outputs"`
	PreExperimentDuration  string                                    `json:"preExperimentDuration"`
	PostExperimentDuration string                                    `json:"postExperimentDuration"`
}

// ExperimentReportConfigurationDataSources lists the data sources for an
// experiment report.
type ExperimentReportConfigurationDataSources struct {
	CloudWatchDashboards []ExperimentReportConfigurationCloudWatchDashboard `json:"cloudWatchDashboards"`
}

// ExperimentReportConfigurationCloudWatchDashboard identifies a CloudWatch
// dashboard whose widgets are captured as snapshot graphs in the experiment report.
type ExperimentReportConfigurationCloudWatchDashboard struct {
	DashboardIdentifier string `json:"dashboardIdentifier"`
}

// ExperimentReportConfigurationOutputs holds the output destinations for an
// experiment report.
type ExperimentReportConfigurationOutputs struct {
	S3Configuration *ExperimentReportConfigurationOutputsS3Configuration `json:"s3Configuration"`
}

// ExperimentReportConfigurationOutputsS3Configuration is the S3 destination for
// a generated experiment report.
type ExperimentReportConfigurationOutputsS3Configuration struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// ExperimentReport describes the generated report for an experiment: its state
// (pending/running/completed/cancelled/failed) and, once completed, the S3
// objects holding the generated artifacts.
type ExperimentReport struct {
	State     *ExperimentReportState     `json:"state"`
	S3Reports []ExperimentReportS3Report `json:"s3Reports"`
}

// ExperimentReportS3Report identifies a single generated report artifact in S3.
type ExperimentReportS3Report struct {
	Arn        string `json:"arn"`
	ReportType string `json:"reportType"`
}

// ExperimentReportState holds the status, optional human-readable reason, and
// structured error info for experiment report generation.
type ExperimentReportState struct {
	Error  *ExperimentReportError `json:"error,omitempty"`
	Status string                 `json:"status"`
	Reason string                 `json:"reason,omitempty"`
}

// ExperimentReportError holds the error code when experiment report generation fails.
type ExperimentReportError struct {
	Code string `json:"code,omitempty"`
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
	Tags                          map[string]string                       `json:"tags"`
	Targets                       map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions                       map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration              *experimentTemplateLogConfigurationDTO  `json:"logConfiguration"`
	ExperimentOptions             *experimentTemplateExperimentOptionsDTO `json:"experimentOptions"`
	ExperimentReportConfiguration *experimentTemplateReportConfigDTO      `json:"experimentReportConfiguration"`
	ClientToken                   string                                  `json:"clientToken"`
	Description                   string                                  `json:"description"`
	RoleArn                       string                                  `json:"roleArn"`
	StopConditions                []experimentTemplateStopConditionDTO    `json:"stopConditions"`
}

// updateExperimentTemplateRequest is the JSON body for PATCH /experimentTemplates/{id}.
type updateExperimentTemplateRequest struct {
	Targets                       map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions                       map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration              *experimentTemplateLogConfigurationDTO  `json:"logConfiguration"`
	ExperimentOptions             *experimentTemplateExperimentOptionsDTO `json:"experimentOptions"`
	ExperimentReportConfiguration *experimentTemplateReportConfigDTO      `json:"experimentReportConfiguration"`
	Description                   string                                  `json:"description"`
	RoleArn                       string                                  `json:"roleArn"`
	StopConditions                []experimentTemplateStopConditionDTO    `json:"stopConditions"`
}

// startExperimentExperimentOptionsDTO is the JSON representation of StartExperiment's
// experimentOptions input: the actions mode ("run-all" or "skip-all") to run the
// experiment with.
type startExperimentExperimentOptionsDTO struct {
	ActionsMode string `json:"actionsMode,omitempty"`
}

// startExperimentRequest is the JSON body for POST /experiments.
type startExperimentRequest struct {
	Tags                 map[string]string                    `json:"tags"`
	ExperimentOptions    *startExperimentExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	ClientToken          string                               `json:"clientToken"`
	ExperimentTemplateID string                               `json:"experimentTemplateId"`
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

// experimentTemplateReportConfigDTO is the JSON representation of an
// experiment template's report configuration, shared by the create/update
// request bodies and the template response DTO (identical wire shape).
type experimentTemplateReportConfigDTO struct {
	DataSources            *experimentTemplateReportDataSourcesDTO `json:"dataSources,omitempty"`
	Outputs                *experimentTemplateReportOutputsDTO     `json:"outputs,omitempty"`
	PreExperimentDuration  string                                  `json:"preExperimentDuration,omitempty"`
	PostExperimentDuration string                                  `json:"postExperimentDuration,omitempty"`
}

// experimentTemplateReportDataSourcesDTO holds the report's data sources.
type experimentTemplateReportDataSourcesDTO struct {
	CloudWatchDashboards []experimentTemplateReportDashboardDTO `json:"cloudWatchDashboards,omitempty"`
}

// experimentTemplateReportDashboardDTO identifies a CloudWatch dashboard.
type experimentTemplateReportDashboardDTO struct {
	DashboardIdentifier string `json:"dashboardIdentifier,omitempty"`
}

// experimentTemplateReportOutputsDTO holds the report's output destinations.
type experimentTemplateReportOutputsDTO struct {
	S3Configuration *experimentTemplateReportOutputsS3DTO `json:"s3Configuration,omitempty"`
}

// experimentTemplateReportOutputsS3DTO is the S3 destination for a report.
type experimentTemplateReportOutputsS3DTO struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix,omitempty"`
}

// experimentTemplateResponseDTO is the outer envelope for experiment-template responses.
type experimentTemplateResponseDTO struct {
	ExperimentTemplate experimentTemplateDTO `json:"experimentTemplate"`
}

// experimentTemplateSummaryDTO is the JSON representation of an experiment template summary (ListExperimentTemplates).
// Mirrors real AWS SDK types.ExperimentTemplateSummary.
type experimentTemplateSummaryDTO struct {
	Tags           map[string]string `json:"tags,omitempty"`
	ID             string            `json:"id"`
	Arn            string            `json:"arn"`
	Description    string            `json:"description,omitempty"`
	CreationTime   float64           `json:"creationTime"`
	LastUpdateTime float64           `json:"lastUpdateTime"`
}

// listExperimentTemplatesResponseDTO is the outer envelope for list responses.
type listExperimentTemplatesResponseDTO struct {
	NextToken           string                         `json:"nextToken,omitempty"`
	ExperimentTemplates []experimentTemplateSummaryDTO `json:"experimentTemplates"`
}

// experimentTemplateDTO is the JSON representation of an experiment template.
type experimentTemplateDTO struct {
	Tags                          map[string]string                       `json:"tags"`
	Targets                       map[string]experimentTemplateTargetDTO  `json:"targets"`
	Actions                       map[string]experimentTemplateActionDTO  `json:"actions"`
	LogConfiguration              *experimentTemplateLogConfigurationDTO  `json:"logConfiguration,omitempty"`
	ExperimentOptions             *experimentTemplateExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	ExperimentReportConfiguration *experimentTemplateReportConfigDTO      `json:"experimentReportConfiguration,omitempty"`
	ID                            string                                  `json:"id"`
	Arn                           string                                  `json:"arn"`
	Description                   string                                  `json:"description,omitempty"`
	RoleArn                       string                                  `json:"roleArn,omitempty"`
	StopConditions                []experimentTemplateStopConditionDTO    `json:"stopConditions"`
	CreationTime                  float64                                 `json:"creationTime"`
	LastUpdateTime                float64                                 `json:"lastUpdateTime"`
}

// experimentResponseDTO is the outer envelope for experiment responses.
type experimentResponseDTO struct {
	Experiment experimentDTO `json:"experiment"`
}

// experimentSummaryDTO is the JSON representation of an experiment summary (ListExperiments).
// Mirrors real AWS SDK types.ExperimentSummary.
type experimentSummaryDTO struct {
	Tags                 map[string]string               `json:"tags,omitempty"`
	ExperimentOptions    *experimentExperimentOptionsDTO `json:"experimentOptions,omitempty"`
	State                experimentStatusDTO             `json:"state"`
	ID                   string                          `json:"id"`
	Arn                  string                          `json:"arn"`
	ExperimentTemplateID string                          `json:"experimentTemplateId"`
	CreationTime         float64                         `json:"creationTime"`
}

// listExperimentsResponseDTO is the outer envelope for list experiments responses.
type listExperimentsResponseDTO struct {
	NextToken   string                 `json:"nextToken,omitempty"`
	Experiments []experimentSummaryDTO `json:"experiments"`
}

// experimentDTO is the JSON representation of a running experiment.
type experimentDTO struct {
	ExperimentOptions                *experimentExperimentOptionsDTO   `json:"experimentOptions,omitempty"`
	ExperimentReportConfiguration    *experimentReportConfigurationDTO `json:"experimentReportConfiguration,omitempty"`
	ExperimentReport                 *experimentReportDTO              `json:"experimentReport,omitempty"`
	Targets                          map[string]experimentTargetDTO    `json:"targets"`
	Actions                          map[string]experimentActionDTO    `json:"actions"`
	LogConfiguration                 *experimentLogConfigurationDTO    `json:"logConfiguration,omitempty"`
	Tags                             map[string]string                 `json:"tags"`
	EndTime                          *float64                          `json:"endTime,omitempty"`
	State                            experimentStatusDTO               `json:"state"`
	Status                           experimentStatusDTO               `json:"status"`
	Arn                              string                            `json:"arn"`
	ExperimentTemplateID             string                            `json:"experimentTemplateId"`
	RoleArn                          string                            `json:"roleArn,omitempty"`
	ID                               string                            `json:"id"`
	StopConditions                   []experimentStopConditionDTO      `json:"stopConditions"`
	CreationTime                     float64                           `json:"creationTime"`
	StartTime                        float64                           `json:"startTime"`
	TargetAccountConfigurationsCount int                               `json:"targetAccountConfigurationsCount,omitempty"`
}

// experimentStatusDTO is the JSON representation of an experiment status.
type experimentStatusDTO struct {
	Error  *experimentStatusErrorDTO `json:"error,omitempty"`
	Status string                    `json:"status"`
	Reason string                    `json:"reason,omitempty"`
}

// experimentStatusErrorDTO is the JSON representation of structured error info on
// failed experiments, matching the real FIS wire shape (types.ExperimentError):
// "code" and "location", not "exceptionName".
type experimentStatusErrorDTO struct {
	Code      string `json:"code,omitempty"`
	Location  string `json:"location,omitempty"`
	AccountID string `json:"accountId,omitempty"`
}

// experimentTargetDTO is the JSON representation of a resolved target. Filters,
// ResourceTags, and SelectionMode mirror the real AWS FIS wire shape
// (types.ExperimentTarget), carried through from the owning template's target.
type experimentTargetDTO struct {
	Parameters    map[string]string                   `json:"parameters,omitempty"`
	ResourceTags  map[string]string                   `json:"resourceTags,omitempty"`
	Filters       []experimentTemplateTargetFilterDTO `json:"filters,omitempty"`
	ResourceType  string                              `json:"resourceType"`
	SelectionMode string                              `json:"selectionMode,omitempty"`
	ResourceArns  []string                            `json:"resourceArns,omitempty"`
}

// experimentActionDTO is the JSON representation of a running experiment action.
type experimentActionDTO struct {
	Parameters  map[string]string          `json:"parameters,omitempty"`
	Targets     map[string]string          `json:"targets,omitempty"`
	State       *experimentActionStatusDTO `json:"state,omitempty"`
	Status      *experimentActionStatusDTO `json:"status,omitempty"`
	StartTime   *float64                   `json:"startTime,omitempty"`
	EndTime     *float64                   `json:"endTime,omitempty"`
	ActionID    string                     `json:"actionId"`
	Description string                     `json:"description,omitempty"`
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

// experimentExperimentOptionsDTO holds account targeting and resolution options,
// plus the resolved actions mode the experiment was started with.
type experimentExperimentOptionsDTO struct {
	AccountTargeting          string `json:"accountTargeting,omitempty"`
	EmptyTargetResolutionMode string `json:"emptyTargetResolutionMode,omitempty"`
	ActionsMode               string `json:"actionsMode,omitempty"`
}

// experimentReportConfigurationDTO is the JSON representation of a running
// experiment's report configuration.
type experimentReportConfigurationDTO struct {
	DataSources            *experimentReportConfigurationDataSourcesDTO `json:"dataSources,omitempty"`
	Outputs                *experimentReportConfigurationOutputsDTO     `json:"outputs,omitempty"`
	PreExperimentDuration  string                                       `json:"preExperimentDuration,omitempty"`
	PostExperimentDuration string                                       `json:"postExperimentDuration,omitempty"`
}

// experimentReportConfigurationDataSourcesDTO holds the report's data sources.
type experimentReportConfigurationDataSourcesDTO struct {
	CloudWatchDashboards []experimentReportConfigurationCloudWatchDashboardDTO `json:"cloudWatchDashboards,omitempty"`
}

// experimentReportConfigurationCloudWatchDashboardDTO identifies a CloudWatch dashboard.
type experimentReportConfigurationCloudWatchDashboardDTO struct {
	DashboardIdentifier string `json:"dashboardIdentifier,omitempty"`
}

// experimentReportConfigurationOutputsDTO holds the report's output destinations.
type experimentReportConfigurationOutputsDTO struct {
	S3Configuration *experimentReportConfigurationOutputsS3ConfigurationDTO `json:"s3Configuration,omitempty"`
}

// experimentReportConfigurationOutputsS3ConfigurationDTO is the S3 destination for a report.
type experimentReportConfigurationOutputsS3ConfigurationDTO struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix,omitempty"`
}

// experimentReportDTO is the JSON representation of a running experiment's
// generated report.
type experimentReportDTO struct {
	State     *experimentReportStateDTO     `json:"state,omitempty"`
	S3Reports []experimentReportS3ReportDTO `json:"s3Reports,omitempty"`
}

// experimentReportS3ReportDTO identifies a single generated report artifact in S3.
type experimentReportS3ReportDTO struct {
	Arn        string `json:"arn,omitempty"`
	ReportType string `json:"reportType,omitempty"`
}

// experimentReportStateDTO is the JSON representation of experiment report generation state.
type experimentReportStateDTO struct {
	Error  *experimentReportErrorDTO `json:"error,omitempty"`
	Status string                    `json:"status"`
	Reason string                    `json:"reason,omitempty"`
}

// experimentReportErrorDTO holds the error code when experiment report generation fails.
type experimentReportErrorDTO struct {
	Code string `json:"code,omitempty"`
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
	ResourceType string
	TargetName   string
}

// resolvedTargetDTO is the JSON representation of a resolved target. The real AWS FIS
// wire shape (types.ResolvedTarget) has exactly three fields: resourceType, targetName,
// and targetInformation (a generic string-to-string map whose documented contents are
// not specified by AWS beyond length/pattern constraints and vary by resource type).
// gopherstack previously emitted invented "resolvedArns"/"targetResourcesCount" fields
// that do not exist on the real type -- a real SDK client deserializing this response
// would see none of that data (unknown JSON keys are dropped) and an always-empty
// targetInformation. Fixed to the real three fields. Because AWS does not publish the
// key schema for targetInformation and gopherstack does not model per-resource-type
// target metadata, targetInformation is honestly left empty here rather than inventing
// a key structure (e.g. stuffing ARNs under a made-up "resourceArn" key) that would look
// official without being verified against real AWS behavior.
type resolvedTargetDTO struct {
	TargetInformation map[string]string `json:"targetInformation,omitempty"`
	ResourceType      string            `json:"resourceType"`
	TargetName        string            `json:"targetName"`
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

// safetyLeverDTO is the JSON representation of a safety lever. The real AWS FIS
// wire shape (types.SafetyLever) has no "tags" field -- GetSafetyLever /
// UpdateSafetyLeverState never surface tags directly, even though a safety
// lever's ARN can still be tagged via the generic TagResource / UntagResource /
// ListTagsForResource operations (see tags.go). A gopherstack-invented "tags"
// field used to leak onto this response; deliberately absent here now.
type safetyLeverDTO struct {
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
