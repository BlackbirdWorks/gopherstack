// Package sagemaker provides stub handlers for the 335 SageMaker SDK operations
// not yet fully implemented. Each stub returns a minimal valid JSON response.
package sagemaker

import (
	"context"
	"encoding/json"
)

// Stub ARN / key constants used across stub responses.
const (
	keyAppImageConfigArn               = "AppImageConfigArn"
	keyArtifactArn                     = "ArtifactArn"
	keyAutoMLJobArn                    = "AutoMLJobArn"
	keyClusterSchedulerConfigArn       = "ClusterSchedulerConfigArn"
	keyCodeRepositoryArn               = "CodeRepositoryArn"
	keyCompilationJobArn               = "CompilationJobArn"
	keyComputeQuotaArn                 = "ComputeQuotaArn"
	keyContextArn                      = "ContextArn"
	keyJobDefinitionArn                = "JobDefinitionArn"
	keyJobDefinitionName               = "JobDefinitionName"
	keyDomainArn                       = "DomainArn"
	keyEdgeDeploymentPlanArn           = "EdgeDeploymentPlanArn"
	keyExperimentArn                   = "ExperimentArn"
	keyFeatureGroupArn                 = "FeatureGroupArn"
	keyFlowDefinitionArn               = "FlowDefinitionArn"
	keyHubArn                          = "HubArn"
	keyHubContentArn                   = "HubContentArn"
	keyHumanTaskUIArn                  = "HumanTaskUiArn"
	keyImageArn                        = "ImageArn"
	keyImageVersionArn                 = "ImageVersionArn"
	keyInferenceComponentArn           = "InferenceComponentArn"
	keyInferenceExperimentArn          = "InferenceExperimentArn"
	keyLabelingJobArn                  = "LabelingJobArn"
	keyLineageGroupArn                 = "LineageGroupArn"
	keyModelCardArn                    = "ModelCardArn"
	keyModelCardExportJobArn           = "ModelCardExportJobArn"
	keyModelPackageArn                 = "ModelPackageArn"
	keyModelPackageGroupArn            = "ModelPackageGroupArn"
	keyMonitoringScheduleArn           = "MonitoringScheduleArn"
	keyNotebookLifecycleConfigArn      = "NotebookInstanceLifecycleConfigArn"
	keyOptimizationJobArn              = "OptimizationJobArn"
	keyPipelineArn                     = "PipelineArn"
	keyPipelineExecutionArn            = "PipelineExecutionArn"
	keyProcessingJobArn                = "ProcessingJobArn"
	keyProjectArn                      = "ProjectArn"
	keySpaceArn                        = "SpaceArn"
	keyStudioLifecycleConfigArn        = "StudioLifecycleConfigArn"
	keyTrackingServerArn               = "TrackingServerArn"
	keyTrainingPlanArn                 = "TrainingPlanArn"
	keyTransformJobArn                 = "TransformJobArn"
	keyTrialArn                        = "TrialArn"
	keyTrialComponentArn               = "TrialComponentArn"
	keyUserProfileArn                  = "UserProfileArn"
	keyWorkforceArn                    = "WorkforceArn"
	keyWorkteamArn                     = "WorkteamArn"
	keyAuthorizedURL                   = "AuthorizedUrl"
	keyGenericArn                      = "Arn"
	keyActionArn                       = "ActionArn"
	keyAppArn                          = "AppArn"
	keyFeatureGroupStatus              = "FeatureGroupStatus"
	keyMonitoringScheduleName          = "MonitoringScheduleName"
	keyMonitoringScheduleStatus        = "MonitoringScheduleStatus"
	keyPipelineExecutionStatus         = "PipelineExecutionStatus"
	keyModelCardStatus                 = "ModelCardStatus"
	keyModelCardVersion                = "ModelCardVersion"
	keyModelApprovalStatus             = "ModelApprovalStatus"
	keyModelPackageStatus              = "ModelPackageStatus"
	keyReservedCapacityArn             = "ReservedCapacityArn"
	keyFeatureGroupName                = "FeatureGroupName"
	keyFeatureDefinitions              = "FeatureDefinitions"
	keyRecordIdentifierFeatureName     = "RecordIdentifierFeatureName"
	keyEventTimeFeatureName            = "EventTimeFeatureName"
	keyMonitoringAlertName             = "MonitoringAlertName"
	keyClusterSchedulerConfigSummaries = "ClusterSchedulerConfigSummaries"
	keyTrainingJobSummaries            = "TrainingJobSummaries"
	keyDeviceFleetName                 = "DeviceFleetName"
	keyStatus                          = "Status"
	statusCompleted                    = algorithmStatusCompleted
	statusInService                    = clusterStatusInService
	statusCompletedUpper               = "COMPLETED"
	statusCreated                      = "Created"
	statusActive                       = "Active"
	statusPendingManualApproval        = "PendingManualApproval"
)

// stubOpsSupported returns the list of stub-implemented operations.
//
//nolint:funlen
func stubOpsSupported() []string {
	return []string{
		"CreateArtifact",
		"CreateCluster",
		"CreateClusterSchedulerConfig",
		"CreateComputeQuota",
		"CreateContext",
		"CreateDeviceFleet",
		"CreateEdgeDeploymentPlan",
		"CreateEdgeDeploymentStage",
		"CreateHub",
		"CreateHubContentPresignedUrls",
		"CreateHubContentReference",
		"CreateInferenceComponent",
		"CreateLabelingJob",
		"CreateMlflowApp",
		"CreateModelCardExportJob",
		"CreatePartnerAppPresignedUrl",
		"CreatePresignedDomainUrl",
		"CreatePresignedMlflowAppUrl",
		"CreatePresignedMlflowTrackingServerUrl",
		"DeleteAction",
		"DeleteAlgorithm",
		"DeleteArtifact",
		"DeleteAssociation",
		"DeleteCluster",
		"DeleteClusterSchedulerConfig",
		"DeleteComputeQuota",
		"DeleteContext",
		"DeleteDeviceFleet",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteHub",
		"DeleteHubContent",
		"DeleteHubContentReference",
		"DeleteInferenceComponent",
		"DeleteMlflowApp",
		"DeleteModelPackageGroupPolicy",
		"DeleteProcessingJob",
		"DeleteWorkforce",
		"DeregisterDevices",
		"DescribeAction",
		"DescribeAlgorithm",
		"DescribeArtifact",
		"DescribeCluster",
		"DescribeClusterEvent",
		"DescribeClusterNode",
		"DescribeClusterSchedulerConfig",
		"DescribeComputeQuota",
		"DescribeContext",
		"DescribeDevice",
		"DescribeDeviceFleet",
		"DescribeEdgeDeploymentPlan",
		"DescribeFeatureMetadata",
		"DescribeHub",
		"DescribeHubContent",
		"DescribeInferenceComponent",
		"DescribeLabelingJob",
		"DescribeLineageGroup",
		"DescribeMlflowApp",
		"DescribeModelCardExportJob",
		"DescribePipelineDefinitionForExecution",
		"DescribeReservedCapacity",
		"DescribeSubscribedWorkteam",
		"DescribeTrainingPlanExtensionHistory",
		"DetachClusterNodeVolume",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"ExtendTrainingPlan",
		"GetDeviceFleetReport",
		"GetLineageGroupPolicy",
		"GetModelPackageGroupPolicy",
		"GetSagemakerServicecatalogPortfolioStatus",
		"GetScalingConfigurationRecommendation",
		"GetSearchSuggestions",
		"ImportHubContent",
		"ListActions",
		"ListAlgorithms",
		"ListAliases",
		"ListArtifacts",
		"ListAssociations",
		"ListCandidatesForAutoMLJob",
		"ListClusterEvents",
		"ListClusterNodes",
		"ListClusterSchedulerConfigs",
		"ListClusters",
		"ListComputeQuotas",
		"ListContexts",
		"ListDataQualityJobDefinitions",
		"ListDeviceFleets",
		"ListDevices",
		"ListEdgeDeploymentPlans",
		"ListHubContentVersions",
		"ListHubContents",
		"ListHubs",
		"ListInferenceComponents",
		"ListLabelingJobs",
		"ListLabelingJobsForWorkteam",
		"ListLineageGroups",
		"ListMlflowApps",
		"ListModelBiasJobDefinitions",
		"ListModelExplainabilityJobDefinitions",
		"ListModelMetadata",
		"ListModelQualityJobDefinitions",
		"ListMonitoringAlertHistory",
		"ListMonitoringAlerts",
		"ListMonitoringExecutions",
		"ListPartnerApps",
		// ListPipelineParametersForExecution — real implementation in handler_accuracy2.go
		"ListPipelineVersions",
		"ListResourceCatalogs",
		"ListStageDevices",
		"ListSubscribedWorkteams",
		"ListTrainingPlans",
		"ListTrialComponents",
		"ListUltraServersByReservedCapacity",
		"ListWorkforces",
		"PutModelPackageGroupPolicy",
		"QueryLineage",
		"RegisterDevices",
		"RenderUiTemplate",
		"Search",
		"SearchTrainingPlanOfferings",
		"StartEdgeDeploymentStage",
		"StartInferenceExperiment",
		"StartSession",
		"StopEdgeDeploymentStage",
		"StopLabelingJob",
		"UpdateAction",
		"UpdateArtifact",
		"UpdateCluster",
		"UpdateClusterSchedulerConfig",
		"UpdateClusterSoftware",
		"UpdateComputeQuota",
		"UpdateContext",
		"UpdateDeviceFleet",
		"UpdateDevices",
		"UpdateFeatureMetadata",
		"UpdateHub",
		"UpdateHubContent",
		"UpdateHubContentReference",
		"UpdateImage",
		"UpdateImageVersion",
		"UpdateInferenceComponent",
		"UpdateInferenceComponentRuntimeConfig",
		"UpdateInferenceExperiment",
		"UpdateMlflowApp",
		"UpdateMonitoringAlert",
		"UpdatePartnerApp",
		"UpdatePipelineExecution",
		"UpdatePipelineVersion",
		"UpdateProject",
		"UpdateWorkteam",
	}
}

// stubResponseFor returns the canned JSON response for stub operations.
// Returns nil if the operation is not a stub.
//
//nolint:funlen,cyclop,gocyclo // 335 stub operations require a large dispatch table
func stubResponseFor(op string) ([]byte, bool) {
	type m = map[string]any

	switch op {
	// -----------------------------------------------------------------------
	// Create — return ARN / primary field
	// -----------------------------------------------------------------------
	case "CreateApp":
		return mustMarshal(m{keyAppArn: ""}), true

	case "CreateArtifact":
		return mustMarshal(m{keyArtifactArn: ""}), true

	case "CreateCluster":
		return mustMarshal(m{keyClusterArn: ""}), true

	case "CreateClusterSchedulerConfig":
		return mustMarshal(m{keyClusterSchedulerConfigArn: ""}), true

	case "CreateComputeQuota":
		return mustMarshal(m{keyComputeQuotaArn: ""}), true

	case "CreateContext":
		return mustMarshal(m{keyContextArn: ""}), true

	case "CreateDeviceFleet":
		return mustMarshal(m{}), true

	case "CreateDomain":
		return mustMarshal(m{keyDomainArn: "", "Url": ""}), true

	case "CreateEdgeDeploymentPlan":
		return mustMarshal(m{keyEdgeDeploymentPlanArn: ""}), true

	case "CreateEdgeDeploymentStage", "CreateEdgePackagingJob":
		return mustMarshal(m{}), true

	case "CreateExperiment":
		return mustMarshal(m{keyExperimentArn: ""}), true

	case "CreateFeatureGroup":
		return mustMarshal(m{keyFeatureGroupArn: ""}), true

	case "CreateHub":
		return mustMarshal(m{keyHubArn: ""}), true

	case "CreateHubContentPresignedUrls":
		return mustMarshal(m{keyAuthorizedURL: ""}), true

	case "CreateHubContentReference":
		return mustMarshal(m{keyHubArn: "", keyHubContentArn: ""}), true

	case "CreateInferenceComponent":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "CreateLabelingJob":
		return mustMarshal(m{keyLabelingJobArn: ""}), true

	case "CreateMlflowApp":
		return mustMarshal(m{keyGenericArn: ""}), true

	case "CreateModelCardExportJob":
		return mustMarshal(m{keyModelCardExportJobArn: ""}), true

	case "CreatePartnerAppPresignedUrl",
		"CreatePresignedDomainUrl",
		"CreatePresignedMlflowAppUrl",
		"CreatePresignedMlflowTrackingServerUrl":
		return mustMarshal(m{keyAuthorizedURL: ""}), true

	case "CreatePipeline":
		return mustMarshal(m{keyPipelineArn: ""}), true

	case "CreateTrial":
		return mustMarshal(m{keyTrialArn: ""}), true

	case "CreateTrialComponent":
		return mustMarshal(m{keyTrialComponentArn: ""}), true

	case "CreateUserProfile":
		return mustMarshal(m{keyUserProfileArn: ""}), true

	// -----------------------------------------------------------------------
	// Delete / stop / misc — return empty object
	// -----------------------------------------------------------------------
	case "DeleteAction",
		"DeleteAlgorithm",
		"DeleteApp",
		"DeleteArtifact",
		"DeleteAssociation",
		"DeleteCluster",
		"DeleteClusterSchedulerConfig",
		"DeleteComputeQuota",
		"DeleteContext",
		"DeleteDeviceFleet",
		"DeleteDomain",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteExperiment",
		"DeleteFeatureGroup",
		"DeleteHub",
		"DeleteHubContent",
		"DeleteHubContentReference",
		"DeleteInferenceComponent",
		"DeleteMlflowApp",
		"DeleteModelPackageGroupPolicy",
		"DeletePipeline",
		"DeleteProcessingJob",
		"DeleteWorkforce",
		"DeleteTrial",
		"DeleteTrialComponent",
		"DeleteUserProfile",
		"DeregisterDevices",
		"DetachClusterNodeVolume",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"ExtendTrainingPlan",
		"ImportHubContent",
		"PutModelPackageGroupPolicy",
		"RegisterDevices",
		"RenderUiTemplate",
		"StartEdgeDeploymentStage",
		"StartSession",
		"StopEdgeDeploymentStage",
		"StopLabelingJob",
		"UpdateClusterSoftware",
		"UpdateDeviceFleet",
		"UpdateDevices",
		"UpdateFeatureMetadata":
		return mustMarshal(m{}), true

	// -----------------------------------------------------------------------
	// Describe ops
	// -----------------------------------------------------------------------
	case "DescribeAction":
		return mustMarshal(m{
			"ActionName": "", keyActionArn: "", "ActionType": "", keyStatus: statusCompleted,
		}), true

	case "DescribeAlgorithm":
		return mustMarshal(m{
			"AlgorithmName": "", "AlgorithmArn": "", "AlgorithmStatus": statusCompleted,
		}), true

	case "DescribeApp":
		return mustMarshal(m{
			keyAppArn: "", "AppType": "", "AppName": "", keyStatus: statusInService,
		}), true

	case "DescribeArtifact":
		return mustMarshal(m{keyArtifactArn: "", "ArtifactType": ""}), true

	case "DescribeCluster":
		return mustMarshal(m{
			keyClusterArn: "", "ClusterStatus": statusInService, "InstanceGroups": []any{},
		}), true

	case "DescribeClusterEvent":
		return mustMarshal(m{"EventId": "", "Message": "", "Timestamp": 0}), true

	case "DescribeClusterNode":
		return mustMarshal(m{"NodeDetails": m{}}), true

	case "DescribeClusterSchedulerConfig":
		return mustMarshal(m{keyClusterSchedulerConfigArn: "", keyStatus: "Creating"}), true

	case "DescribeComputeQuota":
		return mustMarshal(m{keyComputeQuotaArn: "", keyStatus: statusCreated}), true

	case "DescribeContext":
		return mustMarshal(m{keyContextArn: "", "ContextType": ""}), true

	case "DescribeDevice":
		return mustMarshal(m{"DeviceName": "", keyDeviceFleetName: ""}), true

	case "DescribeDeviceFleet":
		return mustMarshal(m{keyDeviceFleetName: "", "DeviceFleetArn": ""}), true

	case "DescribeDomain":
		return mustMarshal(m{keyDomainArn: "", keyDomainID: "", keyStatus: statusInService}), true

	case "DescribeEdgeDeploymentPlan":
		return mustMarshal(m{
			keyEdgeDeploymentPlanArn: "", "EdgeDeploymentPlanName": "", "Stages": []any{},
		}), true

	case "DescribeEdgePackagingJob":
		return mustMarshal(m{
			"EdgePackagingJobArn": "", "EdgePackagingJobName": "", "EdgePackagingJobStatus": statusCompletedUpper,
		}), true

	case "DescribeExperiment":
		return mustMarshal(m{keyExperimentArn: "", keyExperimentName: ""}), true

	case "DescribeFeatureGroup":
		return mustMarshal(m{
			keyFeatureGroupArn:             "",
			keyFeatureGroupName:            "",
			keyFeatureGroupStatus:          statusCreated,
			keyFeatureDefinitions:          []any{},
			keyRecordIdentifierFeatureName: "",
			keyEventTimeFeatureName:        "",
		}), true

	case "DescribeFeatureMetadata":
		return mustMarshal(m{
			keyFeatureGroupArn: "", keyFeatureGroupName: "", "FeatureName": "", "FeatureType": "",
		}), true

	case "DescribeHub":
		return mustMarshal(m{keyHubArn: "", "HubName": "", "HubStatus": statusInService}), true

	case "DescribeHubContent":
		return mustMarshal(m{
			keyHubContentArn: "", "HubContentName": "", "HubContentStatus": "Available",
		}), true

	case "DescribeInferenceComponent":
		return mustMarshal(m{
			keyInferenceComponentArn: "", "InferenceComponentName": "", "InferenceComponentStatus": statusInService,
		}), true

	case "DescribeLabelingJob":
		return mustMarshal(m{
			keyLabelingJobArn: "", "LabelingJobName": "", "LabelingJobStatus": statusCompleted,
		}), true

	case "DescribeLineageGroup":
		return mustMarshal(m{keyLineageGroupArn: "", "LineageGroupName": ""}), true

	case "DescribeMlflowApp":
		return mustMarshal(m{keyGenericArn: "", keyStatus: statusInService}), true

	case "DescribeModelCardExportJob":
		return mustMarshal(m{
			keyModelCardExportJobArn: "", "ModelCardExportJobName": "", keyStatus: statusCompleted,
		}), true

	case "DescribePipeline":
		return mustMarshal(
			m{keyPipelineArn: "", "PipelineName": "", "PipelineStatus": statusActive},
		), true

	case "DescribePipelineDefinitionForExecution":
		return mustMarshal(m{"PipelineDefinition": ""}), true

	case "DescribePipelineExecution":
		return mustMarshal(m{
			keyPipelineExecutionArn: "", keyPipelineExecutionStatus: pipelineStatusSucceeded,
		}), true

	case "DescribeReservedCapacity":
		return mustMarshal(m{keyReservedCapacityArn: "", keyStatus: statusActive}), true

	case "DescribeSubscribedWorkteam":
		return mustMarshal(m{"SubscribedWorkteam": m{keyWorkteamArn: ""}}), true

	case "DescribeTrainingPlanExtensionHistory":
		return mustMarshal(m{keyTrainingPlanArn: "", "Extensions": []any{}}), true

	case "DescribeTrial":
		return mustMarshal(m{keyTrialArn: "", "TrialName": ""}), true

	case "DescribeTrialComponent":
		return mustMarshal(m{keyTrialComponentArn: "", "TrialComponentName": ""}), true

	case "DescribeUserProfile":
		return mustMarshal(
			m{keyUserProfileArn: "", "UserProfileName": "", keyStatus: statusInService},
		), true

	// -----------------------------------------------------------------------
	// Get ops
	// -----------------------------------------------------------------------
	case "GetDeviceFleetReport":
		return mustMarshal(m{keyDeviceFleetName: "", "DeviceFleetArn": ""}), true

	case "GetLineageGroupPolicy":
		return mustMarshal(m{keyLineageGroupArn: "", "ResourcePolicy": ""}), true

	case "GetModelPackageGroupPolicy":
		return mustMarshal(m{"ResourcePolicy": ""}), true

	case "GetSagemakerServicecatalogPortfolioStatus":
		return mustMarshal(m{keyStatus: "Disabled"}), true

	case "GetScalingConfigurationRecommendation":
		return mustMarshal(m{"InferenceRecommendationsJobName": "", "RecommendationId": ""}), true

	case "GetSearchSuggestions":
		return mustMarshal(m{"PropertyNameSuggestions": []any{}}), true

	// -----------------------------------------------------------------------
	// List ops — all return an empty array under a domain-appropriate key
	// -----------------------------------------------------------------------
	case "ListActions":
		return mustMarshal(m{"ActionSummaries": []any{}}), true

	case "ListAlgorithms":
		return mustMarshal(m{"AlgorithmSummaryList": []any{}}), true

	case "ListAliases":
		return mustMarshal(m{"SageMakerImageVersionAliases": []any{}}), true

	case "ListApps", "ListMlflowApps":
		return mustMarshal(m{"Apps": []any{}}), true

	case "ListArtifacts":
		return mustMarshal(m{"ArtifactSummaries": []any{}}), true

	case "ListAssociations":
		return mustMarshal(m{"AssociationSummaries": []any{}}), true

	case "ListCandidatesForAutoMLJob":
		return mustMarshal(m{"Candidates": []any{}}), true

	case "ListClusterEvents":
		return mustMarshal(m{"ClusterEvents": []any{}}), true

	case "ListClusterNodes":
		return mustMarshal(m{"ClusterNodeSummaries": []any{}}), true

	case "ListClusterSchedulerConfigs":
		return mustMarshal(m{keyClusterSchedulerConfigSummaries: []any{}}), true

	case "ListClusters":
		return mustMarshal(m{"ClusterSummaries": []any{}}), true

	case "ListComputeQuotas":
		return mustMarshal(m{"ComputeQuotaSummaries": []any{}}), true

	case "ListContexts":
		return mustMarshal(m{"ContextSummaries": []any{}}), true

	case "ListDataQualityJobDefinitions",
		"ListModelBiasJobDefinitions",
		"ListModelExplainabilityJobDefinitions",
		"ListModelQualityJobDefinitions":
		return mustMarshal(m{"JobDefinitionSummaries": []any{}}), true

	case "ListDeviceFleets":
		return mustMarshal(m{"DeviceFleetSummaries": []any{}}), true

	case "ListDevices":
		return mustMarshal(m{"DeviceSummaries": []any{}}), true

	case "ListDomains":
		return mustMarshal(m{"Domains": []any{}}), true

	case "ListEdgeDeploymentPlans":
		return mustMarshal(m{"EdgeDeploymentPlanSummaries": []any{}}), true

	case "ListExperiments":
		return mustMarshal(m{"ExperimentSummaries": []any{}}), true

	case "ListFeatureGroups":
		return mustMarshal(m{"FeatureGroupSummaries": []any{}}), true

	case "ListHubContentVersions", "ListHubContents":
		return mustMarshal(m{"HubContentSummaries": []any{}}), true

	case "ListHubs":
		return mustMarshal(m{"HubSummaries": []any{}}), true

	case "ListInferenceComponents":
		return mustMarshal(m{"InferenceComponents": []any{}}), true

	case "ListLabelingJobs", "ListLabelingJobsForWorkteam":
		return mustMarshal(m{"LabelingJobSummaryList": []any{}}), true

	case "ListLineageGroups":
		return mustMarshal(m{"LineageGroupSummaries": []any{}}), true

	case "ListModelMetadata":
		return mustMarshal(m{"ModelMetadataSummaries": []any{}}), true

	case "ListMonitoringAlertHistory":
		return mustMarshal(m{"MonitoringAlertHistory": []any{}}), true

	case "ListMonitoringAlerts":
		return mustMarshal(m{"MonitoringAlertSummaries": []any{}}), true

	case "ListMonitoringExecutions":
		return mustMarshal(m{"MonitoringExecutionSummaries": []any{}}), true

	case "ListPartnerApps":
		return mustMarshal(m{"Summaries": []any{}}), true

	case "ListPipelineExecutions":
		return mustMarshal(m{"PipelineExecutionSummaries": []any{}}), true

	case "ListPipelineVersions":
		return mustMarshal(m{"PipelineVersionSummaries": []any{}}), true

	case "ListPipelines":
		return mustMarshal(m{"PipelineSummaries": []any{}}), true

	case "ListResourceCatalogs":
		return mustMarshal(m{"ResourceCatalogs": []any{}}), true

	case "ListStageDevices":
		return mustMarshal(m{"DeviceDeploymentSummaries": []any{}}), true

	case "ListSubscribedWorkteams":
		return mustMarshal(m{"SubscribedWorkteams": []any{}}), true

	case "ListTrainingPlans":
		return mustMarshal(m{"TrainingPlanSummaries": []any{}}), true

	case "ListTrialComponents":
		return mustMarshal(m{"TrialComponentSummaries": []any{}}), true

	case "ListTrials":
		return mustMarshal(m{"TrialSummaries": []any{}}), true

	case "ListUltraServersByReservedCapacity":
		return mustMarshal(m{"UltraServers": []any{}}), true

	case "ListUserProfiles":
		return mustMarshal(m{"UserProfiles": []any{}}), true

	case "ListWorkforces":
		return mustMarshal(m{"Workforces": []any{}}), true

	// -----------------------------------------------------------------------
	// Action / query / pipeline ops
	// -----------------------------------------------------------------------
	case "QueryLineage":
		return mustMarshal(m{"Vertices": []any{}, "Edges": []any{}}), true

	case "Search":
		return mustMarshal(m{"Results": []any{}}), true

	case "SearchTrainingPlanOfferings":
		return mustMarshal(m{"TrainingPlanOfferings": []any{}}), true

	case "StartInferenceExperiment":
		return mustMarshal(m{keyInferenceExperimentArn: ""}), true

	case "StartPipelineExecution":
		return mustMarshal(m{keyPipelineExecutionArn: ""}), true

	// -----------------------------------------------------------------------
	// Update ops
	// -----------------------------------------------------------------------
	case "UpdateAction":
		return mustMarshal(m{keyActionArn: ""}), true

	case "UpdateArtifact":
		return mustMarshal(m{keyArtifactArn: ""}), true

	case "UpdateCluster":
		return mustMarshal(m{keyClusterArn: ""}), true

	case "UpdateClusterSchedulerConfig":
		return mustMarshal(m{keyClusterSchedulerConfigArn: ""}), true

	case "UpdateComputeQuota":
		return mustMarshal(m{keyComputeQuotaArn: ""}), true

	case "UpdateContext":
		return mustMarshal(m{keyContextArn: ""}), true

	case "UpdateDomain":
		return mustMarshal(m{keyDomainArn: ""}), true

	case "UpdateHub":
		return mustMarshal(m{keyHubArn: ""}), true

	case "UpdateHubContent", "UpdateHubContentReference":
		return mustMarshal(m{keyHubArn: "", keyHubContentArn: ""}), true

	case "UpdateInferenceComponent":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "UpdateInferenceComponentRuntimeConfig":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "UpdateInferenceExperiment":
		return mustMarshal(m{keyInferenceExperimentArn: ""}), true

	case "UpdateMlflowApp", "UpdatePartnerApp":
		return mustMarshal(m{keyGenericArn: ""}), true

	case "UpdateMonitoringAlert":
		return mustMarshal(m{keyMonitoringScheduleArn: "", keyMonitoringAlertName: ""}), true

	case "UpdatePipeline", "UpdatePipelineVersion":
		return mustMarshal(m{keyPipelineArn: ""}), true

	case "UpdatePipelineExecution":
		return mustMarshal(m{keyPipelineExecutionArn: ""}), true

	case "UpdateProject":
		return mustMarshal(m{keyProjectArn: ""}), true

	case "UpdateWorkteam":
		return mustMarshal(m{"Workteam": m{keyWorkteamArn: ""}}), true
	}

	return nil, false
}

// mustMarshal marshals v to JSON, panicking on error (only called with static data).
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("sagemaker stub: json.Marshal failed: " + err.Error())
	}

	return b
}

// dispatchStubOps handles all stub SageMaker operations.
//
//nolint:unparam // error return matches the dispatch interface; stubs never fail
func (h *Handler) dispatchStubOps(_ context.Context, op string, _ []byte) ([]byte, bool, error) {
	b, ok := stubResponseFor(op)
	if !ok {
		return nil, false, nil
	}

	return b, true, nil
}
