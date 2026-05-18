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
		"CreateAppImageConfig",
		"CreateArtifact",
		"CreateAutoMLJob",
		"CreateAutoMLJobV2",
		"CreateCluster",
		"CreateClusterSchedulerConfig",
		"CreateCodeRepository",
		"CreateCompilationJob",
		"CreateComputeQuota",
		"CreateContext",
		"CreateDataQualityJobDefinition",
		"CreateDeviceFleet",
		"CreateEdgeDeploymentPlan",
		"CreateEdgeDeploymentStage",
		"CreateEdgePackagingJob",
		"CreateFlowDefinition",
		"CreateHub",
		"CreateHubContentPresignedUrls",
		"CreateHubContentReference",
		"CreateHumanTaskUi",
		"CreateImage",
		"CreateImageVersion",
		"CreateInferenceComponent",
		"CreateInferenceExperiment",
		"CreateInferenceRecommendationsJob",
		"CreateLabelingJob",
		"CreateMlflowApp",
		"CreateMlflowTrackingServer",
		"CreateModelBiasJobDefinition",
		"CreateModelCard",
		"CreateModelCardExportJob",
		"CreateModelExplainabilityJobDefinition",
		"CreateModelPackage",
		"CreateModelPackageGroup",
		"CreateModelQualityJobDefinition",
		"CreateMonitoringSchedule",
		"CreateOptimizationJob",
		"CreatePartnerApp",
		"CreatePartnerAppPresignedUrl",
		"CreatePresignedDomainUrl",
		"CreatePresignedMlflowAppUrl",
		"CreatePresignedMlflowTrackingServerUrl",
		"CreateProject",
		"CreateSpace",
		"CreateStudioLifecycleConfig",
		"CreateTrainingPlan",
		"CreateWorkforce",
		"CreateWorkteam",
		"DeleteAction",
		"DeleteAlgorithm",
		"DeleteAppImageConfig",
		"DeleteArtifact",
		"DeleteAssociation",
		"DeleteCluster",
		"DeleteClusterSchedulerConfig",
		"DeleteCodeRepository",
		"DeleteCompilationJob",
		"DeleteComputeQuota",
		"DeleteContext",
		"DeleteDataQualityJobDefinition",
		"DeleteDeviceFleet",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteFlowDefinition",
		"DeleteHub",
		"DeleteHubContent",
		"DeleteHubContentReference",
		"DeleteHumanTaskUi",
		"DeleteImage",
		"DeleteImageVersion",
		"DeleteInferenceComponent",
		"DeleteInferenceExperiment",
		"DeleteMlflowApp",
		"DeleteMlflowTrackingServer",
		"DeleteModelBiasJobDefinition",
		"DeleteModelCard",
		"DeleteModelExplainabilityJobDefinition",
		"DeleteModelPackage",
		"DeleteModelPackageGroup",
		"DeleteModelPackageGroupPolicy",
		"DeleteModelQualityJobDefinition",
		"DeleteMonitoringSchedule",
		"DeleteOptimizationJob",
		"DeletePartnerApp",
		"DeleteProcessingJob",
		"DeleteProject",
		"DeleteSpace",
		"DeleteStudioLifecycleConfig",
		"DeleteWorkforce",
		"DeleteWorkteam",
		"DeregisterDevices",
		"DescribeAction",
		"DescribeAlgorithm",
		"DescribeAppImageConfig",
		"DescribeArtifact",
		"DescribeAutoMLJob",
		"DescribeAutoMLJobV2",
		"DescribeCluster",
		"DescribeClusterEvent",
		"DescribeClusterNode",
		"DescribeClusterSchedulerConfig",
		"DescribeCodeRepository",
		"DescribeCompilationJob",
		"DescribeComputeQuota",
		"DescribeContext",
		"DescribeDataQualityJobDefinition",
		"DescribeDevice",
		"DescribeDeviceFleet",
		"DescribeEdgeDeploymentPlan",
		"DescribeEdgePackagingJob",
		"DescribeFeatureMetadata",
		"DescribeFlowDefinition",
		"DescribeHub",
		"DescribeHubContent",
		"DescribeHumanTaskUi",
		"DescribeImage",
		"DescribeImageVersion",
		"DescribeInferenceComponent",
		"DescribeInferenceExperiment",
		"DescribeInferenceRecommendationsJob",
		"DescribeLabelingJob",
		"DescribeLineageGroup",
		"DescribeMlflowApp",
		"DescribeMlflowTrackingServer",
		"DescribeModelBiasJobDefinition",
		"DescribeModelCard",
		"DescribeModelCardExportJob",
		"DescribeModelExplainabilityJobDefinition",
		"DescribeModelPackage",
		"DescribeModelPackageGroup",
		"DescribeModelQualityJobDefinition",
		"DescribeMonitoringSchedule",
		"DescribeOptimizationJob",
		"DescribePartnerApp",
		"DescribePipelineDefinitionForExecution",
		"DescribeProject",
		"DescribeReservedCapacity",
		"DescribeSpace",
		"DescribeStudioLifecycleConfig",
		"DescribeSubscribedWorkteam",
		"DescribeTrainingPlan",
		"DescribeTrainingPlanExtensionHistory",
		"DescribeWorkforce",
		"DescribeWorkteam",
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
		"ListAppImageConfigs",
		"ListArtifacts",
		"ListAssociations",
		"ListAutoMLJobs",
		"ListCandidatesForAutoMLJob",
		"ListClusterEvents",
		"ListClusterNodes",
		"ListClusterSchedulerConfigs",
		"ListClusters",
		"ListCodeRepositories",
		"ListCompilationJobs",
		"ListComputeQuotas",
		"ListContexts",
		"ListDataQualityJobDefinitions",
		"ListDeviceFleets",
		"ListDevices",
		"ListEdgeDeploymentPlans",
		"ListEdgePackagingJobs",
		"ListFlowDefinitions",
		"ListHubContentVersions",
		"ListHubContents",
		"ListHubs",
		"ListHumanTaskUis",
		"ListImageVersions",
		"ListImages",
		"ListInferenceComponents",
		"ListInferenceExperiments",
		"ListInferenceRecommendationsJobSteps",
		"ListInferenceRecommendationsJobs",
		"ListLabelingJobs",
		"ListLabelingJobsForWorkteam",
		"ListLineageGroups",
		"ListMlflowApps",
		"ListMlflowTrackingServers",
		"ListModelBiasJobDefinitions",
		"ListModelCardExportJobs",
		"ListModelCardVersions",
		"ListModelCards",
		"ListModelExplainabilityJobDefinitions",
		"ListModelMetadata",
		"ListModelPackageGroups",
		"ListModelPackages",
		"ListModelQualityJobDefinitions",
		"ListMonitoringAlertHistory",
		"ListMonitoringAlerts",
		"ListMonitoringExecutions",
		"ListMonitoringSchedules",
		"ListOptimizationJobs",
		"ListPartnerApps",
		// ListPipelineParametersForExecution — real implementation in handler_accuracy2.go
		"ListPipelineVersions",
		"ListProjects",
		"ListResourceCatalogs",
		"ListSpaces",
		"ListStageDevices",
		"ListStudioLifecycleConfigs",
		"ListSubscribedWorkteams",
		"ListTrainingJobsForHyperParameterTuningJob",
		"ListTrainingPlans",
		"ListTrialComponents",
		"ListUltraServersByReservedCapacity",
		"ListWorkforces",
		"ListWorkteams",
		"PutModelPackageGroupPolicy",
		"QueryLineage",
		"RegisterDevices",
		"RenderUiTemplate",
		"Search",
		"SearchTrainingPlanOfferings",
		"StartEdgeDeploymentStage",
		"StartInferenceExperiment",
		"StartMlflowTrackingServer",
		"StartMonitoringSchedule",
		"StartSession",
		"StopAutoMLJob",
		"StopCompilationJob",
		"StopEdgeDeploymentStage",
		"StopEdgePackagingJob",
		"StopInferenceExperiment",
		"StopInferenceRecommendationsJob",
		"StopLabelingJob",
		"StopMlflowTrackingServer",
		"StopMonitoringSchedule",
		"StopOptimizationJob",
		"UpdateAction",
		"UpdateAppImageConfig",
		"UpdateArtifact",
		"UpdateCluster",
		"UpdateClusterSchedulerConfig",
		"UpdateClusterSoftware",
		"UpdateCodeRepository",
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
		"UpdateMlflowTrackingServer",
		"UpdateModelCard",
		"UpdateModelPackage",
		"UpdateMonitoringAlert",
		"UpdateMonitoringSchedule",
		"UpdatePartnerApp",
		"UpdatePipelineExecution",
		"UpdatePipelineVersion",
		"UpdateProject",
		"UpdateSpace",
		"UpdateUserProfile",
		"UpdateWorkforce",
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

	case "CreateAppImageConfig":
		return mustMarshal(m{keyAppImageConfigArn: ""}), true

	case "CreateArtifact":
		return mustMarshal(m{keyArtifactArn: ""}), true

	case "CreateAutoMLJob", "CreateAutoMLJobV2":
		return mustMarshal(m{keyAutoMLJobArn: ""}), true

	case "CreateCluster":
		return mustMarshal(m{keyClusterArn: ""}), true

	case "CreateClusterSchedulerConfig":
		return mustMarshal(m{keyClusterSchedulerConfigArn: ""}), true

	case "CreateCodeRepository":
		return mustMarshal(m{keyCodeRepositoryArn: ""}), true

	case "CreateCompilationJob":
		return mustMarshal(m{keyCompilationJobArn: ""}), true

	case "CreateComputeQuota":
		return mustMarshal(m{keyComputeQuotaArn: ""}), true

	case "CreateContext":
		return mustMarshal(m{keyContextArn: ""}), true

	case "CreateDataQualityJobDefinition",
		"CreateModelBiasJobDefinition",
		"CreateModelExplainabilityJobDefinition",
		"CreateModelQualityJobDefinition":
		return mustMarshal(m{keyJobDefinitionArn: ""}), true

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

	case "CreateFlowDefinition":
		return mustMarshal(m{keyFlowDefinitionArn: ""}), true

	case "CreateHub":
		return mustMarshal(m{keyHubArn: ""}), true

	case "CreateHubContentPresignedUrls":
		return mustMarshal(m{keyAuthorizedURL: ""}), true

	case "CreateHubContentReference":
		return mustMarshal(m{keyHubArn: "", keyHubContentArn: ""}), true

	case "CreateHumanTaskUi":
		return mustMarshal(m{keyHumanTaskUIArn: ""}), true

	case "CreateImage":
		return mustMarshal(m{keyImageArn: ""}), true

	case "CreateImageVersion":
		return mustMarshal(m{keyImageVersionArn: ""}), true

	case "CreateInferenceComponent":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "CreateInferenceExperiment":
		return mustMarshal(m{keyInferenceExperimentArn: ""}), true

	case "CreateInferenceRecommendationsJob":
		return mustMarshal(m{"JobArn": ""}), true

	case "CreateLabelingJob":
		return mustMarshal(m{keyLabelingJobArn: ""}), true

	case "CreateMlflowApp", "CreatePartnerApp":
		return mustMarshal(m{keyGenericArn: ""}), true

	case "CreateMlflowTrackingServer":
		return mustMarshal(m{keyTrackingServerArn: ""}), true

	case "CreateModelCard":
		return mustMarshal(m{keyModelCardArn: ""}), true

	case "CreateModelCardExportJob":
		return mustMarshal(m{keyModelCardExportJobArn: ""}), true

	case "CreateModelPackage":
		return mustMarshal(m{keyModelPackageArn: ""}), true

	case "CreateModelPackageGroup":
		return mustMarshal(m{keyModelPackageGroupArn: ""}), true

	case "CreateMonitoringSchedule":
		return mustMarshal(m{keyMonitoringScheduleArn: ""}), true

	case "CreateOptimizationJob":
		return mustMarshal(m{keyOptimizationJobArn: ""}), true

	case "CreatePartnerAppPresignedUrl",
		"CreatePresignedDomainUrl",
		"CreatePresignedMlflowAppUrl",
		"CreatePresignedMlflowTrackingServerUrl":
		return mustMarshal(m{keyAuthorizedURL: ""}), true

	case "CreatePipeline":
		return mustMarshal(m{keyPipelineArn: ""}), true

	case "CreateProject":
		return mustMarshal(m{keyProjectArn: "", "ProjectId": ""}), true

	case "CreateSpace":
		return mustMarshal(m{keySpaceArn: ""}), true

	case "CreateStudioLifecycleConfig":
		return mustMarshal(m{keyStudioLifecycleConfigArn: ""}), true

	case "CreateTrainingPlan":
		return mustMarshal(m{keyTrainingPlanArn: ""}), true

	case "CreateTrial":
		return mustMarshal(m{keyTrialArn: ""}), true

	case "CreateTrialComponent":
		return mustMarshal(m{keyTrialComponentArn: ""}), true

	case "CreateUserProfile":
		return mustMarshal(m{keyUserProfileArn: ""}), true

	case "CreateWorkforce":
		return mustMarshal(m{keyWorkforceArn: ""}), true

	case "CreateWorkteam":
		return mustMarshal(m{keyWorkteamArn: ""}), true

	// -----------------------------------------------------------------------
	// Delete / stop / misc — return empty object
	// -----------------------------------------------------------------------
	case "DeleteAction",
		"DeleteAlgorithm",
		"DeleteApp",
		"DeleteAppImageConfig",
		"DeleteArtifact",
		"DeleteAssociation",
		"DeleteCluster",
		"DeleteClusterSchedulerConfig",
		"DeleteCodeRepository",
		"DeleteCompilationJob",
		"DeleteComputeQuota",
		"DeleteContext",
		"DeleteDataQualityJobDefinition",
		"DeleteDeviceFleet",
		"DeleteDomain",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteExperiment",
		"DeleteFeatureGroup",
		"DeleteFlowDefinition",
		"DeleteHub",
		"DeleteHubContent",
		"DeleteHubContentReference",
		"DeleteHumanTaskUi",
		"DeleteImage",
		"DeleteImageVersion",
		"DeleteInferenceComponent",
		"DeleteInferenceExperiment",
		"DeleteMlflowApp",
		"DeleteMlflowTrackingServer",
		"DeleteModelBiasJobDefinition",
		"DeleteModelCard",
		"DeleteModelExplainabilityJobDefinition",
		"DeleteModelPackage",
		"DeleteModelPackageGroup",
		"DeleteModelPackageGroupPolicy",
		"DeleteModelQualityJobDefinition",
		"DeleteMonitoringSchedule",
		"DeleteOptimizationJob",
		"DeletePartnerApp",
		"DeletePipeline",
		"DeleteProcessingJob",
		"DeleteProject",
		"DeleteSpace",
		"DeleteStudioLifecycleConfig",
		"DeleteTrial",
		"DeleteTrialComponent",
		"DeleteUserProfile",
		"DeleteWorkforce",
		"DeleteWorkteam",
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
		"StartMlflowTrackingServer",
		"StartMonitoringSchedule",
		"StartSession",
		"StopAutoMLJob",
		"StopCompilationJob",
		"StopEdgeDeploymentStage",
		"StopEdgePackagingJob",
		"StopInferenceExperiment",
		"StopInferenceRecommendationsJob",
		"StopLabelingJob",
		"StopMlflowTrackingServer",
		"StopMonitoringSchedule",
		"StopOptimizationJob",
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

	case "DescribeAppImageConfig":
		return mustMarshal(m{keyAppImageConfigArn: "", "AppImageConfigName": ""}), true

	case "DescribeArtifact":
		return mustMarshal(m{keyArtifactArn: "", "ArtifactType": ""}), true

	case "DescribeAutoMLJob", "DescribeAutoMLJobV2":
		return mustMarshal(m{
			keyAutoMLJobArn: "", "AutoMLJobName": "", "AutoMLJobStatus": statusCompleted,
		}), true

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

	case "DescribeCodeRepository":
		return mustMarshal(m{keyCodeRepositoryArn: "", "CodeRepositoryName": ""}), true

	case "DescribeCompilationJob":
		return mustMarshal(m{
			keyCompilationJobArn: "", "CompilationJobName": "", "CompilationJobStatus": statusCompletedUpper,
		}), true

	case "DescribeComputeQuota":
		return mustMarshal(m{keyComputeQuotaArn: "", keyStatus: statusCreated}), true

	case "DescribeContext":
		return mustMarshal(m{keyContextArn: "", "ContextType": ""}), true

	case "DescribeDataQualityJobDefinition",
		"DescribeModelBiasJobDefinition",
		"DescribeModelExplainabilityJobDefinition",
		"DescribeModelQualityJobDefinition":
		return mustMarshal(m{keyJobDefinitionArn: "", keyJobDefinitionName: ""}), true

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

	case "DescribeFlowDefinition":
		return mustMarshal(m{
			keyFlowDefinitionArn: "", "FlowDefinitionName": "", "FlowDefinitionStatus": statusActive,
		}), true

	case "DescribeHub":
		return mustMarshal(m{keyHubArn: "", "HubName": "", "HubStatus": statusInService}), true

	case "DescribeHubContent":
		return mustMarshal(m{
			keyHubContentArn: "", "HubContentName": "", "HubContentStatus": "Available",
		}), true

	case "DescribeHumanTaskUi":
		return mustMarshal(m{
			keyHumanTaskUIArn: "", "HumanTaskUiName": "", "HumanTaskUiStatus": statusActive,
		}), true

	case "DescribeImage":
		return mustMarshal(m{keyImageArn: "", "ImageName": "", "ImageStatus": "CREATED"}), true

	case "DescribeImageVersion":
		return mustMarshal(m{
			keyImageVersionArn: "", keyImageArn: "", "ImageVersionStatus": "CREATED", "Version": 0,
		}), true

	case "DescribeInferenceComponent":
		return mustMarshal(m{
			keyInferenceComponentArn: "", "InferenceComponentName": "", "InferenceComponentStatus": statusInService,
		}), true

	case "DescribeInferenceExperiment":
		return mustMarshal(m{keyGenericArn: "", "Name": "", keyStatus: "Running"}), true

	case "DescribeInferenceRecommendationsJob":
		return mustMarshal(m{"JobArn": "", "JobName": "", keyStatus: statusCompletedUpper}), true

	case "DescribeLabelingJob":
		return mustMarshal(m{
			keyLabelingJobArn: "", "LabelingJobName": "", "LabelingJobStatus": statusCompleted,
		}), true

	case "DescribeLineageGroup":
		return mustMarshal(m{keyLineageGroupArn: "", "LineageGroupName": ""}), true

	case "DescribeMlflowApp":
		return mustMarshal(m{keyGenericArn: "", keyStatus: statusInService}), true

	case "DescribeMlflowTrackingServer":
		return mustMarshal(m{
			keyTrackingServerArn: "", "TrackingServerName": "", "TrackingServerStatus": statusCreated,
		}), true

	case "DescribeModelCard":
		return mustMarshal(m{
			keyModelCardArn: "", "ModelCardName": "", keyModelCardStatus: "Draft", keyModelCardVersion: 1,
		}), true

	case "DescribeModelCardExportJob":
		return mustMarshal(m{
			keyModelCardExportJobArn: "", "ModelCardExportJobName": "", keyStatus: statusCompleted,
		}), true

	case "DescribeModelPackage":
		return mustMarshal(m{
			keyModelPackageArn:     "",
			"ModelPackageName":     "",
			keyModelPackageStatus:  statusCompleted,
			keyModelApprovalStatus: statusPendingManualApproval,
		}), true

	case "DescribeModelPackageGroup":
		return mustMarshal(m{
			keyModelPackageGroupArn: "", "ModelPackageGroupName": "", "ModelPackageGroupStatus": statusCompleted,
		}), true

	case "DescribeMonitoringSchedule":
		return mustMarshal(m{
			keyMonitoringScheduleArn:    "",
			keyMonitoringScheduleName:   "",
			keyMonitoringScheduleStatus: "Scheduled",
		}), true

	case "DescribeOptimizationJob":
		return mustMarshal(m{
			keyOptimizationJobArn: "", "OptimizationJobName": "", "OptimizationJobStatus": statusCompletedUpper,
		}), true

	case "DescribePartnerApp":
		return mustMarshal(m{keyGenericArn: "", keyStatus: "Available"}), true

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

	case "DescribeProject":
		return mustMarshal(m{
			keyProjectArn: "", "ProjectId": "", "ProjectName": "", "ProjectStatus": "CreateCompleted",
		}), true

	case "DescribeReservedCapacity":
		return mustMarshal(m{keyReservedCapacityArn: "", keyStatus: statusActive}), true

	case "DescribeSpace":
		return mustMarshal(m{keySpaceArn: "", "SpaceName": "", keyStatus: statusInService}), true

	case "DescribeStudioLifecycleConfig":
		return mustMarshal(m{
			keyStudioLifecycleConfigArn: "", "StudioLifecycleConfigName": "",
		}), true

	case "DescribeSubscribedWorkteam":
		return mustMarshal(m{"SubscribedWorkteam": m{keyWorkteamArn: ""}}), true

	case "DescribeTrainingPlan":
		return mustMarshal(m{keyTrainingPlanArn: "", keyStatus: statusActive}), true

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

	case "DescribeWorkforce":
		return mustMarshal(m{"Workforce": m{keyWorkforceArn: "", "WorkforceName": ""}}), true

	case "DescribeWorkteam":
		return mustMarshal(m{"Workteam": m{keyWorkteamArn: "", "WorkteamName": ""}}), true

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

	case "ListAppImageConfigs":
		return mustMarshal(m{"AppImageConfigs": []any{}}), true

	case "ListApps", "ListMlflowApps":
		return mustMarshal(m{"Apps": []any{}}), true

	case "ListArtifacts":
		return mustMarshal(m{"ArtifactSummaries": []any{}}), true

	case "ListAssociations":
		return mustMarshal(m{"AssociationSummaries": []any{}}), true

	case "ListAutoMLJobs":
		return mustMarshal(m{"AutoMLJobSummaries": []any{}}), true

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

	case "ListCodeRepositories":
		return mustMarshal(m{"CodeRepositorySummaryList": []any{}}), true

	case "ListCompilationJobs":
		return mustMarshal(m{"CompilationJobSummaries": []any{}}), true

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

	case "ListEdgePackagingJobs":
		return mustMarshal(m{"EdgePackagingJobSummaries": []any{}}), true

	case "ListExperiments":
		return mustMarshal(m{"ExperimentSummaries": []any{}}), true

	case "ListFeatureGroups":
		return mustMarshal(m{"FeatureGroupSummaries": []any{}}), true

	case "ListFlowDefinitions":
		return mustMarshal(m{"FlowDefinitionSummaries": []any{}}), true

	case "ListHubContentVersions", "ListHubContents":
		return mustMarshal(m{"HubContentSummaries": []any{}}), true

	case "ListHubs":
		return mustMarshal(m{"HubSummaries": []any{}}), true

	case "ListHumanTaskUis":
		return mustMarshal(m{"HumanTaskUiSummaries": []any{}}), true

	case "ListImageVersions":
		return mustMarshal(m{"ImageVersions": []any{}}), true

	case "ListImages":
		return mustMarshal(m{"Images": []any{}}), true

	case "ListInferenceComponents":
		return mustMarshal(m{"InferenceComponents": []any{}}), true

	case "ListInferenceExperiments":
		return mustMarshal(m{"InferenceExperiments": []any{}}), true

	case "ListInferenceRecommendationsJobSteps":
		return mustMarshal(m{"Steps": []any{}}), true

	case "ListInferenceRecommendationsJobs":
		return mustMarshal(m{"InferenceRecommendationsJobs": []any{}}), true

	case "ListLabelingJobs", "ListLabelingJobsForWorkteam":
		return mustMarshal(m{"LabelingJobSummaryList": []any{}}), true

	case "ListLineageGroups":
		return mustMarshal(m{"LineageGroupSummaries": []any{}}), true

	case "ListMlflowTrackingServers":
		return mustMarshal(m{"TrackingServerSummaries": []any{}}), true

	case "ListModelCardExportJobs":
		return mustMarshal(m{"ModelCardExportJobSummaries": []any{}}), true

	case "ListModelCardVersions":
		return mustMarshal(m{"ModelCardVersionSummaryList": []any{}}), true

	case "ListModelCards":
		return mustMarshal(m{"ModelCardSummaries": []any{}}), true

	case "ListModelMetadata":
		return mustMarshal(m{"ModelMetadataSummaries": []any{}}), true

	case "ListModelPackageGroups":
		return mustMarshal(m{"ModelPackageGroupSummaryList": []any{}}), true

	case "ListModelPackages":
		return mustMarshal(m{"ModelPackageSummaryList": []any{}}), true

	case "ListMonitoringAlertHistory":
		return mustMarshal(m{"MonitoringAlertHistory": []any{}}), true

	case "ListMonitoringAlerts":
		return mustMarshal(m{"MonitoringAlertSummaries": []any{}}), true

	case "ListMonitoringExecutions":
		return mustMarshal(m{"MonitoringExecutionSummaries": []any{}}), true

	case "ListMonitoringSchedules":
		return mustMarshal(m{"MonitoringScheduleSummaries": []any{}}), true

	case "ListOptimizationJobs":
		return mustMarshal(m{"OptimizationJobSummaries": []any{}}), true

	case "ListPartnerApps":
		return mustMarshal(m{"Summaries": []any{}}), true

	case "ListPipelineExecutions":
		return mustMarshal(m{"PipelineExecutionSummaries": []any{}}), true


	case "ListPipelineVersions":
		return mustMarshal(m{"PipelineVersionSummaries": []any{}}), true

	case "ListPipelines":
		return mustMarshal(m{"PipelineSummaries": []any{}}), true

	case "ListProjects":
		return mustMarshal(m{"ProjectSummaryList": []any{}}), true

	case "ListResourceCatalogs":
		return mustMarshal(m{"ResourceCatalogs": []any{}}), true

	case "ListSpaces":
		return mustMarshal(m{"Spaces": []any{}}), true

	case "ListStageDevices":
		return mustMarshal(m{"DeviceDeploymentSummaries": []any{}}), true

	case "ListStudioLifecycleConfigs":
		return mustMarshal(m{"StudioLifecycleConfigs": []any{}}), true

	case "ListSubscribedWorkteams":
		return mustMarshal(m{"SubscribedWorkteams": []any{}}), true

	case "ListTrainingJobsForHyperParameterTuningJob":
		return mustMarshal(m{keyTrainingJobSummaries: []any{}}), true

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

	case "ListWorkteams":
		return mustMarshal(m{"Workteams": []any{}}), true

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

	case "UpdateAppImageConfig":
		return mustMarshal(m{keyAppImageConfigArn: ""}), true

	case "UpdateArtifact":
		return mustMarshal(m{keyArtifactArn: ""}), true

	case "UpdateCluster":
		return mustMarshal(m{keyClusterArn: ""}), true

	case "UpdateClusterSchedulerConfig":
		return mustMarshal(m{keyClusterSchedulerConfigArn: ""}), true

	case "UpdateCodeRepository":
		return mustMarshal(m{keyCodeRepositoryArn: ""}), true

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

	case "UpdateImage":
		return mustMarshal(m{keyImageArn: ""}), true

	case "UpdateImageVersion":
		return mustMarshal(m{keyImageVersionArn: ""}), true

	case "UpdateInferenceComponent":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "UpdateInferenceComponentRuntimeConfig":
		return mustMarshal(m{keyInferenceComponentArn: ""}), true

	case "UpdateInferenceExperiment":
		return mustMarshal(m{keyInferenceExperimentArn: ""}), true

	case "UpdateMlflowApp", "UpdatePartnerApp":
		return mustMarshal(m{keyGenericArn: ""}), true

	case "UpdateMlflowTrackingServer":
		return mustMarshal(m{keyTrackingServerArn: ""}), true

	case "UpdateModelCard":
		return mustMarshal(m{keyModelCardArn: ""}), true

	case "UpdateModelPackage":
		return mustMarshal(m{keyModelPackageArn: ""}), true

	case "UpdateMonitoringAlert":
		return mustMarshal(m{keyMonitoringScheduleArn: "", keyMonitoringAlertName: ""}), true

	case "UpdateMonitoringSchedule":
		return mustMarshal(m{keyMonitoringScheduleArn: ""}), true

	case "UpdatePipeline", "UpdatePipelineVersion":
		return mustMarshal(m{keyPipelineArn: ""}), true

	case "UpdatePipelineExecution":
		return mustMarshal(m{keyPipelineExecutionArn: ""}), true

	case "UpdateProject":
		return mustMarshal(m{keyProjectArn: ""}), true

	case "UpdateSpace":
		return mustMarshal(m{keySpaceArn: ""}), true

	case "UpdateUserProfile":
		return mustMarshal(m{keyUserProfileArn: ""}), true

	case "UpdateWorkforce":
		return mustMarshal(m{"Workforce": m{keyWorkforceArn: ""}}), true

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
