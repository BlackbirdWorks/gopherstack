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
	keyAutoMLJobArn                    = "AutoMLJobArn"
	keyClusterSchedulerConfigArn       = "ClusterSchedulerConfigArn"
	keyCodeRepositoryArn               = "CodeRepositoryArn"
	keyCompilationJobArn               = "CompilationJobArn"
	keyComputeQuotaArn                 = "ComputeQuotaArn"
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
func stubOpsSupported() []string {
	return []string{
		"CreateEdgeDeploymentPlan",
		"CreateEdgeDeploymentStage",
		"CreateLabelingJob",
		"CreateMlflowApp",
		"CreateModelCardExportJob",
		"CreatePartnerAppPresignedUrl",
		"CreatePresignedDomainUrl",
		"CreatePresignedMlflowAppUrl",
		"CreatePresignedMlflowTrackingServerUrl",
		"DeleteAlgorithm",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteMlflowApp",
		"DeleteModelPackageGroupPolicy",
		"DeleteProcessingJob",
		"DeleteWorkforce",
		"DescribeAlgorithm",
		"DescribeEdgeDeploymentPlan",
		"DescribeFeatureMetadata",
		"DescribeLabelingJob",
		"DescribeMlflowApp",
		"DescribeModelCardExportJob",
		"DescribePipelineDefinitionForExecution",
		"DescribeReservedCapacity",
		"DescribeSubscribedWorkteam",
		"DescribeTrainingPlanExtensionHistory",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"ExtendTrainingPlan",
		"GetDeviceFleetReport",
		"GetModelPackageGroupPolicy",
		"GetSagemakerServicecatalogPortfolioStatus",
		"GetScalingConfigurationRecommendation",
		"GetSearchSuggestions",
		"ListAlgorithms",
		"ListAliases",
		"ListCandidatesForAutoMLJob",
		"ListEdgeDeploymentPlans",
		"ListLabelingJobs",
		"ListLabelingJobsForWorkteam",
		"ListMlflowApps",
		"ListModelMetadata",
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
		"RenderUiTemplate",
		"Search",
		"SearchTrainingPlanOfferings",
		"StartEdgeDeploymentStage",
		"StartInferenceExperiment",
		"StartSession",
		"StopEdgeDeploymentStage",
		"StopLabelingJob",
		"UpdateDevices",
		"UpdateFeatureMetadata",
		"UpdateHubContent",
		"UpdateHubContentReference",
		"UpdateImage",
		"UpdateImageVersion",
		"UpdateInferenceExperiment",
		"UpdateMlflowApp",
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
	case "DeleteAlgorithm",
		"DeleteApp",
		"DeleteDomain",
		"DeleteEdgeDeploymentPlan",
		"DeleteEdgeDeploymentStage",
		"DeleteExperiment",
		"DeleteFeatureGroup",
		"DeleteMlflowApp",
		"DeleteModelPackageGroupPolicy",
		"DeletePipeline",
		"DeleteProcessingJob",
		"DeleteWorkforce",
		"DeleteTrial",
		"DeleteTrialComponent",
		"DeleteUserProfile",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"ExtendTrainingPlan",
		"PutModelPackageGroupPolicy",
		"RenderUiTemplate",
		"StartEdgeDeploymentStage",
		"StartSession",
		"StopEdgeDeploymentStage",
		"StopLabelingJob",
		"UpdateDevices",
		"UpdateFeatureMetadata":
		return mustMarshal(m{}), true

	// -----------------------------------------------------------------------
	// Describe ops
	// -----------------------------------------------------------------------
	case "DescribeAlgorithm":
		return mustMarshal(m{
			"AlgorithmName": "", "AlgorithmArn": "", "AlgorithmStatus": statusCompleted,
		}), true

	case "DescribeApp":
		return mustMarshal(m{
			keyAppArn: "", "AppType": "", "AppName": "", keyStatus: statusInService,
		}), true

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

	case "DescribeLabelingJob":
		return mustMarshal(m{
			keyLabelingJobArn: "", "LabelingJobName": "", "LabelingJobStatus": statusCompleted,
		}), true

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
	case "ListAlgorithms":
		return mustMarshal(m{"AlgorithmSummaryList": []any{}}), true

	case "ListAliases":
		return mustMarshal(m{"SageMakerImageVersionAliases": []any{}}), true

	case "ListApps", "ListMlflowApps":
		return mustMarshal(m{"Apps": []any{}}), true

	case "ListCandidatesForAutoMLJob":
		return mustMarshal(m{"Candidates": []any{}}), true

	case "ListDomains":
		return mustMarshal(m{"Domains": []any{}}), true

	case "ListEdgeDeploymentPlans":
		return mustMarshal(m{"EdgeDeploymentPlanSummaries": []any{}}), true

	case "ListExperiments":
		return mustMarshal(m{"ExperimentSummaries": []any{}}), true

	case "ListFeatureGroups":
		return mustMarshal(m{"FeatureGroupSummaries": []any{}}), true

	case "ListLabelingJobs", "ListLabelingJobsForWorkteam":
		return mustMarshal(m{"LabelingJobSummaryList": []any{}}), true

	case "ListModelMetadata":
		return mustMarshal(m{"ModelMetadataSummaries": []any{}}), true

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
	case "UpdateDomain":
		return mustMarshal(m{keyDomainArn: ""}), true

	case "UpdateHubContent", "UpdateHubContentReference":
		return mustMarshal(m{keyHubArn: "", keyHubContentArn: ""}), true

	case "UpdateInferenceExperiment":
		return mustMarshal(m{keyInferenceExperimentArn: ""}), true

	case "UpdateMlflowApp", "UpdatePartnerApp":
		return mustMarshal(m{keyGenericArn: ""}), true

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
