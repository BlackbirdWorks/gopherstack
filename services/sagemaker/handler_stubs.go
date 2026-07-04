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
	keyEdgeDeploymentPlanName          = "EdgeDeploymentPlanName"
	keyDeviceName                      = "DeviceName"
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
	keyAuthorizedURL                   = "AuthorizedUrl"
	keyGenericArn                      = "Arn"
	keyGenericName                     = "Name"
	keyAppArn                          = "AppArn"
	keyFeatureGroupStatus              = "FeatureGroupStatus"
	keyMonitoringScheduleName          = "MonitoringScheduleName"
	keyMonitoringScheduleStatus        = "MonitoringScheduleStatus"
	keyPipelineExecutionStatus         = "PipelineExecutionStatus"
	keyModelCardStatus                 = "ModelCardStatus"
	keyModelCardVersion                = "ModelCardVersion"
	keyModelApprovalStatus             = "ModelApprovalStatus"
	keyModelPackageStatus              = "ModelPackageStatus"
	keyFeatureGroupName                = "FeatureGroupName"
	keyFeatureDefinitions              = "FeatureDefinitions"
	keyRecordIdentifierFeatureName     = "RecordIdentifierFeatureName"
	keyEventTimeFeatureName            = "EventTimeFeatureName"
	keyMonitoringAlertName             = "MonitoringAlertName"
	keyClusterSchedulerConfigSummaries = "ClusterSchedulerConfigSummaries"
	keyTrainingJobSummaries            = "TrainingJobSummaries"
	keyDeviceFleetName                 = "DeviceFleetName"
	keyStatus                          = "Status"
	statusInService                    = clusterStatusInService
	statusCreated                      = "Created"
	statusActive                       = "Active"
	statusPendingManualApproval        = "PendingManualApproval"
)

// stubOpsSupported returns the list of stub-implemented operations.
func stubOpsSupported() []string {
	return []string{
		"CreatePresignedDomainUrl",
		"DeleteModelPackageGroupPolicy",
		"DeleteProcessingJob",
		"DescribeFeatureMetadata",
		"DescribePipelineDefinitionForExecution",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"GetModelPackageGroupPolicy",
		"GetSagemakerServicecatalogPortfolioStatus",
		"ListAliases",
		// ListPipelineParametersForExecution — real implementation in handler_accuracy2.go
		"ListPipelineVersions",
		"ListResourceCatalogs",
		"ListTrialComponents",
		"PutModelPackageGroupPolicy",
		"RenderUiTemplate",
		"StartInferenceExperiment",
		"StartSession",
		"UpdateFeatureMetadata",
		"UpdateHubContent",
		"UpdateHubContentReference",
		"UpdateInferenceExperiment",
		"UpdatePipelineExecution",
		"UpdatePipelineVersion",
		"UpdateProject",
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

	case "CreateExperiment":
		return mustMarshal(m{keyExperimentArn: ""}), true

	case "CreateFeatureGroup":
		return mustMarshal(m{keyFeatureGroupArn: ""}), true

	case "CreatePresignedDomainUrl":
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
	case "DeleteApp",
		"DeleteDomain",
		"DeleteExperiment",
		"DeleteFeatureGroup",
		"DeleteModelPackageGroupPolicy",
		"DeletePipeline",
		"DeleteProcessingJob",
		"DeleteTrial",
		"DeleteTrialComponent",
		"DeleteUserProfile",
		"DisableSagemakerServicecatalogPortfolio",
		"DisassociateTrialComponent",
		"EnableSagemakerServicecatalogPortfolio",
		"PutModelPackageGroupPolicy",
		"RenderUiTemplate",
		"StartSession",
		"UpdateFeatureMetadata":
		return mustMarshal(m{}), true

	// -----------------------------------------------------------------------
	// Describe ops
	// -----------------------------------------------------------------------
	case "DescribeApp":
		return mustMarshal(m{
			keyAppArn: "", "AppType": "", "AppName": "", keyStatus: statusInService,
		}), true

	case "DescribeDomain":
		return mustMarshal(m{keyDomainArn: "", keyDomainID: "", keyStatus: statusInService}), true

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
	case "GetModelPackageGroupPolicy":
		return mustMarshal(m{"ResourcePolicy": ""}), true

	case "GetSagemakerServicecatalogPortfolioStatus":
		return mustMarshal(m{keyStatus: "Disabled"}), true

	// -----------------------------------------------------------------------
	// List ops — all return an empty array under a domain-appropriate key
	// -----------------------------------------------------------------------
	case "ListAliases":
		return mustMarshal(m{"SageMakerImageVersionAliases": []any{}}), true

	case "ListApps":
		return mustMarshal(m{"Apps": []any{}}), true

	case "ListDomains":
		return mustMarshal(m{"Domains": []any{}}), true

	case "ListExperiments":
		return mustMarshal(m{"ExperimentSummaries": []any{}}), true

	case "ListFeatureGroups":
		return mustMarshal(m{"FeatureGroupSummaries": []any{}}), true

	case "ListPipelineExecutions":
		return mustMarshal(m{"PipelineExecutionSummaries": []any{}}), true

	case "ListPipelineVersions":
		return mustMarshal(m{"PipelineVersionSummaries": []any{}}), true

	case "ListPipelines":
		return mustMarshal(m{"PipelineSummaries": []any{}}), true

	case "ListResourceCatalogs":
		return mustMarshal(m{"ResourceCatalogs": []any{}}), true

	case "ListTrialComponents":
		return mustMarshal(m{"TrialComponentSummaries": []any{}}), true

	case "ListTrials":
		return mustMarshal(m{"TrialSummaries": []any{}}), true

	case "ListUserProfiles":
		return mustMarshal(m{"UserProfiles": []any{}}), true

	// -----------------------------------------------------------------------
	// Action / query / pipeline ops
	// -----------------------------------------------------------------------
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

	case "UpdatePipeline", "UpdatePipelineVersion":
		return mustMarshal(m{keyPipelineArn: ""}), true

	case "UpdatePipelineExecution":
		return mustMarshal(m{keyPipelineExecutionArn: ""}), true

	case "UpdateProject":
		return mustMarshal(m{keyProjectArn: ""}), true
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
func (h *Handler) dispatchStubOps(_ context.Context, op string, _ []byte) ([]byte, bool, error) {
	b, ok := stubResponseFor(op)
	if !ok {
		return nil, false, nil
	}

	return b, true, nil
}
