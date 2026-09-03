package sagemaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField = "__type"
)

const (
	errValidationException = "ValidationException"
	keyMessageField        = "message"
	keyClusterArn          = "ClusterArn"
)

const (
	sagemakerService       = "sagemaker"
	sagemakerTargetPrefix  = "SageMaker."
	sagemakerMatchPriority = service.PriorityHeaderExact
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS SageMaker JSON API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new SageMaker handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "SageMaker" }

// Reset clears all resources from the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Shutdown implements service.Shutdowner. It cancels pending lifecycle-transition
// goroutines (training/processing/transform jobs, endpoints, notebooks, pipeline
// executions advancing through statuses) and waits for in-flight ones to drain,
// bounded by ctx, so no goroutines leak on server shutdown.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.Backend == nil {
		return
	}

	h.Backend.Shutdown(ctx)
}

// Ensure Handler implements service.Shutdowner at compile time.
var _ service.Shutdowner = (*Handler)(nil)

// coreOpsSupported returns the operations handled by dispatchCoreOps and
// dispatchNewOps (the original, non-batch-swept operation set). Split out of
// GetSupportedOperations to keep that function short: it only needs to
// aggregate every family's op list, not enumerate this one inline. The list
// itself is further split by verb (Create/Delete-Describe/List-Update) to
// keep each helper under the function-length limit.
func coreOpsSupported() []string {
	ops := make([]string, 0, len(coreCreateOpsSupported())+
		len(coreDeleteDescribeOpsSupported())+len(coreListUpdateOpsSupported()))
	ops = append(ops, coreCreateOpsSupported()...)
	ops = append(ops, coreDeleteDescribeOpsSupported()...)
	ops = append(ops, coreListUpdateOpsSupported()...)

	return ops
}

// coreCreateOpsSupported returns the Create*/batch/association-style core operations.
func coreCreateOpsSupported() []string {
	return []string{
		"AddAssociation",
		"AddTags",
		"AssociateTrialComponent",
		"AttachClusterNodeVolume",
		"BatchAddClusterNodes",
		"BatchDeleteClusterNodes",
		"BatchDescribeModelPackage",
		"BatchRebootClusterNodes",
		"BatchReplaceClusterNodes",
		"CreateAction",
		"CreateAlgorithm",
		"DescribeAlgorithm",
		"DeleteAlgorithm",
		"ListAlgorithms",
		opCreateApp,
		opCreateDomain,
		"CreateEndpoint",
		"CreateEndpointConfig",
		opCreateExperiment,
		opCreateFeatureGroup,
		"CreateHyperParameterTuningJob",
		"CreateModel",
		"CreateNotebookInstance",
		"CreateNotebookInstanceLifecycleConfig",
		opCreatePipeline,
		"CreatePresignedNotebookInstanceUrl",
		"CreateProcessingJob",
		"CreateTrainingJob",
		"CreateTransformJob",
		opCreateTrial,
		opCreateTrialComponent,
		opCreateUserProfile,
	}
}

// coreDeleteDescribeOpsSupported returns the Delete*/Describe* core operations.
func coreDeleteDescribeOpsSupported() []string {
	return []string{
		opDeleteApp,
		opDeleteDomain,
		"DeleteEndpoint",
		"DeleteEndpointConfig",
		opDeleteExperiment,
		opDeleteFeatureGroup,
		"DeleteHyperParameterTuningJob",
		"DeleteModel",
		"DeleteNotebookInstance",
		"DeleteNotebookInstanceLifecycleConfig",
		opDeletePipeline,
		opDeleteTrial,
		opDeleteTrialComponent,
		opDeleteUserProfile,
		"DeleteTags",
		"DeleteTrainingJob",
		opDescribeApp,
		opDescribeDomain,
		"DescribeEndpoint",
		"DescribeEndpointConfig",
		opDescribeExperiment,
		opDescribeFeatureGroup,
		"DescribeHyperParameterTuningJob",
		"DescribeModel",
		"DescribeNotebookInstance",
		"DescribeNotebookInstanceLifecycleConfig",
		"DescribeProcessingJob",
		opDescribePipeline,
		opDescribePipelineExec,
		opDescribeTrial,
		opDescribeTrialComponent,
		opDescribeUserProfile,
		"DescribeTrainingJob",
		"DescribeTransformJob",
	}
}

// coreListUpdateOpsSupported returns the List*/Start*/Stop*/Update* core operations.
func coreListUpdateOpsSupported() []string {
	return []string{
		"DisassociateTrialComponent",
		"ListTrialComponents",
		opListApps,
		opListDomains,
		"ListEndpointConfigs",
		"ListEndpoints",
		opListExperiments,
		opListFeatureGroups,
		"ListHyperParameterTuningJobs",
		"ListModels",
		"ListNotebookInstances",
		"ListNotebookInstanceLifecycleConfigs",
		opListPipelineExecutions,
		"ListPipelineExecutionSteps",
		opListPipelineParametersForExec,
		opListPipelines,
		"ListPipelineVersions",
		"DescribePipelineDefinitionForExecution",
		"UpdatePipelineVersion",
		"UpdatePipelineExecution",
		"ListProcessingJobs",
		"DeleteProcessingJob",
		opListTrials,
		opListUserProfiles,
		"ListTags",
		"ListTrainingJobs",
		"ListTransformJobs",
		"StartNotebookInstance",
		"RetryPipelineExecution",
		"SendPipelineExecutionStepFailure",
		"SendPipelineExecutionStepSuccess",
		opStartPipelineExecution,
		"StopHyperParameterTuningJob",
		"StopNotebookInstance",
		"StopPipelineExecution",
		"StopProcessingJob",
		"StopTrainingJob",
		"StopTransformJob",
		opUpdateDomain,
		"UpdateEndpoint",
		"UpdateEndpointWeightsAndCapacities",
		"UpdateExperiment",
		"UpdateFeatureGroup",
		"UpdateNotebookInstanceLifecycleConfig",
		opUpdatePipeline,
		"UpdateNotebookInstance",
		"UpdateTrainingJob",
		"UpdateTrial",
		"UpdateTrialComponent",
	}
}

// GetSupportedOperations returns the list of supported SageMaker operations.
func (h *Handler) GetSupportedOperations() []string {
	core := coreOpsSupported()
	batch2 := batch2OpsSupported()
	batch3 := batch3SupportedOperations()
	accuracy3 := accuracy3OpsSupported()
	accuracy4 := accuracy4OpsSupported()
	edgeDeployment := edgeDeploymentOpsSupported()
	cluster := clusterOpsSupported()
	lineage := lineageOpsSupported()
	hub := hubOpsSupported()
	labeling := labelingOpsSupported()
	trainingPlanExt := trainingPlanExtOpsSupported()
	automlSearchExt := automlSearchExtOpsSupported()
	modelCardExport := modelCardExportOpsSupported()
	servicecatalog := servicecatalogOpsSupported()
	featureMetadata := featureMetadataOpsSupported()
	presignedSession := presignedSessionOpsSupported()
	aiAndGenericJob := aiAndGenericJobOpsSupported()

	total := len(core) + len(batch2) + len(batch3) + len(accuracy3)
	total += len(accuracy4) + len(edgeDeployment) + len(cluster) + len(lineage) + len(hub)
	total += len(labeling) + len(trainingPlanExt) + len(automlSearchExt) + len(modelCardExport)
	total += len(servicecatalog) + len(featureMetadata) + len(presignedSession) + len(aiAndGenericJob)
	combined := make([]string, 0, total)
	combined = append(combined, core...)
	combined = append(combined, batch2...)
	combined = append(combined, batch3...)
	combined = append(combined, accuracy3...)
	combined = append(combined, accuracy4...)
	combined = append(combined, edgeDeployment...)
	combined = append(combined, cluster...)
	combined = append(combined, lineage...)
	combined = append(combined, hub...)
	combined = append(combined, labeling...)
	combined = append(combined, trainingPlanExt...)
	combined = append(combined, automlSearchExt...)
	combined = append(combined, modelCardExport...)
	combined = append(combined, servicecatalog...)
	combined = append(combined, featureMetadata...)
	combined = append(combined, presignedSession...)
	combined = append(combined, aiAndGenericJob...)

	return combined
}

// batch2OpsSupported returns the 50 real stateful operations implemented in batch 2.
func batch2OpsSupported() []string {
	return []string{
		// ModelPackage CRUD
		"CreateModelPackage",
		"DescribeModelPackage",
		"DeleteModelPackage",
		"ListModelPackages",
		// ModelPackageGroup CRUD
		"CreateModelPackageGroup",
		"DescribeModelPackageGroup",
		"DeleteModelPackageGroup",
		"ListModelPackageGroups",
		"GetModelPackageGroupPolicy",
		"PutModelPackageGroupPolicy",
		"DeleteModelPackageGroupPolicy",
		// AutoMLJob
		"CreateAutoMLJob",
		"CreateAutoMLJobV2",
		"DescribeAutoMLJob",
		"DescribeAutoMLJobV2",
		"StopAutoMLJob",
		"ListAutoMLJobs",
		// CodeRepository
		"CreateCodeRepository",
		"DescribeCodeRepository",
		"UpdateCodeRepository",
		"DeleteCodeRepository",
		"ListCodeRepositories",
		// Project
		"CreateProject",
		"DescribeProject",
		"DeleteProject",
		"ListProjects",
		// Space
		"CreateSpace",
		"DescribeSpace",
		"DeleteSpace",
		"ListSpaces",
		// Image
		"CreateImage",
		"DescribeImage",
		"DeleteImage",
		"ListImages",
		"UpdateImage",
		// ImageVersion
		"CreateImageVersion",
		"DescribeImageVersion",
		"DeleteImageVersion",
		"ListImageVersions",
		"UpdateImageVersion",
		// CompilationJob
		"CreateCompilationJob",
		"DescribeCompilationJob",
		"DeleteCompilationJob",
		"StopCompilationJob",
		"ListCompilationJobs",
		// MonitoringSchedule
		"CreateMonitoringSchedule",
		"DescribeMonitoringSchedule",
		"DeleteMonitoringSchedule",
		opStopMonitoringSchedule,
		"StartMonitoringSchedule",
		"UpdateMonitoringSchedule",
		"ListMonitoringSchedules",
		// Workteam
		"CreateWorkteam",
		"DescribeWorkteam",
		"DeleteWorkteam",
		"ListWorkteams",
		"UpdateWorkteam",
		// Image alias listing / Project update (moved from the former
		// miscDestubOpsSupported list; these are Image and Project family ops).
		"ListAliases",
		"UpdateProject",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return sagemakerService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches SageMaker API requests.
// SageMaker requests carry an X-Amz-Target header beginning with "SageMaker.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, sagemakerTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return sagemakerMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, sagemakerTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for SageMaker requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "sagemaker: failed to read request body", "error", err)

			return writeInternalServerError(c)
		}

		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchCoreOps(ctx, op, body); ok {
		return result, err
	}

	return h.dispatchNewOps(ctx, op, body)
}

func (h *Handler) dispatchCoreOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateModel":
		r, err := h.handleCreateModel(ctx, body)

		return r, true, err
	case "DescribeModel":
		r, err := h.handleDescribeModel(ctx, body)

		return r, true, err
	case "ListModels":
		r, err := h.handleListModels(ctx, body)

		return r, true, err
	case "DeleteModel":
		return nil, true, h.handleDeleteModel(ctx, body)
	case "CreateEndpointConfig":
		r, err := h.handleCreateEndpointConfig(ctx, body)

		return r, true, err
	case "DescribeEndpointConfig":
		r, err := h.handleDescribeEndpointConfig(ctx, body)

		return r, true, err
	case "ListEndpointConfigs":
		r, err := h.handleListEndpointConfigs(ctx, body)

		return r, true, err
	case "DeleteEndpointConfig":
		return nil, true, h.handleDeleteEndpointConfig(ctx, body)
	case "AddTags":
		r, err := h.handleAddTags(ctx, body)

		return r, true, err
	case "ListTags":
		r, err := h.handleListTags(ctx, body)

		return r, true, err
	case "DeleteTags":
		return nil, true, h.handleDeleteTags(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchNewOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	if r, ok, err := h.dispatchLineageAndBatchOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchLineageOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchEndpointOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchTrainingJobOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchNotebookOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchHPTuningJobOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchStatefulOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchBatch2Ops(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchBatch3Ops(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchAccuracy3Ops(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchAccuracy4AndEdgeOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchClusterOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchHubAndLabelingOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchFamilyExtAndStubOps(ctx, op, body); ok {
		return r, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

// dispatchFamilyExtAndStubOps groups the Training Plan / Reserved Capacity,
// AutoML candidates / Search, ModelCard export job, and remaining
// miscellaneous de-stubbed operation dispatch tables so dispatchNewOps only
// needs a single branch for all of them.
func (h *Handler) dispatchFamilyExtAndStubOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchTrainingPlanExtOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchAutoMLSearchExtOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchModelCardExportOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchServicecatalogOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchFeatureMetadataOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchPresignedSessionOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchAIAndGenericJobOps(ctx, op, body)
}

// dispatchAIAndGenericJobOps groups the AIBenchmarkJob/AIRecommendationJob/
// AIWorkloadConfig/generic-Job dispatch tables (all added by the SDK bump
// that introduced CreateJob et al.) so dispatchFamilyExtAndStubOps only
// needs a single branch for all four.
func (h *Handler) dispatchAIAndGenericJobOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchAIBenchmarkJobOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchAIRecommendationJobOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchAIWorkloadConfigOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchJobOps(ctx, op, body)
}

func (h *Handler) dispatchLineageAndBatchOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "AddAssociation":
		r, err := h.handleAddAssociation(ctx, body)

		return r, true, err
	case "AssociateTrialComponent":
		r, err := h.handleAssociateTrialComponent(ctx, body)

		return r, true, err
	case "AttachClusterNodeVolume":
		r, err := h.handleAttachClusterNodeVolume(ctx, body)

		return r, true, err
	case "BatchAddClusterNodes":
		r, err := h.handleBatchAddClusterNodes(ctx, body)

		return r, true, err
	case "BatchDeleteClusterNodes":
		r, err := h.handleBatchDeleteClusterNodes(ctx, body)

		return r, true, err
	case "BatchDescribeModelPackage":
		r, err := h.handleBatchDescribeModelPackage(ctx, body)

		return r, true, err
	case "BatchRebootClusterNodes":
		r, err := h.handleBatchRebootClusterNodes(ctx, body)

		return r, true, err
	case "BatchReplaceClusterNodes":
		r, err := h.handleBatchReplaceClusterNodes(ctx, body)

		return r, true, err
	case "CreateAction":
		r, err := h.handleCreateAction(ctx, body)

		return r, true, err
	case "CreateAlgorithm":
		r, err := h.handleCreateAlgorithm(ctx, body)

		return r, true, err
	case "DescribeAlgorithm":
		r, err := h.handleDescribeAlgorithm(ctx, body)

		return r, true, err
	case "DeleteAlgorithm":
		return nil, true, h.handleDeleteAlgorithm(ctx, body)
	case "ListAlgorithms":
		r, err := h.handleListAlgorithms(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchEndpointOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateEndpoint":
		r, err := h.handleCreateEndpointFSM(ctx, body)

		return r, true, err
	case "DescribeEndpoint":
		r, err := h.handleDescribeEndpoint(ctx, body)

		return r, true, err
	case "ListEndpoints":
		r, err := h.handleListEndpoints(ctx, body)

		return r, true, err
	case "DeleteEndpoint":
		return nil, true, h.handleDeleteEndpoint(ctx, body)
	case "UpdateEndpoint":
		r, err := h.handleUpdateEndpointFSM(ctx, body)

		return r, true, err
	case "UpdateEndpointWeightsAndCapacities":
		r, err := h.handleUpdateEndpointWeightsAndCapacitiesFull(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchTrainingJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	r, handled, err := h.dispatchTrainingOps(ctx, op, body)
	if handled {
		return r, true, err
	}
	r, handled, err = h.dispatchProcessingOps(ctx, op, body)
	if handled {
		return r, true, err
	}
	r, handled, err = h.dispatchPipelineOps(ctx, op, body)
	if handled {
		return r, true, err
	}

	return h.dispatchTransformJobOps(ctx, op, body)
}

func (h *Handler) dispatchTransformJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateTransformJob":
		r, err := h.handleCreateTransformJob(ctx, body)

		return r, true, err
	case "DescribeTransformJob":
		r, err := h.handleDescribeTransformJob(ctx, body)

		return r, true, err
	case "ListTransformJobs":
		r, err := h.handleListTransformJobs(ctx, body)

		return r, true, err
	case "StopTransformJob":
		return nil, true, h.handleStopTransformJob(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchTrainingOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateTrainingJob":
		r, err := h.handleCreateTrainingJobFull(ctx, body)

		return r, true, err
	case "DescribeTrainingJob":
		r, err := h.handleDescribeTrainingJobFull(ctx, body)

		return r, true, err
	case "ListTrainingJobs":
		r, err := h.handleListTrainingJobsFiltered(ctx, body)

		return r, true, err
	case "StopTrainingJob":
		return nil, true, h.handleStopTrainingJobFSM(ctx, body)
	case "DeleteTrainingJob":
		return nil, true, h.handleDeleteTrainingJob(ctx, body)
	case "UpdateTrainingJob":
		r, err := h.handleUpdateTrainingJob(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchProcessingOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateProcessingJob":
		r, err := h.handleCreateProcessingJob(ctx, body)

		return r, true, err
	case "DescribeProcessingJob":
		r, err := h.handleDescribeProcessingJob(ctx, body)

		return r, true, err
	case "StopProcessingJob":
		return nil, true, h.handleStopProcessingJob(ctx, body)
	case "DeleteProcessingJob":
		return nil, true, h.handleDeleteProcessingJob(ctx, body)
	case "ListProcessingJobs":
		r, err := h.handleListProcessingJobs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchPipelineOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "RetryPipelineExecution":
		r, err := h.handleRetryPipelineExecution(ctx, body)

		return r, true, err
	case "StopPipelineExecution":
		r, err := h.handleStopPipelineExecution(ctx, body)

		return r, true, err
	case "SendPipelineExecutionStepSuccess":
		r, err := h.handleSendPipelineExecutionStepSuccess(ctx, body)

		return r, true, err
	case "SendPipelineExecutionStepFailure":
		r, err := h.handleSendPipelineExecutionStepFailure(ctx, body)

		return r, true, err
	case "ListPipelineExecutionSteps":
		r, err := h.handleListPipelineExecutionSteps(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchNotebookOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateNotebookInstance":
		r, err := h.handleCreateNotebookInstanceFull(ctx, body)

		return r, true, err
	case "DescribeNotebookInstance":
		r, err := h.handleDescribeNotebookInstanceFull(ctx, body)

		return r, true, err
	case "ListNotebookInstances":
		r, err := h.handleListNotebookInstances(ctx, body)

		return r, true, err
	case "DeleteNotebookInstance":
		return nil, true, h.handleDeleteNotebookInstance(ctx, body)
	case "StartNotebookInstance":
		return nil, true, h.handleStartNotebookInstance(ctx, body)
	case "StopNotebookInstance":
		return nil, true, h.handleStopNotebookInstance(ctx, body)
	case "UpdateNotebookInstance":
		return nil, true, h.handleUpdateNotebookInstanceFull(ctx, body)
	case "CreatePresignedNotebookInstanceUrl":
		r, err := h.handleCreatePresignedNotebookInstanceURL(ctx, body)

		return r, true, err
	case "CreateNotebookInstanceLifecycleConfig":
		r, err := h.handleCreateNotebookInstanceLifecycleConfig(ctx, body)

		return r, true, err
	case "DescribeNotebookInstanceLifecycleConfig":
		r, err := h.handleDescribeNotebookInstanceLifecycleConfig(ctx, body)

		return r, true, err
	case "UpdateNotebookInstanceLifecycleConfig":
		r, err := h.handleUpdateNotebookInstanceLifecycleConfig(ctx, body)

		return r, true, err
	case "DeleteNotebookInstanceLifecycleConfig":
		return nil, true, h.handleDeleteNotebookInstanceLifecycleConfig(ctx, body)
	case "ListNotebookInstanceLifecycleConfigs":
		r, err := h.handleListNotebookInstanceLifecycleConfigs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchHPTuningJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateHyperParameterTuningJob":
		r, err := h.handleCreateHyperParameterTuningJob(ctx, body)

		return r, true, err
	case "DescribeHyperParameterTuningJob":
		r, err := h.handleDescribeHyperParameterTuningJob(ctx, body)

		return r, true, err
	case "ListHyperParameterTuningJobs":
		r, err := h.handleListHyperParameterTuningJobs(ctx, body)

		return r, true, err
	case "StopHyperParameterTuningJob":
		return nil, true, h.handleStopHyperParameterTuningJob(ctx, body)
	case "DeleteHyperParameterTuningJob":
		return nil, true, h.handleDeleteHyperParameterTuningJob(ctx, body)
	}

	return nil, false, nil
}

// writeInternalServerError renders a ReadBody-failure (body too large, read
// error) as a JSON error body. sagemaker@v1.263.2 types/errors.go models only
// ConflictException/ResourceInUse/ResourceLimitExceeded/ResourceNotFound --
// no generic internal-failure exception exists to reuse, so this sends the
// standard unmodeled "InternalFailure" code: the deserializer's per-operation
// switch won't match any of the four modeled names and falls through to
// smithy.GenericAPIError with this code intact, rather than the bare
// text/plain that previously made every ReadBody failure here undecodable
// (restjson.GetErrorInfo JSON-decodes the body; gopherstack-o7gx).
func writeInternalServerError(c *echo.Context) error {
	payload, err := json.Marshal(map[string]string{
		keyTypeField:    "InternalFailure",
		keyMessageField: "internal server error",
	})
	if err != nil {
		return err
	}

	return c.JSONBlob(http.StatusInternalServerError, payload)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrInvalidParameter):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrResourceNotFound):
		// See ErrResourceNotFound's doc comment for the covered families.
		// Checked before the generic ErrNotFound case below so these
		// families' real "ResourceNotFound" wire exception isn't papered
		// over with the blanket ValidationException the rest of the
		// service emits.
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceNotFound",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrConflictException):
		// ClusterSchedulerConfig/ComputeQuota only — see ErrConflictException's
		// doc comment. Checked before the generic ErrConflict case below so
		// these resources' real "ConflictException" wire exception isn't
		// papered over with the blanket ResourceInUse the rest of the service
		// emits.
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceInUse",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS REST-JSON protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// epochSecondsPtr is the nil-safe, pointer-preserving variant of
// [epochSeconds] for optional timestamp fields (`*time.Time` in the backend
// model, `json:"X,omitempty"` on the wire): a nil input stays nil so
// `omitempty` still drops the field, while a non-nil input is converted to
// an epoch-seconds float64 instead of Go's default RFC3339 string encoding.
func epochSecondsPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	v := epochSeconds(*t)

	return &v
}

// timeFromEpochSeconds is the inverse of [epochSeconds] — it is used by the
// paired UnmarshalJSON methods that accompany every MarshalJSON override in
// this package, so that a struct's own persistence.go snapshot/restore round
// trip (which marshals/unmarshals these same structs directly) stays
// symmetric with the epoch-seconds wire encoding used for API responses.
func timeFromEpochSeconds(f float64) time.Time {
	return time.Unix(int64(f), 0).UTC()
}

// timeFromEpochSecondsPtr is the nil-safe pointer variant of
// [timeFromEpochSeconds], pairing with [epochSecondsPtr].
func timeFromEpochSecondsPtr(f *float64) *time.Time {
	if f == nil {
		return nil
	}

	v := timeFromEpochSeconds(*f)

	return &v
}

// tagObject represents a SageMaker tag in the JSON API format.
type tagObject struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// toTagObjects converts a map of tags to a slice of tag objects sorted by key.
func toTagObjects(tags map[string]string) []tagObject {
	result := make([]tagObject, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagObject{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// fromTagObjects converts a slice of tag objects to a map.
func fromTagObjects(tags []tagObject) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// listResp builds a paginated list response with the given key and items.
func listResp(key string, items []map[string]any, nextToken string) ([]byte, error) {
	resp := map[string]any{key: items}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}
