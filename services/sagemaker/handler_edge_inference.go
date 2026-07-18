package sagemaker

import (
	"context"
)

// accuracy3 operation name constants.
const (
	opCreateEdgePackagingJob                     = "CreateEdgePackagingJob"
	opDescribeEdgePackagingJob                   = "DescribeEdgePackagingJob"
	opStopEdgePackagingJob                       = "StopEdgePackagingJob"
	opListEdgePackagingJobs                      = "ListEdgePackagingJobs"
	opCreateInferenceRecommendationsJob          = "CreateInferenceRecommendationsJob"
	opDescribeInferenceRecommendationsJob        = "DescribeInferenceRecommendationsJob"
	opStopInferenceRecommendationsJob            = "StopInferenceRecommendationsJob"
	opListInferenceRecommendationsJobs           = "ListInferenceRecommendationsJobs"
	opListInferenceRecommendationsJobSteps       = "ListInferenceRecommendationsJobSteps"
	opListMlflowTrackingServers                  = "ListMlflowTrackingServers"
	opUpdateMlflowTrackingServer                 = "UpdateMlflowTrackingServer"
	opListModelCards                             = "ListModelCards"
	opListModelCardVersions                      = "ListModelCardVersions"
	opListModelCardExportJobs                    = "ListModelCardExportJobs"
	opUpdateModelPackage                         = "UpdateModelPackage"
	opUpdateSpace                                = "UpdateSpace"
	opUpdateUserProfile                          = "UpdateUserProfile"
	opListOptimizationJobs                       = "ListOptimizationJobs"
	opListStudioLifecycleConfigs                 = "ListStudioLifecycleConfigs"
	opListInferenceExperiments                   = "ListInferenceExperiments"
	opListFlowDefinitions                        = "ListFlowDefinitions"
	opListHumanTaskUis                           = "ListHumanTaskUis"
	opListAppImageConfigs                        = "ListAppImageConfigs"
	opListTrainingJobsForHyperParameterTuningJob = "ListTrainingJobsForHyperParameterTuningJob"
	keyEdgePackagingJobArn                       = "EdgePackagingJobArn"
	keyJobArn                                    = "JobArn"
)

func accuracy3OpsSupported() []string {
	return []string{
		opCreateEdgePackagingJob,
		opDescribeEdgePackagingJob,
		opStopEdgePackagingJob,
		opListEdgePackagingJobs,
		opCreateInferenceRecommendationsJob,
		opDescribeInferenceRecommendationsJob,
		opStopInferenceRecommendationsJob,
		opListInferenceRecommendationsJobs,
		opListInferenceRecommendationsJobSteps,
		opListMlflowTrackingServers,
		opUpdateMlflowTrackingServer,
		opListModelCards,
		opListModelCardVersions,
		opListModelCardExportJobs,
		opUpdateModelPackage,
		opUpdateSpace,
		opUpdateUserProfile,
		opListOptimizationJobs,
		opListStudioLifecycleConfigs,
		opListInferenceExperiments,
		opListFlowDefinitions,
		opListHumanTaskUis,
		opListAppImageConfigs,
		opListTrainingJobsForHyperParameterTuningJob,
	}
}

// dispatchAccuracy3Ops dispatches the accuracy3 real stateful operations.
func (h *Handler) dispatchAccuracy3Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchEdgeAndInferenceOps(ctx, op, body); ok {
		return r, ok, err
	}

	return h.dispatchListAndUpdateOps(ctx, op, body)
}

func (h *Handler) dispatchEdgeAndInferenceOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateEdgePackagingJob:
		r, err := h.handleCreateEdgePackagingJob(ctx, body)

		return r, true, err
	case opDescribeEdgePackagingJob:
		r, err := h.handleDescribeEdgePackagingJob(ctx, body)

		return r, true, err
	case opStopEdgePackagingJob:
		return nil, true, h.handleStopEdgePackagingJob(ctx, body)
	case opListEdgePackagingJobs:
		r, err := h.handleListEdgePackagingJobs(ctx, body)

		return r, true, err
	case opCreateInferenceRecommendationsJob:
		r, err := h.handleCreateInferenceRecommendationsJob(ctx, body)

		return r, true, err
	case opDescribeInferenceRecommendationsJob:
		r, err := h.handleDescribeInferenceRecommendationsJob(ctx, body)

		return r, true, err
	case opStopInferenceRecommendationsJob:
		return nil, true, h.handleStopInferenceRecommendationsJob(ctx, body)
	case opListInferenceRecommendationsJobs:
		r, err := h.handleListInferenceRecommendationsJobs(ctx, body)

		return r, true, err
	case opListInferenceRecommendationsJobSteps:
		r, err := h.handleListInferenceRecommendationsJobSteps(ctx, body)

		return r, true, err
	case opListTrainingJobsForHyperParameterTuningJob:
		r, err := h.handleListTrainingJobsForHyperParameterTuningJob(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchListAndUpdateOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opListMlflowTrackingServers:
		r, err := h.handleListMlflowTrackingServers(ctx, body)

		return r, true, err
	case opUpdateMlflowTrackingServer:
		r, err := h.handleUpdateMlflowTrackingServer(ctx, body)

		return r, true, err
	case opListModelCards:
		r, err := h.handleListModelCards(ctx, body)

		return r, true, err
	case opListModelCardVersions:
		r, err := h.handleListModelCardVersions(ctx, body)

		return r, true, err
	case opListModelCardExportJobs:
		r, err := h.handleListModelCardExportJobs(ctx, body)

		return r, true, err
	case opUpdateModelPackage:
		r, err := h.handleUpdateModelPackage(ctx, body)

		return r, true, err
	case opUpdateSpace:
		r, err := h.handleUpdateSpace(ctx, body)

		return r, true, err
	case opUpdateUserProfile:
		r, err := h.handleUpdateUserProfile(ctx, body)

		return r, true, err
	case opListOptimizationJobs:
		r, err := h.handleListOptimizationJobs(ctx, body)

		return r, true, err
	case opListStudioLifecycleConfigs:
		r, err := h.handleListStudioLifecycleConfigs(ctx, body)

		return r, true, err
	case opListInferenceExperiments:
		r, err := h.handleListInferenceExperiments(ctx, body)

		return r, true, err
	case opListFlowDefinitions:
		r, err := h.handleListFlowDefinitions(ctx, body)

		return r, true, err
	case opListHumanTaskUis:
		r, err := h.handleListHumanTaskUIs(ctx, body)

		return r, true, err
	case opListAppImageConfigs:
		r, err := h.handleListAppImageConfigs(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}
