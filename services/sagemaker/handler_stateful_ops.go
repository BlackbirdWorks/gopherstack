package sagemaker

import (
	"context"
)

// Resource field key constants used in JSON responses.
const (
	keyDomainID        = "DomainId"
	keyURL             = "Url"
	keyUserProfileName = "UserProfileName"
	keyRoleArn         = "RoleArn"
	keyExperimentName  = "ExperimentName"
	keyStatusField     = keyStatus // alias of keyStatus from handler_stubs.go
)

// Operation name constants for the stateful resource groups.
// Defining them here eliminates goconst warnings when the same string
// appears in switch statements across multiple files.
const (
	opCreateApp                     = "CreateApp"
	opDescribeApp                   = "DescribeApp"
	opListApps                      = "ListApps"
	opDeleteApp                     = "DeleteApp"
	opCreateDomain                  = "CreateDomain"
	opDescribeDomain                = "DescribeDomain"
	opListDomains                   = "ListDomains"
	opDeleteDomain                  = "DeleteDomain"
	opUpdateDomain                  = "UpdateDomain"
	opCreateUserProfile             = "CreateUserProfile"
	opDescribeUserProfile           = "DescribeUserProfile"
	opListUserProfiles              = "ListUserProfiles"
	opDeleteUserProfile             = "DeleteUserProfile"
	opCreateFeatureGroup            = "CreateFeatureGroup"
	opDescribeFeatureGroup          = "DescribeFeatureGroup"
	opListFeatureGroups             = "ListFeatureGroups"
	opDeleteFeatureGroup            = "DeleteFeatureGroup"
	opCreatePipeline                = "CreatePipeline"
	opDescribePipeline              = "DescribePipeline"
	opListPipelines                 = "ListPipelines"
	opUpdatePipeline                = "UpdatePipeline"
	opDeletePipeline                = "DeletePipeline"
	opStartPipelineExecution        = "StartPipelineExecution"
	opDescribePipelineExec          = "DescribePipelineExecution"
	opListPipelineExecutions        = "ListPipelineExecutions"
	opListPipelineParametersForExec = "ListPipelineParametersForExecution"
	opCreateExperiment              = "CreateExperiment"
	opDescribeExperiment            = "DescribeExperiment"
	opListExperiments               = "ListExperiments"
	opDeleteExperiment              = "DeleteExperiment"
	opCreateTrial                   = "CreateTrial"
	opDescribeTrial                 = "DescribeTrial"
	opListTrials                    = "ListTrials"
	opDeleteTrial                   = "DeleteTrial"
	opCreateTrialComponent          = "CreateTrialComponent"
	opDescribeTrialComponent        = "DescribeTrialComponent"
	opDeleteTrialComponent          = "DeleteTrialComponent"
)

func (h *Handler) dispatchStatefulOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchDomainOps(ctx, op, body); ok {
		return r, ok, err
	}

	if r, ok, err := h.dispatchFeatureGroupAndPipelineOps(ctx, op, body); ok {
		return r, ok, err
	}

	if r, ok, err := h.dispatchExperimentAndTrialOps(ctx, op, body); ok {
		return r, ok, err
	}

	if r, ok, err := h.dispatchPipelineVersionOps(ctx, op, body); ok {
		return r, ok, err
	}

	return h.dispatchTrialComponentExtraOps(ctx, op, body)
}

// dispatchTrialComponentExtraOps dispatches the TrialComponent association
// extras (list-with-filters and disassociate) that were added after the core
// Create/Describe/Delete TrialComponent ops above. Kept as its own chained
// dispatcher rather than folded into dispatchExperimentAndTrialOps's switch
// so that switch stays within the cyclomatic-complexity budget.
func (h *Handler) dispatchTrialComponentExtraOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "DisassociateTrialComponent":
		r, err := h.handleDisassociateTrialComponent(ctx, body)

		return r, true, err
	case "ListTrialComponents":
		r, err := h.handleListTrialComponents(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchDomainOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateDomain:
		r, err := h.handleCreateDomain(ctx, body)

		return r, true, err
	case opDescribeDomain:
		r, err := h.handleDescribeDomain(ctx, body)

		return r, true, err
	case opListDomains:
		r, err := h.handleListDomains(ctx, body)

		return r, true, err
	case opDeleteDomain:
		return nil, true, h.handleDeleteDomain(ctx, body)
	case opUpdateDomain:
		r, err := h.handleUpdateDomain(ctx, body)

		return r, true, err
	case opCreateUserProfile:
		r, err := h.handleCreateUserProfile(ctx, body)

		return r, true, err
	case opDescribeUserProfile:
		r, err := h.handleDescribeUserProfile(ctx, body)

		return r, true, err
	case opListUserProfiles:
		r, err := h.handleListUserProfiles(ctx, body)

		return r, true, err
	case opDeleteUserProfile:
		return nil, true, h.handleDeleteUserProfile(ctx, body)
	case opCreateApp:
		r, err := h.handleCreateApp(ctx, body)

		return r, true, err
	case opDescribeApp:
		r, err := h.handleDescribeApp(ctx, body)

		return r, true, err
	case opListApps:
		r, err := h.handleListApps(ctx, body)

		return r, true, err
	case opDeleteApp:
		return nil, true, h.handleDeleteApp(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchFeatureGroupAndPipelineOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateFeatureGroup:
		r, err := h.handleCreateFeatureGroup(ctx, body)

		return r, true, err
	case opDescribeFeatureGroup:
		r, err := h.handleDescribeFeatureGroup(ctx, body)

		return r, true, err
	case opListFeatureGroups:
		r, err := h.handleListFeatureGroups(ctx, body)

		return r, true, err
	case opDeleteFeatureGroup:
		return nil, true, h.handleDeleteFeatureGroup(ctx, body)
	case "UpdateFeatureGroup":
		r, err := h.handleUpdateFeatureGroup(ctx, body)

		return r, true, err
	case opCreatePipeline:
		r, err := h.handleCreatePipelineFull(ctx, body)

		return r, true, err
	case opDescribePipeline:
		r, err := h.handleDescribePipeline(ctx, body)

		return r, true, err
	case opListPipelines:
		r, err := h.handleListPipelines(ctx, body)

		return r, true, err
	case opUpdatePipeline:
		r, err := h.handleUpdatePipelineFull(ctx, body)

		return r, true, err
	case opDeletePipeline:
		r, err := h.handleDeletePipeline(ctx, body)

		return r, true, err
	case opStartPipelineExecution:
		r, err := h.handleStartPipelineExecutionFull(ctx, body)

		return r, true, err
	case opDescribePipelineExec:
		r, err := h.handleDescribePipelineExecution(ctx, body)

		return r, true, err
	case opListPipelineExecutions:
		r, err := h.handleListPipelineExecutions(ctx, body)

		return r, true, err
	case opListPipelineParametersForExec:
		r, err := h.handleListPipelineParametersForExecution(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchExperimentAndTrialOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateExperiment:
		r, err := h.handleCreateExperiment(ctx, body)

		return r, true, err
	case opDescribeExperiment:
		r, err := h.handleDescribeExperiment(ctx, body)

		return r, true, err
	case opListExperiments:
		r, err := h.handleListExperiments(ctx, body)

		return r, true, err
	case opDeleteExperiment:
		r, err := h.handleDeleteExperiment(ctx, body)

		return r, true, err
	case opCreateTrial:
		r, err := h.handleCreateTrial(ctx, body)

		return r, true, err
	case opDescribeTrial:
		r, err := h.handleDescribeTrial(ctx, body)

		return r, true, err
	case opListTrials:
		r, err := h.handleListTrials(ctx, body)

		return r, true, err
	case opDeleteTrial:
		r, err := h.handleDeleteTrial(ctx, body)

		return r, true, err
	case opCreateTrialComponent:
		r, err := h.handleCreateTrialComponent(ctx, body)

		return r, true, err
	case opDescribeTrialComponent:
		r, err := h.handleDescribeTrialComponent(ctx, body)

		return r, true, err
	case opDeleteTrialComponent:
		r, err := h.handleDeleteTrialComponent(ctx, body)

		return r, true, err
	case "UpdateExperiment":
		r, err := h.handleUpdateExperiment(ctx, body)

		return r, true, err
	case "UpdateTrial":
		r, err := h.handleUpdateTrial(ctx, body)

		return r, true, err
	case "UpdateTrialComponent":
		r, err := h.handleUpdateTrialComponent(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}
