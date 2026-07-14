package sagemaker

import (
	"context"
)

// key constants shared by the catalog / ML-ops dispatch and handlers.
const (
	keyNextToken             = "NextToken"
	opStopMonitoringSchedule = "StopMonitoringSchedule"
)

// dispatchBatch2Ops dispatches the 50 new real stateful operations (batch 2)
// by delegating to one sub-dispatcher per resource family, so no single
// switch needs a case for every operation.
func (h *Handler) dispatchBatch2Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	if r, ok, err := h.dispatchModelPackageOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchModelPackageGroupOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchAutoMLCodeRepositoryOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchProjectSpaceOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchImageOps(ctx, op, body); ok {
		return r, true, err
	}

	if r, ok, err := h.dispatchCompilationMonitoringOps(ctx, op, body); ok {
		return r, true, err
	}

	return h.dispatchWorkteamOps(ctx, op, body)
}

func (h *Handler) dispatchModelPackageOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateModelPackage":
		r, err := h.handleCreateModelPackage(ctx, body)

		return r, true, err
	case "DescribeModelPackage":
		r, err := h.handleDescribeModelPackage(ctx, body)

		return r, true, err
	case "DeleteModelPackage":
		return nil, true, h.handleDeleteModelPackage(ctx, body)
	case "ListModelPackages":
		r, err := h.handleListModelPackages(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchModelPackageGroupOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateModelPackageGroup":
		r, err := h.handleCreateModelPackageGroup(ctx, body)

		return r, true, err
	case "DescribeModelPackageGroup":
		r, err := h.handleDescribeModelPackageGroup(ctx, body)

		return r, true, err
	case "DeleteModelPackageGroup":
		return nil, true, h.handleDeleteModelPackageGroup(ctx, body)
	case "ListModelPackageGroups":
		r, err := h.handleListModelPackageGroups(ctx, body)

		return r, true, err
	case "GetModelPackageGroupPolicy":
		r, err := h.handleGetModelPackageGroupPolicy(ctx, body)

		return r, true, err
	case "PutModelPackageGroupPolicy":
		r, err := h.handlePutModelPackageGroupPolicy(ctx, body)

		return r, true, err
	case "DeleteModelPackageGroupPolicy":
		return nil, true, h.handleDeleteModelPackageGroupPolicy(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchAutoMLCodeRepositoryOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateAutoMLJob", "CreateAutoMLJobV2":
		r, err := h.handleCreateAutoMLJob(ctx, body)

		return r, true, err
	case "DescribeAutoMLJob", "DescribeAutoMLJobV2":
		r, err := h.handleDescribeAutoMLJob(ctx, body)

		return r, true, err
	case "StopAutoMLJob":
		return nil, true, h.handleStopAutoMLJob(ctx, body)
	case "ListAutoMLJobs":
		r, err := h.handleListAutoMLJobs(ctx, body)

		return r, true, err
	case "CreateCodeRepository":
		r, err := h.handleCreateCodeRepository(ctx, body)

		return r, true, err
	case "DescribeCodeRepository":
		r, err := h.handleDescribeCodeRepository(ctx, body)

		return r, true, err
	case "UpdateCodeRepository":
		r, err := h.handleUpdateCodeRepository(ctx, body)

		return r, true, err
	case "DeleteCodeRepository":
		return nil, true, h.handleDeleteCodeRepository(ctx, body)
	case "ListCodeRepositories":
		r, err := h.handleListCodeRepositories(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchProjectSpaceOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateProject":
		r, err := h.handleCreateProject(ctx, body)

		return r, true, err
	case "DescribeProject":
		r, err := h.handleDescribeProject(ctx, body)

		return r, true, err
	case "DeleteProject":
		return nil, true, h.handleDeleteProject(ctx, body)
	case "ListProjects":
		r, err := h.handleListProjects(ctx, body)

		return r, true, err
	case "CreateSpace":
		r, err := h.handleCreateSpace(ctx, body)

		return r, true, err
	case "DescribeSpace":
		r, err := h.handleDescribeSpace(ctx, body)

		return r, true, err
	case "DeleteSpace":
		return nil, true, h.handleDeleteSpace(ctx, body)
	case "ListSpaces":
		r, err := h.handleListSpaces(ctx, body)

		return r, true, err
	case "UpdateProject":
		r, err := h.handleUpdateProject(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchImageOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateImage":
		r, err := h.handleCreateImage(ctx, body)

		return r, true, err
	case "DescribeImage":
		r, err := h.handleDescribeImage(ctx, body)

		return r, true, err
	case "DeleteImage":
		return nil, true, h.handleDeleteImage(ctx, body)
	case "ListImages":
		r, err := h.handleListImages(ctx, body)

		return r, true, err
	case "UpdateImage":
		r, err := h.handleUpdateImage(ctx, body)

		return r, true, err
	case "CreateImageVersion":
		r, err := h.handleCreateImageVersion(ctx, body)

		return r, true, err
	case "DescribeImageVersion":
		r, err := h.handleDescribeImageVersion(ctx, body)

		return r, true, err
	case "DeleteImageVersion":
		return nil, true, h.handleDeleteImageVersion(ctx, body)
	case "ListImageVersions":
		r, err := h.handleListImageVersions(ctx, body)

		return r, true, err
	case "UpdateImageVersion":
		r, err := h.handleUpdateImageVersion(ctx, body)

		return r, true, err
	case "ListAliases":
		r, err := h.handleListAliases(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchCompilationMonitoringOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateCompilationJob":
		r, err := h.handleCreateCompilationJob(ctx, body)

		return r, true, err
	case "DescribeCompilationJob":
		r, err := h.handleDescribeCompilationJob(ctx, body)

		return r, true, err
	case "DeleteCompilationJob":
		return nil, true, h.handleDeleteCompilationJob(ctx, body)
	case "StopCompilationJob":
		return nil, true, h.handleStopCompilationJob(ctx, body)
	case "ListCompilationJobs":
		r, err := h.handleListCompilationJobs(ctx, body)

		return r, true, err
	case "CreateMonitoringSchedule":
		r, err := h.handleCreateMonitoringSchedule(ctx, body)

		return r, true, err
	case "DescribeMonitoringSchedule":
		r, err := h.handleDescribeMonitoringSchedule(ctx, body)

		return r, true, err
	case "DeleteMonitoringSchedule":
		return nil, true, h.handleDeleteMonitoringSchedule(ctx, body)
	case opStopMonitoringSchedule:
		return nil, true, h.handleStopMonitoringSchedule(ctx, body)
	case "StartMonitoringSchedule":
		return nil, true, h.handleStartMonitoringSchedule(ctx, body)
	case "UpdateMonitoringSchedule":
		r, err := h.handleUpdateMonitoringSchedule(ctx, body)

		return r, true, err
	case "ListMonitoringSchedules":
		r, err := h.handleListMonitoringSchedules(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchWorkteamOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateWorkteam":
		r, err := h.handleCreateWorkteam(ctx, body)

		return r, true, err
	case "DescribeWorkteam":
		r, err := h.handleDescribeWorkteam(ctx, body)

		return r, true, err
	case "DeleteWorkteam":
		r, err := h.handleDeleteWorkteam(ctx, body)

		return r, true, err
	case "ListWorkteams":
		r, err := h.handleListWorkteams(ctx, body)

		return r, true, err
	case "UpdateWorkteam":
		r, err := h.handleUpdateWorkteam(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}
