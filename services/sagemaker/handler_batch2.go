package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// batch2 key constants to avoid goconst warnings.
const (
	keyNextToken             = "NextToken"
	opStopMonitoringSchedule = "StopMonitoringSchedule"
)

// dispatchBatch2Ops dispatches the 50 new real stateful operations (batch 2).
//
//nolint:cyclop,gocyclo,funlen // large switch is required for dispatching many operations
func (h *Handler) dispatchBatch2Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// ModelPackage
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

	// ModelPackageGroup
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

	// AutoMLJob
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

	// CodeRepository
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

	// Project
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

	// Space
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

	// Image
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

	// ImageVersion
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

	// CompilationJob
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

	// MonitoringSchedule
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

	// Workteam
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

// ---------------------------------------------------------------------------
// ModelPackage handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                    map[string]string `json:"Tags"`
		ModelPackageName        string            `json:"ModelPackageName"`
		ModelPackageGroupName   string            `json:"ModelPackageGroupName"`
		ModelPackageDescription string            `json:"ModelPackageDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackage(ctx,
		req.ModelPackageName, req.ModelPackageGroupName, req.ModelPackageDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageArn": result.ModelPackageArn})
}

func (h *Handler) handleDescribeModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackage(ctx, req.ModelPackageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackage(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackage(ctx, req.ModelPackageName)
}

func (h *Handler) handleListModelPackages(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
		NextToken             string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackages(ctx, req.ModelPackageGroupName, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, mp := range items {
		summaries = append(summaries, map[string]any{
			"ModelPackageName":   mp.ModelPackageName,
			"ModelPackageArn":    mp.ModelPackageArn,
			"ModelPackageStatus": mp.ModelPackageStatus,
			keyCreationTime:      mp.CreationTime,
		})
	}

	return json.Marshal(map[string]any{
		"ModelPackageSummaryList": summaries,
		keyNextToken:              next,
	})
}

// ---------------------------------------------------------------------------
// ModelPackageGroup handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		ModelPackageGroupName        string            `json:"ModelPackageGroupName"`
		ModelPackageGroupDescription string            `json:"ModelPackageGroupDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackageGroup(ctx,
		req.ModelPackageGroupName, req.ModelPackageGroupDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageGroupArn": result.ModelPackageGroupArn})
}

func (h *Handler) handleDescribeModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackageGroup(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackageGroup(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroup(ctx, req.ModelPackageGroupName)
}

func (h *Handler) handleListModelPackageGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackageGroups(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, g := range items {
		summaries = append(summaries, map[string]any{
			"ModelPackageGroupName":   g.ModelPackageGroupName,
			"ModelPackageGroupArn":    g.ModelPackageGroupArn,
			"ModelPackageGroupStatus": g.ModelPackageGroupStatus,
			keyCreationTime:           g.CreationTime,
		})
	}

	return json.Marshal(map[string]any{
		"ModelPackageGroupSummaryList": summaries,
		keyNextToken:                   next,
	})
}

func (h *Handler) handleGetModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"ResourcePolicy": policy})
}

func (h *Handler) handlePutModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
		ResourcePolicy        string `json:"ResourcePolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	if req.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", errInvalidRequest)
	}

	g, err := h.Backend.PutModelPackageGroupPolicy(ctx, req.ModelPackageGroupName, req.ResourcePolicy)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyModelPackageGroupArn: g.ModelPackageGroupArn})
}

func (h *Handler) handleDeleteModelPackageGroupPolicy(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
}

// ---------------------------------------------------------------------------
// AutoMLJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		AutoMLJobName string            `json:"AutoMLJobName"`
		RoleArn       string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAutoMLJob(ctx, req.AutoMLJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"AutoMLJobArn": result.AutoMLJobArn})
}

func (h *Handler) handleDescribeAutoMLJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAutoMLJob(ctx, req.AutoMLJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopAutoMLJob(ctx context.Context, body []byte) error {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	return h.Backend.StopAutoMLJob(ctx, req.AutoMLJobName)
}

func (h *Handler) handleListAutoMLJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListAutoMLJobs(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			"AutoMLJobName":   j.AutoMLJobName,
			"AutoMLJobArn":    j.AutoMLJobArn,
			"AutoMLJobStatus": j.AutoMLJobStatus,
			keyCreationTime:   j.CreationTime,
		})
	}

	return json.Marshal(map[string]any{
		"AutoMLJobSummaries": summaries,
		keyNextToken:         next,
	})
}

// ---------------------------------------------------------------------------
// CodeRepository handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		GitConfig          map[string]string `json:"GitConfig"`
		Tags               map[string]string `json:"Tags"`
		CodeRepositoryName string            `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDescribeCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCodeRepository(ctx, req.CodeRepositoryName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateCodeRepository(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		GitConfig          map[string]string `json:"GitConfig"`
		CodeRepositoryName string            `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateCodeRepository(ctx, req.CodeRepositoryName, req.GitConfig)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDeleteCodeRepository(ctx context.Context, body []byte) error {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCodeRepository(ctx, req.CodeRepositoryName)
}

func (h *Handler) handleListCodeRepositories(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCodeRepositories(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, r := range items {
		summaries = append(summaries, map[string]any{
			"CodeRepositoryName": r.CodeRepositoryName,
			keyCodeRepositoryArn: r.CodeRepositoryArn,
			keyCreationTime:      r.CreationTime,
			keyLastModifiedTime:  r.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"CodeRepositorySummaryList": summaries,
		keyNextToken:                next,
	})
}

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		ProjectName        string            `json:"ProjectName"`
		ProjectDescription string            `json:"ProjectDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateProject(ctx, req.ProjectName, req.ProjectDescription, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ProjectArn": result.ProjectArn,
		"ProjectId":  result.ProjectID,
	})
}

func (h *Handler) handleDescribeProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeProject(ctx, req.ProjectName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteProject(ctx context.Context, body []byte) error {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	return h.Backend.DeleteProject(ctx, req.ProjectName)
}

func (h *Handler) handleListProjects(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListProjects(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, p := range items {
		summaries = append(summaries, map[string]any{
			"ProjectName":   p.ProjectName,
			"ProjectArn":    p.ProjectArn,
			"ProjectId":     p.ProjectID,
			"ProjectStatus": p.ProjectStatus,
			keyCreationTime: p.CreationTime,
		})
	}

	return json.Marshal(map[string]any{
		"ProjectSummaryList": summaries,
		keyNextToken:         next,
	})
}

// ---------------------------------------------------------------------------
// Space handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags      map[string]string `json:"Tags"`
		DomainID  string            `json:"DomainId"`
		SpaceName string            `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateSpace(ctx, req.DomainID, req.SpaceName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"SpaceArn": result.SpaceArn})
}

func (h *Handler) handleDescribeSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeSpace(ctx, req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteSpace(ctx context.Context, body []byte) error {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainID is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	return h.Backend.DeleteSpace(ctx, req.DomainID, req.SpaceName)
}

func (h *Handler) handleListSpaces(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIdEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListSpaces(ctx, req.DomainIDEquals, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, map[string]any{
			"SpaceName":         s.SpaceName,
			"SpaceArn":          s.SpaceArn,
			keyDomainID:         s.DomainID,
			"SpaceStatus":       s.SpaceStatus,
			keyCreationTime:     s.CreationTime,
			keyLastModifiedTime: s.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"Spaces":     summaries,
		keyNextToken: next,
	})
}

// ---------------------------------------------------------------------------
// Image handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ImageName   string            `json:"ImageName"`
		Description string            `json:"Description"`
		RoleArn     string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImage(ctx, req.ImageName, req.Description, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageArn": result.ImageArn})
}

func (h *Handler) handleDescribeImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImage(ctx, req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImage(ctx context.Context, body []byte) error {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImage(ctx, req.ImageName)
}

func (h *Handler) handleListImages(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListImages(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, img := range items {
		summaries = append(summaries, map[string]any{
			"ImageName":         img.ImageName,
			"ImageArn":          img.ImageArn,
			"ImageStatus":       img.ImageStatus,
			keyCreationTime:     img.CreationTime,
			keyLastModifiedTime: img.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"Images":     summaries,
		keyNextToken: next,
	})
}

func (h *Handler) handleUpdateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName        string   `json:"ImageName"`
		Description      *string  `json:"Description"`
		DisplayName      *string  `json:"DisplayName"`
		RoleArn          *string  `json:"RoleArn"`
		DeleteProperties []string `json:"DeleteProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateImage(ctx, req.ImageName, UpdateImageOptions{
		Description:      req.Description,
		DisplayName:      req.DisplayName,
		RoleArn:          req.RoleArn,
		DeleteProperties: req.DeleteProperties,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyImageArn: result.ImageArn})
}

// ---------------------------------------------------------------------------
// ImageVersion handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImageVersion(ctx, req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageVersionArn": result.ImageVersionArn})
}

func (h *Handler) handleDescribeImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		Version   int    `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImageVersion(ctx, req.ImageName, req.Version)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImageVersion(ctx context.Context, body []byte) error {
	var req struct {
		ImageName string `json:"ImageName"`
		Version   int    `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImageVersion(ctx, req.ImageName, req.Version)
}

func (h *Handler) handleListImageVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	items, next := h.Backend.ListImageVersions(ctx, req.ImageName, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, iv := range items {
		summaries = append(summaries, map[string]any{
			"ImageVersionArn":    iv.ImageVersionArn,
			"ImageVersionStatus": iv.ImageVersionStatus,
			"Version":            iv.Version,
			keyCreationTime:      iv.CreationTime,
			keyLastModifiedTime:  iv.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"ImageVersions": summaries,
		keyNextToken:    next,
	})
}

func (h *Handler) handleUpdateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Horovod         *bool    `json:"Horovod"`
		ImageName       string   `json:"ImageName"`
		JobType         string   `json:"JobType"`
		MLFramework     string   `json:"MLFramework"`
		Processor       string   `json:"Processor"`
		ProgrammingLang string   `json:"ProgrammingLang"`
		ReleaseNotes    string   `json:"ReleaseNotes"`
		VendorGuidance  string   `json:"VendorGuidance"`
		AliasesToAdd    []string `json:"AliasesToAdd"`
		AliasesToDelete []string `json:"AliasesToDelete"`
		Version         int      `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateImageVersion(ctx, req.ImageName, req.Version, UpdateImageVersionOptions{
		Horovod:         req.Horovod,
		JobType:         req.JobType,
		MLFramework:     req.MLFramework,
		Processor:       req.Processor,
		ProgrammingLang: req.ProgrammingLang,
		ReleaseNotes:    req.ReleaseNotes,
		VendorGuidance:  req.VendorGuidance,
		AliasesToAdd:    req.AliasesToAdd,
		AliasesToDelete: req.AliasesToDelete,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyImageVersionArn: result.ImageVersionArn})
}

// ---------------------------------------------------------------------------
// CompilationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		CompilationJobName string            `json:"CompilationJobName"`
		RoleArn            string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCompilationJob(ctx, req.CompilationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"CompilationJobArn": result.CompilationJobArn})
}

func (h *Handler) handleDescribeCompilationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCompilationJob(ctx, req.CompilationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteCompilationJob(ctx context.Context, body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCompilationJob(ctx, req.CompilationJobName)
}

func (h *Handler) handleStopCompilationJob(ctx context.Context, body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopCompilationJob(ctx, req.CompilationJobName)
}

func (h *Handler) handleListCompilationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCompilationJobs(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			"CompilationJobName":   j.CompilationJobName,
			"CompilationJobArn":    j.CompilationJobArn,
			"CompilationJobStatus": j.CompilationJobStatus,
			keyCreationTime:        j.CreationTime,
			keyLastModifiedTime:    j.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"CompilationJobSummaries": summaries,
		keyNextToken:              next,
	})
}

// ---------------------------------------------------------------------------
// MonitoringSchedule handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                   map[string]string `json:"Tags"`
		MonitoringScheduleName string            `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMonitoringSchedule(ctx, req.MonitoringScheduleName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleDescribeMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMonitoringSchedule(ctx, req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleStopMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StopMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleStartMonitoringSchedule(ctx context.Context, body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StartMonitoringSchedule(ctx, req.MonitoringScheduleName)
}

func (h *Handler) handleUpdateMonitoringSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMonitoringSchedule(ctx, req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleListMonitoringSchedules(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringSchedules(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, ms := range items {
		summaries = append(summaries, map[string]any{
			"MonitoringScheduleName":   ms.MonitoringScheduleName,
			keyMonitoringScheduleArn:   ms.MonitoringScheduleArn,
			"MonitoringScheduleStatus": ms.MonitoringScheduleStatus,
			keyCreationTime:            ms.CreationTime,
			keyLastModifiedTime:        ms.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"MonitoringScheduleSummaries": summaries,
		keyNextToken:                  next,
	})
}

// ---------------------------------------------------------------------------
// Workteam handlers
// ---------------------------------------------------------------------------

// workteamResponseMap builds the AWS wire representation of a Workteam,
// converting timestamps to epoch seconds as required by the aws-json-1.1 protocol.
func workteamResponseMap(w *Workteam) map[string]any {
	resp := map[string]any{
		"WorkteamName":    w.WorkteamName,
		"WorkteamArn":     w.WorkteamArn,
		"Description":     w.Description,
		"CreateDate":      epochSeconds(w.CreateDate),
		"LastUpdatedDate": epochSeconds(w.LastUpdatedDate),
	}

	if w.WorkforceArn != "" {
		resp[keyWorkforceArn] = w.WorkforceArn
	}

	if w.SubDomain != "" {
		resp["SubDomain"] = w.SubDomain
	}

	resp["MemberDefinitions"] = w.MemberDefinitions
	if w.MemberDefinitions == nil {
		resp["MemberDefinitions"] = []MemberDefinition{}
	}

	return resp
}

func (h *Handler) handleCreateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags              []tagObject        `json:"Tags"`
		WorkteamName      string             `json:"WorkteamName"`
		Description       string             `json:"Description"`
		WorkforceName     string             `json:"WorkforceName"`
		MemberDefinitions []MemberDefinition `json:"MemberDefinitions"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkteam(ctx, CreateWorkteamOptions{
		Name:              req.WorkteamName,
		Description:       req.Description,
		WorkforceName:     req.WorkforceName,
		MemberDefinitions: req.MemberDefinitions,
		Tags:              fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"WorkteamArn": result.WorkteamArn})
}

func (h *Handler) handleDescribeWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkteam(ctx, req.WorkteamName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": workteamResponseMap(result)})
}

func (h *Handler) handleUpdateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName      string             `json:"WorkteamName"`
		Description       string             `json:"Description"`
		MemberDefinitions []MemberDefinition `json:"MemberDefinitions"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkteam(ctx, UpdateWorkteamOptions{
		Name:              req.WorkteamName,
		Description:       req.Description,
		MemberDefinitions: req.MemberDefinitions,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": workteamResponseMap(result)})
}

func (h *Handler) handleDeleteWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkteam(ctx, req.WorkteamName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Success": true})
}

func (h *Handler) handleListWorkteams(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkteams(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, workteamResponseMap(w))
	}

	return json.Marshal(map[string]any{
		"Workteams":  summaries,
		keyNextToken: next,
	})
}
