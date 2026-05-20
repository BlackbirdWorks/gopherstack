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
	_ context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// ModelPackage
	case "CreateModelPackage":
		r, err := h.handleCreateModelPackage(body)

		return r, true, err
	case "DescribeModelPackage":
		r, err := h.handleDescribeModelPackage(body)

		return r, true, err
	case "DeleteModelPackage":
		return nil, true, h.handleDeleteModelPackage(body)
	case "ListModelPackages":
		r, err := h.handleListModelPackages(body)

		return r, true, err

	// ModelPackageGroup
	case "CreateModelPackageGroup":
		r, err := h.handleCreateModelPackageGroup(body)

		return r, true, err
	case "DescribeModelPackageGroup":
		r, err := h.handleDescribeModelPackageGroup(body)

		return r, true, err
	case "DeleteModelPackageGroup":
		return nil, true, h.handleDeleteModelPackageGroup(body)
	case "ListModelPackageGroups":
		r, err := h.handleListModelPackageGroups(body)

		return r, true, err

	// AutoMLJob
	case "CreateAutoMLJob", "CreateAutoMLJobV2":
		r, err := h.handleCreateAutoMLJob(body)

		return r, true, err
	case "DescribeAutoMLJob", "DescribeAutoMLJobV2":
		r, err := h.handleDescribeAutoMLJob(body)

		return r, true, err
	case "StopAutoMLJob":
		return nil, true, h.handleStopAutoMLJob(body)
	case "ListAutoMLJobs":
		r, err := h.handleListAutoMLJobs(body)

		return r, true, err

	// CodeRepository
	case "CreateCodeRepository":
		r, err := h.handleCreateCodeRepository(body)

		return r, true, err
	case "DescribeCodeRepository":
		r, err := h.handleDescribeCodeRepository(body)

		return r, true, err
	case "UpdateCodeRepository":
		r, err := h.handleUpdateCodeRepository(body)

		return r, true, err
	case "DeleteCodeRepository":
		return nil, true, h.handleDeleteCodeRepository(body)
	case "ListCodeRepositories":
		r, err := h.handleListCodeRepositories(body)

		return r, true, err

	// Project
	case "CreateProject":
		r, err := h.handleCreateProject(body)

		return r, true, err
	case "DescribeProject":
		r, err := h.handleDescribeProject(body)

		return r, true, err
	case "DeleteProject":
		return nil, true, h.handleDeleteProject(body)
	case "ListProjects":
		r, err := h.handleListProjects(body)

		return r, true, err

	// Space
	case "CreateSpace":
		r, err := h.handleCreateSpace(body)

		return r, true, err
	case "DescribeSpace":
		r, err := h.handleDescribeSpace(body)

		return r, true, err
	case "DeleteSpace":
		return nil, true, h.handleDeleteSpace(body)
	case "ListSpaces":
		r, err := h.handleListSpaces(body)

		return r, true, err

	// Image
	case "CreateImage":
		r, err := h.handleCreateImage(body)

		return r, true, err
	case "DescribeImage":
		r, err := h.handleDescribeImage(body)

		return r, true, err
	case "DeleteImage":
		return nil, true, h.handleDeleteImage(body)
	case "ListImages":
		r, err := h.handleListImages(body)

		return r, true, err

	// ImageVersion
	case "CreateImageVersion":
		r, err := h.handleCreateImageVersion(body)

		return r, true, err
	case "DescribeImageVersion":
		r, err := h.handleDescribeImageVersion(body)

		return r, true, err
	case "DeleteImageVersion":
		return nil, true, h.handleDeleteImageVersion(body)
	case "ListImageVersions":
		r, err := h.handleListImageVersions(body)

		return r, true, err

	// CompilationJob
	case "CreateCompilationJob":
		r, err := h.handleCreateCompilationJob(body)

		return r, true, err
	case "DescribeCompilationJob":
		r, err := h.handleDescribeCompilationJob(body)

		return r, true, err
	case "DeleteCompilationJob":
		return nil, true, h.handleDeleteCompilationJob(body)
	case "StopCompilationJob":
		return nil, true, h.handleStopCompilationJob(body)
	case "ListCompilationJobs":
		r, err := h.handleListCompilationJobs(body)

		return r, true, err

	// MonitoringSchedule
	case "CreateMonitoringSchedule":
		r, err := h.handleCreateMonitoringSchedule(body)

		return r, true, err
	case "DescribeMonitoringSchedule":
		r, err := h.handleDescribeMonitoringSchedule(body)

		return r, true, err
	case "DeleteMonitoringSchedule":
		return nil, true, h.handleDeleteMonitoringSchedule(body)
	case opStopMonitoringSchedule:
		return nil, true, h.handleStopMonitoringSchedule(body)
	case "StartMonitoringSchedule":
		return nil, true, h.handleStartMonitoringSchedule(body)
	case "UpdateMonitoringSchedule":
		r, err := h.handleUpdateMonitoringSchedule(body)

		return r, true, err
	case "ListMonitoringSchedules":
		r, err := h.handleListMonitoringSchedules(body)

		return r, true, err

	// Workteam
	case "CreateWorkteam":
		r, err := h.handleCreateWorkteam(body)

		return r, true, err
	case "DescribeWorkteam":
		r, err := h.handleDescribeWorkteam(body)

		return r, true, err
	case "DeleteWorkteam":
		return nil, true, h.handleDeleteWorkteam(body)
	case "ListWorkteams":
		r, err := h.handleListWorkteams(body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// ModelPackage handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelPackage(body []byte) ([]byte, error) {
	var req struct {
		ModelPackageName        string            `json:"ModelPackageName"`
		ModelPackageGroupName   string            `json:"ModelPackageGroupName"`
		ModelPackageDescription string            `json:"ModelPackageDescription"`
		Tags                    map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackage(
		req.ModelPackageName, req.ModelPackageGroupName, req.ModelPackageDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageArn": result.ModelPackageArn})
}

func (h *Handler) handleDescribeModelPackage(body []byte) ([]byte, error) {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackage(req.ModelPackageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackage(body []byte) error {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackage(req.ModelPackageName)
}

func (h *Handler) handleListModelPackages(body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
		NextToken             string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackages(req.ModelPackageGroupName, req.NextToken)

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

func (h *Handler) handleCreateModelPackageGroup(body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName        string            `json:"ModelPackageGroupName"`
		ModelPackageGroupDescription string            `json:"ModelPackageGroupDescription"`
		Tags                         map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackageGroup(
		req.ModelPackageGroupName, req.ModelPackageGroupDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageGroupArn": result.ModelPackageGroupArn})
}

func (h *Handler) handleDescribeModelPackageGroup(body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackageGroup(req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackageGroup(body []byte) error {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroup(req.ModelPackageGroupName)
}

func (h *Handler) handleListModelPackageGroups(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackageGroups(req.NextToken)

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

// ---------------------------------------------------------------------------
// AutoMLJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAutoMLJob(body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName string            `json:"AutoMLJobName"`
		RoleArn       string            `json:"RoleArn"`
		Tags          map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAutoMLJob(req.AutoMLJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"AutoMLJobArn": result.AutoMLJobArn})
}

func (h *Handler) handleDescribeAutoMLJob(body []byte) ([]byte, error) {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAutoMLJob(req.AutoMLJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopAutoMLJob(body []byte) error {
	var req struct {
		AutoMLJobName string `json:"AutoMLJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoMLJobName == "" {
		return fmt.Errorf("%w: AutoMLJobName is required", errInvalidRequest)
	}

	return h.Backend.StopAutoMLJob(req.AutoMLJobName)
}

func (h *Handler) handleListAutoMLJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListAutoMLJobs(req.NextToken)

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

func (h *Handler) handleCreateCodeRepository(body []byte) ([]byte, error) {
	var req struct {
		CodeRepositoryName string            `json:"CodeRepositoryName"`
		GitConfig          map[string]string `json:"GitConfig"`
		Tags               map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCodeRepository(req.CodeRepositoryName, req.GitConfig, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDescribeCodeRepository(body []byte) ([]byte, error) {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCodeRepository(req.CodeRepositoryName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateCodeRepository(body []byte) ([]byte, error) {
	var req struct {
		CodeRepositoryName string            `json:"CodeRepositoryName"`
		GitConfig          map[string]string `json:"GitConfig"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateCodeRepository(req.CodeRepositoryName, req.GitConfig)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyCodeRepositoryArn: result.CodeRepositoryArn})
}

func (h *Handler) handleDeleteCodeRepository(body []byte) error {
	var req struct {
		CodeRepositoryName string `json:"CodeRepositoryName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CodeRepositoryName == "" {
		return fmt.Errorf("%w: CodeRepositoryName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCodeRepository(req.CodeRepositoryName)
}

func (h *Handler) handleListCodeRepositories(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCodeRepositories(req.NextToken)

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

func (h *Handler) handleCreateProject(body []byte) ([]byte, error) {
	var req struct {
		ProjectName        string            `json:"ProjectName"`
		ProjectDescription string            `json:"ProjectDescription"`
		Tags               map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateProject(req.ProjectName, req.ProjectDescription, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ProjectArn": result.ProjectArn,
		"ProjectId":  result.ProjectID,
	})
}

func (h *Handler) handleDescribeProject(body []byte) ([]byte, error) {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeProject(req.ProjectName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteProject(body []byte) error {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	return h.Backend.DeleteProject(req.ProjectName)
}

func (h *Handler) handleListProjects(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListProjects(req.NextToken)

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

func (h *Handler) handleCreateSpace(body []byte) ([]byte, error) {
	var req struct {
		DomainID  string            `json:"DomainId"`
		SpaceName string            `json:"SpaceName"`
		Tags      map[string]string `json:"Tags"`
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

	result, err := h.Backend.CreateSpace(req.DomainID, req.SpaceName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"SpaceArn": result.SpaceArn})
}

func (h *Handler) handleDescribeSpace(body []byte) ([]byte, error) {
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

	result, err := h.Backend.DescribeSpace(req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteSpace(body []byte) error {
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

	return h.Backend.DeleteSpace(req.DomainID, req.SpaceName)
}

func (h *Handler) handleListSpaces(body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIdEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListSpaces(req.DomainIDEquals, req.NextToken)

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

func (h *Handler) handleCreateImage(body []byte) ([]byte, error) {
	var req struct {
		ImageName   string            `json:"ImageName"`
		Description string            `json:"Description"`
		RoleArn     string            `json:"RoleArn"`
		Tags        map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImage(req.ImageName, req.Description, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageArn": result.ImageArn})
}

func (h *Handler) handleDescribeImage(body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImage(req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImage(body []byte) error {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImage(req.ImageName)
}

func (h *Handler) handleListImages(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListImages(req.NextToken)

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

// ---------------------------------------------------------------------------
// ImageVersion handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateImageVersion(body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImageVersion(req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageVersionArn": result.ImageVersionArn})
}

func (h *Handler) handleDescribeImageVersion(body []byte) ([]byte, error) {
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

	result, err := h.Backend.DescribeImageVersion(req.ImageName, req.Version)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImageVersion(body []byte) error {
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

	return h.Backend.DeleteImageVersion(req.ImageName, req.Version)
}

func (h *Handler) handleListImageVersions(body []byte) ([]byte, error) {
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

	items, next := h.Backend.ListImageVersions(req.ImageName, req.NextToken)

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

// ---------------------------------------------------------------------------
// CompilationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateCompilationJob(body []byte) ([]byte, error) {
	var req struct {
		CompilationJobName string            `json:"CompilationJobName"`
		RoleArn            string            `json:"RoleArn"`
		Tags               map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateCompilationJob(req.CompilationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"CompilationJobArn": result.CompilationJobArn})
}

func (h *Handler) handleDescribeCompilationJob(body []byte) ([]byte, error) {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeCompilationJob(req.CompilationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteCompilationJob(body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteCompilationJob(req.CompilationJobName)
}

func (h *Handler) handleStopCompilationJob(body []byte) error {
	var req struct {
		CompilationJobName string `json:"CompilationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.CompilationJobName == "" {
		return fmt.Errorf("%w: CompilationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopCompilationJob(req.CompilationJobName)
}

func (h *Handler) handleListCompilationJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListCompilationJobs(req.NextToken)

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

func (h *Handler) handleCreateMonitoringSchedule(body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string            `json:"MonitoringScheduleName"`
		Tags                   map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMonitoringSchedule(req.MonitoringScheduleName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleDescribeMonitoringSchedule(body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMonitoringSchedule(req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMonitoringSchedule(body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMonitoringSchedule(req.MonitoringScheduleName)
}

func (h *Handler) handleStopMonitoringSchedule(body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StopMonitoringSchedule(req.MonitoringScheduleName)
}

func (h *Handler) handleStartMonitoringSchedule(body []byte) error {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	return h.Backend.StartMonitoringSchedule(req.MonitoringScheduleName)
}

func (h *Handler) handleUpdateMonitoringSchedule(body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMonitoringSchedule(req.MonitoringScheduleName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyMonitoringScheduleArn: result.MonitoringScheduleArn})
}

func (h *Handler) handleListMonitoringSchedules(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringSchedules(req.NextToken)

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

func (h *Handler) handleCreateWorkteam(body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string            `json:"WorkteamName"`
		Description  string            `json:"Description"`
		Tags         map[string]string `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkteam(req.WorkteamName, req.Description, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"WorkteamArn": result.WorkteamArn})
}

func (h *Handler) handleDescribeWorkteam(body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkteam(req.WorkteamName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": result})
}

func (h *Handler) handleDeleteWorkteam(body []byte) error {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	return h.Backend.DeleteWorkteam(req.WorkteamName)
}

func (h *Handler) handleListWorkteams(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkteams(req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, map[string]any{
			"WorkteamName":      w.WorkteamName,
			"WorkteamArn":       w.WorkteamArn,
			"Description":       w.Description,
			keyCreationTime:     w.CreationTime,
			keyLastModifiedTime: w.LastModifiedTime,
		})
	}

	return json.Marshal(map[string]any{
		"Workteams":  summaries,
		keyNextToken: next,
	})
}
