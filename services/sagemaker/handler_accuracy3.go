package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
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

// listResp builds a paginated list response with the given key and items.
func listResp(key string, items []map[string]any, nextToken string) ([]byte, error) {
	resp := map[string]any{key: items}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

// accuracy3OpsSupported returns the real stateful operations implemented in accuracy3.
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

// ---------------------------------------------------------------------------
// EdgePackagingJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                 map[string]string `json:"Tags,omitempty"`
		EdgePackagingJobName string            `json:"EdgePackagingJobName"`
		ModelName            string            `json:"ModelName,omitempty"`
		ModelVersion         string            `json:"ModelVersion,omitempty"`
		RoleArn              string            `json:"RoleArn,omitempty"`
		CompilationJobName   string            `json:"CompilationJobName,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateEdgePackagingJob(ctx, CreateEdgePackagingJobOptions{
		EdgePackagingJobName: req.EdgePackagingJobName,
		ModelName:            req.ModelName,
		ModelVersion:         req.ModelVersion,
		RoleArn:              req.RoleArn,
		CompilationJobName:   req.CompilationJobName,
		Tags:                 req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyEdgePackagingJobArn: j.EdgePackagingJobArn})
}

func (h *Handler) handleDescribeEdgePackagingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeEdgePackagingJob(ctx, req.EdgePackagingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"EdgePackagingJobName":   j.EdgePackagingJobName,
		"EdgePackagingJobArn":    j.EdgePackagingJobArn,
		"EdgePackagingJobStatus": j.EdgePackagingJobStatus,
		keyCreationTime:          epochSeconds(j.CreationTime),
		keyLastModifiedTime:      epochSeconds(j.LastModifiedTime),
	}

	if j.ModelName != "" {
		resp["ModelName"] = j.ModelName
	}

	if j.ModelVersion != "" {
		resp["ModelVersion"] = j.ModelVersion
	}

	if j.RoleArn != "" {
		resp[keyRoleArn] = j.RoleArn
	}

	if j.CompilationJobName != "" {
		resp["CompilationJobName"] = j.CompilationJobName
	}

	if j.FailureReason != "" {
		resp["FailureReason"] = j.FailureReason
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStopEdgePackagingJob(ctx context.Context, body []byte) error {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	return h.Backend.StopEdgePackagingJob(ctx, req.EdgePackagingJobName)
}

type edgePackagingJobSummary struct {
	EdgePackagingJobName   string  `json:"EdgePackagingJobName"`
	EdgePackagingJobArn    string  `json:"EdgePackagingJobArn"`
	EdgePackagingJobStatus string  `json:"EdgePackagingJobStatus"`
	ModelName              string  `json:"ModelName,omitempty"`
	ModelVersion           string  `json:"ModelVersion,omitempty"`
	CreationTime           float64 `json:"CreationTime"`
	LastModifiedTime       float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListEdgePackagingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals,omitempty"`
		NameContains string `json:"NameContains,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListEdgePackagingJobs(ctx, req.NextToken, ListEdgePackagingJobsFilter{
		StatusEquals: req.StatusEquals,
		NameContains: req.NameContains,
	})

	summaries := make([]edgePackagingJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, edgePackagingJobSummary{
			EdgePackagingJobName:   j.EdgePackagingJobName,
			EdgePackagingJobArn:    j.EdgePackagingJobArn,
			EdgePackagingJobStatus: j.EdgePackagingJobStatus,
			ModelName:              j.ModelName,
			ModelVersion:           j.ModelVersion,
			CreationTime:           epochSeconds(j.CreationTime),
			LastModifiedTime:       epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"EdgePackagingJobSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags           map[string]string `json:"Tags,omitempty"`
		JobName        string            `json:"JobName"`
		JobType        string            `json:"JobType,omitempty"`
		JobDescription string            `json:"JobDescription,omitempty"`
		RoleArn        string            `json:"RoleArn,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.CreateInferenceRecommendationsJob(ctx, CreateInferenceRecommendationsJobOptions{
		JobName:        req.JobName,
		JobType:        req.JobType,
		JobDescription: req.JobDescription,
		RoleArn:        req.RoleArn,
		Tags:           req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyJobArn: j.JobArn})
}

func (h *Handler) handleDescribeInferenceRecommendationsJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"JobName":                  j.JobName,
		keyJobArn:                  j.JobArn,
		keyStatus:                  j.Status,
		"InferenceRecommendations": []any{},
		keyCreationTime:            epochSeconds(j.CreationTime),
		keyLastModifiedTime:        epochSeconds(j.LastModifiedTime),
	}

	if j.JobType != "" {
		resp["JobType"] = j.JobType
	}

	if j.JobDescription != "" {
		resp["JobDescription"] = j.JobDescription
	}

	if j.RoleArn != "" {
		resp[keyRoleArn] = j.RoleArn
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStopInferenceRecommendationsJob(ctx context.Context, body []byte) error {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceRecommendationsJob(ctx, req.JobName)
}

type inferenceRecommendationsJobSummary struct {
	JobName          string  `json:"JobName"`
	JobArn           string  `json:"JobArn"`
	Status           string  `json:"Status"`
	JobType          string  `json:"JobType,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListInferenceRecommendationsJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListInferenceRecommendationsJobs(ctx, req.NextToken)

	summaries := make([]inferenceRecommendationsJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, inferenceRecommendationsJobSummary{
			JobName:          j.JobName,
			JobArn:           j.JobArn,
			Status:           j.Status,
			JobType:          j.JobType,
			CreationTime:     epochSeconds(j.CreationTime),
			LastModifiedTime: epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"InferenceRecommendationsJobs": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListInferenceRecommendationsJobSteps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		JobName   string `json:"JobName"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	if _, err := h.Backend.DescribeInferenceRecommendationsJob(ctx, req.JobName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Steps": []any{}})
}

// ---------------------------------------------------------------------------
// MLflow tracking server handlers (list + update)
// ---------------------------------------------------------------------------

func (h *Handler) handleListMlflowTrackingServers(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	servers, nextToken := h.Backend.ListMlflowTrackingServers(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(servers))
	for _, s := range servers {
		entry := map[string]any{
			"TrackingServerName":   s.TrackingServerName,
			"TrackingServerArn":    s.TrackingServerArn,
			"TrackingServerStatus": s.TrackingServerStatus,
			keyCreationTime:        epochSeconds(s.CreationTime),
			keyLastModifiedTime:    epochSeconds(s.LastModifiedTime),
		}
		if s.MlflowVersion != "" {
			entry["MlflowVersion"] = s.MlflowVersion
		}

		items = append(items, entry)
	}

	return listResp("TrackingServerSummaries", items, nextToken)
}

func (h *Handler) handleUpdateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
		MlflowVersion      string `json:"MlflowVersion,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateMlflowTrackingServer(ctx, req.TrackingServerName, req.MlflowVersion)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyTrackingServerArn: s.TrackingServerArn})
}

// ---------------------------------------------------------------------------
// ModelCard list handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListModelCards(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cards, nextToken := h.Backend.ListModelCards(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(cards))
	for _, c := range cards {
		items = append(items, map[string]any{
			"ModelCardName":     c.ModelCardName,
			"ModelCardArn":      c.ModelCardArn,
			"ModelCardStatus":   c.ModelCardStatus,
			"ModelCardVersion":  c.ModelCardVersion,
			keyCreationTime:     epochSeconds(c.CreationTime),
			keyLastModifiedTime: epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("ModelCardSummaries", items, nextToken)
}

func (h *Handler) handleListModelCardVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		NextToken     string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	card, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName)
	if err != nil {
		return nil, err
	}

	summaries := []map[string]any{
		{
			"ModelCardName":     card.ModelCardName,
			"ModelCardArn":      card.ModelCardArn,
			"ModelCardStatus":   card.ModelCardStatus,
			"ModelCardVersion":  card.ModelCardVersion,
			keyCreationTime:     epochSeconds(card.CreationTime),
			keyLastModifiedTime: epochSeconds(card.LastModifiedTime),
		},
	}

	return json.Marshal(map[string]any{"ModelCardVersionSummaryList": summaries})
}

func (h *Handler) handleListModelCardExportJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		NextToken     string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName != "" {
		if _, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName); err != nil {
			return nil, err
		}
	}

	return json.Marshal(map[string]any{"ModelCardExportJobSummaries": []any{}})
}

// ---------------------------------------------------------------------------
// Update handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageName    string `json:"ModelPackageName"`
		ModelApprovalStatus string `json:"ModelApprovalStatus,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	mp, err := h.Backend.UpdateModelPackage(ctx, req.ModelPackageName, req.ModelApprovalStatus)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyModelPackageArn: mp.ModelPackageArn})
}

func (h *Handler) handleUpdateSpace(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID  string `json:"DomainId"`
		SpaceName string `json:"SpaceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.SpaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateSpace(ctx, req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keySpaceArn: s.SpaceArn})
}

func (h *Handler) handleUpdateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.UpdateUserProfile(ctx, req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}

// ---------------------------------------------------------------------------
// Batch3 resource list handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListOptimizationJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListOptimizationJobs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, map[string]any{
			"OptimizationJobName":   j.OptimizationJobName,
			"OptimizationJobArn":    j.OptimizationJobArn,
			"OptimizationJobStatus": j.OptimizationJobStatus,
			keyCreationTime:         epochSeconds(j.CreationTime),
			keyLastModifiedTime:     epochSeconds(j.LastModifiedTime),
		})
	}

	return listResp("OptimizationJobSummaries", items, nextToken)
}

func (h *Handler) handleListStudioLifecycleConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListStudioLifecycleConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			"StudioLifecycleConfigName": c.StudioLifecycleConfigName,
			"StudioLifecycleConfigArn":  c.StudioLifecycleConfigArn,
			keyCreationTime:             epochSeconds(c.CreationTime),
			keyLastModifiedTime:         epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("StudioLifecycleConfigs", items, nextToken)
}

func (h *Handler) handleListInferenceExperiments(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListInferenceExperiments(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(exps))
	for _, e := range exps {
		entry := map[string]any{
			"Name":              e.Name,
			"Arn":               e.Arn,
			keyStatus:           e.Status,
			keyCreationTime:     epochSeconds(e.CreationTime),
			keyLastModifiedTime: epochSeconds(e.LastModifiedTime),
		}
		if e.Type != "" {
			entry["Type"] = e.Type
		}

		items = append(items, entry)
	}

	return listResp("InferenceExperiments", items, nextToken)
}

func (h *Handler) handleListFlowDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	defs, nextToken := h.Backend.ListFlowDefinitions(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(defs))
	for _, d := range defs {
		items = append(items, map[string]any{
			"FlowDefinitionName":   d.FlowDefinitionName,
			"FlowDefinitionArn":    d.FlowDefinitionArn,
			"FlowDefinitionStatus": d.FlowDefinitionStatus,
			keyCreationTime:        epochSeconds(d.CreationTime),
		})
	}

	return listResp("FlowDefinitionSummaries", items, nextToken)
}

func (h *Handler) handleListHumanTaskUIs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	uis, nextToken := h.Backend.ListHumanTaskUIs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(uis))
	for _, u := range uis {
		items = append(items, map[string]any{
			"HumanTaskUiName": u.HumanTaskUIName,
			"HumanTaskUiArn":  u.HumanTaskUIArn,
			keyCreationTime:   epochSeconds(u.CreationTime),
		})
	}

	return listResp("HumanTaskUiSummaries", items, nextToken)
}

func (h *Handler) handleListAppImageConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListAppImageConfigs(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, map[string]any{
			"AppImageConfigName": c.AppImageConfigName,
			"AppImageConfigArn":  c.AppImageConfigArn,
			keyCreationTime:      epochSeconds(c.CreationTime),
			keyLastModifiedTime:  epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("AppImageConfigs", items, nextToken)
}

func (h *Handler) handleListTrainingJobsForHyperParameterTuningJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
		NextToken                   string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	jobs, _, err := h.Backend.ListTrainingJobsForHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	summaries := make([]any, 0, len(jobs))

	return json.Marshal(map[string]any{"TrainingJobSummaries": summaries})
}
