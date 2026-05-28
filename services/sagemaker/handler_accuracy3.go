package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// accuracy3OpsSupported returns the real stateful operations implemented in accuracy3.
func accuracy3OpsSupported() []string {
	return []string{
		// EdgePackagingJob
		"CreateEdgePackagingJob",
		"DescribeEdgePackagingJob",
		"StopEdgePackagingJob",
		"ListEdgePackagingJobs",
		// InferenceRecommendationsJob
		"CreateInferenceRecommendationsJob",
		"DescribeInferenceRecommendationsJob",
		"StopInferenceRecommendationsJob",
		"ListInferenceRecommendationsJobs",
		"ListInferenceRecommendationsJobSteps",
		// MLflow tracking server list + update
		"ListMlflowTrackingServers",
		"UpdateMlflowTrackingServer",
		// ModelCard lists
		"ListModelCards",
		"ListModelCardVersions",
		"ListModelCardExportJobs",
		// Updates
		"UpdateModelPackage",
		"UpdateSpace",
		"UpdateUserProfile",
		// Batch3 resource lists
		"ListOptimizationJobs",
		"ListStudioLifecycleConfigs",
		"ListInferenceExperiments",
		"ListFlowDefinitions",
		"ListHumanTaskUis",
		"ListAppImageConfigs",
		// HP tuning related
		"ListTrainingJobsForHyperParameterTuningJob",
	}
}

// dispatchAccuracy3Ops dispatches the accuracy3 real stateful operations.
func (h *Handler) dispatchAccuracy3Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// EdgePackagingJob
	case "CreateEdgePackagingJob":
		r, err := h.handleCreateEdgePackagingJob(ctx, body)

		return r, true, err
	case "DescribeEdgePackagingJob":
		r, err := h.handleDescribeEdgePackagingJob(body)

		return r, true, err
	case "StopEdgePackagingJob":
		return nil, true, h.handleStopEdgePackagingJob(body)
	case "ListEdgePackagingJobs":
		r, err := h.handleListEdgePackagingJobs(body)

		return r, true, err

	// InferenceRecommendationsJob
	case "CreateInferenceRecommendationsJob":
		r, err := h.handleCreateInferenceRecommendationsJob(ctx, body)

		return r, true, err
	case "DescribeInferenceRecommendationsJob":
		r, err := h.handleDescribeInferenceRecommendationsJob(body)

		return r, true, err
	case "StopInferenceRecommendationsJob":
		return nil, true, h.handleStopInferenceRecommendationsJob(body)
	case "ListInferenceRecommendationsJobs":
		r, err := h.handleListInferenceRecommendationsJobs(body)

		return r, true, err
	case "ListInferenceRecommendationsJobSteps":
		r, err := h.handleListInferenceRecommendationsJobSteps(body)

		return r, true, err

	// MLflow tracking server
	case "ListMlflowTrackingServers":
		r, err := h.handleListMlflowTrackingServers(body)

		return r, true, err
	case "UpdateMlflowTrackingServer":
		r, err := h.handleUpdateMlflowTrackingServer(ctx, body)

		return r, true, err

	// ModelCard lists
	case "ListModelCards":
		r, err := h.handleListModelCards(body)

		return r, true, err
	case "ListModelCardVersions":
		r, err := h.handleListModelCardVersions(body)

		return r, true, err
	case "ListModelCardExportJobs":
		r, err := h.handleListModelCardExportJobs(body)

		return r, true, err

	// Update operations
	case "UpdateModelPackage":
		r, err := h.handleUpdateModelPackage(ctx, body)

		return r, true, err
	case "UpdateSpace":
		r, err := h.handleUpdateSpace(ctx, body)

		return r, true, err
	case "UpdateUserProfile":
		r, err := h.handleUpdateUserProfile(ctx, body)

		return r, true, err

	// Batch3 resource lists
	case "ListOptimizationJobs":
		r, err := h.handleListOptimizationJobs(body)

		return r, true, err
	case "ListStudioLifecycleConfigs":
		r, err := h.handleListStudioLifecycleConfigs(body)

		return r, true, err
	case "ListInferenceExperiments":
		r, err := h.handleListInferenceExperiments(body)

		return r, true, err
	case "ListFlowDefinitions":
		r, err := h.handleListFlowDefinitions(body)

		return r, true, err
	case "ListHumanTaskUis":
		r, err := h.handleListHumanTaskUIs(body)

		return r, true, err
	case "ListAppImageConfigs":
		r, err := h.handleListAppImageConfigs(body)

		return r, true, err

	// HP tuning related
	case "ListTrainingJobsForHyperParameterTuningJob":
		r, err := h.handleListTrainingJobsForHyperParameterTuningJob(body)

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

	j, err := h.Backend.CreateEdgePackagingJob(CreateEdgePackagingJobOptions{
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

	_ = ctx

	return json.Marshal(map[string]string{"EdgePackagingJobArn": j.EdgePackagingJobArn})
}

func (h *Handler) handleDescribeEdgePackagingJob(body []byte) ([]byte, error) {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeEdgePackagingJob(req.EdgePackagingJobName)
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

func (h *Handler) handleStopEdgePackagingJob(body []byte) error {
	var req struct {
		EdgePackagingJobName string `json:"EdgePackagingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EdgePackagingJobName == "" {
		return fmt.Errorf("%w: EdgePackagingJobName is required", errInvalidRequest)
	}

	return h.Backend.StopEdgePackagingJob(req.EdgePackagingJobName)
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

func (h *Handler) handleListEdgePackagingJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals,omitempty"`
		NameContains string `json:"NameContains,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListEdgePackagingJobs(req.NextToken, ListEdgePackagingJobsFilter{
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

	j, err := h.Backend.CreateInferenceRecommendationsJob(CreateInferenceRecommendationsJobOptions{
		JobName:        req.JobName,
		JobType:        req.JobType,
		JobDescription: req.JobDescription,
		RoleArn:        req.RoleArn,
		Tags:           req.Tags,
	})
	if err != nil {
		return nil, err
	}

	_ = ctx

	return json.Marshal(map[string]string{"JobArn": j.JobArn})
}

func (h *Handler) handleDescribeInferenceRecommendationsJob(body []byte) ([]byte, error) {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeInferenceRecommendationsJob(req.JobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"JobName":          j.JobName,
		"JobArn":           j.JobArn,
		"Status":           j.Status,
		"InferenceRecommendations": []any{},
		keyCreationTime:    epochSeconds(j.CreationTime),
		keyLastModifiedTime: epochSeconds(j.LastModifiedTime),
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

func (h *Handler) handleStopInferenceRecommendationsJob(body []byte) error {
	var req struct {
		JobName string `json:"JobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobName == "" {
		return fmt.Errorf("%w: JobName is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceRecommendationsJob(req.JobName)
}

type inferenceRecommendationsJobSummary struct {
	JobName          string  `json:"JobName"`
	JobArn           string  `json:"JobArn"`
	Status           string  `json:"Status"`
	JobType          string  `json:"JobType,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListInferenceRecommendationsJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListInferenceRecommendationsJobs(req.NextToken)

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

func (h *Handler) handleListInferenceRecommendationsJobSteps(body []byte) ([]byte, error) {
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

	if _, err := h.Backend.DescribeInferenceRecommendationsJob(req.JobName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Steps": []any{}})
}

// ---------------------------------------------------------------------------
// MLflow tracking server handlers (list + update)
// ---------------------------------------------------------------------------

func (h *Handler) handleListMlflowTrackingServers(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	servers, nextToken := h.Backend.ListMlflowTrackingServers(req.NextToken)

	type trackingServerSummary struct {
		TrackingServerName   string  `json:"TrackingServerName"`
		TrackingServerArn    string  `json:"TrackingServerArn"`
		TrackingServerStatus string  `json:"TrackingServerStatus"`
		MlflowVersion        string  `json:"MlflowVersion,omitempty"`
		CreationTime         float64 `json:"CreationTime"`
		LastModifiedTime     float64 `json:"LastModifiedTime"`
	}

	summaries := make([]trackingServerSummary, 0, len(servers))
	for _, s := range servers {
		summaries = append(summaries, trackingServerSummary{
			TrackingServerName:   s.TrackingServerName,
			TrackingServerArn:    s.TrackingServerArn,
			TrackingServerStatus: s.TrackingServerStatus,
			MlflowVersion:        s.MlflowVersion,
			CreationTime:         epochSeconds(s.CreationTime),
			LastModifiedTime:     epochSeconds(s.LastModifiedTime),
		})
	}

	resp := map[string]any{"TrackingServerSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
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

	s, err := h.Backend.UpdateMlflowTrackingServer(req.TrackingServerName, req.MlflowVersion)
	if err != nil {
		return nil, err
	}

	_ = ctx

	return json.Marshal(map[string]string{keyTrackingServerArn: s.TrackingServerArn})
}

// ---------------------------------------------------------------------------
// ModelCard list handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListModelCards(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cards, nextToken := h.Backend.ListModelCards(req.NextToken)

	type modelCardSummary struct {
		ModelCardName    string  `json:"ModelCardName"`
		ModelCardArn     string  `json:"ModelCardArn"`
		ModelCardStatus  string  `json:"ModelCardStatus"`
		ModelCardVersion int     `json:"ModelCardVersion"`
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}

	summaries := make([]modelCardSummary, 0, len(cards))
	for _, c := range cards {
		summaries = append(summaries, modelCardSummary{
			ModelCardName:    c.ModelCardName,
			ModelCardArn:     c.ModelCardArn,
			ModelCardStatus:  c.ModelCardStatus,
			ModelCardVersion: c.ModelCardVersion,
			CreationTime:     epochSeconds(c.CreationTime),
			LastModifiedTime: epochSeconds(c.LastModifiedTime),
		})
	}

	resp := map[string]any{"ModelCardSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListModelCardVersions(body []byte) ([]byte, error) {
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

	card, err := h.Backend.DescribeModelCard(req.ModelCardName)
	if err != nil {
		return nil, err
	}

	type modelCardVersionSummary struct {
		ModelCardName    string  `json:"ModelCardName"`
		ModelCardArn     string  `json:"ModelCardArn"`
		ModelCardStatus  string  `json:"ModelCardStatus"`
		ModelCardVersion int     `json:"ModelCardVersion"`
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}

	summaries := []modelCardVersionSummary{
		{
			ModelCardName:    card.ModelCardName,
			ModelCardArn:     card.ModelCardArn,
			ModelCardStatus:  card.ModelCardStatus,
			ModelCardVersion: card.ModelCardVersion,
			CreationTime:     epochSeconds(card.CreationTime),
			LastModifiedTime: epochSeconds(card.LastModifiedTime),
		},
	}

	return json.Marshal(map[string]any{"ModelCardVersionSummaryList": summaries})
}

func (h *Handler) handleListModelCardExportJobs(body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		NextToken     string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName != "" {
		if _, err := h.Backend.DescribeModelCard(req.ModelCardName); err != nil {
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

	mp, err := h.Backend.UpdateModelPackage(req.ModelPackageName, req.ModelApprovalStatus)
	if err != nil {
		return nil, err
	}

	_ = ctx

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

	s, err := h.Backend.UpdateSpace(req.DomainID, req.SpaceName)
	if err != nil {
		return nil, err
	}

	_ = ctx

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

	up, err := h.Backend.UpdateUserProfile(req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	_ = ctx

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}

// ---------------------------------------------------------------------------
// Batch3 resource list handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListOptimizationJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListOptimizationJobs(req.NextToken)

	type optimizationJobSummary struct {
		OptimizationJobName   string  `json:"OptimizationJobName"`
		OptimizationJobArn    string  `json:"OptimizationJobArn"`
		OptimizationJobStatus string  `json:"OptimizationJobStatus"`
		CreationTime          float64 `json:"CreationTime"`
		LastModifiedTime      float64 `json:"LastModifiedTime"`
	}

	summaries := make([]optimizationJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, optimizationJobSummary{
			OptimizationJobName:   j.OptimizationJobName,
			OptimizationJobArn:    j.OptimizationJobArn,
			OptimizationJobStatus: j.OptimizationJobStatus,
			CreationTime:          epochSeconds(j.CreationTime),
			LastModifiedTime:      epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"OptimizationJobSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListStudioLifecycleConfigs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListStudioLifecycleConfigs(req.NextToken)

	type lifecycleConfigSummary struct {
		StudioLifecycleConfigName string  `json:"StudioLifecycleConfigName"`
		StudioLifecycleConfigArn  string  `json:"StudioLifecycleConfigArn"`
		CreationTime              float64 `json:"CreationTime"`
		LastModifiedTime          float64 `json:"LastModifiedTime"`
	}

	summaries := make([]lifecycleConfigSummary, 0, len(configs))
	for _, c := range configs {
		summaries = append(summaries, lifecycleConfigSummary{
			StudioLifecycleConfigName: c.StudioLifecycleConfigName,
			StudioLifecycleConfigArn:  c.StudioLifecycleConfigArn,
			CreationTime:              epochSeconds(c.CreationTime),
			LastModifiedTime:          epochSeconds(c.LastModifiedTime),
		})
	}

	resp := map[string]any{"StudioLifecycleConfigs": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListInferenceExperiments(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListInferenceExperiments(req.NextToken)

	type inferenceExperimentSummary struct {
		Name             string  `json:"Name"`
		Arn              string  `json:"Arn"`
		Status           string  `json:"Status"`
		Type             string  `json:"Type,omitempty"`
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}

	summaries := make([]inferenceExperimentSummary, 0, len(exps))
	for _, e := range exps {
		summaries = append(summaries, inferenceExperimentSummary{
			Name:             e.Name,
			Arn:              e.Arn,
			Status:           e.Status,
			Type:             e.Type,
			CreationTime:     epochSeconds(e.CreationTime),
			LastModifiedTime: epochSeconds(e.LastModifiedTime),
		})
	}

	resp := map[string]any{"InferenceExperiments": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListFlowDefinitions(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	defs, nextToken := h.Backend.ListFlowDefinitions(req.NextToken)

	type flowDefinitionSummary struct {
		FlowDefinitionName   string  `json:"FlowDefinitionName"`
		FlowDefinitionArn    string  `json:"FlowDefinitionArn"`
		FlowDefinitionStatus string  `json:"FlowDefinitionStatus"`
		CreationTime         float64 `json:"CreationTime"`
	}

	summaries := make([]flowDefinitionSummary, 0, len(defs))
	for _, d := range defs {
		summaries = append(summaries, flowDefinitionSummary{
			FlowDefinitionName:   d.FlowDefinitionName,
			FlowDefinitionArn:    d.FlowDefinitionArn,
			FlowDefinitionStatus: d.FlowDefinitionStatus,
			CreationTime:         epochSeconds(d.CreationTime),
		})
	}

	resp := map[string]any{"FlowDefinitionSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListHumanTaskUIs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	uis, nextToken := h.Backend.ListHumanTaskUIs(req.NextToken)

	type humanTaskUISummary struct {
		HumanTaskUiName string  `json:"HumanTaskUiName"`
		HumanTaskUiArn  string  `json:"HumanTaskUiArn"`
		CreationTime    float64 `json:"CreationTime"`
	}

	summaries := make([]humanTaskUISummary, 0, len(uis))
	for _, u := range uis {
		summaries = append(summaries, humanTaskUISummary{
			HumanTaskUiName: u.HumanTaskUIName,
			HumanTaskUiArn:  u.HumanTaskUIArn,
			CreationTime:    epochSeconds(u.CreationTime),
		})
	}

	resp := map[string]any{"HumanTaskUiSummaries": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListAppImageConfigs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListAppImageConfigs(req.NextToken)

	type appImageConfigSummary struct {
		AppImageConfigName string  `json:"AppImageConfigName"`
		AppImageConfigArn  string  `json:"AppImageConfigArn"`
		CreationTime       float64 `json:"CreationTime"`
		LastModifiedTime   float64 `json:"LastModifiedTime"`
	}

	summaries := make([]appImageConfigSummary, 0, len(configs))
	for _, c := range configs {
		summaries = append(summaries, appImageConfigSummary{
			AppImageConfigName: c.AppImageConfigName,
			AppImageConfigArn:  c.AppImageConfigArn,
			CreationTime:       epochSeconds(c.CreationTime),
			LastModifiedTime:   epochSeconds(c.LastModifiedTime),
		})
	}

	resp := map[string]any{"AppImageConfigs": summaries}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListTrainingJobsForHyperParameterTuningJob(body []byte) ([]byte, error) {
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

	jobs, _, err := h.Backend.ListTrainingJobsForHyperParameterTuningJob(req.HyperParameterTuningJobName, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]any, 0, len(jobs))

	return json.Marshal(map[string]any{"TrainingJobSummaries": summaries})
}
