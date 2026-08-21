package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Expanded CreateTrainingJob (full fields, gaps #4, #5, #6)
// ---------------------------------------------------------------------------

type createTrainingJobFullRequest struct {
	Environment            map[string]string `json:"Environment,omitempty"`
	VpcConfig              *VpcConfig        `json:"VpcConfig,omitempty"`
	CheckpointConfig       *CheckpointConfig `json:"CheckpointConfig,omitempty"`
	HyperParameters        map[string]string `json:"HyperParameters,omitempty"`
	OutputDataConfig       OutputDataConfig  `json:"OutputDataConfig"`
	TrainingJobName        string            `json:"TrainingJobName"`
	RoleArn                string            `json:"RoleArn"`
	AlgorithmSpecification struct {
		TrainingImage     string `json:"TrainingImage,omitempty"`
		AlgorithmName     string `json:"AlgorithmName,omitempty"`
		TrainingInputMode string `json:"TrainingInputMode,omitempty"`
		MetricDefinitions []struct {
			Name  string `json:"Name"`
			Regex string `json:"Regex,omitempty"`
		} `json:"MetricDefinitions,omitempty"`
		ContainerEntrypoint              []string `json:"ContainerEntrypoint,omitempty"`
		ContainerArguments               []string `json:"ContainerArguments,omitempty"`
		EnableSageMakerMetricsTimeSeries bool     `json:"EnableSageMakerMetricsTimeSeries,omitempty"`
	} `json:"AlgorithmSpecification"`
	Tags                                  []tagObject       `json:"Tags"`
	InputDataConfig                       []Channel         `json:"InputDataConfig,omitempty"`
	ResourceConfig                        ResourceConfig    `json:"ResourceConfig"`
	StoppingCondition                     StoppingCondition `json:"StoppingCondition"`
	EnableNetworkIsolation                bool              `json:"EnableNetworkIsolation,omitempty"`
	EnableManagedSpotTraining             bool              `json:"EnableManagedSpotTraining,omitempty"`
	EnableInterContainerTrafficEncryption bool              `json:"EnableInterContainerTrafficEncryption,omitempty"`
}

func (h *Handler) handleCreateTrainingJobFull(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrainingJobFullRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	metrics := make([]MetricDefinition, len(req.AlgorithmSpecification.MetricDefinitions))
	for i, md := range req.AlgorithmSpecification.MetricDefinitions {
		metrics[i] = MetricDefinition{Name: md.Name, Regex: md.Regex}
	}

	tj, err := h.Backend.CreateTrainingJobFull(ctx, TrainingJobOptions{
		TrainingJobName: req.TrainingJobName,
		RoleArn:         req.RoleArn,
		AlgorithmSpecification: AlgorithmSpecification{
			TrainingImage:                    req.AlgorithmSpecification.TrainingImage,
			AlgorithmName:                    req.AlgorithmSpecification.AlgorithmName,
			TrainingInputMode:                req.AlgorithmSpecification.TrainingInputMode,
			MetricDefinitions:                metrics,
			ContainerEntrypoint:              req.AlgorithmSpecification.ContainerEntrypoint,
			ContainerArguments:               req.AlgorithmSpecification.ContainerArguments,
			EnableSageMakerMetricsTimeSeries: req.AlgorithmSpecification.EnableSageMakerMetricsTimeSeries,
		},
		InputDataConfig:                       req.InputDataConfig,
		OutputDataConfig:                      req.OutputDataConfig,
		ResourceConfig:                        req.ResourceConfig,
		StoppingCondition:                     req.StoppingCondition,
		VpcConfig:                             req.VpcConfig,
		CheckpointConfig:                      req.CheckpointConfig,
		HyperParameters:                       req.HyperParameters,
		Environment:                           req.Environment,
		Tags:                                  fromTagObjects(req.Tags),
		EnableNetworkIsolation:                req.EnableNetworkIsolation,
		EnableManagedSpotTraining:             req.EnableManagedSpotTraining,
		EnableInterContainerTrafficEncryption: req.EnableInterContainerTrafficEncryption,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created training job (full)",
		"name",
		tj.TrainingJobName,
		"arn",
		tj.TrainingJobArn,
	)

	return json.Marshal(map[string]string{keyTrainingJobArn: tj.TrainingJobArn})
}

// describeTrainingJobInput mirrors DescribeTrainingJobInput (api_op_DescribeTrainingJob.go:29-36).
type describeTrainingJobInput struct {
	TrainingJobName string `json:"TrainingJobName"`
}

func (h *Handler) handleDescribeTrainingJobFull(ctx context.Context, body []byte) ([]byte, error) {
	var req describeTrainingJobInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	tj, err := h.Backend.DescribeTrainingJob(ctx, req.TrainingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyTrainingJobName:       tj.TrainingJobName,
		keyTrainingJobArn:        tj.TrainingJobArn,
		keyTrainingJobStatus:     tj.TrainingJobStatus,
		"SecondaryStatus":        tj.SecondaryStatus,
		keyRoleArn:               tj.RoleArn,
		"AlgorithmSpecification": tj.AlgorithmSpecification,
		"ResourceConfig":         tj.ResourceConfig,
		"StoppingCondition":      tj.StoppingCondition,
		keyCreationTime:          epochSeconds(tj.CreationTime),
		keyLastModifiedTime:      epochSeconds(tj.LastModifiedTime),
	}
	addTrainingJobOptionalFields(resp, tj)

	return json.Marshal(resp)
}

func addTrainingJobOptionalFields(resp map[string]any, tj *TrainingJob) {
	if len(tj.InputDataConfig) > 0 {
		resp["InputDataConfig"] = tj.InputDataConfig
	}
	if tj.OutputDataConfig.S3OutputPath != "" {
		resp["OutputDataConfig"] = tj.OutputDataConfig
	}
	if tj.VpcConfig != nil {
		resp["VpcConfig"] = tj.VpcConfig
	}
	if tj.CheckpointConfig != nil {
		resp["CheckpointConfig"] = tj.CheckpointConfig
	}
	if len(tj.HyperParameters) > 0 {
		resp["HyperParameters"] = tj.HyperParameters
	}
	if len(tj.Environment) > 0 {
		resp["Environment"] = tj.Environment
	}
	if tj.ModelArtifacts != nil {
		resp["ModelArtifacts"] = tj.ModelArtifacts
	}
	if tj.TrainingStartTime != nil {
		resp["TrainingStartTime"] = epochSeconds(*tj.TrainingStartTime)
	}
	if tj.TrainingEndTime != nil {
		resp["TrainingEndTime"] = epochSeconds(*tj.TrainingEndTime)
	}
	if tj.BillableTimeInSeconds > 0 {
		resp["BillableTimeInSeconds"] = tj.BillableTimeInSeconds
	}
	if tj.TrainingTimeInSeconds > 0 {
		resp["TrainingTimeInSeconds"] = tj.TrainingTimeInSeconds
	}
	if len(tj.SecondaryStatusTransitions) > 0 {
		resp["SecondaryStatusTransitions"] = buildSecondaryStatusTransitions(
			tj.SecondaryStatusTransitions,
		)
	}
	if tj.FailureReason != "" {
		resp["FailureReason"] = tj.FailureReason
	}
}

func buildSecondaryStatusTransitions(transitions []SecondaryStatusTransition) []map[string]any {
	result := make([]map[string]any, len(transitions))
	for i, t := range transitions {
		entry := map[string]any{
			"Status":        t.Status,
			"StartTime":     epochSeconds(t.StartTime),
			"StatusMessage": t.StatusMessage,
		}
		if t.EndTime != nil {
			entry["EndTime"] = epochSeconds(*t.EndTime)
		}
		result[i] = entry
	}

	return result
}

// stopTrainingJobInput mirrors StopTrainingJobInput (api_op_StopTrainingJob.go:27-34).
type stopTrainingJobInput struct {
	TrainingJobName string `json:"TrainingJobName"`
}

func (h *Handler) handleStopTrainingJobFSM(ctx context.Context, body []byte) error {
	var req stopTrainingJobInput
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopTrainingJobFSM(ctx, req.TrainingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped training job (FSM)", "name", req.TrainingJobName)

	return nil
}

// listTrainingJobsInput mirrors ListTrainingJobsInput (api_op_ListTrainingJobs.go:33-84).
type listTrainingJobsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	NameContains           string   `json:"NameContains,omitempty"`
	StatusEquals           string   `json:"StatusEquals,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	TrainingPlanArnEquals  string   `json:"TrainingPlanArnEquals,omitempty"`
	WarmPoolStatusEquals   string   `json:"WarmPoolStatusEquals,omitempty"`
	MaxResults             int32    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListTrainingJobsFiltered(ctx context.Context, body []byte) ([]byte, error) {
	var req listTrainingJobsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListTrainingJobsFiltered(ctx, req.NextToken, ListTrainingJobsFilter{
		StatusEquals:           req.StatusEquals,
		NameContains:           req.NameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		TrainingPlanArnEquals:  req.TrainingPlanArnEquals,
		WarmPoolStatusEquals:   req.WarmPoolStatusEquals,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})

	summaries := make([]trainingJobSummary, 0, len(jobs))
	for _, tj := range jobs {
		summaries = append(summaries, trainingJobSummary{
			TrainingJobName:   tj.TrainingJobName,
			TrainingJobArn:    tj.TrainingJobArn,
			TrainingJobStatus: tj.TrainingJobStatus,
			CreationTime:      epochSeconds(tj.CreationTime),
			LastModifiedTime:  epochSeconds(tj.LastModifiedTime),
		})
	}

	resp := map[string]any{keyTrainingJobSummaries: summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// TrainingJob handlers
// ---------------------------------------------------------------------------

type trainingJobSummary struct {
	TrainingJobName   string  `json:"TrainingJobName"`
	TrainingJobArn    string  `json:"TrainingJobArn"`
	TrainingJobStatus string  `json:"TrainingJobStatus"`
	CreationTime      float64 `json:"CreationTime"`
	LastModifiedTime  float64 `json:"LastModifiedTime"`
}

// deleteTrainingJobInput mirrors DeleteTrainingJobInput (api_op_DeleteTrainingJob.go:34-42).
type deleteTrainingJobInput struct {
	TrainingJobName string `json:"TrainingJobName"`
}

func (h *Handler) handleDeleteTrainingJob(ctx context.Context, body []byte) error {
	var req deleteTrainingJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTrainingJob(ctx, req.TrainingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted training job", "name", req.TrainingJobName)

	return nil
}

// resourceConfigForUpdate mirrors types.ResourceConfigForUpdate
// (types/types.go:19848-19856) — KeepAlivePeriodInSeconds is the sole member
// and is itself required whenever ResourceConfig is present at all.
type resourceConfigForUpdate struct {
	KeepAlivePeriodInSeconds int32 `json:"KeepAlivePeriodInSeconds"`
}

// updateTrainingJobInput mirrors UpdateTrainingJobInput
// (api_op_UpdateTrainingJob.go:29-56). ProfilerConfig/ProfilerRuleConfigurations/
// RemoteDebugConfig are decoded here for wire-shape fidelity only — see
// UpdateTrainingJobOptions' doc comment for why this backend cannot apply
// them (no profiler/remote-debug concept exists on TrainingJob at all, so
// there is nothing to mutate; this is a disclosed depth limit, not a
// silently-ignored field, and no client-visible response ever claims these
// were applied).
type updateTrainingJobInput struct {
	ResourceConfig             *resourceConfigForUpdate `json:"ResourceConfig"`
	TrainingJobName            string                   `json:"TrainingJobName"`
	ProfilerConfig             json.RawMessage          `json:"ProfilerConfig"`
	ProfilerRuleConfigurations json.RawMessage          `json:"ProfilerRuleConfigurations"`
	RemoteDebugConfig          json.RawMessage          `json:"RemoteDebugConfig"`
}

func (h *Handler) handleUpdateTrainingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req updateTrainingJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	opts := UpdateTrainingJobOptions{}
	if req.ResourceConfig != nil {
		opts.KeepAlivePeriodInSeconds = &req.ResourceConfig.KeepAlivePeriodInSeconds
	}

	tj, err := h.Backend.UpdateTrainingJob(ctx, req.TrainingJobName, opts)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated training job", "name", req.TrainingJobName)

	return json.Marshal(map[string]string{keyTrainingJobArn: tj.TrainingJobArn})
}
