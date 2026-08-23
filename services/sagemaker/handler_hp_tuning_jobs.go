package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// HyperParameterTuningJob handlers
// ---------------------------------------------------------------------------

type hpResourceLimitsRequest struct {
	MaxParallelTrainingJobs int32 `json:"MaxParallelTrainingJobs"`
	MaxNumberOfTrainingJobs int32 `json:"MaxNumberOfTrainingJobs,omitempty"`
	MaxRuntimeInSeconds     int32 `json:"MaxRuntimeInSeconds,omitempty"`
}

// createHPTuningJobRequest mirrors CreateHyperParameterTuningJobInput
// (api_op_CreateHyperParameterTuningJob.go:44-77).
// HyperParameterTuningJobConfig is decoded as raw JSON (preserving the full
// object verbatim — HyperParameterTuningJobObjective/ParameterRanges/
// RandomSeed/StrategyConfig/TrainingJobEarlyStoppingType/
// TuningJobCompletionCriteria all round-trip through it unmodeled); Strategy/
// ResourceLimits are then decoded a second time from that same raw blob into
// their own typed fields, because this file's internal filter/sort/summary
// logic needs them. (A single JSON key cannot bind to two struct fields in
// one Unmarshal pass — encoding/json drops both silently on that ambiguity —
// hence the second decode.)
type createHPTuningJobRequest struct {
	HyperParameterTuningJobName   string          `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobConfig json.RawMessage `json:"HyperParameterTuningJobConfig"`
	Autotune                      json.RawMessage `json:"Autotune"`
	WarmStartConfig               json.RawMessage `json:"WarmStartConfig"`
	TrainingJobDefinition         json.RawMessage `json:"TrainingJobDefinition"`
	TrainingJobDefinitions        json.RawMessage `json:"TrainingJobDefinitions"`
	Tags                          []tagObject     `json:"Tags"`
}

// hpTuningJobConfigStrategyAndLimits mirrors the Strategy/ResourceLimits
// subset of HyperParameterTuningJobConfig (types/types.go:11052-11069).
type hpTuningJobConfigStrategyAndLimits struct {
	Strategy       string                  `json:"Strategy"`
	ResourceLimits hpResourceLimitsRequest `json:"ResourceLimits"`
}

func (h *Handler) handleCreateHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createHPTuningJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if len(req.HyperParameterTuningJobConfig) == 0 {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobConfig is required", errInvalidRequest)
	}

	var config hpTuningJobConfigStrategyAndLimits
	if err := json.Unmarshal(req.HyperParameterTuningJobConfig, &config); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	limits := HPResourceLimits{
		MaxParallelTrainingJobs: config.ResourceLimits.MaxParallelTrainingJobs,
		MaxNumberOfTrainingJobs: config.ResourceLimits.MaxNumberOfTrainingJobs,
		MaxRuntimeInSeconds:     config.ResourceLimits.MaxRuntimeInSeconds,
	}

	j, err := h.Backend.CreateHyperParameterTuningJob(ctx, CreateHyperParameterTuningJobOptions{
		Name:                          req.HyperParameterTuningJobName,
		Strategy:                      config.Strategy,
		Limits:                        limits,
		HyperParameterTuningJobConfig: req.HyperParameterTuningJobConfig,
		Autotune:                      req.Autotune,
		WarmStartConfig:               req.WarmStartConfig,
		TrainingJobDefinition:         req.TrainingJobDefinition,
		TrainingJobDefinitions:        req.TrainingJobDefinitions,
		Tags:                          fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created HP tuning job",
		"name",
		j.HyperParameterTuningJobName,
		"arn",
		j.HyperParameterTuningJobArn,
	)

	return json.Marshal(
		map[string]string{"HyperParameterTuningJobArn": j.HyperParameterTuningJobArn},
	)
}

// describeHPTuningJobInput mirrors DescribeHyperParameterTuningJobInput
// (api_op_DescribeHyperParameterTuningJob.go:29-34).
type describeHPTuningJobInput struct {
	HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
}

func (h *Handler) handleDescribeHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req describeHPTuningJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"HyperParameterTuningJobName":   j.HyperParameterTuningJobName,
		"HyperParameterTuningJobArn":    j.HyperParameterTuningJobArn,
		"HyperParameterTuningJobStatus": j.HyperParameterTuningJobStatus,
		"HyperParameterTuningJobConfig": j.HyperParameterTuningJobConfig,
		"ObjectiveStatusCounters":       j.ObjectiveStatusCounters,
		"TrainingJobStatusCounters":     j.TrainingJobStatusCounters,
		keyCreationTime:                 epochSeconds(j.CreationTime),
		keyLastModifiedTime:             epochSeconds(j.LastModifiedTime),
	}

	if len(j.Autotune) > 0 {
		resp["Autotune"] = j.Autotune
	}

	if len(j.WarmStartConfig) > 0 {
		resp["WarmStartConfig"] = j.WarmStartConfig
	}

	if len(j.TrainingJobDefinition) > 0 {
		resp["TrainingJobDefinition"] = j.TrainingJobDefinition
	}

	if len(j.TrainingJobDefinitions) > 0 {
		resp["TrainingJobDefinitions"] = j.TrainingJobDefinitions
	}

	return json.Marshal(resp)
}

type hpTuningJobSummary struct {
	HyperParameterTuningJobName   string                      `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string                      `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string                      `json:"HyperParameterTuningJobStatus"`
	Strategy                      string                      `json:"Strategy,omitempty"`
	ObjectiveStatusCounters       HPObjectiveStatusCounters   `json:"ObjectiveStatusCounters"`
	TrainingJobStatusCounters     HPTrainingJobStatusCounters `json:"TrainingJobStatusCounters"`
	ResourceLimits                HPResourceLimits            `json:"ResourceLimits"`
	CreationTime                  float64                     `json:"CreationTime"`
	LastModifiedTime              float64                     `json:"LastModifiedTime"`
}

// listHPTuningJobsInput mirrors ListHyperParameterTuningJobsInput
// (api_op_ListHyperParameterTuningJobs.go:30-64).
type listHPTuningJobsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	NameContains           string   `json:"NameContains"`
	StatusEquals           string   `json:"StatusEquals"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListHyperParameterTuningJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listHPTuningJobsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListHyperParameterTuningJobs(ctx, req.NextToken, ListHyperParameterTuningJobsFilter{
		NameContains:           req.NameContains,
		StatusEquals:           req.StatusEquals,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
	})
	summaries := make([]hpTuningJobSummary, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, hpTuningJobSummary{
			HyperParameterTuningJobName:   j.HyperParameterTuningJobName,
			HyperParameterTuningJobArn:    j.HyperParameterTuningJobArn,
			HyperParameterTuningJobStatus: j.HyperParameterTuningJobStatus,
			Strategy:                      j.Strategy,
			ResourceLimits:                j.ResourceLimits,
			ObjectiveStatusCounters:       j.ObjectiveStatusCounters,
			TrainingJobStatusCounters:     j.TrainingJobStatusCounters,
			CreationTime:                  epochSeconds(j.CreationTime),
			LastModifiedTime:              epochSeconds(j.LastModifiedTime),
		})
	}

	resp := map[string]any{"HyperParameterTuningJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// stopHPTuningJobInput mirrors StopHyperParameterTuningJobInput
// (api_op_StopHyperParameterTuningJob.go:27-32).
type stopHPTuningJobInput struct {
	HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
}

func (h *Handler) handleStopHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req stopHPTuningJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: stopped HP tuning job",
		"name",
		req.HyperParameterTuningJobName,
	)

	return nil
}

// deleteHPTuningJobInput mirrors DeleteHyperParameterTuningJobInput
// (api_op_DeleteHyperParameterTuningJob.go:27-32).
type deleteHPTuningJobInput struct {
	HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
}

func (h *Handler) handleDeleteHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req deleteHPTuningJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHyperParameterTuningJob(ctx, req.HyperParameterTuningJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: deleted HP tuning job",
		"name",
		req.HyperParameterTuningJobName,
	)

	return nil
}

type hyperParameterTrainingJobSummary struct {
	TrainingStartTime    *float64          `json:"TrainingStartTime,omitempty"`
	TrainingEndTime      *float64          `json:"TrainingEndTime,omitempty"`
	TunedHyperParameters map[string]string `json:"TunedHyperParameters,omitempty"`
	TrainingJobName      string            `json:"TrainingJobName"`
	TrainingJobArn       string            `json:"TrainingJobArn"`
	TuningJobName        string            `json:"TuningJobName,omitempty"`
	TrainingJobStatus    string            `json:"TrainingJobStatus"`
	FailureReason        string            `json:"FailureReason,omitempty"`
	CreationTime         float64           `json:"CreationTime"`
}

// listTrainingJobsForHPTuningJobInput mirrors
// ListTrainingJobsForHyperParameterTuningJobInput
// (api_op_ListTrainingJobsForHyperParameterTuningJob.go:27-56).
type listTrainingJobsForHPTuningJobInput struct {
	HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	NextToken                   string `json:"NextToken"`
	StatusEquals                string `json:"StatusEquals"`
	SortBy                      string `json:"SortBy"`
	SortOrder                   string `json:"SortOrder"`
	MaxResults                  int32  `json:"MaxResults"`
}

func (h *Handler) handleListTrainingJobsForHyperParameterTuningJob(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req listTrainingJobsForHPTuningJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	jobs, nextToken, err := h.Backend.ListTrainingJobsForHyperParameterTuningJob(
		ctx,
		req.HyperParameterTuningJobName,
		req.NextToken,
		ListTrainingJobsForHyperParameterTuningJobFilter{
			StatusEquals: req.StatusEquals,
			SortBy:       req.SortBy,
			SortOrder:    req.SortOrder,
			MaxResults:   req.MaxResults,
		},
	)
	if err != nil {
		return nil, err
	}

	summaries := make([]hyperParameterTrainingJobSummary, 0, len(jobs))
	for _, tj := range jobs {
		summaries = append(summaries, hyperParameterTrainingJobSummary{
			TrainingJobName:      tj.TrainingJobName,
			TrainingJobArn:       tj.TrainingJobArn,
			TuningJobName:        req.HyperParameterTuningJobName,
			CreationTime:         epochSeconds(tj.CreationTime),
			TrainingStartTime:    epochSecondsPtr(tj.TrainingStartTime),
			TrainingEndTime:      epochSecondsPtr(tj.TrainingEndTime),
			TrainingJobStatus:    tj.TrainingJobStatus,
			TunedHyperParameters: tj.HyperParameters,
			FailureReason:        tj.FailureReason,
		})
	}

	resp := map[string]any{"TrainingJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
