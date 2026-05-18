package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	keyEndpointArn      = "EndpointArn"
	keyTrainingJobArn   = "TrainingJobArn"
	keyCreationTime     = "CreationTime"
	keyLastModifiedTime = "LastModifiedTime"
)

// ---------------------------------------------------------------------------
// Endpoint handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeEndpoint(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName string `json:"EndpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.DescribeEndpoint(req.EndpointName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"EndpointName":       ep.EndpointName,
		keyEndpointArn:       ep.EndpointArn,
		"EndpointConfigName": ep.EndpointConfigName,
		"EndpointStatus":     ep.EndpointStatus,
		keyCreationTime:      epochSeconds(ep.CreationTime),
		keyLastModifiedTime:  epochSeconds(ep.LastModifiedTime),
	}
	if ep.FailureReason != "" {
		resp["FailureReason"] = ep.FailureReason
	}
	if len(ep.ProductionVariants) > 0 {
		resp["ProductionVariants"] = ep.ProductionVariants
	}

	return json.Marshal(resp)
}

type endpointSummary struct {
	EndpointName     string  `json:"EndpointName"`
	EndpointArn      string  `json:"EndpointArn"`
	EndpointStatus   string  `json:"EndpointStatus"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListEndpoints(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	eps, nextToken := h.Backend.ListEndpoints(req.NextToken)
	summaries := make([]endpointSummary, 0, len(eps))

	for _, ep := range eps {
		summaries = append(summaries, endpointSummary{
			EndpointName:     ep.EndpointName,
			EndpointArn:      ep.EndpointArn,
			EndpointStatus:   ep.EndpointStatus,
			CreationTime:     epochSeconds(ep.CreationTime),
			LastModifiedTime: epochSeconds(ep.LastModifiedTime),
		})
	}

	resp := map[string]any{"Endpoints": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteEndpoint(ctx context.Context, body []byte) error {
	var req struct {
		EndpointName string `json:"EndpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpoint(req.EndpointName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint", "name", req.EndpointName)

	return nil
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

func (h *Handler) handleDeleteTrainingJob(ctx context.Context, body []byte) error {
	var req struct {
		TrainingJobName string `json:"TrainingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTrainingJob(req.TrainingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted training job", "name", req.TrainingJobName)

	return nil
}

func (h *Handler) handleUpdateTrainingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingJobName string `json:"TrainingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	tj, err := h.Backend.DescribeTrainingJob(req.TrainingJobName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated training job", "name", req.TrainingJobName)

	return json.Marshal(map[string]string{keyTrainingJobArn: tj.TrainingJobArn})
}

// ---------------------------------------------------------------------------
// NotebookInstance handlers
// ---------------------------------------------------------------------------

type notebookSummary struct {
	NotebookInstanceName   string  `json:"NotebookInstanceName"`
	NotebookInstanceArn    string  `json:"NotebookInstanceArn"`
	NotebookInstanceStatus string  `json:"NotebookInstanceStatus"`
	InstanceType           string  `json:"InstanceType,omitempty"`
	CreationTime           float64 `json:"CreationTime"`
	LastModifiedTime       float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListNotebookInstances(body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals"`
		NameContains string `json:"NameContains"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	nbs, nextToken := h.Backend.ListNotebookInstances(req.NextToken, ListNotebookInstancesFilter{
		StatusEquals: req.StatusEquals,
		NameContains: req.NameContains,
	})
	summaries := make([]notebookSummary, 0, len(nbs))

	for _, nb := range nbs {
		summaries = append(summaries, notebookSummary{
			NotebookInstanceName:   nb.NotebookInstanceName,
			NotebookInstanceArn:    nb.NotebookInstanceArn,
			NotebookInstanceStatus: nb.NotebookInstanceStatus,
			InstanceType:           nb.InstanceType,
			CreationTime:           epochSeconds(nb.CreationTime),
			LastModifiedTime:       epochSeconds(nb.LastModifiedTime),
		})
	}

	resp := map[string]any{"NotebookInstances": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteNotebookInstance(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteNotebookInstance(req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted notebook instance", "name", req.NotebookInstanceName)

	return nil
}

func (h *Handler) handleStartNotebookInstance(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StartNotebookInstance(req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: started notebook instance", "name", req.NotebookInstanceName)

	return nil
}

func (h *Handler) handleStopNotebookInstance(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StopNotebookInstance(req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped notebook instance", "name", req.NotebookInstanceName)

	return nil
}

func (h *Handler) handleCreatePresignedNotebookInstanceURL(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedNotebookInstanceURL(req.NotebookInstanceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"AuthorizedUrl": url})
}

// ---------------------------------------------------------------------------
// HyperParameterTuningJob handlers
// ---------------------------------------------------------------------------

type createHPTuningJobRequest struct {
	HyperParameterTuningJobConfig struct {
		Strategy string `json:"Strategy"`
	} `json:"HyperParameterTuningJobConfig"`
	HyperParameterTuningJobName string      `json:"HyperParameterTuningJobName"`
	Tags                        []tagObject `json:"Tags"`
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

	tags := fromTagObjects(req.Tags)

	j, err := h.Backend.CreateHyperParameterTuningJob(
		req.HyperParameterTuningJobName,
		req.HyperParameterTuningJobConfig.Strategy,
		tags,
	)
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

func (h *Handler) handleDescribeHyperParameterTuningJob(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return nil, fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	j, err := h.Backend.DescribeHyperParameterTuningJob(req.HyperParameterTuningJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"HyperParameterTuningJobName":   j.HyperParameterTuningJobName,
		"HyperParameterTuningJobArn":    j.HyperParameterTuningJobArn,
		"HyperParameterTuningJobStatus": j.HyperParameterTuningJobStatus,
		"Strategy":                      j.Strategy,
		keyCreationTime:                 epochSeconds(j.CreationTime),
		keyLastModifiedTime:             epochSeconds(j.LastModifiedTime),
	})
}

type hpTuningJobSummary struct {
	HyperParameterTuningJobName   string  `json:"HyperParameterTuningJobName"`
	HyperParameterTuningJobArn    string  `json:"HyperParameterTuningJobArn"`
	HyperParameterTuningJobStatus string  `json:"HyperParameterTuningJobStatus"`
	Strategy                      string  `json:"Strategy,omitempty"`
	CreationTime                  float64 `json:"CreationTime"`
	LastModifiedTime              float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListHyperParameterTuningJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListHyperParameterTuningJobs(req.NextToken)
	summaries := make([]hpTuningJobSummary, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, hpTuningJobSummary{
			HyperParameterTuningJobName:   j.HyperParameterTuningJobName,
			HyperParameterTuningJobArn:    j.HyperParameterTuningJobArn,
			HyperParameterTuningJobStatus: j.HyperParameterTuningJobStatus,
			Strategy:                      j.Strategy,
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

func (h *Handler) handleStopHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopHyperParameterTuningJob(req.HyperParameterTuningJobName); err != nil {
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

func (h *Handler) handleDeleteHyperParameterTuningJob(ctx context.Context, body []byte) error {
	var req struct {
		HyperParameterTuningJobName string `json:"HyperParameterTuningJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HyperParameterTuningJobName == "" {
		return fmt.Errorf("%w: HyperParameterTuningJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHyperParameterTuningJob(req.HyperParameterTuningJobName); err != nil {
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
