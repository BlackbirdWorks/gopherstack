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

type createEndpointRequest struct {
	EndpointName       string      `json:"EndpointName"`
	EndpointConfigName string      `json:"EndpointConfigName"`
	Tags               []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateEndpoint(ctx context.Context, body []byte) ([]byte, error) {
	var req createEndpointRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	ep, err := h.Backend.CreateEndpoint(req.EndpointName, req.EndpointConfigName, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint",
		"name",
		ep.EndpointName,
		"arn",
		ep.EndpointArn,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

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

	return json.Marshal(map[string]any{
		"EndpointName":       ep.EndpointName,
		keyEndpointArn:       ep.EndpointArn,
		"EndpointConfigName": ep.EndpointConfigName,
		"EndpointStatus":     ep.EndpointStatus,
		keyCreationTime:      epochSeconds(ep.CreationTime),
		keyLastModifiedTime:  epochSeconds(ep.LastModifiedTime),
	})
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

func (h *Handler) handleUpdateEndpoint(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName       string `json:"EndpointName"`
		EndpointConfigName string `json:"EndpointConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpoint(req.EndpointName, req.EndpointConfigName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated endpoint", "name", ep.EndpointName)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

func (h *Handler) handleUpdateEndpointWeightsAndCapacities(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
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

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: updated endpoint weights and capacities",
		"name",
		req.EndpointName,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// TrainingJob handlers
// ---------------------------------------------------------------------------

type algorithmSpecRequest struct {
	TrainingImage     string `json:"TrainingImage,omitempty"`
	AlgorithmName     string `json:"AlgorithmName,omitempty"`
	TrainingInputMode string `json:"TrainingInputMode,omitempty"`
}

type createTrainingJobRequest struct {
	AlgorithmSpecification algorithmSpecRequest `json:"AlgorithmSpecification"`
	TrainingJobName        string               `json:"TrainingJobName"`
	RoleArn                string               `json:"RoleArn"`
	Tags                   []tagObject          `json:"Tags"`
}

func (h *Handler) handleCreateTrainingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrainingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	algSpec := map[string]string{
		"TrainingImage":     req.AlgorithmSpecification.TrainingImage,
		"AlgorithmName":     req.AlgorithmSpecification.AlgorithmName,
		"TrainingInputMode": req.AlgorithmSpecification.TrainingInputMode,
	}

	tj, err := h.Backend.CreateTrainingJob(req.TrainingJobName, req.RoleArn, algSpec, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created training job",
		"name",
		tj.TrainingJobName,
		"arn",
		tj.TrainingJobArn,
	)

	return json.Marshal(map[string]string{keyTrainingJobArn: tj.TrainingJobArn})
}

func (h *Handler) handleDescribeTrainingJob(_ context.Context, body []byte) ([]byte, error) {
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

	return json.Marshal(map[string]any{
		"TrainingJobName":   tj.TrainingJobName,
		keyTrainingJobArn:   tj.TrainingJobArn,
		"TrainingJobStatus": tj.TrainingJobStatus,
		keyRoleArn:          tj.RoleArn,
		keyCreationTime:     epochSeconds(tj.CreationTime),
		keyLastModifiedTime: epochSeconds(tj.LastModifiedTime),
	})
}

type trainingJobSummary struct {
	TrainingJobName   string  `json:"TrainingJobName"`
	TrainingJobArn    string  `json:"TrainingJobArn"`
	TrainingJobStatus string  `json:"TrainingJobStatus"`
	CreationTime      float64 `json:"CreationTime"`
	LastModifiedTime  float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListTrainingJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListTrainingJobs(req.NextToken)
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

func (h *Handler) handleStopTrainingJob(ctx context.Context, body []byte) error {
	var req struct {
		TrainingJobName string `json:"TrainingJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingJobName == "" {
		return fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopTrainingJob(req.TrainingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped training job", "name", req.TrainingJobName)

	return nil
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

type createNotebookInstanceRequest struct {
	NotebookInstanceName string      `json:"NotebookInstanceName"`
	InstanceType         string      `json:"InstanceType"`
	RoleArn              string      `json:"RoleArn"`
	Tags                 []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateNotebookInstance(ctx context.Context, body []byte) ([]byte, error) {
	var req createNotebookInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	nb, err := h.Backend.CreateNotebookInstance(
		req.NotebookInstanceName,
		req.InstanceType,
		req.RoleArn,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created notebook instance",
		"name",
		nb.NotebookInstanceName,
		"arn",
		nb.NotebookInstanceArn,
	)

	return json.Marshal(map[string]string{"NotebookInstanceArn": nb.NotebookInstanceArn})
}

func (h *Handler) handleDescribeNotebookInstance(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	nb, err := h.Backend.DescribeNotebookInstance(req.NotebookInstanceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"NotebookInstanceName":   nb.NotebookInstanceName,
		"NotebookInstanceArn":    nb.NotebookInstanceArn,
		"NotebookInstanceStatus": nb.NotebookInstanceStatus,
		"InstanceType":           nb.InstanceType,
		keyRoleArn:               nb.RoleArn,
		keyCreationTime:          epochSeconds(nb.CreationTime),
		keyLastModifiedTime:      epochSeconds(nb.LastModifiedTime),
	})
}

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

func (h *Handler) handleUpdateNotebookInstance(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
		InstanceType         string `json:"InstanceType,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateNotebookInstance(req.NotebookInstanceName, req.InstanceType); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated notebook instance", "name", req.NotebookInstanceName)

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
