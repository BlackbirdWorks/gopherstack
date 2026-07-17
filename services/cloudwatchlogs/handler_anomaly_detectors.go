package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type createLogAnomalyDetectorInput struct {
	DetectorName          string   `json:"detectorName"`
	EvaluationFrequency   string   `json:"evaluationFrequency"`
	FilterPattern         string   `json:"filterPattern"`
	KmsKeyID              string   `json:"kmsKeyId"`
	LogGroupArnList       []string `json:"logGroupArnList"`
	AnomalyVisibilityTime int64    `json:"anomalyVisibilityTime"`
}

type createLogAnomalyDetectorOutput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn,omitempty"`
}

// --- DeleteLogAnomalyDetector ---.
type deleteLogAnomalyDetectorInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
}

type deleteLogAnomalyDetectorOutput struct{}

// --- ListLogAnomalyDetectors ---.
type listLogAnomalyDetectorsInput struct {
	NextToken             string   `json:"nextToken"`
	FilterLogGroupArnList []string `json:"filterLogGroupArnList"`
	Limit                 int      `json:"limit"`
}

type listLogAnomalyDetectorsOutput struct {
	NextToken        string               `json:"nextToken,omitempty"`
	AnomalyDetectors []LogAnomalyDetector `json:"anomalyDetectors"`
}

// --- UpdateLogAnomalyDetector ---.
type updateLogAnomalyDetectorInput struct {
	AnomalyDetectorArn    string `json:"anomalyDetectorArn"`
	EvaluationFrequency   string `json:"evaluationFrequency"`
	AnomalyVisibilityTime int64  `json:"anomalyVisibilityTime"`
}

type updateLogAnomalyDetectorOutput struct{}

// --- GetLogAnomalyDetector ---.
type getLogAnomalyDetectorInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
}

type getLogAnomalyDetectorOutput struct {
	AnomalyDetector *LogAnomalyDetector `json:"anomalyDetector,omitempty"`
}

// --- ListAnomalies ---.
type listAnomaliesInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

type listAnomaliesOutput struct {
	NextToken string    `json:"nextToken,omitempty"`
	Anomalies []Anomaly `json:"anomalies"`
}

// --- UpdateAnomaly ---.
type updateAnomalyInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
	AnomalyID          string `json:"anomalyId"`
	SuppressionType    string `json:"suppressionType"`
}

type updateAnomalyOutput struct{}

func (h *Handler) handleCreateLogAnomalyDetector(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input createLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	detectorArn, err := h.Backend.CreateLogAnomalyDetector(
		input.LogGroupArnList, input.DetectorName, input.EvaluationFrequency,
		input.FilterPattern, input.KmsKeyID, input.AnomalyVisibilityTime,
	)
	if err != nil {
		return nil, err
	}

	return &createLogAnomalyDetectorOutput{AnomalyDetectorArn: detectorArn}, nil
}

func (h *Handler) handleDeleteLogAnomalyDetector(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input deleteLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteLogAnomalyDetector(input.AnomalyDetectorArn); err != nil {
		return nil, err
	}

	return &deleteLogAnomalyDetectorOutput{}, nil
}

func (h *Handler) handleListLogAnomalyDetectors(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input listLogAnomalyDetectorsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	detectors, next, err := h.Backend.ListLogAnomalyDetectors(input.FilterLogGroupArnList, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &listLogAnomalyDetectorsOutput{AnomalyDetectors: detectors, NextToken: next}, nil
}

func (h *Handler) handleUpdateLogAnomalyDetector(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input updateLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateLogAnomalyDetector(
		input.AnomalyDetectorArn,
		input.EvaluationFrequency,
		input.AnomalyVisibilityTime,
	); err != nil {
		return nil, err
	}

	return &updateLogAnomalyDetectorOutput{}, nil
}

func (h *Handler) handleGetLogAnomalyDetector(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input getLogAnomalyDetectorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	d, err := h.Backend.GetLogAnomalyDetector(input.AnomalyDetectorArn)
	if err != nil {
		return nil, err
	}

	return &getLogAnomalyDetectorOutput{AnomalyDetector: d}, nil
}

func (h *Handler) handleListAnomalies(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input listAnomaliesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	anomalies, next, err := h.Backend.ListAnomalies(input.AnomalyDetectorArn, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &listAnomaliesOutput{Anomalies: anomalies, NextToken: next}, nil
}

func (h *Handler) handleUpdateAnomaly(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input updateAnomalyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.UpdateAnomaly(input.AnomalyID, input.AnomalyDetectorArn, input.SuppressionType); err != nil {
		return nil, err
	}

	return &updateAnomalyOutput{}, nil
}
