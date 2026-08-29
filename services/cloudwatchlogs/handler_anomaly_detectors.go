package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

type createLogAnomalyDetectorInput struct {
	Tags                  map[string]string `json:"tags"`
	DetectorName          string            `json:"detectorName"`
	EvaluationFrequency   string            `json:"evaluationFrequency"`
	FilterPattern         string            `json:"filterPattern"`
	KmsKeyID              string            `json:"kmsKeyId"`
	LogGroupArnList       []string          `json:"logGroupArnList"`
	AnomalyVisibilityTime int64             `json:"anomalyVisibilityTime"`
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
	NextToken         string `json:"nextToken"`
	FilterLogGroupArn string `json:"filterLogGroupArn"`
	Limit             int    `json:"limit"`
}

type listLogAnomalyDetectorsOutput struct {
	NextToken        string               `json:"nextToken,omitempty"`
	AnomalyDetectors []LogAnomalyDetector `json:"anomalyDetectors"`
}

// --- UpdateLogAnomalyDetector ---.
// Enabled is a required field on the real API (aws-sdk-go-v2
// UpdateLogAnomalyDetectorInput.Enabled), used to pause/restart the detector.
type updateLogAnomalyDetectorInput struct {
	AnomalyDetectorArn    string `json:"anomalyDetectorArn"`
	EvaluationFrequency   string `json:"evaluationFrequency"`
	AnomalyVisibilityTime int64  `json:"anomalyVisibilityTime"`
	Enabled               bool   `json:"enabled"`
}

type updateLogAnomalyDetectorOutput struct{}

// --- GetLogAnomalyDetector ---.
type getLogAnomalyDetectorInput struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
}

// getLogAnomalyDetectorOutput's members sit flat at the top level of the
// response -- there is no "anomalyDetector" wrapper object (confirmed via
// deserializers.go's awsAwsjson11_deserializeOpDocumentGetLogAnomalyDetectorOutput,
// which switches directly on anomalyDetectorStatus/detectorName/etc., not a
// nested key). anomalyDetectorArn is deliberately absent too: it is not a
// member of the real GetLogAnomalyDetectorOutput type at all, only of its
// ListLogAnomalyDetectors sibling. A previous revision wrapped the response
// under "anomalyDetector" and echoed anomalyDetectorArn, so a real SDK
// client's GetLogAnomalyDetectorOutput fields were never populated.
type getLogAnomalyDetectorOutput struct {
	DetectorName          string   `json:"detectorName,omitempty"`
	AnomalyDetectorStatus string   `json:"anomalyDetectorStatus,omitempty"`
	EvaluationFrequency   string   `json:"evaluationFrequency,omitempty"`
	FilterPattern         string   `json:"filterPattern,omitempty"`
	KmsKeyID              string   `json:"kmsKeyId,omitempty"`
	LogGroupArnList       []string `json:"logGroupArnList"`
	AnomalyVisibilityTime int64    `json:"anomalyVisibilityTime,omitempty"`
	CreationTimeStamp     int64    `json:"creationTimeStamp"`
	LastModifiedTimeStamp int64    `json:"lastModifiedTimeStamp,omitempty"`
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
	PatternID          string `json:"patternId"`
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

	if len(input.Tags) > 0 {
		h.setTags(detectorArn, input.Tags)
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
	var filter []string
	if input.FilterLogGroupArn != "" {
		filter = []string{input.FilterLogGroupArn}
	}

	detectors, next, err := h.Backend.ListLogAnomalyDetectors(filter, input.Limit, input.NextToken)
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
		input.Enabled,
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

	return &getLogAnomalyDetectorOutput{
		DetectorName:          d.DetectorName,
		AnomalyDetectorStatus: d.AnomalyDetectorStatus,
		EvaluationFrequency:   d.EvaluationFrequency,
		FilterPattern:         d.FilterPattern,
		KmsKeyID:              d.KmsKeyID,
		LogGroupArnList:       d.LogGroupArnList,
		AnomalyVisibilityTime: d.AnomalyVisibilityTime,
		CreationTimeStamp:     d.CreationTimeStamp,
		LastModifiedTimeStamp: d.LastModifiedTimeStamp,
	}, nil
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
	if err := h.Backend.UpdateAnomaly(
		input.AnomalyID, input.AnomalyDetectorArn, input.SuppressionType, input.PatternID,
	); err != nil {
		return nil, err
	}

	return &updateAnomalyOutput{}, nil
}
