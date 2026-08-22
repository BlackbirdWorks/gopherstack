package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createAlgorithmRequest is the request body for CreateAlgorithm.
type createAlgorithmRequest struct {
	TrainingSpecification   json.RawMessage `json:"TrainingSpecification,omitempty"`
	InferenceSpecification  json.RawMessage `json:"InferenceSpecification,omitempty"`
	ValidationSpecification json.RawMessage `json:"ValidationSpecification,omitempty"`
	AlgorithmName           string          `json:"AlgorithmName"`
	AlgorithmDescription    string          `json:"AlgorithmDescription,omitempty"`
	Tags                    []tagObject     `json:"Tags"`
	CertifyForMarketplace   bool            `json:"CertifyForMarketplace,omitempty"`
}

func (h *Handler) handleCreateAlgorithm(ctx context.Context, body []byte) ([]byte, error) {
	var req createAlgorithmRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AlgorithmName == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", errInvalidRequest)
	}

	if len(req.TrainingSpecification) == 0 {
		return nil, fmt.Errorf("%w: TrainingSpecification is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	al, err := h.Backend.CreateAlgorithm(ctx, CreateAlgorithmOptions{
		AlgorithmName:           req.AlgorithmName,
		AlgorithmDescription:    req.AlgorithmDescription,
		TrainingSpecification:   req.TrainingSpecification,
		InferenceSpecification:  req.InferenceSpecification,
		ValidationSpecification: req.ValidationSpecification,
		CertifyForMarketplace:   req.CertifyForMarketplace,
		Tags:                    tags,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created algorithm",
		"name",
		al.AlgorithmName,
		"arn",
		al.AlgorithmArn,
	)

	return json.Marshal(map[string]string{"AlgorithmArn": al.AlgorithmArn})
}

// describeAlgorithmResponse is the response body for DescribeAlgorithm.
//
// AlgorithmStatusDetails/TrainingSpecification are both "This member is
// required" on the real DescribeAlgorithmOutput (api_op_DescribeAlgorithm.go)
// and so carry no omitempty — CreateAlgorithm now enforces TrainingSpecification
// non-empty, so this always has a real value to emit.
type describeAlgorithmResponse struct {
	AlgorithmArn            string                 `json:"AlgorithmArn"`
	AlgorithmName           string                 `json:"AlgorithmName"`
	AlgorithmStatus         string                 `json:"AlgorithmStatus"`
	AlgorithmDescription    string                 `json:"AlgorithmDescription,omitempty"`
	ProductID               string                 `json:"ProductId,omitempty"`
	AlgorithmStatusDetails  AlgorithmStatusDetails `json:"AlgorithmStatusDetails"`
	TrainingSpecification   json.RawMessage        `json:"TrainingSpecification"`
	InferenceSpecification  json.RawMessage        `json:"InferenceSpecification,omitempty"`
	ValidationSpecification json.RawMessage        `json:"ValidationSpecification,omitempty"`
	CreationTime            float64                `json:"CreationTime"`
	CertifyForMarketplace   bool                   `json:"CertifyForMarketplace,omitempty"`
}

// describeAlgorithmRequest is the request body for DescribeAlgorithm.
type describeAlgorithmRequest struct {
	AlgorithmName string `json:"AlgorithmName"`
}

func (h *Handler) handleDescribeAlgorithm(ctx context.Context, body []byte) ([]byte, error) {
	var req describeAlgorithmRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AlgorithmName == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", errInvalidRequest)
	}

	al, err := h.Backend.DescribeAlgorithm(ctx, req.AlgorithmName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describeAlgorithmResponse{
		AlgorithmArn:            al.AlgorithmArn,
		AlgorithmName:           al.AlgorithmName,
		AlgorithmStatus:         al.AlgorithmStatus,
		AlgorithmStatusDetails:  al.AlgorithmStatusDetails,
		AlgorithmDescription:    al.AlgorithmDescription,
		ProductID:               al.ProductID,
		CreationTime:            epochSeconds(al.CreationTime),
		CertifyForMarketplace:   al.CertifyForMarketplace,
		TrainingSpecification:   al.TrainingSpecification,
		InferenceSpecification:  al.InferenceSpecification,
		ValidationSpecification: al.ValidationSpecification,
	})
}

// deleteAlgorithmRequest is the request body for DeleteAlgorithm.
type deleteAlgorithmRequest struct {
	AlgorithmName string `json:"AlgorithmName"`
}

func (h *Handler) handleDeleteAlgorithm(ctx context.Context, body []byte) error {
	var req deleteAlgorithmRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AlgorithmName == "" {
		return fmt.Errorf("%w: AlgorithmName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAlgorithm(ctx, req.AlgorithmName)
}

func (h *Handler) handleListAlgorithms(ctx context.Context, body []byte) ([]byte, error) {
	var req nameTimeListRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	algorithms, nextToken := h.Backend.ListAlgorithms(ctx, req.NextToken, req.toFilter())

	items := make([]map[string]any, 0, len(algorithms))
	for _, al := range algorithms {
		entry := map[string]any{
			"AlgorithmArn":    al.AlgorithmArn,
			"AlgorithmName":   al.AlgorithmName,
			"AlgorithmStatus": al.AlgorithmStatus,
			keyCreationTime:   epochSeconds(al.CreationTime),
		}
		if al.AlgorithmDescription != "" {
			entry["AlgorithmDescription"] = al.AlgorithmDescription
		}

		items = append(items, entry)
	}

	return listResp("AlgorithmSummaryList", items, nextToken)
}
