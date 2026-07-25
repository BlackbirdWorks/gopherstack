package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ModelPackage handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                    map[string]string `json:"Tags"`
		ModelPackageName        string            `json:"ModelPackageName"`
		ModelPackageGroupName   string            `json:"ModelPackageGroupName"`
		ModelPackageDescription string            `json:"ModelPackageDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackage(ctx,
		req.ModelPackageName, req.ModelPackageGroupName, req.ModelPackageDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageArn": result.ModelPackageArn})
}

func (h *Handler) handleDescribeModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackage(ctx, req.ModelPackageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackage(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageName string `json:"ModelPackageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackage(ctx, req.ModelPackageName)
}

func (h *Handler) handleListModelPackages(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
		NextToken             string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackages(ctx, req.ModelPackageGroupName, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, mp := range items {
		summaries = append(summaries, map[string]any{
			"ModelPackageName":   mp.ModelPackageName,
			"ModelPackageArn":    mp.ModelPackageArn,
			"ModelPackageStatus": mp.ModelPackageStatus,
			keyCreationTime:      epochSeconds(mp.CreationTime),
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

func (h *Handler) handleCreateModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		ModelPackageGroupName        string            `json:"ModelPackageGroupName"`
		ModelPackageGroupDescription string            `json:"ModelPackageGroupDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelPackageGroup(ctx,
		req.ModelPackageGroupName, req.ModelPackageGroupDescription, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ModelPackageGroupArn": result.ModelPackageGroupArn})
}

func (h *Handler) handleDescribeModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackageGroup(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteModelPackageGroup(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroup(ctx, req.ModelPackageGroupName)
}

func (h *Handler) handleListModelPackageGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListModelPackageGroups(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, g := range items {
		summaries = append(summaries, map[string]any{
			"ModelPackageGroupName":   g.ModelPackageGroupName,
			"ModelPackageGroupArn":    g.ModelPackageGroupArn,
			"ModelPackageGroupStatus": g.ModelPackageGroupStatus,
			keyCreationTime:           epochSeconds(g.CreationTime),
		})
	}

	return json.Marshal(map[string]any{
		"ModelPackageGroupSummaryList": summaries,
		keyNextToken:                   next,
	})
}

func (h *Handler) handleGetModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"ResourcePolicy": policy})
}

func (h *Handler) handlePutModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
		ResourcePolicy        string `json:"ResourcePolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	if req.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", errInvalidRequest)
	}

	g, err := h.Backend.PutModelPackageGroupPolicy(ctx, req.ModelPackageGroupName, req.ResourcePolicy)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyModelPackageGroupArn: g.ModelPackageGroupArn})
}

func (h *Handler) handleDeleteModelPackageGroupPolicy(ctx context.Context, body []byte) error {
	var req struct {
		ModelPackageGroupName string `json:"ModelPackageGroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
}

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

// batchDescribeModelPackageRequest is the request body for BatchDescribeModelPackage.
type batchDescribeModelPackageRequest struct {
	ModelPackageArnList []string `json:"ModelPackageArnList"`
}

// modelPackageSummary is a serializable model package for batch describe responses.
type modelPackageSummary struct {
	ModelPackageName      string  `json:"ModelPackageName"`
	ModelPackageArn       string  `json:"ModelPackageArn"`
	ModelPackageStatus    string  `json:"ModelPackageStatus"`
	ModelPackageGroupName string  `json:"ModelPackageGroupName,omitempty"`
	CreationTime          float64 `json:"CreationTime"`
}

// batchDescribeModelPackageError holds an error entry for BatchDescribeModelPackage.
type batchDescribeModelPackageError struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

func (h *Handler) handleBatchDescribeModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req batchDescribeModelPackageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	results := h.Backend.BatchDescribeModelPackage(ctx, req.ModelPackageArnList)

	modelPackageMap := make(map[string]modelPackageSummary)
	errorsMap := make(map[string]batchDescribeModelPackageError)

	for arnStr, result := range results {
		if result.ErrorCode != "" {
			errorsMap[arnStr] = batchDescribeModelPackageError{
				ErrorCode:    result.ErrorCode,
				ErrorMessage: result.ErrorMessage,
			}

			continue
		}

		mp := result.ModelPackage
		modelPackageMap[arnStr] = modelPackageSummary{
			ModelPackageName:      mp.ModelPackageName,
			ModelPackageArn:       mp.ModelPackageArn,
			ModelPackageStatus:    mp.ModelPackageStatus,
			ModelPackageGroupName: mp.ModelPackageGroupName,
			CreationTime:          epochSeconds(mp.CreationTime),
		}
	}

	return json.Marshal(map[string]any{
		"ModelPackageSummaries":             modelPackageMap,
		"BatchDescribeModelPackageErrorMap": errorsMap,
	})
}
