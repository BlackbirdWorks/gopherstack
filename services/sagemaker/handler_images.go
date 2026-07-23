package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Image handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ImageName   string            `json:"ImageName"`
		Description string            `json:"Description"`
		RoleArn     string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImage(ctx, req.ImageName, req.Description, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageArn": result.ImageArn})
}

func (h *Handler) handleDescribeImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImage(ctx, req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImage(ctx context.Context, body []byte) error {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImage(ctx, req.ImageName)
}

func (h *Handler) handleListImages(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListImages(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, img := range items {
		summaries = append(summaries, map[string]any{
			"ImageName":         img.ImageName,
			"ImageArn":          img.ImageArn,
			"ImageStatus":       img.ImageStatus,
			keyCreationTime:     epochSeconds(img.CreationTime),
			keyLastModifiedTime: epochSeconds(img.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"Images":     summaries,
		keyNextToken: next,
	})
}

func (h *Handler) handleUpdateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName        string   `json:"ImageName"`
		Description      *string  `json:"Description"`
		DisplayName      *string  `json:"DisplayName"`
		RoleArn          *string  `json:"RoleArn"`
		DeleteProperties []string `json:"DeleteProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateImage(ctx, req.ImageName, UpdateImageOptions{
		Description:      req.Description,
		DisplayName:      req.DisplayName,
		RoleArn:          req.RoleArn,
		DeleteProperties: req.DeleteProperties,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyImageArn: result.ImageArn})
}

// ---------------------------------------------------------------------------
// ImageVersion handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImageVersion(ctx, req.ImageName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageVersionArn": result.ImageVersionArn})
}

func (h *Handler) handleDescribeImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		Version   int    `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImageVersion(ctx, req.ImageName, req.Version)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteImageVersion(ctx context.Context, body []byte) error {
	var req struct {
		ImageName string `json:"ImageName"`
		Version   int    `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImageVersion(ctx, req.ImageName, req.Version)
}

func (h *Handler) handleListImageVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	items, next := h.Backend.ListImageVersions(ctx, req.ImageName, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, iv := range items {
		summaries = append(summaries, map[string]any{
			"ImageVersionArn":    iv.ImageVersionArn,
			"ImageVersionStatus": iv.ImageVersionStatus,
			"Version":            iv.Version,
			keyCreationTime:      epochSeconds(iv.CreationTime),
			keyLastModifiedTime:  epochSeconds(iv.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{
		"ImageVersions": summaries,
		keyNextToken:    next,
	})
}

func (h *Handler) handleUpdateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Horovod         *bool    `json:"Horovod"`
		ImageName       string   `json:"ImageName"`
		JobType         string   `json:"JobType"`
		MLFramework     string   `json:"MLFramework"`
		Processor       string   `json:"Processor"`
		ProgrammingLang string   `json:"ProgrammingLang"`
		ReleaseNotes    string   `json:"ReleaseNotes"`
		VendorGuidance  string   `json:"VendorGuidance"`
		AliasesToAdd    []string `json:"AliasesToAdd"`
		AliasesToDelete []string `json:"AliasesToDelete"`
		Version         int      `json:"Version"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateImageVersion(ctx, req.ImageName, req.Version, UpdateImageVersionOptions{
		Horovod:         req.Horovod,
		JobType:         req.JobType,
		MLFramework:     req.MLFramework,
		Processor:       req.Processor,
		ProgrammingLang: req.ProgrammingLang,
		ReleaseNotes:    req.ReleaseNotes,
		VendorGuidance:  req.VendorGuidance,
		AliasesToAdd:    req.AliasesToAdd,
		AliasesToDelete: req.AliasesToDelete,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyImageVersionArn: result.ImageVersionArn})
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

func (h *Handler) handleListAliases(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ImageName string `json:"ImageName"`
		NextToken string `json:"NextToken,omitempty"`
		Version   int32  `json:"Version,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	aliases, nextToken, err := h.Backend.ListImageAliases(ctx, req.ImageName, req.Version, req.NextToken)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"SageMakerImageVersionAliases": aliases}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}
