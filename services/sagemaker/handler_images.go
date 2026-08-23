package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Image handlers
// ---------------------------------------------------------------------------

type createImageInput struct {
	ImageName   string      `json:"ImageName"`
	Description string      `json:"Description"`
	DisplayName string      `json:"DisplayName"`
	RoleArn     string      `json:"RoleArn"`
	Tags        []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req createImageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImage(
		ctx, req.ImageName, req.Description, req.DisplayName, req.RoleArn, fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageArn": result.ImageArn})
}

type describeImageInput struct {
	ImageName string `json:"ImageName"`
}

func (h *Handler) handleDescribeImage(ctx context.Context, body []byte) ([]byte, error) {
	var req describeImageInput

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

type deleteImageInput struct {
	ImageName string `json:"ImageName"`
}

func (h *Handler) handleDeleteImage(ctx context.Context, body []byte) error {
	var req deleteImageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImage(ctx, req.ImageName)
}

type listImagesInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListImages(ctx context.Context, body []byte) ([]byte, error) {
	var req listImagesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListImages(ctx, ListImagesParams{
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		NextToken:              req.NextToken,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, img := range items {
		s := map[string]any{
			"ImageName":         img.ImageName,
			"ImageArn":          img.ImageArn,
			"ImageStatus":       img.ImageStatus,
			keyCreationTime:     epochSeconds(img.CreationTime),
			keyLastModifiedTime: epochSeconds(img.LastModifiedTime),
		}

		if img.Description != "" {
			s["Description"] = img.Description
		}

		if img.DisplayName != "" {
			s["DisplayName"] = img.DisplayName
		}

		summaries = append(summaries, s)
	}

	return json.Marshal(map[string]any{
		"Images":     summaries,
		keyNextToken: next,
	})
}

type updateImageInput struct {
	ImageName        string   `json:"ImageName"`
	Description      *string  `json:"Description"`
	DisplayName      *string  `json:"DisplayName"`
	RoleArn          *string  `json:"RoleArn"`
	DeleteProperties []string `json:"DeleteProperties"`
}

func (h *Handler) handleUpdateImage(ctx context.Context, body []byte) ([]byte, error) {
	var req updateImageInput

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

type createImageVersionInput struct {
	Horovod         *bool    `json:"Horovod"`
	ImageName       string   `json:"ImageName"`
	BaseImage       string   `json:"BaseImage"`
	JobType         string   `json:"JobType"`
	MLFramework     string   `json:"MLFramework"`
	Processor       string   `json:"Processor"`
	ProgrammingLang string   `json:"ProgrammingLang"`
	ReleaseNotes    string   `json:"ReleaseNotes"`
	VendorGuidance  string   `json:"VendorGuidance"`
	Aliases         []string `json:"Aliases"`
}

func (h *Handler) handleCreateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req createImageVersionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateImageVersion(ctx, req.ImageName, req.BaseImage, CreateImageVersionOptions{
		Aliases:         req.Aliases,
		Horovod:         req.Horovod,
		JobType:         req.JobType,
		MLFramework:     req.MLFramework,
		Processor:       req.Processor,
		ProgrammingLang: req.ProgrammingLang,
		ReleaseNotes:    req.ReleaseNotes,
		VendorGuidance:  req.VendorGuidance,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ImageVersionArn": result.ImageVersionArn})
}

type describeImageVersionInput struct {
	ImageName string `json:"ImageName"`
	Alias     string `json:"Alias"`
	Version   int    `json:"Version"`
}

func (h *Handler) handleDescribeImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req describeImageVersionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeImageVersion(ctx, req.ImageName, req.Alias, req.Version)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

type deleteImageVersionInput struct {
	ImageName string `json:"ImageName"`
	Alias     string `json:"Alias"`
	Version   int    `json:"Version"`
}

func (h *Handler) handleDeleteImageVersion(ctx context.Context, body []byte) error {
	var req deleteImageVersionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteImageVersion(ctx, req.ImageName, req.Alias, req.Version)
}

type listImageVersionsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	ImageName              string   `json:"ImageName"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy"`
	SortOrder              string   `json:"SortOrder"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListImageVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req listImageVersionsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	items, next := h.Backend.ListImageVersions(ctx, req.ImageName, ListImageVersionsParams{
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		NextToken:              req.NextToken,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, iv := range items {
		summaries = append(summaries, map[string]any{
			keyImageArn:          iv.ImageArn,
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

type updateImageVersionInput struct {
	Horovod         *bool    `json:"Horovod"`
	ImageName       string   `json:"ImageName"`
	Alias           string   `json:"Alias"`
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

func (h *Handler) handleUpdateImageVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req updateImageVersionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateImageVersion(ctx, req.ImageName, req.Version, UpdateImageVersionOptions{
		Alias:           req.Alias,
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

type listAliasesInput struct {
	ImageName  string `json:"ImageName"`
	Alias      string `json:"Alias"`
	NextToken  string `json:"NextToken,omitempty"`
	Version    int32  `json:"Version,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListAliases(ctx context.Context, body []byte) ([]byte, error) {
	var req listAliasesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ImageName == "" {
		return nil, fmt.Errorf("%w: ImageName is required", errInvalidRequest)
	}

	aliases, nextToken, err := h.Backend.ListImageAliases(
		ctx, req.ImageName, req.Alias, req.Version, req.NextToken, req.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"SageMakerImageVersionAliases": aliases}
	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}
