package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) projectVersionOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateProjectVersion":    service.WrapOp(h.handleCreateProjectVersion),
		"DeleteProjectVersion":    service.WrapOp(h.handleDeleteProjectVersion),
		"DescribeProjectVersions": service.WrapOp(h.handleDescribeProjectVersions),
		"CopyProjectVersion":      service.WrapOp(h.handleCopyProjectVersion),
		"StartProjectVersion":     service.WrapOp(h.handleStartProjectVersion),
		"StopProjectVersion":      service.WrapOp(h.handleStopProjectVersion),
	}
}

// =============================================================================
// Project Versions
// =============================================================================

type createProjectVersionReq struct {
	ProjectArn  string `json:"ProjectArn"`
	VersionName string `json:"VersionName"`
}

type createProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCreateProjectVersion(
	_ context.Context, req *createProjectVersionReq,
) (*createProjectVersionResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.VersionName == "" {
		return nil, fmt.Errorf("%w: VersionName is required", ErrValidation)
	}

	v, err := h.Backend.CreateProjectVersion(req.ProjectArn, req.VersionName)
	if err != nil {
		return nil, err
	}

	return &createProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type deleteProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type deleteProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleDeleteProjectVersion(
	_ context.Context, req *deleteProjectVersionReq,
) (*deleteProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &deleteProjectVersionResp{Status: "DELETING"}, nil
}

type describeProjectVersionsReq struct { //nolint:govet // existing issue.
	ProjectArn   string   `json:"ProjectArn"`
	VersionNames []string `json:"VersionNames"`
	NextToken    string   `json:"NextToken"`
	MaxResults   int32    `json:"MaxResults"`
}

type projectVersionDescription struct {
	ProjectVersionArn string  `json:"ProjectVersionArn"`
	Status            string  `json:"Status"`
	StatusMessage     string  `json:"StatusMessage,omitempty"`
	CreationTimestamp float64 `json:"CreationTimestamp"`
}

type describeProjectVersionsResp struct {
	NextToken                  string                      `json:"NextToken,omitempty"`
	ProjectVersionDescriptions []projectVersionDescription `json:"ProjectVersionDescriptions"`
}

func (h *Handler) handleDescribeProjectVersions(
	_ context.Context, req *describeProjectVersionsReq,
) (*describeProjectVersionsResp, error) {
	versions, nextToken, err := h.Backend.DescribeProjectVersions(
		req.ProjectArn, req.VersionNames, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	descriptions := make([]projectVersionDescription, 0, len(versions))
	for _, v := range versions {
		descriptions = append(descriptions, projectVersionDescription{
			ProjectVersionArn: v.ProjectVersionARN,
			Status:            v.Status,
			StatusMessage:     v.StatusMessage,
			CreationTimestamp: epochSeconds(v.CreationTimestamp),
		})
	}

	return &describeProjectVersionsResp{
		ProjectVersionDescriptions: descriptions,
		NextToken:                  nextToken,
	}, nil
}

type copyProjectVersionReq struct {
	SourceProjectVersionArn string `json:"SourceProjectVersionArn"`
	DestinationProjectArn   string `json:"DestinationProjectArn"`
	VersionName             string `json:"VersionName"`
}

type copyProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCopyProjectVersion(
	_ context.Context, req *copyProjectVersionReq,
) (*copyProjectVersionResp, error) {
	if req.SourceProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: SourceProjectVersionArn is required", ErrValidation)
	}

	if req.DestinationProjectArn == "" {
		return nil, fmt.Errorf("%w: DestinationProjectArn is required", ErrValidation)
	}

	v, err := h.Backend.CopyProjectVersion(
		req.SourceProjectVersionArn, req.DestinationProjectArn, req.VersionName,
	)
	if err != nil {
		return nil, err
	}

	return &copyProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type startProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
	MinInferenceUnits int32  `json:"MinInferenceUnits"`
}

type startProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStartProjectVersion(
	_ context.Context, req *startProjectVersionReq,
) (*startProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.StartProjectVersion(req.ProjectVersionArn, req.MinInferenceUnits); err != nil {
		return nil, err
	}

	return &startProjectVersionResp{Status: "RUNNING"}, nil
}

type stopProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type stopProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStopProjectVersion(
	_ context.Context, req *stopProjectVersionReq,
) (*stopProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.StopProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &stopProjectVersionResp{Status: "STOPPED"}, nil
}
