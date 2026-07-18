package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) projectOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateProject":       service.WrapOp(h.handleCreateProject),
		"DeleteProject":       service.WrapOp(h.handleDeleteProject),
		"DescribeProjects":    service.WrapOp(h.handleDescribeProjects),
		"ListProjectPolicies": service.WrapOp(h.handleListProjectPolicies),
		"PutProjectPolicy":    service.WrapOp(h.handlePutProjectPolicy),
		"DeleteProjectPolicy": service.WrapOp(h.handleDeleteProjectPolicy),
	}
}

// =============================================================================
// Projects
// =============================================================================

type createProjectReq struct {
	ProjectName string `json:"ProjectName"`
}

type createProjectResp struct {
	ProjectArn string `json:"ProjectArn"`
}

func (h *Handler) handleCreateProject(_ context.Context, req *createProjectReq) (*createProjectResp, error) {
	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", ErrValidation)
	}

	proj, err := h.Backend.CreateProject(req.ProjectName)
	if err != nil {
		return nil, err
	}

	return &createProjectResp{ProjectArn: proj.ProjectARN}, nil
}

type deleteProjectReq struct {
	ProjectArn string `json:"ProjectArn"`
}

type deleteProjectResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleDeleteProject(_ context.Context, req *deleteProjectReq) (*deleteProjectResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteProject(req.ProjectArn); err != nil {
		return nil, err
	}

	return &deleteProjectResp{Status: "DELETING"}, nil
}

type describeProjectsReq struct { //nolint:govet // existing issue.
	ProjectArns []string `json:"ProjectArns"`
	NextToken   string   `json:"NextToken"`
	MaxResults  int32    `json:"MaxResults"`
}

type projectDescription struct {
	ProjectArn        string  `json:"ProjectArn"`
	Status            string  `json:"Status"`
	CreationTimestamp float64 `json:"CreationTimestamp"`
}

type describeProjectsResp struct {
	NextToken           string               `json:"NextToken,omitempty"`
	ProjectDescriptions []projectDescription `json:"ProjectDescriptions"`
}

func (h *Handler) handleDescribeProjects(
	_ context.Context, req *describeProjectsReq,
) (*describeProjectsResp, error) {
	projects, nextToken, err := h.Backend.DescribeProjects(req.ProjectArns, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	descriptions := make([]projectDescription, 0, len(projects))
	for _, p := range projects {
		descriptions = append(descriptions, projectDescription{
			ProjectArn:        p.ProjectARN,
			Status:            p.Status,
			CreationTimestamp: epochSeconds(p.CreationTimestamp),
		})
	}

	return &describeProjectsResp{
		ProjectDescriptions: descriptions,
		NextToken:           nextToken,
	}, nil
}

// =============================================================================
// Project Policies
// =============================================================================

type listProjectPoliciesReq struct {
	ProjectArn string `json:"ProjectArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type projectPolicyEntry struct {
	ProjectArn           string  `json:"ProjectArn"`
	PolicyName           string  `json:"PolicyName"`
	PolicyRevisionId     string  `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
	PolicyDocument       string  `json:"PolicyDocument"`
	CreationTimestamp    float64 `json:"CreationTimestamp"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp"`
}

type listProjectPoliciesResp struct {
	NextToken       string               `json:"NextToken,omitempty"`
	ProjectPolicies []projectPolicyEntry `json:"ProjectPolicies"`
}

func (h *Handler) handleListProjectPolicies(
	_ context.Context, req *listProjectPoliciesReq,
) (*listProjectPoliciesResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	policies, nextToken, err := h.Backend.ListProjectPolicies(req.ProjectArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]projectPolicyEntry, 0, len(policies))
	for _, p := range policies {
		entries = append(entries, projectPolicyEntry{
			ProjectArn:           p.ProjectARN,
			PolicyName:           p.PolicyName,
			PolicyRevisionId:     p.PolicyRevisionID,
			PolicyDocument:       p.PolicyDocument,
			CreationTimestamp:    epochSeconds(p.CreationTimestamp),
			LastUpdatedTimestamp: epochSeconds(p.LastUpdatedTimestamp),
		})
	}

	return &listProjectPoliciesResp{
		ProjectPolicies: entries,
		NextToken:       nextToken,
	}, nil
}

type putProjectPolicyReq struct {
	ProjectArn       string `json:"ProjectArn"`
	PolicyName       string `json:"PolicyName"`
	PolicyDocument   string `json:"PolicyDocument"`
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

type putProjectPolicyResp struct {
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handlePutProjectPolicy(
	_ context.Context, req *putProjectPolicyReq,
) (*putProjectPolicyResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	revID, err := h.Backend.PutProjectPolicy(
		req.ProjectArn, req.PolicyName, req.PolicyDocument, req.PolicyRevisionId,
	)
	if err != nil {
		return nil, err
	}

	return &putProjectPolicyResp{PolicyRevisionId: revID}, nil
}

type deleteProjectPolicyReq struct {
	ProjectArn       string `json:"ProjectArn"`
	PolicyName       string `json:"PolicyName"`
	PolicyRevisionId string `json:"PolicyRevisionId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteProjectPolicy(
	_ context.Context, req *deleteProjectPolicyReq,
) (*struct{}, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrValidation)
	}

	if err := h.Backend.DeleteProjectPolicy(req.ProjectArn, req.PolicyName, req.PolicyRevisionId); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
