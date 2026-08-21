package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

// createProjectInput mirrors CreateProjectInput (api_op_CreateProject.go:30-59).
type createProjectInput struct {
	ServiceCatalogProvisioningDetails json.RawMessage `json:"ServiceCatalogProvisioningDetails"`
	TemplateProviders                 json.RawMessage `json:"TemplateProviders"`
	ProjectName                       string          `json:"ProjectName"`
	ProjectDescription                string          `json:"ProjectDescription"`
	Tags                              []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req createProjectInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateProject(ctx, CreateProjectOptions{
		Name:                              req.ProjectName,
		Description:                       req.ProjectDescription,
		Tags:                              fromTagObjects(req.Tags),
		ServiceCatalogProvisioningDetails: req.ServiceCatalogProvisioningDetails,
		TemplateProviders:                 req.TemplateProviders,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyProjectArn: result.ProjectArn,
		keyProjectID:  result.ProjectID,
	})
}

// describeProjectInput mirrors DescribeProjectInput (api_op_DescribeProject.go:29-34).
type describeProjectInput struct {
	ProjectName string `json:"ProjectName"`
}

// projectResponseMap builds the AWS wire representation of a Project's
// DescribeProjectOutput. Tags has no wire counterpart on DescribeProjectOutput
// at all — a real client fetches tags via ListTags — so building this map
// explicitly (rather than marshaling the storage struct directly, as the
// previous handler did) is what keeps Tags off the wire.
func projectResponseMap(p *Project) map[string]any {
	resp := map[string]any{
		keyProjectName:      p.ProjectName,
		keyProjectArn:       p.ProjectArn,
		keyProjectID:        p.ProjectID,
		keyProjectStatus:    p.ProjectStatus,
		keyCreationTime:     epochSeconds(p.CreationTime),
		keyLastModifiedTime: epochSeconds(p.LastModifiedTime),
	}

	if p.ProjectDescription != "" {
		resp["ProjectDescription"] = p.ProjectDescription
	}

	if len(p.ServiceCatalogProvisioningDetails) > 0 {
		resp["ServiceCatalogProvisioningDetails"] = p.ServiceCatalogProvisioningDetails
	}

	if len(p.TemplateProviders) > 0 {
		resp["TemplateProviderDetails"] = p.TemplateProviders
	}

	return resp
}

func (h *Handler) handleDescribeProject(ctx context.Context, body []byte) ([]byte, error) {
	var req describeProjectInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeProject(ctx, req.ProjectName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(projectResponseMap(result))
}

// deleteProjectInput mirrors DeleteProjectInput (api_op_DeleteProject.go:27-32).
type deleteProjectInput struct {
	ProjectName string `json:"ProjectName"`
}

func (h *Handler) handleDeleteProject(ctx context.Context, body []byte) error {
	var req deleteProjectInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	return h.Backend.DeleteProject(ctx, req.ProjectName)
}

// listProjectsInput mirrors ListProjectsInput (api_op_ListProjects.go:30-58).
type listProjectsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore,omitempty"`
	NextToken          string   `json:"NextToken"`
	NameContains       string   `json:"NameContains"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListProjects(ctx context.Context, body []byte) ([]byte, error) {
	var req listProjectsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListProjects(ctx, req.NextToken, ListProjectsFilter{
		NameContains:       req.NameContains,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
		CreationTimeAfter:  timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore: timeFromEpochSecondsPtr(req.CreationTimeBefore),
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, p := range items {
		summaries = append(summaries, map[string]any{
			keyProjectName:   p.ProjectName,
			keyProjectArn:    p.ProjectArn,
			keyProjectID:     p.ProjectID,
			keyProjectStatus: p.ProjectStatus,
			keyCreationTime:  epochSeconds(p.CreationTime),
		})
	}

	resp := map[string]any{"ProjectSummaryList": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

// updateProjectInput mirrors UpdateProjectInput (api_op_UpdateProject.go:27-58).
// ServiceCatalogProvisioningUpdateDetails/TemplateProvidersToUpdate are
// deliberately not decoded — see UpdateProject's doc comment in projects.go
// for why they can't be honored yet; this is a disclosed gap, not a
// parsed-then-ignored field.
type updateProjectInput struct {
	ProjectName        string      `json:"ProjectName"`
	ProjectDescription string      `json:"ProjectDescription,omitempty"`
	Tags               []tagObject `json:"Tags,omitempty"`
}

func (h *Handler) handleUpdateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req updateProjectInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProject(ctx, req.ProjectName, req.ProjectDescription, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyProjectArn: p.ProjectArn})
}
