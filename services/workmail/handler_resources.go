package workmail

import (
	"context"
)

// ---- Resources ----

type createResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	Name           string `json:"Name"`
	Type           string `json:"Type"`
	Description    string `json:"Description"`
}

type createResourceResp struct {
	ResourceID string `json:"ResourceId"`
}

func (h *Handler) handleCreateResource(_ context.Context, req *createResourceReq) (*createResourceResp, error) {
	r, err := h.Backend.CreateResource(req.OrganizationID, req.Name, req.Type, req.Description)
	if err != nil {
		return nil, err
	}

	return &createResourceResp{ResourceID: r.ResourceID}, nil
}

type describeResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
}

type describeResourceResp struct {
	ResourceID   string `json:"ResourceId"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	Type         string `json:"Type"`
	Description  string `json:"Description,omitempty"`
	State        string `json:"State"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

func (h *Handler) handleDescribeResource(_ context.Context, req *describeResourceReq) (*describeResourceResp, error) {
	r, err := h.Backend.DescribeResource(req.OrganizationID, req.ResourceID)
	if err != nil {
		return nil, err
	}

	resp := &describeResourceResp{
		ResourceID:  r.ResourceID,
		Name:        r.Name,
		Email:       r.Email,
		Type:        r.ResourceType,
		Description: r.Description,
		State:       r.State,
	}
	if !r.EnabledDate.IsZero() {
		resp.EnabledDate = r.EnabledDate.Unix()
	}
	if !r.DisabledDate.IsZero() {
		resp.DisabledDate = r.DisabledDate.Unix()
	}

	return resp, nil
}

type updateResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	Name           string `json:"Name"`
	Description    string `json:"Description"`
}

func (h *Handler) handleUpdateResource(_ context.Context, req *updateResourceReq) (*emptyResp, error) {
	if err := h.Backend.UpdateResource(req.OrganizationID, req.ResourceID, req.Name, req.Description); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteResourceReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
}

func (h *Handler) handleDeleteResource(_ context.Context, req *deleteResourceReq) (*emptyResp, error) {
	if err := h.Backend.DeleteResource(req.OrganizationID, req.ResourceID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

// listResourcesFiltersReq mirrors aws-sdk-go-v2/service/workmail/types.
// ListResourcesFilters, the ListResourcesInput.Filters wire shape.
type listResourcesFiltersReq struct {
	NamePrefix         string `json:"NamePrefix"`
	PrimaryEmailPrefix string `json:"PrimaryEmailPrefix"`
	State              string `json:"State"`
}

type listResourcesReq struct {
	Filters        *listResourcesFiltersReq `json:"Filters"`
	OrganizationID string                   `json:"OrganizationId"`
	NextToken      string                   `json:"NextToken"`
	MaxResults     int32                    `json:"MaxResults"`
}

type resourceSummaryResp struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Email        string `json:"Email,omitempty"`
	Type         string `json:"Type"`
	State        string `json:"State"`
	Description  string `json:"Description,omitempty"`
	EnabledDate  int64  `json:"EnabledDate,omitempty"`
	DisabledDate int64  `json:"DisabledDate,omitempty"`
}

type listResourcesResp struct {
	NextToken string                `json:"NextToken,omitempty"`
	Resources []resourceSummaryResp `json:"Resources"`
}

func (h *Handler) handleListResources(_ context.Context, req *listResourcesReq) (*listResourcesResp, error) {
	var filter *ResourceFilter
	if req.Filters != nil {
		filter = &ResourceFilter{
			NamePrefix:         req.Filters.NamePrefix,
			PrimaryEmailPrefix: req.Filters.PrimaryEmailPrefix,
			State:              req.Filters.State,
		}
	}

	resources, next, err := h.Backend.ListResources(req.OrganizationID, filter, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]resourceSummaryResp, 0, len(resources))
	for _, r := range resources {
		s := resourceSummaryResp{
			ID:          r.ResourceID,
			Name:        r.Name,
			Email:       r.Email,
			Type:        r.ResourceType,
			State:       r.State,
			Description: r.Description,
		}
		if !r.EnabledDate.IsZero() {
			s.EnabledDate = r.EnabledDate.Unix()
		}
		if !r.DisabledDate.IsZero() {
			s.DisabledDate = r.DisabledDate.Unix()
		}
		summaries = append(summaries, s)
	}

	return &listResourcesResp{Resources: summaries, NextToken: next}, nil
}
