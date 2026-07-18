package workmail

import (
	"context"
)

// ---- DescribeEntity ----

type describeEntityReq struct {
	OrganizationID string `json:"OrganizationId"`
	Email          string `json:"Email"`
}

type describeEntityResp struct {
	EntityID string `json:"EntityId"`
	Name     string `json:"Name"`
	Type     string `json:"Type"`
}

func (h *Handler) handleDescribeEntity(_ context.Context, req *describeEntityReq) (*describeEntityResp, error) {
	entity, err := h.Backend.DescribeEntity(req.OrganizationID, req.Email)
	if err != nil {
		return nil, err
	}

	return &describeEntityResp{EntityID: entity.EntityID, Name: entity.Name, Type: entity.Type}, nil
}
