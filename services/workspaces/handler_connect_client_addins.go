package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildConnectClientAddInsOps returns the map of Connect client add-in operations.
func (h *Handler) buildConnectClientAddInsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnectClientAddIn":    service.WrapOp(h.handleCreateConnectClientAddIn),
		"DeleteConnectClientAddIn":    service.WrapOp(h.handleDeleteConnectClientAddIn),
		"DescribeConnectClientAddIns": service.WrapOp(h.handleDescribeConnectClientAddIns),
		"UpdateConnectClientAddIn":    service.WrapOp(h.handleUpdateConnectClientAddIn),
	}
}

type createConnectClientAddInInput struct {
	Name       string `json:"Name"`
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	URL        string `json:"URL"`
}

type createConnectClientAddInOutput struct {
	AddInId string `json:"AddInId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateConnectClientAddIn(
	_ context.Context, req *createConnectClientAddInInput,
) (*createConnectClientAddInOutput, error) {
	id, err := h.Backend.CreateConnectClientAddIn(req.Name, req.ResourceId, req.URL)
	if err != nil {
		return nil, err
	}

	return &createConnectClientAddInOutput{AddInId: id}, nil
}

type deleteConnectClientAddInInput struct {
	AddInId    string `json:"AddInId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteConnectClientAddIn(
	_ context.Context, req *deleteConnectClientAddInInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteConnectClientAddIn(req.AddInId, req.ResourceId)
}

type describeConnectClientAddInsInput struct {
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type connectAddInResp struct {
	AddInId string `json:"AddInId"` //nolint:revive,staticcheck // existing issue.
	Name    string `json:"Name"`
	URL     string `json:"URL"`
}

type describeConnectClientAddInsOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	AddIns    []connectAddInResp `json:"AddIns"`
}

func (h *Handler) handleDescribeConnectClientAddIns(
	_ context.Context, req *describeConnectClientAddInsInput,
) (*describeConnectClientAddInsOutput, error) {
	addIns, nextToken, err := h.Backend.DescribeConnectClientAddIns(
		req.ResourceId, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connectAddInResp, 0, len(addIns))
	for _, a := range addIns {
		items = append(items, connectAddInResp{AddInId: a.AddInID, Name: a.Name, URL: a.URL})
	}

	return &describeConnectClientAddInsOutput{AddIns: items, NextToken: nextToken}, nil
}

type updateConnectClientAddInInput struct {
	AddInId    string `json:"AddInId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	Name       string `json:"Name"`
	URL        string `json:"URL"`
}

func (h *Handler) handleUpdateConnectClientAddIn(
	_ context.Context, req *updateConnectClientAddInInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateConnectClientAddIn(
		req.AddInId, req.ResourceId, req.Name, req.URL,
	)
}
