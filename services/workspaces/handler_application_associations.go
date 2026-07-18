package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildApplicationAssociationsOps returns the map of application association operations.
func (h *Handler) buildApplicationAssociationsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AssociateWorkspaceApplication": service.WrapOp(h.handleAssociateWorkspaceApplication),
		"DisassociateWorkspaceApplication": service.WrapOp(
			h.handleDisassociateWorkspaceApplication,
		),
		"DeployWorkspaceApplications":     service.WrapOp(h.handleDeployWorkspaceApplications),
		"DescribeWorkspaceAssociations":   service.WrapOp(h.handleDescribeWorkspaceAssociations),
		"DescribeApplicationAssociations": service.WrapOp(h.handleDescribeApplicationAssociations),
		"DescribeApplications":            service.WrapOp(h.handleDescribeApplications),
	}
}

type associateWorkspaceApplicationInput struct {
	WorkspaceId   string `json:"WorkspaceId"`   //nolint:revive,staticcheck // existing issue.
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
}

type workspaceAssocResp struct {
	WorkspaceId          string `json:"WorkspaceId"`          //nolint:revive,staticcheck // existing issue.
	AssociatedResourceId string `json:"AssociatedResourceId"` //nolint:revive,staticcheck // existing issue.
	AssociationStatus    string `json:"AssociationStatus"`
}

type associateWorkspaceApplicationOutput struct {
	Association workspaceAssocResp `json:"Association"`
}

func (h *Handler) handleAssociateWorkspaceApplication(
	_ context.Context, req *associateWorkspaceApplicationInput,
) (*associateWorkspaceApplicationOutput, error) {
	if err := h.Backend.AssociateWorkspaceApplication(req.WorkspaceId, req.ApplicationId); err != nil {
		return nil, err
	}

	return &associateWorkspaceApplicationOutput{
		Association: workspaceAssocResp{
			WorkspaceId:          req.WorkspaceId,
			AssociatedResourceId: req.ApplicationId,
			AssociationStatus:    "INSTALLED", //nolint:goconst // existing issue.
		},
	}, nil
}

type disassociateWorkspaceApplicationInput struct {
	WorkspaceId   string `json:"WorkspaceId"`   //nolint:revive,staticcheck // existing issue.
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
}

type disassociateWorkspaceApplicationOutput struct {
	Association workspaceAssocResp `json:"Association"`
}

func (h *Handler) handleDisassociateWorkspaceApplication(
	_ context.Context, req *disassociateWorkspaceApplicationInput,
) (*disassociateWorkspaceApplicationOutput, error) {
	if err := h.Backend.DisassociateWorkspaceApplication(req.WorkspaceId, req.ApplicationId); err != nil {
		return nil, err
	}

	return &disassociateWorkspaceApplicationOutput{
		Association: workspaceAssocResp{
			WorkspaceId:          req.WorkspaceId,
			AssociatedResourceId: req.ApplicationId,
			AssociationStatus:    "UNINSTALLED",
		},
	}, nil
}

type deployWorkspaceApplicationsInput struct {
	WorkspaceId string `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	Force       bool   `json:"Force"`
}

type deployWorkspaceApplicationsOutput struct {
	Deployment struct {
		Associations []workspaceAssocResp `json:"Associations"`
	} `json:"Deployment"`
}

func (h *Handler) handleDeployWorkspaceApplications(
	_ context.Context, req *deployWorkspaceApplicationsInput,
) (*deployWorkspaceApplicationsOutput, error) {
	associations, err := h.Backend.DeployWorkspaceApplications(req.WorkspaceId, req.Force)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(associations))
	for _, a := range associations {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a[wireKeyWorkspaceID],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	out := &deployWorkspaceApplicationsOutput{}
	out.Deployment.Associations = items

	return out, nil
}

type describeWorkspaceAssociationsInput struct {
	WorkspaceId             string   `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

type describeWorkspaceAssociationsOutput struct {
	Associations []workspaceAssocResp `json:"Associations"`
}

func (h *Handler) handleDescribeWorkspaceAssociations(
	_ context.Context, req *describeWorkspaceAssociationsInput,
) (*describeWorkspaceAssociationsOutput, error) {
	assocs, err := h.Backend.DescribeWorkspaceAssociations(
		req.WorkspaceId,
		req.AssociatedResourceTypes,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a[wireKeyWorkspaceID],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	return &describeWorkspaceAssociationsOutput{Associations: items}, nil
}

type describeApplicationAssociationsInput struct {
	ApplicationId           string   `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
	NextToken               string   `json:"NextToken"`
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
	MaxResults              int32    `json:"MaxResults"`
}

type describeApplicationAssociationsOutput struct {
	NextToken    string               `json:"NextToken,omitempty"`
	Associations []workspaceAssocResp `json:"Associations"`
}

func (h *Handler) handleDescribeApplicationAssociations(
	_ context.Context, req *describeApplicationAssociationsInput,
) (*describeApplicationAssociationsOutput, error) {
	assocs, nextToken, err := h.Backend.DescribeApplicationAssociations(
		req.ApplicationId, req.AssociatedResourceTypes, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceAssocResp, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, workspaceAssocResp{
			WorkspaceId:          a[wireKeyWorkspaceID],
			AssociatedResourceId: a["AssociatedResourceId"],
			AssociationStatus:    a["AssociationStatus"],
		})
	}

	return &describeApplicationAssociationsOutput{Associations: items, NextToken: nextToken}, nil
}

type describeApplicationsInput struct {
	NextToken      string   `json:"NextToken"`
	ApplicationIds []string `json:"ApplicationIds"` //nolint:revive // existing issue.
	MaxResults     int32    `json:"MaxResults"`
}

type applicationResp struct {
	ApplicationId string `json:"ApplicationId"` //nolint:revive,staticcheck // existing issue.
	Name          string `json:"Name"`
	Owner         string `json:"Owner"`
	State         string `json:"State"`
}

type describeApplicationsOutput struct {
	NextToken    string            `json:"NextToken,omitempty"`
	Applications []applicationResp `json:"Applications"`
}

func (h *Handler) handleDescribeApplications(
	_ context.Context, req *describeApplicationsInput,
) (*describeApplicationsOutput, error) {
	apps, nextToken, err := h.Backend.DescribeApplications(
		req.ApplicationIds,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]applicationResp, 0, len(apps))
	for _, a := range apps {
		items = append(items, applicationResp{
			ApplicationId: a.AppID,
			Name:          a.Name,
			Owner:         a.Owner,
			State:         a.State,
		})
	}

	return &describeApplicationsOutput{Applications: items, NextToken: nextToken}, nil
}
