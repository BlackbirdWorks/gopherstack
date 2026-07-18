package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

type listResourceSharePermissionsRequest struct {
	MaxResults       *int32 `json:"maxResults,omitempty"`
	ResourceShareArn string `json:"resourceShareArn"`
	NextToken        string `json:"nextToken"`
}

type listResourceSharePermissionsResponse struct {
	NextToken   string                    `json:"nextToken,omitempty"`
	Permissions []permissionSummaryObject `json:"permissions"`
}

// handleListResourceSharePermissions returns the managed permissions associated with a resource share.
func (h *Handler) handleListResourceSharePermissions(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req listResourceSharePermissionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	perms := h.Backend.ListResourceSharePermissions(req.ResourceShareArn)
	objs := make([]permissionSummaryObject, 0, len(perms))

	for _, d := range perms {
		objs = append(objs, toResourceSharePermissionSummaryObject(d))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(
		listResourceSharePermissionsResponse{NextToken: nextToken, Permissions: page},
	)
}

type associateResourceSharePermissionRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	ResourceShareArn  string `json:"resourceShareArn"`
	PermissionArn     string `json:"permissionArn"`
	Replace           bool   `json:"replace"`
}

type associateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleAssociateResourceSharePermission(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req associateResourceSharePermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateResourceSharePermission(
		req.ResourceShareArn, req.PermissionArn, req.Replace, req.PermissionVersion,
	); err != nil {
		return nil, err
	}

	return json.Marshal(associateResourceSharePermissionResponse{ReturnValue: true})
}

type disassociateResourceSharePermissionRequest struct {
	ResourceShareArn string `json:"resourceShareArn"`
	PermissionArn    string `json:"permissionArn"`
}

type disassociateResourceSharePermissionResponse struct {
	ReturnValue bool `json:"returnValue"`
}

func (h *Handler) handleDisassociateResourceSharePermission(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req disassociateResourceSharePermissionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	if req.PermissionArn == "" {
		return nil, fmt.Errorf("%w: permissionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DisassociateResourceSharePermission(req.ResourceShareArn, req.PermissionArn); err != nil {
		return nil, err
	}

	return json.Marshal(disassociateResourceSharePermissionResponse{ReturnValue: true})
}

type replacePermissionAssociationsRequest struct {
	FromPermissionVersion *int32 `json:"fromPermissionVersion,omitempty"`
	FromPermissionArn     string `json:"fromPermissionArn"`
	ToPermissionArn       string `json:"toPermissionArn"`
}

type replacePermissionAssociationsWork struct {
	ID                string `json:"id"`
	FromPermissionArn string `json:"fromPermissionArn"`
	ToPermissionArn   string `json:"toPermissionArn"`
	Status            string `json:"status"`
}

type replacePermissionAssociationsResponse struct {
	ReplacePermissionAssociationsWork replacePermissionAssociationsWork `json:"replacePermissionAssociationsWork"`
}

func (h *Handler) handleReplacePermissionAssociations(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req replacePermissionAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FromPermissionArn == "" {
		return nil, fmt.Errorf("%w: fromPermissionArn is required", errInvalidRequest)
	}

	if req.ToPermissionArn == "" {
		return nil, fmt.Errorf("%w: toPermissionArn is required", errInvalidRequest)
	}

	workID, err := h.Backend.ReplacePermissionAssociations(
		req.FromPermissionArn,
		req.ToPermissionArn,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(replacePermissionAssociationsResponse{
		ReplacePermissionAssociationsWork: replacePermissionAssociationsWork{
			ID:                workID,
			FromPermissionArn: req.FromPermissionArn,
			ToPermissionArn:   req.ToPermissionArn,
			Status:            "IN_PROGRESS",
		},
	})
}

type permissionAssociationObject struct {
	ResourceShareArn  string `json:"resourceShareArn"`
	PermissionArn     string `json:"permissionArn"`
	PermissionVersion int32  `json:"permissionVersion"`
}

type listPermissionAssociationsRequest struct {
	PermissionVersion *int32 `json:"permissionVersion,omitempty"`
	MaxResults        *int32 `json:"maxResults,omitempty"`
	PermissionArn     string `json:"permissionArn"`
	NextToken         string `json:"nextToken"`
}

type listPermissionAssociationsResponse struct {
	NextToken   string                        `json:"nextToken,omitempty"`
	Permissions []permissionAssociationObject `json:"permissions"`
}

func (h *Handler) handleListPermissionAssociations(_ context.Context, body []byte) ([]byte, error) {
	var req listPermissionAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	assocs := h.Backend.ListPermissionAssociations(req.PermissionArn)
	objs := make([]permissionAssociationObject, 0, len(assocs))

	for _, a := range assocs {
		objs = append(objs, permissionAssociationObject{
			ResourceShareArn:  a.ShareARN,
			PermissionArn:     a.PermissionARN,
			PermissionVersion: a.Version,
		})
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listPermissionAssociationsResponse{NextToken: nextToken, Permissions: page})
}

type listReplacePermissionAssociationsWorkResponse struct {
	NextToken                         string                              `json:"nextToken,omitempty"`
	ReplacePermissionAssociationsWork []replacePermissionAssociationsWork `json:"replacePermissionAssociationsWork"`
}

func (h *Handler) handleListReplacePermissionAssociationsWork(
	_ context.Context,
	_ []byte,
) ([]byte, error) {
	// Mock returns empty list; work items are ephemeral in this implementation.
	return json.Marshal(listReplacePermissionAssociationsWorkResponse{
		ReplacePermissionAssociationsWork: []replacePermissionAssociationsWork{},
	})
}
