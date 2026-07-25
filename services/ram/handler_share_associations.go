package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

// associationObject is the JSON representation of a ResourceShareAssociation.
type associationObject struct {
	ResourceShareArn  string  `json:"resourceShareArn"`
	ResourceShareName string  `json:"resourceShareName"`
	AssociatedEntity  string  `json:"associatedEntity"`
	AssociationType   string  `json:"associationType"`
	Status            string  `json:"status"`
	StatusMessage     string  `json:"statusMessage,omitempty"`
	CreationTime      float64 `json:"creationTime"`
	LastUpdatedTime   float64 `json:"lastUpdatedTime"`
	External          bool    `json:"external"`
}

func toAssociationObject(a *ResourceShareAssociation) associationObject {
	return associationObject{
		ResourceShareArn:  a.ResourceShareARN,
		ResourceShareName: a.ResourceShareName,
		AssociatedEntity:  a.AssociatedEntity,
		AssociationType:   a.AssociationType,
		Status:            a.Status,
		StatusMessage:     a.StatusMessage,
		CreationTime:      epochSeconds(a.CreationTime),
		LastUpdatedTime:   epochSeconds(a.LastUpdatedTime),
		External:          a.External,
	}
}

type associateResourceShareRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	Principals       []string `json:"principals"`
	ResourceArns     []string `json:"resourceArns"`
}

type associateResourceShareResponse struct {
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleAssociateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req associateResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	associations, err := h.Backend.AssociateResourceShare(
		req.ResourceShareArn,
		req.Principals,
		req.ResourceArns,
	)
	if err != nil {
		return nil, err
	}

	if len(req.ResourceArns) > 0 {
		// AssociateResourceShare has no permissionArns parameter in the real API: AWS
		// always auto-associates the default managed permission for any resource type
		// newly introduced to the share that isn't covered by an existing permission yet.
		if autoErr := h.Backend.AutoAssociateDefaultPermissions(req.ResourceShareArn); autoErr != nil {
			return nil, autoErr
		}
	}

	objs := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		objs = append(objs, toAssociationObject(a))
	}

	return json.Marshal(associateResourceShareResponse{ResourceShareAssociations: objs})
}

type disassociateResourceShareRequest struct {
	ResourceShareArn string   `json:"resourceShareArn"`
	Principals       []string `json:"principals"`
	ResourceArns     []string `json:"resourceArns"`
}

type disassociateResourceShareResponse struct {
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleDisassociateResourceShare(_ context.Context, body []byte) ([]byte, error) {
	var req disassociateResourceShareRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareArn == "" {
		return nil, fmt.Errorf("%w: resourceShareArn is required", errInvalidRequest)
	}

	associations, err := h.Backend.DisassociateResourceShare(
		req.ResourceShareArn,
		req.Principals,
		req.ResourceArns,
	)
	if err != nil {
		return nil, err
	}

	objs := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		objs = append(objs, toAssociationObject(a))
	}

	return json.Marshal(disassociateResourceShareResponse{ResourceShareAssociations: objs})
}

type getResourceShareAssociationsRequest struct {
	MaxResults        *int32   `json:"maxResults,omitempty"`
	AssociationType   string   `json:"associationType"`
	AssociationStatus string   `json:"associationStatus"`
	Principal         string   `json:"principal"`
	ResourceArn       string   `json:"resourceArn"`
	NextToken         string   `json:"nextToken"`
	ResourceShareArns []string `json:"resourceShareArns"`
}

type getResourceShareAssociationsResponse struct {
	NextToken                 string              `json:"nextToken,omitempty"`
	ResourceShareAssociations []associationObject `json:"resourceShareAssociations"`
}

func (h *Handler) handleGetResourceShareAssociations(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req getResourceShareAssociationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AssociationType == "" {
		return nil, fmt.Errorf("%w: associationType is required", errInvalidRequest)
	}

	associations := h.Backend.GetResourceShareAssociations(
		req.AssociationType,
		req.ResourceShareArns,
	)

	// Apply principal, resource ARN, or association status filter.
	filtered := make([]associationObject, 0, len(associations))

	for _, a := range associations {
		if req.Principal != "" && a.AssociatedEntity != req.Principal {
			continue
		}

		if req.ResourceArn != "" && a.AssociatedEntity != req.ResourceArn {
			continue
		}

		if req.AssociationStatus != "" && a.Status != req.AssociationStatus {
			continue
		}

		filtered = append(filtered, toAssociationObject(a))
	}

	page, nextToken, err := ramPaginate(filtered, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(
		getResourceShareAssociationsResponse{NextToken: nextToken, ResourceShareAssociations: page},
	)
}
