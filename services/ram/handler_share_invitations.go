package ram

import (
	"context"
	"encoding/json"
	"fmt"
)

// invitationObject is the JSON representation of a ResourceShareInvitation.
type invitationObject struct {
	ResourceShareInvitationArn string  `json:"resourceShareInvitationArn"`
	ResourceShareArn           string  `json:"resourceShareArn"`
	ResourceShareName          string  `json:"resourceShareName"`
	SenderAccountID            string  `json:"senderAccountId"`
	ReceiverAccountID          string  `json:"receiverAccountId"`
	Status                     string  `json:"status"`
	InvitationTimestamp        float64 `json:"invitationTimestamp"`
}

func toInvitationObject(inv *ResourceShareInvitation) invitationObject {
	return invitationObject{
		ResourceShareInvitationArn: inv.InvitationARN,
		ResourceShareArn:           inv.ResourceShareARN,
		ResourceShareName:          inv.ResourceShareName,
		SenderAccountID:            inv.SenderAccountID,
		ReceiverAccountID:          inv.ReceiverAccountID,
		Status:                     inv.Status,
		InvitationTimestamp:        epochSeconds(inv.CreationTime),
	}
}

type acceptResourceShareInvitationRequest struct {
	ResourceShareInvitationArn string `json:"resourceShareInvitationArn"`
}

type acceptResourceShareInvitationResponse struct {
	ResourceShareInvitation invitationObject `json:"resourceShareInvitation"`
}

func (h *Handler) handleAcceptResourceShareInvitation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req acceptResourceShareInvitationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareInvitationArn == "" {
		return nil, fmt.Errorf("%w: resourceShareInvitationArn is required", errInvalidRequest)
	}

	inv, err := h.Backend.AcceptResourceShareInvitation(req.ResourceShareInvitationArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(acceptResourceShareInvitationResponse{
		ResourceShareInvitation: toInvitationObject(inv),
	})
}

type rejectResourceShareInvitationRequest struct {
	ResourceShareInvitationArn string `json:"resourceShareInvitationArn"`
}

type rejectResourceShareInvitationResponse struct {
	ResourceShareInvitation invitationObject `json:"resourceShareInvitation"`
}

func (h *Handler) handleRejectResourceShareInvitation(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req rejectResourceShareInvitationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceShareInvitationArn == "" {
		return nil, fmt.Errorf("%w: resourceShareInvitationArn is required", errInvalidRequest)
	}

	inv, err := h.Backend.RejectResourceShareInvitation(req.ResourceShareInvitationArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rejectResourceShareInvitationResponse{
		ResourceShareInvitation: toInvitationObject(inv),
	})
}

type getResourceShareInvitationsRequest struct {
	MaxResults                  *int32   `json:"maxResults,omitempty"`
	NextToken                   string   `json:"nextToken"`
	ResourceShareInvitationArns []string `json:"resourceShareInvitationArns"`
	ResourceShareArns           []string `json:"resourceShareArns"`
}

type getResourceShareInvitationsResponse struct {
	NextToken                string             `json:"nextToken,omitempty"`
	ResourceShareInvitations []invitationObject `json:"resourceShareInvitations"`
}

func (h *Handler) handleGetResourceShareInvitations(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req getResourceShareInvitationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	invitations := h.Backend.GetResourceShareInvitations(
		req.ResourceShareInvitationArns,
		req.ResourceShareArns,
	)
	objs := make([]invitationObject, 0, len(invitations))

	for _, inv := range invitations {
		objs = append(objs, toInvitationObject(inv))
	}

	page, nextToken, err := ramPaginate(objs, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	return json.Marshal(
		getResourceShareInvitationsResponse{NextToken: nextToken, ResourceShareInvitations: page},
	)
}
