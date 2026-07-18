package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type acceptHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type cancelHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type declineHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type describeHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type describeResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type handshakePartyObject struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type handshakeResourceObject struct {
	Type      string                    `json:"Type"`
	Value     string                    `json:"Value"`
	Resources []handshakeResourceObject `json:"Resources,omitempty"`
}

type handshakeObject struct {
	ID                  string                    `json:"Id"`
	ARN                 string                    `json:"Arn"`
	Action              string                    `json:"Action"`
	State               string                    `json:"State"`
	Parties             []handshakePartyObject    `json:"Parties"`
	Resources           []handshakeResourceObject `json:"Resources"`
	RequestedTimestamp  float64                   `json:"RequestedTimestamp"`
	ExpirationTimestamp float64                   `json:"ExpirationTimestamp"`
}

type acceptHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type cancelHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type declineHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type describeHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type describeResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- EnableAllFeatures --

type enableAllFeaturesResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

// -- ListHandshakes filter --

type handshakeFilter struct {
	ActionType string `json:"ActionType,omitempty"`
}

type listHandshakesFilterRequest struct {
	Filter     handshakeFilter `json:"Filter"`
	NextToken  string          `json:"NextToken,omitempty"`
	MaxResults int             `json:"MaxResults,omitempty"`
}

// -- InviteAccountToOrganization --

type inviteAccountToOrganizationRequest struct {
	Target HandshakeParty `json:"Target"`
	Notes  string         `json:"Notes,omitempty"`
}

type inviteAccountToOrganizationResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

// -- LeaveOrganization --
// (no request body; no response body)

// -- ListHandshakesForAccount --

type listHandshakesForAccountResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Handshakes []handshakeObject `json:"Handshakes"`
}

// -- ListHandshakesForOrganization --

type listHandshakesForOrganizationResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Handshakes []handshakeObject `json:"Handshakes"`
}

// -- ListInboundResponsibilityTransfers --

type listInboundResponsibilityTransfersResponse struct {
	NextToken               string            `json:"NextToken,omitempty"`
	ResponsibilityTransfers []handshakeObject `json:"ResponsibilityTransfers"`
}

// -- ListOutboundResponsibilityTransfers --

type listOutboundResponsibilityTransfersResponse struct {
	NextToken               string            `json:"NextToken,omitempty"`
	ResponsibilityTransfers []handshakeObject `json:"ResponsibilityTransfers"`
}

// -- TerminateResponsibilityTransfer --

type terminateResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type terminateResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- UpdateResponsibilityTransfer --

type updateResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
	Action      string `json:"Action"`
}

type updateResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- InviteOrganizationToTransferResponsibility --

type inviteOrganizationToTransferResponsibilityRequest struct {
	Target HandshakeParty `json:"Target"`
	Notes  string         `json:"Notes,omitempty"`
}

type inviteOrganizationToTransferResponsibilityResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

// dispatchHandshakeOps handles invitation and handshake listing operations.
func (h *Handler) dispatchHandshakeOps(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "AcceptHandshake":
		return true, h.handleAcceptHandshake(c, body)
	case "CancelHandshake":
		return true, h.handleCancelHandshake(c, body)
	case "DeclineHandshake":
		return true, h.handleDeclineHandshake(c, body)
	case "DescribeHandshake":
		return true, h.handleDescribeHandshake(c, body)
	case "EnableAllFeatures":
		return true, h.handleEnableAllFeatures(c, body)
	case "InviteAccountToOrganization":
		return true, h.handleInviteAccountToOrganization(c, body)
	case "LeaveOrganization":
		return true, h.handleLeaveOrganization(c, body)
	case "ListHandshakesForAccount":
		return true, h.handleListHandshakesForAccount(c, body)
	case "ListHandshakesForOrganization":
		return true, h.handleListHandshakesForOrganization(c, body)
	}

	return false, nil
}

// dispatchTransferOps handles responsibility-transfer operations.
func (h *Handler) dispatchTransferOps(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "DescribeResponsibilityTransfer":
		return true, h.handleDescribeResponsibilityTransfer(c, body)
	case "InviteOrganizationToTransferResponsibility":
		return true, h.handleInviteOrganizationToTransferResponsibility(c, body)
	case "ListInboundResponsibilityTransfers":
		return true, h.handleListInboundResponsibilityTransfers(c, body)
	case "ListOutboundResponsibilityTransfers":
		return true, h.handleListOutboundResponsibilityTransfers(c, body)
	case "TerminateResponsibilityTransfer":
		return true, h.handleTerminateResponsibilityTransfer(c, body)
	case "UpdateResponsibilityTransfer":
		return true, h.handleUpdateResponsibilityTransfer(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Handshake handlers
// ----------------------------------------

func (h *Handler) handleAcceptHandshake(c *echo.Context, body []byte) error {
	var req acceptHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.AcceptHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, acceptHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleCancelHandshake(c *echo.Context, body []byte) error {
	var req cancelHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.CancelHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, cancelHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDeclineHandshake(c *echo.Context, body []byte) error {
	var req declineHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DeclineHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, declineHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDescribeHandshake(c *echo.Context, body []byte) error {
	var req describeHandshakeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DescribeHandshake(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeHandshakeResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleDescribeResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req describeResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.DescribeResponsibilityTransfer(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

func (h *Handler) handleEnableAllFeatures(c *echo.Context, _ []byte) error {
	hs, err := h.Backend.EnableAllFeatures()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, enableAllFeaturesResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleInviteAccountToOrganization(c *echo.Context, body []byte) error {
	var req inviteAccountToOrganizationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Target.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Id is required")
	}

	hs, err := h.Backend.InviteAccountToOrganization(req.Target, req.Notes)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, inviteAccountToOrganizationResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleInviteOrganizationToTransferResponsibility(c *echo.Context, body []byte) error {
	var req inviteOrganizationToTransferResponsibilityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Target.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Id is required")
	}

	hs, err := h.Backend.InviteOrganizationToTransferResponsibility(req.Target, req.Notes)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, inviteOrganizationToTransferResponsibilityResponse{Handshake: toHandshakeObject(hs)})
}

func (h *Handler) handleLeaveOrganization(c *echo.Context, _ []byte) error {
	if err := h.Backend.LeaveOrganization(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

//nolint:dupl // similar to handleListHandshakesForOrganization
func (h *Handler) handleListHandshakesForAccount(c *echo.Context, body []byte) error {
	var req listHandshakesFilterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	handshakes, err := h.Backend.ListHandshakesForAccount(req.Filter.ActionType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		if req.Filter.ActionType == "" || hs.Action == req.Filter.ActionType {
			objs = append(objs, toHandshakeObject(hs))
		}
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listHandshakesForAccountResponse{Handshakes: p.Data, NextToken: p.Next})
}

//nolint:dupl // similar to handleListHandshakesForAccount
func (h *Handler) handleListHandshakesForOrganization(c *echo.Context, body []byte) error {
	var req listHandshakesFilterRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	handshakes, err := h.Backend.ListHandshakesForOrganization(req.Filter.ActionType)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		if req.Filter.ActionType == "" || hs.Action == req.Filter.ActionType {
			objs = append(objs, toHandshakeObject(hs))
		}
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listHandshakesForOrganizationResponse{Handshakes: p.Data, NextToken: p.Next})
}

func (h *Handler) handleListInboundResponsibilityTransfers(c *echo.Context, _ []byte) error {
	handshakes, err := h.Backend.ListInboundResponsibilityTransfers()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listInboundResponsibilityTransfersResponse{ResponsibilityTransfers: objs})
}

func (h *Handler) handleListOutboundResponsibilityTransfers(c *echo.Context, _ []byte) error {
	handshakes, err := h.Backend.ListOutboundResponsibilityTransfers()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]handshakeObject, 0, len(handshakes))
	for _, hs := range handshakes {
		objs = append(objs, toHandshakeObject(hs))
	}

	return c.JSON(http.StatusOK, listOutboundResponsibilityTransfersResponse{ResponsibilityTransfers: objs})
}

func (h *Handler) handleTerminateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req terminateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	hs, err := h.Backend.TerminateResponsibilityTransfer(req.HandshakeID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, terminateResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

func (h *Handler) handleUpdateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req updateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.HandshakeID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "HandshakeId is required")
	}

	if req.Action == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Action is required")
	}

	hs, err := h.Backend.UpdateResponsibilityTransfer(req.HandshakeID, req.Action)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateResponsibilityTransferResponse{HandshakeDetails: toHandshakeObject(hs)})
}

// ----------------------------------------
// Handshake conversion helpers
// ----------------------------------------

func toHandshakeObject(h *Handshake) handshakeObject {
	parties := make([]handshakePartyObject, 0, len(h.Parties))
	for _, p := range h.Parties {
		parties = append(parties, handshakePartyObject(p))
	}

	resources := toHandshakeResourceObjects(h.Resources)

	return handshakeObject{
		ID:                  h.ID,
		ARN:                 h.ARN,
		Action:              h.Action,
		State:               h.State,
		RequestedTimestamp:  epochSeconds(h.RequestedTimestamp),
		ExpirationTimestamp: epochSeconds(h.ExpirationTimestamp),
		Parties:             parties,
		Resources:           resources,
	}
}

func toHandshakeResourceObjects(rs []HandshakeResource) []handshakeResourceObject {
	out := make([]handshakeResourceObject, 0, len(rs))

	for _, r := range rs {
		obj := handshakeResourceObject{
			Type:  r.Type,
			Value: r.Value,
		}

		if len(r.Resources) > 0 {
			obj.Resources = toHandshakeResourceObjects(r.Resources)
		}

		out = append(out, obj)
	}

	return out
}
