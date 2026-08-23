package organizations

import (
	"encoding/json"
	"net/http"
	"time"

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

// -- ResponsibilityTransfer wire shape --
//
// Distinct from handshakeObject: types.ResponsibilityTransfer
// (awsAwsjson11_deserializeDocumentResponsibilityTransfer, deserializers.go)
// is its own shape, not a Handshake, with its own field set
// (ActiveHandshakeId/Arn/EndTimestamp/Id/Name/Source/StartTimestamp/Status/
// Target/Type). The response envelope key is "ResponsibilityTransfer"
// (singular, Describe/Terminate/Update -- deserializeOpDocument*Output) or
// "ResponsibilityTransfers" (plural array, the two List ops), never
// "HandshakeDetails".

type transferParticipantObject struct {
	ManagementAccountID    string `json:"ManagementAccountId,omitempty"`
	ManagementAccountEmail string `json:"ManagementAccountEmail,omitempty"`
}

type responsibilityTransferObject struct {
	EndTimestamp      *float64                   `json:"EndTimestamp,omitempty"`
	Source            *transferParticipantObject `json:"Source,omitempty"`
	Target            *transferParticipantObject `json:"Target,omitempty"`
	ActiveHandshakeID string                     `json:"ActiveHandshakeId,omitempty"`
	ARN               string                     `json:"Arn,omitempty"`
	ID                string                     `json:"Id,omitempty"`
	Name              string                     `json:"Name,omitempty"`
	Status            string                     `json:"Status,omitempty"`
	Type              string                     `json:"Type,omitempty"`
	StartTimestamp    float64                    `json:"StartTimestamp"`
}

// -- DescribeResponsibilityTransfer --

type describeResponsibilityTransferRequest struct {
	ID string `json:"Id"`
}

type describeResponsibilityTransferResponse struct {
	ResponsibilityTransfer responsibilityTransferObject `json:"ResponsibilityTransfer"`
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

type listInboundResponsibilityTransfersRequest struct {
	Type       string `json:"Type"`
	ID         string `json:"Id,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listInboundResponsibilityTransfersResponse struct {
	NextToken               string                         `json:"NextToken,omitempty"`
	ResponsibilityTransfers []responsibilityTransferObject `json:"ResponsibilityTransfers"`
}

// -- ListOutboundResponsibilityTransfers --

type listOutboundResponsibilityTransfersRequest struct {
	Type       string `json:"Type"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listOutboundResponsibilityTransfersResponse struct {
	NextToken               string                         `json:"NextToken,omitempty"`
	ResponsibilityTransfers []responsibilityTransferObject `json:"ResponsibilityTransfers"`
}

// -- TerminateResponsibilityTransfer --

type terminateResponsibilityTransferRequest struct {
	EndTimestamp *float64 `json:"EndTimestamp,omitempty"`
	ID           string   `json:"Id"`
}

type terminateResponsibilityTransferResponse struct {
	ResponsibilityTransfer responsibilityTransferObject `json:"ResponsibilityTransfer"`
}

// -- UpdateResponsibilityTransfer --

type updateResponsibilityTransferRequest struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type updateResponsibilityTransferResponse struct {
	ResponsibilityTransfer responsibilityTransferObject `json:"ResponsibilityTransfer"`
}

// -- InviteOrganizationToTransferResponsibility --

type inviteOrganizationToTransferResponsibilityRequest struct {
	Target         HandshakeParty `json:"Target"`
	SourceName     string         `json:"SourceName"`
	Type           string         `json:"Type"`
	Notes          string         `json:"Notes,omitempty"`
	StartTimestamp float64        `json:"StartTimestamp"`
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

	if req.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Id is required")
	}

	rt, err := h.Backend.DescribeResponsibilityTransfer(req.ID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		describeResponsibilityTransferResponse{ResponsibilityTransfer: toResponsibilityTransferObject(rt)},
	)
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

	if req.Target.Type == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Type is required")
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

	if req.Target.Type == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Target.Type is required")
	}

	if req.SourceName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "SourceName is required")
	}

	if req.StartTimestamp == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "StartTimestamp is required")
	}

	if req.Type == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Type is required")
	}

	params := TransferResponsibilityParams{
		SourceName:     req.SourceName,
		StartTimestamp: time.Unix(int64(req.StartTimestamp), 0).UTC(),
		Type:           req.Type,
		Notes:          req.Notes,
	}

	hs, err := h.Backend.InviteOrganizationToTransferResponsibility(req.Target, params)
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

func (h *Handler) handleListInboundResponsibilityTransfers(c *echo.Context, body []byte) error {
	var req listInboundResponsibilityTransfersRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	if req.Type == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Type is required")
	}

	transfers, err := h.Backend.ListInboundResponsibilityTransfers(req.Type, req.ID)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]responsibilityTransferObject, 0, len(transfers))
	for _, rt := range transfers {
		objs = append(objs, toResponsibilityTransferObject(rt))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(
		http.StatusOK,
		listInboundResponsibilityTransfersResponse{ResponsibilityTransfers: p.Data, NextToken: p.Next},
	)
}

func (h *Handler) handleListOutboundResponsibilityTransfers(c *echo.Context, body []byte) error {
	var req listOutboundResponsibilityTransfersRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	if req.Type == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Type is required")
	}

	transfers, err := h.Backend.ListOutboundResponsibilityTransfers(req.Type)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]responsibilityTransferObject, 0, len(transfers))
	for _, rt := range transfers {
		objs = append(objs, toResponsibilityTransferObject(rt))
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(
		http.StatusOK,
		listOutboundResponsibilityTransfersResponse{ResponsibilityTransfers: p.Data, NextToken: p.Next},
	)
}

func (h *Handler) handleTerminateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req terminateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Id is required")
	}

	var endTimestamp *time.Time
	if req.EndTimestamp != nil {
		t := time.Unix(int64(*req.EndTimestamp), 0).UTC()
		endTimestamp = &t
	}

	rt, err := h.Backend.TerminateResponsibilityTransfer(req.ID, endTimestamp)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		terminateResponsibilityTransferResponse{ResponsibilityTransfer: toResponsibilityTransferObject(rt)},
	)
}

func (h *Handler) handleUpdateResponsibilityTransfer(c *echo.Context, body []byte) error {
	var req updateResponsibilityTransferRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Id is required")
	}

	if req.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Name is required")
	}

	rt, err := h.Backend.UpdateResponsibilityTransfer(req.ID, req.Name)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		updateResponsibilityTransferResponse{ResponsibilityTransfer: toResponsibilityTransferObject(rt)},
	)
}

// ----------------------------------------
// ResponsibilityTransfer conversion helpers
// ----------------------------------------

func toTransferParticipantObject(p TransferParticipant) *transferParticipantObject {
	if p.ManagementAccountID == "" && p.ManagementAccountEmail == "" {
		return nil
	}

	return &transferParticipantObject{
		ManagementAccountID:    p.ManagementAccountID,
		ManagementAccountEmail: p.ManagementAccountEmail,
	}
}

func toResponsibilityTransferObject(rt *ResponsibilityTransfer) responsibilityTransferObject {
	obj := responsibilityTransferObject{
		ActiveHandshakeID: rt.ActiveHandshakeID,
		ARN:               rt.ARN,
		ID:                rt.ID,
		Name:              rt.Name,
		Source:            toTransferParticipantObject(rt.Source),
		StartTimestamp:    epochSeconds(rt.StartTimestamp),
		Status:            rt.Status,
		Target:            toTransferParticipantObject(rt.Target),
		Type:              rt.Type,
	}

	if !rt.EndTimestamp.IsZero() {
		end := epochSeconds(rt.EndTimestamp)
		obj.EndTimestamp = &end
	}

	return obj
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
