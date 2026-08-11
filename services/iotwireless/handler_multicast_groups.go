package iotwireless

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type associateWirelessDeviceWithMulticastRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type createMulticastGroupResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type getMulticastGroupResponse struct {
	LoRaWAN     *LoRaWANMulticastGet `json:"LoRaWAN,omitempty"`
	Arn         string               `json:"Arn"`
	ID          string               `json:"Id"`
	Name        string               `json:"Name"`
	Description string               `json:"Description,omitempty"`
	Status      string               `json:"Status"`
	CreatedAt   float64              `json:"CreatedAt,omitempty"`
}

type getMulticastGroupSessionResponse struct {
	LoRaWAN map[string]any `json:"LoRaWAN"`
}

type multicastGroupEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listMulticastGroupsResponse struct {
	NextToken          string                `json:"NextToken"`
	MulticastGroupList []multicastGroupEntry `json:"MulticastGroupList"`
}

type sendDataToMulticastGroupResponse struct {
	MessageID string `json:"MessageId"`
}

func (h *Handler) createMulticastGroup(c *echo.Context) error {
	var req struct {
		LoRaWAN     *LoRaWANMulticast `json:"LoRaWAN,omitempty"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		Tags        []tags.KV         `json:"Tags"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	mg, err := h.Backend.CreateMulticastGroup(
		h.AccountID,
		h.DefaultRegion,
		req.Name,
		req.Description,
		req.LoRaWAN,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createMulticastGroupResponse{
		Arn: mg.ARN,
		ID:  mg.ID,
	})
}

func (h *Handler) getMulticastGroup(c *echo.Context, id string) error {
	mg, err := h.Backend.GetMulticastGroup(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	//nolint:gosec // device count is bounded by real associations
	devicesInGroup := int32(len(h.Backend.ListMulticastGroupDeviceIDs(mg.ID)))

	resp := getMulticastGroupResponse{
		Arn:         mg.ARN,
		ID:          mg.ID,
		Name:        mg.Name,
		Description: mg.Description,
		Status:      mg.Status,
		LoRaWAN:     loRaWANMulticastGetFrom(mg.LoRaWAN, devicesInGroup),
	}
	if !mg.CreatedAt.IsZero() {
		resp.CreatedAt = awstime.Epoch(mg.CreatedAt)
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) listMulticastGroups(c *echo.Context) error {
	groups := h.Backend.ListMulticastGroups(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, groups)

	entries := make([]multicastGroupEntry, 0, len(pg))

	for _, mg := range pg {
		entries = append(entries, multicastGroupEntry{
			Arn:  mg.ARN,
			ID:   mg.ID,
			Name: mg.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listMulticastGroupsResponse{
		MulticastGroupList: entries,
		NextToken:          next,
	})
}

func (h *Handler) deleteMulticastGroup(c *echo.Context, id string) error {
	if err := h.Backend.DeleteMulticastGroup(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateMulticastGroup(c *echo.Context, id string) error {
	var req struct {
		LoRaWAN     *LoRaWANMulticast `json:"LoRaWAN,omitempty"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateMulticastGroup(
		h.AccountID, h.DefaultRegion, id, req.Name, req.Description, req.LoRaWAN,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// disassociateWirelessDeviceFromMulticastGroup removes the association
// between a multicast group and a single wireless device. wirelessDeviceID
// is the {WirelessDeviceId} path segment (DELETE
// /multicast-groups/{Id}/wireless-devices/{WirelessDeviceId}), recovered by
// the caller via lastPathSegment since parseIoTWirelessPath's (op, resource)
// pair only carries the top-level multicast group ID.
func (h *Handler) disassociateWirelessDeviceFromMulticastGroup(
	c *echo.Context,
	multicastGroupID, wirelessDeviceID string,
) error {
	if err := h.Backend.DisassociateWirelessDeviceFromMulticastGroup(multicastGroupID, wirelessDeviceID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getMulticastGroupSession(c *echo.Context, id string) error {
	startedAt, err := h.Backend.GetMulticastGroupSession(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getMulticastGroupSessionResponse{
		LoRaWAN: map[string]any{
			"SessionStartTime": startedAt.Format(time.RFC3339),
		},
	})
}

func (h *Handler) startMulticastGroupSession(c *echo.Context, id string) error {
	if err := h.Backend.StartMulticastGroupSession(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) sendDataToMulticastGroup(c *echo.Context, _ string) error {
	return writeJSON(c, http.StatusCreated, sendDataToMulticastGroupResponse{
		MessageID: uuid.NewString(),
	})
}

func (h *Handler) associateWirelessDeviceWithMulticastGroup(
	c *echo.Context, multicastGroupID string, body []byte,
) error {
	var req associateWirelessDeviceWithMulticastRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// startBulkAssociateWirelessDeviceWithMulticastGroup emulates real AWS's
// "associate every qualifying device" bulk operation. Real AWS filters
// candidates using the request's QueryString search expression; this
// backend has no expression evaluator, so it associates every wireless
// device in the account/region (see
// StartBulkAssociateWirelessDeviceWithMulticastGroup's doc comment).
// Previously this returned 204 without mutating any state at all.
func (h *Handler) startBulkAssociateWirelessDeviceWithMulticastGroup(c *echo.Context, multicastGroupID string) error {
	if err := h.Backend.StartBulkAssociateWirelessDeviceWithMulticastGroup(
		h.AccountID, h.DefaultRegion, multicastGroupID,
	); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// startBulkDisassociateWirelessDeviceFromMulticastGroup emulates real AWS's
// "disassociate every qualifying device" bulk operation, matching the same
// full-corpus semantics as startBulkAssociateWirelessDeviceWithMulticastGroup.
func (h *Handler) startBulkDisassociateWirelessDeviceFromMulticastGroup(
	c *echo.Context,
	multicastGroupID string,
) error {
	if err := h.Backend.StartBulkDisassociateWirelessDeviceFromMulticastGroup(multicastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) cancelMulticastGroupSession(c *echo.Context, multicastGroupID string) error {
	if err := h.Backend.CancelMulticastGroupSession(multicastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
