package iotwireless

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createWirelessDeviceRequest struct {
	LoRaWAN         map[string]any `json:"LoRaWAN,omitempty"`
	Sidewalk        map[string]any `json:"Sidewalk,omitempty"`
	Name            string         `json:"Name"`
	Type            string         `json:"Type"`
	DestinationName string         `json:"DestinationName"`
	Description     string         `json:"Description"`
	Positioning     string         `json:"Positioning,omitempty"`
	Tags            []tags.KV      `json:"Tags"`
}

type createWirelessDeviceResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessDeviceEntry struct {
	LoRaWAN         map[string]any `json:"LoRaWAN,omitempty"`
	Sidewalk        map[string]any `json:"Sidewalk,omitempty"`
	Arn             string         `json:"Arn"`
	ID              string         `json:"Id"`
	Name            string         `json:"Name"`
	Type            string         `json:"Type"`
	DestinationName string         `json:"DestinationName"`
	Description     string         `json:"Description"`
	Positioning     string         `json:"Positioning,omitempty"`
	ThingArn        string         `json:"ThingArn,omitempty"`
	ThingName       string         `json:"ThingName,omitempty"`
}

type listWirelessDevicesResponse struct {
	NextToken          string                `json:"NextToken"`
	WirelessDeviceList []wirelessDeviceEntry `json:"WirelessDeviceList"`
}

type associateWirelessDeviceWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
}

type getWirelessDeviceStatisticsResponse struct {
	WirelessDeviceID     string `json:"WirelessDeviceId,omitempty"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt,omitempty"`
}

type downlinkQueueMessageResponse struct {
	MessageID    string `json:"MessageId,omitempty"`
	ReceivedAt   string `json:"ReceivedAt,omitempty"`
	TransmitMode int32  `json:"TransmitMode"`
}

type listQueuedMessagesResponse struct {
	NextToken                 string                         `json:"NextToken"`
	DownlinkQueueMessagesList []downlinkQueueMessageResponse `json:"DownlinkQueueMessagesList"`
}

type sendDataToWirelessDeviceResponse struct {
	MessageID string `json:"MessageId"`
}

type testWirelessDeviceResponse struct {
	Result string `json:"Result"`
}

// --- Wireless Device handlers ---

func (h *Handler) createWirelessDevice(c *echo.Context, body []byte) error {
	var req createWirelessDeviceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	d, err := h.Backend.CreateWirelessDevice(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Type, req.DestinationName, req.Description, req.Positioning,
		req.LoRaWAN, req.Sidewalk,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessDeviceResponse{Arn: d.ARN, ID: d.ID})
}

func (h *Handler) getWirelessDevice(c *echo.Context, id string) error {
	d, err := h.Backend.GetWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	thingArn := h.Backend.GetWirelessDeviceThingArn(id)

	entry := wirelessDeviceEntryFrom(d)
	entry.ThingArn = thingArn
	entry.ThingName = thingNameFromArn(thingArn)

	return writeJSON(c, http.StatusOK, entry)
}

func (h *Handler) listWirelessDevices(c *echo.Context) error {
	devices := h.Backend.ListWirelessDevices(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, devices)

	entries := make([]wirelessDeviceEntry, 0, len(pg))

	for _, d := range pg {
		entries = append(entries, wirelessDeviceEntryFrom(d))
	}

	return writeJSON(c, http.StatusOK, listWirelessDevicesResponse{
		WirelessDeviceList: entries,
		NextToken:          next,
	})
}

// wirelessDeviceEntryFrom builds a wirelessDeviceEntry from a backend
// WirelessDevice, including the LoRaWAN/Sidewalk/Positioning fields real
// AWS's WirelessDeviceStatistics list-entry shape carries.
func wirelessDeviceEntryFrom(d *WirelessDevice) wirelessDeviceEntry {
	return wirelessDeviceEntry{
		Arn:             d.ARN,
		ID:              d.ID,
		Name:            d.Name,
		Type:            d.Type,
		DestinationName: d.DestinationName,
		Description:     d.Description,
		LoRaWAN:         d.LoRaWAN,
		Sidewalk:        d.Sidewalk,
		Positioning:     d.Positioning,
	}
}

func (h *Handler) deleteWirelessDevice(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithThing(c *echo.Context, wirelessDeviceID string, body []byte) error {
	var req associateWirelessDeviceWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessDeviceWithThing(h.AccountID, h.DefaultRegion, wirelessDeviceID, req.ThingArn)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateWirelessDevice(c *echo.Context, id string) error {
	var req struct {
		LoRaWAN         map[string]any `json:"LoRaWAN"`
		Sidewalk        map[string]any `json:"Sidewalk"`
		Name            string         `json:"Name"`
		Description     string         `json:"Description"`
		DestinationName string         `json:"DestinationName"`
		Positioning     string         `json:"Positioning"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessDevice(
		h.AccountID, h.DefaultRegion, id,
		req.Name, req.Description, req.DestinationName, req.Positioning,
		req.LoRaWAN, req.Sidewalk,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) deregisterWirelessDevice(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWirelessDevice(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessDeviceFromThing(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessDeviceFromThing(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) sendDataToWirelessDevice(c *echo.Context, wirelessDeviceID string) error {
	var req struct {
		PayloadData  string `json:"PayloadData"`
		TransmitMode int32  `json:"TransmitMode"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	messageID := uuid.NewString()

	// Real cross-op state: queue the downlink message so a subsequent
	// ListQueuedMessages reflects it, instead of silently discarding it.
	// TransmitMode is captured too -- ListQueuedMessages' DownlinkQueueMessage
	// response echoes it back, so previously every queued message reported
	// TransmitMode 0 regardless of what the client actually requested.
	h.Backend.EnqueueMessage(wirelessDeviceID, QueuedMessage{
		MessageID:     messageID,
		PayloadBase64: req.PayloadData,
		TransmitMode:  req.TransmitMode,
		ReceivedAt:    time.Now(),
	})

	return writeJSON(c, http.StatusCreated, sendDataToWirelessDeviceResponse{
		MessageID: messageID,
	})
}

func (h *Handler) getWirelessDeviceStatistics(c *echo.Context, id string) error {
	dev, err := h.Backend.GetWirelessDevice(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	resp := getWirelessDeviceStatisticsResponse{WirelessDeviceID: dev.ID}
	if dev.LastUplinkReceivedAt != nil {
		resp.LastUplinkReceivedAt = dev.LastUplinkReceivedAt.UTC().Format(time.RFC3339)
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) testWirelessDevice(c *echo.Context, id string) error {
	if _, err := h.Backend.GetWirelessDevice(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, testWirelessDeviceResponse{
		Result: "PASS",
	})
}

func (h *Handler) listQueuedMessages(c *echo.Context, wirelessDeviceID string) error {
	msgs := h.Backend.ListQueuedMessages(wirelessDeviceID)
	pg, next := paginateQuery(c, msgs)

	items := make([]downlinkQueueMessageResponse, 0, len(pg))
	for _, m := range pg {
		items = append(items, downlinkQueueMessageResponse{
			MessageID:    m.MessageID,
			ReceivedAt:   m.ReceivedAt.UTC().Format(time.RFC3339),
			TransmitMode: m.TransmitMode,
		})
	}

	return writeJSON(c, http.StatusOK, listQueuedMessagesResponse{
		DownlinkQueueMessagesList: items,
		NextToken:                 next,
	})
}

func (h *Handler) deleteQueuedMessages(c *echo.Context, wirelessDeviceID string) error {
	if err := h.Backend.DeleteQueuedMessages(wirelessDeviceID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
