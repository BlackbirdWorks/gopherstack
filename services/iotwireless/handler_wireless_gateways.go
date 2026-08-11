package iotwireless

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createWirelessGatewayRequest struct {
	LoRaWAN     *LoRaWANGateway `json:"LoRaWAN,omitempty"`
	Name        string          `json:"Name"`
	Description string          `json:"Description"`
	Tags        []tags.KV       `json:"Tags"`
}

type createWirelessGatewayResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type wirelessGatewayEntry struct {
	LoRaWAN     *LoRaWANGateway `json:"LoRaWAN,omitempty"`
	Arn         string          `json:"Arn"`
	ID          string          `json:"Id"`
	Name        string          `json:"Name"`
	Description string          `json:"Description"`
	ThingArn    string          `json:"ThingArn,omitempty"`
	ThingName   string          `json:"ThingName,omitempty"`
}

type listWirelessGatewaysResponse struct {
	NextToken           string                 `json:"NextToken"`
	WirelessGatewayList []wirelessGatewayEntry `json:"WirelessGatewayList"`
}

type associateWirelessGatewayWithThingRequest struct {
	ThingArn string `json:"ThingArn"`
}

type getWirelessGatewayStatisticsResponse struct {
	WirelessGatewayID    string `json:"WirelessGatewayId,omitempty"`
	LastUplinkReceivedAt string `json:"LastUplinkReceivedAt,omitempty"`
	ConnectionStatus     string `json:"ConnectionStatus,omitempty"`
}

// --- Wireless Gateway handlers ---

func (h *Handler) createWirelessGateway(c *echo.Context, body []byte) error {
	var req createWirelessGatewayRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	gw, err := h.Backend.CreateWirelessGateway(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.LoRaWAN, tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayResponse{Arn: gw.ARN, ID: gw.ID})
}

func (h *Handler) getWirelessGateway(c *echo.Context, id string) error {
	gw, err := h.Backend.GetWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	thingArn := h.Backend.GetWirelessGatewayThingArn(id)

	return writeJSON(c, http.StatusOK, wirelessGatewayEntry{
		Arn:         gw.ARN,
		ID:          gw.ID,
		Name:        gw.Name,
		Description: gw.Description,
		LoRaWAN:     gw.LoRaWAN,
		ThingArn:    thingArn,
		ThingName:   thingNameFromArn(thingArn),
	})
}

func (h *Handler) listWirelessGateways(c *echo.Context) error {
	gws := h.Backend.ListWirelessGateways(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, gws)

	entries := make([]wirelessGatewayEntry, 0, len(pg))

	for _, gw := range pg {
		entries = append(entries, wirelessGatewayEntry{
			Arn:         gw.ARN,
			ID:          gw.ID,
			Name:        gw.Name,
			Description: gw.Description,
			LoRaWAN:     gw.LoRaWAN,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessGatewaysResponse{WirelessGatewayList: entries, NextToken: next})
}

func (h *Handler) deleteWirelessGateway(c *echo.Context, id string) error {
	err := h.Backend.DeleteWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessGatewayWithThing(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithThingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	err := h.Backend.AssociateWirelessGatewayWithThing(h.AccountID, h.DefaultRegion, gatewayID, req.ThingArn)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateWirelessGateway(c *echo.Context, id string) error {
	var req struct {
		MaxEirp        *float32   `json:"MaxEirp"`
		Name           string     `json:"Name"`
		Description    string     `json:"Description"`
		JoinEuiFilters [][]string `json:"JoinEuiFilters"`
		NetIDFilters   []string   `json:"NetIdFilters"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessGateway(
		h.AccountID, h.DefaultRegion, id, req.Name, req.Description,
		req.JoinEuiFilters, req.NetIDFilters, req.MaxEirp,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessGatewayFromThing(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessGatewayFromThing(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getWirelessGatewayStatistics(c *echo.Context, id string) error {
	gw, err := h.Backend.GetWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	resp := getWirelessGatewayStatisticsResponse{
		WirelessGatewayID: gw.ID,
		ConnectionStatus:  gw.ConnectionStatus,
	}
	if gw.LastUplinkReceivedAt != nil {
		resp.LastUplinkReceivedAt = gw.LastUplinkReceivedAt.UTC().Format(time.RFC3339)
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) getWirelessGatewayFirmwareInformation(c *echo.Context, id string) error {
	gw, err := h.Backend.GetWirelessGateway(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"LoRaWAN": map[string]any{
			"CurrentVersion": map[string]any{
				"PackageVersion": gw.FirmwareVersion,
				"Model":          gw.FirmwareModel,
				"Station":        gw.FirmwareStation,
			},
		},
	})
}
