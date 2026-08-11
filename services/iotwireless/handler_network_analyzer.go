package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createNetworkAnalyzerConfigurationResponse struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

type getNetworkAnalyzerConfigurationResponse struct {
	TraceContent     *TraceContent `json:"TraceContent,omitempty"`
	Arn              string        `json:"Arn"`
	Name             string        `json:"Name"`
	Description      string        `json:"Description"`
	WirelessDevices  []string      `json:"WirelessDevices"`
	WirelessGateways []string      `json:"WirelessGateways"`
	MulticastGroups  []string      `json:"MulticastGroups"`
}

type networkAnalyzerConfigListEntry struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

type listNetworkAnalyzerConfigurationsResponse struct {
	NextToken                        string                           `json:"NextToken"`
	NetworkAnalyzerConfigurationList []networkAnalyzerConfigListEntry `json:"NetworkAnalyzerConfigurationList"`
}

func (h *Handler) createNetworkAnalyzerConfiguration(c *echo.Context) error {
	var req struct {
		TraceContent     *TraceContent `json:"TraceContent,omitempty"`
		Description      string        `json:"Description"`
		Name             string        `json:"Name"`
		WirelessDevices  []string      `json:"WirelessDevices"`
		WirelessGateways []string      `json:"WirelessGateways"`
		MulticastGroups  []string      `json:"MulticastGroups"`
		Tags             []tags.KV     `json:"Tags"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	nc, err := h.Backend.CreateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description,
		req.WirelessDevices, req.WirelessGateways, req.MulticastGroups,
		req.TraceContent,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createNetworkAnalyzerConfigurationResponse{
		Arn:  nc.ARN,
		Name: nc.Name,
	})
}

func (h *Handler) getNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	nc, err := h.Backend.GetNetworkAnalyzerConfig(h.AccountID, h.DefaultRegion, name)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getNetworkAnalyzerConfigurationResponse{
		Arn:              nc.ARN,
		Name:             nc.Name,
		Description:      nc.Description,
		WirelessDevices:  nc.WirelessDevices,
		WirelessGateways: nc.WirelessGateways,
		MulticastGroups:  nc.MulticastGroups,
		TraceContent:     nc.TraceContent,
	})
}

func (h *Handler) listNetworkAnalyzerConfigurations(c *echo.Context) error {
	configs := h.Backend.ListNetworkAnalyzerConfigs(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, configs)

	entries := make([]networkAnalyzerConfigListEntry, 0, len(pg))

	for _, nc := range pg {
		entries = append(entries, networkAnalyzerConfigListEntry{Arn: nc.ARN, Name: nc.Name})
	}

	return writeJSON(c, http.StatusOK, listNetworkAnalyzerConfigurationsResponse{
		NetworkAnalyzerConfigurationList: entries,
		NextToken:                        next,
	})
}

func (h *Handler) deleteNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeleteNetworkAnalyzerConfig(h.AccountID, h.DefaultRegion, name); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateNetworkAnalyzerConfiguration(c *echo.Context, name string) error {
	var req struct {
		TraceContent     *TraceContent `json:"TraceContent,omitempty"`
		Description      string        `json:"Description"`
		WirelessDevices  []string      `json:"WirelessDevices"`
		WirelessGateways []string      `json:"WirelessGateways"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion, name,
		req.Description, req.WirelessDevices, req.WirelessGateways,
		req.TraceContent,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
