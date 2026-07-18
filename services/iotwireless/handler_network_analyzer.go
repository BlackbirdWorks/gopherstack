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
	Arn              string   `json:"Arn"`
	Name             string   `json:"Name"`
	Description      string   `json:"Description"`
	WirelessDevices  []string `json:"WirelessDevices"`
	WirelessGateways []string `json:"WirelessGateways"`
}

type listNetworkAnalyzerConfigurationsResponse struct {
	NextToken                        string `json:"NextToken"`
	NetworkAnalyzerConfigurationList []struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	} `json:"NetworkAnalyzerConfigurationList"`
}

func (h *Handler) createNetworkAnalyzerConfiguration(c *echo.Context) error {
	var req struct {
		Description      string    `json:"Description"`
		Name             string    `json:"Name"`
		WirelessDevices  []string  `json:"WirelessDevices"`
		WirelessGateways []string  `json:"WirelessGateways"`
		Tags             []tags.KV `json:"Tags"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	nc, err := h.Backend.CreateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description,
		req.WirelessDevices, req.WirelessGateways,
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
	})
}

func (h *Handler) listNetworkAnalyzerConfigurations(c *echo.Context) error {
	configs := h.Backend.ListNetworkAnalyzerConfigs(h.AccountID, h.DefaultRegion)

	entries := make([]struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
	}, 0, len(configs))

	for _, nc := range configs {
		entries = append(entries, struct {
			Arn  string `json:"Arn"`
			Name string `json:"Name"`
		}{Arn: nc.ARN, Name: nc.Name})
	}

	return writeJSON(c, http.StatusOK, listNetworkAnalyzerConfigurationsResponse{
		NetworkAnalyzerConfigurationList: entries,
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
		Description      string   `json:"Description"`
		WirelessDevices  []string `json:"WirelessDevices"`
		WirelessGateways []string `json:"WirelessGateways"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateNetworkAnalyzerConfig(
		h.AccountID, h.DefaultRegion, name,
		req.Description, req.WirelessDevices, req.WirelessGateways,
	); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
