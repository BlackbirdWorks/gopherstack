package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createWirelessGatewayTaskResponse struct {
	WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	WirelessGatewayID               string `json:"WirelessGatewayId"`
	Status                          string `json:"Status"`
}

type createWirelessGatewayTaskDefinitionResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type getWirelessGatewayTaskResponse struct {
	WirelessGatewayID               string `json:"WirelessGatewayId"`
	WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	LastUplinkReceivedAt            string `json:"LastUplinkReceivedAt"`
	TaskCreatedAt                   string `json:"TaskCreatedAt"`
	Status                          string `json:"Status"`
}

type getWirelessGatewayTaskDefinitionResponse struct {
	Arn             string `json:"Arn"`
	Name            string `json:"Name"`
	AutoCreateTasks bool   `json:"AutoCreateTasks"`
}

func (h *Handler) createWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	var req struct {
		WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.CreateWirelessGatewayTask(gatewayID, req.WirelessGatewayTaskDefinitionID)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskResponse{
		WirelessGatewayID:               task.WirelessGatewayID,
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	})
}

func (h *Handler) getWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	task, err := h.Backend.GetWirelessGatewayTask(gatewayID)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskResponse{
		WirelessGatewayID:               task.WirelessGatewayID,
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	})
}

func (h *Handler) deleteWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	// Ignore not-found for idempotency.
	_ = h.Backend.DeleteWirelessGatewayTask(gatewayID)

	return stubNoContent(c)
}

func (h *Handler) createWirelessGatewayTaskDefinition(c *echo.Context) error {
	var req struct {
		Name            string `json:"Name"`
		AutoCreateTasks bool   `json:"AutoCreateTasks"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	def, err := h.Backend.CreateWirelessGatewayTaskDefinition(
		h.AccountID,
		h.DefaultRegion,
		req.Name,
		req.AutoCreateTasks,
	)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusCreated, createWirelessGatewayTaskDefinitionResponse{
		Arn: def.ARN,
		ID:  def.ID,
	})
}

func (h *Handler) getWirelessGatewayTaskDefinition(c *echo.Context, id string) error {
	def, err := h.Backend.GetWirelessGatewayTaskDefinition(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayTaskDefinitionResponse{
		Arn:             def.ARN,
		Name:            def.Name,
		AutoCreateTasks: def.AutoCreateTasks,
	})
}

func (h *Handler) listWirelessGatewayTaskDefinitions(c *echo.Context) error {
	defs := h.Backend.ListWirelessGatewayTaskDefinitions()

	type taskDefEntry struct {
		ID              string `json:"Id"`
		Arn             string `json:"Arn"`
		Name            string `json:"Name"`
		AutoCreateTasks bool   `json:"AutoCreateTasks"`
	}

	entries := make([]taskDefEntry, 0, len(defs))

	for _, def := range defs {
		entries = append(entries, taskDefEntry{
			ID:              def.ID,
			Arn:             def.ARN,
			Name:            def.Name,
			AutoCreateTasks: def.AutoCreateTasks,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"TaskDefinitions": entries,
		"NextToken":       "",
	})
}

func (h *Handler) deleteWirelessGatewayTaskDefinition(c *echo.Context, id string) error {
	// Ignore not-found; idempotent delete.
	_ = h.Backend.DeleteWirelessGatewayTaskDefinition(id)

	return stubNoContent(c)
}
