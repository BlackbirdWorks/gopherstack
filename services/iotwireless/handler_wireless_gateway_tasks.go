package iotwireless

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type createWirelessGatewayTaskResponse struct {
	WirelessGatewayTaskDefinitionID string `json:"WirelessGatewayTaskDefinitionId"`
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
	Update          map[string]any `json:"Update,omitempty"`
	Arn             string         `json:"Arn"`
	Name            string         `json:"Name"`
	AutoCreateTasks bool           `json:"AutoCreateTasks"`
}

// taskDefEntry mirrors real AWS's UpdateWirelessGatewayTaskEntry list-entry
// shape: Arn, Id, LoRaWAN only. Name/AutoCreateTasks are NOT present on list
// entries even though they are on Create/Get -- confirmed against
// types.UpdateWirelessGatewayTaskEntry.
type taskDefEntry struct {
	LoRaWAN map[string]any `json:"LoRaWAN,omitempty"`
	ID      string         `json:"Id"`
	Arn     string         `json:"Arn"`
}

type listWirelessGatewayTaskDefinitionsResponse struct {
	NextToken       string         `json:"NextToken"`
	TaskDefinitions []taskDefEntry `json:"TaskDefinitions"`
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
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	})
}

func (h *Handler) getWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	task, err := h.Backend.GetWirelessGatewayTask(gatewayID)
	if err != nil {
		return handleError(c, err)
	}

	resp := getWirelessGatewayTaskResponse{
		WirelessGatewayID:               task.WirelessGatewayID,
		WirelessGatewayTaskDefinitionID: task.TaskDefID,
		Status:                          task.Status,
	}
	if !task.CreatedAt.IsZero() {
		resp.TaskCreatedAt = task.CreatedAt.UTC().Format(time.RFC3339)
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) deleteWirelessGatewayTask(c *echo.Context, gatewayID string) error {
	// Ignore not-found for idempotency.
	_ = h.Backend.DeleteWirelessGatewayTask(gatewayID)

	return stubNoContent(c)
}

func (h *Handler) createWirelessGatewayTaskDefinition(c *echo.Context) error {
	var req struct {
		Update          map[string]any `json:"Update"`
		Name            string         `json:"Name"`
		AutoCreateTasks bool           `json:"AutoCreateTasks"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	def, err := h.Backend.CreateWirelessGatewayTaskDefinition(
		h.AccountID,
		h.DefaultRegion,
		req.Name,
		req.AutoCreateTasks,
		req.Update,
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
		Update:          def.Update,
		AutoCreateTasks: def.AutoCreateTasks,
	})
}

func (h *Handler) listWirelessGatewayTaskDefinitions(c *echo.Context) error {
	defs := h.Backend.ListWirelessGatewayTaskDefinitions()
	page, next := paginateQuery(c, defs)

	entries := make([]taskDefEntry, 0, len(page))

	for _, def := range page {
		var loRaWAN map[string]any
		if update, ok := def.Update["LoRaWAN"].(map[string]any); ok {
			loRaWAN = update
		}

		entries = append(entries, taskDefEntry{
			ID:      def.ID,
			Arn:     def.ARN,
			LoRaWAN: loRaWAN,
		})
	}

	return writeJSON(c, http.StatusOK, listWirelessGatewayTaskDefinitionsResponse{
		TaskDefinitions: entries,
		NextToken:       next,
	})
}

func (h *Handler) deleteWirelessGatewayTaskDefinition(c *echo.Context, id string) error {
	// Ignore not-found; idempotent delete.
	_ = h.Backend.DeleteWirelessGatewayTaskDefinition(id)

	return stubNoContent(c)
}
