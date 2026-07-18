package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createFuotaTaskRequest struct {
	Name                string    `json:"Name"`
	Description         string    `json:"Description"`
	FirmwareUpdateImage string    `json:"FirmwareUpdateImage"`
	FirmwareUpdateRole  string    `json:"FirmwareUpdateRole"`
	Tags                []tags.KV `json:"Tags"`
}

type createFuotaTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type fuotaTaskEntry struct {
	Arn                 string `json:"Arn"`
	ID                  string `json:"Id"`
	Name                string `json:"Name"`
	Description         string `json:"Description,omitempty"`
	FirmwareUpdateImage string `json:"FirmwareUpdateImage,omitempty"`
	FirmwareUpdateRole  string `json:"FirmwareUpdateRole,omitempty"`
}

type listFuotaTasksResponse struct {
	FuotaTaskList []fuotaTaskEntry `json:"FuotaTaskList"`
}

type associateMulticastGroupRequest struct {
	MulticastGroupID string `json:"MulticastGroupId"`
}

type associateWirelessDeviceWithFuotaRequest struct {
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

type listMulticastGroupsByFuotaTaskResponse struct {
	NextToken          string                `json:"NextToken"`
	MulticastGroupList []multicastGroupEntry `json:"MulticastGroupList"`
}

// --- FUOTA Task handlers ---

func (h *Handler) createFuotaTask(c *echo.Context, body []byte) error {
	var req createFuotaTaskRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	ft, err := h.Backend.CreateFuotaTask(
		h.AccountID, h.DefaultRegion,
		req.Name, req.Description, req.FirmwareUpdateImage, req.FirmwareUpdateRole,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createFuotaTaskResponse{Arn: ft.ARN, ID: ft.ID})
}

func (h *Handler) getFuotaTask(c *echo.Context, id string) error {
	ft, err := h.Backend.GetFuotaTask(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, fuotaTaskEntry{
		Arn:                 ft.ARN,
		ID:                  ft.ID,
		Name:                ft.Name,
		Description:         ft.Description,
		FirmwareUpdateImage: ft.FirmwareUpdateImage,
		FirmwareUpdateRole:  ft.FirmwareUpdateRole,
	})
}

func (h *Handler) listFuotaTasks(c *echo.Context) error {
	tasks := h.Backend.ListFuotaTasks(h.AccountID, h.DefaultRegion)
	entries := make([]fuotaTaskEntry, 0, len(tasks))

	for _, ft := range tasks {
		entries = append(entries, fuotaTaskEntry{
			Arn:                 ft.ARN,
			ID:                  ft.ID,
			Name:                ft.Name,
			Description:         ft.Description,
			FirmwareUpdateImage: ft.FirmwareUpdateImage,
			FirmwareUpdateRole:  ft.FirmwareUpdateRole,
		})
	}

	return writeJSON(c, http.StatusOK, listFuotaTasksResponse{FuotaTaskList: entries})
}

func (h *Handler) deleteFuotaTask(c *echo.Context, id string) error {
	err := h.Backend.DeleteFuotaTask(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateMulticastGroupWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateMulticastGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateMulticastGroupWithFuotaTask(fuotaTaskID, req.MulticastGroupID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) associateWirelessDeviceWithFuotaTask(c *echo.Context, fuotaTaskID string, body []byte) error {
	var req associateWirelessDeviceWithFuotaRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, req.WirelessDeviceID); err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateFuotaTask(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateFuotaTask(h.AccountID, h.DefaultRegion, id, req.Name, req.Description); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) startFuotaTask(c *echo.Context, id string) error {
	if err := h.Backend.StartFuotaTask(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) disassociateWirelessDeviceFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, _ string,
) error {
	if err := h.Backend.DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listMulticastGroupsByFuotaTask(c *echo.Context, fuotaTaskID string) error {
	groups := h.Backend.ListMulticastGroupsByFuotaTask(h.AccountID, h.DefaultRegion, fuotaTaskID)
	entries := make([]multicastGroupEntry, 0, len(groups))

	for _, mg := range groups {
		entries = append(entries, multicastGroupEntry{
			Arn:  mg.ARN,
			ID:   mg.ID,
			Name: mg.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listMulticastGroupsByFuotaTaskResponse{
		MulticastGroupList: entries,
	})
}

func (h *Handler) disassociateMulticastGroupFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, _ string,
) error {
	if err := h.Backend.DisassociateMulticastGroupFromFuotaTask(fuotaTaskID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
