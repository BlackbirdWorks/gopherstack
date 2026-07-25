package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createFuotaTaskRequest struct {
	LoRaWAN             map[string]any `json:"LoRaWAN,omitempty"`
	Name                string         `json:"Name"`
	Description         string         `json:"Description"`
	FirmwareUpdateImage string         `json:"FirmwareUpdateImage"`
	FirmwareUpdateRole  string         `json:"FirmwareUpdateRole"`
	Descriptor          string         `json:"Descriptor,omitempty"`
	Tags                []tags.KV      `json:"Tags"`
	FragmentIntervalMS  int32          `json:"FragmentIntervalMS,omitempty"`
	FragmentSizeBytes   int32          `json:"FragmentSizeBytes,omitempty"`
	RedundancyPercent   int32          `json:"RedundancyPercent,omitempty"`
}

type createFuotaTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

// fuotaTaskEntry is the Get response shape. List entries use the narrower
// fuotaTaskListEntry instead -- confirmed against types.FuotaTask, which
// real AWS's ListFuotaTasksOutput uses and which carries only Arn/Id/Name.
type fuotaTaskEntry struct {
	LoRaWAN             map[string]any `json:"LoRaWAN,omitempty"`
	Arn                 string         `json:"Arn"`
	ID                  string         `json:"Id"`
	Name                string         `json:"Name"`
	Description         string         `json:"Description,omitempty"`
	FirmwareUpdateImage string         `json:"FirmwareUpdateImage,omitempty"`
	FirmwareUpdateRole  string         `json:"FirmwareUpdateRole,omitempty"`
	Descriptor          string         `json:"Descriptor,omitempty"`
	Status              string         `json:"Status,omitempty"`
	CreatedAt           float64        `json:"CreatedAt,omitempty"`
	FragmentIntervalMS  int32          `json:"FragmentIntervalMS,omitempty"`
	FragmentSizeBytes   int32          `json:"FragmentSizeBytes,omitempty"`
	RedundancyPercent   int32          `json:"RedundancyPercent,omitempty"`
}

type fuotaTaskListEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listFuotaTasksResponse struct {
	NextToken     string               `json:"NextToken"`
	FuotaTaskList []fuotaTaskListEntry `json:"FuotaTaskList"`
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
		req.Name, req.Description, req.FirmwareUpdateImage, req.FirmwareUpdateRole, req.Descriptor,
		req.FragmentIntervalMS, req.FragmentSizeBytes, req.RedundancyPercent,
		req.LoRaWAN,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createFuotaTaskResponse{Arn: ft.ARN, ID: ft.ID})
}

// fuotaTaskEntryFrom builds the Get response shape from a backend FuotaTask,
// including the epoch-seconds CreatedAt timestamp (this REST-JSON service's
// wire format for Timestamp shapes; see pkgs/awstime).
func fuotaTaskEntryFrom(ft *FuotaTask) fuotaTaskEntry {
	entry := fuotaTaskEntry{
		Arn:                 ft.ARN,
		ID:                  ft.ID,
		Name:                ft.Name,
		Description:         ft.Description,
		FirmwareUpdateImage: ft.FirmwareUpdateImage,
		FirmwareUpdateRole:  ft.FirmwareUpdateRole,
		Descriptor:          ft.Descriptor,
		Status:              ft.Status,
		FragmentIntervalMS:  ft.FragmentIntervalMS,
		FragmentSizeBytes:   ft.FragmentSizeBytes,
		RedundancyPercent:   ft.RedundancyPercent,
		LoRaWAN:             ft.LoRaWAN,
	}
	if !ft.CreatedAt.IsZero() {
		entry.CreatedAt = awstime.Epoch(ft.CreatedAt)
	}

	return entry
}

func (h *Handler) getFuotaTask(c *echo.Context, id string) error {
	ft, err := h.Backend.GetFuotaTask(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, fuotaTaskEntryFrom(ft))
}

func (h *Handler) listFuotaTasks(c *echo.Context) error {
	tasks := h.Backend.ListFuotaTasks(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, tasks)

	entries := make([]fuotaTaskListEntry, 0, len(pg))

	for _, ft := range pg {
		entries = append(entries, fuotaTaskListEntry{
			Arn:  ft.ARN,
			ID:   ft.ID,
			Name: ft.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listFuotaTasksResponse{FuotaTaskList: entries, NextToken: next})
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

// disassociateWirelessDeviceFromFuotaTask removes the association between a
// FUOTA task and a single wireless device. wirelessDeviceID is the
// {WirelessDeviceId} path segment (DELETE
// /fuota-tasks/{Id}/wireless-devices/{WirelessDeviceId}), recovered by the
// caller via lastPathSegment since parseIoTWirelessPath's (op, resource)
// pair only carries the top-level FUOTA task ID.
func (h *Handler) disassociateWirelessDeviceFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, wirelessDeviceID string,
) error {
	if err := h.Backend.DisassociateWirelessDeviceFromFuotaTask(fuotaTaskID, wirelessDeviceID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listMulticastGroupsByFuotaTask(c *echo.Context, fuotaTaskID string) error {
	groups := h.Backend.ListMulticastGroupsByFuotaTask(h.AccountID, h.DefaultRegion, fuotaTaskID)
	pg, next := paginateQuery(c, groups)

	entries := make([]multicastGroupEntry, 0, len(pg))

	for _, mg := range pg {
		entries = append(entries, multicastGroupEntry{
			Arn:  mg.ARN,
			ID:   mg.ID,
			Name: mg.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listMulticastGroupsByFuotaTaskResponse{
		MulticastGroupList: entries,
		NextToken:          next,
	})
}

// disassociateMulticastGroupFromFuotaTask removes the association between a
// FUOTA task and a single multicast group. multicastGroupID is the
// {MulticastGroupId} path segment (DELETE
// /fuota-tasks/{Id}/multicast-groups/{MulticastGroupId}), recovered by the
// caller via lastPathSegment -- see disassociateWirelessDeviceFromFuotaTask.
func (h *Handler) disassociateMulticastGroupFromFuotaTask(
	c *echo.Context,
	fuotaTaskID, multicastGroupID string,
) error {
	if err := h.Backend.DisassociateMulticastGroupFromFuotaTask(fuotaTaskID, multicastGroupID); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
