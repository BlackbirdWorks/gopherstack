package iotwireless

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type associateWirelessGatewayWithCertificateRequest struct {
	IotCertificateID string `json:"IotCertificateId"`
}

type associateWirelessGatewayWithCertificateResponse struct {
	IotCertificateArn string `json:"IotCertificateArn"`
}

type getWirelessDeviceImportTaskResponse struct {
	Arn             string `json:"Arn"`
	ID              string `json:"Id"`
	DestinationName string `json:"DestinationName"`
	Status          string `json:"Status"`
	StatusReason    string `json:"StatusReason"`
	// CreationTime is an ISODateTimeString, not an epoch-seconds number --
	// confirmed against awsRestjson1_deserializeOpDocumentGetWirelessDeviceImportTaskOutput,
	// which parses it with smithytime.ParseDateTime (a string), unlike the
	// epoch-seconds CreatedAt fields on FuotaTask/MulticastGroup.
	CreationTime                   string `json:"CreationTime,omitempty"`
	InitializedImportedDeviceCount int64  `json:"InitializedImportedDeviceCount"`
	PendingImportedDeviceCount     int64  `json:"PendingImportedDeviceCount"`
	OnboardedImportedDeviceCount   int64  `json:"OnboardedImportedDeviceCount"`
	FailedImportedDeviceCount      int64  `json:"FailedImportedDeviceCount"`
}

type getWirelessGatewayCertificateResponse struct {
	IotCertificateID                  string `json:"IotCertificateId"`
	LoRaWANNetworkServerCertificateID string `json:"LoRaWANNetworkServerCertificateId"`
}

type listWirelessDeviceImportTasksResponse struct {
	NextToken                    string                                `json:"NextToken"`
	WirelessDeviceImportTaskList []getWirelessDeviceImportTaskResponse `json:"WirelessDeviceImportTaskList"`
}

type listDevicesForWirelessDeviceImportTaskResponse struct {
	NextToken                  string     `json:"NextToken"`
	DestinationName            string     `json:"DestinationName"`
	Positioning                string     `json:"Positioning,omitempty"`
	ImportedWirelessDeviceList []struct{} `json:"ImportedWirelessDeviceList"`
}

type startWirelessDeviceImportTaskResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type startSingleWirelessDeviceImportTaskResponse struct {
	Arn              string `json:"Arn"`
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

func (h *Handler) associateWirelessGatewayWithCertificate(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithCertificateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	certARN, err := h.Backend.AssociateWirelessGatewayWithCertificate(
		h.AccountID, h.DefaultRegion, gatewayID, req.IotCertificateID,
	)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, associateWirelessGatewayWithCertificateResponse{IotCertificateArn: certARN})
}

func (h *Handler) disassociateWirelessGatewayFromCertificate(c *echo.Context, id string) error {
	if err := h.Backend.DisassociateWirelessGatewayFromCertificate(h.AccountID, h.DefaultRegion, id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getWirelessGatewayCertificate(c *echo.Context, id string) error {
	certID, err := h.Backend.GetWirelessGatewayCertificate(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, getWirelessGatewayCertificateResponse{
		IotCertificateID: certID,
	})
}

func (h *Handler) startWirelessDeviceImportTask(c *echo.Context) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.StartWirelessDeviceImportTask(h.AccountID, h.DefaultRegion, req.DestinationName)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, startWirelessDeviceImportTaskResponse{
		Arn: task.ARN,
		ID:  task.ID,
	})
}

func (h *Handler) startSingleWirelessDeviceImportTask(c *echo.Context) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.StartSingleWirelessDeviceImportTask(h.AccountID, h.DefaultRegion, req.DestinationName)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, startSingleWirelessDeviceImportTaskResponse{
		Arn:              task.ARN,
		WirelessDeviceID: task.WirelessDeviceID,
	})
}

// importTaskEntryFrom builds the wire response shape from a backend
// WirelessDeviceImportTask, formatting CreationTime as an ISO8601 string
// (see the field's doc comment on getWirelessDeviceImportTaskResponse).
func importTaskEntryFrom(task *WirelessDeviceImportTask) getWirelessDeviceImportTaskResponse {
	entry := getWirelessDeviceImportTaskResponse{
		Arn:                            task.ARN,
		ID:                             task.ID,
		DestinationName:                task.DestinationName,
		Status:                         task.Status,
		StatusReason:                   task.StatusReason,
		InitializedImportedDeviceCount: task.InitializedImportedDeviceCount,
		PendingImportedDeviceCount:     task.PendingImportedDeviceCount,
		OnboardedImportedDeviceCount:   task.OnboardedImportedDeviceCount,
		FailedImportedDeviceCount:      task.FailedImportedDeviceCount,
	}
	if !task.CreatedAt.IsZero() {
		entry.CreationTime = task.CreatedAt.UTC().Format(time.RFC3339)
	}

	return entry
}

func (h *Handler) getWirelessDeviceImportTask(c *echo.Context, id string) error {
	task, err := h.Backend.GetWirelessDeviceImportTask(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, importTaskEntryFrom(task))
}

func (h *Handler) deleteWirelessDeviceImportTask(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWirelessDeviceImportTask(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) updateWirelessDeviceImportTask(c *echo.Context, id string) error {
	var req struct {
		DestinationName string `json:"DestinationName"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessDeviceImportTask(id, req.DestinationName); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listWirelessDeviceImportTasks(c *echo.Context) error {
	tasks := h.Backend.ListWirelessDeviceImportTasks()
	pg, next := paginateQuery(c, tasks)

	entries := make([]getWirelessDeviceImportTaskResponse, 0, len(pg))

	for _, task := range pg {
		entries = append(entries, importTaskEntryFrom(task))
	}

	return writeJSON(c, http.StatusOK, listWirelessDeviceImportTasksResponse{
		WirelessDeviceImportTaskList: entries,
		NextToken:                    next,
	})
}

func (h *Handler) listDevicesForWirelessDeviceImportTask(c *echo.Context) error {
	id := c.QueryParam("id")
	if id == "" {
		// The Id query parameter is required by AWS, but clients that omit it
		// still get a well-formed (empty) list rather than a validation error,
		// matching this package's existing lenient-parsing convention.
		return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
			ImportedWirelessDeviceList: []struct{}{},
		})
	}

	task, err := h.Backend.GetWirelessDeviceImportTask(id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, listDevicesForWirelessDeviceImportTaskResponse{
		DestinationName:            task.DestinationName,
		Positioning:                "Disabled",
		ImportedWirelessDeviceList: []struct{}{},
	})
}
