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

// associateWirelessGatewayWithCertificateResponse's only real member is
// IotCertificateId (iotwireless@v1.59.4 deserializers.go:766) -- there is no
// IotCertificateArn member on the real output at all, so a real client's
// IotCertificateId always decoded empty under the old key.
type associateWirelessGatewayWithCertificateResponse struct {
	IotCertificateID string `json:"IotCertificateId"`
}

type getWirelessDeviceImportTaskResponse struct {
	Sidewalk        *sidewalkGetStartImportInfoResponse `json:"Sidewalk,omitempty"`
	Arn             string                              `json:"Arn"`
	ID              string                              `json:"Id"`
	DestinationName string                              `json:"DestinationName"`
	Status          string                              `json:"Status"`
	StatusReason    string                              `json:"StatusReason"`
	// CreationTime is an ISODateTimeString, not an epoch-seconds number --
	// confirmed against awsRestjson1_deserializeOpDocumentGetWirelessDeviceImportTaskOutput,
	// which parses it with smithytime.ParseDateTime (a string), unlike the
	// epoch-seconds CreatedAt fields on FuotaTask/MulticastGroup.
	CreationTime                   string `json:"CreationTime,omitempty"`
	Positioning                    string `json:"Positioning,omitempty"`
	InitializedImportedDeviceCount int64  `json:"InitializedImportedDeviceCount"`
	PendingImportedDeviceCount     int64  `json:"PendingImportedDeviceCount"`
	OnboardedImportedDeviceCount   int64  `json:"OnboardedImportedDeviceCount"`
	FailedImportedDeviceCount      int64  `json:"FailedImportedDeviceCount"`
}

// sidewalkGetStartImportInfoResponse mirrors types.SidewalkGetStartImportInfo.
type sidewalkGetStartImportInfoResponse struct {
	Positioning            *sidewalkPositioningResponse `json:"Positioning,omitempty"`
	Role                   string                       `json:"Role,omitempty"`
	DeviceCreationFileList []string                     `json:"DeviceCreationFileList,omitempty"`
}

// sidewalkPositioningResponse mirrors types.SidewalkPositioning.
type sidewalkPositioningResponse struct {
	DestinationName string `json:"DestinationName,omitempty"`
}

// sidewalkStartImportInfoRequest mirrors types.SidewalkStartImportInfo
// (StartWirelessDeviceImportTaskInput).
type sidewalkStartImportInfoRequest struct {
	Positioning *struct {
		DestinationName string `json:"DestinationName"`
	} `json:"Positioning"`
	DeviceCreationFile string `json:"DeviceCreationFile"`
	Role               string `json:"Role"`
}

func (s *sidewalkStartImportInfoRequest) toModel() WirelessDeviceImportSidewalk {
	if s == nil {
		return WirelessDeviceImportSidewalk{}
	}

	m := WirelessDeviceImportSidewalk{DeviceCreationFile: s.DeviceCreationFile, Role: s.Role}
	if s.Positioning != nil {
		m.PositioningDestination = s.Positioning.DestinationName
	}

	return m
}

// sidewalkUpdateImportInfoRequest mirrors types.SidewalkUpdateImportInfo
// (UpdateWirelessDeviceImportTaskInput) -- the only member is
// DeviceCreationFile; there is no Role or Positioning on the update shape.
type sidewalkUpdateImportInfoRequest struct {
	DeviceCreationFile string `json:"DeviceCreationFile"`
}

func (s *sidewalkUpdateImportInfoRequest) toModel() WirelessDeviceImportSidewalk {
	if s == nil {
		return WirelessDeviceImportSidewalk{}
	}

	return WirelessDeviceImportSidewalk{DeviceCreationFile: s.DeviceCreationFile}
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

// startSingleWirelessDeviceImportTaskResponse was missing Id entirely
// (iotwireless@v1.59.4 api_op_StartSingleWirelessDeviceImportTask.go), so a
// real client's Id always decoded empty.
type startSingleWirelessDeviceImportTaskResponse struct {
	Arn              string `json:"Arn"`
	ID               string `json:"Id"`
	WirelessDeviceID string `json:"WirelessDeviceId"`
}

func (h *Handler) associateWirelessGatewayWithCertificate(c *echo.Context, gatewayID string, body []byte) error {
	var req associateWirelessGatewayWithCertificateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	_, err := h.Backend.AssociateWirelessGatewayWithCertificate(
		h.AccountID, h.DefaultRegion, gatewayID, req.IotCertificateID,
	)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, associateWirelessGatewayWithCertificateResponse(req))
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
		Sidewalk        *sidewalkStartImportInfoRequest `json:"Sidewalk"`
		DestinationName string                          `json:"DestinationName"`
		Positioning     string                          `json:"Positioning"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	task, err := h.Backend.StartWirelessDeviceImportTask(
		h.AccountID, h.DefaultRegion, req.DestinationName, req.Positioning, req.Sidewalk.toModel(),
	)
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
		ID:               task.ID,
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
		Positioning:                    task.Positioning,
		InitializedImportedDeviceCount: task.InitializedImportedDeviceCount,
		PendingImportedDeviceCount:     task.PendingImportedDeviceCount,
		OnboardedImportedDeviceCount:   task.OnboardedImportedDeviceCount,
		FailedImportedDeviceCount:      task.FailedImportedDeviceCount,
	}
	if !task.CreatedAt.IsZero() {
		entry.CreationTime = task.CreatedAt.UTC().Format(time.RFC3339)
	}

	hasSidewalk := task.SidewalkRole != "" ||
		task.SidewalkPositioningDestination != "" ||
		len(task.SidewalkDeviceCreationFiles) > 0
	if hasSidewalk {
		sidewalk := &sidewalkGetStartImportInfoResponse{
			DeviceCreationFileList: task.SidewalkDeviceCreationFiles,
			Role:                   task.SidewalkRole,
		}
		if task.SidewalkPositioningDestination != "" {
			sidewalk.Positioning = &sidewalkPositioningResponse{DestinationName: task.SidewalkPositioningDestination}
		}

		entry.Sidewalk = sidewalk
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
		Sidewalk *sidewalkUpdateImportInfoRequest `json:"Sidewalk"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateWirelessDeviceImportTask(id, req.Sidewalk.toModel()); err != nil {
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
