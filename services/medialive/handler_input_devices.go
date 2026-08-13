package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- InputDevice handlers ---

// inputDeviceOutput mirrors DescribeInputDeviceOutput/UpdateInputDeviceOutput
// (see channelOutput's doc comment for why case matters). gopherstack-7ux2:
// "maintenanceWindowActive" was emitted here but is not a member of either
// types.InputDeviceSummary or DescribeInputDeviceOutput (medialive@v1.101.4
// types/types.go:4498-4556, api_op_DescribeInputDevice.go) -- confirmed
// against both deserializer field switches, which list no such key. Removed;
// it had no reader anywhere in this package either, so nothing downstream
// depended on it.
type inputDeviceOutput struct {
	Tags                    map[string]string `json:"tags"`
	Arn                     string            `json:"arn"`
	ID                      string            `json:"id"`
	Name                    string            `json:"name"`
	SerialNumber            string            `json:"serialNumber"`
	MacAddress              string            `json:"macAddress"`
	DeviceType              string            `json:"type"`
	ConnectionState         string            `json:"connectionState"`
	DeviceSettingsSyncState string            `json:"deviceSettingsSyncState"`
	DeviceUpdateStatus      string            `json:"deviceUpdateStatus"`
}

func toInputDeviceOutput(d *InputDevice) inputDeviceOutput {
	tags := d.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return inputDeviceOutput{
		Tags:                    tags,
		Arn:                     d.ARN,
		ID:                      d.ID,
		Name:                    d.Name,
		SerialNumber:            d.SerialNumber,
		MacAddress:              d.MacAddress,
		DeviceType:              d.DeviceType,
		ConnectionState:         d.ConnectionState,
		DeviceSettingsSyncState: d.DeviceSettingsSyncState,
		DeviceUpdateStatus:      d.DeviceUpdateStatus,
	}
}

func (h *Handler) handleClaimDevice(c *echo.Context, body map[string]any) error {
	// ClaimDeviceInput's real wire field is lowerCamel "id" -- verified
	// against aws-sdk-go-v2/service/medialive's
	// awsRestjson1_serializeOpDocumentClaimDeviceInput. A PascalCase "Id"
	// (the prior key here) is never sent by a real client, so ClaimDevice
	// silently no-oped on every real caller before this fix.
	id, _ := body["id"].(string)

	if _, err := h.Backend.ClaimDevice(id); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputDevices(c *echo.Context) error {
	devices, nextToken, err := h.Backend.ListInputDevices(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]inputDeviceOutput, 0, len(devices))
	for _, d := range devices {
		out = append(out, toInputDeviceOutput(d))
	}

	resp := map[string]any{"inputDevices": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeInputDevice(c *echo.Context, deviceID string) error {
	d, err := h.Backend.DescribeInputDevice(deviceID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputDeviceOutput(d))
}

func (h *Handler) handleUpdateInputDevice(
	c *echo.Context,
	deviceID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)

	d, err := h.Backend.UpdateInputDevice(deviceID, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputDeviceOutput(d))
}

func (h *Handler) handleRebootInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.RebootInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleTransferInputDevice(
	c *echo.Context,
	deviceID string,
	body map[string]any,
) error {
	// TransferInputDeviceInput's real wire fields are lowerCamel
	// "targetCustomerId"/"targetRegion"/"transferMessage" -- verified
	// against awsRestjson1_serializeOpDocumentTransferInputDeviceInput. The
	// prior PascalCase keys here never matched a real client's request
	// body, so TransferInputDevice silently no-oped on every real caller's
	// input before this fix.
	targetCustomerID, _ := body["targetCustomerId"].(string)
	targetRegion, _ := body["targetRegion"].(string)
	message, _ := body["transferMessage"].(string)

	if err := h.Backend.TransferInputDevice(deviceID, targetCustomerID, targetRegion, message); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleAcceptInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.AcceptInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCancelInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.CancelInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRejectInputDeviceTransfer(c *echo.Context, deviceID string) error {
	if err := h.Backend.RejectInputDeviceTransfer(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputDeviceTransfers(c *echo.Context) error {
	transferType := c.QueryParam("transferType")

	transfers, nextToken, err := h.Backend.ListInputDeviceTransfers(transferType, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(transfers))
	for _, t := range transfers {
		out = append(out, map[string]any{
			keyID:              t.DeviceID,
			"targetCustomerId": t.TargetCustomerID,
			"transferType":     t.TransferType,
			keyLowerMessage:    t.Message,
		})
	}

	resp := map[string]any{"inputDeviceTransfers": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- InputDevice lifecycle extra handlers ---

func (h *Handler) handleStartInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.StartInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopInputDevice(c *echo.Context, deviceID string) error {
	if err := h.Backend.StopInputDevice(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartInputDeviceMaintenanceWindow(c *echo.Context, deviceID string) error {
	if err := h.Backend.StartInputDeviceMaintenanceWindow(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeInputDeviceThumbnail(c *echo.Context, deviceID string) error {
	if _, err := h.Backend.DescribeInputDeviceThumbnail(deviceID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ContentType": "image/jpeg", "ContentLength": 0})
}
