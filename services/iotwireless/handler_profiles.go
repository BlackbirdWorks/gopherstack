package iotwireless

// Device profile and service profile handlers are both simple flat CRUD
// resources with the same wire shape. Kept in one file (rather than two
// near-identical files) per project convention: a `dupl` finding from
// splitting near-identical logic into separate files means the ops belong
// grouped in one cohesive file instead.

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type createDeviceProfileRequest struct {
	Name string    `json:"Name"`
	Tags []tags.KV `json:"Tags"`
}

type createDeviceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type deviceProfileEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listDeviceProfilesResponse struct {
	DeviceProfileList []deviceProfileEntry `json:"DeviceProfileList"`
}

// --- Device Profile handlers ---

func (h *Handler) createDeviceProfile(c *echo.Context, body []byte) error {
	var req createDeviceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dp, err := h.Backend.CreateDeviceProfile(h.AccountID, h.DefaultRegion, req.Name, tagKVsToMap(req.Tags))
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createDeviceProfileResponse{Arn: dp.ARN, ID: dp.ID})
}

func (h *Handler) getDeviceProfile(c *echo.Context, id string) error {
	dp, err := h.Backend.GetDeviceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, deviceProfileEntry{
		Arn:  dp.ARN,
		ID:   dp.ID,
		Name: dp.Name,
	})
}

func (h *Handler) listDeviceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListDeviceProfiles(h.AccountID, h.DefaultRegion)
	entries := make([]deviceProfileEntry, 0, len(profiles))

	for _, dp := range profiles {
		entries = append(entries, deviceProfileEntry{
			Arn:  dp.ARN,
			ID:   dp.ID,
			Name: dp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listDeviceProfilesResponse{DeviceProfileList: entries})
}

func (h *Handler) deleteDeviceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteDeviceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

type createServiceProfileRequest struct {
	Name string    `json:"Name"`
	Tags []tags.KV `json:"Tags"`
}

type createServiceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type serviceProfileEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listServiceProfilesResponse struct {
	ServiceProfileList []serviceProfileEntry `json:"ServiceProfileList"`
}

// --- Service Profile handlers ---

func (h *Handler) createServiceProfile(c *echo.Context, body []byte) error {
	var req createServiceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	sp, err := h.Backend.CreateServiceProfile(h.AccountID, h.DefaultRegion, req.Name, tagKVsToMap(req.Tags))
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusCreated, createServiceProfileResponse{Arn: sp.ARN, ID: sp.ID})
}

func (h *Handler) getServiceProfile(c *echo.Context, id string) error {
	sp, err := h.Backend.GetServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	return writeJSON(c, http.StatusOK, serviceProfileEntry{
		Arn:  sp.ARN,
		ID:   sp.ID,
		Name: sp.Name,
	})
}

func (h *Handler) listServiceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListServiceProfiles(h.AccountID, h.DefaultRegion)
	entries := make([]serviceProfileEntry, 0, len(profiles))

	for _, sp := range profiles {
		entries = append(entries, serviceProfileEntry{
			Arn:  sp.ARN,
			ID:   sp.ID,
			Name: sp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listServiceProfilesResponse{ServiceProfileList: entries})
}

func (h *Handler) deleteServiceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
