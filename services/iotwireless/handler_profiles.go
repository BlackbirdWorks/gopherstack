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
	LoRaWAN  map[string]any `json:"LoRaWAN,omitempty"`
	Sidewalk map[string]any `json:"Sidewalk,omitempty"`
	Name     string         `json:"Name"`
	Tags     []tags.KV      `json:"Tags"`
}

type createDeviceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

// deviceProfileEntry is the Get response shape (Arn, Id, Name, LoRaWAN,
// Sidewalk). List entries use the narrower deviceProfileListEntry instead --
// confirmed against types.DeviceProfile, which real AWS's
// ListDeviceProfilesOutput uses and which carries only Arn/Id/Name.
type deviceProfileEntry struct {
	LoRaWAN  map[string]any `json:"LoRaWAN,omitempty"`
	Sidewalk map[string]any `json:"Sidewalk,omitempty"`
	Arn      string         `json:"Arn"`
	ID       string         `json:"Id"`
	Name     string         `json:"Name"`
}

type deviceProfileListEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listDeviceProfilesResponse struct {
	NextToken         string                   `json:"NextToken"`
	DeviceProfileList []deviceProfileListEntry `json:"DeviceProfileList"`
}

// --- Device Profile handlers ---

func (h *Handler) createDeviceProfile(c *echo.Context, body []byte) error {
	var req createDeviceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	dp, err := h.Backend.CreateDeviceProfile(
		h.AccountID, h.DefaultRegion, req.Name, req.LoRaWAN, req.Sidewalk, tagKVsToMap(req.Tags),
	)
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
		Arn:      dp.ARN,
		ID:       dp.ID,
		Name:     dp.Name,
		LoRaWAN:  dp.LoRaWAN,
		Sidewalk: dp.Sidewalk,
	})
}

func (h *Handler) listDeviceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListDeviceProfiles(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, profiles)

	entries := make([]deviceProfileListEntry, 0, len(pg))

	for _, dp := range pg {
		entries = append(entries, deviceProfileListEntry{
			Arn:  dp.ARN,
			ID:   dp.ID,
			Name: dp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listDeviceProfilesResponse{DeviceProfileList: entries, NextToken: next})
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
	LoRaWAN map[string]any `json:"LoRaWAN,omitempty"`
	Name    string         `json:"Name"`
	Tags    []tags.KV      `json:"Tags"`
}

type createServiceProfileResponse struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

// serviceProfileEntry is the Get response shape (Arn, Id, Name, LoRaWAN).
// List entries use the narrower serviceProfileListEntry -- confirmed
// against types.ServiceProfile, which carries only Arn/Id/Name.
type serviceProfileEntry struct {
	LoRaWAN map[string]any `json:"LoRaWAN,omitempty"`
	Arn     string         `json:"Arn"`
	ID      string         `json:"Id"`
	Name    string         `json:"Name"`
}

type serviceProfileListEntry struct {
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type listServiceProfilesResponse struct {
	NextToken          string                    `json:"NextToken"`
	ServiceProfileList []serviceProfileListEntry `json:"ServiceProfileList"`
}

// --- Service Profile handlers ---

func (h *Handler) createServiceProfile(c *echo.Context, body []byte) error {
	var req createServiceProfileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	sp, err := h.Backend.CreateServiceProfile(
		h.AccountID,
		h.DefaultRegion,
		req.Name,
		req.LoRaWAN,
		tagKVsToMap(req.Tags),
	)
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
		Arn:     sp.ARN,
		ID:      sp.ID,
		Name:    sp.Name,
		LoRaWAN: sp.LoRaWAN,
	})
}

func (h *Handler) listServiceProfiles(c *echo.Context) error {
	profiles := h.Backend.ListServiceProfiles(h.AccountID, h.DefaultRegion)
	pg, next := paginateQuery(c, profiles)

	entries := make([]serviceProfileListEntry, 0, len(pg))

	for _, sp := range pg {
		entries = append(entries, serviceProfileListEntry{
			Arn:  sp.ARN,
			ID:   sp.ID,
			Name: sp.Name,
		})
	}

	return writeJSON(c, http.StatusOK, listServiceProfilesResponse{ServiceProfileList: entries, NextToken: next})
}

func (h *Handler) deleteServiceProfile(c *echo.Context, id string) error {
	err := h.Backend.DeleteServiceProfile(h.AccountID, h.DefaultRegion, id)
	if err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
