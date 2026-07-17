package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- CreateEventDataStore ---

type createEventDataStoreBody struct {
	Name                   string                  `json:"Name"`
	BillingMode            string                  `json:"BillingMode"`
	KMSKeyID               string                  `json:"KmsKeyId"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"AdvancedEventSelectors"`
	Tags                   []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
	RetentionPeriod              int32 `json:"RetentionPeriod"`
	MultiRegionEnabled           bool  `json:"MultiRegionEnabled"`
	OrganizationEnabled          bool  `json:"OrganizationEnabled"`
	TerminationProtectionEnabled bool  `json:"TerminationProtectionEnabled"`
}

func (h *Handler) handleCreateEventDataStore(c *echo.Context, body []byte) error {
	var in createEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	eds, err := h.Backend.CreateEventDataStore(
		in.Name,
		in.MultiRegionEnabled,
		in.OrganizationEnabled,
		in.TerminationProtectionEnabled,
		in.RetentionPeriod,
		in.AdvancedEventSelectors,
		in.BillingMode,
		in.KMSKeyID,
		kv,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- DeleteEventDataStore ---

type deleteEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDeleteEventDataStore(c *echo.Context, body []byte) error {
	var in deleteEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteEventDataStore(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetEventDataStore ---

type getEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleGetEventDataStore(c *echo.Context, body []byte) error {
	var in getEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.GetEventDataStore(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- UpdateEventDataStore ---

type updateEventDataStoreBody struct {
	RetentionPeriod              *int32                  `json:"RetentionPeriod"`
	MultiRegionEnabled           *bool                   `json:"MultiRegionEnabled"`
	OrganizationEnabled          *bool                   `json:"OrganizationEnabled"`
	TerminationProtectionEnabled *bool                   `json:"TerminationProtectionEnabled"`
	EventDataStore               string                  `json:"EventDataStore"`
	Name                         string                  `json:"Name"`
	BillingMode                  string                  `json:"BillingMode"`
	KMSKeyID                     string                  `json:"KmsKeyId"`
	AdvancedEventSelectors       []AdvancedEventSelector `json:"AdvancedEventSelectors"`
}

func (h *Handler) handleUpdateEventDataStore(c *echo.Context, body []byte) error {
	var in updateEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.UpdateEventDataStore(
		in.EventDataStore, in.Name,
		in.MultiRegionEnabled, in.OrganizationEnabled, in.TerminationProtectionEnabled,
		in.RetentionPeriod,
		in.AdvancedEventSelectors,
		in.BillingMode,
		in.KMSKeyID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- ListEventDataStores ---

func (h *Handler) handleListEventDataStores(c *echo.Context, _ []byte) error {
	list := h.Backend.ListEventDataStores()
	items := make([]map[string]any, 0, len(list))
	for _, eds := range list {
		items = append(items, edsToMap(eds))
	}

	return c.JSON(http.StatusOK, map[string]any{"EventDataStores": items})
}

// --- RestoreEventDataStore ---

type restoreEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleRestoreEventDataStore(c *echo.Context, body []byte) error {
	var in restoreEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.RestoreEventDataStore(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- StartEventDataStoreIngestion ---

type startEventDataStoreIngestionBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleStartEventDataStoreIngestion(c *echo.Context, body []byte) error {
	var in startEventDataStoreIngestionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.StartEventDataStoreIngestion(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- StopEventDataStoreIngestion ---

type stopEventDataStoreIngestionBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleStopEventDataStoreIngestion(c *echo.Context, body []byte) error {
	var in stopEventDataStoreIngestionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.StopEventDataStoreIngestion(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- DisableFederation ---

type disableFederationBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDisableFederation(c *echo.Context, body []byte) error {
	var in disableFederationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.EventDataStore == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventDataStore is required"),
		)
	}

	eds, err := h.Backend.DisableFederation(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyEDSArn:          eds.EventDataStoreARN,
		"FederationStatus": eds.FederationStatus,
	})
}

// --- EnableFederation ---

type enableFederationBody struct {
	EventDataStore    string `json:"EventDataStore"`
	FederationRoleArn string `json:"FederationRoleArn"`
}

func (h *Handler) handleEnableFederation(c *echo.Context, body []byte) error {
	var in enableFederationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.EventDataStore == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventDataStore is required"),
		)
	}

	eds, err := h.Backend.EnableFederation(in.EventDataStore, in.FederationRoleArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyEDSArn:           eds.EventDataStoreARN,
		"FederationStatus":  eds.FederationStatus,
		"FederationRoleArn": eds.FederationRoleArn,
	})
}

// edsToMap converts an EventDataStore to the JSON map used in API responses.
func edsToMap(eds *EventDataStore) map[string]any {
	m := map[string]any{
		keyEDSArn:                      eds.EventDataStoreARN,
		keyName:                        eds.Name,
		keyStatus:                      eds.Status,
		"MultiRegionEnabled":           eds.MultiRegionEnabled,
		"OrganizationEnabled":          eds.OrganizationEnabled,
		"TerminationProtectionEnabled": eds.TerminationProtected,
		"RetentionPeriod":              eds.RetentionPeriod,
		"CreatedTimestamp":             eds.CreatedTimestamp,
		"UpdatedTimestamp":             eds.UpdatedTimestamp,
	}
	if eds.BillingMode != "" {
		m["BillingMode"] = eds.BillingMode
	}
	if eds.FederationStatus != "" {
		m["FederationStatus"] = eds.FederationStatus
	}
	if eds.FederationRoleArn != "" {
		m["FederationRoleArn"] = eds.FederationRoleArn
	}
	if eds.KMSKeyID != "" {
		m["KmsKeyId"] = eds.KMSKeyID
	}
	advSels := eds.AdvancedEventSelectors
	if advSels == nil {
		advSels = []AdvancedEventSelector{}
	}
	m["AdvancedEventSelectors"] = advSels
	if len(eds.InsightSelectors) > 0 {
		m[keyInsightSelectors] = eds.InsightSelectors
	}

	return m
}
