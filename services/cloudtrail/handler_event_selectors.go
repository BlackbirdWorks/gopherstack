package cloudtrail

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- PutEventSelectors ---

type putEventSelectorsBody struct {
	TrailName              string                  `json:"TrailName"`
	EventSelectors         []EventSelector         `json:"EventSelectors"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"AdvancedEventSelectors"`
}

func (h *Handler) handlePutEventSelectors(c *echo.Context, body []byte) error {
	var in putEventSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.TrailName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "TrailName is required"))
	}

	t, err := h.Backend.PutEventSelectors(in.TrailName, in.EventSelectors, in.AdvancedEventSelectors)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyTrailARN: t.TrailARN,
	}
	if len(t.AdvancedEventSelectors) > 0 {
		resp["AdvancedEventSelectors"] = t.AdvancedEventSelectors
	} else {
		selectors := t.EventSelectors
		if selectors == nil {
			selectors = []EventSelector{}
		}
		resp["EventSelectors"] = selectors
	}

	return c.JSON(http.StatusOK, resp)
}

// --- GetEventSelectors ---

type getEventSelectorsBody struct {
	TrailName string `json:"TrailName"`
}

func (h *Handler) handleGetEventSelectors(c *echo.Context, body []byte) error {
	var in getEventSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	trailARN, selectors, advancedSelectors, err := h.Backend.GetEventSelectors(in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyTrailARN: trailARN,
	}
	if len(advancedSelectors) > 0 {
		resp["AdvancedEventSelectors"] = advancedSelectors
	} else {
		if selectors == nil {
			selectors = []EventSelector{}
		}
		resp["EventSelectors"] = selectors
	}

	return c.JSON(http.StatusOK, resp)
}

// --- PutInsightSelectors ---

// putInsightSelectorsBody mirrors PutInsightSelectorsInput. TrailName and
// EventDataStore are mutually exclusive: Insights can be configured on
// either a trail or an event data store.
type putInsightSelectorsBody struct {
	TrailName        string            `json:"TrailName"`
	EventDataStore   string            `json:"EventDataStore"`
	InsightSelectors []InsightSelector `json:"InsightSelectors"`
}

func (h *Handler) handlePutInsightSelectors(c *echo.Context, body []byte) error {
	var in putInsightSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	switch {
	case in.TrailName != "":
		t, err := h.Backend.PutInsightSelectors(in.TrailName, in.InsightSelectors)
		if err != nil {
			return h.handleError(c, err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyTrailARN:         t.TrailARN,
			keyInsightSelectors: t.InsightSelectors,
		})
	case in.EventDataStore != "":
		eds, err := h.Backend.PutEDSInsightSelectors(in.EventDataStore, in.InsightSelectors)
		if err != nil {
			return h.handleError(c, err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyEDSArn:           eds.EventDataStoreARN,
			keyInsightSelectors: eds.InsightSelectors,
		})
	default:
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "TrailName or EventDataStore is required"),
		)
	}
}

// --- GetInsightSelectors ---

// getInsightSelectorsBody mirrors GetInsightSelectorsInput. TrailName and
// EventDataStore are mutually exclusive.
type getInsightSelectorsBody struct {
	TrailName      string `json:"TrailName"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleGetInsightSelectors(c *echo.Context, body []byte) error {
	var in getInsightSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	switch {
	case in.TrailName != "":
		trailARN, selectors, err := h.Backend.GetInsightSelectors(in.TrailName)
		if err != nil {
			return h.handleError(c, err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyTrailARN:         trailARN,
			keyInsightSelectors: selectors,
		})
	case in.EventDataStore != "":
		edsARN, selectors, err := h.Backend.GetEDSInsightSelectors(in.EventDataStore)
		if err != nil {
			return h.handleError(c, err)
		}

		return c.JSON(http.StatusOK, map[string]any{
			keyEDSArn:           edsARN,
			keyInsightSelectors: selectors,
		})
	default:
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "TrailName or EventDataStore is required"),
		)
	}
}

// --- GetEventConfiguration ---

type getEventConfigurationBody struct {
	EventDataStore string `json:"EventDataStore"`
	TrailName      string `json:"TrailName"`
}

func (h *Handler) handleGetEventConfiguration(c *echo.Context, body []byte) error {
	var in getEventConfigurationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	resourceARN, isTrail, err := h.resolveEventConfigResource(in.EventDataStore, in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	cfg := h.Backend.GetEventConfiguration(resourceARN)

	return c.JSON(http.StatusOK, eventConfigToMap(resourceARN, isTrail, cfg))
}

// --- PutEventConfiguration ---

type putEventConfigurationBody struct {
	EventDataStore            string           `json:"EventDataStore"`
	TrailName                 string           `json:"TrailName"`
	MaxEventSize              string           `json:"MaxEventSize"`
	AggregationConfigurations []map[string]any `json:"AggregationConfigurations"`
	ContextKeySelectors       []map[string]any `json:"ContextKeySelectors"`
}

func (h *Handler) handlePutEventConfiguration(c *echo.Context, body []byte) error {
	var in putEventConfigurationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	resourceARN, isTrail, err := h.resolveEventConfigResource(in.EventDataStore, in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	cfg := h.Backend.PutEventConfiguration(
		resourceARN, in.AggregationConfigurations, in.ContextKeySelectors, in.MaxEventSize,
	)

	return c.JSON(http.StatusOK, eventConfigToMap(resourceARN, isTrail, cfg))
}

// resolveEventConfigResource resolves the TrailName or EventDataStore
// parameter shared by GetEventConfiguration/PutEventConfiguration to a
// concrete resource ARN, reporting whether it is a trail (as opposed to an
// event data store) -- the response echoes back TrailARN or
// EventDataStoreArn depending on which kind of resource was targeted.
func (h *Handler) resolveEventConfigResource(eventDataStore, trailName string) (string, bool, error) {
	switch {
	case trailName != "":
		t, err := h.Backend.GetTrail(trailName)
		if err != nil {
			return "", false, err
		}

		return t.TrailARN, true, nil
	case eventDataStore != "":
		eds, err := h.Backend.GetEventDataStore(eventDataStore)
		if err != nil {
			return "", false, err
		}

		return eds.EventDataStoreARN, false, nil
	default:
		return "", false, fmt.Errorf("%w: EventDataStore or TrailName is required", errInvalidRequest)
	}
}

// eventConfigToMap converts an EventConfiguration to the JSON map used by
// GetEventConfiguration/PutEventConfiguration API responses.
func eventConfigToMap(resourceARN string, isTrail bool, cfg *EventConfiguration) map[string]any {
	aggCfgs := cfg.AggregationConfigurations
	if aggCfgs == nil {
		aggCfgs = []map[string]any{}
	}
	ctxSelectors := cfg.ContextKeySelectors
	if ctxSelectors == nil {
		ctxSelectors = []map[string]any{}
	}

	m := map[string]any{
		"AggregationConfigurations": aggCfgs,
		"ContextKeySelectors":       ctxSelectors,
	}
	if cfg.MaxEventSize != "" {
		m["MaxEventSize"] = cfg.MaxEventSize
	}
	if isTrail {
		m[keyTrailARN] = resourceARN
	} else {
		m[keyEDSArn] = resourceARN
	}

	return m
}

// --- ListInsightsData ---

func (h *Handler) handleListInsightsData(c *echo.Context, _ []byte) error {
	data := h.Backend.ListInsightsData()

	return c.JSON(http.StatusOK, map[string]any{"Insights": data})
}

// --- ListInsightsMetricData ---

func (h *Handler) handleListInsightsMetricData(c *echo.Context, _ []byte) error {
	data := h.Backend.ListInsightsMetricData()

	return c.JSON(http.StatusOK, map[string]any{"Values": data})
}
