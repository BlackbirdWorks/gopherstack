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

// listInsightsDataBody mirrors ListInsightsDataInput's two required fields.
type listInsightsDataBody struct {
	DataType      string `json:"DataType"`
	InsightSource string `json:"InsightSource"`
}

func (h *Handler) handleListInsightsData(c *echo.Context, body []byte) error {
	var in listInsightsDataBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}
	if in.DataType == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "DataType is required"))
	}
	if in.InsightSource == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "InsightSource is required"),
		)
	}

	data := h.Backend.ListInsightsData()

	// Real ListInsightsDataOutput wraps its list under "Events" (types.Event),
	// not "Insights" -- confirmed against cloudtrail@v1.58.4's
	// awsAwsjson11_deserializeOpDocumentListInsightsDataOutput.
	return c.JSON(http.StatusOK, map[string]any{"Events": data})
}

// --- ListInsightsMetricData ---

// listInsightsMetricDataBody mirrors ListInsightsMetricDataInput's real
// fields relevant to this backend: EventName, EventSource, and InsightType
// are all required; ErrorCode and TrailName are optional.
type listInsightsMetricDataBody struct {
	EventName   string `json:"EventName"`
	EventSource string `json:"EventSource"`
	InsightType string `json:"InsightType"`
	ErrorCode   string `json:"ErrorCode"`
	TrailName   string `json:"TrailName"`
}

func (h *Handler) handleListInsightsMetricData(c *echo.Context, body []byte) error {
	var in listInsightsMetricDataBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}
	if in.EventName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "EventName is required"))
	}
	if in.EventSource == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventSource is required"),
		)
	}
	if in.InsightType == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "InsightType is required"),
		)
	}

	// Real ListInsightsMetricDataOutput is a flat time series
	// (ErrorCode/EventName/EventSource/InsightType/NextToken/Timestamps/
	// TrailARN/Values), not a "Values"-wrapped list of records -- confirmed
	// against cloudtrail@v1.58.4's
	// awsAwsjson11_deserializeOpDocumentListInsightsMetricDataOutput.
	resp := map[string]any{
		"EventName":   in.EventName,
		"EventSource": in.EventSource,
		"InsightType": in.InsightType,
		"Timestamps":  []float64{},
		"Values":      h.Backend.ListInsightsMetricData(),
	}
	if in.ErrorCode != "" {
		resp["ErrorCode"] = in.ErrorCode
	}
	if in.TrailName != "" {
		trail, err := h.Backend.GetTrail(in.TrailName)
		if err != nil {
			return h.handleError(c, err)
		}
		resp["TrailARN"] = trail.TrailARN
	}

	return c.JSON(http.StatusOK, resp)
}
