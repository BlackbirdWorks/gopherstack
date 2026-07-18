package cloudtrail

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- StartQuery ---

type startQueryBody struct {
	QueryStatement string `json:"QueryStatement"`
	EventDataStore string `json:"EventDataStore"`
	DeliveryS3URI  string `json:"DeliveryS3Uri"`
}

func (h *Handler) handleStartQuery(c *echo.Context, body []byte) error {
	var in startQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryStatement == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "QueryStatement is required"),
		)
	}

	q, err := h.Backend.StartQuery(in.QueryStatement, in.EventDataStore, in.DeliveryS3URI)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyQueryID: q.QueryID})
}

// --- CancelQuery ---

type cancelQueryBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleCancelQuery(c *echo.Context, body []byte) error {
	var in cancelQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.CancelQuery(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:     q.QueryID,
		keyQueryStatus: q.QueryStatus,
	})
}

// --- DescribeQuery ---

type describeQueryBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDescribeQuery(c *echo.Context, body []byte) error {
	var in describeQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.DescribeQuery(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyQueryID:     q.QueryID,
		"QueryString":  q.QueryString,
		keyQueryStatus: q.QueryStatus,
		"CreationTime": float64(q.CreationTime.Unix()),
	}
	if q.DeliveryS3URI != "" {
		resp["DeliveryS3Uri"] = q.DeliveryS3URI
	}
	if q.ErrorMessage != "" {
		resp["ErrorMessage"] = q.ErrorMessage
	}

	return c.JSON(http.StatusOK, resp)
}

// --- GetQueryResults ---

type getQueryResultsBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleGetQueryResults(c *echo.Context, body []byte) error {
	var in getQueryResultsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.GetQueryResults(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:        q.QueryID,
		keyQueryStatus:    q.QueryStatus,
		"QueryResultRows": []any{},
		"QueryStatistics": map[string]any{
			"TotalResultsCount": 0,
			"BytesScanned":      0,
		},
	})
}

// --- ListQueries ---

func (h *Handler) handleListQueries(c *echo.Context, _ []byte) error {
	list := h.Backend.ListQueries()
	items := make([]map[string]any, 0, len(list))
	for _, q := range list {
		items = append(items, map[string]any{
			keyQueryID:     q.QueryID,
			keyQueryStatus: q.QueryStatus,
			"CreationTime": q.CreationTime,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Queries": items})
}

// --- GenerateQuery ---

type generateQueryBody struct {
	Prompt          string   `json:"Prompt"`
	EventDataStores []string `json:"EventDataStores"`
}

func (h *Handler) handleGenerateQuery(c *echo.Context, body []byte) error {
	var in generateQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if len(in.EventDataStores) == 0 {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventDataStores is required"),
		)
	}
	if in.Prompt == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "Prompt is required"))
	}

	gq := h.Backend.GenerateQuery(in.EventDataStores, in.Prompt)

	return c.JSON(http.StatusOK, map[string]any{
		"QueryStatement":               gq.QueryStatement,
		"QueryAlias":                   gq.QueryAlias,
		"EventDataStoreOwnerAccountId": gq.OwnerAccountID,
	})
}

// --- SearchSampleQueries ---

func (h *Handler) handleSearchSampleQueries(c *echo.Context, _ []byte) error {
	results := h.Backend.SearchSampleQueries()

	return c.JSON(http.StatusOK, map[string]any{"SampleQueries": results})
}
