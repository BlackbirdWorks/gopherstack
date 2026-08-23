package cloudtrail

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultQueriesPageSize is the ListQueries page size used when the caller
// omits MaxResults. defaultQueryResultsPageSize is the analogous default for
// GetQueryResults' MaxQueryResults.
const (
	defaultQueriesPageSize      = 1000
	defaultQueryResultsPageSize = 1000
)

// --- StartQuery ---

// startQueryBody matches the real StartQueryInput exactly: QueryStatement,
// QueryAlias, QueryParameters, DeliveryS3Uri, EventDataStoreOwnerAccountId.
// There is no "EventDataStore" input field on the real API (that was a
// gopherstack-invented field, now removed) -- the target event data store is
// embedded in QueryStatement's FROM clause, per real CloudTrail Lake SQL.
type startQueryBody struct {
	QueryStatement               string   `json:"QueryStatement"`
	QueryAlias                   string   `json:"QueryAlias"`
	DeliveryS3URI                string   `json:"DeliveryS3Uri"`
	EventDataStoreOwnerAccountID string   `json:"EventDataStoreOwnerAccountId"`
	QueryParameters              []string `json:"QueryParameters"`
}

func (h *Handler) handleStartQuery(c *echo.Context, body []byte) error {
	var in startQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	// Real StartQuery requires either QueryStatement, or a QueryAlias (with
	// QueryParameters) for the dashboard-population use case.
	if in.QueryStatement == "" && in.QueryAlias == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "QueryStatement or QueryAlias is required"),
		)
	}

	edsARN := extractQueryFromTarget(in.QueryStatement)

	q, err := h.Backend.StartQuery(in.QueryStatement, edsARN, in.DeliveryS3URI, in.QueryAlias)
	if err != nil {
		return h.handleError(c, err)
	}

	ownerID := in.EventDataStoreOwnerAccountID
	if ownerID == "" {
		ownerID = h.Backend.AccountID()
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:                     q.QueryID,
		"EventDataStoreOwnerAccountId": ownerID,
	})
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

// describeQueryBody's EventDataStore field is deprecated on the real
// DescribeQueryInput ("no longer required") but still accepted -- kept here
// for backward wire compatibility, unlike StartQuery's invented field.
//
// Neither QueryId nor QueryAlias is required alone: "You must specify either
// QueryId or QueryAlias. Specifying the QueryAlias parameter returns
// information about the last query run for the alias." RefreshId (view a
// dashboard query's results as of a specific refresh) is accepted on the real
// input but not modeled: gopherstack's dashboard refreshes don't create
// linked Query records to disambiguate by.
type describeQueryBody struct {
	QueryID        string `json:"QueryId"`
	QueryAlias     string `json:"QueryAlias"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDescribeQuery(c *echo.Context, body []byte) error {
	var in describeQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	var (
		q   *Query
		err error
	)

	switch {
	case in.QueryID != "":
		q, err = h.Backend.DescribeQuery(in.QueryID)
	case in.QueryAlias != "":
		q, err = h.Backend.DescribeQueryByAlias(in.QueryAlias)
	default:
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "QueryId or QueryAlias is required"),
		)
	}
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyQueryID:     q.QueryID,
		"QueryString":  q.QueryString,
		keyQueryStatus: q.QueryStatus,
		// QueryStatisticsForDescribeQuery: BytesScanned/CreationTime/
		// EventsMatched/EventsScanned/ExecutionTimeInMillis, nested -- there is
		// no top-level CreationTime field on the real DescribeQueryOutput.
		"QueryStatistics": map[string]any{
			"BytesScanned":          q.BytesScanned,
			"CreationTime":          float64(q.CreationTime.Unix()),
			"EventsMatched":         q.EventsMatched,
			"EventsScanned":         q.EventsScanned,
			"ExecutionTimeInMillis": q.ExecutionTimeInMillis,
		},
	}
	if q.DeliveryS3URI != "" {
		resp["DeliveryS3Uri"] = q.DeliveryS3URI
	}
	if q.ErrorMessage != "" {
		resp["ErrorMessage"] = q.ErrorMessage
	}
	if q.EventDataStoreOwnerID != "" {
		resp["EventDataStoreOwnerAccountId"] = q.EventDataStoreOwnerID
	}

	return c.JSON(http.StatusOK, resp)
}

// --- GetQueryResults ---

type getQueryResultsBody struct {
	QueryID         string `json:"QueryId"`
	EventDataStore  string `json:"EventDataStore"`
	NextToken       string `json:"NextToken"`
	MaxQueryResults int    `json:"MaxQueryResults"`
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

	p := page.New(q.QueryResultRows, in.NextToken, in.MaxQueryResults, defaultQueryResultsPageSize)

	resp := map[string]any{
		keyQueryID:        q.QueryID,
		keyQueryStatus:    q.QueryStatus,
		"QueryResultRows": p.Data,
		"QueryStatistics": map[string]any{
			"BytesScanned":      q.BytesScanned,
			"ResultsCount":      len(p.Data),
			"TotalResultsCount": len(q.QueryResultRows),
		},
	}
	if p.Next != "" {
		resp["NextToken"] = p.Next
	}
	if q.ErrorMessage != "" {
		resp["ErrorMessage"] = q.ErrorMessage
	}

	return c.JSON(http.StatusOK, resp)
}

// --- ListQueries ---

type listQueriesBody struct {
	EventDataStore string `json:"EventDataStore"`
	QueryStatus    string `json:"QueryStatus"`
	NextToken      string `json:"NextToken"`
	MaxResults     int    `json:"MaxResults"`
}

func (h *Handler) handleListQueries(c *echo.Context, body []byte) error {
	var in listQueriesBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	list := h.Backend.ListQueries()
	filtered := make([]*Query, 0, len(list))

	for _, q := range list {
		if in.QueryStatus != "" && q.QueryStatus != in.QueryStatus {
			continue
		}
		if in.EventDataStore != "" && !queryMatchesEventDataStore(q, in.EventDataStore) {
			continue
		}
		filtered = append(filtered, q)
	}

	p := page.New(filtered, in.NextToken, in.MaxResults, defaultQueriesPageSize)
	items := make([]map[string]any, 0, len(p.Data))
	for _, q := range p.Data {
		items = append(items, map[string]any{
			keyQueryID:     q.QueryID,
			keyQueryStatus: q.QueryStatus,
			"CreationTime": float64(q.CreationTime.Unix()),
		})
	}

	resp := map[string]any{"Queries": items}
	if p.Next != "" {
		resp["NextToken"] = p.Next
	}

	return c.JSON(http.StatusOK, resp)
}

// queryMatchesEventDataStore reports whether q belongs to the requested
// event data store (matched by exact ARN or ARN suffix, so a bare ID also
// matches). A query whose target could not be resolved at StartQuery time
// (EventDataStoreARN == "") never matches an explicit filter.
func queryMatchesEventDataStore(q *Query, want string) bool {
	if q.EventDataStoreARN == "" {
		return false
	}

	return q.EventDataStoreARN == want || strings.HasSuffix(q.EventDataStoreARN, want)
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
