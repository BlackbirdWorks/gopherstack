package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func parseJobsQueriesRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id != "" && method == http.MethodGet {
		return mcRoute{operation: opGetJobsQueryResults, resource: id}
	}

	if id == "" && method == http.MethodPost {
		return mcRoute{operation: opStartJobsQuery}
	}

	return mcRoute{operation: opUnknown}
}

// --- Jobs query handlers ---

// jobsQueryStatusComplete is the JobsQueryStatus value reported for every
// GetJobsQueryResults response. Queries in this backend are resolved
// synchronously at GetJobsQueryResults time (see StartJobsQuery/
// GetJobsQueryResults in jobs_query.go), so results are always immediately
// available -- COMPLETE is therefore always the accurate status, unlike
// real AWS where a query can still be SUBMITTED/PROGRESSING/ERROR.
const jobsQueryStatusComplete = "COMPLETE"

type jobsQueryResultsOutput struct {
	Status string `json:"status"`
	Jobs   []*Job `json:"jobs"`
}

func (h *Handler) handleGetJobsQueryResults(c *echo.Context, queryID string) error {
	jobs := h.Backend.GetJobsQueryResults(queryID)

	return c.JSON(http.StatusOK, jobsQueryResultsOutput{Jobs: jobs, Status: jobsQueryStatusComplete})
}

// --- StartJobsQuery handler ---

type startJobsQueryInput struct {
	Order      string           `json:"order,omitempty"`
	MaxResults *int             `json:"maxResults,omitempty"`
	FilterList []map[string]any `json:"filterList,omitempty"`
}

// startJobsQueryOutput mirrors the real StartJobsQueryOutput wire shape,
// whose sole member is "id" -- not "queryId".
type startJobsQueryOutput struct {
	QueryID string `json:"id"`
}

func (h *Handler) handleStartJobsQuery(c *echo.Context, body []byte) error {
	var in startJobsQueryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	queryID, err := h.Backend.StartJobsQuery(in.FilterList, maxResults, in.Order)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, startJobsQueryOutput{QueryID: queryID})
}
