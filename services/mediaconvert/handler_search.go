package mediaconvert

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- SearchJobs handler ---

type searchJobsOutput struct {
	NextToken string `json:"nextToken,omitempty"`
	Jobs      []*Job `json:"jobs"`
}

func (h *Handler) handleSearchJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	statusFilter := q.Get("status")
	queueFilter := q.Get("queue")
	order := q.Get("order")
	maxResults := parseMaxResults(q.Get("maxResults"))

	jobs := h.Backend.ListJobsFiltered(statusFilter, queueFilter, order)
	if jobs == nil {
		jobs = []*Job{}
	}

	if inputFile := q.Get("inputFile"); inputFile != "" {
		filtered := jobs[:0:0]

		for _, j := range jobs {
			if jobMatchesInputFile(j, inputFile) {
				filtered = append(filtered, j)
			}
		}

		jobs = filtered
	}

	nextTokenIn := q.Get("nextToken")
	pg := page.New(jobs, nextTokenIn, maxResults, defaultListPageSize)

	out := searchJobsOutput{Jobs: pg.Data}
	if pg.Next != "" {
		out.NextToken = pg.Next
	}

	return c.JSON(http.StatusOK, out)
}
