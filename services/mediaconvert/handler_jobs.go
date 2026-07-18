package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func parseJobRoute(method, suffix string) mcRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mcRoute{operation: opListJobs}
		case http.MethodPost:
			return mcRoute{operation: opCreateJob}
		}
	}

	switch method {
	case http.MethodGet:
		return mcRoute{operation: opGetJob, resource: id}
	case http.MethodPut:
		return mcRoute{operation: opUpdateJob, resource: id}
	case http.MethodDelete:
		return mcRoute{operation: opCancelJob, resource: id}
	}

	return mcRoute{operation: opUnknown}
}

// --- Job handlers ---

type createJobInput struct {
	AccelerationSettings *AccelerationSettings `json:"accelerationSettings,omitempty"`
	Settings             map[string]any        `json:"settings,omitempty"`
	Tags                 map[string]string     `json:"tags,omitempty"`
	UserMetadata         map[string]string     `json:"userMetadata,omitempty"`
	Role                 string                `json:"role"`
	Queue                string                `json:"queue,omitempty"`
	JobTemplate          string                `json:"jobTemplate,omitempty"`
	BillingTagsSource    string                `json:"billingTagsSource,omitempty"`
	ClientRequestToken   string                `json:"clientRequestToken,omitempty"`
	// JobEngineVersion is the wire field name the real MediaConvert API uses
	// on CreateJobInput (it becomes JobEngineVersionRequested on the Job
	// output resource -- the request and response field names differ).
	JobEngineVersion string           `json:"jobEngineVersion,omitempty"`
	HopDestinations  []HopDestination `json:"hopDestinations,omitempty"`
	Priority         int              `json:"priority,omitempty"`
}

type jobWrapper struct {
	Job *Job `json:"job"`
}

type jobsListOutput struct {
	NextToken  string `json:"nextToken,omitempty"`
	Jobs       []*Job `json:"jobs"`
	TotalCount int    `json:"totalCount"`
}

func (h *Handler) handleCreateJob(c *echo.Context, body []byte) error {
	var in createJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Role == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "role is required"))
	}

	accelMode := ""
	if in.AccelerationSettings != nil {
		accelMode = in.AccelerationSettings.Mode
	}

	j, err := h.Backend.CreateJobFull(
		in.Role,
		in.Queue,
		in.JobTemplate,
		in.Settings,
		in.Tags,
		in.UserMetadata,
		in.BillingTagsSource,
		in.ClientRequestToken,
		accelMode,
		in.JobEngineVersion,
		in.Priority,
		in.HopDestinations,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, jobWrapper{Job: j})
}

type updateJobInput struct {
	Priority        *int             `json:"priority,omitempty"`
	Queue           string           `json:"queue,omitempty"`
	HopDestinations []HopDestination `json:"hopDestinations,omitempty"`
}

func (h *Handler) handleUpdateJob(c *echo.Context, id string, body []byte) error {
	var in updateJobInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	j, err := h.Backend.UpdateJob(id, in.Queue, in.Priority, in.HopDestinations)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleGetJob(c *echo.Context, id string) error {
	j, err := h.Backend.GetJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, jobWrapper{Job: j})
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	q := c.Request().URL.Query()
	statusFilter := q.Get("status")
	queueFilter := q.Get("queue")
	order := q.Get("order")
	maxResults := parseMaxResults(q.Get("maxResults"))

	jobs := h.Backend.ListJobsFiltered(statusFilter, queueFilter, order)
	if jobs == nil {
		jobs = []*Job{}
	}

	nextTokenIn := q.Get("nextToken")
	pg := page.New(jobs, nextTokenIn, maxResults, defaultJobsPageSize)

	out := jobsListOutput{Jobs: pg.Data, TotalCount: len(jobs)}
	if pg.Next != "" {
		out.NextToken = pg.Next
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleCancelJob(c *echo.Context, id string) error {
	if err := h.Backend.CancelJob(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
