package iot

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	keyExecution        = "execution"
	jobsDataServiceName = "iot-jobs-data"
	jobsDataNext        = "$next"
)

// jobsDataPath splits /things/{thingName}/jobs[/{jobId}] for the device-side Jobs data plane.
func jobsDataPath(path string) (string, string, bool) {
	rest, found := strings.CutPrefix(path, "/things/")
	if !found {
		return "", "", false
	}

	thing, tail, found := strings.Cut(rest, "/jobs")
	if !found || thing == "" || strings.Contains(thing, "/") {
		return "", "", false
	}

	if tail == "" {
		return thing, "", true
	}

	jobID, found := strings.CutPrefix(tail, "/")
	if !found || strings.Contains(jobID, "/") {
		return "", "", false
	}

	return thing, jobID, true
}

// isJobsDataRequest reports a request signed for the iot-jobs-data service (the device job agent).
func isJobsDataRequest(r *http.Request) bool {
	if httputils.ExtractServiceFromRequest(r) != jobsDataServiceName {
		return false
	}

	_, _, ok := jobsDataPath(r.URL.Path)

	return ok
}

// jobsDataBackend is the device-side Jobs data plane surface of the backend.
type jobsDataBackend interface {
	GetPendingJobExecutions(thingName string) ([]*DeviceJobExecution, []*DeviceJobExecution, error)
	StartNextPendingJobExecution(thingName string, statusDetails map[string]string) (*DeviceJobExecution, bool, error)
	DescribeDeviceJobExecution(
		thingName, jobID string,
		executionNumber int64,
		includeDocument bool,
	) (*DeviceJobExecution, error)
	UpdateDeviceJobExecution(in *UpdateDeviceJobExecutionInput) (*DeviceJobExecution, error)
}

func (h *Handler) jobsData() (jobsDataBackend, bool) {
	b, ok := h.Backend.(jobsDataBackend)

	return b, ok
}

type deviceExecutionJSON struct {
	StatusDetails   map[string]string  `json:"statusDetails,omitempty"`
	JobID           string             `json:"jobId"`
	ThingName       string             `json:"thingName,omitempty"`
	Status          JobExecutionStatus `json:"status,omitempty"`
	JobDocument     string             `json:"jobDocument,omitempty"`
	QueuedAt        int64              `json:"queuedAt"`
	StartedAt       int64              `json:"startedAt,omitempty"`
	LastUpdatedAt   int64              `json:"lastUpdatedAt"`
	VersionNumber   int64              `json:"versionNumber"`
	ExecutionNumber int64              `json:"executionNumber"`
}

func toDeviceExecutionJSON(e *DeviceJobExecution, summary bool) deviceExecutionJSON {
	out := deviceExecutionJSON{
		JobID:           e.JobID,
		QueuedAt:        e.QueuedAt,
		StartedAt:       e.StartedAt,
		LastUpdatedAt:   e.LastUpdatedAt,
		VersionNumber:   e.VersionNumber,
		ExecutionNumber: e.ExecutionNumber,
	}

	if !summary {
		out.ThingName = e.ThingName
		out.Status = e.Status
		out.StatusDetails = e.StatusDetails
		out.JobDocument = e.JobDocument
	}

	return out
}

func (h *Handler) handleJobsData(c *echo.Context) error {
	if _, ok := h.jobsData(); !ok {
		return c.JSON(
			http.StatusBadRequest,
			awsErrBody{errTypeInvalidRequest, "job execution data plane is unavailable"},
		)
	}

	thing, jobID, _ := jobsDataPath(c.Request().URL.Path)
	method := c.Request().Method

	switch {
	case jobID == "" && method == http.MethodGet:
		return h.handleGetPendingJobExecutions(c, thing)
	case jobID == jobsDataNext && method == http.MethodPut:
		return h.handleStartNextPendingJobExecution(c, thing)
	case jobID != "" && method == http.MethodGet:
		return h.handleDescribeDeviceJobExecution(c, thing, jobID)
	case jobID != "" && method == http.MethodPost:
		return h.handleUpdateDeviceJobExecution(c, thing, jobID)
	}

	return c.JSON(
		http.StatusBadRequest,
		awsErrBody{errTypeInvalidRequest, "unknown operation: " + method + " " + c.Request().URL.Path},
	)
}

func (h *Handler) handleGetPendingJobExecutions(c *echo.Context, thing string) error {
	inProgress, queued, err := h.mustJobsData().GetPendingJobExecutions(thing)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := func(in []*DeviceJobExecution) []deviceExecutionJSON {
		out := make([]deviceExecutionJSON, 0, len(in))
		for _, e := range in {
			out = append(out, toDeviceExecutionJSON(e, true))
		}

		return out
	}

	return c.JSON(http.StatusOK, map[string]any{
		"inProgressJobs": summaries(inProgress),
		"queuedJobs":     summaries(queued),
	})
}

func (h *Handler) handleStartNextPendingJobExecution(c *echo.Context, thing string) error {
	var body struct {
		StatusDetails map[string]string `json:"statusDetails"`
	}
	if err := decodeJSONBody(c, &body); err != nil {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	exec, found, err := h.mustJobsData().StartNextPendingJobExecution(thing, body.StatusDetails)
	if err != nil {
		return h.handleError(c, err)
	}

	if !found {
		return c.JSON(http.StatusOK, map[string]any{})
	}

	return c.JSON(http.StatusOK, map[string]any{keyExecution: toDeviceExecutionJSON(exec, false)})
}

func (h *Handler) handleDescribeDeviceJobExecution(c *echo.Context, thing, jobID string) error {
	executionNumber, _ := strconv.ParseInt(c.QueryParam("executionNumber"), 10, 64)
	includeDoc := c.QueryParam("includeJobDocument") != "false"

	exec, err := h.mustJobsData().DescribeDeviceJobExecution(thing, jobID, executionNumber, includeDoc)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyExecution: toDeviceExecutionJSON(exec, false)})
}

func (h *Handler) handleUpdateDeviceJobExecution(c *echo.Context, thing, jobID string) error {
	var body struct {
		StatusDetails            map[string]string `json:"statusDetails"`
		Status                   string            `json:"status"`
		ExpectedVersion          int64             `json:"expectedVersion"`
		ExecutionNumber          int64             `json:"executionNumber"`
		IncludeJobDocument       bool              `json:"includeJobDocument"`
		IncludeJobExecutionState bool              `json:"includeJobExecutionState"`
	}
	if err := decodeJSONBody(c, &body); err != nil {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	exec, err := h.mustJobsData().UpdateDeviceJobExecution(&UpdateDeviceJobExecutionInput{
		JobID:           jobID,
		ThingName:       thing,
		Status:          body.Status,
		ExpectedVersion: body.ExpectedVersion,
		ExecutionNumber: body.ExecutionNumber,
		StatusDetails:   body.StatusDetails,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	out := map[string]any{}

	if body.IncludeJobExecutionState {
		out["executionState"] = map[string]any{
			"status":             exec.Status,
			jobsKeyStatusDetails: exec.StatusDetails,
			"versionNumber":      exec.VersionNumber,
		}
	}

	if body.IncludeJobDocument {
		out["jobDocument"] = exec.JobDocument
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) mustJobsData() jobsDataBackend {
	b, _ := h.jobsData()

	return b
}
