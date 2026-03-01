package transcribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const transcribeTargetPrefix = "Transcribe."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Transcribe operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Transcribe handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Transcribe" }

// GetSupportedOperations returns the list of supported Transcribe operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"StartTranscriptionJob",
		"GetTranscriptionJob",
		"ListTranscriptionJobs",
	}
}

// RouteMatcher returns a function that matches Transcribe requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), transcribeTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Transcribe action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, transcribeTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type transcriptionJobNameInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
}

// ExtractResource extracts the job name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req transcriptionJobNameInput
	_ = json.Unmarshal(body, &req)

	return req.TranscriptionJobName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Transcribe", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "StartTranscriptionJob":
		result, err = h.handleStartTranscriptionJob(body)
	case "GetTranscriptionJob":
		result, err = h.handleGetTranscriptionJob(body)
	case "ListTranscriptionJobs":
		result, err = h.handleListTranscriptionJobs(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type handleStartTranscriptionJobInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
	LanguageCode         string `json:"LanguageCode"`
	Media                struct {
		MediaFileURI string `json:"MediaFileUri"`
	} `json:"Media"`
}

func (h *Handler) handleStartTranscriptionJob(body []byte) (any, error) {
	var req handleStartTranscriptionJobInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if req.TranscriptionJobName == "" {
		return nil, fmt.Errorf("%w: TranscriptionJobName is required", errInvalidRequest)
	}

	job, err := h.Backend.StartTranscriptionJob(req.TranscriptionJobName, req.LanguageCode, req.Media.MediaFileURI)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TranscriptionJob": map[string]any{
			"TranscriptionJobName":   job.JobName,
			"TranscriptionJobStatus": job.JobStatus,
			"LanguageCode":           job.LanguageCode,
			"Transcript": map[string]string{
				"TranscriptFileUri": "s3://synthetic-transcripts/" + job.JobName + ".json",
			},
		},
	}, nil
}

func (h *Handler) handleGetTranscriptionJob(body []byte) (any, error) {
	var req transcriptionJobNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	job, err := h.Backend.GetTranscriptionJob(req.TranscriptionJobName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TranscriptionJob": map[string]any{
			"TranscriptionJobName":   job.JobName,
			"TranscriptionJobStatus": job.JobStatus,
			"LanguageCode":           job.LanguageCode,
			"Transcript": map[string]any{
				"TranscriptFileUri":         "s3://synthetic-transcripts/" + job.JobName + ".json",
				"RedactedTranscriptFileUri": nil,
			},
		},
	}, nil
}

type handleListTranscriptionJobsInput struct {
	Status string `json:"Status"`
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListTranscriptionJobs(body []byte) (any, error) {
	var req handleListTranscriptionJobsInput
	_ = json.Unmarshal(body, &req)

	jobs := h.Backend.ListTranscriptionJobs(req.Status)

	summaries := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, map[string]any{
			"TranscriptionJobName":   j.JobName,
			"TranscriptionJobStatus": j.JobStatus,
			"LanguageCode":           j.LanguageCode,
		})
	}

	return map[string]any{
		"TranscriptionJobSummaries": summaries,
	}, nil
}
