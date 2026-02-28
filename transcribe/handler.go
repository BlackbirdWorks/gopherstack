package transcribe

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const transcribeTargetPrefix = "Transcribe."

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
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), transcribeTargetPrefix)
		switch action {
		case "StartTranscriptionJob":
			return h.handleStartTranscriptionJob(c, body)
		case "GetTranscriptionJob":
			return h.handleGetTranscriptionJob(c, body)
		case "ListTranscriptionJobs":
			return h.handleListTranscriptionJobs(c, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

type handleStartTranscriptionJobInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
	LanguageCode         string `json:"LanguageCode"`
	Media                struct {
		MediaFileURI string `json:"MediaFileUri"`
	} `json:"Media"`
}

func (h *Handler) handleStartTranscriptionJob(c *echo.Context, body []byte) error {
	var req handleStartTranscriptionJobInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if req.TranscriptionJobName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "TranscriptionJobName is required"})
	}

	job, err := h.Backend.StartTranscriptionJob(req.TranscriptionJobName, req.LanguageCode, req.Media.MediaFileURI)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TranscriptionJob": map[string]any{
			"TranscriptionJobName":   job.JobName,
			"TranscriptionJobStatus": job.JobStatus,
			"LanguageCode":           job.LanguageCode,
			"Transcript": map[string]string{
				"TranscriptFileUri": "s3://synthetic-transcripts/" + job.JobName + ".json",
			},
		},
	})
}

func (h *Handler) handleGetTranscriptionJob(c *echo.Context, body []byte) error {
	var req transcriptionJobNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	job, err := h.Backend.GetTranscriptionJob(req.TranscriptionJobName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TranscriptionJob": map[string]any{
			"TranscriptionJobName":   job.JobName,
			"TranscriptionJobStatus": job.JobStatus,
			"LanguageCode":           job.LanguageCode,
			"Transcript": map[string]any{
				"TranscriptFileUri":         "s3://synthetic-transcripts/" + job.JobName + ".json",
				"RedactedTranscriptFileUri": nil,
			},
		},
	})
}

type handleListTranscriptionJobsInput struct {
	Status string `json:"Status"`
}

func (h *Handler) handleListTranscriptionJobs(c *echo.Context, body []byte) error {
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

	return c.JSON(http.StatusOK, map[string]any{
		"TranscriptionJobSummaries": summaries,
	})
}
