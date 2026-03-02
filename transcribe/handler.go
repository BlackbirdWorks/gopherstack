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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"StartTranscriptionJob": service.WrapOp(h.handleStartTranscriptionJob),
		"GetTranscriptionJob":   service.WrapOp(h.handleGetTranscriptionJob),
		"ListTranscriptionJobs": service.WrapOp(h.handleListTranscriptionJobs),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
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

type transcriptOutput struct {
	RedactedTranscriptFileURI *string `json:"RedactedTranscriptFileURI"`
	TranscriptFileURI         string  `json:"TranscriptFileURI"`
}

type transcriptionJobOutput struct {
	Transcript             transcriptOutput `json:"Transcript"`
	TranscriptionJobName   string           `json:"TranscriptionJobName"`
	TranscriptionJobStatus string           `json:"TranscriptionJobStatus"`
	LanguageCode           string           `json:"LanguageCode"`
}

type startTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type getTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type handleStartTranscriptionJobInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
	LanguageCode         string `json:"LanguageCode"`
	Media                struct {
		MediaFileURI string `json:"MediaFileUri"`
	} `json:"Media"`
}

func (h *Handler) handleStartTranscriptionJob(
	_ context.Context,
	in *handleStartTranscriptionJobInput,
) (*startTranscriptionJobOutput, error) {
	if in.TranscriptionJobName == "" {
		return nil, fmt.Errorf("%w: TranscriptionJobName is required", errInvalidRequest)
	}

	job, err := h.Backend.StartTranscriptionJob(in.TranscriptionJobName, in.LanguageCode, in.Media.MediaFileURI)
	if err != nil {
		return nil, err
	}

	return &startTranscriptionJobOutput{
		TranscriptionJob: transcriptionJobOutput{
			TranscriptionJobName:   job.JobName,
			TranscriptionJobStatus: job.JobStatus,
			LanguageCode:           job.LanguageCode,
			Transcript: transcriptOutput{
				TranscriptFileURI: "s3://synthetic-transcripts/" + job.JobName + ".json",
			},
		},
	}, nil
}

func (h *Handler) handleGetTranscriptionJob(
	_ context.Context,
	in *transcriptionJobNameInput,
) (*getTranscriptionJobOutput, error) {
	job, err := h.Backend.GetTranscriptionJob(in.TranscriptionJobName)
	if err != nil {
		return nil, err
	}

	return &getTranscriptionJobOutput{
		TranscriptionJob: transcriptionJobOutput{
			TranscriptionJobName:   job.JobName,
			TranscriptionJobStatus: job.JobStatus,
			LanguageCode:           job.LanguageCode,
			Transcript: transcriptOutput{
				TranscriptFileURI:         "s3://synthetic-transcripts/" + job.JobName + ".json",
				RedactedTranscriptFileURI: nil,
			},
		},
	}, nil
}

type transcriptionJobSummary struct {
	TranscriptionJobName   string `json:"TranscriptionJobName"`
	TranscriptionJobStatus string `json:"TranscriptionJobStatus"`
	LanguageCode           string `json:"LanguageCode"`
}

type listTranscriptionJobsOutput struct {
	TranscriptionJobSummaries []transcriptionJobSummary `json:"TranscriptionJobSummaries"`
}

type handleListTranscriptionJobsInput struct {
	Status string `json:"Status"`
}

func (h *Handler) handleListTranscriptionJobs(
	_ context.Context,
	in *handleListTranscriptionJobsInput,
) (*listTranscriptionJobsOutput, error) {
	jobs := h.Backend.ListTranscriptionJobs(in.Status)

	summaries := make([]transcriptionJobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, transcriptionJobSummary{
			TranscriptionJobName:   j.JobName,
			TranscriptionJobStatus: j.JobStatus,
			LanguageCode:           j.LanguageCode,
		})
	}

	return &listTranscriptionJobsOutput{
		TranscriptionJobSummaries: summaries,
	}, nil
}
