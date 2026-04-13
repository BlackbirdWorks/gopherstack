package transcribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	Backend StorageBackend
}

// NewHandler creates a new Transcribe handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Transcribe" }

// GetSupportedOperations returns the list of supported Transcribe operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCallAnalyticsCategory",
		"CreateLanguageModel",
		"CreateMedicalVocabulary",
		"CreateVocabulary",
		"CreateVocabularyFilter",
		"DeleteCallAnalyticsCategory",
		"DeleteCallAnalyticsJob",
		"DeleteLanguageModel",
		"DeleteMedicalScribeJob",
		"DeleteMedicalTranscriptionJob",
		"DeleteTranscriptionJob",
		"GetTranscriptionJob",
		"ListTranscriptionJobs",
		"StartTranscriptionJob",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "transcribe" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Transcribe instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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
	body, err := httputils.ReadBody(c.Request())
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
		"StartTranscriptionJob":         service.WrapOp(h.handleStartTranscriptionJob),
		"GetTranscriptionJob":           service.WrapOp(h.handleGetTranscriptionJob),
		"ListTranscriptionJobs":         service.WrapOp(h.handleListTranscriptionJobs),
		"DeleteTranscriptionJob":        service.WrapOp(h.handleDeleteTranscriptionJob),
		"CreateCallAnalyticsCategory":   service.WrapOp(h.handleCreateCallAnalyticsCategory),
		"DeleteCallAnalyticsCategory":   service.WrapOp(h.handleDeleteCallAnalyticsCategory),
		"CreateLanguageModel":           service.WrapOp(h.handleCreateLanguageModel),
		"DeleteLanguageModel":           service.WrapOp(h.handleDeleteLanguageModel),
		"CreateMedicalVocabulary":       service.WrapOp(h.handleCreateMedicalVocabulary),
		"CreateVocabulary":              service.WrapOp(h.handleCreateVocabulary),
		"CreateVocabularyFilter":        service.WrapOp(h.handleCreateVocabularyFilter),
		"DeleteCallAnalyticsJob":        service.WrapOp(h.handleDeleteCallAnalyticsJob),
		"DeleteMedicalScribeJob":        service.WrapOp(h.handleDeleteMedicalScribeJob),
		"DeleteMedicalTranscriptionJob": service.WrapOp(h.handleDeleteMedicalTranscriptionJob),
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
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type transcriptOutput struct {
	RedactedTranscriptFileURI *string `json:"RedactedTranscriptFileUri"`
	TranscriptFileURI         string  `json:"TranscriptFileUri"`
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

// transcribeMedia holds the media file URI for a transcription job request.
type transcribeMedia struct {
	MediaFileURI string `json:"MediaFileUri"`
}

type handleStartTranscriptionJobInput struct {
	TranscriptionJobName string          `json:"TranscriptionJobName"`
	LanguageCode         string          `json:"LanguageCode"`
	Media                transcribeMedia `json:"Media"`
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
	NextToken                 string                    `json:"NextToken,omitempty"`
	TranscriptionJobSummaries []transcriptionJobSummary `json:"TranscriptionJobSummaries"`
}

type handleListTranscriptionJobsInput struct {
	Status    string `json:"Status"`
	NextToken string `json:"NextToken"`
}

func (h *Handler) handleListTranscriptionJobs(
	_ context.Context,
	in *handleListTranscriptionJobsInput,
) (*listTranscriptionJobsOutput, error) {
	jobs, nextToken := h.Backend.ListTranscriptionJobs(in.Status, in.NextToken)

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
		NextToken:                 nextToken,
	}, nil
}

func (h *Handler) handleDeleteTranscriptionJob(
	_ context.Context,
	in *transcriptionJobNameInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteTranscriptionJob(in.TranscriptionJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- CreateCallAnalyticsCategory ---

type createCallAnalyticsCategoryInput struct {
	CategoryName string `json:"CategoryName"`
	InputType    string `json:"InputType"`
}

type callAnalyticsCategoryProperties struct {
	CategoryName string `json:"CategoryName"`
	InputType    string `json:"InputType,omitempty"`
}

type createCallAnalyticsCategoryOutput struct {
	CategoryProperties *callAnalyticsCategoryProperties `json:"CategoryProperties"`
}

func (h *Handler) handleCreateCallAnalyticsCategory(
	_ context.Context,
	in *createCallAnalyticsCategoryInput,
) (*createCallAnalyticsCategoryOutput, error) {
	if in.CategoryName == "" {
		return nil, fmt.Errorf("%w: CategoryName is required", errInvalidRequest)
	}

	cat, err := h.Backend.CreateCallAnalyticsCategory(in.CategoryName, in.InputType)
	if err != nil {
		return nil, err
	}

	return &createCallAnalyticsCategoryOutput{
		CategoryProperties: &callAnalyticsCategoryProperties{
			CategoryName: cat.CategoryName,
			InputType:    cat.InputType,
		},
	}, nil
}

// --- DeleteCallAnalyticsCategory ---

type deleteCallAnalyticsCategoryInput struct {
	CategoryName string `json:"CategoryName"`
}

func (h *Handler) handleDeleteCallAnalyticsCategory(
	_ context.Context,
	in *deleteCallAnalyticsCategoryInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteCallAnalyticsCategory(in.CategoryName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- CreateLanguageModel ---

type createLanguageModelInput struct {
	ModelName     string `json:"ModelName"`
	BaseModelName string `json:"BaseModelName"`
	LanguageCode  string `json:"LanguageCode"`
}

type createLanguageModelOutput struct {
	ModelName     string `json:"ModelName"`
	BaseModelName string `json:"BaseModelName"`
	LanguageCode  string `json:"LanguageCode"`
	ModelStatus   string `json:"ModelStatus"`
}

func (h *Handler) handleCreateLanguageModel(
	_ context.Context,
	in *createLanguageModelInput,
) (*createLanguageModelOutput, error) {
	if in.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	m, err := h.Backend.CreateLanguageModel(in.ModelName, in.BaseModelName, in.LanguageCode)
	if err != nil {
		return nil, err
	}

	return &createLanguageModelOutput{
		ModelName:     m.ModelName,
		BaseModelName: m.BaseModelName,
		LanguageCode:  m.LanguageCode,
		ModelStatus:   m.ModelStatus,
	}, nil
}

// --- DeleteLanguageModel ---

type deleteLanguageModelInput struct {
	ModelName string `json:"ModelName"`
}

func (h *Handler) handleDeleteLanguageModel(
	_ context.Context,
	in *deleteLanguageModelInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteLanguageModel(in.ModelName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- CreateMedicalVocabulary ---

type createMedicalVocabularyInput struct {
	VocabularyName    string `json:"VocabularyName"`
	LanguageCode      string `json:"LanguageCode"`
	VocabularyFileURI string `json:"VocabularyFileUri"`
}

type createMedicalVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleCreateMedicalVocabulary(
	_ context.Context,
	in *createMedicalVocabularyInput,
) (*createMedicalVocabularyOutput, error) {
	if in.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", errInvalidRequest)
	}

	v, err := h.Backend.CreateMedicalVocabulary(in.VocabularyName, in.LanguageCode, in.VocabularyFileURI)
	if err != nil {
		return nil, err
	}

	return &createMedicalVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- CreateVocabulary ---

type createVocabularyInput struct {
	VocabularyName string `json:"VocabularyName"`
	LanguageCode   string `json:"LanguageCode"`
}

type createVocabularyOutput struct {
	VocabularyName  string `json:"VocabularyName"`
	LanguageCode    string `json:"LanguageCode"`
	VocabularyState string `json:"VocabularyState"`
}

func (h *Handler) handleCreateVocabulary(
	_ context.Context,
	in *createVocabularyInput,
) (*createVocabularyOutput, error) {
	if in.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", errInvalidRequest)
	}

	v, err := h.Backend.CreateVocabulary(in.VocabularyName, in.LanguageCode)
	if err != nil {
		return nil, err
	}

	return &createVocabularyOutput{
		VocabularyName:  v.VocabularyName,
		LanguageCode:    v.LanguageCode,
		VocabularyState: v.VocabularyState,
	}, nil
}

// --- CreateVocabularyFilter ---

type createVocabularyFilterInput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
	LanguageCode         string `json:"LanguageCode"`
}

type createVocabularyFilterOutput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
	LanguageCode         string `json:"LanguageCode"`
}

func (h *Handler) handleCreateVocabularyFilter(
	_ context.Context,
	in *createVocabularyFilterInput,
) (*createVocabularyFilterOutput, error) {
	if in.VocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", errInvalidRequest)
	}

	f, err := h.Backend.CreateVocabularyFilter(in.VocabularyFilterName, in.LanguageCode)
	if err != nil {
		return nil, err
	}

	return &createVocabularyFilterOutput{
		VocabularyFilterName: f.VocabularyFilterName,
		LanguageCode:         f.LanguageCode,
	}, nil
}

// --- DeleteCallAnalyticsJob ---

type deleteCallAnalyticsJobInput struct {
	CallAnalyticsJobName string `json:"CallAnalyticsJobName"`
}

func (h *Handler) handleDeleteCallAnalyticsJob(
	_ context.Context,
	in *deleteCallAnalyticsJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteCallAnalyticsJob(in.CallAnalyticsJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- DeleteMedicalScribeJob ---

type deleteMedicalScribeJobInput struct {
	MedicalScribeJobName string `json:"MedicalScribeJobName"`
}

func (h *Handler) handleDeleteMedicalScribeJob(
	_ context.Context,
	in *deleteMedicalScribeJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalScribeJob(in.MedicalScribeJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- DeleteMedicalTranscriptionJob ---

type deleteMedicalTranscriptionJobInput struct {
	MedicalTranscriptionJobName string `json:"MedicalTranscriptionJobName"`
}

func (h *Handler) handleDeleteMedicalTranscriptionJob(
	_ context.Context,
	in *deleteMedicalTranscriptionJobInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteMedicalTranscriptionJob(in.MedicalTranscriptionJobName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
