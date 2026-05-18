package transcribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Transcribe handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears handler state by delegating to the backend.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Transcribe" }

// GetSupportedOperations returns the list of supported Transcribe operations.
func (h *Handler) GetSupportedOperations() []string {
	return allSupportedOps()
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

// extractResourceRequest is the union of all resource-name fields across Transcribe operations.
type extractResourceRequest struct {
	TranscriptionJobName        string `json:"TranscriptionJobName"`
	CallAnalyticsJobName        string `json:"CallAnalyticsJobName"`
	CategoryName                string `json:"CategoryName"`
	ModelName                   string `json:"ModelName"`
	VocabularyName              string `json:"VocabularyName"`
	VocabularyFilterName        string `json:"VocabularyFilterName"`
	MedicalScribeJobName        string `json:"MedicalScribeJobName"`
	MedicalTranscriptionJobName string `json:"MedicalTranscriptionJobName"`
}

// ExtractResource extracts the primary resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractResourceRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return ""
	}

	switch {
	case req.TranscriptionJobName != "":
		return req.TranscriptionJobName
	case req.CallAnalyticsJobName != "":
		return req.CallAnalyticsJobName
	case req.CategoryName != "":
		return req.CategoryName
	case req.ModelName != "":
		return req.ModelName
	case req.VocabularyName != "":
		return req.VocabularyName
	case req.VocabularyFilterName != "":
		return req.VocabularyFilterName
	case req.MedicalScribeJobName != "":
		return req.MedicalScribeJobName
	case req.MedicalTranscriptionJobName != "":
		return req.MedicalTranscriptionJobName
	default:
		return ""
	}
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Transcribe", "application/x-amz-json-1.1",
			allSupportedOps(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		// Transcription jobs
		"StartTranscriptionJob":  service.WrapOp(h.handleStartTranscriptionJob),
		"GetTranscriptionJob":    service.WrapOp(h.handleGetTranscriptionJob),
		"ListTranscriptionJobs":  service.WrapOp(h.handleListTranscriptionJobs),
		"DeleteTranscriptionJob": service.WrapOp(h.handleDeleteTranscriptionJob),
		// Call Analytics categories
		"CreateCallAnalyticsCategory": service.WrapOp(h.handleCreateCallAnalyticsCategory),
		"DeleteCallAnalyticsCategory": service.WrapOp(h.handleDeleteCallAnalyticsCategory),
		"GetCallAnalyticsCategory":    service.WrapOp(h.handleGetCallAnalyticsCategory),
		"UpdateCallAnalyticsCategory": service.WrapOp(h.handleUpdateCallAnalyticsCategory),
		"ListCallAnalyticsCategories": service.WrapOp(h.handleListCallAnalyticsCategories),
		// Call Analytics jobs
		"StartCallAnalyticsJob":  service.WrapOp(h.handleStartCallAnalyticsJob),
		"GetCallAnalyticsJob":    service.WrapOp(h.handleGetCallAnalyticsJob),
		"ListCallAnalyticsJobs":  service.WrapOp(h.handleListCallAnalyticsJobs),
		"DeleteCallAnalyticsJob": service.WrapOp(h.handleDeleteCallAnalyticsJob),
		// Language models
		"CreateLanguageModel":   service.WrapOp(h.handleCreateLanguageModel),
		"DeleteLanguageModel":   service.WrapOp(h.handleDeleteLanguageModel),
		"DescribeLanguageModel": service.WrapOp(h.handleDescribeLanguageModel),
		"ListLanguageModels":    service.WrapOp(h.handleListLanguageModels),
		// Vocabularies
		"CreateVocabulary": service.WrapOp(h.handleCreateVocabulary),
		"GetVocabulary":    service.WrapOp(h.handleGetVocabulary),
		"UpdateVocabulary": service.WrapOp(h.handleUpdateVocabulary),
		"DeleteVocabulary": service.WrapOp(h.handleDeleteVocabulary),
		"ListVocabularies": service.WrapOp(h.handleListVocabularies),
		// Vocabulary filters
		"CreateVocabularyFilter": service.WrapOp(h.handleCreateVocabularyFilter),
		"GetVocabularyFilter":    service.WrapOp(h.handleGetVocabularyFilter),
		"UpdateVocabularyFilter": service.WrapOp(h.handleUpdateVocabularyFilter),
		"DeleteVocabularyFilter": service.WrapOp(h.handleDeleteVocabularyFilter),
		"ListVocabularyFilters":  service.WrapOp(h.handleListVocabularyFilters),
		// Medical vocabularies
		"CreateMedicalVocabulary": service.WrapOp(h.handleCreateMedicalVocabulary),
		"GetMedicalVocabulary":    service.WrapOp(h.handleGetMedicalVocabulary),
		"UpdateMedicalVocabulary": service.WrapOp(h.handleUpdateMedicalVocabulary),
		"DeleteMedicalVocabulary": service.WrapOp(h.handleDeleteMedicalVocabulary),
		"ListMedicalVocabularies": service.WrapOp(h.handleListMedicalVocabularies),
		// Medical Scribe jobs
		"StartMedicalScribeJob":  service.WrapOp(h.handleStartMedicalScribeJob),
		"GetMedicalScribeJob":    service.WrapOp(h.handleGetMedicalScribeJob),
		"ListMedicalScribeJobs":  service.WrapOp(h.handleListMedicalScribeJobs),
		"DeleteMedicalScribeJob": service.WrapOp(h.handleDeleteMedicalScribeJob),
		// Medical Transcription jobs
		"StartMedicalTranscriptionJob":  service.WrapOp(h.handleStartMedicalTranscriptionJob),
		"GetMedicalTranscriptionJob":    service.WrapOp(h.handleGetMedicalTranscriptionJob),
		"ListMedicalTranscriptionJobs":  service.WrapOp(h.handleListMedicalTranscriptionJobs),
		"DeleteMedicalTranscriptionJob": service.WrapOp(h.handleDeleteMedicalTranscriptionJob),
		// Tags
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errorBody("NotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorBody("ConflictException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter), errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, errorBody("BadRequestException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorBody("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorBody("InternalFailureException", err.Error()))
	}
}

// errorBody builds the standard Transcribe error response body.
func errorBody(code, msg string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": msg,
	}
}

type transcriptionJobNameInput struct {
	TranscriptionJobName string `json:"TranscriptionJobName"`
}

type transcriptOutput struct {
	RedactedTranscriptFileURI *string `json:"RedactedTranscriptFileUri"`
	TranscriptFileURI         string  `json:"TranscriptFileUri"`
}

type transcriptionJobOutput struct {
	Tags                      map[string]string           `json:"Tags,omitempty"`
	Settings                  *TranscriptionSettings      `json:"Settings,omitempty"`
	ModelSettings             *ModelSettings              `json:"ModelSettings,omitempty"`
	ContentRedaction          *ContentRedaction           `json:"ContentRedaction,omitempty"`
	Subtitles                 *SubtitlesOutput            `json:"Subtitles,omitempty"`
	Transcript                transcriptOutput            `json:"Transcript"`
	CreationTime              *string                     `json:"CreationTime,omitempty"`
	StartTime                 *string                     `json:"StartTime,omitempty"`
	CompletionTime            *string                     `json:"CompletionTime,omitempty"`
	TranscriptionJobName      string                      `json:"TranscriptionJobName"`
	TranscriptionJobStatus    string                      `json:"TranscriptionJobStatus"`
	LanguageCode              string                      `json:"LanguageCode,omitempty"`
	MediaFormat               string                      `json:"MediaFormat,omitempty"`
	OutputBucketName          string                      `json:"OutputBucketName,omitempty"`
	OutputKey                 string                      `json:"OutputKey,omitempty"`
	FailureReason             string                      `json:"FailureReason,omitempty"`
	LanguageOptions           []string                    `json:"LanguageOptions,omitempty"`
	ToxicityDetection         []ToxicityDetectionSettings `json:"ToxicityDetection,omitempty"`
	IdentifiedLanguageScore   float32                     `json:"IdentifiedLanguageScore,omitempty"`
	MediaSampleRateHertz      int32                       `json:"MediaSampleRateHertz,omitempty"`
	IdentifyLanguage          bool                        `json:"IdentifyLanguage,omitempty"`
	IdentifyMultipleLanguages bool                        `json:"IdentifyMultipleLanguages,omitempty"`
}

type startTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type getTranscriptionJobOutput struct {
	TranscriptionJob transcriptionJobOutput `json:"TranscriptionJob"`
}

type handleStartTranscriptionJobInput struct {
	Settings                  *TranscriptionSettings      `json:"Settings"`
	Tags                      map[string]string           `json:"Tags"`
	Subtitles                 *SubtitlesInput             `json:"Subtitles"`
	ContentRedaction          *ContentRedaction           `json:"ContentRedaction"`
	ModelSettings             *ModelSettings              `json:"ModelSettings"`
	Media                     Media                       `json:"Media"`
	MediaFormat               string                      `json:"MediaFormat"`
	OutputEncryptionKMSKeyID  string                      `json:"OutputEncryptionKMSKeyID"`
	OutputKey                 string                      `json:"OutputKey"`
	OutputBucketName          string                      `json:"OutputBucketName"`
	TranscriptionJobName      string                      `json:"TranscriptionJobName"`
	LanguageCode              string                      `json:"LanguageCode"`
	LanguageOptions           []string                    `json:"LanguageOptions"`
	ToxicityDetection         []ToxicityDetectionSettings `json:"ToxicityDetection"`
	MediaSampleRateHertz      int32                       `json:"MediaSampleRateHertz"`
	IdentifyMultipleLanguages bool                        `json:"IdentifyMultipleLanguages"`
	IdentifyLanguage          bool                        `json:"IdentifyLanguage"`
}

func (h *Handler) handleStartTranscriptionJob(
	_ context.Context,
	in *handleStartTranscriptionJobInput,
) (*startTranscriptionJobOutput, error) {
	subtitlesOut := (*SubtitlesOutput)(nil)
	if in.Subtitles != nil {
		subtitlesOut = &SubtitlesOutput{Formats: in.Subtitles.Formats, OutputStartIndex: in.Subtitles.OutputStartIndex}
	}

	job, err := h.Backend.StartTranscriptionJob(&TranscriptionJob{
		JobName:                   in.TranscriptionJobName,
		LanguageCode:              in.LanguageCode,
		Media:                     in.Media,
		MediaFormat:               in.MediaFormat,
		MediaSampleRateHertz:      in.MediaSampleRateHertz,
		OutputBucketName:          in.OutputBucketName,
		OutputKey:                 in.OutputKey,
		OutputEncryptionKMSKeyID:  in.OutputEncryptionKMSKeyID,
		Settings:                  in.Settings,
		ModelSettings:             in.ModelSettings,
		ContentRedaction:          in.ContentRedaction,
		Subtitles:                 subtitlesOut,
		IdentifyLanguage:          in.IdentifyLanguage,
		IdentifyMultipleLanguages: in.IdentifyMultipleLanguages,
		LanguageOptions:           in.LanguageOptions,
		ToxicityDetection:         in.ToxicityDetection,
		Tags:                      in.Tags,
	})
	if err != nil {
		return nil, err
	}

	transcriptURI := buildTranscriptURI(job)

	return &startTranscriptionJobOutput{
		TranscriptionJob: buildTranscriptionJobOutput(job, transcriptURI),
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

	transcriptURI := buildTranscriptURI(job)

	return &getTranscriptionJobOutput{
		TranscriptionJob: buildTranscriptionJobOutput(job, transcriptURI),
	}, nil
}

func buildTranscriptURI(job *TranscriptionJob) string {
	if job.OutputBucketName != "" {
		key := job.OutputKey
		if key == "" {
			key = job.JobName + ".json"
		}

		return "s3://" + job.OutputBucketName + "/" + key
	}

	return "s3://synthetic-transcripts/" + job.JobName + ".json"
}

func buildTranscriptionJobOutput(job *TranscriptionJob, transcriptURI string) transcriptionJobOutput {
	out := transcriptionJobOutput{
		TranscriptionJobName:      job.JobName,
		TranscriptionJobStatus:    job.JobStatus,
		LanguageCode:              job.LanguageCode,
		MediaFormat:               job.MediaFormat,
		MediaSampleRateHertz:      job.MediaSampleRateHertz,
		OutputBucketName:          job.OutputBucketName,
		OutputKey:                 job.OutputKey,
		FailureReason:             job.FailureReason,
		Settings:                  job.Settings,
		ModelSettings:             job.ModelSettings,
		ContentRedaction:          job.ContentRedaction,
		Subtitles:                 job.Subtitles,
		IdentifyLanguage:          job.IdentifyLanguage,
		IdentifyMultipleLanguages: job.IdentifyMultipleLanguages,
		LanguageOptions:           job.LanguageOptions,
		ToxicityDetection:         job.ToxicityDetection,
		IdentifiedLanguageScore:   job.IdentifiedLanguageScore,
		Tags:                      job.Tags,
		Transcript: transcriptOutput{
			TranscriptFileURI: transcriptURI,
		},
	}

	if job.ContentRedaction != nil {
		redacted := "s3://synthetic-transcripts/" + job.JobName + "-redacted.json"
		out.Transcript.RedactedTranscriptFileURI = &redacted
	}

	if !job.CreationTime.IsZero() {
		s := job.CreationTime.Format(time.RFC3339)
		out.CreationTime = &s
	}

	if !job.StartTime.IsZero() {
		s := job.StartTime.Format(time.RFC3339)
		out.StartTime = &s
	}

	if !job.CompletionTime.IsZero() {
		s := job.CompletionTime.Format(time.RFC3339)
		out.CompletionTime = &s
	}

	return out
}

type transcriptionJobSummary struct {
	CreationTime           *string `json:"CreationTime,omitempty"`
	CompletionTime         *string `json:"CompletionTime,omitempty"`
	TranscriptionJobName   string  `json:"TranscriptionJobName"`
	TranscriptionJobStatus string  `json:"TranscriptionJobStatus"`
	LanguageCode           string  `json:"LanguageCode,omitempty"`
	FailureReason          string  `json:"FailureReason,omitempty"`
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
		s := transcriptionJobSummary{
			TranscriptionJobName:   j.JobName,
			TranscriptionJobStatus: j.JobStatus,
			LanguageCode:           j.LanguageCode,
			FailureReason:          j.FailureReason,
		}

		if !j.CreationTime.IsZero() {
			ts := j.CreationTime.Format(time.RFC3339)
			s.CreationTime = &ts
		}

		if !j.CompletionTime.IsZero() {
			ts := j.CompletionTime.Format(time.RFC3339)
			s.CompletionTime = &ts
		}

		summaries = append(summaries, s)
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
	CategoryName string              `json:"CategoryName"`
	InputType    string              `json:"InputType"`
	Rules        []CallAnalyticsRule `json:"Rules"`
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
	cat, err := h.Backend.CreateCallAnalyticsCategory(&CallAnalyticsCategory{
		CategoryName: in.CategoryName,
		InputType:    in.InputType,
		Rules:        in.Rules,
	})
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
	InputDataConfig *InputDataConfig `json:"InputDataConfig"`
	ModelName       string           `json:"ModelName"`
	BaseModelName   string           `json:"BaseModelName"`
	LanguageCode    string           `json:"LanguageCode"`
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
	m, err := h.Backend.CreateLanguageModel(&LanguageModel{
		ModelName:       in.ModelName,
		BaseModelName:   in.BaseModelName,
		LanguageCode:    in.LanguageCode,
		InputDataConfig: in.InputDataConfig,
	})
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
	VocabularyFileURI string `json:"VocabularyFileURI"`
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
	VocabularyName    string   `json:"VocabularyName"`
	LanguageCode      string   `json:"LanguageCode"`
	VocabularyFileURI string   `json:"VocabularyFileURI"`
	Phrases           []string `json:"Phrases"`
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
	v, err := h.Backend.CreateVocabulary(&Vocabulary{
		VocabularyName:    in.VocabularyName,
		LanguageCode:      in.LanguageCode,
		Phrases:           in.Phrases,
		VocabularyFileURI: in.VocabularyFileURI,
	})
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
	VocabularyFilterName    string   `json:"VocabularyFilterName"`
	LanguageCode            string   `json:"LanguageCode"`
	VocabularyFilterFileURI string   `json:"VocabularyFilterFileURI"`
	Words                   []string `json:"Words"`
}

type createVocabularyFilterOutput struct {
	VocabularyFilterName string `json:"VocabularyFilterName"`
	LanguageCode         string `json:"LanguageCode"`
}

func (h *Handler) handleCreateVocabularyFilter(
	_ context.Context,
	in *createVocabularyFilterInput,
) (*createVocabularyFilterOutput, error) {
	f, err := h.Backend.CreateVocabularyFilter(&VocabularyFilter{
		VocabularyFilterName:    in.VocabularyFilterName,
		LanguageCode:            in.LanguageCode,
		Words:                   in.Words,
		VocabularyFilterFileURI: in.VocabularyFilterFileURI,
	})
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
