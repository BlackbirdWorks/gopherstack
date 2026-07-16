package transcribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

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

// transcribeTag is the wire representation of a single AWS resource tag, matching
// the real Transcribe SDK's types.Tag shape ({Key, Value}). Real AWS Transcribe
// serializes Tags as a JSON array of these objects, never as a plain JSON object/map.
type transcribeTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToMap converts the wire-format tag list into the plain map[string]string
// used for internal backend storage.
func tagsToMap(tags []transcribeTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// tagsFromMap converts an internal map[string]string tag collection into the
// wire-format tag list expected by real AWS Transcribe clients.
func tagsFromMap(tags map[string]string) []transcribeTag {
	if len(tags) == 0 {
		return nil
	}

	list := make([]transcribeTag, 0, len(tags))
	for k, v := range tags {
		list = append(list, transcribeTag{Key: k, Value: v})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })

	return list
}

// transcriptOutput is the shared Transcript wire shape returned by Get/Start
// operations across transcription jobs, call analytics jobs, and medical
// transcription jobs.
type transcriptOutput struct {
	RedactedTranscriptFileURI *string `json:"RedactedTranscriptFileUri"`
	TranscriptFileURI         string  `json:"TranscriptFileUri"`
}

// allSupportedOps returns all 43 supported operations in sorted order.
func allSupportedOps() []string {
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
		"DeleteMedicalVocabulary",
		"DeleteTranscriptionJob",
		"DeleteVocabulary",
		"DeleteVocabularyFilter",
		"DescribeLanguageModel",
		"GetCallAnalyticsCategory",
		"GetCallAnalyticsJob",
		"GetMedicalScribeJob",
		"GetMedicalTranscriptionJob",
		"GetMedicalVocabulary",
		"GetTranscriptionJob",
		"GetVocabulary",
		"GetVocabularyFilter",
		"ListCallAnalyticsCategories",
		"ListCallAnalyticsJobs",
		"ListLanguageModels",
		"ListMedicalScribeJobs",
		"ListMedicalTranscriptionJobs",
		"ListMedicalVocabularies",
		"ListTagsForResource",
		"ListTranscriptionJobs",
		"ListVocabularies",
		"ListVocabularyFilters",
		"StartCallAnalyticsJob",
		"StartMedicalScribeJob",
		"StartMedicalTranscriptionJob",
		"StartTranscriptionJob",
		"TagResource",
		"UntagResource",
		"UpdateCallAnalyticsCategory",
		"UpdateMedicalVocabulary",
		"UpdateVocabulary",
		"UpdateVocabularyFilter",
	}
}
