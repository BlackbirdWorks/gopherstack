package textract

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

const (
	textractTargetPrefix = "Textract."
	keyTypeField         = "__type"
	keyMessageField      = "message"
	modelVersion10       = "1.0"
	textractContentType  = "application/x-amz-json-1.1"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// validateQueriesConfig returns an error when QUERIES is in featureTypes but
// no QueriesConfig is provided. Real AWS enforces this constraint.
func validateQueriesConfig(featureTypes []string, qc *QueriesConfig) error {
	for _, ft := range featureTypes {
		if ft == featureTypeQueries {
			if qc == nil || len(qc.Queries) == 0 {
				return fmt.Errorf(
					"%w: QueriesConfig must be provided when FeatureTypes includes QUERIES",
					errInvalidRequest,
				)
			}

			return nil
		}
	}

	return nil
}

func validateAnalyzeDocumentFeatureTypes(featureTypes []string) error {
	if len(featureTypes) == 0 {
		return fmt.Errorf("%w: FeatureTypes must contain at least one value", errInvalidRequest)
	}

	for _, ft := range featureTypes {
		switch ft {
		case featureTypeTables, featureTypeForms, featureTypeQueries,
			featureTypeSignatures, featureTypeLayout:
		default:
			return fmt.Errorf(
				"%w: invalid FeatureType %q (valid: TABLES, FORMS, QUERIES, SIGNATURES, LAYOUT)",
				errInvalidRequest,
				ft,
			)
		}
	}

	return nil
}

// Handler is the Echo HTTP handler for Amazon Textract operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Textract handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state. Implements service.Resettable.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Shutdown implements service.Shutdowner. It cancels any in-flight delayed
// async-job completion goroutines and waits for them to exit (bounded by ctx)
// so they cannot mutate backend state after the process begins shutting down.
func (h *Handler) Shutdown(ctx context.Context) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Shutdown(ctx)
	}
}

// Ensure Handler implements service.Shutdowner at compile time.
var _ service.Shutdowner = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string { return "Textract" }

// GetSupportedOperations returns the list of supported Textract operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AnalyzeDocument",
		"AnalyzeExpense",
		"AnalyzeID",
		"CreateAdapter",
		"CreateAdapterVersion",
		"DeleteAdapter",
		"DeleteAdapterVersion",
		"DetectDocumentText",
		"GetAdapter",
		"GetAdapterVersion",
		"GetDocumentAnalysis",
		"GetDocumentTextDetection",
		"GetExpenseAnalysis",
		"GetLendingAnalysis",
		"GetLendingAnalysisSummary",
		"ListAdapterVersions",
		"ListAdapters",
		"ListTagsForResource",
		"StartDocumentAnalysis",
		"StartDocumentTextDetection",
		"StartExpenseAnalysis",
		"StartLendingAnalysis",
		"TagResource",
		"UntagResource",
		"UpdateAdapter",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "textract" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Textract instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Textract requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), textractTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Textract action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, textractTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type documentLocationInput struct {
	DocumentLocation struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
}

// ExtractResource extracts the S3 document URI from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req documentLocationInput
	_ = json.Unmarshal(body, &req)

	bucket := req.DocumentLocation.S3Object.Bucket
	key := req.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return ""
	}

	return "s3://" + bucket + "/" + key
}

// regionFromRequest resolves the AWS region for a request from its SigV4
// credential scope, falling back to the backend's default region.
func (h *Handler) regionFromRequest(c *echo.Context) string {
	return httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := h.regionFromRequest(c)

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Textract", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AnalyzeDocument":            service.WrapOp(h.handleAnalyzeDocument),
		"AnalyzeExpense":             service.WrapOp(h.handleAnalyzeExpense),
		"AnalyzeID":                  service.WrapOp(h.handleAnalyzeID),
		"CreateAdapter":              service.WrapOp(h.handleCreateAdapter),
		"CreateAdapterVersion":       service.WrapOp(h.handleCreateAdapterVersion),
		"DeleteAdapter":              service.WrapOp(h.handleDeleteAdapter),
		"DeleteAdapterVersion":       service.WrapOp(h.handleDeleteAdapterVersion),
		"DetectDocumentText":         service.WrapOp(h.handleDetectDocumentText),
		"GetAdapter":                 service.WrapOp(h.handleGetAdapter),
		"GetAdapterVersion":          service.WrapOp(h.handleGetAdapterVersion),
		"GetDocumentAnalysis":        service.WrapOp(h.handleGetDocumentAnalysis),
		"GetDocumentTextDetection":   service.WrapOp(h.handleGetDocumentTextDetection),
		"GetExpenseAnalysis":         service.WrapOp(h.handleGetExpenseAnalysis),
		"GetLendingAnalysis":         service.WrapOp(h.handleGetLendingAnalysis),
		"GetLendingAnalysisSummary":  service.WrapOp(h.handleGetLendingAnalysisSummary),
		"ListAdapterVersions":        service.WrapOp(h.handleListAdapterVersions),
		"ListAdapters":               service.WrapOp(h.handleListAdapters),
		"ListTagsForResource":        service.WrapOp(h.handleListTagsForResource),
		"StartDocumentAnalysis":      service.WrapOp(h.handleStartDocumentAnalysis),
		"StartDocumentTextDetection": service.WrapOp(h.handleStartDocumentTextDetection),
		"StartExpenseAnalysis":       service.WrapOp(h.handleStartExpenseAnalysis),
		"StartLendingAnalysis":       service.WrapOp(h.handleStartLendingAnalysis),
		"TagResource":                service.WrapOp(h.handleTagResource),
		"UntagResource":              service.WrapOp(h.handleUntagResource),
		"UpdateAdapter":              service.WrapOp(h.handleUpdateAdapter),
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

	var code string
	var status int

	switch {
	case errors.Is(err, ErrJobNotFound):
		code, status = "InvalidJobIdException", http.StatusBadRequest
	case errors.Is(err, ErrAdapterNotFound), errors.Is(err, ErrAdapterVersionNotFound):
		code, status = "InvalidParameterException", http.StatusBadRequest
	case errors.Is(err, ErrValidation), errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		code, status = "ValidationException", http.StatusBadRequest
	default:
		code, status = "InternalServerError", http.StatusInternalServerError
	}

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{Type: code, Message: err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", textractContentType)

	return c.JSONBlob(status, payload)
}

func documentURI(bucket, key string) string {
	if bucket == "" && key == "" {
		return "inline-document"
	}

	return "s3://" + bucket + "/" + key
}

// startJobResponse is the shared response shape for every Start* async job
// operation (StartDocumentAnalysis, StartDocumentTextDetection,
// StartExpenseAnalysis, StartLendingAnalysis).
type startJobResponse struct {
	JobID string `json:"JobId"`
}

// emptyResponse is a response with no fields, shared by the Delete*/Tag*/Untag*
// operations.
type emptyResponse struct{}

// asyncInput is the shared input shape for StartDocumentAnalysis and
// StartDocumentTextDetection.
type asyncInput struct {
	NotificationChannel *NotificationChannel `json:"NotificationChannel"`
	OutputConfig        *OutputConfig        `json:"OutputConfig"`
	QueriesConfig       *QueriesConfig       `json:"QueriesConfig"`
	DocumentLocation    struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
	JobTag             string   `json:"JobTag"`
	ClientRequestToken string   `json:"ClientRequestToken"`
	FeatureTypes       []string `json:"FeatureTypes"`
}

// getJobInput is the shared input shape for GetDocumentAnalysis and
// GetDocumentTextDetection.
type getJobInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}
