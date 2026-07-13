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

// documentInput is the input for synchronous document operations.
type documentInput struct {
	QueriesConfig *QueriesConfig `json:"QueriesConfig"`
	Document      struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
		Bytes []byte `json:"Bytes"`
	} `json:"Document"`
	FeatureTypes []string `json:"FeatureTypes"`
}

// analyzeDocumentResponse is the response for AnalyzeDocument.
type analyzeDocumentResponse struct {
	AnalyzeDocumentModelVersion string  `json:"AnalyzeDocumentModelVersion"`
	Blocks                      []Block `json:"Blocks"`
	DocumentMetadata            struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

// detectDocumentTextResponse is the response for DetectDocumentText.
type detectDocumentTextResponse struct {
	DetectDocumentTextModelVersion string  `json:"DetectDocumentTextModelVersion"`
	Blocks                         []Block `json:"Blocks"`
	DocumentMetadata               struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func documentURI(bucket, key string) string {
	if bucket == "" && key == "" {
		return "inline-document"
	}

	return "s3://" + bucket + "/" + key
}

func (h *Handler) handleAnalyzeDocument(
	ctx context.Context,
	in *documentInput,
) (*analyzeDocumentResponse, error) {
	if err := validateAnalyzeDocumentFeatureTypes(in.FeatureTypes); err != nil {
		return nil, err
	}

	if err := validateQueriesConfig(in.FeatureTypes, in.QueriesConfig); err != nil {
		return nil, err
	}

	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)

	var blocks []Block

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		blocks = b.AnalyzeDocumentWithFeatures(ctx, uri, in.FeatureTypes, in.QueriesConfig)
	} else {
		blocks = h.Backend.AnalyzeDocument(ctx, uri)
	}

	resp := &analyzeDocumentResponse{
		Blocks:                      blocks,
		AnalyzeDocumentModelVersion: modelVersion10,
	}
	// Pages=1 for now; should reflect actual page count for multi-page PDFs.
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

func (h *Handler) handleDetectDocumentText(
	ctx context.Context,
	in *documentInput,
) (*detectDocumentTextResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	blocks := h.Backend.DetectDocumentText(ctx, uri)

	resp := &detectDocumentTextResponse{
		Blocks:                         blocks,
		DetectDocumentTextModelVersion: modelVersion10,
	}
	// Pages=1 for now; should reflect actual page count for multi-page PDFs.
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// asyncInput is the input for async document operations.
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

type startJobResponse struct {
	JobID string `json:"JobId"`
}

func (h *Handler) handleStartDocumentAnalysis(
	ctx context.Context,
	in *asyncInput,
) (*startJobResponse, error) {
	if err := validateAnalyzeDocumentFeatureTypes(in.FeatureTypes); err != nil {
		return nil, err
	}

	if err := validateQueriesConfig(in.FeatureTypes, in.QueriesConfig); err != nil {
		return nil, err
	}

	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *DocumentJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartDocumentAnalysisWithOptions(
			ctx,
			uri,
			in.FeatureTypes,
			in.QueriesConfig,
			in.OutputConfig,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartDocumentAnalysis(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}

// getJobInput is the input for Get* async job operations.
type getJobInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// getDocumentAnalysisResponse is the response for GetDocumentAnalysis.
type getDocumentAnalysisResponse struct {
	JobStatus                   string         `json:"JobStatus"`
	NextToken                   string         `json:"NextToken,omitempty"`
	StatusMessage               string         `json:"StatusMessage,omitempty"`
	AnalyzeDocumentModelVersion string         `json:"AnalyzeDocumentModelVersion"`
	Blocks                      []Block        `json:"Blocks"`
	Warnings                    []WarningBlock `json:"Warnings,omitempty"`
	DocumentMetadata            struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetDocumentAnalysis(
	ctx context.Context,
	in *getJobInput,
) (*getDocumentAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetDocumentAnalysis(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	blocks, nextToken := PaginateBlocks(job.Blocks, in.MaxResults, in.NextToken)

	resp := &getDocumentAnalysisResponse{
		JobStatus:                   job.JobStatus,
		Blocks:                      blocks,
		NextToken:                   nextToken,
		StatusMessage:               job.StatusMessage,
		Warnings:                    job.Warnings,
		AnalyzeDocumentModelVersion: modelVersion10,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

func (h *Handler) handleStartDocumentTextDetection(
	ctx context.Context,
	in *asyncInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *DocumentJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartDocumentTextDetectionWithOptions(
			ctx,
			uri,
			in.OutputConfig,
			in.NotificationChannel,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartDocumentTextDetection(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}

// getDocumentTextDetectionResponse is the response for GetDocumentTextDetection.
type getDocumentTextDetectionResponse struct {
	JobStatus                      string         `json:"JobStatus"`
	NextToken                      string         `json:"NextToken,omitempty"`
	StatusMessage                  string         `json:"StatusMessage,omitempty"`
	DetectDocumentTextModelVersion string         `json:"DetectDocumentTextModelVersion"`
	Blocks                         []Block        `json:"Blocks"`
	Warnings                       []WarningBlock `json:"Warnings,omitempty"`
	DocumentMetadata               struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetDocumentTextDetection(
	ctx context.Context,
	in *getJobInput,
) (*getDocumentTextDetectionResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetDocumentTextDetection(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	blocks, nextToken := PaginateBlocks(job.Blocks, in.MaxResults, in.NextToken)

	resp := &getDocumentTextDetectionResponse{
		JobStatus:                      job.JobStatus,
		Blocks:                         blocks,
		NextToken:                      nextToken,
		StatusMessage:                  job.StatusMessage,
		Warnings:                       job.Warnings,
		DetectDocumentTextModelVersion: modelVersion10,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// analyzeExpenseInput is the input for AnalyzeExpense.
type analyzeExpenseInput struct {
	Document struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
		Bytes []byte `json:"Bytes"`
	} `json:"Document"`
}

// analyzeExpenseResponse is the response for AnalyzeExpense.
type analyzeExpenseResponse struct {
	ExpenseDocuments []ExpenseDocument `json:"ExpenseDocuments"`
	DocumentMetadata struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleAnalyzeExpense(
	ctx context.Context,
	in *analyzeExpenseInput,
) (*analyzeExpenseResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	docs := h.Backend.AnalyzeExpense(ctx, uri)

	resp := &analyzeExpenseResponse{ExpenseDocuments: docs}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// analyzeIDInput is the input for AnalyzeID.
type analyzeIDInput struct {
	DocumentPages []struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
		Bytes []byte `json:"Bytes"`
	} `json:"DocumentPages"`
}

// analyzeIDResponse is the response for AnalyzeID.
type analyzeIDResponse struct {
	AnalyzeIDModelVersion string             `json:"AnalyzeIDModelVersion"`
	IdentityDocuments     []IdentityDocument `json:"IdentityDocuments"`
	DocumentMetadata      struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleAnalyzeID(
	ctx context.Context,
	in *analyzeIDInput,
) (*analyzeIDResponse, error) {
	if len(in.DocumentPages) == 0 {
		return nil, fmt.Errorf("%w: DocumentPages is required", errInvalidRequest)
	}

	uris := make([]string, 0, len(in.DocumentPages))
	for _, dp := range in.DocumentPages {
		uris = append(uris, documentURI(dp.S3Object.Bucket, dp.S3Object.Name))
	}

	docs := h.Backend.AnalyzeID(ctx, uris)

	resp := &analyzeIDResponse{
		AnalyzeIDModelVersion: modelVersion10,
		IdentityDocuments:     docs,
	}
	resp.DocumentMetadata.Pages = len(in.DocumentPages)

	return resp, nil
}

// createAdapterInput is the input for CreateAdapter.
type createAdapterInput struct {
	Tags               map[string]string `json:"Tags"`
	AdapterName        string            `json:"AdapterName"`
	AutoUpdate         string            `json:"AutoUpdate"`
	Description        string            `json:"Description"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	FeatureTypes       []string          `json:"FeatureTypes"`
}

// createAdapterResponse is the response for CreateAdapter.
type createAdapterResponse struct {
	AdapterID string `json:"AdapterId"`
}

func (h *Handler) handleCreateAdapter(
	ctx context.Context,
	in *createAdapterInput,
) (*createAdapterResponse, error) {
	if in.AdapterName == "" {
		return nil, fmt.Errorf("%w: AdapterName is required", errInvalidRequest)
	}

	var adapter *Adapter
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		adapter, err = b.CreateAdapterWithToken(
			ctx,
			in.AdapterName, in.Description, in.AutoUpdate,
			in.FeatureTypes, in.Tags, in.ClientRequestToken,
		)
	} else {
		adapter, err = h.Backend.CreateAdapter(
			ctx, in.AdapterName, in.Description, in.AutoUpdate, in.FeatureTypes, in.Tags,
		)
	}

	if err != nil {
		return nil, err
	}

	return &createAdapterResponse{AdapterID: adapter.AdapterID}, nil
}

// getAdapterInput is the input for GetAdapter.
type getAdapterInput struct {
	AdapterID string `json:"AdapterId"`
}

// getAdapterResponse is the response for GetAdapter.
type getAdapterResponse struct {
	Tags         map[string]string `json:"Tags"`
	AdapterID    string            `json:"AdapterId"`
	AdapterName  string            `json:"AdapterName"`
	AutoUpdate   string            `json:"AutoUpdate"`
	CreationTime string            `json:"CreationTime"`
	Description  string            `json:"Description"`
	FeatureTypes []string          `json:"FeatureTypes"`
}

func (h *Handler) handleGetAdapter(
	ctx context.Context,
	in *getAdapterInput,
) (*getAdapterResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	adapter, err := h.Backend.GetAdapter(ctx, in.AdapterID)
	if err != nil {
		return nil, err
	}

	return &getAdapterResponse{
		AdapterID:    adapter.AdapterID,
		AdapterName:  adapter.AdapterName,
		AutoUpdate:   adapter.AutoUpdate,
		CreationTime: adapter.CreationTime.Format("2006-01-02T15:04:05Z"),
		Description:  adapter.Description,
		FeatureTypes: adapter.FeatureTypes,
		Tags:         adapter.Tags,
	}, nil
}

// updateAdapterInput is the input for UpdateAdapter.
type updateAdapterInput struct {
	AdapterID   string `json:"AdapterId"`
	AutoUpdate  string `json:"AutoUpdate"`
	Description string `json:"Description"`
}

// updateAdapterResponse is the response for UpdateAdapter.
type updateAdapterResponse struct {
	Tags         map[string]string `json:"Tags"`
	AdapterID    string            `json:"AdapterId"`
	AdapterName  string            `json:"AdapterName"`
	AutoUpdate   string            `json:"AutoUpdate"`
	CreationTime string            `json:"CreationTime"`
	Description  string            `json:"Description"`
	FeatureTypes []string          `json:"FeatureTypes"`
}

func (h *Handler) handleUpdateAdapter(
	ctx context.Context,
	in *updateAdapterInput,
) (*updateAdapterResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	adapter, err := h.Backend.UpdateAdapter(ctx, in.AdapterID, in.Description, in.AutoUpdate)
	if err != nil {
		return nil, err
	}

	return &updateAdapterResponse{
		AdapterID:    adapter.AdapterID,
		AdapterName:  adapter.AdapterName,
		AutoUpdate:   adapter.AutoUpdate,
		CreationTime: adapter.CreationTime.Format("2006-01-02T15:04:05Z"),
		Description:  adapter.Description,
		FeatureTypes: adapter.FeatureTypes,
		Tags:         adapter.Tags,
	}, nil
}

// listAdaptersInput is the input for ListAdapters.
type listAdaptersInput struct{}

// listAdaptersResponse is the response for ListAdapters.
type listAdaptersResponse struct {
	Adapters []adapterSummary `json:"Adapters"`
}

type adapterSummary struct {
	AdapterID    string   `json:"AdapterId"`
	AdapterName  string   `json:"AdapterName"`
	CreationTime string   `json:"CreationTime"`
	FeatureTypes []string `json:"FeatureTypes"`
}

func (h *Handler) handleListAdapters(
	ctx context.Context,
	_ *listAdaptersInput,
) (*listAdaptersResponse, error) {
	adapters := h.Backend.ListAdapters(ctx)
	summaries := make([]adapterSummary, 0, len(adapters))

	for _, a := range adapters {
		summaries = append(summaries, adapterSummary{
			AdapterID:    a.AdapterID,
			AdapterName:  a.AdapterName,
			CreationTime: a.CreationTime.Format("2006-01-02T15:04:05Z"),
			FeatureTypes: a.FeatureTypes,
		})
	}

	return &listAdaptersResponse{Adapters: summaries}, nil
}

// deleteAdapterInput is the input for DeleteAdapter.
type deleteAdapterInput struct {
	AdapterID string `json:"AdapterId"`
}

// emptyResponse is a response with no fields.
type emptyResponse struct{}

func (h *Handler) handleDeleteAdapter(
	ctx context.Context,
	in *deleteAdapterInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapter(ctx, in.AdapterID); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// createAdapterVersionInput is the input for CreateAdapterVersion.
type createAdapterVersionInput struct {
	Tags               map[string]string `json:"Tags"`
	DatasetConfig      *DatasetConfig    `json:"DatasetConfig"`
	OutputConfig       *OutputConfig     `json:"OutputConfig"`
	AdapterID          string            `json:"AdapterId"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId string `json:"KMSKeyId"`
}

// createAdapterVersionResponse is the response for CreateAdapterVersion.
type createAdapterVersionResponse struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleCreateAdapterVersion(
	ctx context.Context,
	in *createAdapterVersionInput,
) (*createAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	var av *AdapterVersion
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		av, err = b.CreateAdapterVersionWithOptions(
			ctx,
			in.AdapterID, in.Tags,
			in.DatasetConfig, in.OutputConfig,
			in.KMSKeyId, in.ClientRequestToken,
		)
	} else {
		av, err = h.Backend.CreateAdapterVersion(ctx, in.AdapterID, in.Tags)
	}

	if err != nil {
		return nil, err
	}

	return &createAdapterVersionResponse{
		AdapterID:      av.AdapterID,
		AdapterVersion: av.AdapterVersion,
	}, nil
}

// getAdapterVersionInput is the input for GetAdapterVersion.
type getAdapterVersionInput struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

// getAdapterVersionResponse is the response for GetAdapterVersion.
type getAdapterVersionResponse struct {
	Tags              map[string]string  `json:"Tags"`
	DatasetConfig     *DatasetConfig     `json:"DatasetConfig,omitempty"`
	OutputConfig      *OutputConfig      `json:"OutputConfig,omitempty"`
	EvaluationMetrics *EvaluationMetrics `json:"EvaluationMetrics,omitempty"`
	AdapterID         string             `json:"AdapterId"`
	AdapterVersion    string             `json:"AdapterVersion"`
	CreationTime      string             `json:"CreationTime"`
	Status            string             `json:"Status"`
	StatusMessage     string             `json:"StatusMessage"`
	//nolint:revive,staticcheck // KMSKeyId: AWS SDK field name convention
	KMSKeyId     string   `json:"KMSKeyId,omitempty"`
	FeatureTypes []string `json:"FeatureTypes"`
}

func (h *Handler) handleGetAdapterVersion(
	ctx context.Context,
	in *getAdapterVersionInput,
) (*getAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	av, err := h.Backend.GetAdapterVersion(ctx, in.AdapterID, in.AdapterVersion)
	if err != nil {
		return nil, err
	}

	return &getAdapterVersionResponse{
		AdapterID:         av.AdapterID,
		AdapterVersion:    av.AdapterVersion,
		CreationTime:      av.CreationTime.Format("2006-01-02T15:04:05Z"),
		FeatureTypes:      av.FeatureTypes,
		Status:            av.Status,
		StatusMessage:     av.StatusMessage,
		Tags:              av.Tags,
		DatasetConfig:     av.DatasetConfig,
		OutputConfig:      av.OutputConfig,
		KMSKeyId:          av.KMSKeyId,
		EvaluationMetrics: av.EvaluationMetrics,
	}, nil
}

// listAdapterVersionsInput is the input for ListAdapterVersions.
type listAdapterVersionsInput struct {
	AdapterID string `json:"AdapterId"`
}

// listAdapterVersionsResponse is the response for ListAdapterVersions.
type listAdapterVersionsResponse struct {
	AdapterID       string                  `json:"AdapterId"`
	AdapterVersions []adapterVersionSummary `json:"AdapterVersions"`
}

type adapterVersionSummary struct {
	AdapterVersion string   `json:"AdapterVersion"`
	CreationTime   string   `json:"CreationTime"`
	Status         string   `json:"Status"`
	StatusMessage  string   `json:"StatusMessage,omitempty"`
	FeatureTypes   []string `json:"FeatureTypes"`
}

func (h *Handler) handleListAdapterVersions(
	ctx context.Context,
	in *listAdapterVersionsInput,
) (*listAdapterVersionsResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	versions, err := h.Backend.ListAdapterVersions(ctx, in.AdapterID)
	if err != nil {
		return nil, err
	}

	summaries := make([]adapterVersionSummary, 0, len(versions))
	for _, av := range versions {
		summaries = append(summaries, adapterVersionSummary{
			AdapterVersion: av.AdapterVersion,
			CreationTime:   av.CreationTime.Format("2006-01-02T15:04:05Z"),
			FeatureTypes:   av.FeatureTypes,
			Status:         av.Status,
			StatusMessage:  av.StatusMessage,
		})
	}

	return &listAdapterVersionsResponse{
		AdapterID:       in.AdapterID,
		AdapterVersions: summaries,
	}, nil
}

// deleteAdapterVersionInput is the input for DeleteAdapterVersion.
type deleteAdapterVersionInput struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleDeleteAdapterVersion(
	ctx context.Context,
	in *deleteAdapterVersionInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapterVersion(ctx, in.AdapterID, in.AdapterVersion); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// tagResourceInput is the input for TagResource.
type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*emptyResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(ctx, in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// untagResourceInput is the input for UntagResource.
type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*emptyResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// listTagsForResourceInput is the input for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

// listTagsForResourceResponse is the response for ListTagsForResource.
type listTagsForResourceResponse struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceResponse, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceResponse{Tags: tags}, nil
}

// getExpenseAnalysisInput is the input for GetExpenseAnalysis.
type getExpenseAnalysisInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// getExpenseAnalysisResponse is the response for GetExpenseAnalysis.
type getExpenseAnalysisResponse struct {
	AnalyzeExpenseModelVersion string            `json:"AnalyzeExpenseModelVersion"`
	JobStatus                  string            `json:"JobStatus"`
	StatusMessage              string            `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock    `json:"Warnings,omitempty"`
	ExpenseDocuments           []ExpenseDocument `json:"ExpenseDocuments"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetExpenseAnalysis(
	ctx context.Context,
	in *getExpenseAnalysisInput,
) (*getExpenseAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetExpenseAnalysis(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getExpenseAnalysisResponse{
		AnalyzeExpenseModelVersion: modelVersion10,
		ExpenseDocuments:           job.ExpenseDocuments,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// startExpenseAnalysisInput is the input for StartExpenseAnalysis.
type startExpenseAnalysisInput struct {
	DocumentLocation struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
	NotificationChannel *NotificationChannel `json:"NotificationChannel"`
	OutputConfig        *OutputConfig        `json:"OutputConfig"`
	JobTag              string               `json:"JobTag"`
	ClientRequestToken  string               `json:"ClientRequestToken"`
}

func (h *Handler) handleStartExpenseAnalysis(
	ctx context.Context,
	in *startExpenseAnalysisInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *ExpenseJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartExpenseAnalysisWithOptions(
			ctx,
			uri,
			in.OutputConfig,
			in.NotificationChannel,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartExpenseAnalysis(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}

// getLendingAnalysisInput is the input for GetLendingAnalysis.
type getLendingAnalysisInput struct {
	JobID      string `json:"JobId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// getLendingAnalysisResponse is the response for GetLendingAnalysis.
type getLendingAnalysisResponse struct {
	AnalyzeLendingModelVersion string          `json:"AnalyzeLendingModelVersion"`
	JobStatus                  string          `json:"JobStatus"`
	StatusMessage              string          `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock  `json:"Warnings,omitempty"`
	Results                    []LendingResult `json:"Results"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetLendingAnalysis(
	ctx context.Context,
	in *getLendingAnalysisInput,
) (*getLendingAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetLendingAnalysis(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getLendingAnalysisResponse{
		AnalyzeLendingModelVersion: modelVersion10,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
		Results:                    job.Results,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// getLendingAnalysisSummaryInput is the input for GetLendingAnalysisSummary.
type getLendingAnalysisSummaryInput struct {
	JobID string `json:"JobId"`
}

// getLendingAnalysisSummaryResponse is the response for GetLendingAnalysisSummary.
type getLendingAnalysisSummaryResponse struct {
	Summary                    *LendingSummary `json:"Summary,omitempty"`
	AnalyzeLendingModelVersion string          `json:"AnalyzeLendingModelVersion"`
	JobStatus                  string          `json:"JobStatus"`
	StatusMessage              string          `json:"StatusMessage,omitempty"`
	Warnings                   []WarningBlock  `json:"Warnings,omitempty"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetLendingAnalysisSummary(
	ctx context.Context,
	in *getLendingAnalysisSummaryInput,
) (*getLendingAnalysisSummaryResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetLendingAnalysisSummary(ctx, in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getLendingAnalysisSummaryResponse{
		AnalyzeLendingModelVersion: modelVersion10,
		JobStatus:                  job.JobStatus,
		StatusMessage:              job.StatusMessage,
		Warnings:                   job.Warnings,
		Summary:                    job.Summary,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// startLendingAnalysisInput is the input for StartLendingAnalysis.
type startLendingAnalysisInput struct {
	DocumentLocation struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
	NotificationChannel *NotificationChannel `json:"NotificationChannel"`
	OutputConfig        *OutputConfig        `json:"OutputConfig"`
	JobTag              string               `json:"JobTag"`
	ClientRequestToken  string               `json:"ClientRequestToken"`
}

func (h *Handler) handleStartLendingAnalysis(
	ctx context.Context,
	in *startLendingAnalysisInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	var job *LendingJob
	var err error

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		job, err = b.StartLendingAnalysisWithOptions(
			ctx,
			uri,
			in.OutputConfig,
			in.NotificationChannel,
			in.JobTag,
			in.ClientRequestToken,
		)
	} else {
		job, err = h.Backend.StartLendingAnalysis(ctx, uri)
	}

	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}
