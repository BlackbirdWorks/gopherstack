package textract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const textractTargetPrefix = "Textract."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Textract operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Textract handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

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
		"StartDocumentAnalysis",
		"StartDocumentTextDetection",
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

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Textract", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
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
		"StartDocumentAnalysis":      service.WrapOp(h.handleStartDocumentAnalysis),
		"StartDocumentTextDetection": service.WrapOp(h.handleStartDocumentTextDetection),
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
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidJobIdException",
			"message": err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServerError",
			"message": err.Error(),
		})
	}
}

// documentInput is the input for synchronous document operations.
type documentInput struct {
	Document struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
		Bytes []byte `json:"Bytes"`
	} `json:"Document"`
	FeatureTypes []string `json:"FeatureTypes"`
}

// documentResponse is the response for synchronous document operations.
type documentResponse struct {
	Blocks           []Block `json:"Blocks"`
	DocumentMetadata struct {
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
	_ context.Context,
	in *documentInput,
) (*documentResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	blocks := h.Backend.AnalyzeDocument(uri)

	resp := &documentResponse{Blocks: blocks}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

func (h *Handler) handleDetectDocumentText(
	_ context.Context,
	in *documentInput,
) (*documentResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	blocks := h.Backend.DetectDocumentText(uri)

	resp := &documentResponse{Blocks: blocks}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// asyncInput is the input for async document operations.
type asyncInput struct {
	DocumentLocation struct {
		S3Object struct {
			Bucket string `json:"Bucket"`
			Name   string `json:"Name"`
		} `json:"S3Object"`
	} `json:"DocumentLocation"`
	NotificationChannel struct {
		RoleArn     string `json:"RoleArn"`
		SNSTopicArn string `json:"SNSTopicArn"`
	} `json:"NotificationChannel"`
	FeatureTypes []string `json:"FeatureTypes"`
}

type startJobResponse struct {
	JobID string `json:"JobId"`
}

func (h *Handler) handleStartDocumentAnalysis(
	_ context.Context,
	in *asyncInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	job, err := h.Backend.StartDocumentAnalysis(uri)
	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}

type getJobInput struct {
	JobID string `json:"JobId"`
}

type getJobResponse struct {
	JobStatus        string  `json:"JobStatus"`
	Blocks           []Block `json:"Blocks"`
	DocumentMetadata struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetDocumentAnalysis(
	_ context.Context,
	in *getJobInput,
) (*getJobResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetDocumentAnalysis(in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getJobResponse{
		JobStatus: job.JobStatus,
		Blocks:    job.Blocks,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

func (h *Handler) handleStartDocumentTextDetection(
	_ context.Context,
	in *asyncInput,
) (*startJobResponse, error) {
	bucket := in.DocumentLocation.S3Object.Bucket
	key := in.DocumentLocation.S3Object.Name

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("%w: DocumentLocation.S3Object.Bucket and Name are required", errInvalidRequest)
	}

	uri := "s3://" + bucket + "/" + key

	job, err := h.Backend.StartDocumentTextDetection(uri)
	if err != nil {
		return nil, err
	}

	return &startJobResponse{JobID: job.JobID}, nil
}

func (h *Handler) handleGetDocumentTextDetection(
	_ context.Context,
	in *getJobInput,
) (*getJobResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobID is required", errInvalidRequest)
	}

	job, err := h.Backend.GetDocumentTextDetection(in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getJobResponse{
		JobStatus: job.JobStatus,
		Blocks:    job.Blocks,
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
	_ context.Context,
	in *analyzeExpenseInput,
) (*analyzeExpenseResponse, error) {
	uri := documentURI(in.Document.S3Object.Bucket, in.Document.S3Object.Name)
	docs := h.Backend.AnalyzeExpense(uri)

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
	_ context.Context,
	in *analyzeIDInput,
) (*analyzeIDResponse, error) {
	if len(in.DocumentPages) == 0 {
		return nil, fmt.Errorf("%w: DocumentPages is required", errInvalidRequest)
	}

	uris := make([]string, 0, len(in.DocumentPages))
	for _, dp := range in.DocumentPages {
		uris = append(uris, documentURI(dp.S3Object.Bucket, dp.S3Object.Name))
	}

	docs := h.Backend.AnalyzeID(uris)

	resp := &analyzeIDResponse{
		AnalyzeIDModelVersion: "1.0",
		IdentityDocuments:     docs,
	}
	resp.DocumentMetadata.Pages = len(in.DocumentPages)

	return resp, nil
}

// createAdapterInput is the input for CreateAdapter.
type createAdapterInput struct {
	Tags         map[string]string `json:"Tags"`
	AdapterName  string            `json:"AdapterName"`
	Description  string            `json:"Description"`
	FeatureTypes []string          `json:"FeatureTypes"`
}

// createAdapterResponse is the response for CreateAdapter.
type createAdapterResponse struct {
	AdapterID string `json:"AdapterId"`
}

func (h *Handler) handleCreateAdapter(
	_ context.Context,
	in *createAdapterInput,
) (*createAdapterResponse, error) {
	if in.AdapterName == "" {
		return nil, fmt.Errorf("%w: AdapterName is required", errInvalidRequest)
	}

	adapter, err := h.Backend.CreateAdapter(in.AdapterName, in.Description, in.FeatureTypes, in.Tags)
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
	_ context.Context,
	in *getAdapterInput,
) (*getAdapterResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	adapter, err := h.Backend.GetAdapter(in.AdapterID)
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

// deleteAdapterInput is the input for DeleteAdapter.
type deleteAdapterInput struct {
	AdapterID string `json:"AdapterId"`
}

// emptyResponse is a response with no fields.
type emptyResponse struct{}

func (h *Handler) handleDeleteAdapter(
	_ context.Context,
	in *deleteAdapterInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapter(in.AdapterID); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// createAdapterVersionInput is the input for CreateAdapterVersion.
type createAdapterVersionInput struct {
	Tags      map[string]string `json:"Tags"`
	AdapterID string            `json:"AdapterId"`
}

// createAdapterVersionResponse is the response for CreateAdapterVersion.
type createAdapterVersionResponse struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleCreateAdapterVersion(
	_ context.Context,
	in *createAdapterVersionInput,
) (*createAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	av, err := h.Backend.CreateAdapterVersion(in.AdapterID, in.Tags)
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
	Tags           map[string]string `json:"Tags"`
	AdapterID      string            `json:"AdapterId"`
	AdapterVersion string            `json:"AdapterVersion"`
	CreationTime   string            `json:"CreationTime"`
	Status         string            `json:"Status"`
	StatusMessage  string            `json:"StatusMessage"`
	FeatureTypes   []string          `json:"FeatureTypes"`
}

func (h *Handler) handleGetAdapterVersion(
	_ context.Context,
	in *getAdapterVersionInput,
) (*getAdapterVersionResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	av, err := h.Backend.GetAdapterVersion(in.AdapterID, in.AdapterVersion)
	if err != nil {
		return nil, err
	}

	return &getAdapterVersionResponse{
		AdapterID:      av.AdapterID,
		AdapterVersion: av.AdapterVersion,
		CreationTime:   av.CreationTime.Format("2006-01-02T15:04:05Z"),
		FeatureTypes:   av.FeatureTypes,
		Status:         av.Status,
		StatusMessage:  av.StatusMessage,
		Tags:           av.Tags,
	}, nil
}

// deleteAdapterVersionInput is the input for DeleteAdapterVersion.
type deleteAdapterVersionInput struct {
	AdapterID      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) handleDeleteAdapterVersion(
	_ context.Context,
	in *deleteAdapterVersionInput,
) (*emptyResponse, error) {
	if in.AdapterID == "" {
		return nil, fmt.Errorf("%w: AdapterId is required", errInvalidRequest)
	}

	if in.AdapterVersion == "" {
		return nil, fmt.Errorf("%w: AdapterVersion is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAdapterVersion(in.AdapterID, in.AdapterVersion); err != nil {
		return nil, err
	}

	return &emptyResponse{}, nil
}

// getExpenseAnalysisInput is the input for GetExpenseAnalysis.
type getExpenseAnalysisInput struct {
	JobID string `json:"JobId"`
}

// getExpenseAnalysisResponse is the response for GetExpenseAnalysis.
type getExpenseAnalysisResponse struct {
	AnalyzeExpenseModelVersion string            `json:"AnalyzeExpenseModelVersion"`
	JobStatus                  string            `json:"JobStatus"`
	ExpenseDocuments           []ExpenseDocument `json:"ExpenseDocuments"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetExpenseAnalysis(
	_ context.Context,
	in *getExpenseAnalysisInput,
) (*getExpenseAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetExpenseAnalysis(in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getExpenseAnalysisResponse{
		AnalyzeExpenseModelVersion: "1.0",
		ExpenseDocuments:           job.ExpenseDocuments,
		JobStatus:                  job.JobStatus,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}

// getLendingAnalysisInput is the input for GetLendingAnalysis.
type getLendingAnalysisInput struct {
	JobID string `json:"JobId"`
}

// getLendingAnalysisResponse is the response for GetLendingAnalysis.
type getLendingAnalysisResponse struct {
	AnalyzeLendingModelVersion string          `json:"AnalyzeLendingModelVersion"`
	JobStatus                  string          `json:"JobStatus"`
	Results                    []LendingResult `json:"Results"`
	DocumentMetadata           struct {
		Pages int `json:"Pages"`
	} `json:"DocumentMetadata"`
}

func (h *Handler) handleGetLendingAnalysis(
	_ context.Context,
	in *getLendingAnalysisInput,
) (*getLendingAnalysisResponse, error) {
	if in.JobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", errInvalidRequest)
	}

	job, err := h.Backend.GetLendingAnalysis(in.JobID)
	if err != nil {
		return nil, err
	}

	resp := &getLendingAnalysisResponse{
		AnalyzeLendingModelVersion: "1.0",
		JobStatus:                  job.JobStatus,
		Results:                    job.Results,
	}
	resp.DocumentMetadata.Pages = 1

	return resp, nil
}
