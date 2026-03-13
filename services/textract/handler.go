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
		"DetectDocumentText",
		"StartDocumentAnalysis",
		"GetDocumentAnalysis",
		"StartDocumentTextDetection",
		"GetDocumentTextDetection",
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
		"DetectDocumentText":         service.WrapOp(h.handleDetectDocumentText),
		"StartDocumentAnalysis":      service.WrapOp(h.handleStartDocumentAnalysis),
		"GetDocumentAnalysis":        service.WrapOp(h.handleGetDocumentAnalysis),
		"StartDocumentTextDetection": service.WrapOp(h.handleStartDocumentTextDetection),
		"GetDocumentTextDetection":   service.WrapOp(h.handleGetDocumentTextDetection),
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
