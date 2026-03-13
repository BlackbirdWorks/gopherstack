package serverlessrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	serverlessrepoService       = "serverlessrepo"
	serverlessrepoMatchPriority = 87
	// pathSegmentsMax is used to split the URL path into at most 2 parts.
	pathSegmentsMax = 2
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS Serverless Application Repository REST API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Serverless Application Repository handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ServerlessRepo" }

// GetSupportedOperations returns the list of supported Serverless Application Repository operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return serverlessrepoService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Serverless Application Repository API requests.
// All path-based matches are gated on the SigV4 service name to prevent routing conflicts.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != serverlessrepoService {
			return false
		}

		path := c.Request().URL.Path

		return path == "/applications" || strings.HasPrefix(path, "/applications/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return serverlessrepoMatchPriority }

// ExtractOperation extracts the operation name from the HTTP method and request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	// /applications → list or create
	if path == "/applications" || path == "/applications/" {
		switch method {
		case http.MethodGet:
			return "ListApplications"
		case http.MethodPost:
			return "CreateApplication"
		}
	}

	// /applications/{applicationId}
	if strings.HasPrefix(path, "/applications/") {
		switch method {
		case http.MethodGet:
			return "GetApplication"
		case http.MethodPatch:
			return "UpdateApplication"
		case http.MethodDelete:
			return "DeleteApplication"
		}
	}

	return ""
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for Serverless Application Repository requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "serverlessrepo: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, c.Request(), body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusNoContent)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, req *http.Request, body []byte) ([]byte, error) {
	switch op {
	case "CreateApplication":
		return h.handleCreateApplication(ctx, body)
	case "GetApplication":
		return h.handleGetApplication(req)
	case "ListApplications":
		return h.handleListApplications()
	case "UpdateApplication":
		return h.handleUpdateApplication(ctx, req, body)
	case "DeleteApplication":
		return h.handleDeleteApplication(ctx, req)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		// json.Marshal on map[string]string never returns an error; _ is intentional.
		payload, _ := json.Marshal(map[string]string{
			"__type":  "NotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ConflictException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InternalServerException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

// extractApplicationName extracts the application name from the URL path
// at /applications/{applicationId} (URL-encoded). If the segment is an ARN
// (e.g. arn:aws:serverlessrepo:us-east-1:123:applications/my-app), the name
// after the final "/" is extracted.
func extractApplicationName(req *http.Request) (string, error) {
	path := req.URL.Path
	path = strings.TrimPrefix(path, "/applications/")
	path = strings.SplitN(path, "/", pathSegmentsMax)[0]

	name, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("%w: invalid application id encoding", errInvalidRequest)
	}

	// Accept ARN-form application IDs (e.g. arn:aws:serverlessrepo:us-east-1:123456789:applications/my-app).
	// Validate the ARN has the expected /applications/<name> structure before extracting the name.
	if strings.HasPrefix(name, "arn:") {
		const arnResourcePrefix = "/applications/"

		idx := strings.Index(name, arnResourcePrefix)
		if idx < 0 {
			return "", fmt.Errorf("%w: ARN does not contain expected /applications/ resource path", errInvalidRequest)
		}

		name = strings.TrimSuffix(name[idx+len(arnResourcePrefix):], "/")
		if name == "" {
			return "", fmt.Errorf("%w: ARN has empty application name", errInvalidRequest)
		}
	}

	if name == "" {
		return "", fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	return name, nil
}

// isoTimestamp converts a [time.Time] to an RFC3339 UTC string, matching the AWS SAR API shape.
func isoTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// createApplicationRequest is the request body for CreateApplication.
type createApplicationRequest struct {
	Tags            map[string]string `json:"tags"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	Author          string            `json:"author"`
	SourceCodeURL   string            `json:"sourceCodeUrl"`
	SemanticVersion string            `json:"semanticVersion"`
}

// applicationResponse represents the API response shape for a single application.
type applicationResponse struct {
	CreationTime    string            `json:"creationTime"`
	Tags            map[string]string `json:"labels,omitempty"`
	ApplicationID   string            `json:"applicationId"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	Author          string            `json:"author"`
	SourceCodeURL   string            `json:"sourceCodeUrl,omitempty"`
	SemanticVersion string            `json:"version,omitempty"`
}

// applicationSummary is a summary used in list responses.
type applicationSummary struct {
	CreationTime  string `json:"creationTime"`
	ApplicationID string `json:"applicationId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	Author        string `json:"author"`
}

func toApplicationResponse(a *Application) applicationResponse {
	return applicationResponse{
		ApplicationID:   a.ApplicationID,
		Name:            a.Name,
		Description:     a.Description,
		Author:          a.Author,
		SourceCodeURL:   a.SourceCodeURL,
		SemanticVersion: a.SemanticVersion,
		CreationTime:    isoTimestamp(a.CreationTime),
		Tags:            a.Tags,
	}
}

func (h *Handler) handleCreateApplication(ctx context.Context, body []byte) ([]byte, error) {
	var req createApplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	a, err := h.Backend.CreateApplication(
		req.Name,
		req.Description,
		req.Author,
		req.SourceCodeURL,
		req.SemanticVersion,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created application", "name", a.Name, "id", a.ApplicationID)

	resp := toApplicationResponse(a)

	return json.Marshal(resp)
}

func (h *Handler) handleGetApplication(req *http.Request) ([]byte, error) {
	name, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.GetApplication(name)
	if err != nil {
		return nil, err
	}

	resp := toApplicationResponse(a)

	return json.Marshal(resp)
}

func (h *Handler) handleListApplications() ([]byte, error) {
	apps := h.Backend.ListApplications()
	summaries := make([]applicationSummary, 0, len(apps))

	for _, a := range apps {
		summaries = append(summaries, applicationSummary{
			ApplicationID: a.ApplicationID,
			Name:          a.Name,
			Description:   a.Description,
			Author:        a.Author,
			CreationTime:  isoTimestamp(a.CreationTime),
		})
	}

	return json.Marshal(map[string]any{"applications": summaries})
}

// updateApplicationRequest is the request body for UpdateApplication.
type updateApplicationRequest struct {
	Description string `json:"description"`
	Author      string `json:"author"`
}

func (h *Handler) handleUpdateApplication(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nil, nameErr
	}

	var updateReq updateApplicationRequest
	if err := json.Unmarshal(body, &updateReq); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	a, err := h.Backend.UpdateApplication(name, updateReq.Description, updateReq.Author)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: updated application", "name", a.Name)

	resp := toApplicationResponse(a)

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteApplication(ctx context.Context, req *http.Request) ([]byte, error) {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nil, nameErr
	}

	if err := h.Backend.DeleteApplication(name); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: deleted application", "name", name)

	return nil, nil
}
