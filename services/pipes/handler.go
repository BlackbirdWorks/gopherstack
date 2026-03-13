package pipes

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

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	pipesService       = "pipes"
	pipesMatchPriority = 87
	pipesPathPrefix    = "/v1/pipes"
	pipesTagsPrefix    = "/tags/"
	pipeNameSegment    = "pipes"
	// pipePathMinSegments is the minimum path segments for /v1/pipes/{name}: ["v1", "pipes", "{name}"].
	pipePathMinSegments = 3
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
	errMissingARN     = errors.New("missing resource ARN in path")
)

// Handler is the HTTP handler for the EventBridge Pipes REST API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Pipes handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Pipes" }

// GetSupportedOperations returns the list of supported Pipes operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreatePipe",
		"DescribePipe",
		"ListPipes",
		"DeletePipe",
		"UpdatePipe",
		"StartPipe",
		"StopPipe",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return pipesService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Pipes API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != pipesService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, pipesPathPrefix) || strings.HasPrefix(path, pipesTagsPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return pipesMatchPriority }

// ExtractOperation extracts the operation name from the HTTP method and path.
//
//nolint:cyclop // path routing table has necessary branches for each HTTP method + resource combination
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if strings.HasPrefix(path, pipesTagsPrefix) {
		switch method {
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	switch {
	case method == http.MethodGet && len(segments) >= 2 && segments[1] == pipeNameSegment &&
		(len(segments) == 2 || (len(segments) == 3 && segments[2] == "")):
		return "ListPipes"
	case len(segments) == pipePathMinSegments && segments[0] == "v1" && segments[1] == pipeNameSegment:
		switch method {
		case http.MethodPost:
			return "CreatePipe"
		case http.MethodGet:
			return "DescribePipe"
		case http.MethodDelete:
			return "DeletePipe"
		case http.MethodPut:
			return "UpdatePipe"
		}
	case len(segments) == 4 && segments[0] == "v1" && segments[1] == pipeNameSegment:
		switch segments[3] {
		case "start":
			return "StartPipe"
		case "stop":
			return "StopPipe"
		}
	}

	return "Unknown"
}

// ExtractResource extracts the pipe name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= pipePathMinSegments && segments[0] == "v1" && segments[1] == pipeNameSegment {
		return segments[2]
	}

	return ""
}

// Handler returns the Echo handler function for Pipes requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		method := c.Request().Method
		path := c.Request().URL.Path

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "pipes: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, method, path, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op, method, path string, body []byte) ([]byte, error) {
	_ = method

	switch op {
	case "CreatePipe":
		return h.handleCreatePipe(ctx, path, body)
	case "DescribePipe":
		return h.handleDescribePipe(ctx, path)
	case "ListPipes":
		return h.handleListPipes(ctx)
	case "DeletePipe":
		return h.handleDeletePipe(ctx, path)
	case "UpdatePipe":
		return h.handleUpdatePipe(ctx, path, body)
	case "StartPipe":
		return h.handleStartPipe(ctx, path)
	case "StopPipe":
		return h.handleStopPipe(ctx, path)
	case "TagResource":
		return h.handleTagResource(ctx, path, body)
	case "UntagResource":
		return h.handleUntagResource(ctx, path)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, path)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "NotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ConflictException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// extractPipeName extracts the pipe name from a /v1/pipes/{name}[/...] path.
func extractPipeName(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) >= pipePathMinSegments {
		return segments[2]
	}

	return ""
}

type createPipeRequest struct {
	Tags         map[string]string `json:"Tags"`
	RoleArn      string            `json:"RoleArn"`
	Source       string            `json:"Source"`
	Target       string            `json:"Target"`
	Description  string            `json:"Description"`
	DesiredState string            `json:"DesiredState"`
}

type pipeResponse struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Arn              string            `json:"Arn"`
	Name             string            `json:"Name"`
	RoleArn          string            `json:"RoleArn"`
	Source           string            `json:"Source"`
	Target           string            `json:"Target"`
	Description      string            `json:"Description,omitempty"`
	DesiredState     string            `json:"DesiredState"`
	CurrentState     string            `json:"CurrentState"`
}

func toPipeResponse(p *Pipe) pipeResponse {
	return pipeResponse{
		Arn:              p.ARN,
		Name:             p.Name,
		RoleArn:          p.RoleARN,
		Source:           p.Source,
		Target:           p.Target,
		Description:      p.Description,
		DesiredState:     p.DesiredState,
		CurrentState:     p.CurrentState,
		CreationTime:     p.CreationTime,
		LastModifiedTime: p.LastModifiedTime,
		Tags:             p.Tags,
	}
}

func (h *Handler) handleCreatePipe(_ context.Context, path string, body []byte) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	var req createPipeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	p, err := h.Backend.CreatePipe(
		name,
		req.RoleArn,
		req.Source,
		req.Target,
		req.Description,
		req.DesiredState,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	resp := toPipeResponse(p)

	return json.Marshal(resp)
}

func (h *Handler) handleDescribePipe(_ context.Context, path string) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	p, err := h.Backend.GetPipe(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
}

type pipeSummary struct {
	Arn          string `json:"Arn"`
	Name         string `json:"Name"`
	CurrentState string `json:"CurrentState"`
	DesiredState string `json:"DesiredState"`
}

type listPipesResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Pipes     []pipeSummary `json:"Pipes"`
}

func (h *Handler) handleListPipes(_ context.Context) ([]byte, error) {
	pipes := h.Backend.ListPipes()
	items := make([]pipeSummary, 0, len(pipes))

	for _, p := range pipes {
		items = append(items, pipeSummary{
			Arn:          p.ARN,
			Name:         p.Name,
			CurrentState: p.CurrentState,
			DesiredState: p.DesiredState,
		})
	}

	return json.Marshal(listPipesResponse{Pipes: items})
}

func (h *Handler) handleDeletePipe(_ context.Context, path string) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	if err := h.Backend.DeletePipe(name); err != nil {
		return nil, err
	}

	return nil, nil
}

type updatePipeRequest struct {
	RoleArn     string `json:"RoleArn"`
	Target      string `json:"Target"`
	Description string `json:"Description"`
}

func (h *Handler) handleUpdatePipe(_ context.Context, path string, body []byte) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	var req updatePipeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	p, err := h.Backend.UpdatePipe(name, req.RoleArn, req.Target, req.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
}

func (h *Handler) handleStartPipe(_ context.Context, path string) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	p, err := h.Backend.StartPipe(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
}

func (h *Handler) handleStopPipe(_ context.Context, path string) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	p, err := h.Backend.StopPipe(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
}

type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, path string, body []byte) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	var req tagResourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, unmarshalErr)
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return nil, tagErr
	}

	return nil, nil
}

func (h *Handler) handleUntagResource(_ context.Context, path string) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// TODO: parse tagKeys query parameter when needed; returning nil for now.
	if untagErr := h.Backend.UntagResource(resourceARN, nil); untagErr != nil {
		return nil, untagErr
	}

	return nil, nil
}

type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, path string) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listTagsResponse{Tags: tags})
}

// extractTagsARN extracts and URL-decodes the ARN from a /tags/{arn} path.
func extractTagsARN(path string) (string, error) {
	rawARN := strings.TrimPrefix(path, pipesTagsPrefix)
	if rawARN == "" {
		return "", errMissingARN
	}

	decoded, err := url.PathUnescape(rawARN)
	if err != nil {
		return "", fmt.Errorf("invalid ARN encoding: %w", err)
	}

	return decoded, nil
}
