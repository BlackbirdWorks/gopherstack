package pipes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
	keyTypeField    = "__type"
)

const (
	opCreatePipe          = "CreatePipe"
	opDescribePipe        = "DescribePipe"
	opListPipes           = "ListPipes"
	opDeletePipe          = "DeletePipe"
	opUpdatePipe          = "UpdatePipe"
	opStartPipe           = "StartPipe"
	opStopPipe            = "StopPipe"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
)

const (
	pipesService        = "pipes"
	pipesMatchPriority  = 87
	pipesPathPrefix     = "/v1/pipes"
	pipesTagsPrefix     = "/tags/"
	pipeNameSegment     = "pipes"
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
	runner    *Runner
	cancel    context.CancelFunc
	AccountID string
	Region    string
}

// NewHandler creates a new Pipes handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
		runner:    NewRunner(backend),
	}
}

// GetRunner returns the handler's Runner so callers can configure target invokers.
func (h *Handler) GetRunner() *Runner {
	return h.runner
}

// Name returns the service name.
func (h *Handler) Name() string { return "Pipes" }

// StartWorker implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.runner == nil {
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.runner.Start(runCtx)

	return nil
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.cancel != nil {
		h.cancel()
	}

	h.runner.Wait(ctx)
}

var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// GetSupportedOperations returns the list of supported Pipes operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreatePipe,
		opDescribePipe,
		opListPipes,
		opDeletePipe,
		opUpdatePipe,
		opStartPipe,
		opStopPipe,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

func (h *Handler) ChaosServiceName() string { return pipesService }

func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != pipesService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, pipesPathPrefix) || strings.HasPrefix(path, pipesTagsPrefix)
	}
}

func (h *Handler) MatchPriority() int { return pipesMatchPriority }

// ExtractOperation determines the operation name from the HTTP request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if op := extractTagsOp(path, method); op != "" {
		return op
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if op := extractPipeListOp(segments, method); op != "" {
		return op
	}
	if op := extractPipeCRUDOp(segments, method); op != "" {
		return op
	}
	if op := extractPipeActionOp(segments, method); op != "" {
		return op
	}

	return "Unknown"
}

// extractTagsOp returns the tag operation for tags-prefix paths.
func extractTagsOp(path, method string) string {
	if !strings.HasPrefix(path, pipesTagsPrefix) {
		return ""
	}
	switch method {
	case http.MethodGet:

		return opListTagsForResource
	case http.MethodPost:

		return opTagResource
	case http.MethodDelete:

		return opUntagResource
	}

	return ""
}

// extractPipeListOp returns the list-pipes operation when applicable.
func extractPipeListOp(segments []string, method string) string {
	if method == http.MethodGet && len(segments) >= 2 && segments[1] == pipeNameSegment &&
		(len(segments) == 2 || (len(segments) == 3 && segments[2] == "")) {
		return opListPipes
	}

	return ""
}

// extractPipeCRUDOp returns CRUD operations for /v1/pipes/{name} paths.
func extractPipeCRUDOp(segments []string, method string) string {
	if len(segments) != pipePathMinSegments || segments[0] != "v1" || segments[1] != pipeNameSegment {
		return ""
	}
	switch method {
	case http.MethodPost:

		return opCreatePipe
	case http.MethodGet:

		return opDescribePipe
	case http.MethodDelete:

		return opDeletePipe
	case http.MethodPut:

		return opUpdatePipe
	}

	return ""
}

// extractPipeActionOp returns start/stop operations for /v1/pipes/{name}/{action} paths.
func extractPipeActionOp(segments []string, method string) string {
	if len(segments) != 4 || segments[0] != "v1" || segments[1] != pipeNameSegment || method != http.MethodPost {
		return ""
	}
	switch segments[3] {
	case "start":

		return opStartPipe
	case "stop":

		return opStopPipe
	}

	return ""
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= pipePathMinSegments && segments[0] == "v1" &&
		segments[1] == pipeNameSegment {
		return segments[2]
	}

	return ""
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		query := c.Request().URL.Query()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "pipes: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, path, query, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(
	ctx context.Context,
	op, path string,
	query url.Values,
	body []byte,
) ([]byte, error) {
	switch op {
	case opCreatePipe:

		return h.handleCreatePipe(ctx, path, body)
	case opDescribePipe:

		return h.handleDescribePipe(ctx, path)
	case opListPipes:

		return h.handleListPipes(ctx, query)
	case opDeletePipe:

		return h.handleDeletePipe(ctx, path)
	case opUpdatePipe:

		return h.handleUpdatePipe(ctx, path, body)
	case opStartPipe:

		return h.handleStartPipe(ctx, path)
	case opStopPipe:

		return h.handleStopPipe(ctx, path)
	case opTagResource:

		return h.handleTagResource(ctx, path, body)
	case opUntagResource:

		return h.handleUntagResource(ctx, path, query)
	case opListTagsForResource:

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
			keyTypeField:    "NotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ValidationException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):

		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}
}

func extractPipeName(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) >= pipePathMinSegments {
		return segments[2]
	}

	return ""
}

// epochMillis returns t as a Unix epoch value with millisecond precision,
// matching the AWS Pipes API timestamp format (fractional seconds, 3 decimal places).
func epochMillis(t time.Time) float64 {
	const msPerSec = 1000.0

	return float64(t.UnixMilli()) / msPerSec
}

type createPipeRequest struct {
	Tags                    map[string]string        `json:"Tags"`
	SourceParameters        *SourceParameters        `json:"SourceParameters"`
	TargetParameters        *TargetParameters        `json:"TargetParameters"`
	DeadLetterConfig        *DeadLetterConfig        `json:"DeadLetterConfig"`
	LogConfiguration        *LogConfiguration        `json:"LogConfiguration"`
	EnrichmentParameters    *EnrichmentParameters    `json:"EnrichmentParameters"`
	RuntimeMetricsStreaming *RuntimeMetricsStreaming `json:"RuntimeMetricsStreaming"`
	RoleArn                 string                   `json:"RoleArn"`
	Source                  string                   `json:"Source"`
	Target                  string                   `json:"Target"`
	Description             string                   `json:"Description"`
	Enrichment              string                   `json:"Enrichment"`
	KmsKeyIdentifier        string                   `json:"KmsKeyIdentifier"`
	DesiredState            string                   `json:"DesiredState"`
}

type updatePipeRequest struct {
	SourceParameters        *SourceParameters        `json:"SourceParameters"`
	TargetParameters        *TargetParameters        `json:"TargetParameters"`
	DeadLetterConfig        *DeadLetterConfig        `json:"DeadLetterConfig"`
	LogConfiguration        *LogConfiguration        `json:"LogConfiguration"`
	EnrichmentParameters    *EnrichmentParameters    `json:"EnrichmentParameters"`
	RuntimeMetricsStreaming *RuntimeMetricsStreaming `json:"RuntimeMetricsStreaming"`
	Description             *string                  `json:"Description"`
	RoleArn                 string                   `json:"RoleArn"`
	Target                  string                   `json:"Target"`
	Enrichment              string                   `json:"Enrichment"`
	KmsKeyIdentifier        string                   `json:"KmsKeyIdentifier"`
	DesiredState            string                   `json:"DesiredState"`
}

type pipeResponse struct {
	SourceParameters        *SourceParameters        `json:"SourceParameters,omitempty"`
	TargetParameters        *TargetParameters        `json:"TargetParameters,omitempty"`
	DeadLetterConfig        *DeadLetterConfig        `json:"DeadLetterConfig,omitempty"`
	LogConfiguration        *LogConfiguration        `json:"LogConfiguration,omitempty"`
	EnrichmentParameters    *EnrichmentParameters    `json:"EnrichmentParameters,omitempty"`
	RuntimeMetricsStreaming *RuntimeMetricsStreaming `json:"RuntimeMetricsStreaming,omitempty"`
	Tags                    map[string]string        `json:"Tags,omitempty"`
	Arn                     string                   `json:"Arn"`
	Name                    string                   `json:"Name"`
	RoleArn                 string                   `json:"RoleArn"`
	Source                  string                   `json:"Source"`
	Target                  string                   `json:"Target"`
	Description             string                   `json:"Description,omitempty"`
	Enrichment              string                   `json:"Enrichment,omitempty"`
	KmsKeyIdentifier        string                   `json:"KmsKeyIdentifier,omitempty"`
	DesiredState            string                   `json:"DesiredState"`
	CurrentState            string                   `json:"CurrentState"`
	StateReason             string                   `json:"StateReason,omitempty"`
	CreationTime            float64                  `json:"CreationTime"`
	LastModifiedTime        float64                  `json:"LastModifiedTime"`
}

func toPipeResponse(p *Pipe) pipeResponse {
	return pipeResponse{
		Arn:                     p.ARN,
		Name:                    p.Name,
		RoleArn:                 p.RoleARN,
		Source:                  p.Source,
		Target:                  p.Target,
		Description:             p.Description,
		Enrichment:              p.Enrichment,
		KmsKeyIdentifier:        p.KmsKeyIdentifier,
		DesiredState:            p.DesiredState,
		CurrentState:            p.CurrentState,
		StateReason:             p.StateReason,
		CreationTime:            epochMillis(p.CreationTime),
		LastModifiedTime:        epochMillis(p.LastModifiedTime),
		Tags:                    p.Tags,
		SourceParameters:        p.SourceParameters,
		TargetParameters:        p.TargetParameters,
		DeadLetterConfig:        p.DeadLetterConfig,
		LogConfiguration:        p.LogConfiguration,
		EnrichmentParameters:    p.EnrichmentParameters,
		RuntimeMetricsStreaming: p.RuntimeMetricsStreaming,
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

	p, err := h.Backend.CreatePipe(CreatePipeInput{
		Name:                    name,
		RoleARN:                 req.RoleArn,
		Source:                  req.Source,
		Target:                  req.Target,
		Description:             req.Description,
		Enrichment:              req.Enrichment,
		KmsKeyIdentifier:        req.KmsKeyIdentifier,
		DesiredState:            req.DesiredState,
		Tags:                    req.Tags,
		SourceParameters:        req.SourceParameters,
		TargetParameters:        req.TargetParameters,
		DeadLetterConfig:        req.DeadLetterConfig,
		LogConfiguration:        req.LogConfiguration,
		EnrichmentParameters:    req.EnrichmentParameters,
		RuntimeMetricsStreaming: req.RuntimeMetricsStreaming,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
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
	Arn              string  `json:"Arn"`
	Name             string  `json:"Name"`
	Source           string  `json:"Source"`
	Target           string  `json:"Target"`
	Description      string  `json:"Description,omitempty"`
	CurrentState     string  `json:"CurrentState"`
	DesiredState     string  `json:"DesiredState"`
	StateReason      string  `json:"StateReason,omitempty"`
	CreationTime     float64 `json:"CreationTime"`
	LastModifiedTime float64 `json:"LastModifiedTime"`
}

type listPipesResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Pipes     []pipeSummary `json:"Pipes"`
}

func (h *Handler) handleListPipes(_ context.Context, query url.Values) ([]byte, error) {
	f := ListPipesFilter{
		NamePrefix:   query.Get("NamePrefix"),
		DesiredState: query.Get("DesiredState"),
		CurrentState: query.Get("CurrentState"),
		SourcePrefix: query.Get("SourcePrefix"),
		TargetPrefix: query.Get("TargetPrefix"),
		NextToken:    query.Get("NextToken"),
	}

	if limitStr := query.Get("Limit"); limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("%w: Limit must be a positive integer", ErrValidation)
		}

		f.Limit = n
	}

	res := h.Backend.ListPipes(f)
	items := make([]pipeSummary, 0, len(res.Pipes))

	for _, p := range res.Pipes {
		items = append(items, pipeSummary{
			Arn:              p.ARN,
			Name:             p.Name,
			Source:           p.Source,
			Target:           p.Target,
			Description:      p.Description,
			CurrentState:     p.CurrentState,
			DesiredState:     p.DesiredState,
			StateReason:      p.StateReason,
			CreationTime:     epochMillis(p.CreationTime),
			LastModifiedTime: epochMillis(p.LastModifiedTime),
		})
	}

	return json.Marshal(listPipesResponse{Pipes: items, NextToken: res.NextToken})
}

func (h *Handler) handleDeletePipe(_ context.Context, path string) ([]byte, error) {
	name := extractPipeName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing pipe name in path", errInvalidRequest)
	}

	p, err := h.Backend.DeletePipe(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toPipeResponse(p))
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

	p, err := h.Backend.UpdatePipe(name, UpdatePipeInput{
		RoleARN:                 req.RoleArn,
		Target:                  req.Target,
		Description:             req.Description,
		Enrichment:              req.Enrichment,
		KmsKeyIdentifier:        req.KmsKeyIdentifier,
		DesiredState:            req.DesiredState,
		SourceParameters:        req.SourceParameters,
		TargetParameters:        req.TargetParameters,
		DeadLetterConfig:        req.DeadLetterConfig,
		LogConfiguration:        req.LogConfiguration,
		EnrichmentParameters:    req.EnrichmentParameters,
		RuntimeMetricsStreaming: req.RuntimeMetricsStreaming,
	})
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

func (h *Handler) handleUntagResource(
	_ context.Context,
	path string,
	query url.Values,
) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tagKeys := query["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
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

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
