package bedrock

import (
	"encoding/json"
	"errors"
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
	guardrailsPrefix            = "/guardrails"
	foundationModelsPrefix      = "/foundation-models"
	provisionedModelThroughput  = "/provisioned-model-throughput"
	provisionedModelThroughputs = "/provisioned-model-throughputs"
	listTagsForResourcePath     = "/listTagsForResource"
	tagResourcePath             = "/tagResource"
	untagResourcePath           = "/untagResource"
)

// isoTime is a [time.Time] that marshals as RFC3339.
type isoTime struct {
	time.Time
}

func (t isoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Format(time.RFC3339))
}

// Handler is the Echo HTTP handler for Amazon Bedrock operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Bedrock handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Bedrock" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateGuardrail",
		"GetGuardrail",
		"ListGuardrails",
		"UpdateGuardrail",
		"DeleteGuardrail",
		"ListFoundationModels",
		"GetFoundationModel",
		"CreateProvisionedModelThroughput",
		"GetProvisionedModelThroughput",
		"ListProvisionedModelThroughputs",
		"UpdateProvisionedModelThroughput",
		"DeleteProvisionedModelThroughput",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "bedrock" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Bedrock requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, guardrailsPrefix) ||
			strings.HasPrefix(path, foundationModelsPrefix) ||
			strings.HasPrefix(path, provisionedModelThroughput) ||
			path == listTagsForResourcePath ||
			path == tagResourcePath ||
			path == untagResourcePath
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name from the request.
//
//nolint:cyclop // dispatch function enumerates all operations
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch {
	case path == guardrailsPrefix && method == http.MethodPost:
		return "CreateGuardrail"
	case path == guardrailsPrefix && method == http.MethodGet:
		return "ListGuardrails"
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
		return "GetGuardrail"
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
		return "UpdateGuardrail"
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
		return "DeleteGuardrail"
	case path == foundationModelsPrefix && method == http.MethodGet:
		return "ListFoundationModels"
	case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
		return "GetFoundationModel"
	case path == provisionedModelThroughput && method == http.MethodPost:
		return "CreateProvisionedModelThroughput"
	case path == provisionedModelThroughputs && method == http.MethodGet:
		return "ListProvisionedModelThroughputs"
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
		return "GetProvisionedModelThroughput"
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPut:
		return "UpdateProvisionedModelThroughput"
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
		return "DeleteProvisionedModelThroughput"
	case path == listTagsForResourcePath:
		return "ListTagsForResource"
	case path == tagResourcePath:
		return "TagResource"
	case path == untagResourcePath:
		return "UntagResource"
	default:
		return "Unknown"
	}
}

// ExtractResource extracts a resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if id, ok := strings.CutPrefix(path, guardrailsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, foundationModelsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, provisionedModelThroughput+"/"); ok {
		return id
	}

	return ""
}

// Handler returns the Echo handler function for Bedrock requests.
//
//nolint:gocognit,gocyclo,cyclop // dispatch function enumerates all operations
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method
		log := logger.Load(r.Context())

		var body []byte
		if method == http.MethodPost || method == http.MethodPut {
			var err error
			body, err = httputils.ReadBody(r)
			if err != nil {
				log.ErrorContext(r.Context(), "bedrock: failed to read request body", "error", err)

				return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
			}
		}

		switch {
		case path == guardrailsPrefix && method == http.MethodPost:
			return h.handleCreateGuardrail(c, body)
		case path == guardrailsPrefix && method == http.MethodGet:
			return h.handleListGuardrails(c)
		case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
			return h.handleGetGuardrail(
				c,
				decodePath(strings.TrimPrefix(path, guardrailsPrefix+"/")),
			)
		case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
			return h.handleUpdateGuardrail(
				c,
				decodePath(strings.TrimPrefix(path, guardrailsPrefix+"/")),
				body,
			)
		case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
			return h.handleDeleteGuardrail(
				c,
				decodePath(strings.TrimPrefix(path, guardrailsPrefix+"/")),
			)
		case path == foundationModelsPrefix && method == http.MethodGet:
			return h.handleListFoundationModels(c)
		case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
			return h.handleGetFoundationModel(
				c,
				decodePath(strings.TrimPrefix(path, foundationModelsPrefix+"/")),
			)
		case path == provisionedModelThroughput && method == http.MethodPost:
			return h.handleCreateProvisionedModelThroughput(c, body)
		case path == provisionedModelThroughputs && method == http.MethodGet:
			return h.handleListProvisionedModelThroughputs(c)
		case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
			return h.handleGetProvisionedModelThroughput(
				c,
				decodePath(strings.TrimPrefix(path, provisionedModelThroughput+"/")),
			)
		case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPut:
			return h.handleUpdateProvisionedModelThroughput(
				c,
				decodePath(strings.TrimPrefix(path, provisionedModelThroughput+"/")),
				body,
			)
		case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
			return h.handleDeleteProvisionedModelThroughput(
				c,
				decodePath(strings.TrimPrefix(path, provisionedModelThroughput+"/")),
			)
		case path == listTagsForResourcePath && method == http.MethodPost:
			return h.handleListTagsForResource(c, body)
		case path == tagResourcePath && method == http.MethodPost:
			return h.handleTagResource(c, body)
		case path == untagResourcePath && method == http.MethodPost:
			return h.handleUntagResource(c, body)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
		}
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"message": msg, "type": code}
}

// parseBody parses JSON bytes into a value of type T.
func parseBody[T any](body []byte) (*T, error) {
	var v T
	if len(body) == 0 {
		return &v, nil
	}

	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	return &v, nil
}

// decodePath URL-decodes a path segment (e.g., ARNs encoded with %3A).
func decodePath(s string) string {
	decoded, err := url.PathUnescape(s)
	if err != nil {
		return s
	}

	return decoded
}

// --- Guardrail handlers ---

type createGuardrailInput struct {
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
	Tags                    []Tag  `json:"tags"`
}

type createGuardrailOutput struct {
	CreatedAt    isoTime `json:"createdAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleCreateGuardrail(c *echo.Context, body []byte) error {
	in, err := parseBody[createGuardrailInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	g, opErr := h.Backend.CreateGuardrail(
		in.Name, in.Description, in.BlockedInputMessaging, in.BlockedOutputsMessaging, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		CreatedAt:    isoTime{g.CreatedAt},
	})
}

type guardrailDetailOutput struct {
	CreatedAt               isoTime `json:"createdAt"`
	UpdatedAt               isoTime `json:"updatedAt"`
	GuardrailID             string  `json:"guardrailId"`
	GuardrailArn            string  `json:"guardrailArn"`
	Name                    string  `json:"name"`
	Description             string  `json:"description"`
	Status                  string  `json:"status"`
	Version                 string  `json:"version"`
	BlockedInputMessaging   string  `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string  `json:"blockedOutputsMessaging"`
}

func (h *Handler) handleGetGuardrail(c *echo.Context, id string) error {
	g, err := h.Backend.GetGuardrail(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, guardrailDetailOutput{
		GuardrailID:             g.GuardrailID,
		GuardrailArn:            g.GuardrailArn,
		Name:                    g.Name,
		Description:             g.Description,
		Status:                  g.Status,
		Version:                 g.Version,
		BlockedInputMessaging:   g.BlockedInputMessaging,
		BlockedOutputsMessaging: g.BlockedOutputsMessaging,
		CreatedAt:               isoTime{g.CreatedAt},
		UpdatedAt:               isoTime{g.UpdatedAt},
	})
}

type guardrailSummaryOutput struct {
	CreatedAt   isoTime `json:"createdAt"`
	UpdatedAt   isoTime `json:"updatedAt"`
	ID          string  `json:"id"`
	Arn         string  `json:"arn"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Status      string  `json:"status"`
	Version     string  `json:"version"`
}

type listGuardrailsOutput struct {
	NextToken  string                   `json:"nextToken,omitempty"`
	Guardrails []guardrailSummaryOutput `json:"guardrails"`
}

func (h *Handler) handleListGuardrails(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	guardrails, outToken := h.Backend.ListGuardrails(nextToken)
	summaries := make([]guardrailSummaryOutput, 0, len(guardrails))

	for _, g := range guardrails {
		summaries = append(summaries, guardrailSummaryOutput{
			ID:          g.GuardrailID,
			Arn:         g.Arn,
			Name:        g.Name,
			Description: g.Description,
			Status:      g.Status,
			Version:     g.Version,
			CreatedAt:   isoTime{g.CreatedAt},
			UpdatedAt:   isoTime{g.UpdatedAt},
		})
	}

	resp := listGuardrailsOutput{Guardrails: summaries}
	if outToken != "" {
		resp.NextToken = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

type updateGuardrailInput struct {
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
}

type updateGuardrailOutput struct {
	UpdatedAt    isoTime `json:"updatedAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleUpdateGuardrail(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[updateGuardrailInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	g, opErr := h.Backend.UpdateGuardrail(id, in.Description, in.BlockedInputMessaging, in.BlockedOutputsMessaging)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, updateGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		UpdatedAt:    isoTime{g.UpdatedAt},
	})
}

func (h *Handler) handleDeleteGuardrail(c *echo.Context, id string) error {
	if err := h.Backend.DeleteGuardrail(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Foundation model handlers ---

type foundationModelSummaryOutput struct {
	ModelArn         string   `json:"modelArn"`
	ModelID          string   `json:"modelId"`
	ModelName        string   `json:"modelName"`
	ProviderName     string   `json:"providerName"`
	InputModalities  []string `json:"inputModalities,omitempty"`
	OutputModalities []string `json:"outputModalities,omitempty"`
}

type listFoundationModelsOutput struct {
	NextToken      string                         `json:"nextToken,omitempty"`
	ModelSummaries []foundationModelSummaryOutput `json:"modelSummaries"`
}

func (h *Handler) handleListFoundationModels(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	models, outToken := h.Backend.ListFoundationModels(nextToken)
	summaries := make([]foundationModelSummaryOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, foundationModelSummaryOutput{
			ModelArn:         m.ModelArn,
			ModelID:          m.ModelID,
			ModelName:        m.ModelName,
			ProviderName:     m.ProviderName,
			InputModalities:  m.InputModalities,
			OutputModalities: m.OutputModalities,
		})
	}

	return c.JSON(http.StatusOK, listFoundationModelsOutput{ModelSummaries: summaries, NextToken: outToken})
}

type getFoundationModelOutput struct {
	ModelDetails foundationModelSummaryOutput `json:"modelDetails"`
}

func (h *Handler) handleGetFoundationModel(c *echo.Context, modelID string) error {
	m, err := h.Backend.GetFoundationModel(modelID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, getFoundationModelOutput{
		ModelDetails: foundationModelSummaryOutput{
			ModelArn:         m.ModelArn,
			ModelID:          m.ModelID,
			ModelName:        m.ModelName,
			ProviderName:     m.ProviderName,
			InputModalities:  m.InputModalities,
			OutputModalities: m.OutputModalities,
		},
	})
}

// --- Provisioned model throughput handlers ---

type createProvisionedModelThroughputInput struct {
	ProvisionedModelName string `json:"provisionedModelName"`
	ModelID              string `json:"modelId"`
	CommitmentDuration   string `json:"commitmentDuration,omitempty"`
	Tags                 []Tag  `json:"tags"`
	ModelUnits           int32  `json:"modelUnits"`
}

type createProvisionedModelThroughputOutput struct {
	ProvisionedModelArn string `json:"provisionedModelArn"`
}

func (h *Handler) handleCreateProvisionedModelThroughput(c *echo.Context, body []byte) error {
	in, err := parseBody[createProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	pmt, opErr := h.Backend.CreateProvisionedModelThroughput(
		in.ProvisionedModelName,
		in.ModelID,
		in.ModelUnits,
		in.CommitmentDuration,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createProvisionedModelThroughputOutput{
		ProvisionedModelArn: pmt.ProvisionedModelArn,
	})
}

type provisionedModelSummaryOutput struct {
	CreationTime         isoTime `json:"creationTime"`
	LastModifiedTime     isoTime `json:"lastModifiedTime"`
	ProvisionedModelArn  string  `json:"provisionedModelArn"`
	ProvisionedModelName string  `json:"provisionedModelName"`
	ModelArn             string  `json:"modelArn"`
	DesiredModelArn      string  `json:"desiredModelArn"`
	FoundationModelArn   string  `json:"foundationModelArn"`
	Status               string  `json:"status"`
	CommitmentDuration   string  `json:"commitmentDuration,omitempty"`
	ModelUnits           int32   `json:"modelUnits"`
	DesiredModelUnits    int32   `json:"desiredModelUnits"`
}

func pmtToOutput(pmt *ProvisionedModelThroughput) provisionedModelSummaryOutput {
	return provisionedModelSummaryOutput{
		ProvisionedModelArn:  pmt.ProvisionedModelArn,
		ProvisionedModelName: pmt.ProvisionedModelName,
		ModelArn:             pmt.ModelArn,
		DesiredModelArn:      pmt.DesiredModelArn,
		FoundationModelArn:   pmt.FoundationModelArn,
		Status:               pmt.Status,
		ModelUnits:           pmt.ModelUnits,
		DesiredModelUnits:    pmt.DesiredModelUnits,
		CommitmentDuration:   pmt.CommitmentDuration,
		CreationTime:         isoTime{pmt.CreationTime},
		LastModifiedTime:     isoTime{pmt.LastModifiedTime},
	}
}

func (h *Handler) handleGetProvisionedModelThroughput(c *echo.Context, id string) error {
	pmt, err := h.Backend.GetProvisionedModelThroughput(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, pmtToOutput(pmt))
}

type listProvisionedModelThroughputsOutput struct {
	NextToken                 string                          `json:"nextToken,omitempty"`
	ProvisionedModelSummaries []provisionedModelSummaryOutput `json:"provisionedModelSummaries"`
}

func (h *Handler) handleListProvisionedModelThroughputs(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	pmts, outToken := h.Backend.ListProvisionedModelThroughputs(nextToken)
	summaries := make([]provisionedModelSummaryOutput, 0, len(pmts))

	for _, pmt := range pmts {
		summaries = append(summaries, pmtToOutput(pmt))
	}

	return c.JSON(
		http.StatusOK,
		listProvisionedModelThroughputsOutput{ProvisionedModelSummaries: summaries, NextToken: outToken},
	)
}

type updateProvisionedModelThroughputInput struct {
	ModelUnits *int32 `json:"modelUnits,omitempty"`
	ModelID    string `json:"modelId"`
}

func (h *Handler) handleUpdateProvisionedModelThroughput(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[updateProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	_, opErr := h.Backend.UpdateProvisionedModelThroughput(id, in.ModelID, in.ModelUnits)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteProvisionedModelThroughput(c *echo.Context, id string) error {
	if err := h.Backend.DeleteProvisionedModelThroughput(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Tag handlers ---

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceARN"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	in, err := parseBody[listTagsForResourceInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	tags, opErr := h.Backend.ListTagsForResource(in.ResourceARN)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	if tags == nil {
		tags = []Tag{}
	}

	return c.JSON(http.StatusOK, listTagsForResourceOutput{Tags: tags})
}

type tagResourceInput struct {
	ResourceARN string `json:"resourceARN"`
	Tags        []Tag  `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[tagResourceInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if opErr := h.Backend.TagResource(in.ResourceARN, in.Tags); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceARN"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[untagResourceInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if opErr := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}
