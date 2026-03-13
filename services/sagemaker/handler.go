package sagemaker

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
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	sagemakerService       = "sagemaker"
	sagemakerTargetPrefix  = "SageMaker."
	sagemakerMatchPriority = service.PriorityHeaderExact
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS SageMaker JSON API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new SageMaker handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "SageMaker" }

// GetSupportedOperations returns the list of supported SageMaker operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateModel",
		"DescribeModel",
		"ListModels",
		"DeleteModel",
		"CreateEndpointConfig",
		"DescribeEndpointConfig",
		"ListEndpointConfigs",
		"DeleteEndpointConfig",
		"AddTags",
		"ListTags",
		"DeleteTags",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return sagemakerService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches SageMaker API requests.
// SageMaker requests carry an X-Amz-Target header beginning with "SageMaker.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, sagemakerTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return sagemakerMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, sagemakerTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for SageMaker requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "sagemaker: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "CreateModel":
		return h.handleCreateModel(ctx, body)
	case "DescribeModel":
		return h.handleDescribeModel(ctx, body)
	case "ListModels":
		return h.handleListModels()
	case "DeleteModel":
		return h.handleDeleteModel(ctx, body)
	case "CreateEndpointConfig":
		return h.handleCreateEndpointConfig(ctx, body)
	case "DescribeEndpointConfig":
		return h.handleDescribeEndpointConfig(ctx, body)
	case "ListEndpointConfigs":
		return h.handleListEndpointConfigs()
	case "DeleteEndpointConfig":
		return h.handleDeleteEndpointConfig(ctx, body)
	case "AddTags":
		return h.handleAddTags(ctx, body)
	case "ListTags":
		return h.handleListTags(ctx, body)
	case "DeleteTags":
		return h.handleDeleteTags(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceInUse",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS REST-JSON protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// tagObject represents a SageMaker tag in the JSON API format.
type tagObject struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// toTagObjects converts a map of tags to a slice of tag objects.
func toTagObjects(tags map[string]string) []tagObject {
	result := make([]tagObject, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagObject{Key: k, Value: v})
	}

	return result
}

// fromTagObjects converts a slice of tag objects to a map.
func fromTagObjects(tags []tagObject) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// createModelRequest is the request body for CreateModel.
type createModelRequest struct {
	PrimaryContainer *ContainerDefinition  `json:"PrimaryContainer"`
	ModelName        string                `json:"ModelName"`
	ExecutionRoleArn string                `json:"ExecutionRoleArn"`
	Tags             []tagObject           `json:"Tags"`
	Containers       []ContainerDefinition `json:"Containers"`
}

// modelSummary is a summary of a SageMaker model for list responses.
type modelSummary struct {
	ModelName    string  `json:"ModelName"`
	ModelArn     string  `json:"ModelArn"`
	CreationTime float64 `json:"CreationTime"`
}

// describeModelResponse is the response body for DescribeModel.
type describeModelResponse struct {
	PrimaryContainer *ContainerDefinition  `json:"PrimaryContainer,omitempty"`
	ModelArn         string                `json:"ModelArn"`
	ModelName        string                `json:"ModelName"`
	ExecutionRoleArn string                `json:"ExecutionRoleArn"`
	Containers       []ContainerDefinition `json:"Containers,omitempty"`
	CreationTime     float64               `json:"CreationTime"`
}

func (h *Handler) handleCreateModel(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	m, err := h.Backend.CreateModel(
		req.ModelName,
		req.ExecutionRoleArn,
		req.PrimaryContainer,
		req.Containers,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created model", "name", m.ModelName, "arn", m.ModelARN)

	return json.Marshal(map[string]string{"ModelArn": m.ModelARN})
}

func (h *Handler) handleDescribeModel(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelName string `json:"ModelName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	m, err := h.Backend.DescribeModel(req.ModelName)
	if err != nil {
		return nil, err
	}

	resp := describeModelResponse{
		ModelName:        m.ModelName,
		ModelArn:         m.ModelARN,
		ExecutionRoleArn: m.ExecutionRoleARN,
		CreationTime:     epochSeconds(m.CreationTime),
		PrimaryContainer: m.PrimaryContainer,
		Containers:       m.Containers,
	}

	if len(resp.Containers) == 0 {
		resp.Containers = nil
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListModels() ([]byte, error) {
	models := h.Backend.ListModels()
	summaries := make([]modelSummary, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, modelSummary{
			ModelName:    m.ModelName,
			ModelArn:     m.ModelARN,
			CreationTime: epochSeconds(m.CreationTime),
		})
	}

	return json.Marshal(map[string]any{"Models": summaries})
}

func (h *Handler) handleDeleteModel(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelName string `json:"ModelName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteModel(req.ModelName); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted model", "name", req.ModelName)

	return nil, nil
}

// createEndpointConfigRequest is the request body for CreateEndpointConfig.
type createEndpointConfigRequest struct {
	EndpointConfigName string              `json:"EndpointConfigName"`
	Tags               []tagObject         `json:"Tags"`
	ProductionVariants []ProductionVariant `json:"ProductionVariants"`
}

// endpointConfigSummary is a summary of an endpoint config for list responses.
type endpointConfigSummary struct {
	EndpointConfigArn  string  `json:"EndpointConfigArn"`
	EndpointConfigName string  `json:"EndpointConfigName"`
	CreationTime       float64 `json:"CreationTime"`
}

// describeEndpointConfigResponse is the response body for DescribeEndpointConfig.
type describeEndpointConfigResponse struct {
	EndpointConfigArn  string              `json:"EndpointConfigArn"`
	EndpointConfigName string              `json:"EndpointConfigName"`
	ProductionVariants []ProductionVariant `json:"ProductionVariants"`
	CreationTime       float64             `json:"CreationTime"`
}

func (h *Handler) handleCreateEndpointConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req createEndpointConfigRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	ec, err := h.Backend.CreateEndpointConfig(req.EndpointConfigName, req.ProductionVariants, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint config",
		"name",
		ec.EndpointConfigName,
		"arn",
		ec.EndpointConfigARN,
	)

	return json.Marshal(map[string]string{"EndpointConfigArn": ec.EndpointConfigARN})
}

func (h *Handler) handleDescribeEndpointConfig(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointConfigName string `json:"EndpointConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	ec, err := h.Backend.DescribeEndpointConfig(req.EndpointConfigName)
	if err != nil {
		return nil, err
	}

	resp := describeEndpointConfigResponse{
		EndpointConfigName: ec.EndpointConfigName,
		EndpointConfigArn:  ec.EndpointConfigARN,
		ProductionVariants: ec.ProductionVariants,
		CreationTime:       epochSeconds(ec.CreationTime),
	}

	if len(resp.ProductionVariants) == 0 {
		resp.ProductionVariants = nil
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListEndpointConfigs() ([]byte, error) {
	configs := h.Backend.ListEndpointConfigs()
	summaries := make([]endpointConfigSummary, 0, len(configs))

	for _, ec := range configs {
		summaries = append(summaries, endpointConfigSummary{
			EndpointConfigName: ec.EndpointConfigName,
			EndpointConfigArn:  ec.EndpointConfigARN,
			CreationTime:       epochSeconds(ec.CreationTime),
		})
	}

	return json.Marshal(map[string]any{"EndpointConfigs": summaries})
}

func (h *Handler) handleDeleteEndpointConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointConfigName string `json:"EndpointConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpointConfig(req.EndpointConfigName); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint config", "name", req.EndpointConfigName)

	return nil, nil
}

func (h *Handler) handleAddTags(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string      `json:"ResourceArn"`
		Tags        []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	if err := h.Backend.AddTags(req.ResourceArn, tags); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: added tags", "resource", req.ResourceArn)

	return json.Marshal(map[string]any{"Tags": req.Tags})
}

func (h *Handler) handleListTags(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTags(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Tags": toTagObjects(tags)})
}

func (h *Handler) handleDeleteTags(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTags(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted tags", "resource", req.ResourceArn)

	return nil, nil
}
