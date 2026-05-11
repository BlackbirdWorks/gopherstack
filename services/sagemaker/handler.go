package sagemaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField = "__type"
)

const (
	errValidationException = "ValidationException"
	keyMessageField        = "message"
	keyClusterArn          = "ClusterArn"
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

// Reset clears all resources from the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns the list of supported SageMaker operations.
func (h *Handler) GetSupportedOperations() []string {
	core := []string{
		"AddAssociation",
		"AddTags",
		"AssociateTrialComponent",
		"AttachClusterNodeVolume",
		"BatchAddClusterNodes",
		"BatchDeleteClusterNodes",
		"BatchDescribeModelPackage",
		"BatchRebootClusterNodes",
		"BatchReplaceClusterNodes",
		"CreateAction",
		"CreateAlgorithm",
		opCreateApp,
		opCreateDomain,
		"CreateEndpoint",
		"CreateEndpointConfig",
		opCreateExperiment,
		opCreateFeatureGroup,
		"CreateHyperParameterTuningJob",
		"CreateModel",
		"CreateNotebookInstance",
		opCreatePipeline,
		"CreatePresignedNotebookInstanceUrl",
		"CreateTrainingJob",
		opCreateTrial,
		opCreateTrialComponent,
		opCreateUserProfile,
		opDeleteApp,
		opDeleteDomain,
		"DeleteEndpoint",
		"DeleteEndpointConfig",
		opDeleteExperiment,
		opDeleteFeatureGroup,
		"DeleteHyperParameterTuningJob",
		"DeleteModel",
		"DeleteNotebookInstance",
		opDeletePipeline,
		opDeleteTrial,
		opDeleteTrialComponent,
		opDeleteUserProfile,
		"DeleteTags",
		"DeleteTrainingJob",
		opDescribeApp,
		opDescribeDomain,
		"DescribeEndpoint",
		"DescribeEndpointConfig",
		opDescribeExperiment,
		opDescribeFeatureGroup,
		"DescribeHyperParameterTuningJob",
		"DescribeModel",
		"DescribeNotebookInstance",
		opDescribePipeline,
		opDescribePipelineExec,
		opDescribeTrial,
		opDescribeTrialComponent,
		opDescribeUserProfile,
		"DescribeTrainingJob",
		opListApps,
		opListDomains,
		"ListEndpointConfigs",
		"ListEndpoints",
		opListExperiments,
		opListFeatureGroups,
		"ListHyperParameterTuningJobs",
		"ListModels",
		"ListNotebookInstances",
		opListPipelineExecutions,
		opListPipelines,
		opListTrials,
		opListUserProfiles,
		"ListTags",
		"ListTrainingJobs",
		"StartNotebookInstance",
		opStartPipelineExecution,
		"StopHyperParameterTuningJob",
		"StopNotebookInstance",
		"StopTrainingJob",
		opUpdateDomain,
		"UpdateEndpoint",
		"UpdateEndpointWeightsAndCapacities",
		opUpdatePipeline,
		"UpdateNotebookInstance",
		"UpdateTrainingJob",
	}

	return append(core, stubOpsSupported()...)
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
	if result, ok, err := h.dispatchCoreOps(ctx, op, body); ok {
		return result, err
	}

	return h.dispatchNewOps(ctx, op, body)
}

func (h *Handler) dispatchCoreOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateModel":
		r, err := h.handleCreateModel(ctx, body)

		return r, true, err
	case "DescribeModel":
		r, err := h.handleDescribeModel(ctx, body)

		return r, true, err
	case "ListModels":
		r, err := h.handleListModels(body)

		return r, true, err
	case "DeleteModel":
		return nil, true, h.handleDeleteModel(ctx, body)
	case "CreateEndpointConfig":
		r, err := h.handleCreateEndpointConfig(ctx, body)

		return r, true, err
	case "DescribeEndpointConfig":
		r, err := h.handleDescribeEndpointConfig(ctx, body)

		return r, true, err
	case "ListEndpointConfigs":
		r, err := h.handleListEndpointConfigs(body)

		return r, true, err
	case "DeleteEndpointConfig":
		return nil, true, h.handleDeleteEndpointConfig(ctx, body)
	case "AddTags":
		r, err := h.handleAddTags(ctx, body)

		return r, true, err
	case "ListTags":
		r, err := h.handleListTags(ctx, body)

		return r, true, err
	case "DeleteTags":
		return nil, true, h.handleDeleteTags(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchNewOps(ctx context.Context, op string, body []byte) ([]byte, error) {
	if r, ok, err := h.dispatchLineageAndBatchOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchEndpointOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchTrainingJobOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchNotebookOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchHPTuningJobOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchStatefulOps(ctx, op, body); ok {
		return r, err
	}

	if r, ok, err := h.dispatchStubOps(ctx, op, body); ok {
		return r, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

func (h *Handler) dispatchLineageAndBatchOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "AddAssociation":
		r, err := h.handleAddAssociation(ctx, body)

		return r, true, err
	case "AssociateTrialComponent":
		r, err := h.handleAssociateTrialComponent(ctx, body)

		return r, true, err
	case "AttachClusterNodeVolume":
		r, err := h.handleAttachClusterNodeVolume(ctx, body)

		return r, true, err
	case "BatchAddClusterNodes":
		r, err := h.handleBatchAddClusterNodes(ctx, body)

		return r, true, err
	case "BatchDeleteClusterNodes":
		r, err := h.handleBatchDeleteClusterNodes(ctx, body)

		return r, true, err
	case "BatchDescribeModelPackage":
		r, err := h.handleBatchDescribeModelPackage(body)

		return r, true, err
	case "BatchRebootClusterNodes":
		r, err := h.handleBatchRebootClusterNodes(ctx, body)

		return r, true, err
	case "BatchReplaceClusterNodes":
		r, err := h.handleBatchReplaceClusterNodes(ctx, body)

		return r, true, err
	case "CreateAction":
		r, err := h.handleCreateAction(ctx, body)

		return r, true, err
	case "CreateAlgorithm":
		r, err := h.handleCreateAlgorithm(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchEndpointOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateEndpoint":
		r, err := h.handleCreateEndpoint(ctx, body)

		return r, true, err
	case "DescribeEndpoint":
		r, err := h.handleDescribeEndpoint(ctx, body)

		return r, true, err
	case "ListEndpoints":
		r, err := h.handleListEndpoints(body)

		return r, true, err
	case "DeleteEndpoint":
		return nil, true, h.handleDeleteEndpoint(ctx, body)
	case "UpdateEndpoint":
		r, err := h.handleUpdateEndpoint(ctx, body)

		return r, true, err
	case "UpdateEndpointWeightsAndCapacities":
		r, err := h.handleUpdateEndpointWeightsAndCapacities(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchTrainingJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateTrainingJob":
		r, err := h.handleCreateTrainingJob(ctx, body)

		return r, true, err
	case "DescribeTrainingJob":
		r, err := h.handleDescribeTrainingJob(ctx, body)

		return r, true, err
	case "ListTrainingJobs":
		r, err := h.handleListTrainingJobs(body)

		return r, true, err
	case "StopTrainingJob":
		return nil, true, h.handleStopTrainingJob(ctx, body)
	case "DeleteTrainingJob":
		return nil, true, h.handleDeleteTrainingJob(ctx, body)
	case "UpdateTrainingJob":
		r, err := h.handleUpdateTrainingJob(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchNotebookOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateNotebookInstance":
		r, err := h.handleCreateNotebookInstance(ctx, body)

		return r, true, err
	case "DescribeNotebookInstance":
		r, err := h.handleDescribeNotebookInstance(ctx, body)

		return r, true, err
	case "ListNotebookInstances":
		r, err := h.handleListNotebookInstances(body)

		return r, true, err
	case "DeleteNotebookInstance":
		return nil, true, h.handleDeleteNotebookInstance(ctx, body)
	case "StartNotebookInstance":
		return nil, true, h.handleStartNotebookInstance(ctx, body)
	case "StopNotebookInstance":
		return nil, true, h.handleStopNotebookInstance(ctx, body)
	case "UpdateNotebookInstance":
		return nil, true, h.handleUpdateNotebookInstance(ctx, body)
	case "CreatePresignedNotebookInstanceUrl":
		r, err := h.handleCreatePresignedNotebookInstanceURL(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchHPTuningJobOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateHyperParameterTuningJob":
		r, err := h.handleCreateHyperParameterTuningJob(ctx, body)

		return r, true, err
	case "DescribeHyperParameterTuningJob":
		r, err := h.handleDescribeHyperParameterTuningJob(ctx, body)

		return r, true, err
	case "ListHyperParameterTuningJobs":
		r, err := h.handleListHyperParameterTuningJobs(body)

		return r, true, err
	case "StopHyperParameterTuningJob":
		return nil, true, h.handleStopHyperParameterTuningJob(ctx, body)
	case "DeleteHyperParameterTuningJob":
		return nil, true, h.handleDeleteHyperParameterTuningJob(ctx, body)
	}

	return nil, false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrInvalidParameter):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceInUse",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
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

// toTagObjects converts a map of tags to a slice of tag objects sorted by key.
func toTagObjects(tags map[string]string) []tagObject {
	result := make([]tagObject, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagObject{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

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

func (h *Handler) handleListModels(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	models, nextToken := h.Backend.ListModels(req.NextToken)
	summaries := make([]modelSummary, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, modelSummary{
			ModelName:    m.ModelName,
			ModelArn:     m.ModelARN,
			CreationTime: epochSeconds(m.CreationTime),
		})
	}

	resp := map[string]any{"Models": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteModel(ctx context.Context, body []byte) error {
	var req struct {
		ModelName string `json:"ModelName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteModel(req.ModelName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted model", "name", req.ModelName)

	return nil
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

func (h *Handler) handleListEndpointConfigs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListEndpointConfigs(req.NextToken)
	summaries := make([]endpointConfigSummary, 0, len(configs))

	for _, ec := range configs {
		summaries = append(summaries, endpointConfigSummary{
			EndpointConfigName: ec.EndpointConfigName,
			EndpointConfigArn:  ec.EndpointConfigARN,
			CreationTime:       epochSeconds(ec.CreationTime),
		})
	}

	resp := map[string]any{"EndpointConfigs": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteEndpointConfig(ctx context.Context, body []byte) error {
	var req struct {
		EndpointConfigName string `json:"EndpointConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.EndpointConfigName == "" {
		return fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteEndpointConfig(req.EndpointConfigName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted endpoint config", "name", req.EndpointConfigName)

	return nil
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

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTags(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
		NextToken   string `json:"NextToken"`
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

	allTags := toTagObjects(tags)
	startIdx := parseNextToken(req.NextToken)
	if startIdx >= len(allTags) {
		return json.Marshal(map[string]any{"Tags": []tagObject{}})
	}
	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(allTags) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(allTags)
	}

	resp := map[string]any{"Tags": allTags[startIdx:end]}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteTags(ctx context.Context, body []byte) error {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTags(req.ResourceArn, req.TagKeys); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted tags", "resource", req.ResourceArn)

	return nil
}

// addAssociationRequest is the request body for AddAssociation.
type addAssociationRequest struct {
	SourceArn       string      `json:"SourceArn"`
	DestinationArn  string      `json:"DestinationArn"`
	AssociationType string      `json:"AssociationType"`
	Tags            []tagObject `json:"Tags"`
}

func (h *Handler) handleAddAssociation(ctx context.Context, body []byte) ([]byte, error) {
	var req addAssociationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", errInvalidRequest)
	}

	if req.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	assoc, err := h.Backend.AddAssociation(req.SourceArn, req.DestinationArn, req.AssociationType, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: added association", "arn", assoc.AssociationArn)

	return json.Marshal(map[string]string{"AssociationArn": assoc.AssociationArn})
}

// associateTrialComponentRequest is the request body for AssociateTrialComponent.
type associateTrialComponentRequest struct {
	TrialName          string `json:"TrialName"`
	TrialComponentName string `json:"TrialComponentName"`
}

func (h *Handler) handleAssociateTrialComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req associateTrialComponentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", errInvalidRequest)
	}

	if req.TrialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", errInvalidRequest)
	}

	assoc, err := h.Backend.AssociateTrialComponent(req.TrialName, req.TrialComponentName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: associated trial component",
		"trial", assoc.TrialArn, "component", assoc.TrialComponentArn)

	return json.Marshal(map[string]string{
		"TrialArn":          assoc.TrialArn,
		"TrialComponentArn": assoc.TrialComponentArn,
	})
}

// clusterNodeVolumeRequest is the volume config in the handler request.
type clusterNodeVolumeRequest struct {
	VolumeName string `json:"VolumeName"`
	SizeInGB   int32  `json:"SizeInGB,omitempty"`
}

// attachClusterNodeVolumeRequest is the request body for AttachClusterNodeVolume.
type attachClusterNodeVolumeRequest struct {
	ClusterName  string                   `json:"ClusterName"`
	NodeID       string                   `json:"NodeId"`
	VolumeConfig clusterNodeVolumeRequest `json:"VolumeConfig"`
}

func (h *Handler) handleAttachClusterNodeVolume(ctx context.Context, body []byte) ([]byte, error) {
	var req attachClusterNodeVolumeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	if req.NodeID == "" {
		return nil, fmt.Errorf("%w: NodeId is required", errInvalidRequest)
	}

	vol := ClusterNodeVolume{
		VolumeName: req.VolumeConfig.VolumeName,
		SizeInGB:   req.VolumeConfig.SizeInGB,
	}

	clusterArn, nodeID, err := h.Backend.AttachClusterNodeVolume(req.ClusterName, req.NodeID, vol)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: attached cluster node volume", "cluster", clusterArn, "node", nodeID)

	return json.Marshal(map[string]string{
		keyClusterArn: clusterArn,
		"NodeId":      nodeID,
	})
}

// clusterNodeRequest represents a node config in batch cluster operations.
type clusterNodeRequest struct {
	NodeID       string `json:"NodeId"`
	InstanceType string `json:"InstanceType,omitempty"`
}

// batchClusterNodesWithFailures is a shared helper for cluster batch operations that return Failures.
func (h *Handler) batchClusterNodesWithFailures(
	ctx context.Context,
	clusterName, logMsg string,
	nodes []ClusterNode,
	fn func(string, []ClusterNode) (string, []string, error),
) ([]byte, error) {
	clusterArn, failures, err := fn(clusterName, nodes)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, logMsg, "cluster", clusterArn)

	if failures == nil {
		failures = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Failures":    failures,
	})
}

// toClusterNodes converts a slice of clusterNodeRequest to ClusterNode.
func toClusterNodes(reqs []clusterNodeRequest) []ClusterNode {
	nodes := make([]ClusterNode, 0, len(reqs))

	for _, r := range reqs {
		nodes = append(nodes, ClusterNode{
			NodeID:       r.NodeID,
			InstanceType: r.InstanceType,
		})
	}

	return nodes
}

// batchAddClusterNodesRequest is the request body for BatchAddClusterNodes.
type batchAddClusterNodesRequest struct {
	ClusterName string               `json:"ClusterName"`
	NodeConfigs []clusterNodeRequest `json:"NodeConfigs"`
}

func (h *Handler) handleBatchAddClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchAddClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	return h.batchClusterNodesWithFailures(
		ctx,
		req.ClusterName,
		"sagemaker: batch added cluster nodes",
		toClusterNodes(req.NodeConfigs),
		h.Backend.BatchAddClusterNodes,
	)
}

// batchDeleteClusterNodesRequest is the request body for BatchDeleteClusterNodes.
type batchDeleteClusterNodesRequest struct {
	ClusterName string   `json:"ClusterName"`
	NodeIDs     []string `json:"NodeIds"`
}

func (h *Handler) handleBatchDeleteClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchDeleteClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, errored, successful, err := h.Backend.BatchDeleteClusterNodes(req.ClusterName, req.NodeIDs)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: batch deleted cluster nodes", "cluster", clusterArn)

	if errored == nil {
		errored = []string{}
	}

	if successful == nil {
		successful = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Errors":      errored,
		"Successful":  successful,
	})
}

// batchDescribeModelPackageRequest is the request body for BatchDescribeModelPackage.
type batchDescribeModelPackageRequest struct {
	ModelPackageArnList []string `json:"ModelPackageArnList"`
}

// modelPackageSummary is a serializable model package for batch describe responses.
type modelPackageSummary struct {
	ModelPackageName      string  `json:"ModelPackageName"`
	ModelPackageArn       string  `json:"ModelPackageArn"`
	ModelPackageStatus    string  `json:"ModelPackageStatus"`
	ModelPackageGroupName string  `json:"ModelPackageGroupName,omitempty"`
	CreationTime          float64 `json:"CreationTime"`
}

// batchDescribeModelPackageError holds an error entry for BatchDescribeModelPackage.
type batchDescribeModelPackageError struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

func (h *Handler) handleBatchDescribeModelPackage(body []byte) ([]byte, error) {
	var req batchDescribeModelPackageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	results := h.Backend.BatchDescribeModelPackage(req.ModelPackageArnList)

	modelPackageMap := make(map[string]modelPackageSummary)
	errorsMap := make(map[string]batchDescribeModelPackageError)

	for arnStr, result := range results {
		if result.ErrorCode != "" {
			errorsMap[arnStr] = batchDescribeModelPackageError{
				ErrorCode:    result.ErrorCode,
				ErrorMessage: result.ErrorMessage,
			}

			continue
		}

		mp := result.ModelPackage
		modelPackageMap[arnStr] = modelPackageSummary{
			ModelPackageName:      mp.ModelPackageName,
			ModelPackageArn:       mp.ModelPackageArn,
			ModelPackageStatus:    mp.ModelPackageStatus,
			ModelPackageGroupName: mp.ModelPackageGroupName,
			CreationTime:          epochSeconds(mp.CreationTime),
		}
	}

	return json.Marshal(map[string]any{
		"ModelPackageSummaries":             modelPackageMap,
		"BatchDescribeModelPackageErrorMap": errorsMap,
	})
}

// batchRebootClusterNodesRequest is the request body for BatchRebootClusterNodes.
type batchRebootClusterNodesRequest struct {
	ClusterName string   `json:"ClusterName"`
	NodeIDs     []string `json:"NodeIds"`
}

func (h *Handler) handleBatchRebootClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchRebootClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, failures, successful, err := h.Backend.BatchRebootClusterNodes(req.ClusterName, req.NodeIDs)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: batch rebooted cluster nodes", "cluster", clusterArn)

	if failures == nil {
		failures = []string{}
	}

	if successful == nil {
		successful = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Failures":    failures,
		"Successful":  successful,
	})
}

// batchReplaceClusterNodesRequest is the request body for BatchReplaceClusterNodes.
type batchReplaceClusterNodesRequest struct {
	ClusterName string               `json:"ClusterName"`
	Nodes       []clusterNodeRequest `json:"Nodes"`
}

func (h *Handler) handleBatchReplaceClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchReplaceClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	return h.batchClusterNodesWithFailures(
		ctx,
		req.ClusterName,
		"sagemaker: batch replaced cluster nodes",
		toClusterNodes(req.Nodes),
		h.Backend.BatchReplaceClusterNodes,
	)
}

// actionSourceRequest is the source for a CreateAction request.
type actionSourceRequest struct {
	SourceURI  string `json:"SourceUri"`
	SourceType string `json:"SourceType,omitempty"`
}

// createActionRequest is the request body for CreateAction.
type createActionRequest struct {
	Properties  map[string]string   `json:"Properties"`
	Source      actionSourceRequest `json:"Source"`
	ActionName  string              `json:"ActionName"`
	ActionType  string              `json:"ActionType"`
	Description string              `json:"Description,omitempty"`
	Status      string              `json:"Status,omitempty"`
	Tags        []tagObject         `json:"Tags"`
}

func (h *Handler) handleCreateAction(ctx context.Context, body []byte) ([]byte, error) {
	var req createActionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ActionName == "" {
		return nil, fmt.Errorf("%w: ActionName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	source := ActionSource{
		SourceURI:  req.Source.SourceURI,
		SourceType: req.Source.SourceType,
	}

	a, err := h.Backend.CreateAction(
		req.ActionName,
		req.ActionType,
		req.Description,
		req.Status,
		source,
		req.Properties,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created action", "name", a.ActionName, "arn", a.ActionArn)

	return json.Marshal(map[string]string{"ActionArn": a.ActionArn})
}

// createAlgorithmRequest is the request body for CreateAlgorithm.
type createAlgorithmRequest struct {
	AlgorithmName        string      `json:"AlgorithmName"`
	AlgorithmDescription string      `json:"AlgorithmDescription,omitempty"`
	Tags                 []tagObject `json:"Tags"`
}

func (h *Handler) handleCreateAlgorithm(ctx context.Context, body []byte) ([]byte, error) {
	var req createAlgorithmRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AlgorithmName == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)

	al, err := h.Backend.CreateAlgorithm(req.AlgorithmName, req.AlgorithmDescription, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created algorithm", "name", al.AlgorithmName, "arn", al.AlgorithmArn)

	return json.Marshal(map[string]string{"AlgorithmArn": al.AlgorithmArn})
}
