package forecast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const forecastTargetPrefix = "AmazonForecast."

type operationMode string

const (
	modeCreate   operationMode = "create"
	modeDescribe operationMode = "describe"
	modeList     operationMode = "list"
	modeDelete   operationMode = "delete"
	modeUpdate   operationMode = "update"

	defaultListPageSize = 100
)

type operationSpec struct {
	kind      resourceKind
	mode      operationMode
	nameField string
	arnField  string
	listField string
}

// Handler serves Amazon Forecast JSON protocol operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]operationSpec
}

// NewHandler creates Forecast HTTP handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend, ops: forecastOperations()}
}

// Name returns service registry name.
func (h *Handler) Name() string { return "Forecast" }

// Reset clears all backend state for the /_gopherstack/reset test hook.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns fault injection service identifier.
func (h *Handler) ChaosServiceName() string { return "forecast" }

// ChaosOperations returns supported fault injection operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns regions handled by instance.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// GetSupportedOperations reports implemented Amazon Forecast operations.
func (h *Handler) GetSupportedOperations() []string {
	result := make([]string, 0, len(h.ops)+1)
	for operation := range h.ops {
		result = append(result, operation)
	}
	result = append(result, "ListMonitorEvaluations")
	result = append(result, "DeleteResourceTree")
	result = append(result, "ResumeResource")
	result = append(result, "StopResource")
	result = append(result, "GetAccuracyMetrics")
	result = append(result, "ListTagsForResource")
	result = append(result, "TagResource")
	result = append(result, "UntagResource")

	return result
}

// RouteMatcher matches Forecast target requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), forecastTargetPrefix)
	}
}

// MatchPriority returns Forecast header match priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts operation from Forecast target.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), forecastTargetPrefix)
}

// ExtractResource returns no generic resource identifier.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c,
			logger.Load(c.Request().Context()),
			h.Name(),
			"application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var input map[string]any
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	if action == "ListMonitorEvaluations" {
		return h.dispatchListMonitorEvaluations(input)
	}
	if action == "DeleteResourceTree" {
		return h.dispatchDeleteResourceTree(input)
	}
	if action == "ResumeResource" {
		return h.dispatchResumeResource(input)
	}
	if action == "StopResource" {
		return h.dispatchStopResource(input)
	}
	if action == "GetAccuracyMetrics" {
		return h.dispatchGetAccuracyMetrics(input)
	}
	if action == "ListTagsForResource" {
		return h.dispatchListTagsForResource(input)
	}
	if action == "TagResource" {
		return h.dispatchTagResource(input)
	}
	if action == "UntagResource" {
		return h.dispatchUntagResource(input)
	}

	spec, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrValidation, action)
	}

	output, err := h.execute(spec, input)
	if err != nil {
		return nil, err
	}

	return json.Marshal(output)
}

func (h *Handler) execute(spec operationSpec, input map[string]any) (map[string]any, error) {
	switch spec.mode {
	case modeCreate:
		resource, err := h.Backend.create(
			spec.kind,
			stringValue(input[spec.nameField]),
			input,
			createFails(spec.kind, input),
		)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.arnField: resource.ARN}, nil
	case modeDescribe:
		resource, err := h.Backend.describe(spec.kind, resourceIdentifier(spec, input))
		if err != nil {
			return nil, err
		}

		return resourceOutput(spec, resource), nil
	case modeUpdate:
		resource, err := h.Backend.update(spec.kind, resourceIdentifier(spec, input), input)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.arnField: resource.ARN}, nil
	case modeDelete:
		if err := h.Backend.delete(spec.kind, resourceIdentifier(spec, input)); err != nil {
			return nil, err
		}

		return map[string]any{}, nil
	case modeList:

		return listOutput(spec, h.Backend.list(spec.kind), input)
	default:

		return nil, fmt.Errorf("%w: unsupported operation mode", ErrValidation)
	}
}

func (h *Handler) dispatchListMonitorEvaluations(input map[string]any) ([]byte, error) {
	evaluations, err := h.Backend.listMonitorEvaluations(stringValue(input["MonitorArn"]))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"PredictorMonitorEvaluations": evaluations})
}

func (h *Handler) dispatchDeleteResourceTree(input map[string]any) ([]byte, error) {
	err := h.Backend.DeleteResourceTree(stringValue(input["ResourceArn"]))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) dispatchResumeResource(input map[string]any) ([]byte, error) {
	err := h.Backend.UpdateResourceStatus(stringValue(input["ResourceArn"]), statusActive)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) dispatchStopResource(input map[string]any) ([]byte, error) {
	err := h.Backend.UpdateResourceStatus(stringValue(input["ResourceArn"]), statusStopped)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) dispatchGetAccuracyMetrics(input map[string]any) ([]byte, error) {
	metrics, err := h.Backend.GetAccuracyMetrics(stringValue(input[fieldPredictorArn]))
	if err != nil {
		return nil, err
	}

	return json.Marshal(metrics)
}

func (h *Handler) dispatchListTagsForResource(input map[string]any) ([]byte, error) {
	tags, err := h.Backend.ListTagsForResource(stringValue(input["ResourceArn"]))
	if err != nil {
		return nil, err
	}
	var tagList []map[string]string
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}

	return json.Marshal(map[string]any{"Tags": tagList})
}

func (h *Handler) dispatchTagResource(input map[string]any) ([]byte, error) {
	tags := make(map[string]string)
	if tagsInput, ok := input["Tags"].([]any); ok {
		for _, tag := range tagsInput {
			if t, okTag := tag.(map[string]any); okTag {
				tags[stringValue(t["Key"])] = stringValue(t["Value"])
			}
		}
	}
	err := h.Backend.TagResource(stringValue(input["ResourceArn"]), tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) dispatchUntagResource(input map[string]any) ([]byte, error) {
	var tagKeys []string
	if keys, ok := input["TagKeys"].([]any); ok {
		for _, k := range keys {
			tagKeys = append(tagKeys, stringValue(k))
		}
	}
	err := h.Backend.UntagResource(stringValue(input["ResourceArn"]), tagKeys)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func resourceIdentifier(spec operationSpec, input map[string]any) string {
	if value := stringValue(input[spec.arnField]); value != "" {
		return value
	}

	return stringValue(input[spec.nameField])
}

func resourceOutput(spec operationSpec, resource *Resource) map[string]any {
	output := cloneMap(resource.Data)
	output[spec.nameField] = resource.Name
	output[spec.arnField] = resource.ARN
	output["Status"] = resource.Status
	output["CreationTime"] = awstime.Epoch(resource.CreatedAt)
	output["LastModificationTime"] = awstime.Epoch(resource.UpdatedAt)

	return output
}

func listOutput(spec operationSpec, resources []*Resource, input map[string]any) (map[string]any, error) {
	maxResults := 0
	if mr, ok := input["MaxResults"].(float64); ok {
		maxResults = int(mr)
	}
	nextToken, _ := input["NextToken"].(string)

	if err := page.ValidateToken(nextToken); err != nil {
		return nil, fmt.Errorf("%w: NextToken %q is not valid", ErrInvalidNextToken, nextToken)
	}

	summaries := make([]map[string]any, 0, len(resources))
	for _, r := range resources {
		summaries = append(summaries, resourceOutput(spec, r))
	}

	pg := page.New(summaries, nextToken, maxResults, defaultListPageSize)
	out := map[string]any{spec.listField: pg.Data}
	if pg.Next != "" {
		out["NextToken"] = pg.Next
	}

	return out, nil
}

func createFails(kind resourceKind, input map[string]any) bool {
	if kind != kindDatasetImportJob {
		return false
	}

	dataSource, ok := input["DataSource"].(map[string]any)
	if !ok {
		return true
	}
	s3Config, ok := dataSource["S3Config"].(map[string]any)
	if !ok {
		return true
	}

	return stringValue(s3Config["Path"]) == ""
}

const forecastContentType = "application/x-amz-json-1.1"

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var code int
	var errType string

	switch {
	case errors.Is(err, ErrNotFound):
		code, errType = http.StatusBadRequest, "ResourceNotFoundException"
	case errors.Is(err, ErrAlreadyExists):
		code, errType = http.StatusBadRequest, "ResourceAlreadyExistsException"
	case errors.Is(err, ErrResourceInUse):
		code, errType = http.StatusBadRequest, "ResourceInUseException"
	case errors.Is(err, ErrInvalidNextToken):
		code, errType = http.StatusBadRequest, "InvalidNextTokenException"
	case errors.Is(err, ErrValidation):
		code, errType = http.StatusBadRequest, "InvalidInputException"
	default:
		code, errType = http.StatusInternalServerError, "InternalServerException"
	}

	payload, marshalErr := json.Marshal(map[string]string{"__type": errType, "message": err.Error()})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", forecastContentType)

	return c.JSONBlob(code, payload)
}

func forecastOperations() map[string]operationSpec {
	operations := make(map[string]operationSpec)
	addCRUD(
		operations,
		"DatasetGroup",
		kindDatasetGroup,
		"DatasetGroupName",
		"DatasetGroupArn",
		"DatasetGroups",
		true,
	)
	// update=false: real Forecast has no UpdateDataset operation (verified against
	// aws-sdk-go-v2/service/forecast.Client: only UpdateDatasetGroup exists among
	// dataset-family Update* methods; datasets are immutable after creation, only
	// re-imported via CreateDatasetImportJob). A prior pass wired this addCRUD call
	// with update=true, which both advertised and dispatched a fabricated
	// "UpdateDataset" operation no real client can send — caught by pkgs/sdkcheck's
	// reverse check (commit 12cfe14d5; gopherstack-vhw2 category A). Unlike some
	// other findings in this campaign, nothing legitimate depended on the route (no
	// test exercised it, PARITY.md's Dataset family note already only claimed
	// Create/Describe/Delete/List), so it is deleted outright rather than kept
	// wired-but-unadvertised.
	addCRUD(operations, "Dataset", kindDataset, "DatasetName", "DatasetArn", "Datasets", false)
	addCRUD(
		operations,
		"DatasetImportJob",
		kindDatasetImportJob,
		"DatasetImportJobName",
		"DatasetImportJobArn",
		"DatasetImportJobs",
		false,
	)
	addCRUD(operations, "Predictor", kindPredictor, "PredictorName", fieldPredictorArn, "Predictors", false)
	addCRUD(
		operations,
		"PredictorBacktestExportJob",
		kindPredictorBacktestExport,
		"PredictorBacktestExportJobName",
		"PredictorBacktestExportJobArn",
		"PredictorBacktestExportJobs",
		false,
	)
	addCRUD(operations, "Forecast", kindForecast, "ForecastName", "ForecastArn", "Forecasts", false)
	addCRUD(
		operations,
		"ForecastExportJob",
		kindForecastExport,
		"ForecastExportJobName",
		"ForecastExportJobArn",
		"ForecastExportJobs",
		false,
	)
	addCRUD(
		operations,
		"ExplainabilityExport",
		kindExplainabilityExport,
		"ExplainabilityExportName",
		"ExplainabilityExportArn",
		"ExplainabilityExports",
		false,
	)
	addCRUD(
		operations,
		"WhatIfAnalysis",
		kindWhatIfAnalysis,
		"WhatIfAnalysisName",
		"WhatIfAnalysisArn",
		"WhatIfAnalyses",
		false,
	)
	addCRUD(
		operations,
		"WhatIfForecast",
		kindWhatIfForecast,
		"WhatIfForecastName",
		"WhatIfForecastArn",
		"WhatIfForecasts",
		false,
	)
	addCRUD(
		operations,
		"WhatIfForecastExport",
		kindWhatIfForecastExport,
		"WhatIfForecastExportName",
		"WhatIfForecastExportArn",
		"WhatIfForecastExports",
		false,
	)
	addCRUD(operations, "Monitor", kindMonitor, "MonitorName", "MonitorArn", "Monitors", false)
	addCRUD(
		operations,
		"Explainability",
		kindExplainability,
		"ExplainabilityName",
		"ExplainabilityArn",
		"Explainabilities",
		false,
	)
	operations["CreateAutoPredictor"] = operationSpec{
		kind: kindPredictor, mode: modeCreate, nameField: "PredictorName",
		arnField: fieldPredictorArn, listField: "Predictors",
	}
	operations["DescribeAutoPredictor"] = operationSpec{
		kind: kindPredictor, mode: modeDescribe, nameField: "PredictorName",
		arnField: fieldPredictorArn, listField: "Predictors",
	}

	return operations
}

func addCRUD(
	operations map[string]operationSpec,
	base string,
	kind resourceKind,
	nameField string,
	arnField string,
	listField string,
	update bool,
) {
	spec := operationSpec{
		kind: kind, nameField: nameField, arnField: arnField, listField: listField,
	}
	operations["Create"+base] = withMode(spec, modeCreate)
	operations["Describe"+base] = withMode(spec, modeDescribe)
	operations["List"+plural(base)] = withMode(spec, modeList)
	operations["Delete"+base] = withMode(spec, modeDelete)
	if update {
		operations["Update"+base] = withMode(spec, modeUpdate)
	}
}

func withMode(spec operationSpec, mode operationMode) operationSpec {
	spec.mode = mode

	return spec
}

func plural(base string) string {
	switch base {
	case "WhatIfAnalysis":

		return "WhatIfAnalyses"
	case "Explainability":

		return "Explainabilities"
	default:

		return base + "s"
	}
}
