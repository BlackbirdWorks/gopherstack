package forecast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
)

type operationSpec struct {
	kind        resourceKind
	mode        operationMode
	nameField   string
	arnField    string
	statusField string
	listField   string
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
		return listOutput(spec, h.Backend.list(spec.kind)), nil
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
	output[spec.statusField] = resource.Status
	output["CreationTime"] = resource.CreatedAt
	output["LastModificationTime"] = resource.UpdatedAt

	return output
}

func listOutput(spec operationSpec, resources []*Resource) map[string]any {
	summaries := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		summaries = append(summaries, resourceOutput(spec, resource))
	}

	return map[string]any{spec.listField: summaries}
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

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorPayload("ResourceNotFoundException", err))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorPayload("ResourceAlreadyExistsException", err))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorPayload("InvalidInputException", err))
	default:
		return c.JSON(http.StatusInternalServerError, errorPayload("InternalServerException", err))
	}
}

func errorPayload(errorType string, err error) map[string]string {
	return map[string]string{"__type": errorType, "message": err.Error()}
}

func forecastOperations() map[string]operationSpec {
	operations := make(map[string]operationSpec)
	addCRUD(
		operations,
		"DatasetGroup",
		kindDatasetGroup,
		"DatasetGroupName",
		"DatasetGroupArn",
		"Status",
		"DatasetGroups",
		true,
	)
	addCRUD(operations, "Dataset", kindDataset, "DatasetName", "DatasetArn", "Status", "Datasets", true)
	addCRUD(
		operations,
		"DatasetImportJob",
		kindDatasetImportJob,
		"DatasetImportJobName",
		"DatasetImportJobArn",
		"Status",
		"DatasetImportJobs",
		false,
	)
	addCRUD(operations, "Predictor", kindPredictor, "PredictorName", "PredictorArn", "Status", "Predictors", false)
	addCRUD(
		operations,
		"PredictorBacktestExportJob",
		kindPredictorBacktestExport,
		"PredictorBacktestExportJobName",
		"PredictorBacktestExportJobArn",
		"Status",
		"PredictorBacktestExportJobs",
		false,
	)
	addCRUD(operations, "Forecast", kindForecast, "ForecastName", "ForecastArn", "Status", "Forecasts", false)
	addCRUD(
		operations,
		"ForecastExportJob",
		kindForecastExport,
		"ForecastExportJobName",
		"ForecastExportJobArn",
		"Status",
		"ForecastExportJobs",
		false,
	)
	addCRUD(
		operations,
		"ExplainabilityExport",
		kindExplainabilityExport,
		"ExplainabilityExportName",
		"ExplainabilityExportArn",
		"Status",
		"ExplainabilityExports",
		false,
	)
	addCRUD(
		operations,
		"WhatIfAnalysis",
		kindWhatIfAnalysis,
		"WhatIfAnalysisName",
		"WhatIfAnalysisArn",
		"Status",
		"WhatIfAnalyses",
		false,
	)
	addCRUD(
		operations,
		"WhatIfForecast",
		kindWhatIfForecast,
		"WhatIfForecastName",
		"WhatIfForecastArn",
		"Status",
		"WhatIfForecasts",
		false,
	)
	addCRUD(
		operations,
		"WhatIfForecastExport",
		kindWhatIfForecastExport,
		"WhatIfForecastExportName",
		"WhatIfForecastExportArn",
		"Status",
		"WhatIfForecastExports",
		false,
	)
	addCRUD(operations, "Monitor", kindMonitor, "MonitorName", "MonitorArn", "Status", "Monitors", false)
	operations["CreateAutoPredictor"] = operationSpec{
		kind: kindPredictor, mode: modeCreate, nameField: "PredictorName",
		arnField: "PredictorArn", statusField: "Status", listField: "Predictors",
	}

	return operations
}

func addCRUD(
	operations map[string]operationSpec,
	base string,
	kind resourceKind,
	nameField string,
	arnField string,
	statusField string,
	listField string,
	update bool,
) {
	spec := operationSpec{
		kind: kind, nameField: nameField, arnField: arnField, statusField: statusField, listField: listField,
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
	default:
		return base + "s"
	}
}
