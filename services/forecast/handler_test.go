package forecast_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/forecast"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newHandler() *forecast.Handler {
	return forecast.NewHandler(forecast.NewInMemoryBackend(testAccountID, testRegion))
}

func request(t *testing.T, h *forecast.Handler, operation string, body map[string]any) (int, map[string]any) {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonForecast."+operation)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(ctx))

	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))

	return rec.Code, response
}

func TestHandler_MetadataAndRouting(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "Forecast", h.Name())
	assert.Equal(t, "forecast", h.ChaosServiceName())
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
	assert.Contains(t, h.GetSupportedOperations(), "CreateAutoPredictor")
	assert.Contains(t, h.GetSupportedOperations(), "ListMonitorEvaluations")

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonForecast.CreateDataset")
	ctx := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(ctx))
	assert.Equal(t, "CreateDataset", h.ExtractOperation(ctx))
}

func TestHandler_ResourceLifecycles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		name       string
		create     string
		describe   string
		list       string
		delete     string
		arnField   string
		status     string
		listField  string
	}{
		{
			name: "dataset_group", create: "CreateDatasetGroup", describe: "DescribeDatasetGroup",
			list: "ListDatasetGroups", delete: "DeleteDatasetGroup", arnField: "DatasetGroupArn",
			status: "Status", listField: "DatasetGroups",
			createBody: map[string]any{"DatasetGroupName": "sales-group", "Domain": "RETAIL"},
		},
		{
			name: "dataset", create: "CreateDataset", describe: "DescribeDataset",
			list: "ListDatasets", delete: "DeleteDataset", arnField: "DatasetArn",
			status: "Status", listField: "Datasets",
			createBody: map[string]any{
				"DatasetName": "sales", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
				"DataFrequency": "D",
			},
		},
		{
			name: "dataset_import_job", create: "CreateDatasetImportJob", describe: "DescribeDatasetImportJob",
			list: "ListDatasetImportJobs", delete: "DeleteDatasetImportJob", arnField: "DatasetImportJobArn",
			status: "Status", listField: "DatasetImportJobs",
			createBody: map[string]any{
				"DatasetImportJobName": "import", "DatasetArn": "arn:aws:forecast:us-east-1:000000000000:dataset/sales",
				"DataSource": map[string]any{"S3Config": map[string]any{"Path": "s3://bucket/train.csv"}},
			},
		},
		{
			name: "predictor", create: "CreatePredictor", describe: "DescribePredictor",
			list: "ListPredictors", delete: "DeletePredictor", arnField: "PredictorArn",
			status: "Status", listField: "Predictors",
			createBody: map[string]any{"PredictorName": "daily", "ForecastHorizon": 14, "PerformAutoML": true},
		},
		{
			name: "predictor_backtest_export", create: "CreatePredictorBacktestExportJob",
			describe: "DescribePredictorBacktestExportJob", list: "ListPredictorBacktestExportJobs",
			delete: "DeletePredictorBacktestExportJob", arnField: "PredictorBacktestExportJobArn",
			status: "Status", listField: "PredictorBacktestExportJobs",
			createBody: map[string]any{"PredictorBacktestExportJobName": "backtest", "PredictorArn": "predictor"},
		},
		{
			name: "forecast", create: "CreateForecast", describe: "DescribeForecast",
			list: "ListForecasts", delete: "DeleteForecast", arnField: "ForecastArn",
			status: "Status", listField: "Forecasts",
			createBody: map[string]any{"ForecastName": "supply", "PredictorArn": "predictor"},
		},
		{
			name: "forecast_export", create: "CreateForecastExportJob", describe: "DescribeForecastExportJob",
			list: "ListForecastExportJobs", delete: "DeleteForecastExportJob", arnField: "ForecastExportJobArn",
			status: "Status", listField: "ForecastExportJobs",
			createBody: map[string]any{"ForecastExportJobName": "forecast-export", "ForecastArn": "forecast"},
		},
		{
			name: "explainability_export", create: "CreateExplainabilityExport",
			describe: "DescribeExplainabilityExport", list: "ListExplainabilityExports",
			delete: "DeleteExplainabilityExport", arnField: "ExplainabilityExportArn",
			status: "Status", listField: "ExplainabilityExports",
			createBody: map[string]any{"ExplainabilityExportName": "explain-export", "ExplainabilityArn": "explain"},
		},
		{
			name: "explainability", create: "CreateExplainability",
			describe: "DescribeExplainability", list: "ListExplainabilities",
			delete: "DeleteExplainability", arnField: "ExplainabilityArn",
			status: "Status", listField: "Explainabilities",
			createBody: map[string]any{"ExplainabilityName": "forecast-explain", "ResourceArn": "predictor"},
		},
		{
			name: "what_if_analysis", create: "CreateWhatIfAnalysis", describe: "DescribeWhatIfAnalysis",
			list: "ListWhatIfAnalyses", delete: "DeleteWhatIfAnalysis", arnField: "WhatIfAnalysisArn",
			status: "Status", listField: "WhatIfAnalyses",
			createBody: map[string]any{"WhatIfAnalysisName": "promo-analysis", "ForecastArn": "forecast"},
		},
		{
			name: "what_if_forecast", create: "CreateWhatIfForecast", describe: "DescribeWhatIfForecast",
			list: "ListWhatIfForecasts", delete: "DeleteWhatIfForecast", arnField: "WhatIfForecastArn",
			status: "Status", listField: "WhatIfForecasts",
			createBody: map[string]any{"WhatIfForecastName": "promo-forecast", "WhatIfAnalysisArn": "analysis"},
		},
		{
			name: "what_if_forecast_export", create: "CreateWhatIfForecastExport",
			describe: "DescribeWhatIfForecastExport", list: "ListWhatIfForecastExports",
			delete: "DeleteWhatIfForecastExport", arnField: "WhatIfForecastExportArn",
			status: "Status", listField: "WhatIfForecastExports",
			createBody: map[string]any{"WhatIfForecastExportName": "promo-export", "WhatIfAnalysisArn": "analysis"},
		},
		{
			name: "monitor", create: "CreateMonitor", describe: "DescribeMonitor",
			list: "ListMonitors", delete: "DeleteMonitor", arnField: "MonitorArn",
			status: "Status", listField: "Monitors",
			createBody: map[string]any{"MonitorName": "quality-monitor", "ResourceArn": "predictor"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, created := request(t, h, tt.create, tt.createBody)
			require.Equal(t, http.StatusOK, code)
			resourceARN, ok := created[tt.arnField].(string)
			require.True(t, ok)
			assert.Contains(t, resourceARN, "arn:aws:forecast:")

			code, pending := request(t, h, tt.describe, map[string]any{tt.arnField: resourceARN})
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, "CREATE_PENDING", pending[tt.status])

			_, active := request(t, h, tt.describe, map[string]any{tt.arnField: resourceARN})
			assert.Equal(t, "ACTIVE", active[tt.status])

			_, listed := request(t, h, tt.list, map[string]any{})
			resources, ok := listed[tt.listField].([]any)
			require.True(t, ok)
			require.Len(t, resources, 1)

			code, _ = request(t, h, tt.delete, map[string]any{tt.arnField: resourceARN})
			require.Equal(t, http.StatusOK, code)

			// After delete, resource is removed; describe returns ResourceNotFoundException.
			code, notFound := request(t, h, tt.describe, map[string]any{tt.arnField: resourceARN})
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceNotFoundException", notFound["__type"])

			// After delete, list returns empty.
			_, afterDelete := request(t, h, tt.list, map[string]any{})
			afterResources, _ := afterDelete[tt.listField].([]any)
			assert.Empty(t, afterResources)
		})
	}
}

func TestHandler_DatasetImportCreateFailure(t *testing.T) {
	t.Parallel()

	h := newHandler()
	_, created := request(t, h, "CreateDatasetImportJob", map[string]any{
		"DatasetImportJobName": "broken-import",
		"DatasetArn":           "dataset",
		"DataSource":           map[string]any{"S3Config": map[string]any{}},
	})
	_, described := request(t, h, "DescribeDatasetImportJob", map[string]any{
		"DatasetImportJobArn": created["DatasetImportJobArn"],
	})
	assert.Equal(t, "CREATE_FAILED", described["Status"])
}

func TestHandler_ConfigurationRetentionAndUpdates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	schema := map[string]any{"Attributes": []any{
		map[string]any{"AttributeName": "item_id", "AttributeType": "string"},
		map[string]any{"AttributeName": "demand", "AttributeType": "float"},
	}}
	_, createdDataset := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "schema-dataset", "Schema": schema, "DataFrequency": "D",
	})
	_, dataset := request(t, h, "DescribeDataset", map[string]any{"DatasetArn": createdDataset["DatasetArn"]})
	assert.Equal(t, schema, dataset["Schema"])

	config := map[string]any{
		"PredictorName": "automl-predictor", "PerformAutoML": true, "PerformHPO": true,
		"HyperParameterTuningJobConfig": map[string]any{
			"ParameterRanges": map[string]any{"ContinuousParameterRanges": []any{
				map[string]any{"Name": "learning_rate", "MinValue": 0.01, "MaxValue": 0.2},
			}},
		},
	}
	_, createdPredictor := request(t, h, "CreateAutoPredictor", config)
	_, predictor := request(t, h, "DescribePredictor", map[string]any{"PredictorArn": createdPredictor["PredictorArn"]})
	assert.Equal(t, true, predictor["PerformAutoML"])
	assert.Equal(t, true, predictor["PerformHPO"])
	assert.Equal(t, config["HyperParameterTuningJobConfig"], predictor["HyperParameterTuningJobConfig"])

	_, createdGroup := request(t, h, "CreateDatasetGroup", map[string]any{"DatasetGroupName": "group"})
	_, updated := request(t, h, "UpdateDatasetGroup", map[string]any{
		"DatasetGroupArn": createdGroup["DatasetGroupArn"], "DatasetArns": []any{"dataset-a", "dataset-b"},
	})
	assert.NotEmpty(t, updated["DatasetGroupArn"])
	_, group := request(
		t,
		h,
		"DescribeDatasetGroup",
		map[string]any{"DatasetGroupArn": createdGroup["DatasetGroupArn"]},
	)
	assert.Equal(t, []any{"dataset-a", "dataset-b"}, group["DatasetArns"])
}

func TestHandler_MonitorEvaluationsAndErrors(t *testing.T) {
	t.Parallel()

	h := newHandler()
	_, created := request(t, h, "CreateMonitor", map[string]any{
		"MonitorName": "monitor-evaluations", "ResourceArn": "arn:aws:forecast:us-east-1:000000000000:predictor/p1",
	})
	code, response := request(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": created["MonitorArn"]})
	require.Equal(t, http.StatusOK, code)
	evaluations, ok := response["PredictorMonitorEvaluations"].([]any)
	require.True(t, ok)
	require.Len(t, evaluations, 1)
	assert.Equal(t, "SUCCESS", evaluations[0].(map[string]any)["EvaluationState"])

	code, response = request(t, h, "DescribeForecast", map[string]any{"ForecastArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", response["__type"])
}

// TestHandler_TagOperations_UnknownResourceNotFound verifies that TagResource,
// UntagResource, and ListTagsForResource return ResourceNotFoundException for
// an ARN that was never created, matching the real Amazon Forecast API (which
// models ResourceNotFoundException on all three operations). Previously these
// operations accepted any ARN and silently wrote/read an orphaned tag map
// entry instead of validating the resource exists.
func TestHandler_TagOperations_UnknownResourceNotFound(t *testing.T) {
	t.Parallel()

	unknownARN := "arn:aws:forecast:us-east-1:000000000000:dataset-group/never-created"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "tag_resource",
			action: "TagResource",
			body: map[string]any{
				"ResourceArn": unknownARN,
				"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
			},
		},
		{
			name:   "untag_resource",
			action: "UntagResource",
			body:   map[string]any{"ResourceArn": unknownARN, "TagKeys": []any{"env"}},
		},
		{
			name:   "list_tags_for_resource",
			action: "ListTagsForResource",
			body:   map[string]any{"ResourceArn": unknownARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestHandler_ListOperations_InvalidNextToken verifies that a malformed
// NextToken on a List* operation returns InvalidNextTokenException, matching
// the real Amazon Forecast API (which models InvalidNextTokenException on
// every List operation). Previously a malformed token silently decoded to 0
// and restarted pagination from the beginning instead of erroring.
func TestHandler_ListOperations_InvalidNextToken(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, resp := request(t, h, "ListDatasetGroups", map[string]any{"NextToken": "not-valid-base64!!"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidNextTokenException", resp["__type"])
}

func TestProvider(t *testing.T) {
	t.Parallel()

	p := &forecast.Provider{}
	assert.Equal(t, "Forecast", p.Name())
	_, err := p.Init(nil)
	require.ErrorIs(t, err, forecast.ErrNilAppContext)
	handler, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.Equal(t, "Forecast", handler.Name())
}
