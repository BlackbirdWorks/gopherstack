package forecast_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// --- shared test helpers ---

func newHandler() *forecast.Handler {
	return forecast.NewHandler(forecast.NewInMemoryBackend(testAccountID, testRegion))
}

func request(t *testing.T, h *forecast.Handler, operation string, body map[string]any) (int, map[string]any) {
	t.Helper()

	rec := doRequest(t, h, operation, body)

	return rec.Code, unmarshalResponse(t, rec)
}

// doRequest performs a raw request and returns the recorder, for tests that
// need to inspect headers or the status code independently of the decoded body.
func doRequest(t *testing.T, h *forecast.Handler, operation string, body map[string]any) *httptest.ResponseRecorder {
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

	return rec
}

func unmarshalResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))

	return response
}

// --- FK-chain builders ---
//
// CreateForecast/CreateMonitor/CreateDatasetImportJob/etc. now validate that
// their ARN-reference fields (PredictorArn, DatasetArn, ...) resolve to a
// real resource (ResourceNotFoundException otherwise, matching real Amazon
// Forecast). These helpers build the minimal prerequisite chain so tests can
// ask for "a real PredictorArn" etc. without repeating the CreatePredictor
// boilerplate at every call site.

// createPredictor creates a Predictor on h and returns its ARN.
func createPredictor(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	code, created := request(t, h, "CreatePredictor", minimalCreatePredictorBody(t, h, "fk-predictor"))
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["PredictorArn"].(string)
	require.True(t, ok)

	return arn
}

// createDatasetGroup creates a DatasetGroup on h and returns its ARN, for
// tests that only need a real DatasetGroupArn to satisfy CreatePredictor's
// InputDataConfig.DatasetGroupArn requirement.
func createDatasetGroup(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	code, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "fk-dataset-group", "Domain": "RETAIL",
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["DatasetGroupArn"].(string)
	require.True(t, ok)

	return arn
}

// minimalCreatePredictorBody returns a CreatePredictorInput body carrying
// every field aws-sdk-go-v2/service/forecast@v1.44.4/validators.go's
// validateOpCreatePredictorInput marks required (PredictorName,
// ForecastHorizon, InputDataConfig, FeaturizationConfig) -- including
// InputDataConfig.DatasetGroupArn, which validateInputDataConfig also marks
// required, so this creates a real DatasetGroup to reference -- for tests
// that only care about a resource existing rather than exercising these
// fields directly.
func minimalCreatePredictorBody(t *testing.T, h *forecast.Handler, name string) map[string]any {
	t.Helper()

	return map[string]any{
		"PredictorName":       name,
		"ForecastHorizon":     10,
		"InputDataConfig":     map[string]any{"DatasetGroupArn": createDatasetGroup(t, h)},
		"FeaturizationConfig": map[string]any{},
	}
}

// minimalDataDestination returns a DataDestination body for tests that only
// need CreateForecastExportJob/CreatePredictorBacktestExportJob/
// CreateExplainabilityExport/CreateWhatIfForecastExport's required
// Destination field present, not deeply validated.
func minimalDataDestination() map[string]any {
	return map[string]any{
		"S3Config": map[string]any{
			"Path":    "s3://fk-bucket/export",
			"RoleArn": "arn:aws:iam::000000000000:role/forecast-export",
		},
	}
}

// createDataset creates a Dataset on h and returns its ARN.
func createDataset(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	code, created := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "fk-dataset", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
		"DataFrequency": "D", "Schema": map[string]any{"Attributes": []any{}},
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["DatasetArn"].(string)
	require.True(t, ok)

	return arn
}

// createForecast creates a Predictor and a Forecast referencing it on h, and
// returns the Forecast's ARN.
func createForecast(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	predictorARN := createPredictor(t, h)
	code, created := request(t, h, "CreateForecast", map[string]any{
		"ForecastName": "fk-forecast", "PredictorArn": predictorARN,
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["ForecastArn"].(string)
	require.True(t, ok)

	return arn
}

// createExplainability creates a Predictor and an Explainability resource
// referencing it on h, and returns the Explainability's ARN.
func createExplainability(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	predictorARN := createPredictor(t, h)
	code, created := request(t, h, "CreateExplainability", map[string]any{
		"ExplainabilityName": "fk-explainability", "ResourceArn": predictorARN,
		"ExplainabilityConfig": map[string]any{
			"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL",
		},
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["ExplainabilityArn"].(string)
	require.True(t, ok)

	return arn
}

// createWhatIfAnalysis creates the Predictor->Forecast->WhatIfAnalysis chain
// on h, and returns the WhatIfAnalysis's ARN.
func createWhatIfAnalysis(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	forecastARN := createForecast(t, h)
	code, created := request(t, h, "CreateWhatIfAnalysis", map[string]any{
		"WhatIfAnalysisName": "fk-what-if-analysis", "ForecastArn": forecastARN,
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["WhatIfAnalysisArn"].(string)
	require.True(t, ok)

	return arn
}

// createWhatIfForecast creates the Predictor->Forecast->WhatIfAnalysis->
// WhatIfForecast chain on h, and returns the WhatIfForecast's ARN.
func createWhatIfForecast(t *testing.T, h *forecast.Handler) string {
	t.Helper()

	analysisARN := createWhatIfAnalysis(t, h)
	code, created := request(t, h, "CreateWhatIfForecast", map[string]any{
		"WhatIfForecastName": "fk-what-if-forecast", "WhatIfAnalysisArn": analysisARN,
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["WhatIfForecastArn"].(string)
	require.True(t, ok)

	return arn
}

// --- routing and metadata ---

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

// --- protocol accuracy ---

func TestHandler_Protocol_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantCT     string
		wantStatus int
	}{
		{
			name:       "success_has_json11_content_type",
			action:     "CreateDatasetGroup",
			body:       map[string]any{"DatasetGroupName": "ct-group", "Domain": "RETAIL"},
			wantStatus: http.StatusOK,
			wantCT:     "application/x-amz-json-1.1",
		},
		{
			name:       "error_has_json11_content_type",
			action:     "DescribeForecast",
			body:       map[string]any{"ForecastArn": "arn:aws:forecast:us-east-1:000000000000:forecast/missing"},
			wantStatus: http.StatusBadRequest,
			wantCT:     "application/x-amz-json-1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestHandler_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "not_found_resource",
			action: "DescribeForecast",
			body:   map[string]any{"ForecastArn": "arn:aws:forecast:us-east-1:000000000000:forecast/missing"},
		},
		{
			name:   "missing_name_invalid_input",
			action: "CreateDatasetGroup",
			body:   map[string]any{"DatasetGroupName": "", "Domain": "RETAIL"},
		},
		{
			name:   "unknown_operation",
			action: "NoSuchOperation",
			body:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			m := unmarshalResponse(t, rec)
			assert.NotEmpty(t, m["__type"], "__type must be present in error envelope")
			assert.NotEmpty(t, m["message"], "message must be present in error envelope")
		})
	}
}

func TestHandler_Protocol_NonPost(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/some-path", nil)
	req.Header.Set("X-Amz-Target", "AmazonForecast.ListForecasts")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	h := newHandler()
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_Protocol_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	h := newHandler()
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- shared CRUD engine (spans every resource kind) ---

func TestHandler_ResourceLifecycles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody func(t *testing.T, h *forecast.Handler) map[string]any
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
			createBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{"DatasetGroupName": "sales-group", "Domain": "RETAIL"}
			},
		},
		{
			name: "dataset", create: "CreateDataset", describe: "DescribeDataset",
			list: "ListDatasets", delete: "DeleteDataset", arnField: "DatasetArn",
			status: "Status", listField: "Datasets",
			createBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"DatasetName": "sales", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
					"DataFrequency": "D", "Schema": map[string]any{"Attributes": []any{}},
				}
			},
		},
		{
			name: "dataset_import_job", create: "CreateDatasetImportJob", describe: "DescribeDatasetImportJob",
			list: "ListDatasetImportJobs", delete: "DeleteDatasetImportJob", arnField: "DatasetImportJobArn",
			status: "Status", listField: "DatasetImportJobs",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"DatasetImportJobName": "import", "DatasetArn": createDataset(t, h),
					"DataSource": map[string]any{"S3Config": map[string]any{"Path": "s3://bucket/train.csv"}},
				}
			},
		},
		{
			name: "predictor", create: "CreatePredictor", describe: "DescribePredictor",
			list: "ListPredictors", delete: "DeletePredictor", arnField: "PredictorArn",
			status: "Status", listField: "Predictors",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				body := minimalCreatePredictorBody(t, h, "daily")
				body["PerformAutoML"] = true

				return body
			},
		},
		{
			name: "predictor_backtest_export", create: "CreatePredictorBacktestExportJob",
			describe: "DescribePredictorBacktestExportJob", list: "ListPredictorBacktestExportJobs",
			delete: "DeletePredictorBacktestExportJob", arnField: "PredictorBacktestExportJobArn",
			status: "Status", listField: "PredictorBacktestExportJobs",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"PredictorBacktestExportJobName": "backtest", "PredictorArn": createPredictor(t, h),
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name: "forecast", create: "CreateForecast", describe: "DescribeForecast",
			list: "ListForecasts", delete: "DeleteForecast", arnField: "ForecastArn",
			status: "Status", listField: "Forecasts",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{"ForecastName": "supply", "PredictorArn": createPredictor(t, h)}
			},
		},
		{
			name: "forecast_export", create: "CreateForecastExportJob", describe: "DescribeForecastExportJob",
			list: "ListForecastExportJobs", delete: "DeleteForecastExportJob", arnField: "ForecastExportJobArn",
			status: "Status", listField: "ForecastExportJobs",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ForecastExportJobName": "forecast-export", "ForecastArn": createForecast(t, h),
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name: "explainability_export", create: "CreateExplainabilityExport",
			describe: "DescribeExplainabilityExport", list: "ListExplainabilityExports",
			delete: "DeleteExplainabilityExport", arnField: "ExplainabilityExportArn",
			status: "Status", listField: "ExplainabilityExports",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ExplainabilityExportName": "explain-export", "ExplainabilityArn": createExplainability(t, h),
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name: "explainability", create: "CreateExplainability",
			describe: "DescribeExplainability", list: "ListExplainabilities",
			delete: "DeleteExplainability", arnField: "ExplainabilityArn",
			status: "Status", listField: "Explainabilities",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ExplainabilityName": "forecast-explain", "ResourceArn": createPredictor(t, h),
					"ExplainabilityConfig": map[string]any{
						"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL",
					},
				}
			},
		},
		{
			name: "what_if_analysis", create: "CreateWhatIfAnalysis", describe: "DescribeWhatIfAnalysis",
			list: "ListWhatIfAnalyses", delete: "DeleteWhatIfAnalysis", arnField: "WhatIfAnalysisArn",
			status: "Status", listField: "WhatIfAnalyses",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{"WhatIfAnalysisName": "promo-analysis", "ForecastArn": createForecast(t, h)}
			},
		},
		{
			name: "what_if_forecast", create: "CreateWhatIfForecast", describe: "DescribeWhatIfForecast",
			list: "ListWhatIfForecasts", delete: "DeleteWhatIfForecast", arnField: "WhatIfForecastArn",
			status: "Status", listField: "WhatIfForecasts",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"WhatIfForecastName": "promo-forecast", "WhatIfAnalysisArn": createWhatIfAnalysis(t, h),
				}
			},
		},
		{
			name: "what_if_forecast_export", create: "CreateWhatIfForecastExport",
			describe: "DescribeWhatIfForecastExport", list: "ListWhatIfForecastExports",
			delete: "DeleteWhatIfForecastExport", arnField: "WhatIfForecastExportArn",
			status: "Status", listField: "WhatIfForecastExports",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"WhatIfForecastExportName": "promo-export",
					"WhatIfForecastArns":       []any{createWhatIfForecast(t, h)},
					"Destination":              minimalDataDestination(),
				}
			},
		},
		{
			name: "monitor", create: "CreateMonitor", describe: "DescribeMonitor",
			list: "ListMonitors", delete: "DeleteMonitor", arnField: "MonitorArn",
			status: "Status", listField: "Monitors",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{"MonitorName": "quality-monitor", "ResourceArn": createPredictor(t, h)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, created := request(t, h, tt.create, tt.createBody(t, h))
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

func TestHandler_ConfigurationRetentionAndUpdates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	schema := map[string]any{"Attributes": []any{
		map[string]any{"AttributeName": "item_id", "AttributeType": "string"},
		map[string]any{"AttributeName": "demand", "AttributeType": "float"},
	}}
	_, createdDataset := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "schema-dataset", "Schema": schema, "DataFrequency": "D",
		"Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
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

	_, createdGroup := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "group", "Domain": "RETAIL",
	})
	datasetSchema := map[string]any{"Attributes": []any{
		map[string]any{"AttributeName": "item_id", "AttributeType": "string"},
	}}
	_, datasetA := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "dataset-a", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES", "Schema": datasetSchema,
	})
	_, datasetB := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "dataset-b", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES", "Schema": datasetSchema,
	})
	datasetArns := []any{datasetA["DatasetArn"], datasetB["DatasetArn"]}
	_, updated := request(t, h, "UpdateDatasetGroup", map[string]any{
		"DatasetGroupArn": createdGroup["DatasetGroupArn"], "DatasetArns": datasetArns,
	})
	assert.NotEmpty(t, updated["DatasetGroupArn"])
	_, group := request(
		t,
		h,
		"DescribeDatasetGroup",
		map[string]any{"DatasetGroupArn": createdGroup["DatasetGroupArn"]},
	)
	assert.Equal(t, datasetArns, group["DatasetArns"])
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

// TestHandler_ErrorPaths verifies error-envelope shape (__type + message) for
// the three error classes Forecast Create/Describe operations can surface.
func TestHandler_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:     "not_found_forecast",
			action:   "DescribeForecast",
			body:     map[string]any{"ForecastArn": "arn:aws:forecast:us-east-1:000000000000:forecast/missing"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "already_exists_dataset_group",
			action:   "CreateDatasetGroup",
			body:     map[string]any{"DatasetGroupName": "dup-group", "Domain": "RETAIL"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceAlreadyExistsException",
		},
		{
			name:     "invalid_input_empty_name",
			action:   "CreateForecast",
			body:     map[string]any{"ForecastName": "", "PredictorArn": "predictor"},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantType == "ResourceAlreadyExistsException" {
				// Pre-create to trigger conflict
				code, _ := request(t, h, tt.action, tt.body)
				require.Equal(t, http.StatusOK, code)
			}

			code, m := request(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantType, m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
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
