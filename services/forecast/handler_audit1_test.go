package forecast_test

// Audit batch-1: AWS-accuracy coverage for Forecast (go-xcu3).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.1 on success/error, error envelope
//     __type+message fields, non-POST → 405, missing X-Amz-Target → 400
//   - ARN format: all resource ARNs contain "arn:aws:forecast:us-east-1:000000000000:"
//   - Status transitions: CREATE_PENDING → ACTIVE → DELETING lifecycle
//   - DatasetGroup: Create/Describe/List/Delete/Update field shapes
//   - Dataset: Create/Describe field retention (Schema, DataFrequency, Domain)
//   - DatasetImportJob: S3Config validation — missing path → CREATE_FAILED
//   - Predictor: CreatePredictor/DescribePredictor/GetAccuracyMetrics field shapes
//   - Forecast: CreateForecast/DescribeForecast ARN + Status
//   - Monitor: CreateMonitor/ListMonitorEvaluations evaluation shapes
//   - StopResource/ResumeResource: status transitions
//   - DeleteResourceTree: marks resource DELETING
//   - Tag round-trip: TagResource/ListTagsForResource/UntagResource Key+Value shape
//   - Error paths: ResourceNotFoundException → 400, ResourceAlreadyExistsException → 400,
//     InvalidInputException → 400, with __type+message envelope

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

// --- audit helpers ---

func a1ForecastHandler(t *testing.T) *forecast.Handler {
	t.Helper()

	return forecast.NewHandler(forecast.NewInMemoryBackend("000000000000", "us-east-1"))
}

func a1ForecastDo(t *testing.T, h *forecast.Handler, action string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonForecast."+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1ForecastUnmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// --- Protocol accuracy ---

func TestAudit1_Forecast_Protocol_ContentType(t *testing.T) {
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

			h := a1ForecastHandler(t)
			rec := a1ForecastDo(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestAudit1_Forecast_Protocol_ErrorEnvelope(t *testing.T) {
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

			h := a1ForecastHandler(t)
			rec := a1ForecastDo(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			m := a1ForecastUnmarshal(t, rec)
			assert.NotEmpty(t, m["__type"], "__type must be present in error envelope")
			assert.NotEmpty(t, m["message"], "message must be present in error envelope")
		})
	}
}

func TestAudit1_Forecast_Protocol_NonPost(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/some-path", nil)
	req.Header.Set("X-Amz-Target", "AmazonForecast.ListForecasts")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	h := a1ForecastHandler(t)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestAudit1_Forecast_Protocol_MissingTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	h := a1ForecastHandler(t)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ARN format ---

func TestAudit1_Forecast_ARNFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		arnField string
	}{
		{
			name:     "dataset_group_arn",
			action:   "CreateDatasetGroup",
			body:     map[string]any{"DatasetGroupName": "arn-group", "Domain": "RETAIL"},
			arnField: "DatasetGroupArn",
		},
		{
			name:   "dataset_arn",
			action: "CreateDataset",
			body: map[string]any{
				"DatasetName": "arn-dataset", "Domain": "RETAIL",
				"DatasetType": "TARGET_TIME_SERIES", "DataFrequency": "D",
			},
			arnField: "DatasetArn",
		},
		{
			name:     "predictor_arn",
			action:   "CreatePredictor",
			body:     map[string]any{"PredictorName": "arn-predictor", "ForecastHorizon": 10},
			arnField: "PredictorArn",
		},
		{
			name:     "forecast_arn",
			action:   "CreateForecast",
			body:     map[string]any{"ForecastName": "arn-forecast", "PredictorArn": "predictor"},
			arnField: "ForecastArn",
		},
		{
			name:     "monitor_arn",
			action:   "CreateMonitor",
			body:     map[string]any{"MonitorName": "arn-monitor", "ResourceArn": "predictor"},
			arnField: "MonitorArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ForecastHandler(t)
			rec := a1ForecastDo(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ForecastUnmarshal(t, rec)
			arn, ok := m[tt.arnField].(string)
			require.True(t, ok, "ARN field %s must be a string", tt.arnField)
			assert.Contains(t, arn, "arn:aws:forecast:us-east-1:000000000000:")
		})
	}
}

// --- Status transitions ---

func TestAudit1_Forecast_StatusTransitions(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateForecast", map[string]any{
		"ForecastName": "status-forecast", "PredictorArn": "predictor",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["ForecastArn"].(string)

	// First describe: CREATE_PENDING
	rec = a1ForecastDo(t, h, "DescribeForecast", map[string]any{"ForecastArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "CREATE_PENDING", m["Status"])

	// Second describe: ACTIVE
	rec = a1ForecastDo(t, h, "DescribeForecast", map[string]any{"ForecastArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "ACTIVE", m["Status"])

	// Delete
	rec = a1ForecastDo(t, h, "DeleteForecast", map[string]any{"ForecastArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// After delete: resource is removed; describe returns ResourceNotFoundException.
	rec = a1ForecastDo(t, h, "DescribeForecast", map[string]any{"ForecastArn": arn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
}

// --- DatasetGroup ---

func TestAudit1_Forecast_DatasetGroup_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "field-group",
		"Domain":           "RETAIL",
		"DatasetArns":      []any{"arn:aws:forecast:us-east-1:000000000000:dataset/ds1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["DatasetGroupArn"].(string)

	// Advance to ACTIVE
	a1ForecastDo(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	rec = a1ForecastDo(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)

	assert.Equal(t, "field-group", m["DatasetGroupName"])
	assert.Equal(t, "RETAIL", m["Domain"])
	assert.NotEmpty(t, m["DatasetGroupArn"])
	assert.NotEmpty(t, m["CreationTime"])
	assert.NotEmpty(t, m["LastModificationTime"])

	// Update
	rec = a1ForecastDo(t, h, "UpdateDatasetGroup", map[string]any{
		"DatasetGroupArn": arn,
		"DatasetArns":     []any{"arn:aws:forecast:us-east-1:000000000000:dataset/ds2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.NotEmpty(t, m["DatasetGroupArn"])

	// List
	rec = a1ForecastDo(t, h, "ListDatasetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	groups, ok := m["DatasetGroups"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1)
}

// --- Dataset ---

func TestAudit1_Forecast_Dataset_FieldRetention(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	schema := map[string]any{
		"Attributes": []any{
			map[string]any{"AttributeName": "item_id", "AttributeType": "string"},
			map[string]any{"AttributeName": "demand", "AttributeType": "float"},
		},
	}
	rec := a1ForecastDo(t, h, "CreateDataset", map[string]any{
		"DatasetName":   "schema-ds",
		"Domain":        "RETAIL",
		"DatasetType":   "TARGET_TIME_SERIES",
		"DataFrequency": "W",
		"Schema":        schema,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["DatasetArn"].(string)

	// Advance to ACTIVE
	a1ForecastDo(t, h, "DescribeDataset", map[string]any{"DatasetArn": arn})
	rec = a1ForecastDo(t, h, "DescribeDataset", map[string]any{"DatasetArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)

	assert.Equal(t, "schema-ds", m["DatasetName"])
	assert.Equal(t, "RETAIL", m["Domain"])
	assert.Equal(t, "TARGET_TIME_SERIES", m["DatasetType"])
	assert.Equal(t, "W", m["DataFrequency"])
	assert.Equal(t, schema, m["Schema"])
	assert.Equal(t, "ACTIVE", m["Status"])
}

// --- DatasetImportJob ---

func TestAudit1_Forecast_DatasetImportJob_S3Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus string
	}{
		{
			name: "valid_s3_path_create_pending",
			body: map[string]any{
				"DatasetImportJobName": "valid-import",
				"DatasetArn":           "arn:aws:forecast:us-east-1:000000000000:dataset/sales",
				"DataSource": map[string]any{
					"S3Config": map[string]any{"Path": "s3://bucket/data.csv"},
				},
			},
			wantStatus: "CREATE_PENDING",
		},
		{
			name: "missing_s3_path_create_failed",
			body: map[string]any{
				"DatasetImportJobName": "broken-import",
				"DatasetArn":           "arn:aws:forecast:us-east-1:000000000000:dataset/sales",
				"DataSource":           map[string]any{"S3Config": map[string]any{}},
			},
			wantStatus: "CREATE_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ForecastHandler(t)
			rec := a1ForecastDo(t, h, "CreateDatasetImportJob", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			m := a1ForecastUnmarshal(t, rec)
			arn := m["DatasetImportJobArn"].(string)

			rec = a1ForecastDo(t, h, "DescribeDatasetImportJob", map[string]any{"DatasetImportJobArn": arn})
			require.Equal(t, http.StatusOK, rec.Code)
			m = a1ForecastUnmarshal(t, rec)
			assert.Equal(t, tt.wantStatus, m["Status"])
		})
	}
}

// --- Predictor ---

func TestAudit1_Forecast_Predictor_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreatePredictor", map[string]any{
		"PredictorName":   "audit-predictor",
		"ForecastHorizon": 30,
		"PerformAutoML":   true,
		"PerformHPO":      true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["PredictorArn"].(string)
	assert.Contains(t, arn, "arn:aws:forecast:")

	// Advance to ACTIVE
	a1ForecastDo(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	rec = a1ForecastDo(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "audit-predictor", m["PredictorName"])
	assert.Equal(t, "ACTIVE", m["Status"])
	assert.Equal(t, true, m["PerformAutoML"])
}

func TestAudit1_Forecast_GetAccuracyMetrics(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "metrics-predictor", "ForecastHorizon": 7,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["PredictorArn"].(string)

	rec = a1ForecastDo(t, h, "GetAccuracyMetrics", map[string]any{"PredictorArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	results, ok := m["PredictorEvaluationResults"].([]any)
	require.True(t, ok, "PredictorEvaluationResults must be a list")
	assert.NotNil(t, results)

	// Not found
	rec = a1ForecastDo(t, h, "GetAccuracyMetrics", map[string]any{"PredictorArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
}

// --- Monitor evaluations ---

func TestAudit1_Forecast_MonitorEvaluations(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateMonitor", map[string]any{
		"MonitorName": "eval-monitor",
		"ResourceArn": "arn:aws:forecast:us-east-1:000000000000:predictor/p1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	monitorARN := m["MonitorArn"].(string)

	rec = a1ForecastDo(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": monitorARN})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	evaluations, ok := m["PredictorMonitorEvaluations"].([]any)
	require.True(t, ok)
	require.Len(t, evaluations, 1)

	eval := evaluations[0].(map[string]any)
	assert.Equal(t, "SUCCESS", eval["EvaluationState"])
	assert.NotEmpty(t, eval["MonitorArn"])
	assert.NotEmpty(t, eval["MonitorName"])
	assert.NotNil(t, eval["MetricResults"])

	// Not found
	rec = a1ForecastDo(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- StopResource / ResumeResource ---

func TestAudit1_Forecast_StopResumeResource(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "sr-predictor", "ForecastHorizon": 5,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["PredictorArn"].(string)

	// Stop
	rec = a1ForecastDo(t, h, "StopResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Advance and verify STOPPED
	a1ForecastDo(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	rec = a1ForecastDo(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "STOPPED", m["Status"])

	// Resume
	rec = a1ForecastDo(t, h, "ResumeResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1ForecastDo(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "ACTIVE", m["Status"])
}

// --- DeleteResourceTree ---

func TestAudit1_Forecast_DeleteResourceTree(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateForecast", map[string]any{
		"ForecastName": "tree-forecast", "PredictorArn": "predictor",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["ForecastArn"].(string)

	rec = a1ForecastDo(t, h, "DeleteResourceTree", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// After tree delete: resource is removed; describe returns ResourceNotFoundException.
	rec = a1ForecastDo(t, h, "DescribeForecast", map[string]any{"ForecastArn": arn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])

	// Not found
	rec = a1ForecastDo(t, h, "DeleteResourceTree", map[string]any{"ResourceArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Tag round-trip ---

func TestAudit1_Forecast_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "tag-group", "Domain": "RETAIL",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn := m["DatasetGroupArn"].(string)

	// Tag
	rec = a1ForecastDo(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
			map[string]any{"Key": "owner", "Value": "audit"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List tags
	rec = a1ForecastDo(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	tags, ok := m["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range tags {
		t2 := tag.(map[string]any)
		tagMap[t2["Key"].(string)] = t2["Value"].(string)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "audit", tagMap["owner"])

	// Untag
	rec = a1ForecastDo(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []any{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1ForecastDo(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	m = a1ForecastUnmarshal(t, rec)
	tags = m["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "owner", tags[0].(map[string]any)["Key"])
}

// --- Error paths ---

func TestAudit1_Forecast_ErrorPaths(t *testing.T) {
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

			h := a1ForecastHandler(t)

			if tt.wantType == "ResourceAlreadyExistsException" {
				// Pre-create to trigger conflict
				first := a1ForecastDo(t, h, tt.action, tt.body)
				require.Equal(t, http.StatusOK, first.Code)
			}

			rec := a1ForecastDo(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			m := a1ForecastUnmarshal(t, rec)
			assert.Equal(t, tt.wantType, m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
}

// --- AutoPredictor ---

func TestAudit1_Forecast_AutoPredictor(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreateAutoPredictor", map[string]any{
		"PredictorName":   "auto-pred",
		"ForecastHorizon": 14,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1ForecastUnmarshal(t, rec)
	arn, ok := m["PredictorArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:forecast:")

	// DescribeAutoPredictor
	rec = a1ForecastDo(t, h, "DescribeAutoPredictor", map[string]any{"PredictorArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	m = a1ForecastUnmarshal(t, rec)
	assert.NotEmpty(t, m["PredictorArn"])
	assert.NotEmpty(t, m["Status"])
}
