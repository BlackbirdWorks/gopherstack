package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParityC_Delete_ResourceRemovedFromMap verifies that calling Delete* on a
// Forecast resource actually removes it from the backend (AWS behavior), rather
// than leaving a DELETING tombstone. Subsequent Describe and List calls must
// return ResourceNotFoundException / empty list.
func TestParityC_Delete_ResourceRemovedFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		create     string
		createBody map[string]any
		arnField   string
		describe   string
		deleteOp   string
		list       string
		listField  string
	}{
		{
			name:       "dataset_group",
			create:     "CreateDatasetGroup",
			createBody: map[string]any{"DatasetGroupName": "del-dg", "Domain": "RETAIL"},
			arnField:   "DatasetGroupArn",
			describe:   "DescribeDatasetGroup",
			deleteOp:   "DeleteDatasetGroup",
			list:       "ListDatasetGroups",
			listField:  "DatasetGroups",
		},
		{
			name:   "dataset",
			create: "CreateDataset",
			createBody: map[string]any{
				"DatasetName": "del-ds", "DatasetType": "TARGET_TIME_SERIES", "Domain": "RETAIL",
			},
			arnField:  "DatasetArn",
			describe:  "DescribeDataset",
			deleteOp:  "DeleteDataset",
			list:      "ListDatasets",
			listField: "Datasets",
		},
		{
			name:   "predictor",
			create: "CreatePredictor",
			createBody: map[string]any{
				"PredictorName": "del-pred", "ForecastHorizon": 5,
			},
			arnField:  "PredictorArn",
			describe:  "DescribePredictor",
			deleteOp:  "DeletePredictor",
			list:      "ListPredictors",
			listField: "Predictors",
		},
		{
			name:   "forecast",
			create: "CreateForecast",
			createBody: map[string]any{
				"ForecastName": "del-fc", "PredictorArn": "pred",
			},
			arnField:  "ForecastArn",
			describe:  "DescribeForecast",
			deleteOp:  "DeleteForecast",
			list:      "ListForecasts",
			listField: "Forecasts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, created := request(t, h, tt.create, tt.createBody)
			require.Equal(t, http.StatusOK, code)

			resARN, ok := created[tt.arnField].(string)
			require.True(t, ok)

			// Advance to ACTIVE so delete is valid.
			request(t, h, tt.describe, map[string]any{tt.arnField: resARN})
			request(t, h, tt.describe, map[string]any{tt.arnField: resARN})

			code, _ = request(t, h, tt.deleteOp, map[string]any{tt.arnField: resARN})
			require.Equal(t, http.StatusOK, code)

			// Describe must return 400 ResourceNotFoundException — not 200 with DELETING.
			code, resp := request(t, h, tt.describe, map[string]any{tt.arnField: resARN})
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])

			// List must return empty.
			_, listResp := request(t, h, tt.list, map[string]any{})
			items, _ := listResp[tt.listField].([]any)
			assert.Empty(t, items, "deleted resource must not appear in list")
		})
	}
}

// TestParityC_Delete_IdempotencyFails verifies that deleting the same resource
// twice returns ResourceNotFoundException on the second call.
func TestParityC_Delete_IdempotencyFails(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "idem-dg", "Domain": "RETAIL",
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["DatasetGroupArn"].(string)

	code, _ = request(t, h, "DeleteDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	require.Equal(t, http.StatusOK, code)

	code, resp := request(t, h, "DeleteDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestParityC_DeleteResourceTree_TransitiveDelete verifies that
// DeleteResourceTree removes the root resource AND all dependent children,
// mirroring AWS Forecast behavior.
func TestParityC_DeleteResourceTree_TransitiveDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create predictor (root).
	_, cp := request(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "tree-pred", "ForecastHorizon": 5,
	})
	predARN := cp["PredictorArn"].(string)

	// Create forecast referencing predictor.
	_, cf := request(t, h, "CreateForecast", map[string]any{
		"ForecastName": "tree-fc", "PredictorArn": predARN,
	})
	fcARN := cf["ForecastArn"].(string)

	// DeleteResourceTree on predictor → must delete predictor AND forecast.
	code, _ := request(t, h, "DeleteResourceTree", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)

	// Predictor gone.
	code, resp := request(t, h, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])

	// Forecast (child) also gone.
	code, resp = request(t, h, "DescribeForecast", map[string]any{"ForecastArn": fcARN})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestParityC_DeleteResourceTree_NotFound verifies that DeleteResourceTree
// returns ResourceNotFoundException when the target ARN does not exist.
func TestParityC_DeleteResourceTree_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, resp := request(t, h, "DeleteResourceTree", map[string]any{
		"ResourceArn": "arn:aws:forecast:us-east-1:000000000000:forecast/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestParityC_UpdateResourceStatus_ARNIndex verifies that StopResource and
// ResumeResource use the O(1) ARN index and correctly update resource status.
func TestParityC_UpdateResourceStatus_ARNIndex(t *testing.T) {
	t.Parallel()

	h := newHandler()
	_, cp := request(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "arn-pred", "ForecastHorizon": 5,
	})
	predARN := cp["PredictorArn"].(string)

	// Advance past CREATE_PENDING.
	request(t, h, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	request(t, h, "DescribePredictor", map[string]any{"PredictorArn": predARN})

	// Stop.
	code, _ := request(t, h, "StopResource", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)

	_, desc := request(t, h, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	assert.Equal(t, "STOPPED", desc["Status"])

	// Resume.
	code, _ = request(t, h, "ResumeResource", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)

	_, desc = request(t, h, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	assert.Equal(t, "ACTIVE", desc["Status"])
}

// TestParityC_UpdateResourceStatus_NotFound verifies that StopResource on a
// missing ARN returns ResourceNotFoundException.
func TestParityC_UpdateResourceStatus_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, resp := request(t, h, "StopResource", map[string]any{
		"ResourceArn": "arn:aws:forecast:us-east-1:000000000000:predictor/ghost",
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestParityC_ARNIndex_RebuildAfterReset verifies that creating resources after
// Reset works correctly — the ARN index must be cleared on Reset so stale
// entries don't cause incorrect lookups.
func TestParityC_ARNIndex_RebuildAfterReset(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create + delete a resource.
	_, c1 := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "pre-reset-dg", "Domain": "RETAIL",
	})
	arn1 := c1["DatasetGroupArn"].(string)
	request(t, h, "DeleteDatasetGroup", map[string]any{"DatasetGroupArn": arn1})

	// Reset backend.
	h.Reset()

	// Create a new resource with the same name — must succeed (no stale conflict).
	code, c2 := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "pre-reset-dg", "Domain": "CUSTOM",
	})
	require.Equal(t, http.StatusOK, code)
	arn2 := c2["DatasetGroupArn"].(string)
	assert.NotEmpty(t, arn2)

	// ARN lookup must work (StopResource is cross-kind lookup via index).
	// Use a predictor for stop/resume since dataset groups don't support it,
	// but verify the new resource is listable to confirm index integrity.
	_, listResp := request(t, h, "ListDatasetGroups", map[string]any{})
	items := listResp["DatasetGroups"].([]any)
	require.Len(t, items, 1)
}
