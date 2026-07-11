package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed snapshot
// bytes surface as an error rather than silently producing an empty or
// partially-decoded backend.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	h := newHandler()
	err := h.Backend.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_RestoreVersionMismatch verifies that a snapshot whose version
// doesn't match the current backend -- including one with no version field
// at all, which decodes as 0 and is exactly the shape Forecast produced
// before Phase 3.3 added persistence in the first place -- is discarded
// cleanly rather than partially decoded: the backend resets to empty state
// and Restore returns no error.
func TestHandler_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	h := newHandler()
	_, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "seed-group", "Domain": "RETAIL",
	})
	require.NotEmpty(t, created["DatasetGroupArn"])

	err := h.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, listed := request(t, h, "ListDatasetGroups", map[string]any{})
	groups, _ := listed["DatasetGroups"].([]any)
	assert.Empty(t, groups)
}

// TestHandler_SnapshotRestoreDelegate verifies that Handler.Snapshot/Restore
// delegate to the backend -- this is the dead-wiring fix: Forecast had
// neither of these methods before, so cli.go's generic setupPersistence
// never picked the service up at all.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := newHandler()
	_, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "delegate-group", "Domain": "RETAIL",
	})
	groupARN := created["DatasetGroupArn"].(string)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	_, described := request(t, h2, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": groupARN})
	assert.Equal(t, "delegate-group", described["DatasetGroupName"])
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource kind's store.Table (all sharing the
// Resource type, one Table per kind -- see store_setup.go), plus the raw
// maps left un-converted: evaluations and tags. It also exercises the
// arnIndex cross-table reverse lookup, which is deliberately NOT serialized
// -- Restore rebuilds it from the restored tables (persistence.go's
// rebuildARNIndex) -- via StopResource and DeleteResourceTree, both of which
// resolve their target exclusively through arnIndex.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	h := newHandler()

	_, dg := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "full-group", "Domain": "RETAIL",
	})
	dgARN := dg["DatasetGroupArn"].(string)

	_, ds := request(t, h, "CreateDataset", map[string]any{
		"DatasetName": "full-dataset", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
		"DataFrequency": "D",
	})
	dsARN := ds["DatasetArn"].(string)

	_, dij := request(t, h, "CreateDatasetImportJob", map[string]any{
		"DatasetImportJobName": "full-import", "DatasetArn": dsARN,
		"DataSource": map[string]any{"S3Config": map[string]any{"Path": "s3://bucket/train.csv"}},
	})
	dijARN := dij["DatasetImportJobArn"].(string)

	_, pred := request(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "full-predictor", "ForecastHorizon": 14,
	})
	predARN := pred["PredictorArn"].(string)

	_, fc := request(t, h, "CreateForecast", map[string]any{
		"ForecastName": "full-forecast", "PredictorArn": predARN,
	})
	fcARN := fc["ForecastArn"].(string)

	_, mon := request(t, h, "CreateMonitor", map[string]any{
		"MonitorName": "full-monitor", "ResourceArn": predARN,
	})
	monARN := mon["MonitorArn"].(string)

	// Advance the dataset group to ACTIVE (the first Describe flips
	// CREATE_PENDING -> ACTIVE) so the round trip also covers a non-default
	// Status value on an in-place-mutated Resource.
	request(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": dgARN})

	// Tag one resource; tags is a raw (non-store.Table) map keyed by ARN.
	code, _ := request(t, h, "TagResource", map[string]any{
		"ResourceArn": predARN,
		"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, code)

	snap := h.Backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := forecast.NewHandler(forecast.NewInMemoryBackend(testAccountID, testRegion))
	require.NoError(t, fresh.Backend.Restore(t.Context(), snap))

	// Every resource kind's store.Table round-trips.
	_, group := request(t, fresh, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": dgARN})
	assert.Equal(t, "ACTIVE", group["Status"])

	_, dataset := request(t, fresh, "DescribeDataset", map[string]any{"DatasetArn": dsARN})
	assert.Equal(t, "full-dataset", dataset["DatasetName"])

	_, importJob := request(t, fresh, "DescribeDatasetImportJob", map[string]any{"DatasetImportJobArn": dijARN})
	assert.Equal(t, "full-import", importJob["DatasetImportJobName"])

	_, predictor := request(t, fresh, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	assert.Equal(t, "full-predictor", predictor["PredictorName"])

	_, forecastResource := request(t, fresh, "DescribeForecast", map[string]any{"ForecastArn": fcARN})
	assert.Equal(t, "full-forecast", forecastResource["ForecastName"])

	// evaluations (raw map) round-trips.
	code, evalResp := request(t, fresh, "ListMonitorEvaluations", map[string]any{"MonitorArn": monARN})
	require.Equal(t, http.StatusOK, code)
	evaluations, _ := evalResp["PredictorMonitorEvaluations"].([]any)
	require.Len(t, evaluations, 1)

	// tags (raw map) round-trips.
	code, tagsResp := request(t, fresh, "ListTagsForResource", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)
	tagList, _ := tagsResp["Tags"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "env", tagList[0].(map[string]any)["Key"])
	assert.Equal(t, "test", tagList[0].(map[string]any)["Value"])

	// arnIndex (rebuilt on Restore, not serialized) resolves ARN-only lookups
	// after restore: StopResource uses it exclusively.
	code, _ = request(t, fresh, "StopResource", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)
	_, stopped := request(t, fresh, "DescribePredictor", map[string]any{"PredictorArn": predARN})
	assert.Equal(t, "STOPPED", stopped["Status"])

	// DeleteResourceTree also resolves its root exclusively via arnIndex, and
	// its cascade (arnReferencedBy scanning every table) must still find the
	// forecast referencing the predictor after restore.
	code, _ = request(t, fresh, "DeleteResourceTree", map[string]any{"ResourceArn": predARN})
	require.Equal(t, http.StatusOK, code)
	code, _ = request(t, fresh, "DescribeForecast", map[string]any{"ForecastArn": fcARN})
	assert.Equal(t, http.StatusBadRequest, code)

	assert.Equal(t, testAccountID, fresh.Backend.AccountID())
	assert.Equal(t, testRegion, fresh.Backend.Region())
}
