package forecast_test

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

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// Forecast operation, extracted from
// forecast@v1.44.4/serializers.go's
// awsAwsjson11_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("AmazonForecast.<Op>"), always POSTing
// to "/" (JSON-RPC 1.1, services/_PROTOCOLS.md).
//
// All 63 real ops are covered -- an implausible count for a service this
// size, so it was re-extracted directly rather than trusted: 55 come from
// forecastOperations()'s addCRUD-built h.ops map (13 resource kinds x
// Create/Describe/List/Delete, DatasetGroup alone also getting Update, plus
// 2 explicit CreateAutoPredictor/DescribeAutoPredictor entries), the other
// 8 (ListMonitorEvaluations, DeleteResourceTree, ResumeResource,
// StopResource, GetAccuracyMetrics, ListTagsForResource, TagResource,
// UntagResource) from a hardcoded if-chain in dispatch().
//
// This is a MIXED diff, not one of the three kinds cleanly: the 55 h.ops
// entries are SELF-REFERENTIALLY COLLAPSED -- GetSupportedOperations()
// literally ranges over h.ops (handler.go:69-71), and dispatch()'s fallback
// looks up the same h.ops map (handler.go:149), so a wrong key in
// forecastOperations() would be invisible to any same-repo diff between
// "the list" and "the dispatch table": both sides silently agree on the
// wrong string. The other 8 are GENUINELY INDEPENDENT: GetSupportedOperations()
// appends them as separate string literals (handler.go:73-80) while
// dispatch() tests for them via a separate `action == "X"` if-chain
// (handler.go:124-147) -- two independently hand-written lists. Either way,
// this table sidesteps the blind spot: every target string here is
// hardcoded from the real SDK, independent of forecastOperations() and the
// if-chain both.
//
// Also excluded correctly: no addCRUD call passes update=true for the
// "Dataset" resource kind (only DatasetGroup does), so no "UpdateDataset"
// key exists -- matching the real API, which has no such operation
// (handler.go:391-401 records this by comment; verified independently here
// by confirming "UpdateDataset" is absent from the real 63-op serializer
// list above).
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateAutoPredictor", "AmazonForecast.CreateAutoPredictor"},
		{"CreateDataset", "AmazonForecast.CreateDataset"},
		{"CreateDatasetGroup", "AmazonForecast.CreateDatasetGroup"},
		{"CreateDatasetImportJob", "AmazonForecast.CreateDatasetImportJob"},
		{"CreateExplainability", "AmazonForecast.CreateExplainability"},
		{"CreateExplainabilityExport", "AmazonForecast.CreateExplainabilityExport"},
		{"CreateForecast", "AmazonForecast.CreateForecast"},
		{"CreateForecastExportJob", "AmazonForecast.CreateForecastExportJob"},
		{"CreateMonitor", "AmazonForecast.CreateMonitor"},
		{"CreatePredictor", "AmazonForecast.CreatePredictor"},
		{"CreatePredictorBacktestExportJob", "AmazonForecast.CreatePredictorBacktestExportJob"},
		{"CreateWhatIfAnalysis", "AmazonForecast.CreateWhatIfAnalysis"},
		{"CreateWhatIfForecast", "AmazonForecast.CreateWhatIfForecast"},
		{"CreateWhatIfForecastExport", "AmazonForecast.CreateWhatIfForecastExport"},
		{"DeleteDataset", "AmazonForecast.DeleteDataset"},
		{"DeleteDatasetGroup", "AmazonForecast.DeleteDatasetGroup"},
		{"DeleteDatasetImportJob", "AmazonForecast.DeleteDatasetImportJob"},
		{"DeleteExplainability", "AmazonForecast.DeleteExplainability"},
		{"DeleteExplainabilityExport", "AmazonForecast.DeleteExplainabilityExport"},
		{"DeleteForecast", "AmazonForecast.DeleteForecast"},
		{"DeleteForecastExportJob", "AmazonForecast.DeleteForecastExportJob"},
		{"DeleteMonitor", "AmazonForecast.DeleteMonitor"},
		{"DeletePredictor", "AmazonForecast.DeletePredictor"},
		{"DeletePredictorBacktestExportJob", "AmazonForecast.DeletePredictorBacktestExportJob"},
		{"DeleteResourceTree", "AmazonForecast.DeleteResourceTree"},
		{"DeleteWhatIfAnalysis", "AmazonForecast.DeleteWhatIfAnalysis"},
		{"DeleteWhatIfForecast", "AmazonForecast.DeleteWhatIfForecast"},
		{"DeleteWhatIfForecastExport", "AmazonForecast.DeleteWhatIfForecastExport"},
		{"DescribeAutoPredictor", "AmazonForecast.DescribeAutoPredictor"},
		{"DescribeDataset", "AmazonForecast.DescribeDataset"},
		{"DescribeDatasetGroup", "AmazonForecast.DescribeDatasetGroup"},
		{"DescribeDatasetImportJob", "AmazonForecast.DescribeDatasetImportJob"},
		{"DescribeExplainability", "AmazonForecast.DescribeExplainability"},
		{"DescribeExplainabilityExport", "AmazonForecast.DescribeExplainabilityExport"},
		{"DescribeForecast", "AmazonForecast.DescribeForecast"},
		{"DescribeForecastExportJob", "AmazonForecast.DescribeForecastExportJob"},
		{"DescribeMonitor", "AmazonForecast.DescribeMonitor"},
		{"DescribePredictor", "AmazonForecast.DescribePredictor"},
		{"DescribePredictorBacktestExportJob", "AmazonForecast.DescribePredictorBacktestExportJob"},
		{"DescribeWhatIfAnalysis", "AmazonForecast.DescribeWhatIfAnalysis"},
		{"DescribeWhatIfForecast", "AmazonForecast.DescribeWhatIfForecast"},
		{"DescribeWhatIfForecastExport", "AmazonForecast.DescribeWhatIfForecastExport"},
		{"GetAccuracyMetrics", "AmazonForecast.GetAccuracyMetrics"},
		{"ListDatasetGroups", "AmazonForecast.ListDatasetGroups"},
		{"ListDatasetImportJobs", "AmazonForecast.ListDatasetImportJobs"},
		{"ListDatasets", "AmazonForecast.ListDatasets"},
		{"ListExplainabilities", "AmazonForecast.ListExplainabilities"},
		{"ListExplainabilityExports", "AmazonForecast.ListExplainabilityExports"},
		{"ListForecastExportJobs", "AmazonForecast.ListForecastExportJobs"},
		{"ListForecasts", "AmazonForecast.ListForecasts"},
		{"ListMonitorEvaluations", "AmazonForecast.ListMonitorEvaluations"},
		{"ListMonitors", "AmazonForecast.ListMonitors"},
		{"ListPredictorBacktestExportJobs", "AmazonForecast.ListPredictorBacktestExportJobs"},
		{"ListPredictors", "AmazonForecast.ListPredictors"},
		{"ListTagsForResource", "AmazonForecast.ListTagsForResource"},
		{"ListWhatIfAnalyses", "AmazonForecast.ListWhatIfAnalyses"},
		{"ListWhatIfForecastExports", "AmazonForecast.ListWhatIfForecastExports"},
		{"ListWhatIfForecasts", "AmazonForecast.ListWhatIfForecasts"},
		{"ResumeResource", "AmazonForecast.ResumeResource"},
		{"StopResource", "AmazonForecast.StopResource"},
		{"TagResource", "AmazonForecast.TagResource"},
		{"UntagResource", "AmazonForecast.UntagResource"},
		{"UpdateDatasetGroup", "AmazonForecast.UpdateDatasetGroup"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Amazon Forecast
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to dispatch()'s single unmatched-route
// return: `fmt.Errorf("%w: %s", ErrValidation, action)` (handler.go:149-152).
//
// This is the fourth of six services in this class whose sentinel does NOT
// assert cleanly on wire type: ErrValidation maps to "InvalidInputException"
// (handler.go:364), the SAME type used for every field-required/out-of-range
// validation error this service raises (store.go, validation.go -- dozens of
// call sites). Message text alone doesn't cleanly disambiguate either,
// because ErrValidation's own Error() text is just the literal string
// "InvalidInputException" (awserr.New("InvalidInputException", ...),
// errors.go:11) with the format-string suffix appended by %w -- so the
// unmatched-route message is exactly "InvalidInputException: <action>",
// while a real validation failure's message is "InvalidInputException:
// <field description>" (e.g. "InvalidInputException: resource name is
// required"). Those are only reliably distinguishable by checking whether
// the message's suffix, after "InvalidInputException: ", is EXACTLY the
// action name verbatim -- which is what the unmatched-route fallback does
// (it echoes back `action` with no other text) and no real validation
// message does (they always describe a field or requirement, never echo
// the operation name). This table asserts that specific equality rather
// than a substring, since a substring check ("contains the op name") would
// false-positive on any op whose name happens to appear inside its own
// legitimate error text.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := forecast.NewHandler(forecast.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEqual(t, "InvalidInputException: "+tc.op, resp.Message,
				"target=%s op=%s: dispatched to the unmatched-route handler, body=%s",
				tc.target, tc.op, rec.Body.String())
		})
	}
}
