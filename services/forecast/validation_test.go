package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

// --- enum-field validation (Domain / DatasetType / ImportMode) ---

// TestCreateDatasetGroup_DomainValidation verifies that CreateDatasetGroup
// requires a Domain field and rejects a value outside the Amazon Forecast
// Domain enum (RETAIL, CUSTOM, INVENTORY_PLANNING, EC2_CAPACITY, WORK_FORCE,
// WEB_TRAFFIC, METRICS) with InvalidInputException.
func TestCreateDatasetGroup_DomainValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_domain_rejected",
			body:     map[string]any{"DatasetGroupName": "no-domain"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unrecognized_domain_rejected",
			body:     map[string]any{"DatasetGroupName": "bad-domain", "Domain": "NOT_A_DOMAIN"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "retail_domain_accepted",
			body:     map[string]any{"DatasetGroupName": "retail-group", "Domain": "RETAIL"},
			wantCode: http.StatusOK,
		},
		{
			name:     "metrics_domain_accepted",
			body:     map[string]any{"DatasetGroupName": "metrics-group", "Domain": "METRICS"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, "CreateDatasetGroup", tt.body)
			assert.Equal(t, tt.wantCode, code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", resp["__type"])
			}
		})
	}
}

// TestCreateDataset_EnumValidation verifies that CreateDataset requires
// Domain, DatasetType, and Schema, and rejects a Domain/DatasetType value
// outside the Amazon Forecast enums.
func TestCreateDataset_EnumValidation(t *testing.T) {
	t.Parallel()

	validSchema := map[string]any{"Attributes": []any{}}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_domain_rejected",
			body: map[string]any{
				"DatasetName": "ds", "DatasetType": "TARGET_TIME_SERIES", "Schema": validSchema,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_dataset_type_rejected",
			body: map[string]any{
				"DatasetName": "ds", "Domain": "RETAIL", "Schema": validSchema,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_schema_rejected",
			body: map[string]any{
				"DatasetName": "ds", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unrecognized_domain_rejected",
			body: map[string]any{
				"DatasetName": "ds", "Domain": "NOT_A_DOMAIN", "DatasetType": "TARGET_TIME_SERIES",
				"Schema": validSchema,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unrecognized_dataset_type_rejected",
			body: map[string]any{
				"DatasetName": "ds", "Domain": "RETAIL", "DatasetType": "NOT_A_TYPE", "Schema": validSchema,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_fields_accepted",
			body: map[string]any{
				"DatasetName": "ds", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES", "Schema": validSchema,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "related_time_series_accepted",
			body: map[string]any{
				"DatasetName": "ds2", "Domain": "RETAIL", "DatasetType": "RELATED_TIME_SERIES", "Schema": validSchema,
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, "CreateDataset", tt.body)
			assert.Equal(t, tt.wantCode, code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", resp["__type"])
			}
		})
	}
}

// TestCreateDataset_DataFrequencyValidation verifies that the optional
// DataFrequency field, when present, must match Amazon Forecast's documented
// format (an optional 1-2 digit interval followed by Y/M/W/D/H/min) --
// unlike Domain/DatasetType, DataFrequency has no types.X enum in the SDK
// (it's server-validated free text), so this is a format check rather than
// an enum-membership check.
func TestCreateDataset_DataFrequencyValidation(t *testing.T) {
	t.Parallel()

	validSchema := map[string]any{"Attributes": []any{}}

	tests := []struct {
		dataFrequency any
		name          string
		wantCode      int
	}{
		{name: "omitted_accepted", dataFrequency: nil, wantCode: http.StatusOK},
		{name: "bare_day_accepted", dataFrequency: "D", wantCode: http.StatusOK},
		{name: "bare_week_accepted", dataFrequency: "W", wantCode: http.StatusOK},
		{name: "bare_hour_accepted", dataFrequency: "H", wantCode: http.StatusOK},
		{name: "bare_minute_accepted", dataFrequency: "min", wantCode: http.StatusOK},
		{name: "prefixed_week_accepted", dataFrequency: "2W", wantCode: http.StatusOK},
		{name: "prefixed_minute_accepted", dataFrequency: "15min", wantCode: http.StatusOK},
		{name: "unrecognized_unit_rejected", dataFrequency: "1Day", wantCode: http.StatusBadRequest},
		{name: "unit_only_no_digits_rejected", dataFrequency: "5", wantCode: http.StatusBadRequest},
		{name: "garbage_rejected", dataFrequency: "hourly", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := map[string]any{
				"DatasetName": "ds", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES", "Schema": validSchema,
			}
			if tt.dataFrequency != nil {
				body["DataFrequency"] = tt.dataFrequency
			}

			code, resp := request(t, h, "CreateDataset", body)
			assert.Equal(t, tt.wantCode, code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", resp["__type"])
			}
		})
	}
}

// TestCreateDatasetImportJob_ImportModeValidation verifies that ImportMode is
// optional (omitting it is not an error, matching AWS's FULL default) but,
// when present, must be one of Amazon Forecast's ImportMode enum values
// (FULL, INCREMENTAL).
func TestCreateDatasetImportJob_ImportModeValidation(t *testing.T) {
	t.Parallel()

	validSource := map[string]any{"S3Config": map[string]any{"Path": "s3://bucket/train.csv"}}

	tests := []struct {
		importMode any
		name       string
		wantCode   int
	}{
		{name: "omitted_import_mode_accepted", importMode: nil, wantCode: http.StatusOK},
		{name: "full_accepted", importMode: "FULL", wantCode: http.StatusOK},
		{name: "incremental_accepted", importMode: "INCREMENTAL", wantCode: http.StatusOK},
		{name: "unrecognized_import_mode_rejected", importMode: "PARTIAL", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			datasetARN := createDataset(t, h)

			body := map[string]any{
				"DatasetImportJobName": "import-job", "DatasetArn": datasetARN, "DataSource": validSource,
			}
			if tt.importMode != nil {
				body["ImportMode"] = tt.importMode
			}

			code, resp := request(t, h, "CreateDatasetImportJob", body)
			assert.Equal(t, tt.wantCode, code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", resp["__type"])
			}
		})
	}
}

// --- cross-resource FK existence validation ---

// TestCreate_FKReferenceValidation verifies that every Create* operation
// carrying a required ARN-reference field (PredictorArn, DatasetArn, ...)
// rejects a dangling reference with ResourceNotFoundException and a missing
// reference with InvalidInputException, matching real Amazon Forecast
// (which returns ResourceNotFoundException for a dangling reference on
// exactly these fields per the SDK's deserializers.go).
func TestCreate_FKReferenceValidation(t *testing.T) {
	t.Parallel()

	const danglingARN = "arn:aws:forecast:us-east-1:000000000000:predictor/does-not-exist"

	tests := []struct {
		buildBody func(t *testing.T, h *forecast.Handler) map[string]any
		name      string
		action    string
	}{
		{
			name:   "dataset_import_job_dataset_arn",
			action: "CreateDatasetImportJob",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"DatasetImportJobName": "job",
					"DatasetArn":           "arn:aws:forecast:us-east-1:000000000000:dataset/does-not-exist",
					"DataSource":           map[string]any{"S3Config": map[string]any{"Path": "s3://bucket/x.csv"}},
				}
			},
		},
		{
			name:   "predictor_backtest_export_predictor_arn",
			action: "CreatePredictorBacktestExportJob",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"PredictorBacktestExportJobName": "job", "PredictorArn": danglingARN,
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name:   "forecast_predictor_arn",
			action: "CreateForecast",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{"ForecastName": "fc", "PredictorArn": danglingARN}
			},
		},
		{
			name:   "forecast_export_forecast_arn",
			action: "CreateForecastExportJob",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"ForecastExportJobName": "job",
					"ForecastArn":           "arn:aws:forecast:us-east-1:000000000000:forecast/does-not-exist",
					"Destination":           minimalDataDestination(),
				}
			},
		},
		{
			name:   "explainability_export_explainability_arn",
			action: "CreateExplainabilityExport",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"ExplainabilityExportName": "job",
					"ExplainabilityArn": "arn:aws:forecast:us-east-1:000000000000:" +
						"explainability-export/does-not-exist",
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name:   "explainability_resource_arn",
			action: "CreateExplainability",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"ExplainabilityName": "job", "ResourceArn": danglingARN,
					"ExplainabilityConfig": map[string]any{
						"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL",
					},
				}
			},
		},
		{
			name:   "monitor_resource_arn",
			action: "CreateMonitor",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{"MonitorName": "mon", "ResourceArn": danglingARN}
			},
		},
		{
			name:   "what_if_analysis_forecast_arn",
			action: "CreateWhatIfAnalysis",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"WhatIfAnalysisName": "wia",
					"ForecastArn":        "arn:aws:forecast:us-east-1:000000000000:forecast/does-not-exist",
				}
			},
		},
		{
			name:   "what_if_forecast_analysis_arn",
			action: "CreateWhatIfForecast",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"WhatIfForecastName": "wif",
					"WhatIfAnalysisArn": "arn:aws:forecast:us-east-1:000000000000:" +
						"what-if-analysis/does-not-exist",
				}
			},
		},
		{
			name:   "what_if_forecast_export_forecast_arns",
			action: "CreateWhatIfForecastExport",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"WhatIfForecastExportName": "wife",
					"WhatIfForecastArns": []any{
						"arn:aws:forecast:us-east-1:000000000000:what-if-forecast/does-not-exist",
					},
					"Destination": minimalDataDestination(),
				}
			},
		},
		{
			name:   "predictor_input_data_config_dataset_group_arn",
			action: "CreatePredictor",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"PredictorName": "pred", "ForecastHorizon": 5,
					"InputDataConfig": map[string]any{
						"DatasetGroupArn": "arn:aws:forecast:us-east-1:000000000000:dataset-group/does-not-exist",
					},
					"FeaturizationConfig": map[string]any{},
				}
			},
		},
		{
			name:   "auto_predictor_data_config_dataset_group_arn",
			action: "CreateAutoPredictor",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"PredictorName": "auto-pred",
					"DataConfig": map[string]any{
						"DatasetGroupArn": "arn:aws:forecast:us-east-1:000000000000:dataset-group/does-not-exist",
					},
				}
			},
		},
		{
			name:   "dataset_group_dataset_arns",
			action: "CreateDatasetGroup",
			buildBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{
					"DatasetGroupName": "dg", "Domain": "RETAIL",
					"DatasetArns": []any{"arn:aws:forecast:us-east-1:000000000000:dataset/does-not-exist"},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, tt.action, tt.buildBody(t, h))
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestCreateForecast_MissingPredictorArn verifies that omitting a required
// FK field entirely (as opposed to supplying a dangling one) is a field
// validation error (InvalidInputException), not a not-found error, matching
// the client-side "This member is required" rule the real SDK enforces on
// CreateForecastInput.PredictorArn.
func TestCreateForecast_MissingPredictorArn(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, resp := request(t, h, "CreateForecast", map[string]any{"ForecastName": "fc"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidInputException", resp["__type"])
}

// TestCreatePassthroughFields_PresenceValidation covers gopherstack-wl0s: six
// Create* operations whose generic-CRUD handler (handler.go's execute/
// store.go's cloneMap) stores and echoes the whole input map, so a supplied
// value already round-trips fine -- what was missing was rejecting a
// request that omits a field aws-sdk-go-v2/service/forecast@v1.44.4/
// validators.go marks "This member is required": CreateExplainability's
// ExplainabilityConfig; CreateForecastExportJob's, CreatePredictorBacktest
// ExportJob's, CreateExplainabilityExport's, and CreateWhatIfForecastExport's
// shared Destination; and CreatePredictor's ForecastHorizon, InputDataConfig,
// and FeaturizationConfig (three fields, not the two the originating audit
// guessed -- verified directly against validateOpCreatePredictorInput).
//
// Each case proves both directions: omitting the field is rejected with
// InvalidInputException (the code every one of these ops' own
// awsAwsjson11_deserializeOpError<Op> switch declares for InvalidInputException,
// confirmed per op in deserializers.go), and supplying it is accepted and
// the value round-trips unchanged through the matching Describe* operation.
func TestCreatePassthroughFields_PresenceValidation(t *testing.T) {
	t.Parallel()

	explainabilityConfig := map[string]any{"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL"}

	tests := []struct {
		validBody      func(t *testing.T, h *forecast.Handler) map[string]any
		roundTripCheck func(t *testing.T, describeResp map[string]any)
		name           string
		action         string
		describeOp     string
		arnField       string
		missingField   string
	}{
		{
			name:   "create_explainability_explainability_config",
			action: "CreateExplainability", describeOp: "DescribeExplainability",
			arnField: "ExplainabilityArn", missingField: "ExplainabilityConfig",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ExplainabilityName": "presence-explain", "ResourceArn": createPredictor(t, h),
					"ExplainabilityConfig": explainabilityConfig,
				}
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, explainabilityConfig, resp["ExplainabilityConfig"])
			},
		},
		{
			name:   "create_forecast_export_job_destination",
			action: "CreateForecastExportJob", describeOp: "DescribeForecastExportJob",
			arnField: "ForecastExportJobArn", missingField: "Destination",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ForecastExportJobName": "presence-fc-export", "ForecastArn": createForecast(t, h),
					"Destination": minimalDataDestination(),
				}
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, minimalDataDestination(), resp["Destination"])
			},
		},
		{
			name:   "create_predictor_backtest_export_job_destination",
			action: "CreatePredictorBacktestExportJob", describeOp: "DescribePredictorBacktestExportJob",
			arnField: "PredictorBacktestExportJobArn", missingField: "Destination",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"PredictorBacktestExportJobName": "presence-pred-export", "PredictorArn": createPredictor(t, h),
					"Destination": minimalDataDestination(),
				}
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, minimalDataDestination(), resp["Destination"])
			},
		},
		{
			name:   "create_explainability_export_destination",
			action: "CreateExplainabilityExport", describeOp: "DescribeExplainabilityExport",
			arnField: "ExplainabilityExportArn", missingField: "Destination",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"ExplainabilityExportName": "presence-explain-export",
					"ExplainabilityArn":        createExplainability(t, h),
					"Destination":              minimalDataDestination(),
				}
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, minimalDataDestination(), resp["Destination"])
			},
		},
		{
			name:   "create_what_if_forecast_export_destination",
			action: "CreateWhatIfForecastExport", describeOp: "DescribeWhatIfForecastExport",
			arnField: "WhatIfForecastExportArn", missingField: "Destination",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"WhatIfForecastExportName": "presence-wif-export",
					"WhatIfForecastArns":       []any{createWhatIfForecast(t, h)},
					"Destination":              minimalDataDestination(),
				}
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, minimalDataDestination(), resp["Destination"])
			},
		},
		{
			name:   "create_predictor_forecast_horizon",
			action: "CreatePredictor", describeOp: "DescribePredictor",
			arnField: "PredictorArn", missingField: "ForecastHorizon",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return minimalCreatePredictorBody(t, h, "presence-pred-horizon")
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.InEpsilon(t, float64(10), resp["ForecastHorizon"], 0)
			},
		},
		{
			name:   "create_predictor_input_data_config",
			action: "CreatePredictor", describeOp: "DescribePredictor",
			arnField: "PredictorArn", missingField: "InputDataConfig",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return minimalCreatePredictorBody(t, h, "presence-pred-input")
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(t, resp, "InputDataConfig")
				assert.NotEmpty(t, resp["InputDataConfig"].(map[string]any)["DatasetGroupArn"])
			},
		},
		{
			name:   "create_predictor_featurization_config",
			action: "CreatePredictor", describeOp: "DescribePredictor",
			arnField: "PredictorArn", missingField: "FeaturizationConfig",
			validBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return minimalCreatePredictorBody(t, h, "presence-pred-featurization")
			},
			roundTripCheck: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(t, resp, "FeaturizationConfig")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_missing_rejected", func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := tt.validBody(t, h)
			delete(body, tt.missingField)

			code, resp := request(t, h, tt.action, body)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})

		t.Run(tt.name+"_present_round_trips", func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, created := request(t, h, tt.action, tt.validBody(t, h))
			require.Equal(t, http.StatusOK, code)
			arn, ok := created[tt.arnField].(string)
			require.True(t, ok)

			_, described := request(t, h, tt.describeOp, map[string]any{tt.arnField: arn})
			tt.roundTripCheck(t, described)
		})
	}
}

// TestCreateDatasetGroup_DatasetArnsOptional verifies that CreateDatasetGroup
// accepts an omitted DatasetArns list, matching real Amazon Forecast
// (CreateDatasetGroupRequest's "required" list in the botocore model names
// only DatasetGroupName and Domain; DatasetArns carries no "This member is
// required" constraint in aws-sdk-go-v2/service/forecast/validators.go's
// validateOpCreateDatasetGroupInput).
func TestCreateDatasetGroup_DatasetArnsOptional(t *testing.T) {
	t.Parallel()

	h := newHandler()
	code, _ := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "no-datasets", "Domain": "RETAIL",
	})
	assert.Equal(t, http.StatusOK, code)
}

// TestUpdateDatasetGroup_DatasetArnsValidation verifies UpdateDatasetGroup's
// DatasetArns semantics: the field itself is required (UpdateDatasetGroupInput
// has "This member is required" on DatasetArns per validators.go's
// validateOpUpdateDatasetGroupInput, unlike Create's optional field), a
// dangling entry is ResourceNotFoundException, and an empty (but present)
// list is legal -- the underlying ArnList shape in botocore's forecast model
// sets no "min" constraint, so replacing a group's datasets with none is a
// valid way to clear it.
func TestUpdateDatasetGroup_DatasetArnsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		datasetArns any
		name        string
		wantType    string
		wantCode    int
	}{
		{
			name:        "missing_datasetarns_rejected",
			datasetArns: nil,
			wantCode:    http.StatusBadRequest,
			wantType:    "InvalidInputException",
		},
		{
			name:        "dangling_arn_rejected",
			datasetArns: []any{"arn:aws:forecast:us-east-1:000000000000:dataset/does-not-exist"},
			wantCode:    http.StatusBadRequest,
			wantType:    "ResourceNotFoundException",
		},
		{
			name:        "empty_list_accepted",
			datasetArns: []any{},
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			_, created := request(t, h, "CreateDatasetGroup", map[string]any{
				"DatasetGroupName": "target-group", "Domain": "RETAIL",
			})
			body := map[string]any{"DatasetGroupArn": created["DatasetGroupArn"]}
			if tt.datasetArns != nil {
				body["DatasetArns"] = tt.datasetArns
			}

			code, resp := request(t, h, "UpdateDatasetGroup", body)
			assert.Equal(t, tt.wantCode, code)
			if tt.wantType != "" {
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

// TestCreateExplainability_ResourceArnAcceptsEitherKind verifies that
// CreateExplainability's ResourceArn resolves against either a Predictor ARN
// or a Forecast ARN, matching real Amazon Forecast (Explainability can be
// computed for either resource type).
func TestCreateExplainability_ResourceArnAcceptsEitherKind(t *testing.T) {
	t.Parallel()

	t.Run("predictor_resource_arn", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		predictorARN := createPredictor(t, h)
		code, _ := request(t, h, "CreateExplainability", map[string]any{
			"ExplainabilityName": "explain-pred", "ResourceArn": predictorARN,
			"ExplainabilityConfig": map[string]any{
				"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL",
			},
		})
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("forecast_resource_arn", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		forecastARN := createForecast(t, h)
		code, _ := request(t, h, "CreateExplainability", map[string]any{
			"ExplainabilityName": "explain-fc", "ResourceArn": forecastARN,
			"ExplainabilityConfig": map[string]any{
				"TimePointGranularity": "ALL", "TimeSeriesGranularity": "ALL",
			},
		})
		assert.Equal(t, http.StatusOK, code)
	})
}

// --- Delete* status-gate (ResourceInUseException) ---

// TestDelete_ResourceInUseWhileCreatePending verifies that Delete* rejects a
// resource that hasn't advanced past CREATE_PENDING with
// ResourceInUseException, matching real Amazon Forecast's documented Delete*
// preconditions (e.g. DeletePredictor: "You can delete only predictor that
// have a status of ACTIVE or CREATE_FAILED").
func TestDelete_ResourceInUseWhileCreatePending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody func(t *testing.T, h *forecast.Handler) map[string]any
		name       string
		createOp   string
		describeOp string
		deleteOp   string
		arnField   string
	}{
		{
			name: "dataset_group", createOp: "CreateDatasetGroup", describeOp: "DescribeDatasetGroup",
			deleteOp: "DeleteDatasetGroup", arnField: "DatasetGroupArn",
			createBody: func(*testing.T, *forecast.Handler) map[string]any {
				return map[string]any{"DatasetGroupName": "pending-dg", "Domain": "RETAIL"}
			},
		},
		{
			name: "predictor", createOp: "CreatePredictor", describeOp: "DescribePredictor",
			deleteOp: "DeletePredictor", arnField: "PredictorArn",
			createBody: func(t *testing.T, h *forecast.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"PredictorName": "pending-pred", "ForecastHorizon": 5,
					"InputDataConfig":     map[string]any{"DatasetGroupArn": createDatasetGroup(t, h)},
					"FeaturizationConfig": map[string]any{},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, created := request(t, h, tt.createOp, tt.createBody(t, h))
			require.Equal(t, http.StatusOK, code)
			arn, ok := created[tt.arnField].(string)
			require.True(t, ok)

			// No Describe call: the resource is still CREATE_PENDING.
			code, resp := request(t, h, tt.deleteOp, map[string]any{tt.arnField: arn})
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceInUseException", resp["__type"])

			// Advancing to ACTIVE via Describe makes the same resource
			// deletable: the block is on status, not a permanent lock.
			request(t, h, tt.describeOp, map[string]any{tt.arnField: arn})
			code, _ = request(t, h, tt.deleteOp, map[string]any{tt.arnField: arn})
			assert.Equal(t, http.StatusOK, code)
		})
	}
}

// TestDelete_CreateFailedDatasetImportJobDeletableImmediately verifies that a
// DatasetImportJob created in CREATE_FAILED status (missing S3Config.Path)
// is deletable without ever passing through Describe, matching real Amazon
// Forecast's "ACTIVE or CREATE_FAILED" precondition for DeleteDatasetImportJob.
func TestDelete_CreateFailedDatasetImportJobDeletableImmediately(t *testing.T) {
	t.Parallel()

	h := newHandler()
	datasetARN := createDataset(t, h)

	code, created := request(t, h, "CreateDatasetImportJob", map[string]any{
		"DatasetImportJobName": "failed-import", "DatasetArn": datasetARN,
		"DataSource": map[string]any{"S3Config": map[string]any{}},
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["DatasetImportJobArn"].(string)

	// No Describe call: the resource was created directly in CREATE_FAILED.
	code, _ = request(t, h, "DeleteDatasetImportJob", map[string]any{"DatasetImportJobArn": arn})
	assert.Equal(t, http.StatusOK, code)
}

// TestDelete_UnrestrictedKindsDeletableWhileCreatePending verifies that
// PredictorBacktestExportJob and ExplainabilityExport -- the two kinds whose
// real Amazon Forecast Delete* docs carry no status precondition -- can be
// deleted immediately after creation, unlike the kinds covered by
// TestDelete_ResourceInUseWhileCreatePending.
func TestDelete_UnrestrictedKindsDeletableWhileCreatePending(t *testing.T) {
	t.Parallel()

	t.Run("predictor_backtest_export_job", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		predictorARN := createPredictor(t, h)
		code, created := request(t, h, "CreatePredictorBacktestExportJob", map[string]any{
			"PredictorBacktestExportJobName": "backtest", "PredictorArn": predictorARN,
			"Destination": minimalDataDestination(),
		})
		require.Equal(t, http.StatusOK, code)
		arn := created["PredictorBacktestExportJobArn"].(string)

		code, _ = request(t, h, "DeletePredictorBacktestExportJob",
			map[string]any{"PredictorBacktestExportJobArn": arn})
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("explainability_export", func(t *testing.T) {
		t.Parallel()

		h := newHandler()
		explainabilityARN := createExplainability(t, h)
		code, created := request(t, h, "CreateExplainabilityExport", map[string]any{
			"ExplainabilityExportName": "export", "ExplainabilityArn": explainabilityARN,
			"Destination": minimalDataDestination(),
		})
		require.Equal(t, http.StatusOK, code)
		arn := created["ExplainabilityExportArn"].(string)

		code, _ = request(t, h, "DeleteExplainabilityExport", map[string]any{"ExplainabilityExportArn": arn})
		assert.Equal(t, http.StatusOK, code)
	})
}
