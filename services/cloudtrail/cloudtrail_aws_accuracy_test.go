package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestAdvancedEventSelectors verifies PutEventSelectors and GetEventSelectors with
// AdvancedEventSelectors support (mutually exclusive with basic EventSelectors).
func TestAdvancedEventSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "put_advanced_event_selectors_success",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "adv-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "adv-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Log all S3 data events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
								{"Field": "resources.type", "Equals": []string{"AWS::S3::Object"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["TrailARN"])
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok, "response should contain AdvancedEventSelectors")
				assert.Len(t, advSels, 1)
				// Basic selectors should NOT be present when advanced are active.
				_, hasBasic := resp["EventSelectors"]
				assert.False(t, hasBasic, "basic EventSelectors should not be in response with advanced selectors")
			},
		},
		{
			name: "get_advanced_event_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "get-adv-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "get-adv-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Management events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Management"}},
							},
						},
					},
				})
				rec := doCloudTrailOp(t, h.h, "GetEventSelectors", map[string]any{
					"TrailName": "get-adv-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["TrailARN"])
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				assert.Equal(t, "Management events", sel["Name"])
			},
		},
		{
			name: "advanced_selectors_replace_basic_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "mutual-trail",
					"S3BucketName": "bucket",
				})
				// First put basic selectors.
				doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "mutual-trail",
					"EventSelectors": []map[string]any{
						{"ReadWriteType": "All", "IncludeManagementEvents": true},
					},
				})
				// Then put advanced selectors — should replace basic.
				rec := doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "mutual-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "All data events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				// Verify GetEventSelectors returns only advanced now.
				getRec := doCloudTrailOp(t, h.h, "GetEventSelectors", map[string]any{
					"TrailName": "mutual-trail",
				})
				getResp := parseCloudTrailResp(t, getRec)
				advSels, ok := getResp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				// Basic selectors should be empty now.
				basicSels, hasSels := getResp["EventSelectors"].([]any)
				if hasSels {
					assert.Empty(t, basicSels, "basic selectors should be empty after applying advanced")
				}
			},
		},
		{
			name: "basic_selectors_replace_advanced_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "reverse-mutual-trail",
					"S3BucketName": "bucket",
				})
				// Put advanced selectors first.
				doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "reverse-mutual-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Network activity",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"NetworkActivity"}},
							},
						},
					},
				})
				// Now put basic selectors — should replace advanced.
				rec := doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "reverse-mutual-trail",
					"EventSelectors": []map[string]any{
						{"ReadWriteType": "WriteOnly", "IncludeManagementEvents": true},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				basicSels, ok := resp["EventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, basicSels, 1)
			},
		},
		{
			name: "advanced_selectors_with_multiple_field_conditions",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "multi-cond-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "multi-cond-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Specific S3 bucket",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
								{"Field": "resources.type", "Equals": []string{"AWS::S3::Object"}},
								{"Field": "resources.ARN", "StartsWith": []string{"arn:aws:s3:::my-bucket/"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				advSels := resp["AdvancedEventSelectors"].([]any)
				require.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				fieldSels := sel["FieldSelectors"].([]any)
				assert.Len(t, fieldSels, 3)
			},
		},
		{
			name: "put_advanced_selectors_trail_not_found",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "nonexistent-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "sel",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestGetTrailStatusFullResponse verifies that GetTrailStatus returns the full
// AWS-accurate status object including timing fields.
func TestGetTrailStatusFullResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "status_before_logging_has_no_times",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "status-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "GetTrailStatus", map[string]any{
					"Name": "status-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, false, resp["IsLogging"])
				// No timing fields before logging starts.
				_, hasStart := resp["StartLoggingTime"]
				assert.False(t, hasStart, "StartLoggingTime should not be set before StartLogging")
			},
		},
		{
			name: "status_after_start_logging_has_start_time",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "logging-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "StartLogging", map[string]any{
					"Name": "logging-trail",
				})
				rec := doCloudTrailOp(t, h.h, "GetTrailStatus", map[string]any{
					"Name": "logging-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, true, resp["IsLogging"])
				assert.NotNil(t, resp["StartLoggingTime"], "StartLoggingTime should be set after StartLogging")
				assert.NotNil(t, resp["LatestDeliveryTime"], "LatestDeliveryTime should be set after StartLogging")
			},
		},
		{
			name: "status_after_stop_logging_has_stop_time",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "stop-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "StartLogging", map[string]any{"Name": "stop-trail"})
				doCloudTrailOp(t, h.h, "StopLogging", map[string]any{"Name": "stop-trail"})
				rec := doCloudTrailOp(t, h.h, "GetTrailStatus", map[string]any{
					"Name": "stop-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, false, resp["IsLogging"])
				assert.NotNil(t, resp["StopLoggingTime"], "StopLoggingTime should be set after StopLogging")
				assert.NotNil(t, resp["StartLoggingTime"], "StartLoggingTime should remain set")
			},
		},
		{
			name: "status_by_arn_returns_correct_status",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "arn-status-trail",
					"S3BucketName": "bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)

				doCloudTrailOp(t, h.h, "StartLogging", map[string]any{"Name": trailARN})
				rec := doCloudTrailOp(t, h.h, "GetTrailStatus", map[string]any{
					"Name": trailARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, true, resp["IsLogging"])
			},
		},
		{
			name: "get_status_not_found_returns_404",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "GetTrailStatus", map[string]any{
					"Name": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestInsightSelectors verifies PutInsightSelectors and GetInsightSelectors with
// correct HasInsightSelectors tracking.
func TestInsightSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "put_and_get_insight_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "insight-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "insight-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["TrailARN"])
				selectors, ok := resp["InsightSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, selectors, 1)
				sel := selectors[0].(map[string]any)
				assert.Equal(t, "ApiCallRateInsight", sel["InsightType"])
			},
		},
		{
			name: "put_insight_selectors_sets_has_insight_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "has-insight-trail",
					"S3BucketName": "bucket",
				})
				// Initially HasInsightSelectors should be false.
				descRec := doCloudTrailOp(t, h.h, "DescribeTrails", nil)
				descResp := parseCloudTrailResp(t, descRec)
				list := descResp["trailList"].([]any)
				require.Len(t, list, 1)
				trail := list[0].(map[string]any)
				assert.Equal(t, false, trail["HasInsightSelectors"])

				// Put insight selectors.
				doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "has-insight-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiErrorRateInsight"},
					},
				})
				// Now HasInsightSelectors should be true.
				descRec2 := doCloudTrailOp(t, h.h, "DescribeTrails", nil)
				descResp2 := parseCloudTrailResp(t, descRec2)
				list2 := descResp2["trailList"].([]any)
				require.Len(t, list2, 1)
				trail2 := list2[0].(map[string]any)
				assert.Equal(t, true, trail2["HasInsightSelectors"])
			},
		},
		{
			name: "clear_insight_selectors_causes_insight_not_enabled_error",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "clear-insight-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "clear-insight-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				// Clear by passing empty list.
				doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName":        "clear-insight-trail",
					"InsightSelectors": []any{},
				})
				// AWS returns InsightNotEnabledException when no selectors are configured.
				rec := doCloudTrailOp(t, h.h, "GetInsightSelectors", map[string]any{
					"TrailName": "clear-insight-trail",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "InsightNotEnabledException", resp["__type"])
			},
		},
		{
			name: "get_insight_selectors_returns_insight_not_enabled_on_new_trail",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "empty-insight-trail",
					"S3BucketName": "bucket",
				})
				// AWS returns InsightNotEnabledException when trail has no insight selectors.
				rec := doCloudTrailOp(t, h.h, "GetInsightSelectors", map[string]any{
					"TrailName": "empty-insight-trail",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "InsightNotEnabledException", resp["__type"])
			},
		},
		{
			name: "put_insight_selectors_trail_not_found",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "missing-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "get_insight_selectors_trail_not_found",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "GetInsightSelectors", map[string]any{
					"TrailName": "missing-trail",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "put_insight_selectors_missing_trail_name",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "both_insight_types",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "both-insights-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "both-insights-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
						{"InsightType": "ApiErrorRateInsight"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				sels := resp["InsightSelectors"].([]any)
				assert.Len(t, sels, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestEDSFederation verifies EnableFederation and DisableFederation properly
// track federation status on event data stores.
func TestEDSFederation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "new_eds_has_disabled_federation",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "fed-test-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "DISABLED", resp["FederationStatus"])
			},
		},
		{
			name: "enable_federation_sets_status_and_role",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "enable-fed-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				roleArn := "arn:aws:iam::123456789012:role/CloudTrailFederationRole"
				rec := doCloudTrailOp(t, h.h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": roleArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "ENABLED", resp["FederationStatus"])
				assert.Equal(t, roleArn, resp["FederationRoleArn"])
				assert.Equal(t, edsARN, resp["EventDataStoreArn"])
			},
		},
		{
			name: "disable_federation_after_enable",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "disable-fed-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				doCloudTrailOp(t, h.h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": "arn:aws:iam::123456789012:role/TestRole",
				})

				rec := doCloudTrailOp(t, h.h, "DisableFederation", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "DISABLED", resp["FederationStatus"])
				_, hasRole := resp["FederationRoleArn"]
				assert.False(t, hasRole, "FederationRoleArn should be cleared after disable")
			},
		},
		{
			name: "federation_status_persisted_in_get_eds",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "fed-persist-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				roleArn := "arn:aws:iam::123456789012:role/FedRole"
				doCloudTrailOp(t, h.h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": roleArn,
				})

				// GetEventDataStore should reflect updated federation status.
				getRec := doCloudTrailOp(t, h.h, "GetEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, "ENABLED", getResp["FederationStatus"])
				assert.Equal(t, roleArn, getResp["FederationRoleArn"])
			},
		},
		{
			name: "enable_federation_eds_not_found",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "EnableFederation", map[string]any{
					"EventDataStore":    "nonexistent-eds",
					"FederationRoleArn": "arn:aws:iam::123:role/R",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "disable_federation_eds_not_found",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "DisableFederation", map[string]any{
					"EventDataStore": "nonexistent-eds",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "enable_federation_missing_eds_field",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "EnableFederation", map[string]any{
					"FederationRoleArn": "arn:aws:iam::123:role/R",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "disable_federation_missing_eds_field",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "DisableFederation", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestTerminationProtection verifies that DeleteEventDataStore refuses to delete
// event data stores with TerminationProtectionEnabled=true.
func TestTerminationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "delete_protected_eds_returns_409",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name":                         "protected-eds",
					"TerminationProtectionEnabled": true,
				})
				assert.Equal(t, http.StatusOK, createRec.Code)
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h.h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Contains(t, resp["__type"], "TerminationProtect")
			},
		},
		{
			name: "delete_unprotected_eds_succeeds",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name":                         "unprotected-eds",
					"TerminationProtectionEnabled": false,
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h.h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "disable_protection_then_delete_succeeds",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name":                         "was-protected-eds",
					"TerminationProtectionEnabled": true,
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				// Update to disable termination protection.
				boolFalse := false
				doCloudTrailOp(t, h.h, "UpdateEventDataStore", map[string]any{
					"EventDataStore":               edsARN,
					"TerminationProtectionEnabled": &boolFalse,
				})

				rec := doCloudTrailOp(t, h.h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestEDSAdvancedEventSelectors verifies that EventDataStore supports AdvancedEventSelectors
// in Create, Get, and Update operations.
func TestEDSAdvancedEventSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "create_eds_with_advanced_event_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "adv-sel-eds",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Log S3 data events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
								{"Field": "resources.type", "Equals": []string{"AWS::S3::Object"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["EventDataStoreArn"])
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
			},
		},
		{
			name: "get_eds_returns_advanced_event_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "get-adv-eds",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "All management events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Management"}},
							},
						},
					},
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				getRec := doCloudTrailOp(t, h.h, "GetEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				advSels, ok := getResp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				assert.Equal(t, "All management events", sel["Name"])
			},
		},
		{
			name: "update_eds_advanced_event_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "update-adv-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h.h, "UpdateEventDataStore", map[string]any{
					"EventDataStore": edsARN,
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Updated selector",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				assert.Equal(t, "Updated selector", sel["Name"])
			},
		},
		{
			name: "create_eds_with_billing_mode",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name":        "billing-eds",
					"BillingMode": "FIXED_RETENTION_PRICING",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "FIXED_RETENTION_PRICING", resp["BillingMode"])
			},
		},
		{
			name: "create_eds_default_billing_mode",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateEventDataStore", map[string]any{
					"Name": "default-billing-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "EXTENDABLE_RETENTION_PRICING", resp["BillingMode"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestLookupEvents verifies LookupEvents accepts various input parameters and
// returns a well-formed response.
func TestLookupEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "empty_body_returns_empty_list",
			body: nil,
		},
		{
			name: "max_results_param_accepted",
			body: map[string]any{"MaxResults": 10},
		},
		{
			name: "lookup_by_event_name",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventName",
						"AttributeValue": "CreateTrail",
					},
				},
			},
		},
		{
			name: "lookup_by_username",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "Username",
						"AttributeValue": "testuser",
					},
				},
			},
		},
		{
			name: "lookup_by_resource_type",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "ResourceType",
						"AttributeValue": "AWS::CloudTrail::Trail",
					},
				},
			},
		},
		{
			name: "lookup_by_resource_name",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "ResourceName",
						"AttributeValue": "my-trail",
					},
				},
			},
		},
		{
			name: "lookup_by_event_source",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{
						"AttributeKey":   "EventSource",
						"AttributeValue": "cloudtrail.amazonaws.com",
					},
				},
			},
		},
		{
			name: "lookup_with_time_range",
			body: map[string]any{
				"StartTime": 1700000000,
				"EndTime":   1700086400,
			},
		},
		{
			name: "lookup_with_next_token",
			body: map[string]any{
				"NextToken":  "some-continuation-token",
				"MaxResults": 50,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok, "response should contain Events array")
			assert.NotNil(t, events)
		})
	}
}

// TestTrailFields verifies that the trail response includes all expected AWS fields.
func TestTrailFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "trail_has_has_insight_selectors_field",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "fields-trail",
					"S3BucketName": "bucket",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				_, hasField := resp["HasInsightSelectors"]
				assert.True(t, hasField, "trail should have HasInsightSelectors field")
				assert.Equal(t, false, resp["HasInsightSelectors"])
			},
		},
		{
			name: "trail_has_is_organization_trail_field",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				rec := doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "org-fields-trail",
					"S3BucketName": "bucket",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				_, hasField := resp["IsOrganizationTrail"]
				assert.True(t, hasField, "trail should have IsOrganizationTrail field")
				assert.Equal(t, false, resp["IsOrganizationTrail"])
			},
		},
		{
			name: "describe_trails_includes_has_insight_selectors",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "desc-insight-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "PutInsightSelectors", map[string]any{
					"TrailName": "desc-insight-trail",
					"InsightSelectors": []map[string]any{
						{"InsightType": "ApiCallRateInsight"},
					},
				})
				rec := doCloudTrailOp(t, h.h, "DescribeTrails", nil)
				resp := parseCloudTrailResp(t, rec)
				list := resp["trailList"].([]any)
				require.Len(t, list, 1)
				trail := list[0].(map[string]any)
				assert.Equal(t, true, trail["HasInsightSelectors"])
			},
		},
		{
			name: "get_trail_includes_all_standard_fields",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":                       "complete-trail",
					"S3BucketName":               "my-bucket",
					"S3KeyPrefix":                "prefix/",
					"SnsTopicName":               "my-topic",
					"IncludeGlobalServiceEvents": true,
					"IsMultiRegionTrail":         true,
					"EnableLogFileValidation":    true,
				})
				rec := doCloudTrailOp(t, h.h, "GetTrail", map[string]any{
					"Name": "complete-trail",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				trail := resp["Trail"].(map[string]any)
				assert.NotEmpty(t, trail["TrailARN"])
				assert.NotEmpty(t, trail["HomeRegion"])
				assert.Equal(t, true, trail["IncludeGlobalServiceEvents"])
				assert.Equal(t, true, trail["IsMultiRegionTrail"])
				assert.Equal(t, true, trail["LogFileValidationEnabled"])
				assert.Equal(t, false, trail["HasCustomEventSelectors"])
				assert.Equal(t, false, trail["HasInsightSelectors"])
				assert.Equal(t, false, trail["IsOrganizationTrail"])
				assert.Equal(t, "prefix/", trail["S3KeyPrefix"])
				assert.Equal(t, "my-topic", trail["SnsTopicName"])
				assert.NotEmpty(t, trail["SnsTopicARN"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// TestPersistenceWithNewFields verifies that Snapshot/Restore correctly persists
// all new fields introduced in the AWS-accuracy audit.
func TestPersistenceWithNewFields(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a trail with advanced event selectors and insight selectors.
	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "persist-adv-trail",
		"S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "StartLogging", map[string]any{"Name": "persist-adv-trail"})
	doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
		"AdvancedEventSelectors": []map[string]any{
			{
				"Name": "Log all",
				"FieldSelectors": []map[string]any{
					{"Field": "eventCategory", "Equals": []string{"Management"}},
				},
			},
		},
	})
	doCloudTrailOp(t, h, "PutInsightSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
		"InsightSelectors": []map[string]any{
			{"InsightType": "ApiCallRateInsight"},
		},
	})

	// Create an EDS with federation enabled.
	createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":        "persist-fed-eds",
		"BillingMode": "FIXED_RETENTION_PRICING",
	})
	createResp := parseCloudTrailResp(t, createRec)
	edsARN := createResp["EventDataStoreArn"].(string)

	doCloudTrailOp(t, h, "EnableFederation", map[string]any{
		"EventDataStore":    edsARN,
		"FederationRoleArn": "arn:aws:iam::123456789012:role/FedRole",
	})

	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(snap))

	// Verify trail with advanced event selectors is restored.
	getRec := doCloudTrailOp(t, h2, "GetEventSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	advSels, ok := getResp["AdvancedEventSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, advSels, 1)

	// Verify insight selectors are restored.
	insightRec := doCloudTrailOp(t, h2, "GetInsightSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, insightRec.Code)
	insightResp := parseCloudTrailResp(t, insightRec)
	sels, ok := insightResp["InsightSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, sels, 1)

	// Verify EDS federation status is restored.
	getEDSRec := doCloudTrailOp(t, h2, "GetEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, getEDSRec.Code)
	getEDSResp := parseCloudTrailResp(t, getEDSRec)
	assert.Equal(t, "ENABLED", getEDSResp["FederationStatus"])
	assert.Equal(t, "FIXED_RETENTION_PRICING", getEDSResp["BillingMode"])

	// Verify trail status (StartLoggingTime) is also restored.
	statusRec := doCloudTrailOp(t, h2, "GetTrailStatus", map[string]any{
		"Name": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, statusRec.Code)
	statusResp := parseCloudTrailResp(t, statusRec)
	assert.Equal(t, true, statusResp["IsLogging"])
	assert.NotNil(t, statusResp["StartLoggingTime"])
}

// TestHasCustomEventSelectorsTracking verifies HasCustomEventSelectors is updated
// correctly as event selectors are added and removed.
func TestHasCustomEventSelectorsTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *awsAccuracyHandler)
		name string
	}{
		{
			name: "no_selectors_means_no_custom",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "no-custom-trail",
					"S3BucketName": "bucket",
				})
				rec := doCloudTrailOp(t, h.h, "GetTrail", map[string]any{
					"Name": "no-custom-trail",
				})
				resp := parseCloudTrailResp(t, rec)
				trail := resp["Trail"].(map[string]any)
				assert.Equal(t, false, trail["HasCustomEventSelectors"])
			},
		},
		{
			name: "basic_selectors_set_has_custom",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "basic-custom-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "basic-custom-trail",
					"EventSelectors": []map[string]any{
						{"ReadWriteType": "All", "IncludeManagementEvents": true},
					},
				})
				rec := doCloudTrailOp(t, h.h, "GetTrail", map[string]any{
					"Name": "basic-custom-trail",
				})
				resp := parseCloudTrailResp(t, rec)
				trail := resp["Trail"].(map[string]any)
				assert.Equal(t, true, trail["HasCustomEventSelectors"])
			},
		},
		{
			name: "advanced_selectors_set_has_custom",
			ops: func(t *testing.T, h *awsAccuracyHandler) {
				t.Helper()
				doCloudTrailOp(t, h.h, "CreateTrail", map[string]any{
					"Name":         "adv-custom-trail",
					"S3BucketName": "bucket",
				})
				doCloudTrailOp(t, h.h, "PutEventSelectors", map[string]any{
					"TrailName": "adv-custom-trail",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "sel",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
							},
						},
					},
				})
				rec := doCloudTrailOp(t, h.h, "GetTrail", map[string]any{
					"Name": "adv-custom-trail",
				})
				resp := parseCloudTrailResp(t, rec)
				trail := resp["Trail"].(map[string]any)
				assert.Equal(t, true, trail["HasCustomEventSelectors"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := &awsAccuracyHandler{h: newTestCloudTrailHandler()}
			tt.ops(t, h)
		})
	}
}

// awsAccuracyHandler is a thin wrapper used within this test file to hold a handler.
type awsAccuracyHandler struct {
	h *cloudtrail.Handler
}
