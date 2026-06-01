package mwaa_test

// AWS-accuracy audit batch-2 tests (go-r648).
//
// Covers: WeeklyMaintenanceWindowStart on create (validation + persistence),
// create-time MinWorkers>MaxWorkers rejection, resolved default values stored
// correctly (workers, webservers, schedulers), schedulers on update (v2/v1
// boundary), webserver bounds on update, transient status promotion
// (CREATING_SNAPSHOT/UPDATE_ROLLING_BACK/PENDING), GetMetrics env isolation,
// GetMetrics HTTP response shape, ListTagsForResource HTTP 404, LoggingConfig
// Enabled bool pointer round-trip, tags deep-copy safety, ARN index size
// consistency, and ListEnvironments MaxResults validation.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// ─────────────────────────────────────────────────────────────
// 1. WeeklyMaintenanceWindowStart on CREATE – validation
// ─────────────────────────────────────────────────────────────

func TestAuditB2_WeeklyMaint_Create_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "monday_midnight", value: "MON:00:00"},
		{name: "sunday_end_of_day", value: "SUN:23:59"},
		{name: "tuesday_noon", value: "TUE:12:30"},
		{name: "wednesday_midday", value: "WED:06:45"},
		{name: "thursday", value: "THU:18:00"},
		{name: "friday", value: "FRI:09:15"},
		{name: "saturday", value: "SAT:21:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.WeeklyMaintenanceWindowStart = tt.value
			_, err := b.CreateEnvironment(testRegion, testAccountID, "wmw-ok-env", req)
			require.NoError(t, err)
		})
	}
}

func TestAuditB2_WeeklyMaint_Create_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "hour_24", value: "MON:24:00"},
		{name: "minute_60", value: "MON:00:60"},
		{name: "lowercase_day", value: "mon:00:00"},
		{name: "only_two_parts", value: "MON:00"},
		{name: "invalid_day", value: "XYZ:00:00"},
		{name: "empty_string_ok_actually_skipped", value: ""}, // empty = not set = no error
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.WeeklyMaintenanceWindowStart = tt.value
			_, err := b.CreateEnvironment(testRegion, testAccountID, "wmw-inv-env", req)

			if tt.value == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestAuditB2_WeeklyMaint_Create_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.WeeklyMaintenanceWindowStart = "FRI:03:30"
	_, err := b.CreateEnvironment(testRegion, testAccountID, "wmw-persist-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment("wmw-persist-env")
	require.NoError(t, err)
	assert.Equal(t, "FRI:03:30", env.WeeklyMaintenanceWindowStart)
}

func TestAuditB2_WeeklyMaint_Create_HTTP_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/wmw-http-env", map[string]any{
		"DagS3Path":                    "dags/",
		"ExecutionRoleArn":             "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":              "arn:aws:s3:::b",
		"WeeklyMaintenanceWindowStart": "MON:25:00",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 2. Create-time MinWorkers > MaxWorkers rejection
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Create_MinWorkersExceedsMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		min     int32
		max     int32
		wantErr bool
	}{
		{name: "min_5_max_3_rejected", min: 5, max: 3, wantErr: true},
		{name: "min_1_max_1_ok", min: 1, max: 1, wantErr: false},
		{name: "min_10_max_25_ok", min: 10, max: 25, wantErr: false},
		{name: "min_20_max_10_rejected", min: 20, max: 10, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.MinWorkers = tt.min
			req.MaxWorkers = tt.max
			_, err := b.CreateEnvironment(testRegion, testAccountID, "worker-range-env", req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 3. Default values stored correctly on create
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Defaults_WorkersStoredOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "defaults-env", newCreateReq())
	require.NoError(t, err)

	env, err := b.GetEnvironment("defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(10), env.MaxWorkers, "default MaxWorkers should be 10")
	assert.Equal(t, int32(1), env.MinWorkers, "default MinWorkers should be 1")
}

func TestAuditB2_Defaults_WebserversStoredOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "ws-defaults-env", newCreateReq())
	require.NoError(t, err)

	env, err := b.GetEnvironment("ws-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(2), env.MaxWebservers, "default MaxWebservers should be 2")
	assert.Equal(t, int32(2), env.MinWebservers, "default MinWebservers should be 2")
}

func TestAuditB2_Defaults_SchedulersV2OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "2.9.2"
	_, err := b.CreateEnvironment(testRegion, testAccountID, "sched-v2-defaults-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment("sched-v2-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(2), env.Schedulers, "default Schedulers for v2 should be 2")
}

func TestAuditB2_Defaults_SchedulersV1OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	_, err := b.CreateEnvironment(testRegion, testAccountID, "sched-v1-defaults-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment("sched-v1-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(1), env.Schedulers, "default Schedulers for v1 should be 1")
}

// ─────────────────────────────────────────────────────────────
// 4. Schedulers on UPDATE – boundary values
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Schedulers_Update_V2Boundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		schedulers int32
		wantErr    bool
	}{
		{name: "v2_min_2_ok", schedulers: 2, wantErr: false},
		{name: "v2_max_5_ok", schedulers: 5, wantErr: false},
		{name: "v2_mid_3_ok", schedulers: 3, wantErr: false},
		{name: "v2_below_min_1_rejected", schedulers: 1, wantErr: true},
		{name: "v2_above_max_6_rejected", schedulers: 6, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(testRegion, testAccountID, "sched-upd-env", newCreateReq())
			require.NoError(t, err)

			_, err = b.UpdateEnvironment("sched-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
				Schedulers:     tt.schedulers,
				AirflowVersion: "2.10.3",
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAuditB2_Schedulers_Update_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "sched-persist-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("sched-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		Schedulers:     4,
		AirflowVersion: "2.10.3",
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment("sched-persist-env")
	require.NoError(t, err)
	assert.Equal(t, int32(4), env.Schedulers)
}

// ─────────────────────────────────────────────────────────────
// 5. Webserver bounds on UPDATE
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Webservers_Update_MinExceedsMax(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "ws-upd-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("ws-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 4,
		MaxWebservers: 2,
	})
	require.Error(t, err)
}

func TestAuditB2_Webservers_Update_ValidRange(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "ws-upd-ok-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("ws-upd-ok-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 1,
		MaxWebservers: 5,
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment("ws-upd-ok-env")
	require.NoError(t, err)
	assert.Equal(t, int32(1), env.MinWebservers)
	assert.Equal(t, int32(5), env.MaxWebservers)
}

func TestAuditB2_Webservers_Update_MaxExceeds5_Rejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "ws-upd-over-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("ws-upd-over-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 1,
		MaxWebservers: 6,
	})
	require.Error(t, err)
}

// ─────────────────────────────────────────────────────────────
// 6. Transient status promotion on GetEnvironment
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Status_CreatingSnapshot_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("snapshot-env")
	env.Status = "CREATING_SNAPSHOT"

	got, err := b.GetEnvironment("snapshot-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING_SNAPSHOT", got.Status, "first Get returns the transient status")

	got2, err := b.GetEnvironment("snapshot-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status, "second Get promotes to AVAILABLE")
}

func TestAuditB2_Status_UpdateRollingBack_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("rollback-env")
	env.Status = "UPDATE_ROLLING_BACK"

	got, err := b.GetEnvironment("rollback-env")
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLING_BACK", got.Status)

	got2, err := b.GetEnvironment("rollback-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status)
}

func TestAuditB2_Status_Pending_PromotedOnGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env := b.AddEnvironmentInternal("pending-env")
	env.Status = "PENDING"

	got, err := b.GetEnvironment("pending-env")
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status)

	got2, err := b.GetEnvironment("pending-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", got2.Status)
}

// Statuses that should NOT be promoted (terminal/steady states).
func TestAuditB2_Status_Terminal_NotPromoted(t *testing.T) {
	t.Parallel()

	terminalStatuses := []string{"AVAILABLE", "CREATE_FAILED", "UPDATE_FAILED", "UNAVAILABLE", "ERROR"}

	for _, status := range terminalStatuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("terminal-env-" + status)
			env.Status = status

			got, err := b.GetEnvironment("terminal-env-" + status)
			require.NoError(t, err)
			assert.Equal(t, status, got.Status, "terminal status %q must not be promoted", status)

			got2, err := b.GetEnvironment("terminal-env-" + status)
			require.NoError(t, err)
			assert.Equal(t, status, got2.Status, "terminal status %q must not be promoted on second Get", status)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 7. GetMetrics – environment isolation
// ─────────────────────────────────────────────────────────────

func TestAuditB2_GetMetrics_IsolatedBetweenEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for _, name := range []string{"metrics-env-a", "metrics-env-b"} {
		_, err := b.CreateEnvironment(testRegion, testAccountID, name, newCreateReq())
		require.NoError(t, err)
	}

	err := b.PublishMetrics("metrics-env-a", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{
			{MetricName: "OnlyForA"},
		},
	})
	require.NoError(t, err)

	dataB, err := b.GetMetrics("metrics-env-b")
	require.NoError(t, err)
	assert.Empty(t, dataB, "metrics for env-b must not contain env-a's metrics")
}

func TestAuditB2_GetMetrics_EmptyBeforePublish(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(testRegion, testAccountID, "no-metrics-env", newCreateReq())
	require.NoError(t, err)

	data, err := b.GetMetrics("no-metrics-env")
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestAuditB2_GetMetrics_HTTP_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/metrics-shape-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	pubRec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/metrics-shape-env",
		map[string]any{
			"MetricData": []any{
				map[string]any{"MetricName": "TestMetric"},
			},
		},
	)
	require.Equal(t, http.StatusOK, pubRec.Code)

	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/metrics-shape-env", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	_, ok := resp["MetricData"]
	assert.True(t, ok, "response must have MetricData key")

	metricData, ok := resp["MetricData"].([]any)
	require.True(t, ok)
	assert.Len(t, metricData, 1)
}

func TestAuditB2_GetMetrics_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/does-not-exist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 8. ListTagsForResource HTTP scenarios
// ─────────────────────────────────────────────────────────────

func TestAuditB2_ListTagsForResource_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	unknownARN := "arn:aws:airflow:us-east-1:123456789012:environment/does-not-exist"
	rec := doMWAARequest(t, h, http.MethodGet, "/tags/"+unknownARN, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAuditB2_ListTagsForResource_HTTP_EmptyTagsShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/notags-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	rec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["Tags"]
	assert.True(t, ok, "response must have Tags key even when empty")
}

// ─────────────────────────────────────────────────────────────
// 9. LoggingConfiguration Enabled bool pointer round-trip
// ─────────────────────────────────────────────────────────────

func TestAuditB2_LoggingConfig_Enabled_RoundTrip(t *testing.T) {
	t.Parallel()

	trueVal := true
	falseVal := false

	tests := []struct {
		enabled *bool
		name    string
	}{
		{name: "enabled_true", enabled: &trueVal},
		{name: "enabled_false", enabled: &falseVal},
		{name: "enabled_nil", enabled: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.LoggingConfiguration = &mwaa.LoggingConfiguration{
				SchedulerLogs: &mwaa.ModuleLoggingConfiguration{
					LogLevel: "INFO",
					Enabled:  tt.enabled,
				},
			}
			_, err := b.CreateEnvironment(testRegion, testAccountID, "logging-enabled-env", req)
			require.NoError(t, err)

			env, err := b.GetEnvironment("logging-enabled-env")
			require.NoError(t, err)
			require.NotNil(t, env.LoggingConfiguration)
			require.NotNil(t, env.LoggingConfiguration.SchedulerLogs)

			got := env.LoggingConfiguration.SchedulerLogs.Enabled
			if tt.enabled == nil {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.Equal(t, *tt.enabled, *got)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 10. Tags deep-copy safety
// ─────────────────────────────────────────────────────────────

func TestAuditB2_Tags_MutationDoesNotAffectStoredEnv(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{"original": "value"}
	_, err := b.CreateEnvironment(testRegion, testAccountID, "deep-copy-env", req)
	require.NoError(t, err)

	env1, err := b.GetEnvironment("deep-copy-env")
	require.NoError(t, err)

	// Mutate the returned tags.
	env1.Tags["injected"] = "malicious"

	env2, err := b.GetEnvironment("deep-copy-env")
	require.NoError(t, err)
	assert.NotContains(t, env2.Tags, "injected",
		"mutation of returned tags must not affect the stored environment")
}

func TestAuditB2_NetworkConfig_MutationDoesNotAffectStoredEnv(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-aaa"},
		SecurityGroupIDs: []string{"sg-111"},
	}
	_, err := b.CreateEnvironment(testRegion, testAccountID, "nc-copy-env", req)
	require.NoError(t, err)

	env1, err := b.GetEnvironment("nc-copy-env")
	require.NoError(t, err)

	// Replace the pointer entirely.
	env1.NetworkConfiguration = nil

	env2, err := b.GetEnvironment("nc-copy-env")
	require.NoError(t, err)
	require.NotNil(t, env2.NetworkConfiguration,
		"stored NetworkConfiguration must survive mutation of returned copy")
	assert.Equal(t, []string{"subnet-aaa"}, env2.NetworkConfiguration.SubnetIDs)
}

// ─────────────────────────────────────────────────────────────
// 11. ARN index consistency
// ─────────────────────────────────────────────────────────────

func TestAuditB2_ARNIndex_GrowsOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))

	for i := range 3 {
		_, err := b.CreateEnvironment(testRegion, testAccountID,
			fmt.Sprintf("arn-env-%d", i), newCreateReq())
		require.NoError(t, err)
	}

	assert.Equal(t, 3, mwaa.ARNIndexSize(b))
}

func TestAuditB2_ARNIndex_ShrinksOnDelete(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	_, err := b.CreateEnvironment(testRegion, testAccountID, "arn-del-env", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.ARNIndexSize(b))

	_, err = b.DeleteEnvironment("arn-del-env")
	require.NoError(t, err)
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))
}

// ─────────────────────────────────────────────────────────────
// 12. ListEnvironments HTTP – MaxResults validation
// ─────────────────────────────────────────────────────────────

func TestAuditB2_ListEnvironments_HTTP_MaxResults_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantCode   int
	}{
		{name: "max_results_1", maxResults: "1", wantCode: http.StatusOK},
		{name: "max_results_50", maxResults: "50", wantCode: http.StatusOK},
		{name: "max_results_100", maxResults: "100", wantCode: http.StatusOK},
		{name: "max_results_101_rejected", maxResults: "101", wantCode: http.StatusBadRequest},
		{name: "max_results_0_rejected", maxResults: "0", wantCode: http.StatusBadRequest},
		{name: "max_results_negative_rejected", maxResults: "-1", wantCode: http.StatusBadRequest},
		{name: "max_results_non_numeric", maxResults: "abc", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodGet,
				"/environments?MaxResults="+tt.maxResults, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAuditB2_ListEnvironments_HTTP_NoMaxResults_UsesDefault(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/environments", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["Environments"]
	assert.True(t, ok, "response must have Environments key")
}

func TestAuditB2_ListEnvironments_HTTP_NextToken_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	for i := range 5 {
		rec := doMWAARequest(t, h, http.MethodPut,
			fmt.Sprintf("/environments/page-env-%02d", i),
			map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
				"SourceBucketArn":  "arn:aws:s3:::b",
			},
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Request page 1 with size 3.
	rec1 := doMWAARequest(t, h, http.MethodGet, "/environments?MaxResults=3", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	envs1 := resp1["Environments"].([]any)
	assert.Len(t, envs1, 3)

	nextToken, ok := resp1["NextToken"].(string)
	require.True(t, ok, "page 1 must include NextToken when more results exist")
	require.NotEmpty(t, nextToken)

	// Request page 2 using the NextToken.
	rec2 := doMWAARequest(t, h, http.MethodGet,
		"/environments?MaxResults=3&NextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	envs2 := resp2["Environments"].([]any)
	assert.Len(t, envs2, 2, "second page should have the remaining 2 environments")
	_, hasToken := resp2["NextToken"]
	assert.False(t, hasToken, "last page must not include NextToken")
}

// ─────────────────────────────────────────────────────────────
// 13. UntagResource backend – 404 on unknown ARN
// ─────────────────────────────────────────────────────────────

func TestAuditB2_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.UntagResource("arn:aws:airflow:us-east-1:123456789012:environment/ghost", []string{"k"})
	require.Error(t, err)
}

func TestAuditB2_UntagResource_MultipleKeys(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(testRegion, testAccountID, "multi-untag-env", newCreateReq())
	require.NoError(t, err)

	err = b.TagResource(env.ARN, map[string]string{"a": "1", "b": "2", "c": "3"})
	require.NoError(t, err)

	err = b.UntagResource(env.ARN, []string{"a", "c"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(env.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, tags)
}

// ─────────────────────────────────────────────────────────────
// 14. EnvironmentCount helper mirrors backend state
// ─────────────────────────────────────────────────────────────

func TestAuditB2_EnvironmentCount_CreateDelete(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	assert.Equal(t, 0, mwaa.EnvironmentCount(b))

	_, err := b.CreateEnvironment(testRegion, testAccountID, "count-env-1", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.EnvironmentCount(b))

	_, err = b.CreateEnvironment(testRegion, testAccountID, "count-env-2", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 2, mwaa.EnvironmentCount(b))

	_, err = b.DeleteEnvironment("count-env-1")
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.EnvironmentCount(b))
}

// ─────────────────────────────────────────────────────────────
// 15. HTTP snapshot: defaults visible via GetEnvironment HTTP
// ─────────────────────────────────────────────────────────────

func TestAuditB2_HTTP_GetEnvironment_DefaultsPresent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/snap-defaults-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Second GET to get AVAILABLE state.
	_ = doMWAARequest(t, h, http.MethodGet, "/environments/snap-defaults-env", nil)
	getRec := doMWAARequest(t, h, http.MethodGet, "/environments/snap-defaults-env", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	env := resp["Environment"].(map[string]any)
	assert.InEpsilon(t, float64(10), env["MaxWorkers"], 0.001, "default MaxWorkers=10 should be visible via HTTP")
	assert.InEpsilon(t, float64(1), env["MinWorkers"], 0.001, "default MinWorkers=1 should be visible via HTTP")
	assert.Equal(t, "2.10.3", env["AirflowVersion"])
	assert.Equal(t, "mw1.small", env["EnvironmentClass"])
	assert.Equal(t, "AVAILABLE", env["Status"])
}
