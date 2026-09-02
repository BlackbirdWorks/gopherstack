package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func assertErrorType(t *testing.T, body []byte, wantType string) {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, wantType, resp["__type"])
}

func assertAppStatus(t *testing.T, body []byte, wantStatus string) {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	detail, ok := resp["ApplicationDetail"].(map[string]any)
	require.True(t, ok, "response missing ApplicationDetail")
	assert.Equal(t, wantStatus, detail["ApplicationStatus"])
}

func TestKAV2_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			input: map[string]any{
				"ApplicationName":      "test-app",
				"RuntimeEnvironment":   "FLINK-1_18",
				"ServiceExecutionRole": "arn:aws:iam::000000000000:role/service-role",
			},
			wantStatus: http.StatusOK,
			wantName:   "test-app",
		},
		{
			name:       "invalid json",
			input:      nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.name == "invalid json" {
				rec := doRawKAV2Request(t, h, "CreateApplication", []byte("not-json"))
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			var body any = tt.input
			rec := doKAV2Request(t, h, "CreateApplication", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				detail, ok := out["ApplicationDetail"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, detail["ApplicationName"])
				assert.Equal(t, "READY", detail["ApplicationStatus"])
			}
		})
	}
}

func TestKAV2_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	input := map[string]any{
		"ApplicationName":    "dup-app",
		"RuntimeEnvironment": "FLINK-1_18",
	}

	rec := doKAV2Request(t, h, "CreateApplication", input)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplication", input)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestKAV2_CreateApplication_RequiresName(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"RuntimeEnvironment": "FLINK-1_18",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestKAV2_CreateApplication_RequiresRuntimeEnvironment(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName": "test-app",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestKAV2_CreateApplication_InlineConfiguration verifies that CreateApplication
// seeds inputs/outputs/reference-data-sources/VPC configs/CloudWatch logging
// options supplied inline via ApplicationConfiguration and
// CloudWatchLoggingOptions -- the standard Terraform/CloudFormation
// provisioning pattern (a single CreateApplication call, not a
// CreateApplication followed by a series of Add* calls). Before this fix,
// ApplicationConfiguration/CloudWatchLoggingOptions were silently discarded:
// CreateApplication returned 200 OK but every application came back with
// empty configuration.
func TestKAV2_CreateApplication_InlineConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":      "inline-config-app",
		"RuntimeEnvironment":   "SQL-1_0",
		"ServiceExecutionRole": "arn:aws:iam::000000000000:role/svc",
		"ApplicationConfiguration": map[string]any{
			"SqlApplicationConfiguration": map[string]any{
				"Inputs": []map[string]any{
					{
						"NamePrefix": "SOURCE_SQL_STREAM",
						"KinesisStreamsInput": map[string]any{
							"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/in",
						},
					},
				},
				"Outputs": []map[string]any{
					{
						"Name": "OUTPUT",
						"KinesisStreamsOutput": map[string]any{
							"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						},
					},
				},
				"ReferenceDataSources": []map[string]any{
					{
						"TableName": "REF_TABLE",
						"S3ReferenceDataSource": map[string]any{
							"BucketARN": "arn:aws:s3:::bucket",
							"FileKey":   "key",
						},
					},
				},
			},
			"VpcConfigurations": []map[string]any{
				{
					"SubnetIds":        []string{"subnet-1"},
					"SecurityGroupIds": []string{"sg-1"},
				},
			},
		},
		"CloudWatchLoggingOptions": []map[string]any{
			{"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	detail := out["ApplicationDetail"].(map[string]any)

	// Inline config must not bump the version past 1 -- real AWS keeps a
	// freshly created application, even with inline config, at version 1.
	assert.InEpsilon(t, 1.0, detail["ApplicationVersionId"], 1e-9)

	appConfig, ok := detail["ApplicationConfigurationDescription"].(map[string]any)
	require.True(t, ok, "expected ApplicationConfigurationDescription to be populated")
	sqlConfig, ok := appConfig["SqlApplicationConfigurationDescription"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, sqlConfig["InputDescriptions"], 1)
	assert.Len(t, sqlConfig["OutputDescriptions"], 1)
	assert.Len(t, sqlConfig["ReferenceDataSourceDescriptions"], 1)

	// VpcConfigurationDescriptions lives inside
	// ApplicationConfigurationDescription in real AWS's ApplicationDetail
	// shape, not at the top level (there is no top-level
	// ApplicationDetail.VpcConfigurationDescriptions field) -- see appConfigDesc.
	assert.Len(t, appConfig["VpcConfigurationDescriptions"], 1)
	assert.Len(t, detail["CloudWatchLoggingOptionDescriptions"], 1)
}

// TestKAV2_NonNilSlices_InDetailOutput verifies DescribeApplication's
// response always includes ApplicationName/ApplicationStatus even when no
// optional configuration has been added.
func TestKAV2_NonNilSlices_InDetailOutput(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "slice-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	detail := out["ApplicationDetail"].(map[string]any)
	// Tags should be present (may be empty but not nil)
	assert.Contains(t, detail, "ApplicationName")
	assert.Equal(t, "READY", detail["ApplicationStatus"])
}

func TestKAV2_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		create     bool
		wantStatus int
	}{
		{
			name:       "found",
			appName:    "existing-app",
			create:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			appName:    "missing-app",
			create:     false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.create {
				createRec := doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "FLINK-1_18",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			rec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
				"ApplicationName": tt.appName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestKAV2_ListApplications(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	for _, name := range []string{"app1", "app2"} {
		rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    name,
			"RuntimeEnvironment": "FLINK-1_18",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doKAV2Request(t, h, "ListApplications", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, ok := out["ApplicationSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2)
}

func TestKAV2_DeleteApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	rec := doKAV2Request(t, h, "DeleteApplication", map[string]any{
		"ApplicationName": "del-app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "del-app",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestKAV2_StartStopApplication(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "lifecycle-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	rec := doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	detail := out["ApplicationDetail"].(map[string]any)
	assert.Equal(t, "RUNNING", detail["ApplicationStatus"])

	rec = doKAV2Request(t, h, "StopApplication", map[string]any{
		"ApplicationName": "lifecycle-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestKAV2_StartStopUpdate_ReturnOperationId verifies that StartApplication,
// StopApplication, and UpdateApplication surface a non-empty OperationId,
// which real AWS clients use to poll DescribeApplicationOperation /
// ListApplicationOperations for the request's outcome.
func TestKAV2_StartStopUpdate_ReturnOperationId(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "opid-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})

	startRec := doKAV2Request(t, h, "StartApplication", map[string]any{"ApplicationName": "opid-app"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut["OperationId"])

	stopRec := doKAV2Request(t, h, "StopApplication", map[string]any{"ApplicationName": "opid-app"})
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopOut))
	assert.NotEmpty(t, stopOut["OperationId"])

	updateRec := doKAV2Request(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "opid-app",
		"ApplicationDescription":      "updated",
		"CurrentApplicationVersionId": 1,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	assert.NotEmpty(t, updateOut["OperationId"])
}

func TestKAV2_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantRole   string
		rawBody    []byte
		wantStatus int
	}{
		{
			name: "update_service_execution_role",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "upd-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName":             "upd-app",
				"ServiceExecutionRoleUpdate":  "arn:aws:iam::000000000000:role/new-role",
				"CurrentApplicationVersionId": 1,
			},
			wantStatus: http.StatusOK,
			wantRole:   "arn:aws:iam::000000000000:role/new-role",
		},
		{
			name:       "not_found",
			body:       map[string]any{"ApplicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_json",
			rawBody:    []byte("bad json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "version_mismatch",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "upd-app-conflict",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName":             "upd-app-conflict",
				"ServiceExecutionRoleUpdate":  "arn:aws:iam::000000000000:role/should-not-apply",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			if tt.rawBody != nil {
				rec = doRawKAV2Request(t, h, "UpdateApplication", tt.rawBody)
			} else {
				rec = doKAV2Request(t, h, "UpdateApplication", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRole != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				detail := out["ApplicationDetail"].(map[string]any)
				assert.Equal(t, tt.wantRole, detail["ServiceExecutionRole"])
			}
		})
	}
}

// ─── Lifecycle: state-transition parity with real AWS ──────────────────────

// TestLifecycle_StartApplication verifies StartApplication state transitions
// match real AWS Kinesis Analytics v2 behavior:
//   - READY → RUNNING succeeds (200)
//   - RUNNING → RUNNING fails with ResourceInUseException (409)
func TestLifecycle_StartApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "ready_to_running",
			preStart:   false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "already_running",
			preStart:   true,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "lifecycle-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "lifecycle-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "StartApplication", map[string]any{
				"ApplicationName": "lifecycle-app",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestLifecycle_StopApplication verifies StopApplication state transitions
// match real AWS Kinesis Analytics v2 behavior:
//   - RUNNING → READY succeeds (200)
//   - READY → READY fails with ResourceInUseException (409)
func TestLifecycle_StopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "running_to_ready",
			preStart:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "already_ready",
			preStart:   false,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "lifecycle-stop-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "lifecycle-stop-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "StopApplication", map[string]any{
				"ApplicationName": "lifecycle-stop-app",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestLifecycle_CreateApplicationSnapshot verifies snapshot creation requires
// the application to be in RUNNING state, matching real AWS behavior.
func TestLifecycle_CreateApplicationSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "running_succeeds",
			preStart:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ready_fails",
			preStart:   false,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "snap-lifecycle-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "snap-lifecycle-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
				"ApplicationName": "snap-lifecycle-app",
				"SnapshotName":    "snap-1",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestLifecycle_StartStopRoundTrip verifies a full READY→RUNNING→READY cycle.
func TestLifecycle_StartStopRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "roundtrip-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify initial state is READY.
	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "READY")

	// Start → RUNNING.
	rec = doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "RUNNING")

	// Stop → READY.
	rec = doKAV2Request(t, h, "StopApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "READY")
}

// TestLifecycle_SnapshotDuplicateName verifies duplicate snapshot name
// returns ResourceInUseException when app is RUNNING.
func TestLifecycle_SnapshotDuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "dup-snap-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "dup-snap-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "dup-snap-app",
		"SnapshotName":    "snap-dup",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "dup-snap-app",
		"SnapshotName":    "snap-dup",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
	assertErrorType(t, rec.Body.Bytes(), "ResourceInUseException")
}
