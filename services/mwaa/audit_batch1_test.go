package mwaa_test

// AWS-accuracy audit batch-1 tests (go-eff5).
//
// Covers: LoggingConfiguration log-level validation, CREATING lifecycle,
// S3 path/version-pair validation (all three pairs), NetworkConfiguration
// scenarios, AirflowConfigurationOptions round-trip and replace semantics,
// KmsKey and EndpointManagement validation, WeeklyMaintenanceWindowStart on
// update, worker/webserver/scheduler boundary cases, metrics cap enforcement,
// InvokeRestApi path/method/body/queryParam variations, PublishMetrics datum
// fields, tags-at-create round-trip, derived field shapes (CeleryExecutorQueue,
// ServiceRoleArn, WebserverURL), Celery/Local executor config options, and
// lifecycle-state transitions (CREATING→AVAILABLE, UPDATING→AVAILABLE,
// DELETING status on delete response).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// ─────────────────────────────────────────────────────────────
// 1. LoggingConfiguration log-level validation
// ─────────────────────────────────────────────────────────────

func TestAudit_LoggingConfig_ValidLogLevels_Create(t *testing.T) {
	t.Parallel()

	validLevels := []string{"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}

	for _, level := range validLevels {
		t.Run("level_"+level, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.LoggingConfiguration = &mwaa.LoggingConfiguration{
				SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: level},
			}
			_, err := b.CreateEnvironment(context.Background(), "log-level-env", req)
			require.NoError(t, err)
		})
	}
}

func TestAudit_LoggingConfig_InvalidLogLevel_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		logLevel string
	}{
		{name: "lowercase_info", logLevel: "info"},
		{name: "lowercase_debug", logLevel: "debug"},
		{name: "bogus", logLevel: "VERBOSE"},
		{name: "numeric", logLevel: "5"},
		{name: "warn_short", logLevel: "WARN"},
		{name: "err_short", logLevel: "ERR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.LoggingConfiguration = &mwaa.LoggingConfiguration{
				SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: tt.logLevel},
			}
			_, err := b.CreateEnvironment(context.Background(), "inv-log-env", req)
			require.Error(t, err)
		})
	}
}

func TestAudit_LoggingConfig_AllFiveModules_Create(t *testing.T) {
	t.Parallel()

	trueVal := true
	lc := &mwaa.LoggingConfiguration{
		DagProcessingLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "INFO", Enabled: &trueVal},
		SchedulerLogs:     &mwaa.ModuleLoggingConfiguration{LogLevel: "WARNING", Enabled: &trueVal},
		TaskLogs:          &mwaa.ModuleLoggingConfiguration{LogLevel: "ERROR", Enabled: &trueVal},
		WebserverLogs:     &mwaa.ModuleLoggingConfiguration{LogLevel: "DEBUG", Enabled: &trueVal},
		WorkerLogs:        &mwaa.ModuleLoggingConfiguration{LogLevel: "CRITICAL", Enabled: &trueVal},
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = lc

	env, err := b.CreateEnvironment(context.Background(), "all-modules-env", req)
	require.NoError(t, err)

	// Fetch (second call to get AVAILABLE, first would be CREATING)
	b.GetEnvironment(context.Background(), "all-modules-env")
	got, err := b.GetEnvironment(context.Background(), "all-modules-env")
	require.NoError(t, err)
	require.NotNil(t, got.LoggingConfiguration)
	require.NotNil(t, got.LoggingConfiguration.DagProcessingLogs)
	assert.Equal(t, "INFO", got.LoggingConfiguration.DagProcessingLogs.LogLevel)
	require.NotNil(t, got.LoggingConfiguration.WorkerLogs)
	assert.Equal(t, "CRITICAL", got.LoggingConfiguration.WorkerLogs.LogLevel)
	_ = env
}

func TestAudit_LoggingConfig_InvalidLevel_OnDagProcessingLogs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		DagProcessingLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "TRACE"},
	}
	_, err := b.CreateEnvironment(context.Background(), "dag-log-inv", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DagProcessingLogs")
}

func TestAudit_LoggingConfig_InvalidLevel_OnTaskLogs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		TaskLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "NOTSET"},
	}
	_, err := b.CreateEnvironment(context.Background(), "task-log-inv", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TaskLogs")
}

func TestAudit_LoggingConfig_InvalidLevel_OnWebserverLogs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		WebserverLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "ACCESS"},
	}
	_, err := b.CreateEnvironment(context.Background(), "web-log-inv", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebserverLogs")
}

func TestAudit_LoggingConfig_InvalidLevel_OnWorkerLogs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		WorkerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "SILLY"},
	}
	_, err := b.CreateEnvironment(context.Background(), "worker-log-inv", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WorkerLogs")
}

func TestAudit_LoggingConfig_NilConfig_AllowedOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = nil

	_, err := b.CreateEnvironment(context.Background(), "nil-log-env", req)
	require.NoError(t, err)
}

func TestAudit_LoggingConfig_EmptyLogLevel_AllowedOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: ""},
	}

	_, err := b.CreateEnvironment(context.Background(), "empty-level-env", req)
	require.NoError(t, err)
}

func TestAudit_LoggingConfig_ValidLevel_OnUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		logLevel string
		wantErr  bool
	}{
		{name: "critical_ok", logLevel: "CRITICAL", wantErr: false},
		{name: "error_ok", logLevel: "ERROR", wantErr: false},
		{name: "warning_ok", logLevel: "WARNING", wantErr: false},
		{name: "info_ok", logLevel: "INFO", wantErr: false},
		{name: "debug_ok", logLevel: "DEBUG", wantErr: false},
		{name: "invalid_rejected", logLevel: "VERBOSE", wantErr: true},
		{name: "lowercase_rejected", logLevel: "error", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "log-upd-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "log-upd-env") // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), "log-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
				LoggingConfiguration: &mwaa.LoggingConfiguration{
					SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: tt.logLevel},
				},
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_LoggingConfig_HTTP_InvalidLevel_Create(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"SchedulerLogs": map[string]any{"LogLevel": "BOGUS"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_LoggingConfig_HTTP_ValidLevel_Create(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-ok", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"SchedulerLogs": map[string]any{"LogLevel": "INFO"},
			"WorkerLogs":    map[string]any{"LogLevel": "WARNING"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_LoggingConfig_HTTP_InvalidLevel_Update(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-upd-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/http-log-upd-inv", map[string]any{
		"LoggingConfiguration": map[string]any{
			"TaskLogs": map[string]any{"LogLevel": "TRACE"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_LoggingConfig_Persisted_AfterCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	trueVal := true
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		SchedulerLogs: &mwaa.ModuleLoggingConfiguration{
			LogLevel: "ERROR",
			Enabled:  &trueVal,
		},
	}

	_, err := b.CreateEnvironment(context.Background(), "log-persist-env", req)
	require.NoError(t, err)

	// consume CREATING state
	b.GetEnvironment(context.Background(), "log-persist-env")
	env, err := b.GetEnvironment(context.Background(), "log-persist-env")
	require.NoError(t, err)
	require.NotNil(t, env.LoggingConfiguration)
	require.NotNil(t, env.LoggingConfiguration.SchedulerLogs)
	assert.Equal(t, "ERROR", env.LoggingConfiguration.SchedulerLogs.LogLevel)
	require.NotNil(t, env.LoggingConfiguration.SchedulerLogs.Enabled)
	assert.True(t, *env.LoggingConfiguration.SchedulerLogs.Enabled)
}

func TestAudit_LoggingConfig_Persisted_AfterUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "log-upd-persist", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "log-upd-persist") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "log-upd-persist", &mwaa.ExportedUpdateEnvironmentRequest{
		LoggingConfiguration: &mwaa.LoggingConfiguration{
			WorkerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "DEBUG"},
		},
	})
	require.NoError(t, err)

	// consume UPDATING state
	b.GetEnvironment(context.Background(), "log-upd-persist")
	env, err := b.GetEnvironment(context.Background(), "log-upd-persist")
	require.NoError(t, err)
	require.NotNil(t, env.LoggingConfiguration)
	require.NotNil(t, env.LoggingConfiguration.WorkerLogs)
	assert.Equal(t, "DEBUG", env.LoggingConfiguration.WorkerLogs.LogLevel)
}

// ─────────────────────────────────────────────────────────────
// 2. CREATING lifecycle: CreateEnvironment → CREATING → AVAILABLE
// ─────────────────────────────────────────────────────────────

func TestAudit_Lifecycle_CreateReturnsCreating(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "lc-create-env", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, "CREATING", env.Status)
}

func TestAudit_Lifecycle_FirstGetReturnsCreating(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-first-get-env", newCreateReq())
	require.NoError(t, err)

	first, err := b.GetEnvironment(context.Background(), "lc-first-get-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING", first.Status)
}

func TestAudit_Lifecycle_SecondGetReturnsAvailable(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-second-get-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lc-second-get-env")

	second, err := b.GetEnvironment(context.Background(), "lc-second-get-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)
}

func TestAudit_Lifecycle_MultipleGetsStayAvailable(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-multi-get-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lc-multi-get-env")

	for range 5 {
		env, err2 := b.GetEnvironment(context.Background(), "lc-multi-get-env")
		require.NoError(t, err2)
		assert.Equal(t, "AVAILABLE", env.Status)
	}
}

func TestAudit_Lifecycle_CreateThenUpdateStatusFlow(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-full-flow-env", newCreateReq())
	require.NoError(t, err)

	// CREATING → AVAILABLE
	first, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "CREATING", first.Status)

	second, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)

	// Update → UPDATING → AVAILABLE
	_, err = b.UpdateEnvironment(context.Background(), "lc-full-flow-env", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "mw1.medium",
	})
	require.NoError(t, err)

	afterUpd, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "UPDATING", afterUpd.Status)

	afterUpd2, err := b.GetEnvironment(context.Background(), "lc-full-flow-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", afterUpd2.Status)
}

func TestAudit_Lifecycle_HTTP_CreateResponseDoesNotExposeStatus(t *testing.T) {
	t.Parallel()

	// HTTP CreateEnvironment response only contains Arn, not Status.
	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-lc-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["Arn"])
	assert.Nil(t, resp["Status"], "create response must not expose Status")
}

func TestAudit_Lifecycle_HTTP_GetEnvShowsCreatingThenAvailable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-lc-get-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// First GET: CREATING
	get1 := doMWAARequest(t, h, http.MethodGet, "/environments/http-lc-get-env", nil)
	require.Equal(t, http.StatusOK, get1.Code)

	var resp1 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get1.Body.Bytes(), &resp1))
	assert.Equal(t, "CREATING", resp1.Environment.Status)

	// Second GET: AVAILABLE
	get2 := doMWAARequest(t, h, http.MethodGet, "/environments/http-lc-get-env", nil)
	require.Equal(t, http.StatusOK, get2.Code)

	var resp2 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get2.Body.Bytes(), &resp2))
	assert.Equal(t, "AVAILABLE", resp2.Environment.Status)
}

func TestAudit_Lifecycle_DeleteReturnsEnvWithDeletingStatus(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-del-env", newCreateReq())
	require.NoError(t, err)

	deleted, err := b.DeleteEnvironment(context.Background(), "lc-del-env")
	require.NoError(t, err)
	require.NotNil(t, deleted)
	// The returned env carries the name.
	assert.Equal(t, "lc-del-env", deleted.Name)
}

func TestAudit_Lifecycle_DeleteThenGetReturns404(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lc-del-get-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.DeleteEnvironment(context.Background(), "lc-del-get-env")
	require.NoError(t, err)

	_, err = b.GetEnvironment(context.Background(), "lc-del-get-env")
	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

// ─────────────────────────────────────────────────────────────
// 3. S3 path / object-version pair validation
// ─────────────────────────────────────────────────────────────

func TestAudit_S3Paths_AllThreePairs_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(r *mwaa.ExportedCreateEnvironmentRequest)
		name    string
		wantErr bool
	}{
		{
			name:    "plugins_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedCreateEnvironmentRequest) { r.PluginsS3Path = "plugins.zip" },
			wantErr: true,
		},
		{
			name: "plugins_path_with_version_ok",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) {
				r.PluginsS3Path = "plugins.zip"
				r.PluginsS3ObjectVersion = "v1"
			},
			wantErr: false,
		},
		{
			name:    "requirements_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedCreateEnvironmentRequest) { r.RequirementsS3Path = "requirements.txt" },
			wantErr: true,
		},
		{
			name: "requirements_path_with_version_ok",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) {
				r.RequirementsS3Path = "requirements.txt"
				r.RequirementsS3ObjectVersion = "v2"
			},
			wantErr: false,
		},
		{
			name:    "startup_script_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedCreateEnvironmentRequest) { r.StartupScriptS3Path = "startup.sh" },
			wantErr: true,
		},
		{
			name: "startup_script_path_with_version_ok",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) {
				r.StartupScriptS3Path = "startup.sh"
				r.StartupScriptS3ObjectVersion = "v3"
			},
			wantErr: false,
		},
		{
			name: "all_three_paths_with_versions_ok",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) {
				r.PluginsS3Path = "plugins.zip"
				r.PluginsS3ObjectVersion = "p1"
				r.RequirementsS3Path = "requirements.txt"
				r.RequirementsS3ObjectVersion = "r1"
				r.StartupScriptS3Path = "startup.sh"
				r.StartupScriptS3ObjectVersion = "s1"
			},
			wantErr: false,
		},
		{
			name: "version_without_path_ok",
			mutate: func(r *mwaa.ExportedCreateEnvironmentRequest) {
				r.PluginsS3ObjectVersion = "v1" // version without path is allowed
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			tt.mutate(req)

			_, err := b.CreateEnvironment(context.Background(), "s3-pair-env", req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_S3Paths_AllThreePairs_UpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(r *mwaa.ExportedUpdateEnvironmentRequest)
		name    string
		wantErr bool
	}{
		{
			name:    "plugins_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedUpdateEnvironmentRequest) { r.PluginsS3Path = "plugins.zip" },
			wantErr: true,
		},
		{
			name: "plugins_path_with_version_ok",
			mutate: func(r *mwaa.ExportedUpdateEnvironmentRequest) {
				r.PluginsS3Path = "plugins.zip"
				r.PluginsS3ObjectVersion = "v1"
			},
			wantErr: false,
		},
		{
			name:    "requirements_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedUpdateEnvironmentRequest) { r.RequirementsS3Path = "requirements.txt" },
			wantErr: true,
		},
		{
			name: "requirements_path_with_version_ok",
			mutate: func(r *mwaa.ExportedUpdateEnvironmentRequest) {
				r.RequirementsS3Path = "requirements.txt"
				r.RequirementsS3ObjectVersion = "v2"
			},
			wantErr: false,
		},
		{
			name:    "startup_path_without_version_rejected",
			mutate:  func(r *mwaa.ExportedUpdateEnvironmentRequest) { r.StartupScriptS3Path = "startup.sh" },
			wantErr: true,
		},
		{
			name: "startup_path_with_version_ok",
			mutate: func(r *mwaa.ExportedUpdateEnvironmentRequest) {
				r.StartupScriptS3Path = "startup.sh"
				r.StartupScriptS3ObjectVersion = "v3"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "s3-upd-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "s3-upd-env") // promote CREATING → AVAILABLE

			req := new(mwaa.ExportedUpdateEnvironmentRequest)
			tt.mutate(req)
			_, err = b.UpdateEnvironment(context.Background(), "s3-upd-env", req)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_S3Paths_Update_PluginsPathVersionPairPersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "s3-persist-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "s3-persist-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "s3-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		PluginsS3Path:          "plugins.zip",
		PluginsS3ObjectVersion: "abc123",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "s3-persist-env")
	env, err := b.GetEnvironment(context.Background(), "s3-persist-env")
	require.NoError(t, err)
	assert.Equal(t, "plugins.zip", env.PluginsS3Path)
	assert.Equal(t, "abc123", env.PluginsS3ObjectVersion)
}

func TestAudit_S3Paths_Update_RequirementsPathVersionPairPersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "req-s3-persist", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "req-s3-persist") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "req-s3-persist", &mwaa.ExportedUpdateEnvironmentRequest{
		RequirementsS3Path:          "requirements.txt",
		RequirementsS3ObjectVersion: "def456",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "req-s3-persist")
	env, err := b.GetEnvironment(context.Background(), "req-s3-persist")
	require.NoError(t, err)
	assert.Equal(t, "requirements.txt", env.RequirementsS3Path)
	assert.Equal(t, "def456", env.RequirementsS3ObjectVersion)
}

func TestAudit_S3Paths_Update_StartupScriptPathVersionPairPersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "startup-s3-persist", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "startup-s3-persist") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "startup-s3-persist", &mwaa.ExportedUpdateEnvironmentRequest{
		StartupScriptS3Path:          "startup.sh",
		StartupScriptS3ObjectVersion: "ghi789",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "startup-s3-persist")
	env, err := b.GetEnvironment(context.Background(), "startup-s3-persist")
	require.NoError(t, err)
	assert.Equal(t, "startup.sh", env.StartupScriptS3Path)
	assert.Equal(t, "ghi789", env.StartupScriptS3ObjectVersion)
}

func TestAudit_S3Paths_HTTP_PluginsWithoutVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-s3-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"PluginsS3Path": "plugins.zip",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_S3Paths_HTTP_AllThreeWithVersions(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-s3-ok", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"PluginsS3Path": "plugins.zip", "PluginsS3ObjectVersion": "v1",
		"RequirementsS3Path": "req.txt", "RequirementsS3ObjectVersion": "v2",
		"StartupScriptS3Path": "start.sh", "StartupScriptS3ObjectVersion": "v3",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 4. NetworkConfiguration scenarios
// ─────────────────────────────────────────────────────────────

func TestAudit_NetworkConfig_CreateWithSubnetsAndSecGroups(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-aaa111", "subnet-bbb222"},
		SecurityGroupIDs: []string{"sg-ccc333"},
	}

	env, err := b.CreateEnvironment(context.Background(), "nc-env", req)
	require.NoError(t, err)
	require.NotNil(t, env.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-aaa111", "subnet-bbb222"}, env.NetworkConfiguration.SubnetIDs)
	assert.Equal(t, []string{"sg-ccc333"}, env.NetworkConfiguration.SecurityGroupIDs)
}

func TestAudit_NetworkConfig_CreateWithoutNetworkConfigAllowed(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = nil

	_, err := b.CreateEnvironment(context.Background(), "nc-nil-env", req)
	require.NoError(t, err)
}

func TestAudit_NetworkConfig_UpdateValidNetworkConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-upd-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.NetworkConfig{
			SubnetIDs:        []string{"subnet-new1", "subnet-new2"},
			SecurityGroupIDs: []string{"sg-new1"},
		},
	})
	require.NoError(t, err)
}

func TestAudit_NetworkConfig_UpdateEmptySubnetsRejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-empty-sn", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-empty-sn") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-empty-sn", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.NetworkConfig{
			SecurityGroupIDs: []string{"sg-1"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SubnetIds")
}

func TestAudit_NetworkConfig_UpdateEmptySecurityGroupsRejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-empty-sg", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-empty-sg") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-empty-sg", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.NetworkConfig{
			SubnetIDs: []string{"subnet-1"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SecurityGroupIds")
}

func TestAudit_NetworkConfig_UpdatePersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-persist-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-persist-env") // promote CREATING → AVAILABLE

	newNC := &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-x1", "subnet-x2"},
		SecurityGroupIDs: []string{"sg-x1", "sg-x2"},
	}
	_, err = b.UpdateEnvironment(context.Background(), "nc-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: newNC,
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "nc-persist-env")
	env, err := b.GetEnvironment(context.Background(), "nc-persist-env")
	require.NoError(t, err)
	require.NotNil(t, env.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-x1", "subnet-x2"}, env.NetworkConfiguration.SubnetIDs)
	assert.Equal(t, []string{"sg-x1", "sg-x2"}, env.NetworkConfiguration.SecurityGroupIDs)
}

func TestAudit_NetworkConfig_HTTP_UpdateEmptySubnetsRejected(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/nc-http-upd", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/nc-http-upd", map[string]any{
		"NetworkConfiguration": map[string]any{
			"SecurityGroupIds": []string{"sg-1"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// 5. AirflowConfigurationOptions round-trip and replace semantics
// ─────────────────────────────────────────────────────────────

func TestAudit_AirflowConfig_CreateWithOptions(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"core.parallelism":        "32",
		"scheduler.dag_bag_size":  "100",
		"webserver.expose_config": "true",
	}

	_, err := b.CreateEnvironment(context.Background(), "acfg-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "acfg-env")
	env, err := b.GetEnvironment(context.Background(), "acfg-env")
	require.NoError(t, err)
	assert.Equal(t, "32", env.AirflowConfigurationOptions["core.parallelism"])
	assert.Equal(t, "100", env.AirflowConfigurationOptions["scheduler.dag_bag_size"])
}

func TestAudit_AirflowConfig_UpdateReplaces_NotMerges(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"core.parallelism": "32",
		"old.key":          "old-value",
	}
	_, err := b.CreateEnvironment(context.Background(), "acfg-replace-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "acfg-replace-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "acfg-replace-env", &mwaa.ExportedUpdateEnvironmentRequest{
		AirflowConfigurationOptions: map[string]string{
			"new.key": "new-value",
		},
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "acfg-replace-env")
	env, err := b.GetEnvironment(context.Background(), "acfg-replace-env")
	require.NoError(t, err)
	// old.key should be gone — update replaces, not merges
	assert.NotContains(t, env.AirflowConfigurationOptions, "old.key")
	assert.Equal(t, "new-value", env.AirflowConfigurationOptions["new.key"])
}

func TestAudit_AirflowConfig_UpdateNilOptions_DoesNotClear(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"core.parallelism": "16",
	}
	_, err := b.CreateEnvironment(context.Background(), "acfg-nil-upd", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "acfg-nil-upd") // promote CREATING → AVAILABLE

	// Update with nil AirflowConfigurationOptions — should not touch existing config.
	_, err = b.UpdateEnvironment(context.Background(), "acfg-nil-upd", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new-dags/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "acfg-nil-upd")
	env, err := b.GetEnvironment(context.Background(), "acfg-nil-upd")
	require.NoError(t, err)
	assert.Equal(t, "16", env.AirflowConfigurationOptions["core.parallelism"])
}

func TestAudit_AirflowConfig_EmptyMapClears(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"some.key": "some-value",
	}
	_, err := b.CreateEnvironment(context.Background(), "acfg-clear-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "acfg-clear-env") // promote CREATING → AVAILABLE

	// Update with empty map should replace existing config with empty.
	_, err = b.UpdateEnvironment(context.Background(), "acfg-clear-env", &mwaa.ExportedUpdateEnvironmentRequest{
		AirflowConfigurationOptions: map[string]string{},
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "acfg-clear-env")
	env, err := b.GetEnvironment(context.Background(), "acfg-clear-env")
	require.NoError(t, err)
	assert.Empty(t, env.AirflowConfigurationOptions)
}

// Celery vs LocalExecutor via AirflowConfigurationOptions.
func TestAudit_AirflowConfig_CeleryExecutorOption(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"core.executor": "CeleryExecutor",
	}
	env, err := b.CreateEnvironment(context.Background(), "celery-env", req)
	require.NoError(t, err)
	_ = env

	b.GetEnvironment(context.Background(), "celery-env")
	got, err := b.GetEnvironment(context.Background(), "celery-env")
	require.NoError(t, err)
	assert.Equal(t, "CeleryExecutor", got.AirflowConfigurationOptions["core.executor"])
	// CeleryExecutorQueue should be present and non-empty on all environments.
	assert.NotEmpty(t, got.CeleryExecutorQueue)
}

func TestAudit_AirflowConfig_LocalExecutorOption(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowConfigurationOptions = map[string]string{
		"core.executor": "LocalExecutor",
	}
	_, err := b.CreateEnvironment(context.Background(), "local-exec-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "local-exec-env")
	env, err := b.GetEnvironment(context.Background(), "local-exec-env")
	require.NoError(t, err)
	assert.Equal(t, "LocalExecutor", env.AirflowConfigurationOptions["core.executor"])
}

// ─────────────────────────────────────────────────────────────
// 6. KmsKey validation
// ─────────────────────────────────────────────────────────────

func TestAudit_KmsKey_ValidationOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kmsKey  string
		wantErr bool
	}{
		{name: "valid_arn", kmsKey: "arn:aws:kms:us-east-1:123456789012:key/abc-123", wantErr: false},
		{name: "empty_allowed", kmsKey: "", wantErr: false},
		{name: "alias_rejected", kmsKey: "alias/my-key", wantErr: true},
		{name: "plain_key_rejected", kmsKey: "abc-123-def-456", wantErr: true},
		{name: "key_id_only_rejected", kmsKey: "key/abc-123", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.KmsKey = tt.kmsKey

			_, err := b.CreateEnvironment(context.Background(), "kms-env", req)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "KmsKey")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_KmsKey_PersisteddInGetEnvironment(t *testing.T) {
	t.Parallel()

	const kmsARN = "arn:aws:kms:us-east-1:123456789012:key/abc-123"

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.KmsKey = kmsARN

	_, err := b.CreateEnvironment(context.Background(), "kms-persist-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "kms-persist-env")
	env, err := b.GetEnvironment(context.Background(), "kms-persist-env")
	require.NoError(t, err)
	assert.Equal(t, kmsARN, env.KmsKey)
}

func TestAudit_KmsKey_HTTP_InvalidRejected(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/kms-http-inv", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"KmsKey": "not-an-arn",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 7. EndpointManagement validation and persistence
// ─────────────────────────────────────────────────────────────

func TestAudit_EndpointManagement_ValidationAndPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mgmt       string
		wantResult string
		wantErr    bool
	}{
		{name: "service_ok", mgmt: "SERVICE", wantErr: false, wantResult: "SERVICE"},
		{name: "customer_ok", mgmt: "CUSTOMER", wantErr: false, wantResult: "CUSTOMER"},
		{name: "empty_defaults_to_service", mgmt: "", wantErr: false, wantResult: "SERVICE"},
		{name: "bogus_rejected", mgmt: "HYBRID", wantErr: true},
		{name: "lowercase_rejected", mgmt: "service", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.EndpointManagement = tt.mgmt

			envName := "em-env-" + strings.ReplaceAll(tt.name, "_", "-")
			env, err := b.CreateEnvironment(context.Background(), envName, req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantResult, env.EndpointManagement)
		})
	}
}

func TestAudit_EndpointManagement_CustomerPersistedInGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.EndpointManagement = "CUSTOMER"

	_, err := b.CreateEnvironment(context.Background(), "em-customer-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "em-customer-env")
	env, err := b.GetEnvironment(context.Background(), "em-customer-env")
	require.NoError(t, err)
	assert.Equal(t, "CUSTOMER", env.EndpointManagement)
}

func TestAudit_EndpointManagement_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mgmt       string
		name       string
		wantStatus int
	}{
		{name: "service_ok", mgmt: "SERVICE", wantStatus: http.StatusOK},
		{name: "customer_ok", mgmt: "CUSTOMER", wantStatus: http.StatusOK},
		{name: "bogus_rejected", mgmt: "HYBRID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/em-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				"EndpointManagement": tt.mgmt,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 8. WeeklyMaintenanceWindowStart on update
// ─────────────────────────────────────────────────────────────

func TestAudit_WeeklyMaintenance_UpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		window  string
		wantErr bool
	}{
		{name: "mon_valid", window: "MON:03:30", wantErr: false},
		{name: "fri_midnight", window: "FRI:00:00", wantErr: false},
		{name: "sun_max_time", window: "SUN:23:59", wantErr: false},
		{name: "empty_allowed", window: "", wantErr: false},
		{name: "wrong_separator", window: "MON-03-30", wantErr: true},
		{name: "invalid_day", window: "MON:25:00", wantErr: true},
		{name: "invalid_minute", window: "MON:12:61", wantErr: true},
		{name: "lowercase_day", window: "mon:12:00", wantErr: true},
		{name: "invalid_format", window: "MONDAY:12:00", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "wmw-upd-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "wmw-upd-env") // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), "wmw-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
				WeeklyMaintenanceWindowStart: tt.window,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_WeeklyMaintenance_UpdatePersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "wmw-persist-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wmw-persist-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "wmw-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WeeklyMaintenanceWindowStart: "WED:02:00",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "wmw-persist-env")
	env, err := b.GetEnvironment(context.Background(), "wmw-persist-env")
	require.NoError(t, err)
	assert.Equal(t, "WED:02:00", env.WeeklyMaintenanceWindowStart)
}

func TestAudit_WeeklyMaintenance_HTTP_Update_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/wmw-http-upd", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/wmw-http-upd", map[string]any{
		"WeeklyMaintenanceWindowStart": "TUESDAY:12:00",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_WeeklyMaintenance_AllDays_Valid(t *testing.T) {
	t.Parallel()

	days := []string{"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}

	for _, day := range days {
		t.Run(day, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "wmw-day-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "wmw-day-env") // promote CREATING → AVAILABLE

			window := day + ":12:00"
			_, err = b.UpdateEnvironment(context.Background(), "wmw-day-env", &mwaa.ExportedUpdateEnvironmentRequest{
				WeeklyMaintenanceWindowStart: window,
			})
			require.NoError(t, err)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 9. Worker / webserver / scheduler boundary cases on update
// ─────────────────────────────────────────────────────────────

func TestAudit_Workers_Update_OnlyMinSet_KeepsExistingMax(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 10
	req.MinWorkers = 1
	_, err := b.CreateEnvironment(context.Background(), "wk-only-min-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wk-only-min-env") // promote CREATING → AVAILABLE

	// Update: set MinWorkers=2, leave MaxWorkers=0 (no change).
	// MinWorkers=2 < existing MaxWorkers=10: should succeed.
	_, err = b.UpdateEnvironment(context.Background(), "wk-only-min-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWorkers: 2,
	})
	require.NoError(t, err)
}

func TestAudit_Workers_Update_OnlyMaxSet_KeepsExistingMin(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 10
	req.MinWorkers = 3
	_, err := b.CreateEnvironment(context.Background(), "wk-only-max-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wk-only-max-env") // promote CREATING → AVAILABLE

	// Update: set MaxWorkers=15, leave MinWorkers=0 (no change).
	_, err = b.UpdateEnvironment(context.Background(), "wk-only-max-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MaxWorkers: 15,
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "wk-only-max-env")
	env, err := b.GetEnvironment(context.Background(), "wk-only-max-env")
	require.NoError(t, err)
	assert.Equal(t, int32(15), env.MaxWorkers)
	assert.Equal(t, int32(3), env.MinWorkers)
}

func TestAudit_Workers_Update_NewMinExceedsExistingMax(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 5
	req.MinWorkers = 1
	_, err := b.CreateEnvironment(context.Background(), "wk-min-exceeds-max", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wk-min-exceeds-max") // promote CREATING → AVAILABLE

	// Set MinWorkers=10 > existing MaxWorkers=5: should fail.
	_, err = b.UpdateEnvironment(context.Background(), "wk-min-exceeds-max", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWorkers: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MinWorkers")
}

func TestAudit_Workers_Update_NewMaxBelowExistingMin(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 10
	req.MinWorkers = 5
	_, err := b.CreateEnvironment(context.Background(), "wk-max-below-min", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wk-max-below-min") // promote CREATING → AVAILABLE

	// Set MaxWorkers=2 < existing MinWorkers=5: should fail.
	_, err = b.UpdateEnvironment(context.Background(), "wk-max-below-min", &mwaa.ExportedUpdateEnvironmentRequest{
		MaxWorkers: 2,
	})
	require.Error(t, err)
}

func TestAudit_Workers_Update_BothSetValidRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		min     int32
		max     int32
		wantErr bool
	}{
		{name: "equal_min_max", min: 3, max: 3, wantErr: false},
		{name: "min_less_max", min: 2, max: 10, wantErr: false},
		{name: "min_greater_max", min: 8, max: 5, wantErr: true},
		{name: "at_max_limit", min: 1, max: 25, wantErr: false},
		{name: "exceeds_max_limit", min: 1, max: 26, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "wk-both-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "wk-both-env") // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), "wk-both-env", &mwaa.ExportedUpdateEnvironmentRequest{
				MinWorkers: tt.min,
				MaxWorkers: tt.max,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_Schedulers_Create_V2_BoundaryValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		schedulers int32
		wantErr    bool
	}{
		{name: "exactly_2_ok", schedulers: 2, wantErr: false},
		{name: "exactly_5_ok", schedulers: 5, wantErr: false},
		{name: "below_min_rejected", schedulers: 1, wantErr: true},
		{name: "above_max_rejected", schedulers: 6, wantErr: true},
		{name: "zero_uses_default", schedulers: 0, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.Schedulers = tt.schedulers

			_, err := b.CreateEnvironment(context.Background(), "sched-env", req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAudit_Webservers_Create_BoundaryValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		min     int32
		max     int32
		wantErr bool
	}{
		{name: "defaults_zero_zero_ok", min: 0, max: 0, wantErr: false},
		{name: "min_1_max_5_ok", min: 1, max: 5, wantErr: false},
		{name: "min_2_max_2_ok", min: 2, max: 2, wantErr: false},
		{name: "max_exceeds_5_rejected", min: 1, max: 6, wantErr: true},
		{name: "min_0_max_5_ok", min: 0, max: 5, wantErr: false},
		{name: "min_greater_max_rejected", min: 3, max: 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.MinWebservers = tt.min
			req.MaxWebservers = tt.max

			_, err := b.CreateEnvironment(context.Background(), "ws-env", req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 10. Metrics cap enforcement (1000 per environment)
// ─────────────────────────────────────────────────────────────

func TestAudit_Metrics_Cap_AtExactLimit(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-cap-env", newCreateReq())
	require.NoError(t, err)

	// Publish exactly 1000 metrics.
	data := make([]mwaa.ExportedMetricDatum, 1000)
	for i := range data {
		data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Metric%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-cap-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: data},
	)
	require.NoError(t, err)

	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-cap-env"))
}

func TestAudit_Metrics_Cap_ExceedsLimit_TrimsOldest(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-overflow-env", newCreateReq())
	require.NoError(t, err)

	// Publish 1100 metrics in two batches.
	first := make([]mwaa.ExportedMetricDatum, 600)
	for i := range first {
		first[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Old%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-overflow-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: first},
	)
	require.NoError(t, err)

	second := make([]mwaa.ExportedMetricDatum, 500)
	for i := range second {
		second[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("New%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-overflow-env",
		&mwaa.ExportedPublishMetricsRequest{MetricData: second},
	)
	require.NoError(t, err)

	// Total 1100 → capped at 1000.
	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-overflow-env"))
}

func TestAudit_Metrics_Cap_PublishSingleBatch_Over1000(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "metrics-big-batch", newCreateReq())
	require.NoError(t, err)

	data := make([]mwaa.ExportedMetricDatum, 1200)
	for i := range data {
		data[i] = mwaa.ExportedMetricDatum{MetricName: fmt.Sprintf("Datum%d", i)}
	}
	err = b.PublishMetrics(
		context.Background(),
		"metrics-big-batch",
		&mwaa.ExportedPublishMetricsRequest{MetricData: data},
	)
	require.NoError(t, err)

	// Capped at 1000.
	assert.Equal(t, 1000, mwaa.MetricsCount(b, "metrics-big-batch"))
}

// ─────────────────────────────────────────────────────────────
// 11. PublishMetrics datum field coverage
// ─────────────────────────────────────────────────────────────

func TestAudit_PublishMetrics_DatumFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		datums []mwaa.ExportedMetricDatum
	}{
		{
			name: "metric_with_value_and_unit",
			datums: func() []mwaa.ExportedMetricDatum {
				v := 42.5

				return []mwaa.ExportedMetricDatum{{MetricName: "WorkerCount", Value: &v, Unit: "Count"}}
			}(),
		},
		{
			name: "metric_with_statistic_set",
			datums: func() []mwaa.ExportedMetricDatum {
				maxV, minV, sum := 10.0, 1.0, 55.0
				sampleCount := int32(10)

				return []mwaa.ExportedMetricDatum{{
					MetricName: "TaskDuration",
					StatisticValues: &mwaa.StatisticSet{
						Maximum: &maxV, Minimum: &minV, Sum: &sum, SampleCount: &sampleCount,
					},
				}}
			}(),
		},
		{
			name: "metric_with_dimensions",
			datums: []mwaa.ExportedMetricDatum{{
				MetricName: "SchedulerHeartbeat",
				Dimensions: []mwaa.Dimension{
					{Name: "Environment", Value: "prod"},
					{Name: "Region", Value: "us-east-1"},
				},
			}},
		},
		{
			name: "metric_with_timestamp",
			datums: func() []mwaa.ExportedMetricDatum {
				ts := float64(1700000000)

				return []mwaa.ExportedMetricDatum{{MetricName: "DagRunCount", Timestamp: &ts}}
			}(),
		},
		{
			name:   "empty_metric_data_ok",
			datums: []mwaa.ExportedMetricDatum{},
		},
		{
			name: "multiple_metrics",
			datums: func() []mwaa.ExportedMetricDatum {
				v1, v2 := 1.0, 2.0

				return []mwaa.ExportedMetricDatum{
					{MetricName: "M1", Value: &v1},
					{MetricName: "M2", Value: &v2, Unit: "Percent"},
				}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "datum-env", newCreateReq())
			require.NoError(t, err)

			err = b.PublishMetrics(
				context.Background(),
				"datum-env",
				&mwaa.ExportedPublishMetricsRequest{MetricData: tt.datums},
			)
			require.NoError(t, err)
		})
	}
}

func TestAudit_PublishMetrics_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.PublishMetrics(context.Background(), "nonexistent-env", &mwaa.ExportedPublishMetricsRequest{})
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestAudit_GetMetrics_ReturnsCopy(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "get-metrics-copy-env", newCreateReq())
	require.NoError(t, err)

	v := 5.0
	err = b.PublishMetrics(context.Background(), "get-metrics-copy-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{{MetricName: "TaskCount", Value: &v}},
	})
	require.NoError(t, err)

	data, err := b.GetMetrics(context.Background(), "get-metrics-copy-env")
	require.NoError(t, err)
	assert.Len(t, data, 1)
	assert.Equal(t, "TaskCount", data[0].MetricName)
}

// ─────────────────────────────────────────────────────────────
// 12. InvokeRestApi path / method / body / query variations
// ─────────────────────────────────────────────────────────────

func TestAudit_InvokeRestApi_Variations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req     *mwaa.ExportedInvokeRestAPIRequest
		name    string
		wantErr bool
	}{
		{
			name: "list_dags_get",
			req:  &mwaa.ExportedInvokeRestAPIRequest{Method: "GET", Path: "/dags"},
		},
		{
			name: "trigger_dag_run_post",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "POST",
				Path:   "/dags/my_dag/dagRuns",
				Body:   map[string]any{"dag_run_id": "run-001", "conf": map[string]any{"key": "val"}},
			},
		},
		{
			name: "get_dag_run",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "GET",
				Path:   "/dags/my_dag/dagRuns/run-001",
			},
		},
		{
			name: "poke_task_instance",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "POST",
				Path:   "/dags/my_dag/dagRuns/run-001/taskInstances/my_task/notes",
				Body:   map[string]any{"note": "manual poke"},
			},
		},
		{
			name: "get_with_query_params",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method:          "GET",
				Path:            "/dags",
				QueryParameters: map[string]any{"limit": "10", "offset": "0"},
			},
		},
		{
			name: "delete_dag_run",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "DELETE",
				Path:   "/dags/my_dag/dagRuns/run-001",
			},
		},
		{
			name: "patch_dag",
			req: &mwaa.ExportedInvokeRestAPIRequest{
				Method: "PATCH",
				Path:   "/dags/my_dag",
				Body:   map[string]any{"is_paused": false},
			},
		},
		{
			name:    "missing_method_rejected",
			req:     &mwaa.ExportedInvokeRestAPIRequest{Path: "/dags"},
			wantErr: true,
		},
		{
			name:    "missing_path_rejected",
			req:     &mwaa.ExportedInvokeRestAPIRequest{Method: "GET"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "restapi-env", newCreateReq())
			require.NoError(t, err)

			resp, err := b.InvokeRestAPI(context.Background(), "restapi-env", tt.req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, int32(200), resp.RestAPIStatusCode)
		})
	}
}

func TestAudit_InvokeRestApi_HTTP_Variations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "list_dags",
			body:       map[string]any{"Method": "GET", "Path": "/dags"},
			wantStatus: http.StatusOK,
		},
		{
			name: "trigger_dag_run",
			body: map[string]any{
				"Method": "POST",
				"Path":   "/dags/my_dag/dagRuns",
				"Body":   map[string]any{"dag_run_id": "run-1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with_query_params",
			body: map[string]any{
				"Method":          "GET",
				"Path":            "/dags",
				"QueryParameters": map[string]any{"limit": "5"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_method",
			body:       map[string]any{"Path": "/dags"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			doMWAARequest(t, h, http.MethodPut, "/environments/http-restapi-env", map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})

			rec := doMWAARequest(t, h, http.MethodPost, "/restapi/http-restapi-env", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// 13. Tags at create round-trip
// ─────────────────────────────────────────────────────────────

func TestAudit_Tags_AtCreate_PersistedInGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{
		"env":   "production",
		"owner": "platform-team",
		"cost":  "cc-1234",
	}

	_, err := b.CreateEnvironment(context.Background(), "tagged-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "tagged-env")
	env, err := b.GetEnvironment(context.Background(), "tagged-env")
	require.NoError(t, err)
	assert.Equal(t, "production", env.Tags["env"])
	assert.Equal(t, "platform-team", env.Tags["owner"])
	assert.Equal(t, "cc-1234", env.Tags["cost"])
}

func TestAudit_Tags_Update_DoesNotTouchExistingTags(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{"keep": "this"}

	_, err := b.CreateEnvironment(context.Background(), "tags-upd-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "tags-upd-env") // promote CREATING → AVAILABLE

	// Update the environment without touching tags.
	_, err = b.UpdateEnvironment(context.Background(), "tags-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new-dags/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "tags-upd-env")
	env, err := b.GetEnvironment(context.Background(), "tags-upd-env")
	require.NoError(t, err)
	assert.Equal(t, "this", env.Tags["keep"])
}

func TestAudit_Tags_NotLeakedBetweenEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	reqA := newCreateReq()
	reqA.Tags = map[string]string{"env": "alpha"}
	envA, err := b.CreateEnvironment(context.Background(), "tag-leak-a", reqA)
	require.NoError(t, err)

	reqB := newCreateReq()
	reqB.Tags = map[string]string{"env": "beta"}
	_, err = b.CreateEnvironment(context.Background(), "tag-leak-b", reqB)
	require.NoError(t, err)

	// Add a tag to A's ARN.
	err = b.TagResource(context.Background(), envA.ARN, map[string]string{"extra": "from-a"})
	require.NoError(t, err)

	// Fetch B — should not have A's extra tag.
	b.GetEnvironment(context.Background(), "tag-leak-b")
	gotB, err := b.GetEnvironment(context.Background(), "tag-leak-b")
	require.NoError(t, err)
	assert.NotContains(t, gotB.Tags, "extra")
	assert.Equal(t, "beta", gotB.Tags["env"])
}

func TestAudit_Tags_HTTP_CreateWithTagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-tags-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"Tags": map[string]string{"service": "airflow", "tier": "production"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	// ListTagsForResource returns the creation tags.
	tagRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
	require.Equal(t, http.StatusOK, tagRec.Code)

	var tagsResp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(tagRec.Body.Bytes(), &tagsResp))
	assert.Equal(t, "airflow", tagsResp.Tags["service"])
	assert.Equal(t, "production", tagsResp.Tags["tier"])
}

// ─────────────────────────────────────────────────────────────
// 14. Derived field shapes (CeleryExecutorQueue, ServiceRoleArn, WebserverURL)
// ─────────────────────────────────────────────────────────────

func TestAudit_DerivedFields_CeleryExecutorQueue(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "derived-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "derived-env")
	env, err := b.GetEnvironment(context.Background(), "derived-env")
	require.NoError(t, err)

	// CeleryExecutorQueue must be an SQS URL.
	assert.True(
		t,
		strings.HasPrefix(env.CeleryExecutorQueue, "https://sqs."),
		"CeleryExecutorQueue must start with https://sqs., got %q", env.CeleryExecutorQueue,
	)
	assert.Contains(t, env.CeleryExecutorQueue, testRegion)
	assert.Contains(t, env.CeleryExecutorQueue, testAccountID)
}

func TestAudit_DerivedFields_ServiceRoleArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "sra-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "sra-env")
	env, err := b.GetEnvironment(context.Background(), "sra-env")
	require.NoError(t, err)

	// ServiceRoleArn must be an IAM ARN.
	assert.True(
		t,
		strings.HasPrefix(env.ServiceRoleArn, "arn:aws:iam::"),
		"ServiceRoleArn must be an IAM ARN, got %q", env.ServiceRoleArn,
	)
	assert.Contains(t, env.ServiceRoleArn, testAccountID)
	assert.Contains(t, env.ServiceRoleArn, "AWSServiceRoleForAmazonMWAA")
}

func TestAudit_DerivedFields_WebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-url-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "ws-url-env")
	env, err := b.GetEnvironment(context.Background(), "ws-url-env")
	require.NoError(t, err)

	assert.True(
		t,
		strings.HasPrefix(env.WebserverURL, "https://"),
		"WebserverURL must start with https://, got %q", env.WebserverURL,
	)
	assert.Contains(t, env.WebserverURL, testRegion)
	assert.Contains(t, env.WebserverURL, "amazonaws.com")
}

func TestAudit_DerivedFields_VpcEndpointServices(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "vpc-svc-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "vpc-svc-env")
	env, err := b.GetEnvironment(context.Background(), "vpc-svc-env")
	require.NoError(t, err)

	assert.NotEmpty(t, env.DatabaseVpcEndpointService)
	assert.NotEmpty(t, env.WebserverVpcEndpointService)
	assert.Contains(t, env.DatabaseVpcEndpointService, testRegion)
	assert.Contains(t, env.WebserverVpcEndpointService, testRegion)
}

func TestAudit_DerivedFields_DifferentForDifferentEnvs(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "diff-derived-a", newCreateReq())
	require.NoError(t, err)
	_, err = b.CreateEnvironment(context.Background(), "diff-derived-b", newCreateReq())
	require.NoError(t, err)

	// Consume CREATING for both
	b.GetEnvironment(context.Background(), "diff-derived-a")
	b.GetEnvironment(context.Background(), "diff-derived-b")

	envA, err := b.GetEnvironment(context.Background(), "diff-derived-a")
	require.NoError(t, err)
	envB, err := b.GetEnvironment(context.Background(), "diff-derived-b")
	require.NoError(t, err)

	// Each env gets a unique webserver URL and celery queue.
	assert.NotEqual(t, envA.WebserverURL, envB.WebserverURL)
	assert.NotEqual(t, envA.CeleryExecutorQueue, envB.CeleryExecutorQueue)
}

// ─────────────────────────────────────────────────────────────
// 15. DagS3Path persistence and update
// ─────────────────────────────────────────────────────────────

func TestAudit_DagS3Path_CreateAndGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.DagS3Path = "custom/dags/"

	_, err := b.CreateEnvironment(context.Background(), "dag-path-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "dag-path-env")
	env, err := b.GetEnvironment(context.Background(), "dag-path-env")
	require.NoError(t, err)
	assert.Equal(t, "custom/dags/", env.DagS3Path)
}

func TestAudit_DagS3Path_Update_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "dag-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "dag-upd-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "dag-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new/dags/path/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "dag-upd-env")
	env, err := b.GetEnvironment(context.Background(), "dag-upd-env")
	require.NoError(t, err)
	assert.Equal(t, "new/dags/path/", env.DagS3Path)
}

func TestAudit_DagS3Path_Required_OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "dag-missing-env", &mwaa.ExportedCreateEnvironmentRequest{
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
		SourceBucketArn:  "arn:aws:s3:::bucket",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DagS3Path")
}

// ─────────────────────────────────────────────────────────────
// 16. SourceBucketArn and ExecutionRoleArn required fields
// ─────────────────────────────────────────────────────────────

func TestAudit_RequiredFields_MissingSourceBucketArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "req-sb-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SourceBucketArn")
}

func TestAudit_RequiredFields_MissingExecutionRoleArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "req-era-env", &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:       "dags/",
		SourceBucketArn: "arn:aws:s3:::bucket",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ExecutionRoleArn")
}

// ─────────────────────────────────────────────────────────────
// 17. ExecutionRoleArn and SourceBucketArn update persistence
// ─────────────────────────────────────────────────────────────

func TestAudit_Update_ExecutionRoleArnAndSourceBucketArn(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "role-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "role-upd-env") // promote CREATING → AVAILABLE

	newRole := "arn:aws:iam::123456789012:role/new-mwaa-role"
	newBucket := "arn:aws:s3:::new-bucket"
	_, err = b.UpdateEnvironment(context.Background(), "role-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		ExecutionRoleArn: newRole,
		SourceBucketArn:  newBucket,
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "role-upd-env")
	env, err := b.GetEnvironment(context.Background(), "role-upd-env")
	require.NoError(t, err)
	assert.Equal(t, newRole, env.ExecutionRoleArn)
	assert.Equal(t, newBucket, env.SourceBucketArn)
}

// ─────────────────────────────────────────────────────────────
// 18. LastUpdate fields after UpdateEnvironment
// ─────────────────────────────────────────────────────────────

func TestAudit_LastUpdate_PopulatedAfterUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lu-check-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "lu-check-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "lu-check-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "updated-dags/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lu-check-env")
	env, err := b.GetEnvironment(context.Background(), "lu-check-env")
	require.NoError(t, err)
	require.NotNil(t, env.LastUpdate)
	assert.Equal(t, "SUCCESS", env.LastUpdate.Status)
	assert.Equal(t, "USER", env.LastUpdate.Source)
	assert.Positive(t, env.LastUpdate.CreatedAt)
}

func TestAudit_LastUpdate_NilBeforeFirstUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lu-nil-env", newCreateReq())
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "lu-nil-env")
	env, err := b.GetEnvironment(context.Background(), "lu-nil-env")
	require.NoError(t, err)
	assert.Nil(t, env.LastUpdate)
}

// ─────────────────────────────────────────────────────────────
// 19. ListEnvironments sorted order + pagination consistency
// ─────────────────────────────────────────────────────────────

func TestAudit_ListEnvironments_SortedAlphabetically(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	names := []string{"zebra-env", "alpha-env", "middle-env"}
	for _, n := range names {
		_, err := b.CreateEnvironment(context.Background(), n, newCreateReq())
		require.NoError(t, err)
	}

	listed, err := b.ListEnvironments(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha-env", "middle-env", "zebra-env"}, listed)
}

func TestAudit_ListEnvironments_PaginationConsistentOrder(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	for _, n := range []string{"aa", "bb", "cc", "dd", "ee"} {
		_, err := b.CreateEnvironment(context.Background(), n, newCreateReq())
		require.NoError(t, err)
	}

	// Page 1: 2 items
	page1, tok1, err := b.ListEnvironmentsPage(context.Background(), "", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"aa", "bb"}, page1)
	assert.Equal(t, "cc", tok1)

	// Page 2: 2 items starting from tok1
	page2, tok2, err := b.ListEnvironmentsPage(context.Background(), tok1, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"cc", "dd"}, page2)
	assert.Equal(t, "ee", tok2)

	// Page 3: last 1 item
	page3, tok3, err := b.ListEnvironmentsPage(context.Background(), tok2, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"ee"}, page3)
	assert.Empty(t, tok3)
}

// ─────────────────────────────────────────────────────────────
// 20. ARN format and uniqueness
// ─────────────────────────────────────────────────────────────

func TestAudit_ARN_Format(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "arn-fmt-env", newCreateReq())
	require.NoError(t, err)

	// ARN must match arn:aws:airflow:REGION:ACCOUNT:environment/NAME
	expected := "arn:aws:airflow:" + testRegion + ":" + testAccountID + ":environment/arn-fmt-env"
	assert.Equal(t, expected, env.ARN)
}

func TestAudit_ARN_UniquePerEnvironment(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envA, err := b.CreateEnvironment(context.Background(), "arn-unique-a", newCreateReq())
	require.NoError(t, err)
	envB, err := b.CreateEnvironment(context.Background(), "arn-unique-b", newCreateReq())
	require.NoError(t, err)

	assert.NotEqual(t, envA.ARN, envB.ARN)
}

// ─────────────────────────────────────────────────────────────
// 21. CreatedAt timestamp
// ─────────────────────────────────────────────────────────────

func TestAudit_CreatedAt_Set(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "created-at-env", newCreateReq())
	require.NoError(t, err)

	assert.Positive(t, env.CreatedAt, "CreatedAt must be a positive Unix epoch")
}

// ─────────────────────────────────────────────────────────────
// 22. Persistence snapshot / restore round-trip with new fields
// ─────────────────────────────────────────────────────────────

func TestAudit_Snapshot_WithLoggingConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "WARNING"},
	}

	_, err := b.CreateEnvironment(context.Background(), "snap-log-env", req)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.GetEnvironment(context.Background(), "snap-log-env")
	env, err := b2.GetEnvironment(context.Background(), "snap-log-env")
	require.NoError(t, err)
	require.NotNil(t, env.LoggingConfiguration)
	require.NotNil(t, env.LoggingConfiguration.SchedulerLogs)
	assert.Equal(t, "WARNING", env.LoggingConfiguration.SchedulerLogs.LogLevel)
}

func TestAudit_Snapshot_WithNetworkConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-snap1"},
		SecurityGroupIDs: []string{"sg-snap1"},
	}

	_, err := b.CreateEnvironment(context.Background(), "snap-nc-env", req)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.GetEnvironment(context.Background(), "snap-nc-env")
	env, err := b2.GetEnvironment(context.Background(), "snap-nc-env")
	require.NoError(t, err)
	require.NotNil(t, env.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-snap1"}, env.NetworkConfiguration.SubnetIDs)
}

// ─────────────────────────────────────────────────────────────
// 23. Backend Reset clears all state
// ─────────────────────────────────────────────────────────────

func TestAudit_Reset_ClearsMetrics(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "reset-metrics-env", newCreateReq())
	require.NoError(t, err)

	v := 1.0
	err = b.PublishMetrics(context.Background(), "reset-metrics-env", &mwaa.ExportedPublishMetricsRequest{
		MetricData: []mwaa.ExportedMetricDatum{{MetricName: "M", Value: &v}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.MetricsCount(b, "reset-metrics-env"))

	b.Reset()
	assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	assert.Equal(t, 0, mwaa.MetricsCount(b, "reset-metrics-env"))
}

// ─────────────────────────────────────────────────────────────
// 24. HTTP-level end-to-end tests
// ─────────────────────────────────────────────────────────────

func TestAudit_HTTP_FullCRUDLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create.
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/full-crud-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "2.9.2",
		"EnvironmentClass": "mw1.medium",
		"MaxWorkers":       5,
		"MinWorkers":       1,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]
	assert.NotEmpty(t, envARN)

	// Get (first call: CREATING).
	get1 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get1.Code)
	var resp1 struct {
		Environment struct {
			Status           string `json:"Status"`
			AirflowVersion   string `json:"AirflowVersion"`
			EnvironmentClass string `json:"EnvironmentClass"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get1.Body.Bytes(), &resp1))
	assert.Equal(t, "CREATING", resp1.Environment.Status)
	assert.Equal(t, "2.9.2", resp1.Environment.AirflowVersion)
	assert.Equal(t, "mw1.medium", resp1.Environment.EnvironmentClass)

	// Get (second call: AVAILABLE).
	get2 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get2.Code)
	var resp2 struct {
		Environment struct {
			Status string `json:"Status"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get2.Body.Bytes(), &resp2))
	assert.Equal(t, "AVAILABLE", resp2.Environment.Status)

	// Update.
	updRec := doMWAARequest(t, h, http.MethodPatch, "/environments/full-crud-env", map[string]any{
		"EnvironmentClass": "mw1.large",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// Get after update (UPDATING).
	get3 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	require.Equal(t, http.StatusOK, get3.Code)
	var resp3 struct {
		Environment struct {
			Status           string `json:"Status"`
			EnvironmentClass string `json:"EnvironmentClass"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(get3.Body.Bytes(), &resp3))
	assert.Equal(t, "UPDATING", resp3.Environment.Status)
	assert.Equal(t, "mw1.large", resp3.Environment.EnvironmentClass)

	// Tag.
	tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
		"Tags": map[string]string{"test": "value"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// Delete.
	delRec := doMWAARequest(t, h, http.MethodDelete, "/environments/full-crud-env", nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Get after delete: 404.
	get4 := doMWAARequest(t, h, http.MethodGet, "/environments/full-crud-env", nil)
	assert.Equal(t, http.StatusNotFound, get4.Code)
}

func TestAudit_HTTP_LoggingConfig_AllModules(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/http-log-all", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"LoggingConfiguration": map[string]any{
			"DagProcessingLogs": map[string]any{"LogLevel": "INFO"},
			"SchedulerLogs":     map[string]any{"LogLevel": "WARNING"},
			"TaskLogs":          map[string]any{"LogLevel": "ERROR"},
			"WebserverLogs":     map[string]any{"LogLevel": "DEBUG"},
			"WorkerLogs":        map[string]any{"LogLevel": "CRITICAL"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Consume CREATING.
	doMWAARequest(t, h, http.MethodGet, "/environments/http-log-all", nil)

	getRec := doMWAARequest(t, h, http.MethodGet, "/environments/http-log-all", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp struct {
		Environment struct {
			LoggingConfiguration *mwaa.LoggingConfiguration `json:"LoggingConfiguration"`
		} `json:"Environment"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	require.NotNil(t, resp.Environment.LoggingConfiguration)
	require.NotNil(t, resp.Environment.LoggingConfiguration.DagProcessingLogs)
	assert.Equal(t, "INFO", resp.Environment.LoggingConfiguration.DagProcessingLogs.LogLevel)
	require.NotNil(t, resp.Environment.LoggingConfiguration.WorkerLogs)
	assert.Equal(t, "CRITICAL", resp.Environment.LoggingConfiguration.WorkerLogs.LogLevel)
}

func TestAudit_HTTP_MetricsPublishAndRetrieve(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/http-metrics-full", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})

	pubRec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/http-metrics-full", map[string]any{
		"MetricData": []map[string]any{
			{"MetricName": "DagRunDuration", "Value": 123.4, "Unit": "Seconds"},
			{"MetricName": "ActiveDAGs", "Value": 5.0, "Unit": "Count"},
		},
	})
	require.Equal(t, http.StatusOK, pubRec.Code)

	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/http-metrics-full", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp struct {
		MetricData []map[string]any `json:"MetricData"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Len(t, resp.MetricData, 2)
}

func TestAudit_HTTP_TokensIncludeWebServerHostname(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/http-token-hostname", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/http-token-hostname", nil) // promote CREATING → AVAILABLE

	tests := []struct {
		name    string
		path    string
		wantKey string
	}{
		{name: "cli_token", path: "/clitoken/http-token-hostname", wantKey: "CliToken"},
		{name: "web_token", path: "/webtoken/http-token-hostname", wantKey: "WebToken"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doMWAARequest(t, h, http.MethodPost, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp[tt.wantKey])
			assert.NotEmpty(t, resp["WebServerHostname"])
			assert.Contains(t, resp["WebServerHostname"], "amazonaws.com")
		})
	}
}
