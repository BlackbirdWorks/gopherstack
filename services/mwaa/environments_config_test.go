package mwaa_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestLoggingConfig_ValidLogLevels_Create(t *testing.T) {
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

func TestLoggingConfig_InvalidLogLevel_Create(t *testing.T) {
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

func TestLoggingConfig_AllFiveModules_Create(t *testing.T) {
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

func TestLoggingConfig_InvalidLevel_OnDagProcessingLogs(t *testing.T) {
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

func TestLoggingConfig_InvalidLevel_OnTaskLogs(t *testing.T) {
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

func TestLoggingConfig_InvalidLevel_OnWebserverLogs(t *testing.T) {
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

func TestLoggingConfig_InvalidLevel_OnWorkerLogs(t *testing.T) {
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

func TestLoggingConfig_NilConfig_AllowedOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = nil

	_, err := b.CreateEnvironment(context.Background(), "nil-log-env", req)
	require.NoError(t, err)
}

func TestLoggingConfig_EmptyLogLevel_AllowedOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: ""},
	}

	_, err := b.CreateEnvironment(context.Background(), "empty-level-env", req)
	require.NoError(t, err)
}

func TestLoggingConfig_ValidLevel_OnUpdate(t *testing.T) {
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

func TestLoggingConfig_Persisted_AfterCreate(t *testing.T) {
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

func TestLoggingConfig_Persisted_AfterUpdate(t *testing.T) {
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

func TestLoggingConfig_Enabled_RoundTrip(t *testing.T) {
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
			_, err := b.CreateEnvironment(context.Background(), "logging-enabled-env", req)
			require.NoError(t, err)

			env, err := b.GetEnvironment(context.Background(), "logging-enabled-env")
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

func TestS3Paths_AllThreePairs_CreateValidation(t *testing.T) {
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

func TestS3Paths_AllThreePairs_UpdateValidation(t *testing.T) {
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

func TestS3Paths_Update_PluginsPathVersionPairPersisted(t *testing.T) {
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

func TestS3Paths_Update_RequirementsPathVersionPairPersisted(t *testing.T) {
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

func TestS3Paths_Update_StartupScriptPathVersionPairPersisted(t *testing.T) {
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

func TestNetworkConfig_CreateWithSubnetsAndSecGroups(t *testing.T) {
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

// TestNetworkConfig_CreateWithoutNetworkConfigRejected verifies that omitting
// NetworkConfiguration on CreateEnvironment is rejected. AWS's
// CreateEnvironmentInput.NetworkConfiguration member is documented as required
// (docs.aws.amazon.com/mwaa/latest/API/API_CreateEnvironment.html) and
// aws-sdk-go-v2's generated client-side validator (validateOpCreateEnvironmentInput)
// refuses to even send a request that omits it.
func TestNetworkConfig_CreateWithoutNetworkConfigRejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = nil

	_, err := b.CreateEnvironment(context.Background(), "nc-nil-env", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NetworkConfiguration")
}

func TestNetworkConfig_UpdateValidNetworkConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-upd-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-upd-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.UpdateNetworkConfig{
			SecurityGroupIDs: []string{"sg-new1"},
		},
	})
	require.NoError(t, err)
}

// TestNetworkConfig_UpdateSecurityGroupsOnlyAccepted verifies that
// UpdateEnvironment accepts a NetworkConfiguration carrying only
// SecurityGroupIds. AWS's UpdateNetworkConfigurationInput wire shape has no
// SubnetIds member at all -- subnets are immutable after creation -- so a
// real client updating just the security groups must not be rejected.

func TestNetworkConfig_UpdateSecurityGroupsOnlyAccepted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-empty-sn", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-empty-sn") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-empty-sn", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.UpdateNetworkConfig{
			SecurityGroupIDs: []string{"sg-1"},
		},
	})
	require.NoError(t, err)
}

func TestNetworkConfig_UpdateEmptySecurityGroupsRejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "nc-empty-sg", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-empty-sg") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-empty-sg", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.UpdateNetworkConfig{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SecurityGroupIds")
}

// TestNetworkConfig_UpdatePersisted verifies that updating
// SecurityGroupIds via UpdateEnvironment leaves the original SubnetIds
// (set at creation) untouched, since AWS's update wire shape cannot carry
// subnets at all.

func TestNetworkConfig_UpdatePersisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-orig1", "subnet-orig2"},
		SecurityGroupIDs: []string{"sg-orig1"},
	}
	_, err := b.CreateEnvironment(context.Background(), "nc-persist-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "nc-persist-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "nc-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.UpdateNetworkConfig{
			SecurityGroupIDs: []string{"sg-x1", "sg-x2"},
		},
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "nc-persist-env")
	env, err := b.GetEnvironment(context.Background(), "nc-persist-env")
	require.NoError(t, err)
	require.NotNil(t, env.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-orig1", "subnet-orig2"}, env.NetworkConfiguration.SubnetIDs,
		"SubnetIds must survive an update since AWS cannot change them via UpdateEnvironment")
	assert.Equal(t, []string{"sg-x1", "sg-x2"}, env.NetworkConfiguration.SecurityGroupIDs)
}

func TestAirflowConfig_CreateWithOptions(t *testing.T) {
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

func TestAirflowConfig_UpdateReplaces_NotMerges(t *testing.T) {
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

func TestAirflowConfig_UpdateNilOptions_DoesNotClear(t *testing.T) {
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

func TestAirflowConfig_EmptyMapClears(t *testing.T) {
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

func TestAirflowConfig_CeleryExecutorOption(t *testing.T) {
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

func TestAirflowConfig_LocalExecutorOption(t *testing.T) {
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

func TestKmsKey_ValidationOnCreate(t *testing.T) {
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

func TestKmsKey_PersistedInGetEnvironment(t *testing.T) {
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

func TestEndpointManagement_ValidationAndPersistence(t *testing.T) {
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

func TestEndpointManagement_CustomerPersistedInGet(t *testing.T) {
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
