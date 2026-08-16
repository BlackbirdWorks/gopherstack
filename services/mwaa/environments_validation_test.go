package mwaa_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestCreateEnvironment_WebserverAccessMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accessMode string
		wantErr    bool
	}{
		{name: "public_only_valid", accessMode: "PUBLIC_ONLY", wantErr: false},
		{name: "private_only_valid", accessMode: "PRIVATE_ONLY", wantErr: false},
		{name: "public_and_private_valid", accessMode: "PUBLIC_AND_PRIVATE", wantErr: false},
		{name: "empty_uses_default", accessMode: "", wantErr: false},
		{name: "invalid_mode", accessMode: "INVALID_MODE", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:      "arn:aws:s3:::bucket",
				NetworkConfiguration: testNetworkConfig(),
				WebserverAccessMode:  tt.accessMode,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateEnvironment_EnvironmentClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		class   string
		wantErr bool
	}{
		{name: "mw1_micro_valid", class: "mw1.micro", wantErr: false},
		{name: "mw1_small_valid", class: "mw1.small", wantErr: false},
		{name: "mw1_medium_valid", class: "mw1.medium", wantErr: false},
		{name: "mw1_large_valid", class: "mw1.large", wantErr: false},
		{name: "mw1_xlarge_valid", class: "mw1.xlarge", wantErr: false},
		{name: "mw1_2xlarge_valid", class: "mw1.2xlarge", wantErr: false},
		{name: "empty_uses_default", class: "", wantErr: false},
		{name: "invalid_class", class: "mw1.huge", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "env", &mwaa.ExportedCreateEnvironmentRequest{
				DagS3Path:            "dags/",
				ExecutionRoleArn:     "arn:aws:iam::123456789012:role/role",
				SourceBucketArn:      "arn:aws:s3:::bucket",
				NetworkConfiguration: testNetworkConfig(),
				EnvironmentClass:     tt.class,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEnvironmentName_ValidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{name: "single_letter", envName: "a"},
		{name: "all_lowercase", envName: "myenv"},
		{name: "all_uppercase", envName: "MYENV"},
		{name: "mixed_case", envName: "MyEnv"},
		{name: "with_numbers", envName: "env123"},
		{name: "with_hyphen", envName: "my-env"},
		{name: "with_underscore", envName: "my_env"},
		{name: "starts_uppercase", envName: "Env"},
		{name: "max_length", envName: strings.Repeat("a", 80)},
		{name: "alphanumeric_hyphen_underscore", envName: "Env-1_abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
			require.NoError(t, err)
		})
	}
}

func TestEnvironmentName_InvalidNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{name: "empty_name", envName: ""},
		{name: "starts_with_digit", envName: "1env"},
		{name: "starts_with_hyphen", envName: "-env"},
		{name: "starts_with_underscore", envName: "_env"},
		{name: "contains_dot", envName: "my.env"},
		{name: "contains_space", envName: "my env"},
		{name: "contains_at", envName: "my@env"},
		{name: "too_long", envName: strings.Repeat("a", 81)},
		{name: "contains_slash", envName: "my/env"},
		{name: "contains_colon", envName: "my:env"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
			require.Error(t, err)
		})
	}
}

func TestEnvironmentName_SpaceRejectedByBackend(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "my env", newCreateReq())
	require.Error(t, err)
}

func TestEnvironmentName_ExactlyMaxLength(t *testing.T) {
	t.Parallel()

	envName := "A" + strings.Repeat("b", 79) // 80 chars, starts with letter
	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
}

func TestEnvironmentName_ExactlyMinLength(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "a", newCreateReq())
	require.NoError(t, err)
}

// ─────────────────────────────────────────────────────────────
// Gap 2: Airflow version validation
// ─────────────────────────────────────────────────────────────

func TestEnvironmentName_AllSupportedSpecialChars(t *testing.T) {
	t.Parallel()

	validNames := []string{
		"a-b", "a_b", "abc123", "Abc-Def_1", "Z",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), name, newCreateReq())
			require.NoError(t, err)
		})
	}
}

func TestAirflowVersion_SupportedVersions(t *testing.T) {
	t.Parallel()

	supported := []string{
		"2.10.3", "2.9.2", "2.8.1", "2.7.2",
		"2.6.3", "2.5.1", "2.4.3", "2.2.2", "1.10.12",
	}

	for _, v := range supported {
		t.Run("version_"+strings.ReplaceAll(v, ".", "_"), func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.AirflowVersion = v
			_, err := b.CreateEnvironment(context.Background(), "env-v", req)
			require.NoError(t, err)
		})
	}
}

func TestAirflowVersion_UnsupportedVersions(t *testing.T) {
	t.Parallel()

	unsupported := []string{
		"3.0.0",
		"2.3.0",
		"2.1.0",
		"1.9.0",
		"bogus",
		"latest",
	}

	for _, v := range unsupported {
		t.Run("version_"+strings.ReplaceAll(v, ".", "_"), func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.AirflowVersion = v
			_, err := b.CreateEnvironment(context.Background(), "env-inv", req)
			require.Error(t, err)
		})
	}
}

func TestAirflowVersion_EmptyUsesDefault(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = ""
	env, err := b.CreateEnvironment(context.Background(), "env-default", req)
	require.NoError(t, err)
	assert.NotEmpty(t, env.AirflowVersion)
}

func TestAirflowVersion_Update_InvalidVersion(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "update-ver-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "update-ver-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "update-ver-env", &mwaa.ExportedUpdateEnvironmentRequest{
		AirflowVersion: "99.0.0",
	})
	require.Error(t, err)
}

func TestAirflowVersion_Update_ValidVersion(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "update-ver-ok", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "update-ver-ok") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "update-ver-ok", &mwaa.ExportedUpdateEnvironmentRequest{
		AirflowVersion: "2.9.2",
	})
	require.NoError(t, err)
}

func TestAirflowVersion_Update_EmptyVersionAllowed(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "update-ver-empty", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "update-ver-empty") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "update-ver-empty", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new-dags/",
	})
	require.NoError(t, err)
}

func TestAirflowVersion_V1_SchedulerConstraint(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	req.Schedulers = 2 // v1 only supports 1 scheduler

	_, err := b.CreateEnvironment(context.Background(), "v1-schedulers-env", req)
	require.Error(t, err)
}

func TestAirflowVersion_V1_SingleSchedulerOK(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	req.Schedulers = 1

	_, err := b.CreateEnvironment(context.Background(), "v1-scheduler-ok", req)
	require.NoError(t, err)
}

func TestMaxWorkers_UpperBound_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxWorkers int32
		wantErr    bool
	}{
		{name: "at_limit", maxWorkers: 25, wantErr: false},
		{name: "below_limit", maxWorkers: 10, wantErr: false},
		{name: "one_over_limit", maxWorkers: 26, wantErr: true},
		{name: "way_over_limit", maxWorkers: 100, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.MaxWorkers = tt.maxWorkers
			req.MinWorkers = 1
			_, err := b.CreateEnvironment(context.Background(), "workers-env", req)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMaxWorkers_UpperBound_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxWorkers int32
		wantErr    bool
	}{
		{name: "at_limit", maxWorkers: 25, wantErr: false},
		{name: "exceeds_limit", maxWorkers: 26, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "workers-upd-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "workers-upd-env") // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(
				context.Background(),
				"workers-upd-env",
				&mwaa.ExportedUpdateEnvironmentRequest{
					MaxWorkers: tt.maxWorkers,
				},
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMaxWorkers_ZeroUnbounded(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 0 // 0 means use default, no upper bound check
	_, err := b.CreateEnvironment(context.Background(), "workers-zero", req)
	require.NoError(t, err)
}

func TestMaxWorkers_Update_ZeroNoCheck(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "workers-zero-upd", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "workers-zero-upd") // promote CREATING → AVAILABLE

	// MaxWorkers=0 in update means "don't change" — no validation should fire.
	_, err = b.UpdateEnvironment(context.Background(), "workers-zero-upd", &mwaa.ExportedUpdateEnvironmentRequest{
		MaxWorkers: 0,
	})
	require.NoError(t, err)
}

// TestWorkerReplacementStrategy_AbsentFromCreate verifies WorkerReplacementStrategy
// is not part of the CreateEnvironment request or response shape at all: AWS's
// CreateEnvironmentInput has no such member (it only exists on
// UpdateEnvironmentInput -- see models.go's createEnvironmentRequest doc
// comment), so a value supplied on Create must be silently ignored rather than
// validated or persisted anywhere on the resulting Environment.
func TestWorkerReplacementStrategy_AbsentFromCreate(t *testing.T) {
	t.Parallel()

	h := mwaa.NewHandler(mwaa.NewInMemoryBackend(testRegion, testAccountID))
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/wrs-create-ignored", map[string]any{
		"DagS3Path":                 "dags/",
		"ExecutionRoleArn":          "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":           "arn:aws:s3:::bucket",
		"NetworkConfiguration":      networkConfigBody(),
		"WorkerReplacementStrategy": "BOGUS_VALUE_A_REAL_CLIENT_COULD_NEVER_SEND",
	})
	require.Equal(t, http.StatusOK, rec.Code, "an unknown/invented field on Create must not fail validation")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasField := resp["WorkerReplacementStrategy"]
	assert.False(t, hasField, "CreateEnvironment response must never echo WorkerReplacementStrategy")
}

func TestWorkerReplacementStrategy_UpdateInvalid(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "wrs-upd-invalid"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), envName) // promote to AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), envName, &mwaa.ExportedUpdateEnvironmentRequest{
		WorkerReplacementStrategy: "PREFERRED",
	})
	require.Error(t, err)
}

// ─────────────────────────────────────────────────────────────
// Token hostname accuracy — CreateCliToken / CreateWebLoginToken
// ─────────────────────────────────────────────────────────────

func TestWorkerReplacementStrategy_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy string
		name     string
	}{
		{name: "forced", strategy: "FORCED"},
		{name: "graceful", strategy: "GRACEFUL"},
		{name: "empty", strategy: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "strategy-env-"+tt.name, newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "strategy-env-"+tt.name) // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(
				context.Background(),
				"strategy-env-"+tt.name,
				&mwaa.ExportedUpdateEnvironmentRequest{
					WorkerReplacementStrategy: tt.strategy,
				},
			)
			require.NoError(t, err)
		})
	}
}

func TestWorkerReplacementStrategy_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []string{
		"IMMEDIATE",
		"TERMINATION_WITH_DRAIN", // not a real WorkerReplacementStrategy value
		"forced",
		"graceful",
		"TERMINATE",
		"REPLACE",
	}

	for _, strategy := range tests {
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "strategy-inv-env", newCreateReq())
			require.NoError(t, err)

			_, err = b.UpdateEnvironment(
				context.Background(),
				"strategy-inv-env",
				&mwaa.ExportedUpdateEnvironmentRequest{
					WorkerReplacementStrategy: strategy,
				},
			)
			require.Error(t, err)
		})
	}
}

func TestWorkerReplacementStrategy_StoredInLastUpdate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "lu-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "lu-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "lu-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WorkerReplacementStrategy: "FORCED",
	})
	require.NoError(t, err)

	// Fetch and verify LastUpdate contains the strategy.
	env, err := b.GetEnvironment(context.Background(), "lu-env")
	require.NoError(t, err)
	require.NotNil(t, env.LastUpdate)
	assert.Equal(t, "FORCED", env.LastUpdate.WorkerReplacementStrategy)
}

// ─────────────────────────────────────────────────────────────
// Gap 5: Tag limit (50 per resource)
// ─────────────────────────────────────────────────────────────

func TestUpdateWorkerReplacementStrategy_Persisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "forced", strategy: "FORCED"},
		{name: "graceful", strategy: "GRACEFUL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "persist-strat-"+tt.name, newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "persist-strat-"+tt.name) // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(
				context.Background(),
				"persist-strat-"+tt.name,
				&mwaa.ExportedUpdateEnvironmentRequest{
					WorkerReplacementStrategy: tt.strategy,
				},
			)
			require.NoError(t, err)

			// Fetch and check LastUpdate carries the strategy.
			env, err := b.GetEnvironment(context.Background(), "persist-strat-"+tt.name)
			require.NoError(t, err)
			require.NotNil(t, env.LastUpdate)
			assert.Equal(t, tt.strategy, env.LastUpdate.WorkerReplacementStrategy)
		})
	}
}

func TestUpdateWebserverAccessMode_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode string
		name string
	}{
		{name: "public", mode: "PUBLIC_ONLY"},
		{name: "private", mode: "PRIVATE_ONLY"},
		{name: "public_and_private", mode: "PUBLIC_AND_PRIVATE"},
		{name: "empty", mode: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "wam-env-"+tt.name, newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "wam-env-"+tt.name) // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(
				context.Background(),
				"wam-env-"+tt.name,
				&mwaa.ExportedUpdateEnvironmentRequest{
					WebserverAccessMode: tt.mode,
				},
			)
			require.NoError(t, err)
		})
	}
}

func TestUpdateWebserverAccessMode_InvalidValue(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "wam-inv-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment(context.Background(), "wam-inv-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WebserverAccessMode: "BOGUS_MODE",
	})
	require.Error(t, err)
}

func TestUpdateWebserverAccessMode_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "wam-persist", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wam-persist") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "wam-persist", &mwaa.ExportedUpdateEnvironmentRequest{
		WebserverAccessMode: "PRIVATE_ONLY",
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "wam-persist")
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE_ONLY", env.WebserverAccessMode)
}

// ─────────────────────────────────────────────────────────────
// Gap 7: EnvironmentClass validation in UpdateEnvironment
// ─────────────────────────────────────────────────────────────

func TestUpdateEnvironmentClass_ValidClasses(t *testing.T) {
	t.Parallel()

	classes := []string{
		"mw1.micro", "mw1.small", "mw1.medium", "mw1.large", "mw1.xlarge", "mw1.2xlarge",
	}

	for _, cls := range classes {
		t.Run(cls, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "class-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "class-env") // promote CREATING → AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), "class-env", &mwaa.ExportedUpdateEnvironmentRequest{
				EnvironmentClass: cls,
			})
			require.NoError(t, err)
		})
	}
}

func TestUpdateEnvironmentClass_InvalidClass(t *testing.T) {
	t.Parallel()

	tests := []string{
		"mw1.huge", "mw2.small", "t3.large", "bogus", "LARGE",
	}

	for _, cls := range tests {
		t.Run(cls, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "class-inv-env", newCreateReq())
			require.NoError(t, err)

			_, err = b.UpdateEnvironment(context.Background(), "class-inv-env", &mwaa.ExportedUpdateEnvironmentRequest{
				EnvironmentClass: cls,
			})
			require.Error(t, err)
		})
	}
}

func TestUpdateEnvironmentClass_EmptyAllowed(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "class-empty-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "class-empty-env") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "class-empty-env", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "",
	})
	require.NoError(t, err)
}

func TestUpdateEnvironmentClass_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "class-persist", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "class-persist") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "class-persist", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "mw1.large",
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "class-persist")
	require.NoError(t, err)
	assert.Equal(t, "mw1.large", env.EnvironmentClass)
}

// ─────────────────────────────────────────────────────────────
// Gap 8: JWT-shaped CLI and web login tokens
// ─────────────────────────────────────────────────────────────

func TestWeeklyMaint_Create_ValidValues(t *testing.T) {
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
			_, err := b.CreateEnvironment(context.Background(), "wmw-ok-env", req)
			require.NoError(t, err)
		})
	}
}

func TestWeeklyMaint_Create_InvalidValues(t *testing.T) {
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
			_, err := b.CreateEnvironment(context.Background(), "wmw-inv-env", req)

			if tt.value == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestWeeklyMaint_Create_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.WeeklyMaintenanceWindowStart = "FRI:03:30"
	_, err := b.CreateEnvironment(context.Background(), "wmw-persist-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "wmw-persist-env")
	require.NoError(t, err)
	assert.Equal(t, "FRI:03:30", env.WeeklyMaintenanceWindowStart)
}

func TestWeeklyMaintenance_UpdateValidation(t *testing.T) {
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

func TestWeeklyMaintenance_UpdatePersisted(t *testing.T) {
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

func TestWeeklyMaintenance_AllDays_Valid(t *testing.T) {
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

func TestCreate_MinWorkersExceedsMax(t *testing.T) {
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
			_, err := b.CreateEnvironment(context.Background(), "worker-range-env", req)
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

func TestDefaults_WorkersStoredOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "defaults-env", newCreateReq())
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(10), env.MaxWorkers, "default MaxWorkers should be 10")
	assert.Equal(t, int32(1), env.MinWorkers, "default MinWorkers should be 1")
}

func TestDefaults_WebserversStoredOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-defaults-env", newCreateReq())
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "ws-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(2), env.MaxWebservers, "default MaxWebservers should be 2")
	assert.Equal(t, int32(2), env.MinWebservers, "default MinWebservers should be 2")
}

func TestDefaults_SchedulersV2OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "2.9.2"
	_, err := b.CreateEnvironment(context.Background(), "sched-v2-defaults-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "sched-v2-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(2), env.Schedulers, "default Schedulers for v2 should be 2")
}

func TestDefaults_SchedulersV1OnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	_, err := b.CreateEnvironment(context.Background(), "sched-v1-defaults-env", req)
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "sched-v1-defaults-env")
	require.NoError(t, err)

	assert.Equal(t, int32(1), env.Schedulers, "default Schedulers for v1 should be 1")
}

// ─────────────────────────────────────────────────────────────
// 4. Schedulers on UPDATE – boundary values
// ─────────────────────────────────────────────────────────────

func TestSchedulers_Update_V2Boundaries(t *testing.T) {
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
			_, err := b.CreateEnvironment(context.Background(), "sched-upd-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "sched-upd-env")

			_, err = b.UpdateEnvironment(context.Background(), "sched-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
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

func TestSchedulers_Update_Persisted(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "sched-persist-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "sched-persist-env")

	_, err = b.UpdateEnvironment(context.Background(), "sched-persist-env", &mwaa.ExportedUpdateEnvironmentRequest{
		Schedulers:     4,
		AirflowVersion: "2.10.3",
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "sched-persist-env")
	require.NoError(t, err)
	assert.Equal(t, int32(4), env.Schedulers)
}

// ─────────────────────────────────────────────────────────────
// 5. Webserver bounds on UPDATE
// ─────────────────────────────────────────────────────────────

func TestSchedulers_Create_V2_BoundaryValues(t *testing.T) {
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

func TestWebservers_Update_MinExceedsMax(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-upd-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment(context.Background(), "ws-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 4,
		MaxWebservers: 2,
	})
	require.Error(t, err)
}

func TestWebservers_Update_ValidRange(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-upd-ok-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "ws-upd-ok-env")

	_, err = b.UpdateEnvironment(context.Background(), "ws-upd-ok-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 1,
		MaxWebservers: 5,
	})
	require.NoError(t, err)

	env, err := b.GetEnvironment(context.Background(), "ws-upd-ok-env")
	require.NoError(t, err)
	assert.Equal(t, int32(1), env.MinWebservers)
	assert.Equal(t, int32(5), env.MaxWebservers)
}

func TestWebservers_Update_MaxExceeds5_Rejected(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "ws-upd-over-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment(context.Background(), "ws-upd-over-env", &mwaa.ExportedUpdateEnvironmentRequest{
		MinWebservers: 1,
		MaxWebservers: 6,
	})
	require.Error(t, err)
}

// ─────────────────────────────────────────────────────────────
// 6. Transient status promotion on GetEnvironment
// ─────────────────────────────────────────────────────────────

func TestWebservers_Create_BoundaryValues(t *testing.T) {
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

func TestWorkers_Update_OnlyMinSet_KeepsExistingMax(t *testing.T) {
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

func TestWorkers_Update_OnlyMaxSet_KeepsExistingMin(t *testing.T) {
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

func TestWorkers_Update_NewMinExceedsExistingMax(t *testing.T) {
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

func TestWorkers_Update_NewMaxBelowExistingMin(t *testing.T) {
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

// TestWorkers_Update_RejectedRequestDoesNotMutate verifies that when the
// worker-bounds check rejects an UpdateEnvironment call, none of the other
// fields in the same request are applied. env is the live stored pointer,
// not a copy, so validating before mutating (rather than after) is required
// to avoid silently persisting a rejected request's other fields.
func TestWorkers_Update_RejectedRequestDoesNotMutate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 5
	req.MinWorkers = 1
	_, err := b.CreateEnvironment(context.Background(), "wk-reject-no-mutate", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "wk-reject-no-mutate") // promote CREATING → AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), "wk-reject-no-mutate", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path:  "dags/should-not-apply/",
		MinWorkers: 10, // > existing MaxWorkers=5: rejected
	})
	require.Error(t, err)

	env, err := b.GetEnvironment(context.Background(), "wk-reject-no-mutate")
	require.NoError(t, err)
	assert.NotEqual(t, "dags/should-not-apply/", env.DagS3Path)
	assert.Equal(t, int32(5), env.MaxWorkers)
	assert.Equal(t, int32(1), env.MinWorkers)
}

func TestWorkers_Update_BothSetValidRange(t *testing.T) {
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
