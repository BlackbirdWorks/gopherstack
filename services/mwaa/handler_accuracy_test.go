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

// ─────────────────────────────────────────────────────────────
// Gap 1: Environment name validation
// ─────────────────────────────────────────────────────────────

func TestAccuracy_EnvironmentName_ValidNames(t *testing.T) {
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

func TestAccuracy_EnvironmentName_InvalidNames(t *testing.T) {
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

func TestAccuracy_EnvironmentName_HTTPValidation(t *testing.T) {
	t.Parallel()

	// HTTP tests only for names that form valid URL paths (no spaces or
	// URL-reserved chars that would panic httptest.NewRequest).
	tests := []struct {
		name       string
		envName    string
		wantStatus int
	}{
		{
			name:       "starts_with_digit_rejected",
			envName:    "1env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "contains_dot_rejected",
			envName:    "my.env",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "too_long_rejected",
			envName:    strings.Repeat("a", 81),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_name_accepted",
			envName:    "valid-env-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_name_with_underscore",
			envName:    "Valid_Env",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":        "dags/",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
				"SourceBucketArn":  "arn:aws:s3:::bucket",
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestAccuracy_EnvironmentName_SpaceRejectedByBackend(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "my env", newCreateReq())
	require.Error(t, err)
}

func TestAccuracy_EnvironmentName_ExactlyMaxLength(t *testing.T) {
	t.Parallel()

	envName := "A" + strings.Repeat("b", 79) // 80 chars, starts with letter
	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
}

func TestAccuracy_EnvironmentName_ExactlyMinLength(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "a", newCreateReq())
	require.NoError(t, err)
}

// ─────────────────────────────────────────────────────────────
// Gap 2: Airflow version validation
// ─────────────────────────────────────────────────────────────

func TestAccuracy_AirflowVersion_SupportedVersions(t *testing.T) {
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

func TestAccuracy_AirflowVersion_UnsupportedVersions(t *testing.T) {
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

func TestAccuracy_AirflowVersion_EmptyUsesDefault(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = ""
	env, err := b.CreateEnvironment(context.Background(), "env-default", req)
	require.NoError(t, err)
	assert.NotEmpty(t, env.AirflowVersion)
}

func TestAccuracy_AirflowVersion_HTTPCreate_InvalidVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-test", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "3.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestAccuracy_AirflowVersion_HTTPCreate_ValidVersion(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-test-ok", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"SourceBucketArn":  "arn:aws:s3:::bucket",
		"AirflowVersion":   "2.9.2",
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestAccuracy_AirflowVersion_Update_InvalidVersion(t *testing.T) {
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

func TestAccuracy_AirflowVersion_Update_ValidVersion(t *testing.T) {
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

func TestAccuracy_AirflowVersion_Update_EmptyVersionAllowed(t *testing.T) {
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

func TestAccuracy_AirflowVersion_Update_HTTP_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doMWAARequest(t, h, http.MethodPut, "/environments/ver-upd-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/ver-upd-env", map[string]any{
		"AirflowVersion": "0.0.1",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 3: MaxWorkers upper bound (25)
// ─────────────────────────────────────────────────────────────

func TestAccuracy_MaxWorkers_UpperBound_Create(t *testing.T) {
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

func TestAccuracy_MaxWorkers_UpperBound_Update(t *testing.T) {
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

func TestAccuracy_MaxWorkers_ZeroUnbounded(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.MaxWorkers = 0 // 0 means use default, no upper bound check
	_, err := b.CreateEnvironment(context.Background(), "workers-zero", req)
	require.NoError(t, err)
}

func TestAccuracy_MaxWorkers_HTTP_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"MaxWorkers": 50,
		"MinWorkers": 1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestAccuracy_MaxWorkers_HTTP_AtLimit(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-at-limit", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"MaxWorkers": 25,
		"MinWorkers": 1,
	})
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestAccuracy_MaxWorkers_HTTP_Update_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/workers-upd-http", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/workers-upd-http", map[string]any{
		"MaxWorkers": 30,
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 4: WorkerReplacementStrategy validation
// ─────────────────────────────────────────────────────────────

func TestAccuracy_WorkerReplacementStrategy_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy string
		name     string
	}{
		{name: "forced", strategy: "FORCED"},
		{name: "drain", strategy: "TERMINATION_WITH_DRAIN"},
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

func TestAccuracy_WorkerReplacementStrategy_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []string{
		"IMMEDIATE",
		"GRACEFUL",
		"forced",
		"drain",
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

func TestAccuracy_WorkerReplacementStrategy_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy   string
		name       string
		wantStatus int
	}{
		{name: "forced_valid", strategy: "FORCED", wantStatus: http.StatusOK},
		{name: "drain_valid", strategy: "TERMINATION_WITH_DRAIN", wantStatus: http.StatusOK},
		{name: "invalid", strategy: "IMMEDIATE", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/ws-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/ws-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/ws-"+tt.name, map[string]any{
				"WorkerReplacementStrategy": tt.strategy,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestAccuracy_WorkerReplacementStrategy_StoredInLastUpdate(t *testing.T) {
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

func TestAccuracy_TagLimit_CreateEnvironment_Exceeds(t *testing.T) {
	t.Parallel()

	tags := make(map[string]any, 51)
	for i := range 51 {
		tags[strings.Repeat("k", i+1)] = "v"
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()

	tagMap := make(map[string]string, 51)
	for k, v := range tags {
		tagMap[k] = v.(string)
	}

	req.Tags = tagMap

	_, err := b.CreateEnvironment(context.Background(), "tag-env", req)
	require.Error(t, err)
}

func TestAccuracy_TagLimit_CreateEnvironment_AtLimit(t *testing.T) {
	t.Parallel()

	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[strings.Repeat("k", i+1)] = "v"
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = tags

	_, err := b.CreateEnvironment(context.Background(), "tag-at-limit", req)
	require.NoError(t, err)
}

func TestAccuracy_TagLimit_TagResource_Exceeds(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 48 tags.
	initialTags := make(map[string]string, 48)
	for i := range 48 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-resource-env", req)
	require.NoError(t, err)

	// Adding 3 new tags should exceed the 50-tag limit.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"new1": "v", "new2": "v", "new3": "v"})
	require.Error(t, err)
}

func TestAccuracy_TagLimit_TagResource_UpdateExistingTagsOK(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 50 tags.
	initialTags := make(map[string]string, 50)
	for i := range 50 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-update-ok", req)
	require.NoError(t, err)

	// Updating an existing tag (same key) does not increase count — should succeed.
	firstKey := strings.Repeat("k", 1)
	err = b.TagResource(context.Background(), env.ARN, map[string]string{firstKey: "updated"})
	require.NoError(t, err)
}

func TestAccuracy_TagLimit_TagResource_AddToFull(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 50 tags.
	initialTags := make(map[string]string, 50)
	for i := range 50 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-full-env", req)
	require.NoError(t, err)

	// Adding even one genuinely new tag must fail.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"brand-new-key": "v"})
	require.Error(t, err)
}

func TestAccuracy_TagLimit_HTTP_TagResource_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-tag-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	// Build 51 tags.
	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[strings.Repeat("t", i+1)] = "val"
	}

	tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{"Tags": tags})
	assert.Equal(t, http.StatusBadRequest, tagRec.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 6: WebserverAccessMode validation in UpdateEnvironment
// ─────────────────────────────────────────────────────────────

func TestAccuracy_UpdateWebserverAccessMode_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode string
		name string
	}{
		{name: "public", mode: "PUBLIC_ONLY"},
		{name: "private", mode: "PRIVATE_ONLY"},
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

func TestAccuracy_UpdateWebserverAccessMode_InvalidValue(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "wam-inv-env", newCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment(context.Background(), "wam-inv-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WebserverAccessMode: "BOGUS_MODE",
	})
	require.Error(t, err)
}

func TestAccuracy_UpdateWebserverAccessMode_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode       string
		name       string
		wantStatus int
	}{
		{name: "public_valid", mode: "PUBLIC_ONLY", wantStatus: http.StatusOK},
		{name: "private_valid", mode: "PRIVATE_ONLY", wantStatus: http.StatusOK},
		{name: "invalid", mode: "UNKNOWN", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/wam-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/wam-http-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/wam-http-"+tt.name, map[string]any{
				"WebserverAccessMode": tt.mode,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestAccuracy_UpdateWebserverAccessMode_Persisted(t *testing.T) {
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

func TestAccuracy_UpdateEnvironmentClass_ValidClasses(t *testing.T) {
	t.Parallel()

	classes := []string{
		"mw1.small", "mw1.medium", "mw1.large", "mw1.xlarge", "mw1.2xlarge",
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

func TestAccuracy_UpdateEnvironmentClass_InvalidClass(t *testing.T) {
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

func TestAccuracy_UpdateEnvironmentClass_EmptyAllowed(t *testing.T) {
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

func TestAccuracy_UpdateEnvironmentClass_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		class      string
		name       string
		wantStatus int
	}{
		{name: "large_valid", class: "mw1.large", wantStatus: http.StatusOK},
		{name: "invalid_class", class: "mw1.huge", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/cls-http-"+tt.name, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			doMWAARequest(t, h, http.MethodGet, "/environments/cls-http-"+tt.name, nil) // promote CREATING → AVAILABLE

			rec2 := doMWAARequest(t, h, http.MethodPatch, "/environments/cls-http-"+tt.name, map[string]any{
				"EnvironmentClass": tt.class,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestAccuracy_UpdateEnvironmentClass_Persisted(t *testing.T) {
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

func TestAccuracy_CliToken_JWTShaped(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "jwt-cli-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "jwt-cli-env") // promote CREATING → AVAILABLE

	token, _, err := b.CreateCliToken(context.Background(), "jwt-cli-env")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "CLI token must be three dot-separated JWT segments")
	assert.NotEmpty(t, parts[0], "header segment must not be empty")
	assert.NotEmpty(t, parts[1], "payload segment must not be empty")
	assert.NotEmpty(t, parts[2], "signature segment must not be empty")
}

func TestAccuracy_WebLoginToken_JWTShaped(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "jwt-web-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "jwt-web-env") // promote CREATING → AVAILABLE

	token, _, err := b.CreateWebLoginToken(context.Background(), "jwt-web-env")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "web login token must be three dot-separated JWT segments")
	assert.NotEmpty(t, parts[0])
	assert.NotEmpty(t, parts[1])
	assert.NotEmpty(t, parts[2])
}

func TestAccuracy_CliToken_DifferentFromWebToken(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "token-diff-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "token-diff-env") // promote CREATING → AVAILABLE

	cli, _, err := b.CreateCliToken(context.Background(), "token-diff-env")
	require.NoError(t, err)

	web, _, err := b.CreateWebLoginToken(context.Background(), "token-diff-env")
	require.NoError(t, err)

	assert.NotEqual(t, cli, web, "CLI token and web login token must differ")
}

func TestAccuracy_Token_DifferentPerEnvironment(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "env-token-a", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "env-token-a") // promote CREATING → AVAILABLE
	_, err = b.CreateEnvironment(context.Background(), "env-token-b", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "env-token-b") // promote CREATING → AVAILABLE

	tokenA, _, err := b.CreateCliToken(context.Background(), "env-token-a")
	require.NoError(t, err)

	tokenB, _, err := b.CreateCliToken(context.Background(), "env-token-b")
	require.NoError(t, err)

	assert.NotEqual(t, tokenA, tokenB, "tokens for different environments must differ")
}

func TestAccuracy_CliToken_HTTP_ResponseStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/jwt-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/jwt-http-env", nil) // promote CREATING → AVAILABLE

	rec := doMWAARequest(t, h, http.MethodPost, "/clitoken/jwt-http-env", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["CliToken"])
	assert.NotEmpty(t, resp["WebServerHostname"])

	parts := strings.Split(resp["CliToken"], ".")
	assert.Len(t, parts, 3)
}

func TestAccuracy_WebLoginToken_HTTP_ResponseStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/jwt-web-http-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	doMWAARequest(t, h, http.MethodGet, "/environments/jwt-web-http-env", nil) // promote CREATING → AVAILABLE

	rec := doMWAARequest(t, h, http.MethodPost, "/webtoken/jwt-web-http-env", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["WebToken"])
	assert.NotEmpty(t, resp["WebServerHostname"])

	parts := strings.Split(resp["WebToken"], ".")
	assert.Len(t, parts, 3)
}

// ─────────────────────────────────────────────────────────────
// Combined / regression scenarios
// ─────────────────────────────────────────────────────────────

func TestAccuracy_FullLifecycle_AllValidations(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with specific supported version and environment class.
	req := newCreateReq()
	req.AirflowVersion = "2.8.1"
	req.EnvironmentClass = "mw1.medium"
	req.MaxWorkers = 20
	req.MinWorkers = 2

	env, err := b.CreateEnvironment(context.Background(), "full-lifecycle-env", req)
	require.NoError(t, err)
	assert.Equal(t, "2.8.1", env.AirflowVersion)
	assert.Equal(t, "mw1.medium", env.EnvironmentClass)
	_, _ = b.GetEnvironment(context.Background(), "full-lifecycle-env") // promote CREATING → AVAILABLE

	// Update with valid strategy and access mode.
	_, err = b.UpdateEnvironment(context.Background(), "full-lifecycle-env", &mwaa.ExportedUpdateEnvironmentRequest{
		WorkerReplacementStrategy: "TERMINATION_WITH_DRAIN",
		WebserverAccessMode:       "PRIVATE_ONLY",
		EnvironmentClass:          "mw1.large",
	})
	require.NoError(t, err)

	got, err := b.GetEnvironment(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE_ONLY", got.WebserverAccessMode)
	assert.Equal(t, "mw1.large", got.EnvironmentClass)

	// Tokens should be JWT-shaped.
	cli, _, err := b.CreateCliToken(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Len(t, strings.Split(cli, "."), 3)

	web, _, err := b.CreateWebLoginToken(context.Background(), "full-lifecycle-env")
	require.NoError(t, err)
	assert.Len(t, strings.Split(web, "."), 3)
}

func TestAccuracy_MultipleValidationErrors_FirstReturned(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "bogus"
	req.EnvironmentClass = "mw99.huge"
	req.MaxWorkers = 999

	_, err := b.CreateEnvironment(context.Background(), "multi-err-env", req)
	require.Error(t, err)
}

func TestAccuracy_CreateEnvironment_NameValidationBeforeBodyValidation(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Invalid name AND invalid body — name error should surface first.
	req := newCreateReq()
	req.DagS3Path = "" // required field

	_, err := b.CreateEnvironment(context.Background(), "1invalid-name", req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "environment name")
}

func TestAccuracy_TagLimit_Create_ExactlyAtLimit_ThenOneMore(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// 50 tags at create — OK.
	tags50 := make(map[string]string, 50)
	for i := range 50 {
		tags50[strings.Repeat("x", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = tags50
	env, err := b.CreateEnvironment(context.Background(), "tag-boundary-env", req)
	require.NoError(t, err)

	// One more new tag — must fail.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"brand-new": "v"})
	require.Error(t, err)
}

func TestAccuracy_AirflowVersion_V1_SchedulerConstraint(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	req.Schedulers = 2 // v1 only supports 1 scheduler

	_, err := b.CreateEnvironment(context.Background(), "v1-schedulers-env", req)
	require.Error(t, err)
}

func TestAccuracy_AirflowVersion_V1_SingleSchedulerOK(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.AirflowVersion = "1.10.12"
	req.Schedulers = 1

	_, err := b.CreateEnvironment(context.Background(), "v1-scheduler-ok", req)
	require.NoError(t, err)
}

func TestAccuracy_MaxWorkers_Update_ZeroNoCheck(t *testing.T) {
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

func TestAccuracy_EnvironmentName_AllSupportedSpecialChars(t *testing.T) {
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

func TestAccuracy_UpdateWorkerReplacementStrategy_Persisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "forced", strategy: "FORCED"},
		{name: "drain", strategy: "TERMINATION_WITH_DRAIN"},
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
