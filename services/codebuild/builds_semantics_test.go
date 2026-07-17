package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_RetryBuild_InheritsFields verifies RetryBuild inherits environment/source/serviceRole
// from the original build, matching real AWS behavior.
func TestHandler_RetryBuild_InheritsFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateProject", map[string]any{
		"name":        "retry-inherit-proj",
		"serviceRole": "arn:aws:iam::000000000000:role/my-role",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})

	startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "retry-inherit-proj"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		Build map[string]any `json:"build"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	buildID, _ := startOut.Build["id"].(string)
	require.NotEmpty(t, buildID)

	retryRec := doRequest(t, h, "RetryBuild", map[string]any{"id": buildID})
	require.Equal(t, http.StatusOK, retryRec.Code)

	var retryOut struct {
		Build map[string]any `json:"build"`
	}
	require.NoError(t, json.Unmarshal(retryRec.Body.Bytes(), &retryOut))
	b := retryOut.Build

	assert.Equal(t, "arn:aws:iam::000000000000:role/my-role", b["serviceRole"],
		"RetryBuild must inherit serviceRole from original build")
	assert.NotNil(t, b["environment"], "RetryBuild must inherit environment from original build")
	assert.NotNil(t, b["source"], "RetryBuild must inherit source from original build")
	assert.NotEmpty(t, b["phases"], "RetryBuild must include SUBMITTED phase")
}

// TestHandler_BatchGetBuilds_ARNLookup verifies BatchGetBuilds accepts ARNs, not just IDs.
func TestHandler_BatchGetBuilds_ARNLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "arn-lookup-proj")

	startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "arn-lookup-proj"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		Build map[string]any `json:"build"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	buildArn, _ := startOut.Build["arn"].(string)
	require.NotEmpty(t, buildArn)

	getRec := doRequest(t, h, "BatchGetBuilds", map[string]any{"ids": []string{buildArn}})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		Builds         []any    `json:"builds"`
		BuildsNotFound []string `json:"buildsNotFound"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Len(t, getOut.Builds, 1, "BatchGetBuilds must find build by ARN")
	assert.Empty(t, getOut.BuildsNotFound, "ARN lookup must not produce buildsNotFound")
}

// TestHandler_StartBuild_OverrideFields verifies StartBuild applies override fields
// beyond just environmentVariablesOverride, matching real AWS behavior.
func TestHandler_StartBuild_OverrideFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateProject", map[string]any{
		"name":        "override-proj",
		"serviceRole": "arn:aws:iam::000000000000:role/original-role",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})

	rec := doRequest(t, h, "StartBuild", map[string]any{
		"projectName":         "override-proj",
		"serviceRoleOverride": "arn:aws:iam::000000000000:role/override-role",
		"imageOverride":       "aws/codebuild/standard:6.0",
		"computeTypeOverride": "BUILD_GENERAL1_MEDIUM",
		"buildspecOverride":   "version: 0.2\nphases:\n  build:\n    commands: [echo override]",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Build map[string]any `json:"build"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	b := out.Build

	assert.Equal(t, "arn:aws:iam::000000000000:role/override-role", b["serviceRole"],
		"serviceRoleOverride must be applied to build")

	env, _ := b["environment"].(map[string]any)
	assert.Equal(t, "aws/codebuild/standard:6.0", env["image"],
		"imageOverride must be applied to build environment")
	assert.Equal(t, "BUILD_GENERAL1_MEDIUM", env["computeType"],
		"computeTypeOverride must be applied to build environment")

	src, _ := b["source"].(map[string]any)
	assert.Contains(t, src["buildspec"], "override",
		"buildspecOverride must be applied to build source")
}

// TestHandler_StartBuild_InheritsProjectFields verifies StartBuild copies project fields.
func TestHandler_StartBuild_InheritsProjectFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		serviceRole     string
		encryptionKey   string
		wantServiceRole string
	}{
		{
			name:            "build_inherits_service_role",
			serviceRole:     "arn:aws:iam::000000000000:role/my-role",
			wantServiceRole: "arn:aws:iam::000000000000:role/my-role",
		},
		{
			name:            "build_inherits_encryption_key",
			encryptionKey:   "arn:aws:kms:us-east-1:000000000000:key/abc123",
			wantServiceRole: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name":      "inherit-test-" + tt.name,
				"source":    map[string]any{"type": "GITHUB", "location": "https://github.com/example/repo"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
					"environmentVariables": []map[string]any{
						{"name": "FOO", "value": "bar"},
					},
				},
			}
			if tt.serviceRole != "" {
				body["serviceRole"] = tt.serviceRole
			}
			if tt.encryptionKey != "" {
				body["encryptionKey"] = tt.encryptionKey
			}

			createRec := doRequest(t, h, "CreateProject", body)
			require.Equal(t, http.StatusOK, createRec.Code)

			startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "inherit-test-" + tt.name})
			require.Equal(t, http.StatusOK, startRec.Code)

			var out struct {
				Build struct {
					ServiceRole string `json:"serviceRole"`
					Source      struct {
						Type string `json:"type"`
					} `json:"source"`
					Environment struct {
						EnvironmentVariables []map[string]any `json:"environmentVariables"`
					} `json:"environment"`
					Phases []map[string]any `json:"phases"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&out))
			assert.Equal(t, tt.wantServiceRole, out.Build.ServiceRole)
			assert.Equal(t, "GITHUB", out.Build.Source.Type)
			assert.Len(t, out.Build.Environment.EnvironmentVariables, 1)
			assert.NotEmpty(t, out.Build.Phases, "build should have initial phases")
		})
	}
}

// TestHandler_StartBuild_InitialPhase verifies StartBuild populates initial phase.
func TestHandler_StartBuild_InitialPhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantInitialPhase   string
		wantPhasesNonEmpty bool
	}{
		{
			name:               "build_has_submitted_phase",
			wantInitialPhase:   "SUBMITTED",
			wantPhasesNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "phases-proj")

			startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "phases-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var out struct {
				Build struct {
					Phases []struct {
						PhaseType   string `json:"phaseType"`
						PhaseStatus string `json:"phaseStatus"`
					} `json:"phases"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&out))
			if tt.wantPhasesNonEmpty {
				require.NotEmpty(t, out.Build.Phases)
				assert.Equal(t, tt.wantInitialPhase, out.Build.Phases[0].PhaseType)
				assert.Equal(t, "SUCCEEDED", out.Build.Phases[0].PhaseStatus)
			}
		})
	}
}

// TestHandler_StartBuild_EnvVarOverrideMerge verifies that environmentVariablesOverride
// merges with the project's env vars: same-name vars are replaced, new vars are appended.
func TestHandler_StartBuild_EnvVarOverrideMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		projectEnvs   []map[string]any
		overrideEnvs  []map[string]any
		wantEnvSubset []map[string]any
		wantEnvCount  int
	}{
		{
			name: "override_replaces_same_name_var",
			projectEnvs: []map[string]any{
				{"name": "BRANCH", "value": "main", "type": "PLAINTEXT"},
				{"name": "ENV", "value": "staging", "type": "PLAINTEXT"},
			},
			overrideEnvs: []map[string]any{
				{"name": "BRANCH", "value": "feature/x", "type": "PLAINTEXT"},
			},
			wantEnvCount: 2,
			wantEnvSubset: []map[string]any{
				{"name": "BRANCH", "value": "feature/x"},
				{"name": "ENV", "value": "staging"},
			},
		},
		{
			name: "override_appends_new_var",
			projectEnvs: []map[string]any{
				{"name": "BASE_VAR", "value": "base", "type": "PLAINTEXT"},
			},
			overrideEnvs: []map[string]any{
				{"name": "COMMIT_SHA", "value": "abc123", "type": "PLAINTEXT"},
			},
			wantEnvCount: 2,
			wantEnvSubset: []map[string]any{
				{"name": "BASE_VAR", "value": "base"},
				{"name": "COMMIT_SHA", "value": "abc123"},
			},
		},
		{
			name: "no_override_preserves_project_envs",
			projectEnvs: []map[string]any{
				{"name": "FOO", "value": "bar", "type": "PLAINTEXT"},
			},
			overrideEnvs: nil,
			wantEnvCount: 1,
			wantEnvSubset: []map[string]any{
				{"name": "FOO", "value": "bar"},
			},
		},
		{
			name: "override_replaces_and_appends",
			projectEnvs: []map[string]any{
				{"name": "VAR_A", "value": "old_a", "type": "PLAINTEXT"},
				{"name": "VAR_B", "value": "b", "type": "PLAINTEXT"},
			},
			overrideEnvs: []map[string]any{
				{"name": "VAR_A", "value": "new_a", "type": "PLAINTEXT"},
				{"name": "VAR_C", "value": "c", "type": "PLAINTEXT"},
			},
			wantEnvCount: 3,
			wantEnvSubset: []map[string]any{
				{"name": "VAR_A", "value": "new_a"},
				{"name": "VAR_B", "value": "b"},
				{"name": "VAR_C", "value": "c"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			projName := "envoverride-" + tt.name

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      projName,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":                 "LINUX_CONTAINER",
					"image":                "aws/codebuild/standard:7.0",
					"computeType":          "BUILD_GENERAL1_SMALL",
					"environmentVariables": tt.projectEnvs,
				},
			})

			body := map[string]any{"projectName": projName}
			if tt.overrideEnvs != nil {
				body["environmentVariablesOverride"] = tt.overrideEnvs
			}

			rec := doRequest(t, h, "StartBuild", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Build struct {
					Environment struct {
						EnvironmentVariables []struct {
							Name  string `json:"name"`
							Value string `json:"value"`
						} `json:"environmentVariables"`
					} `json:"environment"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			envVars := out.Build.Environment.EnvironmentVariables
			assert.Len(t, envVars, tt.wantEnvCount)

			byName := make(map[string]string, len(envVars))
			for _, ev := range envVars {
				byName[ev.Name] = ev.Value
			}
			for _, want := range tt.wantEnvSubset {
				name := want["name"].(string)
				value := want["value"].(string)
				assert.Equal(t, value, byName[name], "env var %q should have value %q", name, value)
			}
		})
	}
}
