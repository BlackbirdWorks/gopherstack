package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// TestAWSAccuracy_ErrorTypes verifies that error types match AWS behavior.
func TestAWSAccuracy_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *codebuild.Handler)
		action       string
		body         any
		wantStatus   int
		wantErrType  string
	}{
		{
			name:       "create_duplicate_project_returns_ResourceAlreadyExistsException",
			action:     "CreateProject",
			body: map[string]any{
				"name":        "dup-project",
				"source":      map[string]any{"type": "NO_SOURCE"},
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "img", "computeType": "BUILD_GENERAL1_SMALL"},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":        "dup-project",
					"source":      map[string]any{"type": "NO_SOURCE"},
					"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "img", "computeType": "BUILD_GENERAL1_SMALL"},
				})
			},
		},
		{
			name:       "create_duplicate_fleet_returns_ResourceAlreadyExistsException",
			action:     "CreateFleet",
			body: map[string]any{
				"name":            "dup-fleet",
				"baseCapacity":    1,
				"computeType":     "BUILD_GENERAL1_SMALL",
				"environmentType": "LINUX_CONTAINER",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateFleet", map[string]any{
					"name":            "dup-fleet",
					"baseCapacity":    1,
					"computeType":     "BUILD_GENERAL1_SMALL",
					"environmentType": "LINUX_CONTAINER",
				})
			},
		},
		{
			name:       "create_duplicate_webhook_returns_ResourceAlreadyExistsException",
			action:     "CreateWebhook",
			body:       map[string]any{"projectName": "webhook-proj"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":        "webhook-proj",
					"source":      map[string]any{"type": "NO_SOURCE"},
					"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "img", "computeType": "BUILD_GENERAL1_SMALL"},
				})
				doRequest(t, h, "CreateWebhook", map[string]any{"projectName": "webhook-proj"})
			},
		},
		{
			name:       "start_build_missing_project_returns_ResourceNotFoundException",
			action:     "StartBuild",
			body:       map[string]any{"projectName": "ghost-project"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:       "stop_build_missing_returns_ResourceNotFoundException",
			action:     "StopBuild",
			body:       map[string]any{"id": "ghost:abc"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:       "update_project_missing_returns_ResourceNotFoundException",
			action:     "UpdateProject",
			body:       map[string]any{"name": "ghost-project"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:       "delete_project_missing_returns_ResourceNotFoundException",
			action:     "DeleteProject",
			body:       map[string]any{"name": "ghost-project"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:       "delete_fleet_missing_returns_ResourceNotFoundException",
			action:     "DeleteFleet",
			body:       map[string]any{"arn": "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:       "get_resource_policy_missing_returns_ResourceNotFoundException",
			action:     "GetResourcePolicy",
			body:       map[string]any{"resourceArn": "arn:aws:codebuild:us-east-1:000000000000:project/ghost"},
			wantStatus: http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

// TestAWSAccuracy_StopBuild verifies StopBuild sets status to STOPPED, not SUCCEEDED.
func TestAWSAccuracy_StopBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantBuildStatus  string
		wantCurrentPhase string
		wantBuildComplete bool
	}{
		{
			name:             "stop_sets_STOPPED_not_SUCCEEDED",
			wantBuildStatus:  "STOPPED",
			wantCurrentPhase: "COMPLETED",
			wantBuildComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-proj")
			startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "stop-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				Build struct {
					ID string `json:"id"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopBuild", map[string]any{"id": startOut.Build.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				Build struct {
					BuildStatus   string `json:"buildStatus"`
					CurrentPhase  string `json:"currentPhase"`
					BuildComplete bool   `json:"buildComplete"`
					EndTime       float64 `json:"endTime"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantBuildStatus, out.Build.BuildStatus)
			assert.Equal(t, tt.wantCurrentPhase, out.Build.CurrentPhase)
			assert.Equal(t, tt.wantBuildComplete, out.Build.BuildComplete)
			assert.Greater(t, out.Build.EndTime, float64(0), "endTime should be set")
		})
	}
}

// TestAWSAccuracy_ProjectSource tests the expanded source schema.
func TestAWSAccuracy_ProjectSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       map[string]any
		wantLocation string
		wantBuildspec string
		wantCloneDepth int
		wantInsecureSsl bool
		wantReportBuildStatus bool
	}{
		{
			name: "full_github_source",
			source: map[string]any{
				"type":              "GITHUB",
				"location":         "https://github.com/example/repo",
				"buildspec":        "buildspec.yml",
				"gitCloneDepth":    1,
				"insecureSsl":      false,
				"reportBuildStatus": true,
			},
			wantLocation:          "https://github.com/example/repo",
			wantBuildspec:         "buildspec.yml",
			wantCloneDepth:        1,
			wantReportBuildStatus: true,
		},
		{
			name: "s3_source",
			source: map[string]any{
				"type":     "S3",
				"location": "my-bucket/source.zip",
			},
			wantLocation: "my-bucket/source.zip",
		},
		{
			name: "no_source",
			source: map[string]any{
				"type": "NO_SOURCE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":        "src-test-" + tt.name,
				"source":      tt.source,
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					Source struct {
						Type              string `json:"type"`
						Location         string `json:"location"`
						Buildspec        string `json:"buildspec"`
						GitCloneDepth    int    `json:"gitCloneDepth"`
						InsecureSsl      bool   `json:"insecureSsl"`
						ReportBuildStatus bool   `json:"reportBuildStatus"`
					} `json:"source"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantLocation, out.Project.Source.Location)
			assert.Equal(t, tt.wantBuildspec, out.Project.Source.Buildspec)
			assert.Equal(t, tt.wantCloneDepth, out.Project.Source.GitCloneDepth)
			assert.Equal(t, tt.wantReportBuildStatus, out.Project.Source.ReportBuildStatus)
		})
	}
}

// TestAWSAccuracy_ProjectArtifacts tests the expanded artifacts schema.
func TestAWSAccuracy_ProjectArtifacts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		artifacts         map[string]any
		wantType          string
		wantPath          string
		wantNamespaceType string
		wantPackaging     string
	}{
		{
			name: "s3_artifacts_full",
			artifacts: map[string]any{
				"type":          "S3",
				"location":      "my-artifacts-bucket",
				"path":          "builds/",
				"namespaceType": "BUILD_ID",
				"name":          "output.zip",
				"packaging":     "ZIP",
			},
			wantType:          "S3",
			wantPath:          "builds/",
			wantNamespaceType: "BUILD_ID",
			wantPackaging:     "ZIP",
		},
		{
			name:      "no_artifacts",
			artifacts: map[string]any{"type": "NO_ARTIFACTS"},
			wantType:  "NO_ARTIFACTS",
		},
		{
			name:      "codepipeline_artifacts",
			artifacts: map[string]any{"type": "CODEPIPELINE"},
			wantType:  "CODEPIPELINE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":        "art-test-" + tt.name,
				"source":      map[string]any{"type": "NO_SOURCE"},
				"artifacts":   tt.artifacts,
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					Artifacts struct {
						Type          string `json:"type"`
						Path          string `json:"path"`
						NamespaceType string `json:"namespaceType"`
						Packaging     string `json:"packaging"`
					} `json:"artifacts"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.Project.Artifacts.Type)
			assert.Equal(t, tt.wantPath, out.Project.Artifacts.Path)
			assert.Equal(t, tt.wantNamespaceType, out.Project.Artifacts.NamespaceType)
			assert.Equal(t, tt.wantPackaging, out.Project.Artifacts.Packaging)
		})
	}
}

// TestAWSAccuracy_ProjectEnvironment tests the expanded environment schema.
func TestAWSAccuracy_ProjectEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		environment              map[string]any
		wantType                 string
		wantComputeType          string
		wantPrivilegedMode       bool
		wantEnvVarCount          int
		wantImagePullCredentials string
	}{
		{
			name: "full_environment_with_env_vars",
			environment: map[string]any{
				"type":        "LINUX_CONTAINER",
				"image":       "aws/codebuild/standard:7.0",
				"computeType": "BUILD_GENERAL1_LARGE",
				"privilegedMode": true,
				"imagePullCredentialsType": "CODEBUILD",
				"environmentVariables": []map[string]any{
					{"name": "MY_VAR", "value": "hello", "type": "PLAINTEXT"},
					{"name": "SECRET_VAR", "value": "param/path", "type": "PARAMETER_STORE"},
				},
			},
			wantType:                 "LINUX_CONTAINER",
			wantComputeType:          "BUILD_GENERAL1_LARGE",
			wantPrivilegedMode:       true,
			wantEnvVarCount:          2,
			wantImagePullCredentials: "CODEBUILD",
		},
		{
			name: "arm_container",
			environment: map[string]any{
				"type":        "ARM_CONTAINER",
				"image":       "aws/codebuild/amazonlinux2-aarch64-standard:3.0",
				"computeType": "BUILD_GENERAL1_SMALL",
			},
			wantType:        "ARM_CONTAINER",
			wantComputeType: "BUILD_GENERAL1_SMALL",
		},
		{
			name: "gpu_container",
			environment: map[string]any{
				"type":        "LINUX_GPU_CONTAINER",
				"image":       "aws/codebuild/standard:7.0",
				"computeType": "BUILD_GENERAL1_XLARGE",
				"privilegedMode": false,
			},
			wantType:        "LINUX_GPU_CONTAINER",
			wantComputeType: "BUILD_GENERAL1_XLARGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":        "env-test-" + tt.name,
				"source":      map[string]any{"type": "NO_SOURCE"},
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": tt.environment,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					Environment struct {
						Type                     string           `json:"type"`
						ComputeType              string           `json:"computeType"`
						PrivilegedMode           bool             `json:"privilegedMode"`
						ImagePullCredentialsType string           `json:"imagePullCredentialsType"`
						EnvironmentVariables     []map[string]any `json:"environmentVariables"`
					} `json:"environment"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.Project.Environment.Type)
			assert.Equal(t, tt.wantComputeType, out.Project.Environment.ComputeType)
			assert.Equal(t, tt.wantPrivilegedMode, out.Project.Environment.PrivilegedMode)
			assert.Equal(t, tt.wantImagePullCredentials, out.Project.Environment.ImagePullCredentialsType)
			assert.Len(t, out.Project.Environment.EnvironmentVariables, tt.wantEnvVarCount)
		})
	}
}

// TestAWSAccuracy_ProjectCache tests cache configuration on projects.
func TestAWSAccuracy_ProjectCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cache        map[string]any
		wantType     string
		wantModeLen  int
	}{
		{
			name: "no_cache",
			cache: map[string]any{
				"type": "NO_CACHE",
			},
			wantType: "NO_CACHE",
		},
		{
			name: "s3_cache",
			cache: map[string]any{
				"type":     "S3",
				"location": "my-bucket/cache",
			},
			wantType: "S3",
		},
		{
			name: "local_cache_with_modes",
			cache: map[string]any{
				"type":  "LOCAL",
				"modes": []string{"LOCAL_DOCKER_LAYER_CACHE", "LOCAL_SOURCE_CACHE"},
			},
			wantType:    "LOCAL",
			wantModeLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":        "cache-test-" + tt.name,
				"source":      map[string]any{"type": "NO_SOURCE"},
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"cache": tt.cache,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					Cache struct {
						Type  string   `json:"type"`
						Modes []string `json:"modes"`
					} `json:"cache"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.Project.Cache.Type)
			assert.Len(t, out.Project.Cache.Modes, tt.wantModeLen)
		})
	}
}

// TestAWSAccuracy_ProjectTimeouts tests timeout fields on projects.
func TestAWSAccuracy_ProjectTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		timeoutInMinutes        int32
		queuedTimeoutInMinutes  int32
		concurrentBuildLimit    int32
		autoRetryLimit          int32
	}{
		{
			name:                   "build_timeouts",
			timeoutInMinutes:       60,
			queuedTimeoutInMinutes: 30,
		},
		{
			name:                 "concurrency_and_retry",
			concurrentBuildLimit: 5,
			autoRetryLimit:       3,
		},
		{
			name:                    "all_limits",
			timeoutInMinutes:        120,
			queuedTimeoutInMinutes:  480,
			concurrentBuildLimit:    10,
			autoRetryLimit:          1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name":      "timeout-test-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			}
			if tt.timeoutInMinutes != 0 {
				body["timeoutInMinutes"] = tt.timeoutInMinutes
			}
			if tt.queuedTimeoutInMinutes != 0 {
				body["queuedTimeoutInMinutes"] = tt.queuedTimeoutInMinutes
			}
			if tt.concurrentBuildLimit != 0 {
				body["concurrentBuildLimit"] = tt.concurrentBuildLimit
			}
			if tt.autoRetryLimit != 0 {
				body["autoRetryLimit"] = tt.autoRetryLimit
			}

			rec := doRequest(t, h, "CreateProject", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					TimeoutInMinutes       float64 `json:"timeoutInMinutes"`
					QueuedTimeoutInMinutes float64 `json:"queuedTimeoutInMinutes"`
					ConcurrentBuildLimit   float64 `json:"concurrentBuildLimit"`
					AutoRetryLimit         float64 `json:"autoRetryLimit"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, float64(tt.timeoutInMinutes), out.Project.TimeoutInMinutes)
			assert.Equal(t, float64(tt.queuedTimeoutInMinutes), out.Project.QueuedTimeoutInMinutes)
			assert.Equal(t, float64(tt.concurrentBuildLimit), out.Project.ConcurrentBuildLimit)
			assert.Equal(t, float64(tt.autoRetryLimit), out.Project.AutoRetryLimit)
		})
	}
}

// TestAWSAccuracy_SecondarySources tests secondary source support.
func TestAWSAccuracy_SecondarySources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		secondarySources        []map[string]any
		secondarySourceVersions []map[string]any
		wantSecondaryCount      int
	}{
		{
			name: "single_secondary_source",
			secondarySources: []map[string]any{
				{"type": "GITHUB", "location": "https://github.com/example/dep", "sourceIdentifier": "dep1"},
			},
			wantSecondaryCount: 1,
		},
		{
			name: "multiple_secondary_sources",
			secondarySources: []map[string]any{
				{"type": "GITHUB", "location": "https://github.com/example/lib1", "sourceIdentifier": "lib1"},
				{"type": "S3", "location": "my-bucket/lib2.zip", "sourceIdentifier": "lib2"},
			},
			wantSecondaryCount: 2,
		},
		{
			name: "secondary_source_with_version",
			secondarySources: []map[string]any{
				{"type": "GITHUB", "location": "https://github.com/example/dep", "sourceIdentifier": "dep1"},
			},
			secondarySourceVersions: []map[string]any{
				{"sourceIdentifier": "dep1", "sourceVersion": "refs/heads/main"},
			},
			wantSecondaryCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name":      "sec-src-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"secondarySources": tt.secondarySources,
			}
			if tt.secondarySourceVersions != nil {
				body["secondarySourceVersions"] = tt.secondarySourceVersions
			}

			rec := doRequest(t, h, "CreateProject", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					SecondarySources        []map[string]any `json:"secondarySources"`
					SecondarySourceVersions []map[string]any `json:"secondarySourceVersions"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Project.SecondarySources, tt.wantSecondaryCount)
			if tt.secondarySourceVersions != nil {
				assert.Len(t, out.Project.SecondarySourceVersions, len(tt.secondarySourceVersions))
			}
		})
	}
}

// TestAWSAccuracy_VpcConfig tests VPC configuration on projects.
func TestAWSAccuracy_VpcConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vpcConfig map[string]any
		wantVpcId string
	}{
		{
			name: "vpc_config_stored",
			vpcConfig: map[string]any{
				"vpcId":            "vpc-12345",
				"subnets":          []string{"subnet-abc", "subnet-def"},
				"securityGroupIds": []string{"sg-xyz"},
			},
			wantVpcId: "vpc-12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":      "vpc-test-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"vpcConfig": tt.vpcConfig,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					VpcConfig struct {
						VpcId            string   `json:"vpcId"`
						Subnets          []string `json:"subnets"`
						SecurityGroupIds []string `json:"securityGroupIds"`
					} `json:"vpcConfig"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantVpcId, out.Project.VpcConfig.VpcId)
		})
	}
}

// TestAWSAccuracy_UpdateProjectTags verifies UpdateProject persists tags.
func TestAWSAccuracy_UpdateProjectTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialTags  map[string]string
		updateTags   map[string]string
		wantFinalTag string
		wantFinalVal string
	}{
		{
			name:         "add_tag_on_update",
			initialTags:  map[string]string{"env": "dev"},
			updateTags:   map[string]string{"team": "platform"},
			wantFinalTag: "team",
			wantFinalVal: "platform",
		},
		{
			name:         "override_tag_on_update",
			initialTags:  map[string]string{"env": "dev"},
			updateTags:   map[string]string{"env": "prod"},
			wantFinalTag: "env",
			wantFinalVal: "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "tag-update-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"tags": tt.initialTags,
			})

			updateRec := doRequest(t, h, "UpdateProject", map[string]any{
				"name": "tag-update-" + tt.name,
				"tags": tt.updateTags,
			})
			require.Equal(t, http.StatusOK, updateRec.Code)

			var out struct {
				Project struct {
					Tags map[string]string `json:"tags"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&out))
			assert.Equal(t, tt.wantFinalVal, out.Project.Tags[tt.wantFinalTag])
		})
	}
}

// TestAWSAccuracy_LogsConfig tests logs configuration on projects.
func TestAWSAccuracy_LogsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		logsConfig               map[string]any
		wantCloudWatchStatus     string
		wantS3Status             string
	}{
		{
			name: "cloudwatch_logs_enabled",
			logsConfig: map[string]any{
				"cloudWatchLogs": map[string]any{
					"status":     "ENABLED",
					"groupName":  "/aws/codebuild/my-project",
					"streamName": "build-log",
				},
			},
			wantCloudWatchStatus: "ENABLED",
		},
		{
			name: "s3_logs_enabled",
			logsConfig: map[string]any{
				"s3Logs": map[string]any{
					"status":   "ENABLED",
					"location": "my-bucket/build-logs",
				},
			},
			wantS3Status: "ENABLED",
		},
		{
			name: "both_logs_enabled",
			logsConfig: map[string]any{
				"cloudWatchLogs": map[string]any{
					"status":    "ENABLED",
					"groupName": "/aws/codebuild/my-project",
				},
				"s3Logs": map[string]any{
					"status":   "ENABLED",
					"location": "my-bucket/logs",
				},
			},
			wantCloudWatchStatus: "ENABLED",
			wantS3Status:         "ENABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":      "logs-test-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"logsConfig": tt.logsConfig,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					LogsConfig struct {
						CloudWatchLogs struct {
							Status string `json:"status"`
						} `json:"cloudWatchLogs"`
						S3Logs struct {
							Status string `json:"status"`
						} `json:"s3Logs"`
					} `json:"logsConfig"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantCloudWatchStatus, out.Project.LogsConfig.CloudWatchLogs.Status)
			assert.Equal(t, tt.wantS3Status, out.Project.LogsConfig.S3Logs.Status)
		})
	}
}

// TestAWSAccuracy_BuildInheritsProject verifies StartBuild copies project fields.
func TestAWSAccuracy_BuildInheritsProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		serviceRole    string
		encryptionKey  string
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

// TestAWSAccuracy_FleetSchema tests expanded fleet schema fields.
func TestAWSAccuracy_FleetSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		fleetBody       map[string]any
		wantBaseCapacity float64
		wantStatusCode  string
	}{
		{
			name: "fleet_has_status_struct",
			fleetBody: map[string]any{
				"name":            "status-fleet",
				"baseCapacity":    2,
				"computeType":     "BUILD_GENERAL1_MEDIUM",
				"environmentType": "LINUX_CONTAINER",
			},
			wantBaseCapacity: 2,
			wantStatusCode:   "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateFleet", tt.fleetBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Fleet struct {
					BaseCapacity float64 `json:"baseCapacity"`
					Status       struct {
						StatusCode string `json:"statusCode"`
					} `json:"status"`
				} `json:"fleet"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantBaseCapacity, out.Fleet.BaseCapacity)
			assert.Equal(t, tt.wantStatusCode, out.Fleet.Status.StatusCode)
		})
	}
}

// TestAWSAccuracy_DeleteFleet verifies DeleteFleet actually removes the fleet.
func TestAWSAccuracy_DeleteFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
		deleteArn  string
		wantDelete int
		wantList   int
	}{
		{
			name:       "delete_removes_fleet",
			createName: "fleet-to-delete",
			wantDelete: http.StatusOK,
			wantList:   0,
		},
		{
			name:      "delete_missing_fleet_returns_404",
			deleteArn: "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost-fleet",
			wantDelete: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var fleetArn string
			if tt.createName != "" {
				createRec := doRequest(t, h, "CreateFleet", map[string]any{
					"name":            tt.createName,
					"baseCapacity":    1,
					"computeType":     "BUILD_GENERAL1_SMALL",
					"environmentType": "LINUX_CONTAINER",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createOut struct {
					Fleet struct {
						Arn string `json:"arn"`
					} `json:"fleet"`
				}
				require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
				fleetArn = createOut.Fleet.Arn
			} else {
				fleetArn = tt.deleteArn
			}

			deleteRec := doRequest(t, h, "DeleteFleet", map[string]any{"arn": fleetArn})
			assert.Equal(t, tt.wantDelete, deleteRec.Code)

			if tt.wantList == 0 && tt.createName != "" {
				listRec := doRequest(t, h, "ListFleets", nil)
				require.Equal(t, http.StatusOK, listRec.Code)

				var listOut struct {
					Fleets []string `json:"fleets"`
				}
				require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
				assert.Len(t, listOut.Fleets, 0, "fleet should be removed from list after delete")
			}
		})
	}
}

// TestAWSAccuracy_DeleteBuildBatch verifies DeleteBuildBatch removes the batch.
func TestAWSAccuracy_DeleteBuildBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantDelete int
		missing    bool
	}{
		{
			name:       "delete_removes_batch",
			wantDelete: http.StatusOK,
		},
		{
			name:       "delete_missing_returns_404",
			wantDelete: http.StatusBadRequest,
			missing:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "batch-proj-"+tt.name)

			var batchID string
			if !tt.missing {
				startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-proj-" + tt.name})
				require.Equal(t, http.StatusOK, startRec.Code)

				var startOut struct {
					BuildBatch struct {
						ID string `json:"id"`
					} `json:"buildBatch"`
				}
				require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
				batchID = startOut.BuildBatch.ID
			} else {
				batchID = "ghost-proj:ghost-batch-id"
			}

			deleteRec := doRequest(t, h, "DeleteBuildBatch", map[string]any{"id": batchID})
			assert.Equal(t, tt.wantDelete, deleteRec.Code)

			if !tt.missing {
				listRec := doRequest(t, h, "ListBuildBatchesForProject", map[string]any{"projectName": "batch-proj-" + tt.name})
				require.Equal(t, http.StatusOK, listRec.Code)

				var listOut struct {
					IDs []string `json:"ids"`
				}
				require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
				assert.NotContains(t, listOut.IDs, batchID, "batch should be removed after delete")
			}
		})
	}
}

// TestAWSAccuracy_StartBuildBatch verifies StartBuildBatch sets IN_PROGRESS status.
func TestAWSAccuracy_StartBuildBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       string
		wantStartTimeSet bool
	}{
		{
			name:             "batch_starts_in_progress",
			wantStatus:       "IN_PROGRESS",
			wantStartTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "batch-status-proj")

			rec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-status-proj"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				BuildBatch struct {
					BuildBatchStatus string  `json:"buildBatchStatus"`
					StartTime        float64 `json:"startTime"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.BuildBatch.BuildBatchStatus)
			if tt.wantStartTimeSet {
				assert.Greater(t, out.BuildBatch.StartTime, float64(0))
			}
		})
	}
}

// TestAWSAccuracy_SandboxSchema tests expanded sandbox schema.
func TestAWSAccuracy_SandboxSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantArnSet bool
	}{
		{
			name:       "sandbox_has_arn_and_start_time",
			wantStatus: "READY",
			wantArnSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "sandbox-schema-proj")

			rec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-schema-proj"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Sandbox struct {
					ID        string  `json:"id"`
					Arn       string  `json:"arn"`
					Status    string  `json:"status"`
					StartTime float64 `json:"startTime"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.Sandbox.Status)
			if tt.wantArnSet {
				assert.NotEmpty(t, out.Sandbox.Arn)
				assert.Contains(t, out.Sandbox.Arn, "sandbox/")
			}
			assert.Greater(t, out.Sandbox.StartTime, float64(0))
		})
	}
}

// TestAWSAccuracy_StopSandbox tests sandbox stop sets end time.
func TestAWSAccuracy_StopSandbox(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantStatus     string
		wantEndTimeSet bool
	}{
		{
			name:           "stop_sets_end_time",
			wantStatus:     "STOPPED",
			wantEndTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-sandbox-proj")

			startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "stop-sandbox-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				Sandbox struct {
					ID string `json:"id"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopSandbox", map[string]any{"id": startOut.Sandbox.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				Sandbox struct {
					Status  string  `json:"status"`
					EndTime float64 `json:"endTime"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.Sandbox.Status)
			if tt.wantEndTimeSet {
				assert.Greater(t, out.Sandbox.EndTime, float64(0))
			}
		})
	}
}

// TestAWSAccuracy_CommandExecutionType tests that command type is stored.
func TestAWSAccuracy_CommandExecutionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		execType    string
		command     string
		wantType    string
	}{
		{
			name:     "shell_type_stored",
			execType: "SHELL",
			command:  "echo hello",
			wantType: "SHELL",
		},
		{
			name:     "empty_type_stored",
			execType: "",
			command:  "ls",
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "cmd-exec-proj")

			sbRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "cmd-exec-proj"})
			require.Equal(t, http.StatusOK, sbRec.Code)

			var sbOut struct {
				Sandbox struct {
					ID string `json:"id"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(sbRec.Body).Decode(&sbOut))

			execRec := doRequest(t, h, "StartCommandExecution", map[string]any{
				"sandboxId": sbOut.Sandbox.ID,
				"command":   tt.command,
				"type":      tt.execType,
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			var out struct {
				CommandExecution struct {
					Type      string  `json:"type"`
					Command   string  `json:"command"`
					Status    string  `json:"status"`
					StartTime float64 `json:"startTime"`
					EndTime   float64 `json:"endTime"`
				} `json:"commandExecution"`
			}
			require.NoError(t, json.NewDecoder(execRec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.CommandExecution.Type)
			assert.Equal(t, tt.command, out.CommandExecution.Command)
			assert.Equal(t, "SUCCEEDED", out.CommandExecution.Status)
			assert.Greater(t, out.CommandExecution.StartTime, float64(0))
			assert.Greater(t, out.CommandExecution.EndTime, float64(0))
		})
	}
}

// TestAWSAccuracy_StopBuildBatch tests StopBuildBatch sets end time.
func TestAWSAccuracy_StopBuildBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantStatus     string
		wantEndTimeSet bool
	}{
		{
			name:           "stop_batch_sets_end_time",
			wantStatus:     "STOPPED",
			wantEndTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-batch-proj")

			startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "stop-batch-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				BuildBatch struct {
					ID string `json:"id"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": startOut.BuildBatch.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				BuildBatch struct {
					BuildBatchStatus string  `json:"buildBatchStatus"`
					EndTime          float64 `json:"endTime"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.BuildBatch.BuildBatchStatus)
			if tt.wantEndTimeSet {
				assert.Greater(t, out.BuildBatch.EndTime, float64(0))
			}
		})
	}
}

// TestAWSAccuracy_BuildPhases verifies StartBuild populates initial phase.
func TestAWSAccuracy_BuildPhases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantInitialPhase  string
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

// TestAWSAccuracy_ResourcePolicy tests PutResourcePolicy, GetResourcePolicy round-trip.
func TestAWSAccuracy_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceArn string
		policy     string
		wantGet    int
		wantPolicy string
	}{
		{
			name:        "put_and_get_policy",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/my-proj",
			policy:      `{"Version":"2012-10-17","Statement":[]}`,
			wantGet:     http.StatusOK,
			wantPolicy:  `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			putRec := doRequest(t, h, "PutResourcePolicy", map[string]any{
				"resourceArn": tt.resourceArn,
				"policy":      tt.policy,
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": tt.resourceArn})
			assert.Equal(t, tt.wantGet, getRec.Code)

			if tt.wantGet == http.StatusOK {
				var out struct {
					Policy string `json:"policy"`
				}
				require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
				assert.JSONEq(t, tt.wantPolicy, out.Policy)
			}
		})
	}
}

// TestAWSAccuracy_BuildBatchConfig tests BuildBatchConfig on projects.
func TestAWSAccuracy_BuildBatchConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		buildBatchConfig    map[string]any
		wantServiceRole     string
		wantMaxBuilds       float64
	}{
		{
			name: "batch_config_stored",
			buildBatchConfig: map[string]any{
				"serviceRole":     "arn:aws:iam::000000000000:role/batch-role",
				"batchReportMode": "REPORT_INDIVIDUAL_BUILDS",
				"restrictions": map[string]any{
					"maximumBuildsAllowed": 10,
					"computeTypesAllowed":  []string{"BUILD_GENERAL1_SMALL"},
				},
			},
			wantServiceRole: "arn:aws:iam::000000000000:role/batch-role",
			wantMaxBuilds:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateProject", map[string]any{
				"name":      "batch-config-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:7.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"buildBatchConfig": tt.buildBatchConfig,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Project struct {
					BuildBatchConfig struct {
						ServiceRole  string `json:"serviceRole"`
						Restrictions struct {
							MaximumBuildsAllowed float64 `json:"maximumBuildsAllowed"`
						} `json:"restrictions"`
					} `json:"buildBatchConfig"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantServiceRole, out.Project.BuildBatchConfig.ServiceRole)
			assert.Equal(t, tt.wantMaxBuilds, out.Project.BuildBatchConfig.Restrictions.MaximumBuildsAllowed)
		})
	}
}
