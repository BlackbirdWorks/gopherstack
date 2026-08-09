package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CreateProject_SourceSchema tests the expanded source schema.
func TestHandler_CreateProject_SourceSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		source                map[string]any
		wantLocation          string
		wantBuildspec         string
		wantCloneDepth        int
		wantInsecureSsl       bool
		wantReportBuildStatus bool
	}{
		{
			name: "full_github_source",
			source: map[string]any{
				"type":              "GITHUB",
				"location":          "https://github.com/example/repo",
				"buildspec":         "buildspec.yml",
				"gitCloneDepth":     1,
				"insecureSsl":       false,
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
				"name":      "src-test-" + tt.name,
				"source":    tt.source,
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
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
						Location          string `json:"location"`
						Buildspec         string `json:"buildspec"`
						GitCloneDepth     int    `json:"gitCloneDepth"`
						InsecureSsl       bool   `json:"insecureSsl"`
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

// TestHandler_CreateProject_ArtifactsSchema tests the expanded artifacts schema.
func TestHandler_CreateProject_ArtifactsSchema(t *testing.T) {
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
				"name":      "art-test-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": tt.artifacts,
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

// TestHandler_CreateProject_EnvironmentSchema tests the expanded environment schema.
func TestHandler_CreateProject_EnvironmentSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		environment              map[string]any
		name                     string
		wantType                 string
		wantComputeType          string
		wantImagePullCredentials string
		wantEnvVarCount          int
		wantPrivilegedMode       bool
	}{
		{
			name: "full_environment_with_env_vars",
			environment: map[string]any{
				"type":                     "LINUX_CONTAINER",
				"image":                    "aws/codebuild/standard:7.0",
				"computeType":              "BUILD_GENERAL1_LARGE",
				"privilegedMode":           true,
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
				"type":           "LINUX_GPU_CONTAINER",
				"image":          "aws/codebuild/standard:7.0",
				"computeType":    "BUILD_GENERAL1_XLARGE",
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
						ImagePullCredentialsType string           `json:"imagePullCredentialsType"`
						EnvironmentVariables     []map[string]any `json:"environmentVariables"`
						PrivilegedMode           bool             `json:"privilegedMode"`
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

// TestHandler_CreateProject_CacheSchema tests cache configuration on projects.
func TestHandler_CreateProject_CacheSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cache       map[string]any
		wantType    string
		wantModeLen int
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
				"name":      "cache-test-" + tt.name,
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
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

// TestHandler_CreateProject_Timeouts tests timeout fields on projects.
func TestHandler_CreateProject_Timeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		timeoutInMinutes       int32
		queuedTimeoutInMinutes int32
		concurrentBuildLimit   int32
		autoRetryLimit         int32
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
			name:                   "all_limits",
			timeoutInMinutes:       120,
			queuedTimeoutInMinutes: 480,
			concurrentBuildLimit:   10,
			autoRetryLimit:         1,
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
			assert.InDelta(t, float64(tt.timeoutInMinutes), out.Project.TimeoutInMinutes, 0)
			assert.InDelta(t, float64(tt.queuedTimeoutInMinutes), out.Project.QueuedTimeoutInMinutes, 0)
			assert.InDelta(t, float64(tt.concurrentBuildLimit), out.Project.ConcurrentBuildLimit, 0)
			assert.InDelta(t, float64(tt.autoRetryLimit), out.Project.AutoRetryLimit, 0)
		})
	}
}

// TestHandler_CreateProject_SecondarySources tests secondary source support.
func TestHandler_CreateProject_SecondarySources(t *testing.T) {
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

// TestHandler_CreateProject_VpcConfig tests VPC configuration on projects.
func TestHandler_CreateProject_VpcConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vpcConfig map[string]any
		wantVpcID string
	}{
		{
			name: "vpc_config_stored",
			vpcConfig: map[string]any{
				"vpcId":            "vpc-12345",
				"subnets":          []string{"subnet-abc", "subnet-def"},
				"securityGroupIds": []string{"sg-xyz"},
			},
			wantVpcID: "vpc-12345",
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
						VpcID            string   `json:"vpcId"`
						Subnets          []string `json:"subnets"`
						SecurityGroupIDs []string `json:"securityGroupIds"`
					} `json:"vpcConfig"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantVpcID, out.Project.VpcConfig.VpcID)
		})
	}
}

// TestHandler_UpdateProject_Tags verifies UpdateProject persists tags.
func TestHandler_UpdateProject_Tags(t *testing.T) {
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
				"tags": tagPairs(tt.initialTags),
			})

			updateRec := doRequest(t, h, "UpdateProject", map[string]any{
				"name": "tag-update-" + tt.name,
				"tags": tagPairs(tt.updateTags),
			})
			require.Equal(t, http.StatusOK, updateRec.Code)

			var out struct {
				Project struct {
					Tags []struct {
						Key   string `json:"key"`
						Value string `json:"value"`
					} `json:"tags"`
				} `json:"project"`
			}
			require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&out))

			var gotVal string
			for _, tag := range out.Project.Tags {
				if tag.Key == tt.wantFinalTag {
					gotVal = tag.Value
				}
			}
			assert.Equal(t, tt.wantFinalVal, gotVal)
		})
	}
}

// TestHandler_CreateProject_LogsConfig tests logs configuration on projects.
func TestHandler_CreateProject_LogsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		logsConfig           map[string]any
		wantCloudWatchStatus string
		wantS3Status         string
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

// TestHandler_CreateProject_BuildBatchConfig tests BuildBatchConfig on projects.
func TestHandler_CreateProject_BuildBatchConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		buildBatchConfig map[string]any
		wantServiceRole  string
		wantMaxBuilds    float64
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
			assert.InDelta(t, tt.wantMaxBuilds, out.Project.BuildBatchConfig.Restrictions.MaximumBuildsAllowed, 0)
		})
	}
}
