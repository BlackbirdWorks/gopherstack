package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// parityDeployIDRe matches the AWS CodeDeploy deployment ID format: d-[A-Z0-9]{9}.
var parityDeployIDRe = regexp.MustCompile(`^d-[A-Z0-9]{9}$`)

// createAppAndDG is a convenience helper to set up an app and deployment group.
func createAppAndDG(t *testing.T, h *codedeploy.Handler, appName, dgName string) {
	t.Helper()
	doRequest(t, h, "CreateApplication", map[string]any{
		"applicationName": appName,
		"computePlatform": "Server",
	})
	doRequest(t, h, "CreateDeploymentGroup", map[string]any{
		"applicationName":     appName,
		"deploymentGroupName": dgName,
		"serviceRoleArn":      "arn:aws:iam::000000000000:role/role",
	})
}

// TestParity_CreateDeployment_S3RevisionRoundTrip verifies that an S3 revision passed to
// CreateDeployment is stored and returned verbatim by GetDeployment, matching real AWS behavior.
func TestParity_CreateDeployment_S3RevisionRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "rev-app", "rev-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "rev-app",
		"deploymentGroupName": "rev-dg",
		"revision": map[string]any{
			"revisionType": "S3",
			"s3Location": map[string]any{
				"bucket":     "my-bucket",
				"key":        "app/bundle.zip",
				"bundleType": "zip",
				"eTag":       "abc123",
				"version":    "v1",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	deployID := createOut["deploymentId"]
	require.NotEmpty(t, deployID)

	getRec := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": deployID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		DeploymentInfo struct {
			Revision struct {
				RevisionType string `json:"revisionType"`
				S3Location   struct {
					Bucket     string `json:"bucket"`
					Key        string `json:"key"`
					BundleType string `json:"bundleType"`
					ETag       string `json:"eTag"`
					Version    string `json:"version"`
				} `json:"s3Location"`
			} `json:"revision"`
		} `json:"deploymentInfo"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	rev := getOut.DeploymentInfo.Revision
	assert.Equal(t, "S3", rev.RevisionType, "revisionType must round-trip")
	assert.Equal(t, "my-bucket", rev.S3Location.Bucket, "s3Location.bucket must round-trip")
	assert.Equal(t, "app/bundle.zip", rev.S3Location.Key, "s3Location.key must round-trip")
	assert.Equal(t, "zip", rev.S3Location.BundleType, "s3Location.bundleType must round-trip")
	assert.Equal(t, "abc123", rev.S3Location.ETag, "s3Location.eTag must round-trip")
	assert.Equal(t, "v1", rev.S3Location.Version, "s3Location.version must round-trip")
}

// TestParity_CreateDeployment_GitHubRevisionRoundTrip verifies that a GitHub revision
// round-trips through CreateDeployment → GetDeployment, matching real AWS behavior.
func TestParity_CreateDeployment_GitHubRevisionRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "gh-app", "gh-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "gh-app",
		"deploymentGroupName": "gh-dg",
		"revision": map[string]any{
			"revisionType": "GitHub",
			"gitHubLocation": map[string]any{
				"repository": "owner/repo",
				"commitId":   "deadbeef1234",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	deployID := createOut["deploymentId"]

	getRec := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": deployID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		DeploymentInfo struct {
			Revision struct {
				RevisionType   string `json:"revisionType"`
				GitHubLocation struct {
					Repository string `json:"repository"`
					CommitID   string `json:"commitId"`
				} `json:"gitHubLocation"`
			} `json:"revision"`
		} `json:"deploymentInfo"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	rev := getOut.DeploymentInfo.Revision
	assert.Equal(t, "GitHub", rev.RevisionType)
	assert.Equal(t, "owner/repo", rev.GitHubLocation.Repository)
	assert.Equal(t, "deadbeef1234", rev.GitHubLocation.CommitID)
}

// TestParity_GetDeployment_DeploymentOverview verifies that GetDeployment includes
// deploymentOverview with instance counts, matching real AWS behavior.
func TestParity_GetDeployment_DeploymentOverview(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "ov-app", "ov-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "ov-app",
		"deploymentGroupName": "ov-dg",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	getRec := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": createOut["deploymentId"]})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		DeploymentInfo struct {
			DeploymentOverview map[string]any `json:"deploymentOverview"`
		} `json:"deploymentInfo"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	ov := getOut.DeploymentInfo.DeploymentOverview
	assert.NotNil(t, ov, "GetDeployment must include deploymentOverview")
	_, hasSucceeded := ov["Succeeded"]
	assert.True(t, hasSucceeded, "deploymentOverview must include Succeeded field")
}

// TestParity_BatchGetDeployments_ReturnsRevision verifies BatchGetDeployments includes
// revision and deploymentOverview per deployment, matching real AWS behavior.
func TestParity_BatchGetDeployments_ReturnsRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "batch-app", "batch-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "batch-app",
		"deploymentGroupName": "batch-dg",
		"revision": map[string]any{
			"revisionType": "S3",
			"s3Location":   map[string]any{"bucket": "b", "key": "k", "bundleType": "zip"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	batchRec := doRequest(t, h, "BatchGetDeployments", map[string]any{
		"deploymentIds": []string{createOut["deploymentId"]},
	})
	require.Equal(t, http.StatusOK, batchRec.Code)

	type batchRevision struct {
		RevisionType string `json:"revisionType"`
	}
	type batchItem struct {
		DeploymentOverview map[string]any `json:"deploymentOverview"`
		Revision           batchRevision  `json:"revision"`
	}
	var batchOut struct {
		DeploymentsInfo []batchItem `json:"deploymentsInfo"`
	}
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batchOut))
	require.Len(t, batchOut.DeploymentsInfo, 1)

	info := batchOut.DeploymentsInfo[0]
	assert.Equal(t, "S3", info.Revision.RevisionType,
		"BatchGetDeployments must include revision per deployment")
	assert.NotNil(t, info.DeploymentOverview,
		"BatchGetDeployments must include deploymentOverview per deployment")
}

// TestParity_CreateDeployment_IDFormat verifies deployment IDs match the AWS format d-[A-Z0-9]{9}.
func TestParity_CreateDeployment_IDFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "id-app", "id-dg")

	tests := []struct{ name string }{
		{"first"},
		{"second"},
		{"third"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "CreateDeployment", map[string]any{
				"applicationName":     "id-app",
				"deploymentGroupName": "id-dg",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Regexp(t, parityDeployIDRe, out["deploymentId"],
				"deploymentId must match d-[A-Z0-9]{9}")
		})
	}
}

// TestParity_StopDeployment_StatusStopped verifies StopDeployment returns status Stopped
// and GetDeployment reflects the updated status, matching real AWS behavior.
func TestParity_StopDeployment_StatusStopped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "stop-app", "stop-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "stop-app",
		"deploymentGroupName": "stop-dg",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	deployID := createOut["deploymentId"]

	stopRec := doRequest(t, h, "StopDeployment", map[string]any{
		"deploymentId": deployID,
	})
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopOut struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopOut))
	assert.Equal(t, "Stopped", stopOut.Status, "StopDeployment must return status Stopped")

	getRec := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": deployID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		DeploymentInfo struct {
			Status string `json:"status"`
		} `json:"deploymentInfo"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "Stopped", getOut.DeploymentInfo.Status,
		"GetDeployment must reflect Stopped status after StopDeployment")
}

// TestParity_DeploymentGroup_ComputePlatformInherited verifies the deployment group
// inherits computePlatform from the application, matching real AWS behavior.
func TestParity_DeploymentGroup_ComputePlatformInherited(t *testing.T) {
	t.Parallel()

	tests := []struct {
		platform string
	}{
		{"Server"},
		{"Lambda"},
		{"ECS"},
	}

	for _, tt := range tests {
		t.Run(tt.platform, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateApplication", map[string]any{
				"applicationName": "cp-app-" + tt.platform,
				"computePlatform": tt.platform,
			})
			doRequest(t, h, "CreateDeploymentGroup", map[string]any{
				"applicationName":     "cp-app-" + tt.platform,
				"deploymentGroupName": "dg",
				"serviceRoleArn":      "arn:aws:iam::000000000000:role/role",
			})

			rec := doRequest(t, h, "GetDeploymentGroup", map[string]any{
				"applicationName":     "cp-app-" + tt.platform,
				"deploymentGroupName": "dg",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				DeploymentGroupInfo struct {
					ComputePlatform string `json:"computePlatform"`
				} `json:"deploymentGroupInfo"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.platform, out.DeploymentGroupInfo.ComputePlatform,
				"deployment group must inherit computePlatform from application")
		})
	}
}
