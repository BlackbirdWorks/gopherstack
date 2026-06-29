package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func makeProject(t *testing.T, h *codebuild.Handler, name string) {
	t.Helper()
	doRequest(t, h, "CreateProject", map[string]any{
		"name":        name,
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})
}

// TestParity_StartBuildBatch_ArnSet verifies StartBuildBatch returns a non-empty Arn,
// matching real AWS behavior.
func TestParity_StartBuildBatch_ArnSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "batch-arn-proj")

	rec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-arn-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	batchArn, _ := out.BuildBatch["arn"].(string)
	assert.NotEmpty(t, batchArn, "StartBuildBatch must return a non-empty arn")
	assert.Contains(t, batchArn, "arn:aws:codebuild", "arn must be a valid ARN format")
}

// TestParity_RetryBuildBatch_ArnSet verifies RetryBuildBatch sets a non-empty Arn.
func TestParity_RetryBuildBatch_ArnSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "retry-batch-proj")

	startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "retry-batch-proj"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	batchID, _ := startOut.BuildBatch["id"].(string)
	require.NotEmpty(t, batchID)

	retryRec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": batchID})
	require.Equal(t, http.StatusOK, retryRec.Code)

	var retryOut struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(retryRec.Body.Bytes(), &retryOut))
	arn, _ := retryOut.BuildBatch["arn"].(string)
	assert.NotEmpty(t, arn, "RetryBuildBatch must return a non-empty arn")
}

// TestParity_BatchGetBuilds_ARNLookup verifies BatchGetBuilds accepts ARNs, not just IDs.
func TestParity_BatchGetBuilds_ARNLookup(t *testing.T) {
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

// TestParity_RetryBuild_InheritsFields verifies RetryBuild inherits environment/source/serviceRole
// from the original build, matching real AWS behavior.
func TestParity_RetryBuild_InheritsFields(t *testing.T) {
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

// TestParity_BatchDeleteBuilds_NotDeleted verifies BatchDeleteBuilds includes buildsNotDeleted
// for IDs that did not exist, matching real AWS behavior.
func TestParity_BatchDeleteBuilds_NotDeleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchDeleteBuilds", map[string]any{
		"ids": []string{"nonexistent-proj:abc123", "also-missing:xyz456"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		BuildsDeleted    []string         `json:"buildsDeleted"`
		BuildsNotDeleted []map[string]any `json:"buildsNotDeleted"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.BuildsDeleted, "no builds deleted when IDs do not exist")
	assert.Len(t, out.BuildsNotDeleted, 2, "both missing IDs must appear in buildsNotDeleted")

	if len(out.BuildsNotDeleted) >= 1 {
		assert.NotEmpty(t, out.BuildsNotDeleted[0]["id"], "buildsNotDeleted item must have id field")
		assert.NotEmpty(t, out.BuildsNotDeleted[0]["statusCode"], "buildsNotDeleted item must have statusCode field")
	}
}

// TestParity_UpdateProjectVisibility_PublicAlias verifies UpdateProjectVisibility returns
// publicProjectAlias when visibility is PUBLIC_READ, matching real AWS behavior.
func TestParity_UpdateProjectVisibility_PublicAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":        "vis-alias-proj",
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		Project map[string]any `json:"project"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	projectArn, _ := createOut.Project["arn"].(string)
	require.NotEmpty(t, projectArn)

	rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
		"projectArn":        projectArn,
		"projectVisibility": "PUBLIC_READ",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ProjectArn         string `json:"projectArn"`
		ProjectVisibility  string `json:"projectVisibility"`
		PublicProjectAlias string `json:"publicProjectAlias"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "PUBLIC_READ", out.ProjectVisibility)
	assert.NotEmpty(t, out.PublicProjectAlias,
		"UpdateProjectVisibility with PUBLIC_READ must return publicProjectAlias")
}

// TestParity_Webhook_FilterGroups verifies CreateWebhook stores and returns filterGroups,
// matching real AWS behavior.
func TestParity_Webhook_FilterGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateProject", map[string]any{
		"name":        "wh-fg-proj",
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "GITHUB", "location": "https://github.com/example/repo"},
	})

	rec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName": "wh-fg-proj",
		"buildType":   "BUILD",
		"filterGroups": [][]map[string]any{
			{
				{"type": "EVENT", "pattern": "PUSH"},
				{"type": "HEAD_REF", "pattern": "^refs/heads/main$"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Webhook map[string]any `json:"webhook"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	w := out.Webhook

	fgs, hasFGs := w["filterGroups"]
	assert.True(t, hasFGs, "CreateWebhook must return filterGroups")
	fgList, _ := fgs.([]any)
	assert.Len(t, fgList, 1, "filterGroups must contain one group")
	if len(fgList) > 0 {
		filters, _ := fgList[0].([]any)
		assert.Len(t, filters, 2, "filter group must contain two filters")
	}
}

// TestParity_StartBuild_OverrideFields verifies StartBuild applies override fields
// beyond just environmentVariablesOverride, matching real AWS behavior.
func TestParity_StartBuild_OverrideFields(t *testing.T) {
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
