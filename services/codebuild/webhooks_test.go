package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"projectName":  "webhook-proj",
				"branchFilter": "main",
				"buildType":    "BUILD",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_project_name",
			body:       map[string]any{"branchFilter": "main"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "project_not_found",
			body: map[string]any{
				"projectName": "nonexistent-proj",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"projectName": "dup-webhook-proj",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create the target project for success and duplicate cases.
			if tt.name == "success" || tt.name == "duplicate_fails" {
				proj := map[string]any{
					"name":      tt.body.(map[string]any)["projectName"],
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				}
				createRec := doRequest(t, h, "CreateProject", proj)
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			if tt.name == "duplicate_fails" {
				// First webhook creation succeeds.
				rec := doRequest(t, h, "CreateWebhook", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateWebhook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateWebhook_ResponseFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create the project first.
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      "wh-proj",
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName":  "wh-proj",
		"branchFilter": "main",
		"buildType":    "BUILD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	webhook, ok := out["webhook"].(map[string]any)
	require.True(t, ok, "response should contain 'webhook' object")
	assert.Equal(t, "wh-proj", webhook["projectName"])
	assert.Equal(t, "main", webhook["branchFilter"])
	assert.Equal(t, "BUILD", webhook["buildType"])
	assert.NotEmpty(t, webhook["url"])
}

// TestCodeBuild_Webhooks covers DeleteWebhook, UpdateWebhook.
func TestCodeBuild_Webhooks(t *testing.T) {
	t.Parallel()

	t.Run("delete_webhook", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "wh-del-proj")
		doRequest(t, h, "CreateWebhook", map[string]any{
			"projectName":  "wh-del-proj",
			"branchFilter": "main",
		})

		delRec := doRequest(t, h, "DeleteWebhook", map[string]any{"projectName": "wh-del-proj"})
		assert.Equal(t, http.StatusOK, delRec.Code)
	})

	t.Run("delete_webhook_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteWebhook", map[string]any{"projectName": "ghost-proj"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update_webhook", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "wh-upd-proj")
		doRequest(t, h, "CreateWebhook", map[string]any{
			"projectName":  "wh-upd-proj",
			"branchFilter": "main",
			"buildType":    "BUILD",
		})

		updRec := doRequest(t, h, "UpdateWebhook", map[string]any{
			"projectName":  "wh-upd-proj",
			"branchFilter": "develop",
			"buildType":    "BUILD_BATCH",
		})
		require.Equal(t, http.StatusOK, updRec.Code)

		var out struct {
			Webhook struct {
				BranchFilter string `json:"branchFilter"`
				BuildType    string `json:"buildType"`
			} `json:"webhook"`
		}
		require.NoError(t, json.NewDecoder(updRec.Body).Decode(&out))
		assert.Equal(t, "develop", out.Webhook.BranchFilter)
		assert.Equal(t, "BUILD_BATCH", out.Webhook.BuildType)
	})

	t.Run("update_webhook_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateWebhook", map[string]any{
			"projectName":  "ghost-proj",
			"branchFilter": "main",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_CreateWebhook_FilterGroups verifies CreateWebhook stores and returns filterGroups,
// matching real AWS behavior.
func TestHandler_CreateWebhook_FilterGroups(t *testing.T) {
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

// TestHandler_CreateWebhook_ExtendedFields verifies the additive Webhook wire
// fields present on real AWS (status/secret/lastModifiedSecret/manualCreation/
// scopeConfiguration/pullRequestBuildPolicy) are populated on creation. This
// emulator never talks to a real GitHub/GitLab/Bitbucket, so webhook creation
// always completes synchronously with status ACTIVE.
func TestHandler_CreateWebhook_ExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestProject(t, h, "wh-ext-proj")

	rec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName":    "wh-ext-proj",
		"branchFilter":   "main",
		"manualCreation": true,
		"scopeConfiguration": map[string]any{
			"name":  "my-org",
			"scope": "GITHUB_ORGANIZATION",
		},
		"pullRequestBuildPolicy": map[string]any{
			"requiresCommentApproval": "ALL_PULL_REQUESTS",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Webhook struct {
			ScopeConfiguration     map[string]any `json:"scopeConfiguration"`
			PullRequestBuildPolicy map[string]any `json:"pullRequestBuildPolicy"`
			Status                 string         `json:"status"`
			Secret                 string         `json:"secret"`
			LastModifiedSecret     float64        `json:"lastModifiedSecret"`
			ManualCreation         bool           `json:"manualCreation"`
		} `json:"webhook"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	assert.Equal(t, "ACTIVE", out.Webhook.Status)
	assert.NotEmpty(t, out.Webhook.Secret, "CreateWebhook must return a secret")
	assert.NotZero(t, out.Webhook.LastModifiedSecret)
	assert.True(t, out.Webhook.ManualCreation)
	assert.Equal(t, "my-org", out.Webhook.ScopeConfiguration["name"])
	assert.Equal(t, "GITHUB_ORGANIZATION", out.Webhook.ScopeConfiguration["scope"])
	assert.Equal(t, "ALL_PULL_REQUESTS", out.Webhook.PullRequestBuildPolicy["requiresCommentApproval"])
}

// TestHandler_UpdateWebhook_RotateSecret verifies rotateSecret regenerates
// Webhook.Secret and bumps LastModifiedSecret, matching real AWS's
// UpdateWebhookInput.rotateSecret. Omitting rotateSecret must leave the
// secret untouched.
func TestHandler_UpdateWebhook_RotateSecret(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestProject(t, h, "wh-rotate-proj")

	createRec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName": "wh-rotate-proj",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		Webhook struct {
			Secret             string  `json:"secret"`
			LastModifiedSecret float64 `json:"lastModifiedSecret"`
		} `json:"webhook"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	originalSecret := createOut.Webhook.Secret
	require.NotEmpty(t, originalSecret)

	// Sequential (not t.Parallel()): both steps mutate the same webhook and
	// must observe each other's effects in order.
	noRotateRec := doRequest(t, h, "UpdateWebhook", map[string]any{
		"projectName":  "wh-rotate-proj",
		"branchFilter": "main",
	})
	require.Equal(t, http.StatusOK, noRotateRec.Code)

	var noRotateOut struct {
		Webhook struct {
			Secret string `json:"secret"`
		} `json:"webhook"`
	}
	require.NoError(t, json.NewDecoder(noRotateRec.Body).Decode(&noRotateOut))
	assert.Equal(t, originalSecret, noRotateOut.Webhook.Secret, "secret must be unchanged without rotateSecret")

	rotateRec := doRequest(t, h, "UpdateWebhook", map[string]any{
		"projectName":  "wh-rotate-proj",
		"rotateSecret": true,
	})
	require.Equal(t, http.StatusOK, rotateRec.Code)

	var rotateOut struct {
		Webhook struct {
			Secret             string  `json:"secret"`
			LastModifiedSecret float64 `json:"lastModifiedSecret"`
		} `json:"webhook"`
	}
	require.NoError(t, json.NewDecoder(rotateRec.Body).Decode(&rotateOut))
	assert.NotEqual(t, originalSecret, rotateOut.Webhook.Secret, "rotateSecret must regenerate the secret")
	assert.GreaterOrEqual(t, rotateOut.Webhook.LastModifiedSecret, createOut.Webhook.LastModifiedSecret)
}

// TestHandler_Webhook_MirroredOnProject verifies that a project's webhook field (as
// returned by BatchGetProjects) reflects CreateWebhook, UpdateWebhook, and DeleteWebhook,
// matching real AWS where Project.Webhook is populated once a webhook exists for that project.
func TestHandler_Webhook_MirroredOnProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "webhook-mirror-proj")

	getProjectWebhook := func() map[string]any {
		rec := doRequest(t, h, "BatchGetProjects", map[string]any{"names": []string{"webhook-mirror-proj"}})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Projects []map[string]any `json:"projects"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		require.Len(t, out.Projects, 1)

		w, _ := out.Projects[0]["webhook"].(map[string]any)

		return w
	}

	assert.Nil(t, getProjectWebhook(), "project must have no webhook before CreateWebhook")

	createRec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName":  "webhook-mirror-proj",
		"branchFilter": "main",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	w := getProjectWebhook()
	require.NotNil(t, w, "project must reflect webhook after CreateWebhook")
	assert.Equal(t, "main", w["branchFilter"])

	updateRec := doRequest(t, h, "UpdateWebhook", map[string]any{
		"projectName":  "webhook-mirror-proj",
		"branchFilter": "develop",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	w = getProjectWebhook()
	require.NotNil(t, w, "project must still reflect webhook after UpdateWebhook")
	assert.Equal(t, "develop", w["branchFilter"], "project webhook must reflect UpdateWebhook changes")

	deleteRec := doRequest(t, h, "DeleteWebhook", map[string]any{"projectName": "webhook-mirror-proj"})
	require.Equal(t, http.StatusOK, deleteRec.Code)

	assert.Nil(t, getProjectWebhook(), "project must have no webhook after DeleteWebhook")
}
