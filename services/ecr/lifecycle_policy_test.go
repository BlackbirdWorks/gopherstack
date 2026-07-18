package ecr_test

// lifecycle_policy_test.go — verifies lifecycle_policy.go's CRUD surface:
// Get/Put/Delete LifecyclePolicy and Start/GetLifecyclePolicyPreview, as
// distinct from the expiry-evaluation semantics covered in lifecycle_test.go.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestLifecyclePolicy_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	makeRepo(t, b, "lp-repo")

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":10}}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	out, err := b.PutLifecyclePolicy(context.Background(), "lp-repo", policy)
	require.NoError(t, err)
	assert.Equal(t, policy, out.LifecyclePolicyText)

	got, err := b.GetLifecyclePolicy(context.Background(), "lp-repo")
	require.NoError(t, err)
	assert.Equal(t, policy, got.LifecyclePolicyText)
	assert.Equal(t, 1, b.LifecyclePolicyCount())

	_, err = b.DeleteLifecyclePolicy(context.Background(), "lp-repo")
	require.NoError(t, err)
	assert.Equal(t, 0, b.LifecyclePolicyCount())

	_, err = b.GetLifecyclePolicy(context.Background(), "lp-repo")
	assert.ErrorIs(t, err, ecr.ErrLifecyclePolicyNotFound)
}

func TestPutLifecyclePolicy_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		policy     string
		wantStatus int
	}{
		{
			name:       "creates_policy",
			repoName:   "policy-repo",
			policy:     `{"rules":[{"rulePriority":1,"description":"test"}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "repo_not_found",
			repoName:   "no-such-repo",
			policy:     `{}`,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			if tt.wantStatus == http.StatusOK {
				doAccuracy(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repoName})
			}

			rec := doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
				"repositoryName":      tt.repoName,
				"lifecyclePolicyText": tt.policy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteLifecyclePolicy_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "repo_exists_but_no_policy",
			repoName:   "no-policy-repo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "repo_not_found",
			repoName:   "ghost-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			if tt.repoName == "no-policy-repo" {
				doAccuracy(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repoName})
			}

			rec := doAccuracy(t, h, "DeleteLifecyclePolicy", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestECR_DeleteLifecyclePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*ecr.Handler)
		repositoryName string
		wantStatus     int
	}{
		{
			name: "deletes lifecycle policy for existing repo",
			setup: func(h *ecr.Handler) {
				rec := doECRRequest(
					t,
					h,
					"CreateRepository",
					map[string]any{"repositoryName": "policy-repo"},
				)
				require.Equal(t, http.StatusOK, rec.Code)

				rec2 := doECRRequest(t, h, "PutLifecyclePolicy", map[string]any{
					"repositoryName":      "policy-repo",
					"lifecyclePolicyText": `{"rules":[]}`,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			},
			repositoryName: "policy-repo",
			wantStatus:     http.StatusOK,
		},
		{
			name:           "nonexistent repository returns not found",
			repositoryName: "nonexistent",
			wantStatus:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECRRequest(t, h, "DeleteLifecyclePolicy", map[string]any{
				"repositoryName": tt.repositoryName,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.repositoryName, out["repositoryName"])
			}
		})
	}
}

func TestPutLifecyclePolicy_And_GetLifecyclePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc-repo")

	policy := `{"rules":[{"rulePriority":1,"description":"keep 10",` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},` +
		`"action":{"type":"expire"}}]}`

	putRec := doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc-repo",
		"lifecyclePolicyText": policy,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "lc-repo",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["lifecyclePolicyText"],
		"GetLifecyclePolicy must return the exact policy set by PutLifecyclePolicy")
}

func TestGetLifecyclePolicy_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "ghost-repo",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetLifecyclePolicy_NonExistentPolicy_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "no-policy")

	rec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "no-policy",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLifecyclePolicy_Put_Delete_Gone(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc-del")

	policy := `{"rules":[{"rulePriority":1,` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":5},` +
		`"action":{"type":"expire"}}]}`
	doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc-del",
		"lifecyclePolicyText": policy,
	})

	delRec := doAccuracy(t, h, "DeleteLifecyclePolicy", map[string]any{
		"repositoryName": "lc-del",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// After deletion, Get must return 404.
	getRec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "lc-del",
	})
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestLifecyclePolicy_Delete_NonExistent_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc-noexist")

	rec := doAccuracy(t, h, "DeleteLifecyclePolicy", map[string]any{
		"repositoryName": "lc-noexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLifecyclePolicyPreview_WithPolicy(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc-preview")
	mustPutImage(t, h, "lc-preview", "v1", `{"schemaVersion":2,"v":1}`)

	policy := `{"rules":[{"rulePriority":1,"description":"keep none",` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":0},` +
		`"action":{"type":"expire"}}]}`

	previewRec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName":      "lc-preview",
		"lifecyclePolicyText": policy,
	})
	require.Equal(t, http.StatusOK, previewRec.Code)
	previewOut := parseAccuracy(t, previewRec)
	assert.NotEmpty(t, previewOut["status"])

	getPreview := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "lc-preview",
	})
	require.Equal(t, http.StatusOK, getPreview.Code)
	pOut := parseAccuracy(t, getPreview)
	assert.Equal(t, "COMPLETE", pOut["status"])
}

func TestLifecyclePolicyPreview_UsesExistingPolicy(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc-use-existing")

	policy := `{"rules":[{"rulePriority":1,` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},` +
		`"action":{"type":"expire"}}]}`
	doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc-use-existing",
		"lifecyclePolicyText": policy,
	})

	// StartLifecyclePolicyPreview without policyText → uses stored policy.
	previewRec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName": "lc-use-existing",
	})
	require.Equal(t, http.StatusOK, previewRec.Code)
	pOut := parseAccuracy(t, previewRec)
	assert.Equal(t, policy, pOut["lifecyclePolicyText"],
		"preview must use the stored lifecycle policy when policyText not provided")
}

func TestGetLifecyclePolicyPreview_NoPreview_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "no-preview")

	rec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "no-preview",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLifecyclePolicyPreview_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "preview-repo")
	policyText := `{"rules":[{"rulePriority":1,` +
		`"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":1},` +
		`"action":{"type":"expire"}}]}`
	doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "preview-repo",
		"lifecyclePolicyText": policyText,
	})

	startRec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName": "preview-repo",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	startOut := parseAccuracy(t, startRec)
	assert.Equal(t, "preview-repo", startOut["repositoryName"])
	assert.Equal(t, "COMPLETE", startOut["status"])

	getRec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "preview-repo",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	getOut := parseAccuracy(t, getRec)
	assert.Equal(t, "preview-repo", getOut["repositoryName"])
	assert.Equal(t, "COMPLETE", getOut["status"])
	assert.Equal(t, policyText, getOut["lifecyclePolicyText"],
		"GetLifecyclePolicyPreview must return the policy text used for the preview")
}

func TestStartLifecyclePolicyPreview_WithInlinePolicy(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "inline-preview")

	inlinePolicy := `{"rules":[]}`
	rec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName":      "inline-preview",
		"lifecyclePolicyText": inlinePolicy,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, inlinePolicy, out["lifecyclePolicyText"])
}

func TestGetLifecyclePolicyPreview_NotStarted_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "no-preview-2")

	rec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "no-preview-2",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"GetLifecyclePolicyPreview without StartLifecyclePolicyPreview must error")
}
