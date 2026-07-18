package ecr_test

// repository_policy_test.go — verifies repository_policy.go: Set/Get/Delete
// RepositoryPolicy, including AWS-accurate RepositoryPolicyNotFoundException
// vs RepositoryNotFoundException precedence.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestRepositoryPolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "basic_allow_policy",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ecr:GetDownloadUrlForLayer"}]}`, //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability
		},
		{
			name:   "deny_policy",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"ecr:*"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			makeRepo(t, b, "policy-repo")

			out, err := b.SetRepositoryPolicy(context.Background(), "policy-repo", tt.policy)
			require.NoError(t, err)
			assert.Equal(t, "policy-repo", out.RepositoryName)

			got, err := b.GetRepositoryPolicy(context.Background(), "policy-repo")
			require.NoError(t, err)
			assert.Equal(t, tt.policy, got.PolicyText)

			_, err = b.DeleteRepositoryPolicy(context.Background(), "policy-repo")
			require.NoError(t, err)

			_, err = b.GetRepositoryPolicy(context.Background(), "policy-repo")
			assert.ErrorIs(t, err, ecr.ErrRepositoryPolicyNotFound)
		})
	}
}

func TestGetRepositoryPolicy_NoPolicy_RepositoryPolicyNotFoundException(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "gp-no-policy-repo")

	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "gp-no-policy-repo",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", out["__type"],
		"GetRepositoryPolicy must return RepositoryPolicyNotFoundException when no policy is set")
}

func TestGetRepositoryPolicy_RepoNotFound_RepositoryNotFoundException(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "ghost-repo",
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryNotFoundException", out["__type"])
}

func TestGetRepositoryPolicy_PolicySet_ReturnsPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[]}`
	h := newAccuracyHandler()
	mustCreateRepo(t, h, "gp-set-repo")

	setRec := doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "gp-set-repo",
		"policyText":     policy,
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "gp-set-repo",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	assert.JSONEq(t, policy, out["policyText"].(string))
}

func TestDeleteRepositoryPolicy_NoPolicy_RepositoryPolicyNotFoundException(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "dp-no-policy-repo")

	rec := doAccuracy(t, h, "DeleteRepositoryPolicy", map[string]any{
		"repositoryName": "dp-no-policy-repo",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", out["__type"],
		"DeleteRepositoryPolicy must return RepositoryPolicyNotFoundException when no policy is set")
}

func TestDeleteRepositoryPolicy_PolicySet_Deletes(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "dp-set-repo")

	setRec := doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "dp-set-repo",
		"policyText":     `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	delRec := doAccuracy(t, h, "DeleteRepositoryPolicy", map[string]any{
		"repositoryName": "dp-set-repo",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "dp-set-repo",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code,
		"GetRepositoryPolicy after Delete must return RepositoryPolicyNotFoundException")
	body := parseAccuracy(t, getRec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", body["__type"])
}

func TestRepositoryPolicy_SetGet_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "repo-pol")

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"ecr:GetDownloadUrlForLayer"}]}`
	doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "repo-pol",
		"policyText":     policy,
	})

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "repo-pol",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["policyText"])
	assert.Equal(t, "repo-pol", out["repositoryName"])
	assert.Equal(t, "123456789012", out["registryId"])
}

func TestRepositoryPolicy_Delete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "repo-pol-del")

	doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "repo-pol-del",
		"policyText":     `{"Version":"2012-10-17","Statement":[]}`,
	})

	delRec := doAccuracy(t, h, "DeleteRepositoryPolicy", map[string]any{
		"repositoryName": "repo-pol-del",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "repo-pol-del",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code,
		"GetRepositoryPolicy must return RepositoryPolicyNotFoundException after policy deletion")
	body := parseAccuracy(t, getRec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", body["__type"])
}

func TestRepositoryPolicy_Get_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "ghost",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRepositoryPolicy_Overwrite(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "pol-overwrite")

	doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "pol-overwrite",
		"policyText":     `{"Version":"2012-10-17","Statement":["first"]}`,
	})

	newPolicy := `{"Version":"2012-10-17","Statement":["second"]}`
	doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "pol-overwrite",
		"policyText":     newPolicy,
	})

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "pol-overwrite",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, newPolicy, out["policyText"],
		"second SetRepositoryPolicy must overwrite the first")
}

func TestRepositoryPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "policy-repo-rt")

	policy := `{"Version":"2012-10-17","Statement":[]}`
	setRec := doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo-rt",
		"policyText":     policy,
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo-rt",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["policyText"])
	assert.Equal(t, "policy-repo-rt", out["repositoryName"])
}
