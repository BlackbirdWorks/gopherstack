package ecr_test

// handler_accuracy_ops2_test.go — ECR AWS-accuracy audit batch-2 ops (go-p6uu)
//
// Four accuracy fixes:
//
//  1. BatchGetImage: RepositoryNotFoundException when repo does not exist
//     (was: silently returning empty images+failures list)
//
//  2. BatchDeleteImage: RepositoryNotFoundException when repo does not exist
//     (was: silently returning empty deleted+failures list)
//
//  3. GetRepositoryPolicy / DeleteRepositoryPolicy: RepositoryPolicyNotFoundException
//     when repo exists but has no policy
//     (was: generic NotFoundException from ErrRegistryPolicyNotFound)
//
//  4. DescribeImages with imageIds: ImageNotFoundException when a requested image
//     is not found (repo exists)
//     (was: RepositoryNotFoundException — wrong exception type)
//
//     ListImageReferrers: ImageNotFoundException when the subject image is not found
//     (was: RepositoryNotFoundException — wrong exception type)

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func newOps2Handler() *ecr.Handler {
	return ecr.NewHandler(ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000"), nil)
}

// ── §1 BatchGetImage — repository existence ───────────────────────────────────

func TestOps2_BatchGetImage_RepoNotFound_TopLevelError(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "ghost-repo",
		"imageIds":       []map[string]any{{"imageDigest": "sha256:abc"}},
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryNotFoundException", out["__type"],
		"BatchGetImage on non-existent repo must return RepositoryNotFoundException")
}

func TestOps2_BatchGetImage_RepoExists_ImageNotFound_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "bgi-repo")

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-repo",
		"imageIds":       []map[string]any{{"imageDigest": "sha256:missing"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, images)
	assert.Len(t, failures, 1, "missing image must produce a per-entry failure, not a top-level error")
}

func TestOps2_BatchGetImage_RepoExists_ImageFound_ReturnsImage(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "bgi-found-repo")
	mustPutImage(t, h, "bgi-found-repo", "v1", `{"schemaVersion":2,"layers":[]}`)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-found-repo",
		"imageIds":       []map[string]any{{"imageTag": "v1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	assert.Len(t, images, 1, "existing image must be returned")
}

func TestOps2_BatchGetImage_MixedFound_And_Missing(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "bgi-mix-repo")
	mustPutImage(t, h, "bgi-mix-repo", "present", `{"schemaVersion":2,"layers":[]}`)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-mix-repo",
		"imageIds": []map[string]any{
			{"imageTag": "present"},
			{"imageTag": "absent"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Len(t, images, 1)
	assert.Len(t, failures, 1)
}

// ── §2 BatchDeleteImage — repository existence ────────────────────────────────

func TestOps2_BatchDeleteImage_RepoNotFound_TopLevelError(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "ghost-repo",
		"imageIds":       []map[string]any{{"imageDigest": "sha256:abc"}},
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryNotFoundException", out["__type"],
		"BatchDeleteImage on non-existent repo must return RepositoryNotFoundException")
}

func TestOps2_BatchDeleteImage_RepoExists_ImageNotFound_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "bdi-repo")

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "bdi-repo",
		"imageIds":       []map[string]any{{"imageDigest": "sha256:missing"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	deleted, _ := out["imageIds"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, deleted)
	assert.Len(t, failures, 1, "missing image must produce a per-entry failure, not a top-level error")
}

func TestOps2_BatchDeleteImage_RepoExists_ImageFound_Deletes(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "bdi-found-repo")
	mustPutImage(t, h, "bdi-found-repo", "del-tag", `{"schemaVersion":2,"layers":[]}`)

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "bdi-found-repo",
		"imageIds":       []map[string]any{{"imageTag": "del-tag"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	deleted, _ := out["imageIds"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Len(t, deleted, 1)
	assert.Empty(t, failures)
}

// ── §3 GetRepositoryPolicy / DeleteRepositoryPolicy — correct error type ──────

func TestOps2_GetRepositoryPolicy_NoPolicy_RepositoryPolicyNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "gp-no-policy-repo")

	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "gp-no-policy-repo",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", out["__type"],
		"GetRepositoryPolicy must return RepositoryPolicyNotFoundException when no policy is set")
}

func TestOps2_GetRepositoryPolicy_RepoNotFound_RepositoryNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()

	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "ghost-repo",
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryNotFoundException", out["__type"])
}

func TestOps2_GetRepositoryPolicy_PolicySet_ReturnsPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[]}`
	h := newOps2Handler()
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
	assert.Equal(t, policy, out["policyText"])
}

func TestOps2_DeleteRepositoryPolicy_NoPolicy_RepositoryPolicyNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "dp-no-policy-repo")

	rec := doAccuracy(t, h, "DeleteRepositoryPolicy", map[string]any{
		"repositoryName": "dp-no-policy-repo",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryPolicyNotFoundException", out["__type"],
		"DeleteRepositoryPolicy must return RepositoryPolicyNotFoundException when no policy is set")
}

func TestOps2_DeleteRepositoryPolicy_PolicySet_Deletes(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
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

// ── §4 DescribeImages / ListImageReferrers — ImageNotFoundException ────────────

func TestOps2_DescribeImages_ByImageID_NotFound_ImageNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "di-not-found-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "di-not-found-repo",
		"imageIds": []map[string]any{
			{"imageDigest": "sha256:9999999999999999999999999999999999999999999999999999999999999999"},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ImageNotFoundException", out["__type"],
		"DescribeImages with missing imageId must return ImageNotFoundException, not RepositoryNotFoundException")
}

func TestOps2_DescribeImages_ByTag_NotFound_ImageNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "di-tag-not-found-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "di-tag-not-found-repo",
		"imageIds": []map[string]any{
			{"imageTag": "ghost"},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ImageNotFoundException", out["__type"])
}

func TestOps2_DescribeImages_ByImageID_Found_ReturnsDetail(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "di-found-repo")
	digest := mustPutImage(t, h, "di-found-repo", "v1", `{"schemaVersion":2,"layers":[]}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "di-found-repo",
		"imageIds":       []map[string]any{{"imageDigest": digest}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Len(t, details, 1, "existing image must be returned")
}

func TestOps2_ListImageReferrers_SubjectNotFound_ImageNotFoundException(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "lir-not-found-repo")

	rec := doAccuracy(t, h, "ListImageReferrers", map[string]any{
		"repositoryName": "lir-not-found-repo",
		"subjectId": map[string]any{
			"imageDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ImageNotFoundException", out["__type"],
		"ListImageReferrers with non-existent subject must return ImageNotFoundException, not RepositoryNotFoundException")
}

func TestOps2_ListImageReferrers_SubjectFound_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newOps2Handler()
	mustCreateRepo(t, h, "lir-found-repo")
	digest := mustPutImage(t, h, "lir-found-repo", "subj", `{"schemaVersion":2,"layers":[]}`)

	rec := doAccuracy(t, h, "ListImageReferrers", map[string]any{
		"repositoryName": "lir-found-repo",
		"subjectId":      map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	referrers, _ := out["referrers"].([]any)
	assert.Empty(t, referrers, "no referrers expected for a fresh image")
}
