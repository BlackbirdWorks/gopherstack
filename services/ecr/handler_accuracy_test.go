package ecr_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newAccuracyBackend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

func newAccuracyHandler() *ecr.Handler {
	return ecr.NewHandler(newAccuracyBackend(), nil)
}

func doAccuracy(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

func parseAccuracy(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func mustCreateRepo(t *testing.T, h *ecr.Handler, name string) {
	t.Helper()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{"repositoryName": name})
	require.Equal(t, http.StatusOK, rec.Code, "CreateRepository %s failed: %s", name, rec.Body.String())
}

func mustPutImage(t *testing.T, h *ecr.Handler, repoName, tag, manifest string) string {
	t.Helper()
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": repoName,
		"imageManifest":  manifest,
		"imageTag":       tag,
	})
	require.Equal(t, http.StatusOK, rec.Code, "PutImage failed: %s", rec.Body.String())
	out := parseAccuracy(t, rec)
	img, _ := out["image"].(map[string]any)

	return img["imageDigest"].(string)
}

// ── Issue 1: KMSKey round-trip ────────────────────────────────────────────────

func TestAccuracy_CreateRepository_KMSKey_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "kms-repo",
		"encryptionConfiguration": map[string]any{
			"encryptionType": "KMS",
			"kmsKey":         "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, ok := out["repository"].(map[string]any)
	require.True(t, ok, "response should have repository field")

	enc, ok := repo["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "encryptionConfiguration should be present")
	assert.Equal(t, "KMS", enc["encryptionType"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123", enc["kmsKey"])
}

func TestAccuracy_CreateRepository_KMSKey_PresentInDescribe(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "kms-repo2",
		"encryptionConfiguration": map[string]any{
			"encryptionType": "KMS",
			"kmsKey":         "arn:aws:kms:us-east-1:123456789012:key/key-002",
		},
	})

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"kms-repo2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 1)

	repo, _ := repos[0].(map[string]any)
	enc, ok := repo["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeRepositories must return encryptionConfiguration")
	assert.Equal(t, "KMS", enc["encryptionType"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/key-002", enc["kmsKey"],
		"kmsKey must be persisted and returned by DescribeRepositories")
}

func TestAccuracy_CreateRepository_KMSKey_Backend_DeepCopy(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	repo, err := b.CreateRepository("kms-deep", "MUTABLE", false, "KMS",
		"arn:aws:kms:us-east-1:123456789012:key/mrk-copy")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/mrk-copy", repo.KMSKey)

	// Mutating returned copy must not affect stored state.
	repo.KMSKey = "mutated"
	repos, err := b.DescribeRepositories([]string{"kms-deep"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/mrk-copy", repos[0].KMSKey,
		"returned copy mutation must not affect stored state")
}

func TestAccuracy_CreateRepository_DefaultEncryptionType_IsAES256(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "default-enc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo := out["repository"].(map[string]any)
	enc, ok := repo["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "encryptionConfiguration must always be present")
	assert.Equal(t, "AES256", enc["encryptionType"],
		"default encryption type must be AES256")
}

func TestAccuracy_CreateRepository_KMSKey_AbsentWhenNotSet(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "no-kms",
		"encryptionConfiguration": map[string]any{
			"encryptionType": "AES256",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo := out["repository"].(map[string]any)
	enc, _ := repo["encryptionConfiguration"].(map[string]any)
	kmsKey, hasKey := enc["kmsKey"]
	if hasKey {
		assert.Empty(t, kmsKey, "kmsKey must be absent or empty for AES256 repos")
	}
}

// ── Issue 2: imagePushedAt numeric format ────────────────────────────────────

func TestAccuracy_DescribeImages_ImagePushedAt_IsNumeric(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "img-repo")
	mustPutImage(t, h, "img-repo", "latest", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "img-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Decode as raw JSON to verify imagePushedAt is a number, not a string.
	var raw struct {
		ImageDetails []map[string]json.RawMessage `json:"imageDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.Len(t, raw.ImageDetails, 1)

	pushedAtRaw, ok := raw.ImageDetails[0]["imagePushedAt"]
	require.True(t, ok, "imagePushedAt must be present")

	var pushedAt float64
	require.NoError(t, json.Unmarshal(pushedAtRaw, &pushedAt),
		"imagePushedAt must decode as a JSON number (float64), not a string")
	assert.Greater(t, pushedAt, float64(0), "imagePushedAt must be a positive Unix timestamp")
}

func TestAccuracy_DescribeImages_ImagePushedAt_NotZero(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "ts-repo")
	mustPutImage(t, h, "ts-repo", "v1", `{"schemaVersion":2,"tag":"v1"}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "ts-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 1)
	detail := details[0].(map[string]any)

	pushedAt, ok := detail["imagePushedAt"].(float64)
	require.True(t, ok, "imagePushedAt must be a number")
	assert.Greater(t, pushedAt, float64(0), "imagePushedAt must be non-zero after PutImage")
}

// ── Issue 3: GetAuthorizationToken registryIds ────────────────────────────────

func TestAccuracy_GetAuthorizationToken_NoRegistryIds_ReturnsSingleToken(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1, "default call returns exactly one token")

	token := data[0].(map[string]any)
	assert.NotEmpty(t, token["authorizationToken"], "authorizationToken must be non-empty")
	assert.Greater(t, token["expiresAt"].(float64), float64(0), "expiresAt must be future unix timestamp")
}

func TestAccuracy_GetAuthorizationToken_MultipleRegistryIds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{
		"registryIds": []string{"111111111111", "222222222222", "333333333333"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	assert.Len(t, data, 3,
		"when registryIds has N elements, N authorization tokens must be returned")
}

func TestAccuracy_GetAuthorizationToken_SingleRegistryId(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{
		"registryIds": []string{"123456789012"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	token := data[0].(map[string]any)
	assert.NotEmpty(t, token["authorizationToken"])
}

func TestAccuracy_GetAuthorizationToken_ProxyEndpoint_HasHTTPSPrefix(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "myregistry.example.com:5000")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	token := data[0].(map[string]any)
	proxyEndpoint, _ := token["proxyEndpoint"].(string)
	assert.True(t,
		len(proxyEndpoint) == 0 ||
			proxyEndpoint[:8] == "https://" ||
			proxyEndpoint[:7] == "http://",
		"proxyEndpoint must use https:// or http:// scheme, got %q", proxyEndpoint)
}

func TestAccuracy_GetAuthorizationToken_ExpiresAt_InFuture(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	expiresAt, _ := data[0].(map[string]any)["expiresAt"].(float64)
	assert.Greater(t, expiresAt, float64(1_700_000_000),
		"expiresAt must be a Unix timestamp well in the future")
}

// ── Issue 4: BatchCheckLayerAvailability repo existence ───────────────────────

func TestAccuracy_BatchCheckLayerAvailability_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "does-not-exist",
		"layerDigests":   []string{"sha256:aabbcc"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"BatchCheckLayerAvailability must return 404 for non-existent repository")
}

func TestAccuracy_BatchCheckLayerAvailability_ExistingRepo_AvailableLayer(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "layer-repo")

	// Upload a layer then check its availability.
	doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "layer-repo",
		"uploadId":       "upload-abc",
		"layerDigests":   []string{"sha256:deadbeef"},
	})

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "layer-repo",
		"layerDigests":   []string{"sha256:deadbeef"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	require.Len(t, layers, 1)
	layer := layers[0].(map[string]any)
	assert.Equal(t, "sha256:deadbeef", layer["layerDigest"])
	assert.Equal(t, "AVAILABLE", layer["layerAvailability"])
}

func TestAccuracy_BatchCheckLayerAvailability_ExistingRepo_MissingLayer(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "empty-layer-repo")

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "empty-layer-repo",
		"layerDigests":   []string{"sha256:nothere"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, layers, "no available layers expected")
	require.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	assert.Equal(t, "sha256:nothere", failure["layerDigest"])
}

// ── Issue 5: DeleteRepository force flag ──────────────────────────────────────

func TestAccuracy_DeleteRepository_NonEmpty_WithoutForce_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "nonempty-repo")
	mustPutImage(t, h, "nonempty-repo", "latest", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DeleteRepository", map[string]any{
		"repositoryName": "nonempty-repo",
		// force omitted (defaults to false)
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"deleting a non-empty repo without force must return 400")

	out := parseAccuracy(t, rec)
	errType, _ := out["__type"].(string)
	assert.Equal(t, "RepositoryNotEmptyException", errType)
}

func TestAccuracy_DeleteRepository_NonEmpty_WithForce_Succeeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "force-repo")
	mustPutImage(t, h, "force-repo", "v1", `{"schemaVersion":2}`)
	mustPutImage(t, h, "force-repo", "v2", `{"schemaVersion":2,"v":"2"}`)

	rec := doAccuracy(t, h, "DeleteRepository", map[string]any{
		"repositoryName": "force-repo",
		"force":          true,
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"force-delete of non-empty repo must succeed")

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	assert.Equal(t, "force-repo", repo["repositoryName"])
}

func TestAccuracy_DeleteRepository_Empty_WithoutForce_Succeeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "empty-del-repo")

	rec := doAccuracy(t, h, "DeleteRepository", map[string]any{
		"repositoryName": "empty-del-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"empty repo can be deleted without force")
}

func TestAccuracy_DeleteRepository_Force_False_ExplicitlySet(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "explicit-noforce")
	mustPutImage(t, h, "explicit-noforce", "img", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DeleteRepository", map[string]any{
		"repositoryName": "explicit-noforce",
		"force":          false,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"explicit force=false must still block non-empty repo deletion")
}

func TestAccuracy_DeleteRepository_Cascades_LayerUploads(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository("cascade-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	// Initiate a layer upload for the repo.
	result, err := b.InitiateLayerUpload("cascade-repo")
	require.NoError(t, err)
	uploadID := result.UploadID

	// Record how many layer uploads exist before deletion.
	assert.Equal(t, 1, b.LayerUploadCount(), "one in-progress upload expected before delete")

	// Delete the repository.
	_, err = b.DeleteRepository("cascade-repo")
	require.NoError(t, err)

	assert.Equal(t, 0, b.LayerUploadCount(),
		"DeleteRepository must clean up in-progress layer uploads for the deleted repo (upload %s)", uploadID)
}

// ── Issue 6: IMMUTABLE tag enforcement ───────────────────────────────────────

func TestAccuracy_PutImage_IMMUTABLE_SameTag_DifferentDigest_Rejected(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-repo",
		"imageTagMutability": "IMMUTABLE",
	})

	// First push succeeds.
	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo",
		"imageManifest":  `{"schemaVersion":2,"v":"1"}`,
		"imageTag":       "stable",
	})
	require.Equal(t, http.StatusOK, rec1.Code, "first push must succeed")

	// Second push with same tag but different manifest (different digest) must fail.
	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo",
		"imageManifest":  `{"schemaVersion":2,"v":"2"}`,
		"imageTag":       "stable",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code,
		"retagging in an IMMUTABLE repo must return 400")

	out := parseAccuracy(t, rec2)
	errType, _ := out["__type"].(string)
	assert.Equal(t, "ImageTagAlreadyExistsException", errType)
}

func TestAccuracy_PutImage_IMMUTABLE_NewTag_Succeeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-new",
		"imageTagMutability": "IMMUTABLE",
	})

	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-new",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "v1",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Different tag is always allowed even in IMMUTABLE repo.
	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-new",
		"imageManifest":  `{"schemaVersion":2,"v":"2"}`,
		"imageTag":       "v2",
	})
	assert.Equal(t, http.StatusOK, rec2.Code,
		"pushing a new tag in an IMMUTABLE repo must succeed")
}

func TestAccuracy_PutImage_IMMUTABLE_SameTagSameDigest_Idempotent(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-idem",
		"imageTagMutability": "IMMUTABLE",
	})

	manifest := `{"schemaVersion":2,"idempotent":true}`

	// First push.
	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-idem",
		"imageManifest":  manifest,
		"imageTag":       "prod",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Exact same push must be idempotent (same tag, same manifest → same digest).
	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-idem",
		"imageManifest":  manifest,
		"imageTag":       "prod",
	})
	assert.Equal(t, http.StatusOK, rec2.Code,
		"pushing identical content to an IMMUTABLE repo must be idempotent")
}

func TestAccuracy_PutImage_MUTABLE_Retag_Succeeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "mutable-retag")

	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "mutable-retag",
		"imageManifest":  `{"schemaVersion":2,"v":"1"}`,
		"imageTag":       "latest",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Retag with different content in MUTABLE repo must succeed.
	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "mutable-retag",
		"imageManifest":  `{"schemaVersion":2,"v":"2"}`,
		"imageTag":       "latest",
	})
	assert.Equal(t, http.StatusOK, rec2.Code,
		"retagging in a MUTABLE repo must always succeed")
}

func TestAccuracy_PutImageTagMutability_Switch_IMMUTABLE_Enforced(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "switch-immutable")

	// Push while MUTABLE.
	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "switch-immutable",
		"imageManifest":  `{"schemaVersion":2,"orig":true}`,
		"imageTag":       "rel",
	})

	// Switch to IMMUTABLE.
	recSwitch := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "switch-immutable",
		"imageTagMutability": "IMMUTABLE",
	})
	require.Equal(t, http.StatusOK, recSwitch.Code)

	// Now retagging must be rejected.
	recRetag := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "switch-immutable",
		"imageManifest":  `{"schemaVersion":2,"orig":false}`,
		"imageTag":       "rel",
	})
	assert.Equal(t, http.StatusBadRequest, recRetag.Code,
		"after switching to IMMUTABLE, retagging must be rejected")
}

// ── Issue 7: DescribeRepositories pagination ──────────────────────────────────

func TestAccuracy_DescribeRepositories_MaxResults_LimitsResponse(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, name := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		mustCreateRepo(t, h, name)
	}

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	assert.Len(t, repos, 2, "maxResults=2 must return at most 2 repositories")
	assert.NotEmpty(t, out["nextToken"], "nextToken must be present when more repos exist")
}

func TestAccuracy_DescribeRepositories_NextToken_ContinuesPagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, name := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		mustCreateRepo(t, h, name)
	}

	// Fetch first page.
	rec1 := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 3,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	out1 := parseAccuracy(t, rec1)
	page1, _ := out1["repositories"].([]any)
	require.Len(t, page1, 3)
	token, _ := out1["nextToken"].(string)
	require.NotEmpty(t, token)

	// Fetch second page using the token.
	rec2 := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"nextToken": token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	out2 := parseAccuracy(t, rec2)
	page2, _ := out2["repositories"].([]any)
	assert.NotEmpty(t, page2, "second page must have results")
}

func TestAccuracy_DescribeRepositories_FullPagination_CoversAll(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	names := []string{"r1", "r2", "r3", "r4", "r5", "r6"}
	for _, name := range names {
		mustCreateRepo(t, h, name)
	}

	seen := map[string]bool{}
	token := ""

	for {
		body := map[string]any{"maxResults": 2}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doAccuracy(t, h, "DescribeRepositories", body)
		require.Equal(t, http.StatusOK, rec.Code)

		out := parseAccuracy(t, rec)
		page, _ := out["repositories"].([]any)

		for _, r := range page {
			repo := r.(map[string]any)
			seen[repo["repositoryName"].(string)] = true
		}

		next, _ := out["nextToken"].(string)
		if next == "" {
			break
		}

		token = next
	}

	assert.Len(t, seen, len(names), "full pagination must enumerate all repositories exactly once")
	for _, name := range names {
		assert.True(t, seen[name], "repo %s must appear in paginated results", name)
	}
}

func TestAccuracy_DescribeRepositories_NoNextToken_AtEnd(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "only-one")

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	nextToken, hasToken := out["nextToken"]
	if hasToken {
		assert.Empty(t, nextToken, "nextToken must be absent or empty on the last page")
	}
}

func TestAccuracy_DescribeRepositories_SortedByName(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, name := range []string{"zzz", "aaa", "mmm", "bbb"} {
		mustCreateRepo(t, h, name)
	}

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 4)

	names := make([]string, 0, len(repos))
	for _, r := range repos {
		names = append(names, r.(map[string]any)["repositoryName"].(string))
	}

	assert.Equal(t, []string{"aaa", "bbb", "mmm", "zzz"}, names,
		"DescribeRepositories must return repos sorted alphabetically by name")
}

// ── Issue 8: ListImages pagination ───────────────────────────────────────────

func TestAccuracy_ListImages_MaxResults_LimitsResponse(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "paginated-images")

	for i := range 5 {
		mustPutImage(t, h, "paginated-images",
			"",
			`{"schemaVersion":2,"idx":`+string(rune('0'+i))+`}`)
	}

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "paginated-images",
		"maxResults":     3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 3, "maxResults=3 must return at most 3 image identifiers")
	assert.NotEmpty(t, out["nextToken"], "nextToken must be present when more images exist")
}

func TestAccuracy_ListImages_NextToken_ContinuesPagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "img-paginate")

	for i := range 4 {
		mustPutImage(t, h, "img-paginate",
			"",
			`{"schemaVersion":2,"n":`+string(rune('0'+i))+`}`)
	}

	rec1 := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "img-paginate",
		"maxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	out1 := parseAccuracy(t, rec1)
	page1, _ := out1["imageIds"].([]any)
	require.Len(t, page1, 2)
	token := out1["nextToken"].(string)
	require.NotEmpty(t, token)

	rec2 := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "img-paginate",
		"nextToken":      token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	out2 := parseAccuracy(t, rec2)
	page2, _ := out2["imageIds"].([]any)
	assert.NotEmpty(t, page2, "second page must return remaining images")
}

// ── Deep copy and state isolation ────────────────────────────────────────────

func TestAccuracy_DescribeRepositories_ReturnedSlice_Isolated(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository("iso-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	repos1, err := b.DescribeRepositories(nil)
	require.NoError(t, err)
	require.Len(t, repos1, 1)

	// Mutate the returned struct — must not affect stored state.
	repos1[0].RepositoryName = "mutated-name"
	repos1[0].ImageTagMutability = "IMMUTABLE"

	repos2, err := b.DescribeRepositories(nil)
	require.NoError(t, err)
	require.Len(t, repos2, 1)
	assert.Equal(t, "iso-repo", repos2[0].RepositoryName,
		"mutating the returned slice must not affect the stored repository")
	assert.Equal(t, "MUTABLE", repos2[0].ImageTagMutability,
		"mutating returned copy must not affect stored imageTagMutability")
}

func TestAccuracy_Repository_ExclusionFilters_DeepCopy(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository("filter-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	filters := []ecr.ImageTagMutabilityExclusionFilter{
		{Filter: "v1.*", FilterType: "WILDCARD"},
	}
	_, err = b.PutImageTagMutability("filter-repo", "IMMUTABLE", filters)
	require.NoError(t, err)

	// Mutate the original filter slice.
	filters[0].Filter = "mutated"

	repos, err := b.DescribeRepositories([]string{"filter-repo"})
	require.NoError(t, err)
	require.Len(t, repos, 1)
	require.Len(t, repos[0].ImageTagMutabilityExclusionFilters, 1)
	assert.Equal(t, "v1.*", repos[0].ImageTagMutabilityExclusionFilters[0].Filter,
		"stored ExclusionFilters must be isolated from caller mutations")
}

func TestAccuracy_PutImage_ReturnedImage_DeepCopy(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository("img-copy", "MUTABLE", false, "", "")
	require.NoError(t, err)

	img1, err := b.PutImage("img-copy", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	// Mutate returned image.
	img1.RepositoryName = "mutated"

	// Retrieve via DescribeImages — must still see original.
	imgs, err := b.DescribeImages("img-copy", nil)
	require.NoError(t, err)
	require.Len(t, imgs, 1)
	assert.Equal(t, "img-copy", imgs[0].RepositoryName,
		"mutation of PutImage return value must not affect stored image")
}

// ── Repository ARN and URI format ────────────────────────────────────────────

func TestAccuracy_CreateRepository_ARN_Format(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "my/namespace/app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo := out["repository"].(map[string]any)

	arn, _ := repo["repositoryArn"].(string)
	assert.Contains(t, arn, "arn:aws:ecr:", "ARN must begin with arn:aws:ecr:")
	assert.Contains(t, arn, "123456789012", "ARN must contain the account ID")
	assert.Contains(t, arn, "my/namespace/app", "ARN must contain the repository name")
}

func TestAccuracy_CreateRepository_URI_Format(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "uri-test-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo := out["repository"].(map[string]any)

	uri, _ := repo["repositoryUri"].(string)
	assert.Contains(t, uri, "uri-test-repo", "repositoryUri must contain the repository name")
}

func TestAccuracy_CreateRepository_RegistryID_Present(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "registry-id-check",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo := out["repository"].(map[string]any)

	registryID, _ := repo["registryId"].(string)
	assert.Equal(t, "123456789012", registryID,
		"registryId must equal the account ID configured on the backend")
}

// ── ScanFrequency accuracy ────────────────────────────────────────────────────

func TestAccuracy_BatchGetRepositoryScanningConfiguration_ScanFrequency_ScanOnPush(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-on-push-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-on-push-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)
	cfg := configs[0].(map[string]any)
	assert.Equal(t, "SCAN_ON_PUSH", cfg["scanFrequency"],
		"scanFrequency must be SCAN_ON_PUSH when scanOnPush=true")
}

func TestAccuracy_BatchGetRepositoryScanningConfiguration_ScanFrequency_Manual(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "manual-scan-repo")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"manual-scan-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)
	cfg := configs[0].(map[string]any)
	assert.Equal(t, "MANUAL", cfg["scanFrequency"],
		"scanFrequency must be MANUAL when scanOnPush=false")
}

// ── Tag operations ───────────────────────────────────────────────────────────

func TestAccuracy_CreateRepository_Tags_Persisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tagged-repo",
		"tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	require.Len(t, tags, 2, "tags provided during CreateRepository must be stored")

	tagMap := map[string]string{}
	for _, t := range tags {
		tag := t.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}
	assert.Equal(t, "prod", tagMap["Env"])
	assert.Equal(t, "platform", tagMap["Team"])
}

func TestAccuracy_UntagResource_RemovesSpecificKeys(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "untag-repo",
		"tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"remove"},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "keep", tag["Key"])
}

// ── Lifecycle policy round-trip ───────────────────────────────────────────────

func TestAccuracy_PutLifecyclePolicy_And_Get_RoundTrip(t *testing.T) {
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

func TestAccuracy_GetLifecyclePolicy_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "ghost-repo",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── Repository policy round-trip ──────────────────────────────────────────────

func TestAccuracy_RepositoryPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "policy-repo")

	policy := `{"Version":"2012-10-17","Statement":[]}`
	setRec := doAccuracy(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo",
		"policyText":     policy,
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	getRec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "policy-repo",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["policyText"])
	assert.Equal(t, "policy-repo", out["repositoryName"])
}

// ── PullThroughCacheRule round-trip ───────────────────────────────────────────

func TestAccuracy_PullThroughCacheRule_Create_Describe_Delete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	createRec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dockerhub",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createOut := parseAccuracy(t, createRec)
	assert.Equal(t, "dockerhub", createOut["ecrRepositoryPrefix"])
	assert.Greater(t, createOut["createdAt"].(float64), float64(0))

	descRec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	descOut := parseAccuracy(t, descRec)
	rules, _ := descOut["pullThroughCacheRules"].([]any)
	assert.Len(t, rules, 1)

	delRec := doAccuracy(t, h, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dockerhub",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	descRec2 := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{})
	descOut2 := parseAccuracy(t, descRec2)
	rules2, _ := descOut2["pullThroughCacheRules"].([]any)
	assert.Empty(t, rules2, "rule must be deleted")
}

// ── AccountSetting round-trip ─────────────────────────────────────────────────

func TestAccuracy_AccountSetting_Put_Get_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	putRec := doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "BASIC_SCAN_TYPE_VERSION",
		"value": "AWS_NATIVE",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetAccountSetting", map[string]any{
		"name": "BASIC_SCAN_TYPE_VERSION",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, "AWS_NATIVE", out["value"])
}

// ── PullTimeUpdateExclusion ───────────────────────────────────────────────────

func TestAccuracy_PullTimeUpdateExclusion_Register_List_Deregister(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	arn := "arn:aws:iam::123456789012:role/MyRole"

	regRec := doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, regRec.Code)
	regOut := parseAccuracy(t, regRec)
	assert.Equal(t, arn, regOut["principalArn"])
	assert.Greater(t, regOut["createdAt"].(float64), float64(0))

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	exclusions, _ := listOut["pullTimeUpdateExclusions"].([]any)
	assert.Len(t, exclusions, 1)
	assert.Equal(t, arn, exclusions[0])

	deregRec := doAccuracy(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, deregRec.Code)

	listRec2 := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	listOut2 := parseAccuracy(t, listRec2)
	exclusions2, _ := listOut2["pullTimeUpdateExclusions"].([]any)
	assert.Empty(t, exclusions2, "exclusion must be removed after deregister")
}

// ── DescribeRepositories with specific names ──────────────────────────────────

func TestAccuracy_DescribeRepositories_ByName_ReturnsOnly_Requested(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, name := range []string{"cat", "dog", "fish"} {
		mustCreateRepo(t, h, name)
	}

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"cat", "fish"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 2)

	names := map[string]bool{}
	for _, r := range repos {
		names[r.(map[string]any)["repositoryName"].(string)] = true
	}
	assert.True(t, names["cat"])
	assert.True(t, names["fish"])
	assert.False(t, names["dog"], "dog must not be in the response when not requested")
}

func TestAccuracy_DescribeRepositories_UnknownName_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"does-not-exist"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── Image scan round-trip ─────────────────────────────────────────────────────

func TestAccuracy_StartImageScan_And_GetFindings(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-repo")
	digest := mustPutImage(t, h, "scan-repo", "latest", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-repo",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)
	scanOut := parseAccuracy(t, scanRec)
	status, _ := scanOut["imageScanStatus"].(map[string]any)
	assert.NotEmpty(t, status["status"])

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-repo",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, findRec.Code)
	findOut := parseAccuracy(t, findRec)
	assert.Equal(t, "scan-repo", findOut["repositoryName"])
}

// ── Registry policy round-trip ────────────────────────────────────────────────

func TestAccuracy_RegistryPolicy_Put_Get_Delete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	policy := `{"Version":"2012-10-17","Statement":[]}`

	putRec := doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": policy})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["policyText"])

	delRec := doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec2 := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, getRec2.Code,
		"GetRegistryPolicy must return 404 after policy is deleted")
}

// ── DescribeRegistry ──────────────────────────────────────────────────────────

func TestAccuracy_DescribeRegistry_ReturnsRegistryID(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "123456789012", out["registryId"],
		"DescribeRegistry must return the configured account ID as registryId")
}

// ── Replication configuration ─────────────────────────────────────────────────

func TestAccuracy_PutReplicationConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	cfg := map[string]any{
		"rules": []map[string]any{
			{
				"destinations": []map[string]any{
					{"region": "us-west-2", "registryId": "999999999999"},
				},
			},
		},
	}

	putRec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": cfg,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	out := parseAccuracy(t, descRec)
	repCfg, ok := out["replicationConfiguration"].(map[string]any)
	require.True(t, ok, "replicationConfiguration must be present after PutReplicationConfiguration")
	rules, _ := repCfg["rules"].([]any)
	assert.Len(t, rules, 1)
}

// ── CreateRepository idempotency error ───────────────────────────────────────

func TestAccuracy_CreateRepository_AlreadyExists_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "dup-repo")

	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "dup-repo",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	out := parseAccuracy(t, rec)
	assert.Equal(t, "RepositoryAlreadyExistsException", out["__type"])
}

// ── CompleteLayerUpload size tracking ────────────────────────────────────────

func TestAccuracy_LayerUpload_PartSize_Tracked(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "upload-size-repo")

	initRec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "upload-size-repo",
	})
	require.Equal(t, http.StatusOK, initRec.Code)
	initOut := parseAccuracy(t, initRec)
	uploadID, _ := initOut["uploadId"].(string)
	require.NotEmpty(t, uploadID)

	// Upload returns lastByteReceived.
	blob := make([]byte, 100)
	uploadRec := doAccuracy(t, h, "UploadLayerPart", map[string]any{
		"repositoryName": "upload-size-repo",
		"uploadId":       uploadID,
		"partFirstByte":  0,
		"partLastByte":   99,
		"layerPartBlob":  blob,
	})
	require.Equal(t, http.StatusOK, uploadRec.Code)
	uploadOut := parseAccuracy(t, uploadRec)
	assert.InDelta(t, float64(99), uploadOut["lastByteReceived"], 0,
		"lastByteReceived must equal partLastByte")
}
