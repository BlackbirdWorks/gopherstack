package ecr_test

// audit_ecr_test.go — Phase-B ECR audit tests.
//
// Covers three real bugs fixed in this audit:
//
//  1. DescribeImageScanFindings for unscanned images now returns ScanNotFoundException
//     instead of a synthetic COMPLETE/no-findings result. AWS returns this error when
//     StartImageScan has not yet been called for the image.
//
//  2. Lifecycle policy evaluator now uses digestTagsIndex (all tags per image) instead
//     of only img.ImageID.ImageTag (the primary tag). An image with multiple tags where
//     the primary tag was cleared is no longer incorrectly treated as "untagged".
//
//  3. Immutability exclusion filters (stored by PutImageTagMutability) are now enforced
//     in PutImage: tags matching an exclusion filter bypass the IMMUTABLE check.
//
// Also covers: layer upload flow, manifest round-trip, BatchCheckLayerAvailability,
// BatchDeleteImage (tag+digest), ListImages/DescribeImages filters, GetAuthorizationToken,
// lifecycle preview (count+age rules), repository/registry policies.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newBackend(t *testing.T) *ecr.InMemoryBackend {
	t.Helper()

	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:9999")
}

func makeRepo(t *testing.T, b *ecr.InMemoryBackend, name string) *ecr.Repository {
	t.Helper()

	repo, err := b.CreateRepository(context.Background(), name, "", false, "", "")
	require.NoError(t, err)

	return repo
}

func makeImage(digest, tag string) ecr.Image {
	return ecr.Image{
		ImageDigest:   digest,
		ImageManifest: fmt.Sprintf(`{"schemaVersion":2,"digest":"%s"}`, digest),
		ImageID:       ecr.ImageIdentifier{ImageDigest: digest, ImageTag: tag},
	}
}

// ---------------------------------------------------------------------------
// 1. DescribeImageScanFindings — ScanNotFoundException for unscanned images
// ---------------------------------------------------------------------------

func TestAuditECR_ScanNotFoundException_UnscannedImage(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name      string
		scanFirst bool
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "unscanned_returns_ScanNotFoundException",
			scanFirst: false,
			wantErr:   true,
			wantErrIs: ecr.ErrScanNotFoundException,
		},
		{
			name:      "scanned_returns_findings",
			scanFirst: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("my-repo")

			digest := "sha256:aabbcc"
			img := makeImage(digest, "v1.0")
			b.AddImageInternal("my-repo", img)

			if tt.scanFirst {
				_, err := b.StartImageScan(context.Background(), "my-repo",
					ecr.ImageIdentifier{ImageDigest: digest})
				require.NoError(t, err)
			}

			_, err := b.DescribeImageScanFindings(context.Background(), "my-repo",
				ecr.ImageIdentifier{ImageDigest: digest})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAuditECR_StartImageScan_ThenDescribeFindings(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("scan-repo")

	digest := "sha256:deadbeef"
	b.AddImageInternal("scan-repo", makeImage(digest, "latest"))

	result, err := b.StartImageScan(context.Background(), "scan-repo",
		ecr.ImageIdentifier{ImageDigest: digest})
	require.NoError(t, err)
	assert.Equal(t, "scan-repo", result.RepositoryName)

	findings, err := b.DescribeImageScanFindings(context.Background(), "scan-repo",
		ecr.ImageIdentifier{ImageDigest: digest})
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", findings.Status)
	assert.Equal(t, digest, findings.ImageID.ImageDigest)
}

// ---------------------------------------------------------------------------
// 2. Lifecycle evaluator multi-tag fix
// ---------------------------------------------------------------------------

func TestAuditECR_LifecycleEvaluator_MultiTagUntaggedDetection(t *testing.T) {
	t.Parallel()

	// An image with two tags where the primary tag is cleared but a tag still
	// exists in digestTagsIndex must NOT be classified as "untagged" by the
	// lifecycle evaluator.
	b := newBackend(t)
	b.CreateRepoInternal("lc-repo")

	digest := "sha256:1234"
	b.AddImageInternal("lc-repo", makeImage(digest, "v1"))
	// Simulate a second tag by calling PutImage directly (which updates digestTagsIndex).
	img2 := makeImage(digest, "stable")
	_, err := b.PutImage(context.Background(), "lc-repo", img2)
	require.NoError(t, err)

	// A lifecycle rule targeting "untagged" images must NOT expire this image.
	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-repo", policy)
	require.NoError(t, err)

	for _, id := range preview.PreviewResults {
		assert.NotEqual(t, digest, id.ImageDigest,
			"multi-tagged image must not be classified as untagged")
	}
}

func TestAuditECR_LifecycleEvaluator_UntaggedOnly(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-repo2")

	digestTagged := "sha256:tagged1"
	digestUntagged := "sha256:untagged1"
	b.AddImageInternal("lc-repo2", makeImage(digestTagged, "release"))
	b.AddImageInternal("lc-repo2", ecr.Image{
		ImageDigest:   digestUntagged,
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: digestUntagged},
	})

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-repo2", policy)
	require.NoError(t, err)

	expired := make(map[string]bool)
	for _, id := range preview.PreviewResults {
		expired[id.ImageDigest] = true
	}

	assert.True(t, expired[digestUntagged], "untagged image must be expired")
	assert.False(t, expired[digestTagged], "tagged image must NOT be expired")
}

func TestAuditECR_LifecycleEvaluator_ImageCountMoreThan(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-count-repo")

	now := time.Now()
	for i := range 5 {
		digest := fmt.Sprintf("sha256:img%02d", i)
		img := ecr.Image{
			ImageDigest:   digest,
			ImageManifest: `{"schemaVersion":2}`,
			ImageID:       ecr.ImageIdentifier{ImageDigest: digest, ImageTag: fmt.Sprintf("v%d", i)},
			ImagePushedAt: now.Add(time.Duration(i) * time.Hour),
		}
		b.AddImageInternal("lc-count-repo", img)
	}

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":3}}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-count-repo", policy)
	require.NoError(t, err)

	assert.Len(t, preview.PreviewResults, 2, "imageCountMoreThan:3 with 5 images must expire 2")
}

// ---------------------------------------------------------------------------
// 3. Immutability exclusion filters
// ---------------------------------------------------------------------------

func TestAuditECR_ImmutableExclusionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		tag            string
		filterPattern  string
		filterType     string
		wantErrOnRetag bool
	}{
		{
			name:           "excluded_tag_can_be_retagged",
			tag:            "latest",
			filterPattern:  "latest",
			filterType:     "WILDCARD",
			wantErrOnRetag: false,
		},
		{
			name:           "excluded_wildcard_prefix_matches",
			tag:            "dev-build",
			filterPattern:  "dev-*",
			filterType:     "WILDCARD",
			wantErrOnRetag: false,
		},
		{
			name:           "non_excluded_tag_rejected",
			tag:            "v1.0.0",
			filterPattern:  "latest",
			filterType:     "WILDCARD",
			wantErrOnRetag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("immutable-repo")

			_, err := b.PutImageTagMutability(context.Background(), "immutable-repo",
				"IMMUTABLE",
				[]ecr.ImageTagMutabilityExclusionFilter{
					{Filter: tt.filterPattern, FilterType: tt.filterType},
				},
			)
			require.NoError(t, err)

			img1 := ecr.Image{
				ImageDigest:   "sha256:digest1",
				ImageManifest: `{"v":1}`,
				ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:digest1", ImageTag: tt.tag},
			}
			_, err = b.PutImage(context.Background(), "immutable-repo", img1)
			require.NoError(t, err)

			img2 := ecr.Image{
				ImageDigest:   "sha256:digest2",
				ImageManifest: `{"v":2}`,
				ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:digest2", ImageTag: tt.tag},
			}
			_, err = b.PutImage(context.Background(), "immutable-repo", img2)

			if tt.wantErrOnRetag {
				assert.ErrorIs(t, err, ecr.ErrImageTagAlreadyExists,
					"non-excluded tag must be rejected in IMMUTABLE repo")
			} else {
				assert.NoError(t, err, "excluded tag must bypass IMMUTABLE check")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Layer upload flow
// ---------------------------------------------------------------------------

func TestAuditECR_LayerUploadFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name: "valid_layer_completes",
			data: []byte("fake-layer-data"),
		},
		{
			name:    "empty_upload_no_error",
			data:    []byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			makeRepo(t, b, "layer-repo")

			init, err := b.InitiateLayerUpload(context.Background(), "layer-repo")
			require.NoError(t, err)
			assert.NotEmpty(t, init.UploadID)

			if len(tt.data) > 0 {
				_, err = b.UploadLayerPart(context.Background(), "layer-repo", init.UploadID,
					0, int64(len(tt.data))-1, tt.data)
				require.NoError(t, err)
			}

			result, err := b.CompleteLayerUpload(context.Background(), "layer-repo", init.UploadID, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				// Only non-empty uploads produce a digest.
				if len(tt.data) > 0 {
					assert.NotEmpty(t, result.LayerDigest)
				}
			}
		})
	}
}

func TestAuditECR_BatchCheckLayerAvailability(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	makeRepo(t, b, "bchk-repo")

	init, err := b.InitiateLayerUpload(context.Background(), "bchk-repo")
	require.NoError(t, err)
	_, err = b.UploadLayerPart(context.Background(), "bchk-repo", init.UploadID, 0, 9, []byte("layer-data"))
	require.NoError(t, err)
	result, err := b.CompleteLayerUpload(context.Background(), "bchk-repo", init.UploadID, nil)
	require.NoError(t, err)
	digest := result.LayerDigest

	avail, failures, err := b.BatchCheckLayerAvailability(context.Background(), "bchk-repo",
		[]string{digest, "sha256:nonexistent"})
	require.NoError(t, err)

	assert.Len(t, avail, 1, "one known layer must be AVAILABLE")
	assert.Equal(t, "AVAILABLE", avail[0].LayerAvailability)
	assert.Equal(t, digest, avail[0].LayerDigest)

	assert.Len(t, failures, 1, "unknown layer must produce a failure")
	assert.Equal(t, "sha256:nonexistent", failures[0].LayerDigest)
}

// ---------------------------------------------------------------------------
// 5. PutImage manifest round-trip
// ---------------------------------------------------------------------------

func TestAuditECR_PutImage_ManifestRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	makeRepo(t, b, "manifest-repo")

	manifest := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":7023,"digest":"sha256:config"},"layers":[]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	img := ecr.Image{
		ImageManifest:          manifest,
		ImageManifestMediaType: "application/vnd.docker.distribution.manifest.v2+json",
		ImageID:                ecr.ImageIdentifier{ImageTag: "v1.0"},
	}

	pushed, err := b.PutImage(context.Background(), "manifest-repo", img)
	require.NoError(t, err)
	assert.NotEmpty(t, pushed.ImageDigest)

	results, failures, err := b.BatchGetImage(context.Background(), "manifest-repo",
		[]ecr.ImageIdentifier{{ImageTag: "v1.0"}})
	require.NoError(t, err)
	assert.Empty(t, failures)
	require.Len(t, results, 1)
	assert.Equal(t, manifest, results[0].ImageManifest)
	assert.Equal(t, pushed.ImageDigest, results[0].ImageDigest)
}

// ---------------------------------------------------------------------------
// 6. Tag mutability enforcement table
// ---------------------------------------------------------------------------

func TestAuditECR_TagMutability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mutability   string
		digest1      string
		digest2      string
		tag          string
		wantRetagErr bool
	}{
		{
			name:         "mutable_allows_retag",
			mutability:   "MUTABLE",
			digest1:      "sha256:aaa",
			digest2:      "sha256:bbb",
			tag:          "latest",
			wantRetagErr: false,
		},
		{
			name:         "immutable_rejects_retag",
			mutability:   "IMMUTABLE",
			digest1:      "sha256:ccc",
			digest2:      "sha256:ddd",
			tag:          "v1",
			wantRetagErr: true,
		},
		{
			name:         "immutable_allows_same_digest",
			mutability:   "IMMUTABLE",
			digest1:      "sha256:eee",
			digest2:      "sha256:eee",
			tag:          "stable",
			wantRetagErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("mut-repo")
			_, err := b.PutImageTagMutability(context.Background(), "mut-repo", tt.mutability, nil)
			require.NoError(t, err)

			img1 := ecr.Image{
				ImageDigest:   tt.digest1,
				ImageManifest: `{"v":1}`,
				ImageID:       ecr.ImageIdentifier{ImageDigest: tt.digest1, ImageTag: tt.tag},
			}
			_, err = b.PutImage(context.Background(), "mut-repo", img1)
			require.NoError(t, err)

			img2 := ecr.Image{
				ImageDigest:   tt.digest2,
				ImageManifest: `{"v":2}`,
				ImageID:       ecr.ImageIdentifier{ImageDigest: tt.digest2, ImageTag: tt.tag},
			}
			_, err = b.PutImage(context.Background(), "mut-repo", img2)

			if tt.wantRetagErr {
				assert.ErrorIs(t, err, ecr.ErrImageTagAlreadyExists)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7. BatchDeleteImage — tag-based and digest-based
// ---------------------------------------------------------------------------

func TestAuditECR_BatchDeleteImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deleteByTag  bool
		wantImgCount int
	}{
		{
			name:         "delete_by_tag_leaves_untagged_image",
			deleteByTag:  true,
			wantImgCount: 1, // image still exists, just untagged
		},
		{
			name:         "delete_by_digest_removes_image",
			deleteByTag:  false,
			wantImgCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("del-repo")

			digest := "sha256:deldel"
			b.AddImageInternal("del-repo", makeImage(digest, "del-tag"))

			var ids []ecr.ImageIdentifier
			if tt.deleteByTag {
				ids = []ecr.ImageIdentifier{{ImageTag: "del-tag"}}
			} else {
				ids = []ecr.ImageIdentifier{{ImageDigest: digest}}
			}

			deleted, failed, err := b.BatchDeleteImage(context.Background(), "del-repo", ids)
			require.NoError(t, err)
			assert.Empty(t, failed)
			assert.Len(t, deleted, 1)

			assert.Equal(t, tt.wantImgCount, b.ImageCount())
		})
	}
}

// ---------------------------------------------------------------------------
// 8. ListImages filters
// ---------------------------------------------------------------------------

func TestAuditECR_ListImages_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
		wantLen   int
	}{
		{name: "tagged_only", tagStatus: "TAGGED", wantLen: 1},
		{name: "untagged_only", tagStatus: "UNTAGGED", wantLen: 1},
		{name: "all", tagStatus: "", wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("list-repo")
			b.AddImageInternal("list-repo", makeImage("sha256:tagged", "v1"))
			b.AddImageInternal("list-repo", ecr.Image{
				ImageDigest:   "sha256:untagged",
				ImageManifest: `{"schemaVersion":2}`,
				ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:untagged"},
			})

			ids, err := b.ListImages(context.Background(), "list-repo", tt.tagStatus)
			require.NoError(t, err)
			assert.Len(t, ids, tt.wantLen)
		})
	}
}

func TestAuditECR_DescribeImages_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("desc-repo")
	b.AddImageInternal("desc-repo", makeImage("sha256:desc1", "v1"))
	b.AddImageInternal("desc-repo", ecr.Image{
		ImageDigest:   "sha256:desc2",
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:desc2"},
	})

	imgs, err := b.DescribeImages(context.Background(), "desc-repo", nil)
	require.NoError(t, err)
	assert.Len(t, imgs, 2, "DescribeImages with no filter must return all images")

	// Lookup by specific ID.
	imgs2, err := b.DescribeImages(context.Background(), "desc-repo",
		[]ecr.ImageIdentifier{{ImageDigest: "sha256:desc1"}})
	require.NoError(t, err)
	assert.Len(t, imgs2, 1)
	assert.Equal(t, "sha256:desc1", imgs2[0].ImageDigest)
}

// ---------------------------------------------------------------------------
// 9. GetAuthorizationToken — via HTTP handler
// ---------------------------------------------------------------------------

func TestAuditECR_GetAuthorizationToken(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name       string
		body       map[string]any
		wantTokens int
	}{
		{
			name:       "no_registry_ids",
			body:       map[string]any{},
			wantTokens: 1,
		},
		{
			name:       "multiple_registry_ids",
			body:       map[string]any{"registryIds": []string{"111", "222"}},
			wantTokens: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECRRequest(t, h, "GetAuthorizationToken", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			authData, authOK := resp["authorizationData"].([]any)
			require.True(t, authOK)
			assert.Len(t, authData, tt.wantTokens)

			for _, raw := range authData {
				entry, entryOK := raw.(map[string]any)
				require.True(t, entryOK)
				tokenRaw, tokenOK := entry["authorizationToken"].(string)
				require.True(t, tokenOK)
				require.NotEmpty(t, tokenRaw)

				decoded, decErr := base64.StdEncoding.DecodeString(tokenRaw)
				require.NoError(t, decErr, "token must be valid base64")
				parts := strings.SplitN(string(decoded), ":", 2)
				require.Len(t, parts, 2, "decoded token must be user:password")
				assert.NotEmpty(t, parts[1], "password must not be empty")
				assert.NotZero(t, entry["expiresAt"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 10. Lifecycle policy preview — sinceImagePushed
// ---------------------------------------------------------------------------

func TestAuditECR_LifecyclePolicyPreview_SinceImagePushed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("age-repo")

	now := time.Now()
	old := ecr.Image{
		ImageDigest:   "sha256:old",
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:old", ImageTag: "old"},
		ImagePushedAt: now.AddDate(0, 0, -10),
	}
	fresh := ecr.Image{
		ImageDigest:   "sha256:fresh",
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:fresh", ImageTag: "fresh"},
		ImagePushedAt: now.AddDate(0, 0, -1),
	}
	b.AddImageInternal("age-repo", old)
	b.AddImageInternal("age-repo", fresh)

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":{"tagStatus":"any","countType":"sinceImagePushed","countNumber":5,"countUnit":"days"}}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "age-repo", policy)
	require.NoError(t, err)

	expired := make(map[string]bool)
	for _, id := range preview.PreviewResults {
		expired[id.ImageDigest] = true
	}

	assert.True(t, expired["sha256:old"], "image older than 5 days must be expired")
	assert.False(t, expired["sha256:fresh"], "image newer than 5 days must not be expired")
}

// ---------------------------------------------------------------------------
// 11. Repository policy CRUD
// ---------------------------------------------------------------------------

func TestAuditECR_RepositoryPolicy_CRUD(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 12. Registry policy CRUD
// ---------------------------------------------------------------------------

func TestAuditECR_RegistryPolicy_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":["ecr:CreateRepository","ecr:ReplicateImage"],"Resource":"*"}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	out, err := b.PutRegistryPolicy(context.Background(), policy)
	require.NoError(t, err)
	assert.Equal(t, policy, out.PolicyText)

	got, err := b.GetRegistryPolicy(context.Background())
	require.NoError(t, err)
	assert.Equal(t, policy, got.PolicyText)

	_, err = b.DeleteRegistryPolicy(context.Background())
	require.NoError(t, err)

	_, err = b.GetRegistryPolicy(context.Background())
	assert.ErrorIs(t, err, ecr.ErrRegistryPolicyNotFound)
}

// ---------------------------------------------------------------------------
// 13. Repository CRUD
// ---------------------------------------------------------------------------

func TestAuditECR_RepositoryCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		repoName       string
		encryptionType string
		kmsKey         string
	}{
		{
			name:           "aes256_default",
			repoName:       "repo-aes",
			encryptionType: "AES256",
		},
		{
			name:           "kms_encryption",
			repoName:       "repo-kms",
			encryptionType: "KMS",
			kmsKey:         "arn:aws:kms:us-east-1:123456789012:key/abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			repo, err := b.CreateRepository(context.Background(), tt.repoName, "", false, tt.encryptionType, tt.kmsKey)
			require.NoError(t, err)
			assert.Equal(t, tt.repoName, repo.RepositoryName)
			assert.NotEmpty(t, repo.RepositoryARN)

			repos, err := b.DescribeRepositories(context.Background(), nil)
			require.NoError(t, err)
			found := false
			for _, r := range repos {
				if r.RepositoryName == tt.repoName {
					found = true
					assert.Equal(t, tt.encryptionType, r.EncryptionType)
					if tt.kmsKey != "" {
						assert.Equal(t, tt.kmsKey, r.KMSKey)
					}
				}
			}
			assert.True(t, found, "created repo must appear in DescribeRepositories")

			_, err = b.DeleteRepository(context.Background(), tt.repoName)
			require.NoError(t, err)
			assert.Equal(t, 0, b.RepositoryCount())
		})
	}
}

// ---------------------------------------------------------------------------
// 14. DeleteRepository non-empty guard (via HTTP handler)
// ---------------------------------------------------------------------------

func TestAuditECR_DeleteRepository_NonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		force    bool
		wantCode int
	}{
		{name: "no_force_rejected", force: false, wantCode: http.StatusBadRequest},
		{name: "force_succeeds", force: true, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create repo.
			rec := doECRRequest(t, h, "CreateRepository",
				map[string]any{"repositoryName": "nonempty-repo"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Push an image so the repo is non-empty.
			rec = doECRRequest(t, h, "PutImage", map[string]any{
				"repositoryName": "nonempty-repo",
				"imageManifest":  `{"schemaVersion":2}`,
				"imageTag":       "v1",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Attempt delete.
			rec = doECRRequest(t, h, "DeleteRepository", map[string]any{
				"repositoryName": "nonempty-repo",
				"force":          tt.force,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "RepositoryNotEmptyException")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 15. ScanNotFoundException via HTTP handler
// ---------------------------------------------------------------------------

func TestAuditECR_ScanNotFoundException_HTTPHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-http-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Push an image so the repo is non-empty — the scan check only fires on existing images.
	rec = doECRRequest(t, h, "PutImage", map[string]any{
		"repositoryName": "scan-http-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "v1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	image := putResp["image"].(map[string]any)
	imageID := image["imageId"].(map[string]any)
	digest := imageID["imageDigest"].(string)

	// DescribeImageScanFindings without calling StartImageScan first must return ScanNotFoundException.
	rec = doECRRequest(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-http-repo",
		"imageId":        map[string]string{"imageDigest": digest},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ScanNotFoundException")
}

// ---------------------------------------------------------------------------
// 16. Lifecycle policy CRUD
// ---------------------------------------------------------------------------

func TestAuditECR_LifecyclePolicy_CRUD(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 17. Tagging resources
// ---------------------------------------------------------------------------

func TestAuditECR_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	repo, err := b.CreateRepository(context.Background(), "tagged-repo", "", false, "", "")
	require.NoError(t, err)

	// Add initial tags via TagResource.
	err = b.TagResource(context.Background(), repo.RepositoryARN, map[string]string{"env": "prod", "team": "platform"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	err = b.TagResource(context.Background(), repo.RepositoryARN, map[string]string{"owner": "alice"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])

	err = b.UntagResource(context.Background(), repo.RepositoryARN, []string{"env"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "untagged key must be absent")
	assert.Equal(t, "platform", tags["team"])
}

// ---------------------------------------------------------------------------
// 18. DescribeImages ImagePushedAt is set on PutImage
// ---------------------------------------------------------------------------

func TestAuditECR_PutImage_PushedAtSet(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	makeRepo(t, b, "pushedat-repo")

	before := time.Now().Add(-time.Second)
	_, err := b.PutImage(context.Background(), "pushedat-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	imgs, err := b.DescribeImages(context.Background(), "pushedat-repo", nil)
	require.NoError(t, err)
	require.Len(t, imgs, 1)
	assert.True(t, imgs[0].ImagePushedAt.After(before),
		"ImagePushedAt must be set to current time on push")
}

// ---------------------------------------------------------------------------
// 19. Replication configuration CRUD
// ---------------------------------------------------------------------------

func TestAuditECR_ReplicationConfiguration(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	cfg := ecr.ReplicationConfig{
		Rules: []ecr.ReplicationRule{
			{
				Destinations: []ecr.ReplicationDestination{
					{Region: "eu-west-1", RegistryID: "111122223333"},
				},
			},
		},
	}

	out, err := b.PutReplicationConfiguration(context.Background(), &cfg)
	require.NoError(t, err)
	assert.Len(t, out.Rules, 1)

	reg, err := b.DescribeRegistry(context.Background())
	require.NoError(t, err)
	require.NotNil(t, reg.ReplicationConfiguration)
	require.Len(t, reg.ReplicationConfiguration.Rules, 1)
	assert.Equal(t, "eu-west-1", reg.ReplicationConfiguration.Rules[0].Destinations[0].Region)
}
