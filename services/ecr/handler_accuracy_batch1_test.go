package ecr_test

// handler_accuracy_batch1_test.go — ECR AWS-accuracy audit batch-1 (go-htk2)
//
// Covers: multi-tag images, tag-removal on retag, untagged images, ListImages filter,
// BatchDeleteImage tag-vs-digest semantics, BatchGetImage, DescribeImages multi-tag +
// pagination, ImageScanning (BASIC/ENHANCED), ReplicationConfiguration,
// RegistryScanningConfiguration, PullThroughCacheRule upstream registries, multi-arch
// manifest list, RepositoryPolicy, LifecyclePolicy edge cases, tags on resources,
// CreateRepository options, identity/ARN fields.

import (
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// ── helpers shared by batch-1 tests ─────────────────────────────────────────

func newBatch1Handler() *ecr.Handler {
	return ecr.NewHandler(ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000"), nil)
}

func newBatch1Backend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

func mustPutManifest(t *testing.T, h *ecr.Handler, repo, tag, manifest string) string {
	t.Helper()

	return mustPutImage(t, h, repo, tag, manifest)
}

// ── §1 Digest computed from manifest only (not tag) ──────────────────────────

func TestBatch1_PutImage_DigestFromManifestOnly(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("digest-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	img1, err := b.PutImage("digest-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"content":"same"}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	img2, err := b.PutImage("digest-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"content":"same"}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v2"},
	})
	require.NoError(t, err)

	assert.Equal(t, img1.ImageDigest, img2.ImageDigest,
		"same manifest content must produce the same digest regardless of tag")
}

func TestBatch1_PutImage_DifferentManifest_DifferentDigest(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("diff-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	img1, err := b.PutImage("diff-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":1}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	img2, err := b.PutImage("diff-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	assert.NotEqual(t, img1.ImageDigest, img2.ImageDigest,
		"different manifest content must produce different digests")
}

// ── §2 Multi-tag images ───────────────────────────────────────────────────────

func TestBatch1_MultiTag_SameManifestTwoTags_SharedDigest(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "multi-tag-repo")

	manifest := `{"schemaVersion":2,"multi":"tag"}`
	d1 := mustPutManifest(t, h, "multi-tag-repo", "v1.0", manifest)
	d2 := mustPutManifest(t, h, "multi-tag-repo", "v1.0.0", manifest)

	assert.Equal(t, d1, d2,
		"same manifest pushed with different tags must yield the same digest")
}

func TestBatch1_MultiTag_DescribeImages_ShowsBothTags(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-multi")

	manifest := `{"schemaVersion":2,"shared":true}`
	mustPutManifest(t, h, "desc-multi", "stable", manifest)
	mustPutManifest(t, h, "desc-multi", "latest", manifest)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-multi",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 1, "same manifest with two tags must appear as one image entry")

	detail := details[0].(map[string]any)
	imageTags, _ := detail["imageTags"].([]any)
	require.Len(t, imageTags, 2, "DescribeImages must list all tags for a multi-tag image")

	tags := make([]string, 0, 2)
	for _, t2 := range imageTags {
		tags = append(tags, t2.(string))
	}
	sort.Strings(tags)
	assert.Equal(t, []string{"latest", "stable"}, tags)
}

func TestBatch1_MultiTag_ListImages_ShowsBothTagEntries(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "list-multi")

	manifest := `{"schemaVersion":2,"shared":true}`
	mustPutManifest(t, h, "list-multi", "alpha", manifest)
	mustPutManifest(t, h, "list-multi", "beta", manifest)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "list-multi",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2,
		"ListImages must return one entry per tag for a multi-tag image")
}

func TestBatch1_MultiTag_Backend_TagIndexPopulated(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("idx-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	manifest := `{"schemaVersion":2}`
	_, err = b.PutImage("idx-repo", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "t1"},
	})
	require.NoError(t, err)

	_, err = b.PutImage("idx-repo", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "t2"},
	})
	require.NoError(t, err)

	assert.Equal(t, 2, b.RepoTagCount("idx-repo"),
		"tagIndex must contain both tags after two pushes with same manifest")

	d1 := b.TagDigest("idx-repo", "t1")
	d2 := b.TagDigest("idx-repo", "t2")
	assert.Equal(t, d1, d2, "both tags must resolve to the same digest")
}

func TestBatch1_MultiTag_ThreeTags_OneDigest(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("three-tag", "MUTABLE", false, "", "")
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"layers":[]}`
	for _, tag := range []string{"v1", "v1.0", "v1.0.0"} {
		_, err = b.PutImage("three-tag", ecr.Image{
			ImageManifest: manifest,
			ImageID:       ecr.ImageIdentifier{ImageTag: tag},
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 3, b.RepoTagCount("three-tag"), "three tags for one digest")
	assert.Equal(t, 1, b.ImageCount(), "one image entry regardless of tag count")
}

// ── §3 Tag removal on retag (MUTABLE) ────────────────────────────────────────

func TestBatch1_Retag_MUTABLE_OldImageBecomesUntagged(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("retag-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("retag-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":1}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "latest"},
	})
	require.NoError(t, err)

	assert.Equal(t, 1, b.RepoTagCount("retag-repo"), "one tag before retag")

	// Push different content with same tag — tag should move.
	_, err = b.PutImage("retag-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "latest"},
	})
	require.NoError(t, err)

	assert.Equal(t, 1, b.RepoTagCount("retag-repo"),
		"after retag, still one tag entry (moved to new digest)")
	assert.Equal(t, 2, b.ImageCount(),
		"old image stays in storage as untagged after retag")
}

func TestBatch1_Retag_MUTABLE_NewTag_PointsToNewDigest(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("newtag-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	img1, err := b.PutImage("newtag-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":1}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "prod"},
	})
	require.NoError(t, err)

	img2, err := b.PutImage("newtag-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"v":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "prod"},
	})
	require.NoError(t, err)

	assert.NotEqual(t, img1.ImageDigest, img2.ImageDigest)
	assert.Equal(t, img2.ImageDigest, b.TagDigest("newtag-repo", "prod"),
		"tag must resolve to the newest pushed digest")
}

func TestBatch1_Retag_MUTABLE_Handler_Succeeds(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "retag-handler")

	mustPutManifest(t, h, "retag-handler", "rel", `{"schemaVersion":2,"v":1}`)

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "retag-handler",
		"imageManifest":  `{"schemaVersion":2,"v":2}`,
		"imageTag":       "rel",
	})
	require.Equal(t, http.StatusOK, rec.Code, "retag in MUTABLE repo must succeed")
}

// ── §4 Untagged images ────────────────────────────────────────────────────────

func TestBatch1_Untagged_Image_PushWithoutTag(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("untag-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("untag-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{},
	})
	require.NoError(t, err)

	assert.Equal(t, 0, b.RepoTagCount("untag-repo"), "untagged image has no tag entries")
	assert.Equal(t, 1, b.ImageCount(), "untagged image still stored")
}

func TestBatch1_Untagged_ListImages_IncludesUntaggedByDefault(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "untag-list")

	// Push one untagged, one tagged.
	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "untag-list",
		"imageManifest":  `{"schemaVersion":2,"untagged":true}`,
	})
	mustPutManifest(t, h, "untag-list", "v1", `{"schemaVersion":2,"tagged":true}`)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "untag-list",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2, "default ListImages includes both tagged and untagged")
}

// ── §5 ListImages tagStatus filter ───────────────────────────────────────────

func TestBatch1_ListImages_Filter_TaggedOnly(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "filter-tagged")

	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "filter-tagged",
		"imageManifest":  `{"schemaVersion":2,"u":true}`,
	})
	mustPutManifest(t, h, "filter-tagged", "v1", `{"schemaVersion":2,"t":true}`)
	mustPutManifest(t, h, "filter-tagged", "v2", `{"schemaVersion":2,"t2":true}`)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "filter-tagged",
		"filter":         map[string]any{"tagStatus": "TAGGED"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2, "TAGGED filter must return only tagged images")

	for _, item := range ids {
		id := item.(map[string]any)
		assert.NotEmpty(t, id["imageTag"], "each result must have an imageTag")
	}
}

func TestBatch1_ListImages_Filter_UntaggedOnly(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "filter-untagged")

	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "filter-untagged",
		"imageManifest":  `{"schemaVersion":2,"u":1}`,
	})
	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "filter-untagged",
		"imageManifest":  `{"schemaVersion":2,"u":2}`,
	})
	mustPutManifest(t, h, "filter-untagged", "tagged", `{"schemaVersion":2,"t":true}`)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "filter-untagged",
		"filter":         map[string]any{"tagStatus": "UNTAGGED"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2, "UNTAGGED filter must return only untagged images")

	for _, item := range ids {
		id := item.(map[string]any)
		assert.Empty(t, id["imageTag"], "untagged results must have empty imageTag")
	}
}

func TestBatch1_ListImages_Filter_Any_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "filter-any")

	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "filter-any",
		"imageManifest":  `{"schemaVersion":2,"u":true}`,
	})
	mustPutManifest(t, h, "filter-any", "v1", `{"schemaVersion":2,"t":true}`)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "filter-any",
		"filter":         map[string]any{"tagStatus": "ANY"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2, "ANY filter returns all images")
}

func TestBatch1_ListImages_Filter_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "nofilter-any")

	doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "nofilter-any",
		"imageManifest":  `{"schemaVersion":2,"u":true}`,
	})
	mustPutManifest(t, h, "nofilter-any", "v1", `{"schemaVersion":2,"t":true}`)

	rec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "nofilter-any",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	assert.Len(t, ids, 2, "no filter returns all images")
}

func TestBatch1_ListImages_Filter_Backend_TAGGED(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("be-tagged", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("be-tagged", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"u":true}`,
	})
	require.NoError(t, err)

	_, err = b.PutImage("be-tagged", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"t":true}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	ids, err := b.ListImages("be-tagged", "TAGGED")
	require.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, "v1", ids[0].ImageTag)
}

func TestBatch1_ListImages_Filter_Backend_UNTAGGED(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("be-untagged", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("be-untagged", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"u":true}`,
	})
	require.NoError(t, err)

	_, err = b.PutImage("be-untagged", ecr.Image{
		ImageManifest: `{"schemaVersion":2,"t":true}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	ids, err := b.ListImages("be-untagged", "UNTAGGED")
	require.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Empty(t, ids[0].ImageTag)
}

// ── §6 BatchDeleteImage tag-vs-digest semantics ───────────────────────────────

func TestBatch1_BatchDeleteImage_ByTag_RemovesTagOnly(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("del-by-tag", "MUTABLE", false, "", "")
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"shared":true}`
	_, err = b.PutImage("del-by-tag", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	_, err = b.PutImage("del-by-tag", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v2"},
	})
	require.NoError(t, err)

	// Delete by tag v1 only.
	deleted, failures, err := b.BatchDeleteImage("del-by-tag", []ecr.ImageIdentifier{
		{ImageTag: "v1"},
	})
	require.NoError(t, err)
	assert.Empty(t, failures)
	assert.Len(t, deleted, 1)

	// Image must still be accessible by digest and by v2 tag.
	assert.Equal(t, 1, b.RepoTagCount("del-by-tag"),
		"only v1 tag removed; v2 tag remains")
	assert.Equal(t, 1, b.ImageCount(), "image itself stays after tag-only delete")
}

func TestBatch1_BatchDeleteImage_ByDigest_RemovesAllTags(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("del-by-digest", "MUTABLE", false, "", "")
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"multi":true}`
	img1, err := b.PutImage("del-by-digest", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "alpha"},
	})
	require.NoError(t, err)

	_, err = b.PutImage("del-by-digest", ecr.Image{
		ImageManifest: manifest,
		ImageID:       ecr.ImageIdentifier{ImageTag: "beta"},
	})
	require.NoError(t, err)

	assert.Equal(t, 2, b.RepoTagCount("del-by-digest"))

	// Delete by digest — must remove both tag bindings.
	deleted, failures, err := b.BatchDeleteImage("del-by-digest", []ecr.ImageIdentifier{
		{ImageDigest: img1.ImageDigest},
	})
	require.NoError(t, err)
	assert.Empty(t, failures)
	assert.Len(t, deleted, 1)

	assert.Equal(t, 0, b.RepoTagCount("del-by-digest"),
		"digest delete must remove all associated tags from index")
	assert.Equal(t, 0, b.ImageCount(), "image itself deleted by digest")
}

func TestBatch1_BatchDeleteImage_Handler_ByTag(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "handler-del-tag")

	manifest := `{"schemaVersion":2,"shared":true}`
	d := mustPutManifest(t, h, "handler-del-tag", "v1", manifest)
	mustPutManifest(t, h, "handler-del-tag", "v2", manifest)

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "handler-del-tag",
		"imageIds":       []map[string]any{{"imageTag": "v1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	imageIDs, _ := out["imageIds"].([]any)
	assert.Len(t, imageIDs, 1)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, failures)

	// v2 tag and image should still exist.
	listRec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "handler-del-tag",
		"filter":         map[string]any{"tagStatus": "TAGGED"},
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	remaining, _ := listOut["imageIds"].([]any)
	assert.Len(t, remaining, 1, "v2 tag must remain after deleting only v1")

	// Access by digest should still work.
	bgiRec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "handler-del-tag",
		"imageIds":       []map[string]any{{"imageDigest": d}},
	})
	require.Equal(t, http.StatusOK, bgiRec.Code)
	bgiOut := parseAccuracy(t, bgiRec)
	images, _ := bgiOut["images"].([]any)
	assert.Len(t, images, 1, "image accessible by digest after tag delete")
}

func TestBatch1_BatchDeleteImage_Handler_ByDigest(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "handler-del-digest")

	manifest := `{"schemaVersion":2,"delboth":true}`
	d := mustPutManifest(t, h, "handler-del-digest", "tag1", manifest)
	mustPutManifest(t, h, "handler-del-digest", "tag2", manifest)

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "handler-del-digest",
		"imageIds":       []map[string]any{{"imageDigest": d}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, failures)

	// All images gone.
	listRec := doAccuracy(t, h, "ListImages", map[string]any{
		"repositoryName": "handler-del-digest",
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	ids, _ := listOut["imageIds"].([]any)
	assert.Empty(t, ids, "all images removed after digest delete")
}

func TestBatch1_BatchDeleteImage_NonExistentImage_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "del-missing")

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "del-missing",
		"imageIds":       []map[string]any{{"imageDigest": "sha256:doesnotexist"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	failures, _ := out["failures"].([]any)
	require.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	assert.Equal(t, "ImageNotFound", failure["failureCode"])
}

func TestBatch1_BatchDeleteImage_Mixed_Success_And_Failure(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "del-mixed")

	d := mustPutManifest(t, h, "del-mixed", "exists", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "del-mixed",
		"imageIds": []map[string]any{
			{"imageDigest": d},
			{"imageTag": "ghost"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	deleted, _ := out["imageIds"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Len(t, deleted, 1, "one successful delete")
	assert.Len(t, failures, 1, "one failure for non-existent tag")
}

// ── §7 BatchGetImage accuracy ─────────────────────────────────────────────────

func TestBatch1_BatchGetImage_ByTag_ReturnsImage(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "bgi-tag")
	mustPutManifest(t, h, "bgi-tag", "v1", `{"schemaVersion":2,"bgi":true}`)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-tag",
		"imageIds":       []map[string]any{{"imageTag": "v1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	require.Len(t, images, 1)

	img := images[0].(map[string]any)
	imageID, _ := img["imageId"].(map[string]any)
	assert.Equal(t, "v1", imageID["imageTag"])
	assert.NotEmpty(t, imageID["imageDigest"])
}

func TestBatch1_BatchGetImage_ByDigest_ReturnsManifest(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "bgi-digest")
	manifest := `{"schemaVersion":2,"bgi":"digest"}`
	d := mustPutManifest(t, h, "bgi-digest", "tag", manifest)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-digest",
		"imageIds":       []map[string]any{{"imageDigest": d}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	require.Len(t, images, 1)

	img := images[0].(map[string]any)
	assert.Equal(t, manifest, img["imageManifest"])
}

func TestBatch1_BatchGetImage_MultipleImages_AllReturned(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "bgi-multi")

	d1 := mustPutManifest(t, h, "bgi-multi", "v1", `{"schemaVersion":2,"v":1}`)
	d2 := mustPutManifest(t, h, "bgi-multi", "v2", `{"schemaVersion":2,"v":2}`)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-multi",
		"imageIds": []map[string]any{
			{"imageDigest": d1},
			{"imageDigest": d2},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	assert.Len(t, images, 2)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, failures)
}

func TestBatch1_BatchGetImage_MissingImage_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "bgi-missing")

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-missing",
		"imageIds":       []map[string]any{{"imageTag": "ghost"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, _ := out["images"].([]any)
	assert.Empty(t, images)
	failures, _ := out["failures"].([]any)
	require.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	assert.Equal(t, "ImageNotFound", failure["failureCode"])
}

func TestBatch1_BatchGetImage_MultiTag_ByEitherTag(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "bgi-multitag")

	manifest := `{"schemaVersion":2,"shared":true}`
	mustPutManifest(t, h, "bgi-multitag", "stable", manifest)
	mustPutManifest(t, h, "bgi-multitag", "latest", manifest)

	for _, tag := range []string{"stable", "latest"} {
		rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
			"repositoryName": "bgi-multitag",
			"imageIds":       []map[string]any{{"imageTag": tag}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		out := parseAccuracy(t, rec)
		images, _ := out["images"].([]any)
		require.Len(t, images, 1, "image accessible via tag %q", tag)

		img := images[0].(map[string]any)
		imgID, _ := img["imageId"].(map[string]any)
		assert.Equal(t, tag, imgID["imageTag"],
			"returned imageId.imageTag must match requested tag")
	}
}

// ── §8 DescribeImages pagination ──────────────────────────────────────────────

func TestBatch1_DescribeImages_MaxResults_Paginates(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-page")

	for i := range 5 {
		doAccuracy(t, h, "PutImage", map[string]any{
			"repositoryName": "desc-page",
			"imageManifest":  fmt.Sprintf(`{"schemaVersion":2,"n":%d}`, i),
		})
	}

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-page",
		"maxResults":     3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Len(t, details, 3, "maxResults=3 must return at most 3 image details")
	assert.NotEmpty(t, out["nextToken"],
		"nextToken must be present when more images remain")
}

func TestBatch1_DescribeImages_NextToken_ContinuesPagination(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-page2")

	for i := range 4 {
		doAccuracy(t, h, "PutImage", map[string]any{
			"repositoryName": "desc-page2",
			"imageManifest":  fmt.Sprintf(`{"schemaVersion":2,"n":%d}`, i),
		})
	}

	rec1 := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-page2",
		"maxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	out1 := parseAccuracy(t, rec1)
	page1, _ := out1["imageDetails"].([]any)
	require.Len(t, page1, 2)
	token, _ := out1["nextToken"].(string)
	require.NotEmpty(t, token)

	rec2 := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-page2",
		"nextToken":      token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	out2 := parseAccuracy(t, rec2)
	page2, _ := out2["imageDetails"].([]any)
	assert.Len(t, page2, 2, "second page must return remaining 2 images")
}

func TestBatch1_DescribeImages_FullPagination_CoversAll(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-fullpage")
	total := 7
	for i := range total {
		doAccuracy(t, h, "PutImage", map[string]any{
			"repositoryName": "desc-fullpage",
			"imageManifest":  fmt.Sprintf(`{"schemaVersion":2,"n":%d}`, i),
		})
	}

	seen := map[string]bool{}
	token := ""
	for {
		body := map[string]any{"repositoryName": "desc-fullpage", "maxResults": 3}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doAccuracy(t, h, "DescribeImages", body)
		require.Equal(t, http.StatusOK, rec.Code)
		out := parseAccuracy(t, rec)

		for _, d := range out["imageDetails"].([]any) {
			detail := d.(map[string]any)
			seen[detail["imageDigest"].(string)] = true
		}

		next, _ := out["nextToken"].(string)
		if next == "" {
			break
		}

		token = next
	}

	assert.Len(t, seen, total, "pagination must enumerate all %d images", total)
}

// ── §9 Multi-arch manifest list support ──────────────────────────────────────

func TestBatch1_ManifestList_OCI_MediaType_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "manifest-list")

	listManifest := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[` +
		`{"mediaType":"application/vnd.oci.image.manifest.v1+json",` +
		`"digest":"sha256:aaa","platform":{"os":"linux","architecture":"amd64"}},` +
		`{"mediaType":"application/vnd.oci.image.manifest.v1+json",` +
		`"digest":"sha256:bbb","platform":{"os":"linux","architecture":"arm64"}}` +
		`]}`

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName":         "manifest-list",
		"imageManifest":          listManifest,
		"imageTag":               "multi",
		"imageManifestMediaType": "application/vnd.oci.image.index.v1+json",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	d := mustPutManifest(t, h, "manifest-list", "multi2", listManifest)

	bgiRec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "manifest-list",
		"imageIds":       []map[string]any{{"imageDigest": d}},
	})
	require.Equal(t, http.StatusOK, bgiRec.Code)
	bgiOut := parseAccuracy(t, bgiRec)
	images, _ := bgiOut["images"].([]any)
	require.Len(t, images, 1)

	img := images[0].(map[string]any)
	assert.Equal(t, listManifest, img["imageManifest"],
		"manifest list content must round-trip via BatchGetImage")
}

func TestBatch1_ManifestList_Docker_MediaType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "docker-list")

	listManifest := `{"schemaVersion":2,` +
		`"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[]}`

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName":         "docker-list",
		"imageManifest":          listManifest,
		"imageTag":               "multi",
		"imageManifestMediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	img, _ := out["image"].(map[string]any)
	assert.Equal(t, "application/vnd.docker.distribution.manifest.list.v2+json",
		img["imageManifestMediaType"],
		"Docker manifest list mediaType must be preserved")
}

func TestBatch1_ManifestList_DescribeImages_ShowsMediaType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "oci-media")

	mediaType := "application/vnd.oci.image.index.v1+json"
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName":         "oci-media",
		"imageManifest":          `{"schemaVersion":2,"media":"oci"}`,
		"imageTag":               "idx",
		"imageManifestMediaType": mediaType,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "oci-media",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	descOut := parseAccuracy(t, descRec)
	details, _ := descOut["imageDetails"].([]any)
	require.Len(t, details, 1)
	detail := details[0].(map[string]any)
	assert.Equal(t, mediaType, detail["imageManifestMediaType"],
		"DescribeImages must preserve imageManifestMediaType")
}

// ── §10 ImageScanning BASIC and ENHANCED ─────────────────────────────────────

func TestBatch1_ImageScanning_BASIC_StartAndGet(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "scan-basic")
	d := mustPutManifest(t, h, "scan-basic", "v1", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-basic",
		"imageId":        map[string]any{"imageDigest": d},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)

	scanOut := parseAccuracy(t, scanRec)
	status, _ := scanOut["imageScanStatus"].(map[string]any)
	assert.NotEmpty(t, status["status"], "imageScanStatus.status must be set")

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-basic",
		"imageId":        map[string]any{"imageDigest": d},
	})
	require.Equal(t, http.StatusOK, findRec.Code)

	findOut := parseAccuracy(t, findRec)
	assert.NotEmpty(t, findOut["repositoryName"])
	assert.Equal(t, "scan-basic", findOut["repositoryName"])
}

func TestBatch1_ImageScanning_StartByScanByTag(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "scan-by-tag")
	mustPutManifest(t, h, "scan-by-tag", "prod", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-by-tag",
		"imageId":        map[string]any{"imageTag": "prod"},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-by-tag",
		"imageId":        map[string]any{"imageTag": "prod"},
	})
	require.Equal(t, http.StatusOK, findRec.Code)
}

func TestBatch1_RegistryScanningConfiguration_ENHANCED(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	putRec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules": []map[string]any{
			{
				"scanFrequency": "CONTINUOUS_SCAN",
				"repositoryFilters": []map[string]any{
					{"filter": "*", "filterType": "WILDCARD"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"],
		"ENHANCED scan type must be persisted and returned")
	rules, _ := cfg["rules"].([]any)
	assert.Len(t, rules, 1, "scanning rules must be persisted")
}

func TestBatch1_RegistryScanningConfiguration_BASIC_Default(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	getRec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "BASIC", cfg["scanType"],
		"default scan type must be BASIC")
}

func TestBatch1_PutImageScanningConfiguration_UpdatesRepo(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "scan-cfg")

	putRec := doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
		"repositoryName": "scan-cfg",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	out := parseAccuracy(t, putRec)
	cfg, _ := out["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"])
}

func TestBatch1_BatchGetRepositoryScanningConfiguration_MultipleRepos(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":             "scan-r1",
		"imageScanningConfiguration": map[string]any{"scanOnPush": true},
	})
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":             "scan-r2",
		"imageScanningConfiguration": map[string]any{"scanOnPush": false},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-r1", "scan-r2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 2)

	cfgMap := map[string]map[string]any{}
	for _, c := range configs {
		cfg := c.(map[string]any)
		cfgMap[cfg["repositoryName"].(string)] = cfg
	}

	assert.Equal(t, "SCAN_ON_PUSH", cfgMap["scan-r1"]["scanFrequency"])
	assert.Equal(t, "MANUAL", cfgMap["scan-r2"]["scanFrequency"])
}

func TestBatch1_BatchGetRepositoryScanningConfiguration_UnknownRepo_Failure(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "known-scan")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"known-scan", "unknown-scan"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Len(t, configs, 1, "known repo returns config")
	assert.Len(t, failures, 1, "unknown repo returns failure")
	failure := failures[0].(map[string]any)
	assert.Equal(t, "unknown-scan", failure["repositoryName"])
}

// ── §11 ReplicationConfiguration ─────────────────────────────────────────────

func TestBatch1_ReplicationConfiguration_CrossRegion(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "us-west-2", "registryId": "123456789012"},
						{"region": "eu-west-1", "registryId": "123456789012"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	destinations, _ := rule["destinations"].([]any)
	assert.Len(t, destinations, 2, "two cross-region destinations must be stored")
}

func TestBatch1_ReplicationConfiguration_CrossAccount(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "us-east-1", "registryId": "999999999999"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)

	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	destinations, _ := rule["destinations"].([]any)
	require.Len(t, destinations, 1)
	dest := destinations[0].(map[string]any)
	assert.Equal(t, "999999999999", dest["registryId"],
		"cross-account registryId must be preserved")
}

func TestBatch1_ReplicationConfiguration_WithRepositoryFilter(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "ap-southeast-1", "registryId": "123456789012"},
					},
					"repositoryFilters": []map[string]any{
						{"filter": "prod/*", "filterType": "PREFIX_MATCH"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)

	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	filters, _ := rule["repositoryFilters"].([]any)
	require.Len(t, filters, 1)
	filter := filters[0].(map[string]any)
	assert.Equal(t, "prod/*", filter["filter"])
	assert.Equal(t, "PREFIX_MATCH", filter["filterType"])
}

func TestBatch1_ReplicationConfiguration_Clear(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{"destinations": []map[string]any{{"region": "us-west-2", "registryId": "123456789012"}}},
			},
		},
	})

	// Clear by setting empty rules.
	clearRec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, clearRec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	assert.Empty(t, rules, "replication rules must be clearable")
}

// ── §12 PullThroughCacheRule upstream registries ──────────────────────────────

func TestBatch1_PullThroughCacheRule_ECR_Public(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "ecr-public",
		"upstreamRegistryUrl": "https://public.ecr.aws",
		"upstreamRegistry":    "ECR_PUBLIC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ecr-public", out["ecrRepositoryPrefix"])
	assert.Equal(t, "ECR_PUBLIC", out["upstreamRegistry"])
}

func TestBatch1_PullThroughCacheRule_Quay(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "quay",
		"upstreamRegistryUrl": "https://quay.io",
		"upstreamRegistry":    "QUAY",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "QUAY", out["upstreamRegistry"])
}

func TestBatch1_PullThroughCacheRule_DockerHub(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dockerhub",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
		"upstreamRegistry":    "DOCKER_HUB",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "DOCKER_HUB", out["upstreamRegistry"])
}

func TestBatch1_PullThroughCacheRule_K8S(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "k8s",
		"upstreamRegistryUrl": "https://registry.k8s.io",
		"upstreamRegistry":    "K8S",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "K8S", out["upstreamRegistry"])
}

func TestBatch1_PullThroughCacheRule_UpstreamRepositoryPrefix(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix":      "quay-prefix",
		"upstreamRegistryUrl":      "https://quay.io",
		"upstreamRegistry":         "QUAY",
		"upstreamRepositoryPrefix": "myorg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "myorg", out["upstreamRepositoryPrefix"])
}

func TestBatch1_PullThroughCacheRule_WithCredentials(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	credArn := "arn:aws:secretsmanager:us-east-1:123456789012:secret/ecr-cred"

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "private-hub",
		"upstreamRegistryUrl": "https://my-registry.example.com",
		"credentialArn":       credArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, credArn, out["credentialArn"])
}

func TestBatch1_PullThroughCacheRule_Update_Credential(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"upstreamRegistryUrl": "https://my-registry.example.com",
	})

	newCred := "arn:aws:secretsmanager:us-east-1:123456789012:secret/new-cred"
	updateRec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"credentialArn":       newCred,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	out := parseAccuracy(t, updateRec)
	assert.Equal(t, newCred, out["credentialArn"])
}

func TestBatch1_PullThroughCacheRule_Validate_Missing_Returns_Invalid(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "nonexistent",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, false, out["isValid"],
		"missing rule must validate as invalid")
	assert.NotEmpty(t, out["failure"], "failure message must be set for missing rule")
}

func TestBatch1_PullThroughCacheRule_Validate_Existing_Returns_Valid(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "valid-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "valid-rule",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, true, out["isValid"])
}

func TestBatch1_PullThroughCacheRule_DescribeByPrefix(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	for _, prefix := range []string{"r1", "r2", "r3"} {
		doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
			"ecrRepositoryPrefix": prefix,
			"upstreamRegistryUrl": "https://registry-1.docker.io",
		})
	}

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"r1", "r3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	rules, _ := out["pullThroughCacheRules"].([]any)
	require.Len(t, rules, 2, "filtering by prefix must return only requested rules")
}

func TestBatch1_PullThroughCacheRule_Duplicate_Returns400(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dup-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dup-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"duplicate rule must return 400")
	out := parseAccuracy(t, rec)
	assert.Equal(t, "PullThroughCacheRuleAlreadyExistsException", out["__type"])
}

// ── §13 RepositoryPolicy accuracy ─────────────────────────────────────────────

func TestBatch1_RepositoryPolicy_SetGet_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
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

func TestBatch1_RepositoryPolicy_Delete(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
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
	assert.Equal(t, http.StatusNotFound, getRec.Code,
		"GetRepositoryPolicy must 404 after policy deletion")
}

func TestBatch1_RepositoryPolicy_Get_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "GetRepositoryPolicy", map[string]any{
		"repositoryName": "ghost",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_RepositoryPolicy_Overwrite(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
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

// ── §14 LifecyclePolicy edge cases ────────────────────────────────────────────

func TestBatch1_LifecyclePolicy_Put_Delete_Gone(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
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

func TestBatch1_LifecyclePolicy_Preview_WithPolicy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "lc-preview")
	mustPutManifest(t, h, "lc-preview", "v1", `{"schemaVersion":2,"v":1}`)

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

func TestBatch1_LifecyclePolicy_Preview_UsesExistingPolicy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
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

func TestBatch1_LifecyclePolicy_Delete_NonExistent_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "lc-noexist")

	rec := doAccuracy(t, h, "DeleteLifecyclePolicy", map[string]any{
		"repositoryName": "lc-noexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── §15 CreateRepository options ──────────────────────────────────────────────

func TestBatch1_CreateRepository_ImageTagMutability_IMMUTABLE(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-create",
		"imageTagMutability": "IMMUTABLE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	assert.Equal(t, "IMMUTABLE", repo["imageTagMutability"])
}

func TestBatch1_CreateRepository_DefaultImageTagMutability_MUTABLE(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "default-mut",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	assert.Equal(t, "MUTABLE", repo["imageTagMutability"],
		"default imageTagMutability must be MUTABLE")
}

func TestBatch1_CreateRepository_ScanOnPush_True(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-on-push",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	scanCfg, _ := repo["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, scanCfg["scanOnPush"])
}

func TestBatch1_CreateRepository_NamespaceSlash(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "org/team/app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	assert.Equal(t, "org/team/app", repo["repositoryName"])
	arn, _ := repo["repositoryArn"].(string)
	assert.Contains(t, arn, "org/team/app", "ARN must contain full namespaced name")
	uri, _ := repo["repositoryUri"].(string)
	assert.Contains(t, uri, "org/team/app", "URI must contain full namespaced name")
}

func TestBatch1_CreateRepository_KMS_RoundTrip_Backend(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	kmsKey := "arn:aws:kms:us-east-1:123456789012:key/mrk-xyz"
	repo, err := b.CreateRepository("kms-be", "MUTABLE", false, "KMS", kmsKey)
	require.NoError(t, err)
	assert.Equal(t, "KMS", repo.EncryptionType)
	assert.Equal(t, kmsKey, repo.KMSKey)

	repos, err := b.DescribeRepositories([]string{"kms-be"})
	require.NoError(t, err)
	assert.Equal(t, kmsKey, repos[0].KMSKey,
		"KMS key must persist in DescribeRepositories")
}

func TestBatch1_CreateRepository_CreatedAt_IsPresent(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "created-at-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	createdAt, ok := repo["createdAt"].(float64)
	require.True(t, ok, "createdAt must be a numeric Unix timestamp")
	assert.Greater(t, createdAt, float64(0), "createdAt must be non-zero")
}

func TestBatch1_CreateRepository_Tags_Applied_On_Create(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tagged-on-create",
		"tags": []map[string]any{
			{"Key": "Project", "Value": "gopherstack"},
			{"Key": "Cost", "Value": "engineering"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	tags, _ := listOut["tags"].([]any)
	assert.Len(t, tags, 2)
}

// ── §16 Tag resource operations ───────────────────────────────────────────────

func TestBatch1_TagResource_And_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tag-ops",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	assert.Len(t, tags, 2)

	tagMap := map[string]string{}
	for _, item := range tags {
		tag := item.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}
	assert.Equal(t, "v1", tagMap["k1"])
	assert.Equal(t, "v2", tagMap["k2"])
}

func TestBatch1_UntagResource_RemovesKeyOnly(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "untag-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
			{"Key": "also-keep", "Value": "sure"},
		},
	})

	doAccuracy(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"remove"},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	tags, _ := parseAccuracy(t, listRec)["tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestBatch1_TagResource_Upsert(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tag-upsert",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]any{{"Key": "env", "Value": "staging"}},
	})
	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	tags, _ := parseAccuracy(t, listRec)["tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "prod", tag["Value"],
		"TagResource must upsert (overwrite existing key)")
}

func TestBatch1_ListTagsForResource_Empty_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "no-tags",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	tags, hasKey := out["tags"]
	if hasKey && tags != nil {
		tagList, _ := tags.([]any)
		assert.Empty(t, tagList)
	}
}

// ── §17 PutImageTagMutability accuracy ────────────────────────────────────────

func TestBatch1_PutImageTagMutability_MUTABLE_To_IMMUTABLE(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "mut-switch")

	rec := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "mut-switch",
		"imageTagMutability": "IMMUTABLE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "IMMUTABLE", out["imageTagMutability"])
	assert.Equal(t, "mut-switch", out["repositoryName"])
	assert.Equal(t, "123456789012", out["registryId"])
}

func TestBatch1_PutImageTagMutability_WithExclusionFilters(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "filter-repo")

	rec := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "filter-repo",
		"imageTagMutability": "IMMUTABLE",
		"imageTagMutabilityExclusionFilters": []map[string]any{
			{"filter": "dev-*", "filterType": "WILDCARD"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	filters, _ := out["imageTagMutabilityExclusionFilters"].([]any)
	require.Len(t, filters, 1)
	filter := filters[0].(map[string]any)
	assert.Equal(t, "dev-*", filter["filter"])
	assert.Equal(t, "WILDCARD", filter["filterType"])
}

func TestBatch1_PutImageTagMutability_NonExistent_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "ghost-repo",
		"imageTagMutability": "IMMUTABLE",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── §18 RegistryPolicy accuracy ───────────────────────────────────────────────

func TestBatch1_RegistryPolicy_PutGet_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	policy := `{"Version":"2012-10-17","Statement":[{"Sid":"AllowCrossAccount",` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},` +
		`"Action":"ecr:CreateRepository"}]}`

	doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": policy})

	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	assert.Equal(t, policy, out["policyText"])
	assert.Equal(t, "123456789012", out["registryId"])
}

func TestBatch1_RegistryPolicy_Delete_Then_Get_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	doAccuracy(t, h, "PutRegistryPolicy", map[string]any{
		"policyText": `{"Version":"2012-10-17","Statement":[]}`,
	})

	doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})

	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_RegistryPolicy_Get_NoPolicy_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"GetRegistryPolicy on fresh backend must return 404")
}

// ── §19 DescribeRegistry ──────────────────────────────────────────────────────

func TestBatch1_DescribeRegistry_AccountID(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("555555555555", "eu-west-1", "")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "555555555555", out["registryId"])
}

func TestBatch1_DescribeRegistry_InitialReplicationConfig_Empty(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	assert.Empty(t, rules, "fresh registry must have no replication rules")
}

// ── §20 Layer upload accuracy ─────────────────────────────────────────────────

func TestBatch1_InitiateLayerUpload_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "ghost",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_InitiateLayerUpload_ReturnsUploadIdAndPartSize(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "upload-meta")

	rec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "upload-meta",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.NotEmpty(t, out["uploadId"], "uploadId must be returned")
	partSize, _ := out["partSize"].(float64)
	assert.Greater(t, partSize, float64(0), "partSize must be positive")
}

func TestBatch1_CompleteLayerUpload_Makes_Layer_Available(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "complete-upload")

	initRec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "complete-upload",
	})
	require.Equal(t, http.StatusOK, initRec.Code)
	uploadID, _ := parseAccuracy(t, initRec)["uploadId"].(string)

	digest := "sha256:cafebabe"
	completeRec := doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "complete-upload",
		"uploadId":       uploadID,
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, completeRec.Code)

	checkRec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "complete-upload",
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, checkRec.Code)
	out := parseAccuracy(t, checkRec)
	layers, _ := out["layers"].([]any)
	require.Len(t, layers, 1)
	assert.Equal(t, "AVAILABLE", layers[0].(map[string]any)["layerAvailability"])
}

// ── §21 GetAuthorizationToken accuracy ───────────────────────────────────────

func TestBatch1_GetAuthorizationToken_Base64Format(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)
	token, _ := data[0].(map[string]any)["authorizationToken"].(string)
	assert.NotEmpty(t, token)

	// Token must be valid base64.
	import64 := func(s string) bool {
		import64 := true
		for _, c := range s {
			if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') &&
				(c < '0' || c > '9') && c != '+' && c != '/' && c != '=' {
				import64 = false
			}
		}

		return import64
	}
	assert.True(t, import64(token), "authorizationToken must be valid base64")
}

func TestBatch1_GetAuthorizationToken_ExpiresAt_IsFloat(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	expiresAt, ok := data[0].(map[string]any)["expiresAt"].(float64)
	assert.True(t, ok, "expiresAt must be a JSON number")
	assert.Greater(t, expiresAt, float64(0), "expiresAt must be positive")
}

// ── §22 Repository CRUD identity fields ─────────────────────────────────────

func TestBatch1_Repository_RegistryID_MatchesAccountID(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("111111111111", "us-west-2", "")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "reg-id-check",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	assert.Equal(t, "111111111111", repo["registryId"])
}

func TestBatch1_Repository_ARN_ContainsRegion(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("123456789012", "ap-northeast-1", "")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "arn-region",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	arn, _ := repo["repositoryArn"].(string)
	assert.Contains(t, arn, "ap-northeast-1", "ARN must contain the configured region")
}

func TestBatch1_Repository_URI_ContainsEndpoint(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "myregistry.local:5000")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "uri-endpoint",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repo, _ := out["repository"].(map[string]any)
	uri, _ := repo["repositoryUri"].(string)
	assert.Contains(t, uri, "myregistry.local:5000",
		"repositoryUri must use the configured endpoint")
	assert.Contains(t, uri, "uri-endpoint")
}

// ── §23 DescribeRepositories identity ────────────────────────────────────────

func TestBatch1_DescribeRepositories_Returns_EncryptionConfiguration(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "enc-check",
		"encryptionConfiguration": map[string]any{
			"encryptionType": "AES256",
		},
	})

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"enc-check"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 1)
	repo, _ := repos[0].(map[string]any)
	enc, ok := repo["encryptionConfiguration"].(map[string]any)
	require.True(t, ok, "encryptionConfiguration must be present in DescribeRepositories")
	assert.Equal(t, "AES256", enc["encryptionType"])
}

func TestBatch1_DescribeRepositories_EmptyRequest_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	for _, name := range []string{"repo-x", "repo-y", "repo-z"} {
		mustCreateRepo(t, h, name)
	}

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	assert.Len(t, repos, 3)
}

// ── §24 AccountSetting round-trip ────────────────────────────────────────────

func TestBatch1_AccountSetting_EnhancedScanVersion(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	for _, val := range []string{"AWS_NATIVE", "CLAIR"} {
		doAccuracy(t, h, "PutAccountSetting", map[string]any{
			"name":  "BASIC_SCAN_TYPE_VERSION",
			"value": val,
		})

		getRec := doAccuracy(t, h, "GetAccountSetting", map[string]any{
			"name": "BASIC_SCAN_TYPE_VERSION",
		})
		require.Equal(t, http.StatusOK, getRec.Code)
		out := parseAccuracy(t, getRec)
		assert.Equal(t, val, out["value"], "account setting must round-trip for value %q", val)
	}
}

func TestBatch1_AccountSetting_MultipleKeys_Independent(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "SETTING_A",
		"value": "val-a",
	})
	doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "SETTING_B",
		"value": "val-b",
	})

	for name, want := range map[string]string{"SETTING_A": "val-a", "SETTING_B": "val-b"} {
		rec := doAccuracy(t, h, "GetAccountSetting", map[string]any{"name": name})
		require.Equal(t, http.StatusOK, rec.Code)
		out := parseAccuracy(t, rec)
		assert.Equal(t, want, out["value"], "setting %q must be independent", name)
	}
}

// ── §25 PullTimeUpdateExclusion ───────────────────────────────────────────────

func TestBatch1_PullTimeUpdateExclusion_MultipleRoles(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	arns := []string{
		"arn:aws:iam::123456789012:role/Role1",
		"arn:aws:iam::123456789012:role/Role2",
	}

	for _, arn := range arns {
		rec := doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
			"principalArn": arn,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	exclusions, _ := out["pullTimeUpdateExclusions"].([]any)
	assert.Len(t, exclusions, 2)
}

func TestBatch1_PullTimeUpdateExclusion_Deregister_Gone(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	arn := "arn:aws:iam::123456789012:role/TempRole"

	doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{"principalArn": arn})

	deregRec := doAccuracy(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, deregRec.Code)

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	exclusions, _ := out["pullTimeUpdateExclusions"].([]any)
	assert.Empty(t, exclusions)
}

// ── §26 RepositoryCreationTemplate ───────────────────────────────────────────

func TestBatch1_RepositoryCreationTemplate_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	createRec := doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":             "prod/",
		"description":        "Production repos",
		"imageTagMutability": "IMMUTABLE",
		"appliedFor":         []string{"REPLICATION", "PULL_THROUGH_CACHE"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	descRec := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	out := parseAccuracy(t, descRec)
	tmpls, _ := out["repositoryCreationTemplates"].([]any)
	assert.Len(t, tmpls, 1)
	tmpl := tmpls[0].(map[string]any)
	assert.Equal(t, "prod/", tmpl["prefix"])
	assert.Equal(t, "IMMUTABLE", tmpl["imageTagMutability"])

	delRec := doAccuracy(t, h, "DeleteRepositoryCreationTemplate", map[string]any{
		"prefix": "prod/",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	descRec2 := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{})
	require.Equal(t, http.StatusOK, descRec2.Code)
	out2 := parseAccuracy(t, descRec2)
	tmpls2, _ := out2["repositoryCreationTemplates"].([]any)
	assert.Empty(t, tmpls2)
}

func TestBatch1_RepositoryCreationTemplate_Update(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":             "staging/",
		"imageTagMutability": "MUTABLE",
	})

	updateRec := doAccuracy(t, h, "UpdateRepositoryCreationTemplate", map[string]any{
		"prefix":             "staging/",
		"imageTagMutability": "IMMUTABLE",
		"description":        "updated staging",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{
		"prefixes": []string{"staging/"},
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	out := parseAccuracy(t, descRec)
	tmpls, _ := out["repositoryCreationTemplates"].([]any)
	require.Len(t, tmpls, 1)
	tmpl := tmpls[0].(map[string]any)
	assert.Equal(t, "IMMUTABLE", tmpl["imageTagMutability"])
	assert.Equal(t, "updated staging", tmpl["description"])
}

// ── §27 Signing configuration ─────────────────────────────────────────────────

func TestBatch1_SigningConfiguration_PutGetDelete(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	profileArn := "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile"
	putRec := doAccuracy(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"signingProfileArn": profileArn,
					"repositoryFilters": []map[string]any{
						{"filter": "prod/*", "filterType": "PREFIX_MATCH"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	cfg, _ := out["signingConfiguration"].(map[string]any)
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	assert.Equal(t, profileArn, rule["signingProfileArn"])

	delRec := doAccuracy(t, h, "DeleteSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, delRec.Code)
}

// ── §28 DeleteRepository cascade ─────────────────────────────────────────────

func TestBatch1_DeleteRepository_Clears_TagIndex(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("del-idx", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("del-idx", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	assert.Equal(t, 1, b.RepoTagCount("del-idx"))

	_, err = b.DeleteRepository("del-idx")
	require.NoError(t, err)

	assert.Equal(t, 0, b.RepoTagCount("del-idx"),
		"DeleteRepository must clear the tagIndex for that repo")
}

func TestBatch1_DeleteRepository_Force_Clears_MultiTag(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "force-multi",
		"imageTagMutability": "MUTABLE",
	})

	manifest := `{"schemaVersion":2,"multi":true}`
	mustPutManifest(t, h, "force-multi", "t1", manifest)
	mustPutManifest(t, h, "force-multi", "t2", manifest)

	rec := doAccuracy(t, h, "DeleteRepository", map[string]any{
		"repositoryName": "force-multi",
		"force":          true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Repository must be gone.
	descRec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"force-multi"},
	})
	assert.Equal(t, http.StatusNotFound, descRec.Code)
}

// ── §29 Backend state isolation (deep copy) ──────────────────────────────────

func TestBatch1_Backend_PutImage_ReturnedValue_Isolated(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("iso-img", "MUTABLE", false, "", "")
	require.NoError(t, err)

	img, err := b.PutImage("iso-img", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	originalDigest := img.ImageDigest
	img.ImageDigest = "mutated"

	// Stored state must be unaffected.
	imgs, err := b.DescribeImages("iso-img", nil)
	require.NoError(t, err)
	require.Len(t, imgs, 1)
	assert.Equal(t, originalDigest, imgs[0].ImageDigest,
		"mutating PutImage return value must not affect stored image")
}

func TestBatch1_Backend_DescribeImages_ReturnedSlice_Isolated(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	_, err := b.CreateRepository("iso-desc", "MUTABLE", false, "", "")
	require.NoError(t, err)

	_, err = b.PutImage("iso-desc", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	imgs, err := b.DescribeImages("iso-desc", nil)
	require.NoError(t, err)
	require.Len(t, imgs, 1)

	// Mutate returned copy.
	imgs[0].RepositoryName = "mutated"

	imgs2, err := b.DescribeImages("iso-desc", nil)
	require.NoError(t, err)
	assert.Equal(t, "iso-desc", imgs2[0].RepositoryName,
		"mutating DescribeImages return value must not affect stored state")
}

// ── §30 Error handling accuracy ───────────────────────────────────────────────

func TestBatch1_CreateRepository_Empty_Name_Returns400(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_GetLifecyclePolicy_NonExistentPolicy_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "no-policy")

	rec := doAccuracy(t, h, "GetLifecyclePolicy", map[string]any{
		"repositoryName": "no-policy",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_GetLifecyclePolicyPreview_NoPreview_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "no-preview")

	rec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "no-preview",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_DeletePullThroughCacheRule_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "ghost-prefix",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_DescribePullThroughCacheRules_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"ghost"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_BatchDeleteImage_EmptyInput_Returns_EmptyResult(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "empty-del")

	rec := doAccuracy(t, h, "BatchDeleteImage", map[string]any{
		"repositoryName": "empty-del",
		"imageIds":       []map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	ids, _ := out["imageIds"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, ids)
	assert.Empty(t, failures)
}

// ── §31 DescribeImages with specific imageIds ─────────────────────────────────

func TestBatch1_DescribeImages_ByTag_Returns_Image(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-by-tag")
	mustPutManifest(t, h, "desc-by-tag", "release", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-by-tag",
		"imageIds":       []map[string]any{{"imageTag": "release"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 1)
	detail := details[0].(map[string]any)
	tags, _ := detail["imageTags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "release", tags[0].(string))
}

func TestBatch1_DescribeImages_ByDigest_Returns_Image(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "desc-by-digest")
	d := mustPutManifest(t, h, "desc-by-digest", "v1", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-by-digest",
		"imageIds":       []map[string]any{{"imageDigest": d}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 1)
	assert.Equal(t, d, details[0].(map[string]any)["imageDigest"])
}

// ── §32 ListImages pagination accuracy ───────────────────────────────────────

func TestBatch1_ListImages_Pagination_FullCoverage(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	mustCreateRepo(t, h, "list-full-page")

	for i := range 6 {
		mustPutManifest(t, h, "list-full-page",
			fmt.Sprintf("v%d", i),
			fmt.Sprintf(`{"schemaVersion":2,"n":%d}`, i))
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		body := map[string]any{
			"repositoryName": "list-full-page",
			"maxResults":     2,
		}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doAccuracy(t, h, "ListImages", body)
		require.Equal(t, http.StatusOK, rec.Code)
		out := parseAccuracy(t, rec)

		for _, id := range out["imageIds"].([]any) {
			item := id.(map[string]any)
			key := fmt.Sprintf("%v:%v", item["imageDigest"], item["imageTag"])
			seen[key] = true
		}

		pages++
		next, _ := out["nextToken"].(string)
		if next == "" {
			break
		}
		token = next
	}

	assert.Equal(t, 3, pages, "6 images / maxResults=2 → 3 pages")
	assert.Len(t, seen, 6, "all 6 images must appear exactly once across pages")
}
