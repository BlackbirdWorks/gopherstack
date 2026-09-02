package ecr_test

// handler_images_test.go — HTTP-handler-level tests for images.go: PutImage,
// BatchGetImage, BatchDeleteImage, DescribeImages, ListImages, and
// PutImageTagMutability, exercised end-to-end via the dispatcher. Backend-level
// coverage for the same ops lives in images_test.go.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func mustPutManifest(t *testing.T, h *ecr.Handler, repo, tag, manifest string) string {
	t.Helper()

	return mustPutImage(t, h, repo, tag, manifest)
}

func TestRetag_MUTABLE_Handler_Succeeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "retag-handler")

	mustPutManifest(t, h, "retag-handler", "rel", `{"schemaVersion":2,"v":1}`)

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "retag-handler",
		"imageManifest":  `{"schemaVersion":2,"v":2}`,
		"imageTag":       "rel",
	})
	require.Equal(t, http.StatusOK, rec.Code, "retag in MUTABLE repo must succeed")
}

func TestMultiTag_SameManifestTwoTags_SharedDigest(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "multi-tag-repo")

	manifest := `{"schemaVersion":2,"multi":"tag"}`
	d1 := mustPutManifest(t, h, "multi-tag-repo", "v1.0", manifest)
	d2 := mustPutManifest(t, h, "multi-tag-repo", "v1.0.0", manifest)

	assert.Equal(t, d1, d2,
		"same manifest pushed with different tags must yield the same digest")
}

func TestMultiTag_DescribeImages_ShowsBothTags(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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
	for _, tg := range imageTags {
		tags = append(tags, tg.(string))
	}
	sort.Strings(tags)
	assert.Equal(t, []string{"latest", "stable"}, tags)
}

func TestMultiTag_ListImages_ShowsBothTagEntries(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestUntagged_ListImages_IncludesUntaggedByDefault(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestListImages_Filter_TagStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tagStatus   string
		untagged    int
		tagged      int
		wantLen     int
		wantAllTags bool
	}{
		{name: "tagged_only", tagStatus: "TAGGED", untagged: 1, tagged: 2, wantLen: 2, wantAllTags: true},
		{name: "untagged_only", tagStatus: "UNTAGGED", untagged: 2, tagged: 1, wantLen: 2},
		{name: "any_returns_all", tagStatus: "ANY", untagged: 1, tagged: 1, wantLen: 2},
		{name: "no_filter_returns_all", tagStatus: "", untagged: 1, tagged: 1, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			repo := "filter-" + tt.name
			mustCreateRepo(t, h, repo)

			for i := range tt.untagged {
				doAccuracy(t, h, "PutImage", map[string]any{
					"repositoryName": repo,
					"imageManifest":  fmt.Sprintf(`{"schemaVersion":2,"u":%d}`, i),
				})
			}
			for i := range tt.tagged {
				mustPutManifest(t, h, repo, fmt.Sprintf("v%d", i), fmt.Sprintf(`{"schemaVersion":2,"t":%d}`, i))
			}

			body := map[string]any{"repositoryName": repo}
			if tt.tagStatus != "" {
				body["filter"] = map[string]any{"tagStatus": tt.tagStatus}
			}

			rec := doAccuracy(t, h, "ListImages", body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			ids, _ := out["imageIds"].([]any)
			assert.Len(t, ids, tt.wantLen, "tagStatus=%q", tt.tagStatus)

			if tt.wantAllTags {
				for _, item := range ids {
					id := item.(map[string]any)
					assert.NotEmpty(t, id["imageTag"], "each result must have an imageTag")
				}
			}
		})
	}
}

func TestBatchDeleteImage_Handler_ByTag(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchDeleteImage_Handler_ByDigest(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchDeleteImage_NonExistentImage_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchDeleteImage_Mixed_Success_And_Failure(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchDeleteImage_EmptyInput_Returns_EmptyResult(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchGetImage_ByTag_ReturnsImage(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchGetImage_ByDigest_ReturnsManifest(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchGetImage_MultipleImages_AllReturned(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchGetImage_MissingImage_ReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestBatchGetImage_MultiTag_ByEitherTag(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestDescribeImages_Pagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestDescribeImages_NextToken_ContinuesPagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestDescribeImages_FullPagination_CoversAll(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestManifestList_OCI_MediaType_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestManifestList_Docker_MediaType(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestManifestList_DescribeImages_ShowsMediaType(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestDescribeImages_ByTag_Returns_Image(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestDescribeImages_ByDigest_Returns_Image(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestListImages_Pagination_EnumeratesAll(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestPutImageTagMutability_MUTABLE_To_IMMUTABLE(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
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

func TestPutImageTagMutability_WithExclusionFilters(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "filter-repo-mut")

	rec := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "filter-repo-mut",
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

func TestPutImageTagMutability_NonExistent_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "PutImageTagMutability", map[string]any{
		"repositoryName":     "ghost-repo",
		"imageTagMutability": "IMMUTABLE",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestImmutableRepo_RejectRetag(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-repo-2",
		"imageTagMutability": "IMMUTABLE",
	})

	// Push first image with tag v1.
	mustPutImage(t, h, "immutable-repo-2", "v1", `{"schemaVersion":2,"v":1}`)

	// Try to push a different manifest with same tag v1 — must fail.
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo-2",
		"imageManifest":  `{"schemaVersion":2,"v":2}`,
		"imageTag":       "v1",
	})
	// AWS returns 400 with ImageTagAlreadyExistsException for IMMUTABLE tag violations.
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"IMMUTABLE repo must reject retagging an existing tag to a different digest")
}

// TestImmutableRepo_SameManifestSameTag_ImageAlreadyExists locks the
// PutImage ImageAlreadyExistsException gap: re-pushing an unchanged
// manifest+tag pair ("no changes to the manifest or image tag after the last
// push", per the real API doc, confirmed against the moto ECR emulator's
// put_image logic) is rejected, independent of repository tag mutability.
// This test previously asserted the opposite (success) as a documented gap;
// see images.go's PutImage ImageAlreadyExistsException comment.
func TestImmutableRepo_SameManifestSameTag_ImageAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-idempotent",
		"imageTagMutability": "IMMUTABLE",
	})

	manifest := `{"schemaVersion":2,"idempotent":true}`
	mustPutImage(t, h, "immutable-idempotent", "stable", manifest)

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-idempotent",
		"imageManifest":  manifest,
		"imageTag":       "stable",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"re-pushing an unchanged manifest+tag must be rejected as a no-op push")

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ImageAlreadyExistsException", out["__type"])
}

func TestImmutableRepo_NewTag_Allowed(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-new-tag",
		"imageTagMutability": "IMMUTABLE",
	})

	mustPutImage(t, h, "immutable-new-tag", "v1", `{"schemaVersion":2,"n":1}`)

	// New tag for new manifest — must succeed.
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-new-tag",
		"imageManifest":  `{"schemaVersion":2,"n":2}`,
		"imageTag":       "v2",
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"IMMUTABLE repo must allow new tags")
}

func TestDescribeImages_ByDigest_NonExistent_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "desc-missing-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-missing-repo",
		"imageIds": []map[string]any{
			{"imageDigest": "sha256:9999999999999999999999999999999999999999999999999999999999999999"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"DescribeImages with non-existent imageId must return an error")
}

func TestDescribeImages_EmptyRepo_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "empty-desc-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "empty-desc-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Empty(t, details)
}

func TestDescribeImages_ImagePushedAt_IsNumeric(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "img-repo")
	mustPutImage(t, h, "img-repo", "latest", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "img-repo",
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

func TestDescribeImages_ImagePushedAt_NotZero(t *testing.T) {
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

// TestDescribeImages_ArtifactMediaType_FromManifest locks that ImageDetail's
// artifactMediaType is derived from the pushed manifest's top-level
// "artifactType" field (OCI 1.1 artifact manifests). A manifest without that
// field (e.g. a plain Docker v2 manifest) must simply omit the field.
func TestDescribeImages_ArtifactMediaType_FromManifest(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "artifact-repo")
	mustPutImage(t, h, "artifact-repo", "v1",
		`{"schemaVersion":2,"artifactType":"application/vnd.example.artifact+type"}`)
	mustPutImage(t, h, "artifact-repo", "plain", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "artifact-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 2)

	byTag := make(map[string]map[string]any)
	for _, d := range details {
		detail := d.(map[string]any)
		tags, _ := detail["imageTags"].([]any)
		require.Len(t, tags, 1)
		byTag[tags[0].(string)] = detail
	}

	assert.Equal(t, "application/vnd.example.artifact+type", byTag["v1"]["artifactMediaType"])
	assert.NotContains(t, byTag["plain"], "artifactMediaType",
		"a manifest with no artifactType must omit artifactMediaType")
}

// TestDescribeImages_SubjectManifestDigest_FromManifest locks that
// ImageDetail's subjectManifestDigest is derived from the pushed manifest's
// OCI "subject.digest" field (used for referrer relationships).
func TestDescribeImages_SubjectManifestDigest_FromManifest(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "subject-repo")
	mustPutImage(t, h, "subject-repo", "referrer",
		`{"schemaVersion":2,"subject":{"digest":"sha256:`+strings.Repeat("a", 64)+`","mediaType":"x"}}`)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "subject-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 1)
	detail := details[0].(map[string]any)

	assert.Equal(t, "sha256:"+strings.Repeat("a", 64), detail["subjectManifestDigest"])
}

// TestDescribeImages_ScanFields_PopulatedAfterScan locks that ImageDetail's
// imageScanFindingsSummary/imageScanStatus are derived from the
// imageScanFindings store once StartImageScan has run, and are absent
// (rather than zero-valued objects) for an image that was never scanned.
func TestDescribeImages_ScanFields_PopulatedAfterScan(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-fields-repo")
	scannedDigest := mustPutImage(t, h, "scan-fields-repo", "scanned", `{"schemaVersion":2,"a":1}`)
	mustPutImage(t, h, "scan-fields-repo", "unscanned", `{"schemaVersion":2,"a":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-fields-repo",
		"imageId":        map[string]any{"imageDigest": scannedDigest},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "scan-fields-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	require.Len(t, details, 2)

	byDigest := make(map[string]map[string]any)
	for _, d := range details {
		detail := d.(map[string]any)
		byDigest[detail["imageDigest"].(string)] = detail
	}

	scanned := byDigest[scannedDigest]
	require.NotNil(t, scanned)
	status, ok := scanned["imageScanStatus"].(map[string]any)
	require.True(t, ok, "imageScanStatus must be present for a scanned image")
	assert.Equal(t, "COMPLETE", status["status"])
	summary, ok := scanned["imageScanFindingsSummary"].(map[string]any)
	require.True(t, ok, "imageScanFindingsSummary must be present for a scanned image")
	completedAt, ok := summary["imageScanCompletedAt"].(float64)
	require.True(t, ok, "imageScanCompletedAt must be a number")
	assert.Positive(t, completedAt)

	for _, detail := range byDigest {
		if detail["imageDigest"] == scannedDigest {
			continue
		}

		assert.NotContains(t, detail, "imageScanStatus", "an unscanned image must omit imageScanStatus")
		assert.NotContains(t, detail, "imageScanFindingsSummary",
			"an unscanned image must omit imageScanFindingsSummary")
	}
}

// TestDescribeImages_LastRecordedPullTime_ViaBatchGetImage locks that
// BatchGetImage stamps lastRecordedPullTime, surfaced via DescribeImages.
func TestDescribeImages_LastRecordedPullTime_ViaBatchGetImage(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "pull-time-repo")
	digest := mustPutImage(t, h, "pull-time-repo", "v1", `{"schemaVersion":2}`)

	beforeRec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "pull-time-repo"})
	require.Equal(t, http.StatusOK, beforeRec.Code)
	beforeDetail := parseAccuracy(t, beforeRec)["imageDetails"].([]any)[0].(map[string]any)
	assert.NotContains(t, beforeDetail, "lastRecordedPullTime",
		"an image that was never pulled must omit lastRecordedPullTime")

	getRec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "pull-time-repo",
		"imageIds":       []map[string]any{{"imageDigest": digest}},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	afterRec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "pull-time-repo"})
	require.Equal(t, http.StatusOK, afterRec.Code)
	afterDetail := parseAccuracy(t, afterRec)["imageDetails"].([]any)[0].(map[string]any)
	pullTime, ok := afterDetail["lastRecordedPullTime"].(float64)
	require.True(t, ok, "lastRecordedPullTime must be a number after BatchGetImage")
	assert.Positive(t, pullTime)
}

// TestDescribeImages_LastRecordedPullTime_ViaGetDownloadUrlForLayer locks
// that resolving a layer's download URL stamps lastRecordedPullTime on every
// image whose manifest references that layer digest.
func TestDescribeImages_LastRecordedPullTime_ViaGetDownloadUrlForLayer(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "pull-time-layer-repo")

	layerDigest := mustUploadLayerHTTP(t, h, "pull-time-layer-repo", []byte("layer-bytes"))
	manifest := `{"schemaVersion":2,"layers":[{"digest":"` + layerDigest + `"}]}`
	mustPutImage(t, h, "pull-time-layer-repo", "v1", manifest)

	dlRec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "pull-time-layer-repo",
		"layerDigest":    layerDigest,
	})
	require.Equal(t, http.StatusOK, dlRec.Code)

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "pull-time-layer-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	detail := parseAccuracy(t, rec)["imageDetails"].([]any)[0].(map[string]any)
	pullTime, ok := detail["lastRecordedPullTime"].(float64)
	require.True(t, ok, "lastRecordedPullTime must be a number after GetDownloadUrlForLayer")
	assert.Positive(t, pullTime)
}

// TestDescribeImages_LastArchivedAt_LastActivatedAt_ViaUpdateImageStorageClass
// locks that UpdateImageStorageClass stamps lastArchivedAt/lastActivatedAt,
// surfaced via DescribeImages.
func TestDescribeImages_LastArchivedAt_LastActivatedAt_ViaUpdateImageStorageClass(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "storage-class-time-repo")
	digest := mustPutImage(t, h, "storage-class-time-repo", "v1", `{"schemaVersion":2}`)

	archiveRec := doAccuracy(t, h, "UpdateImageStorageClass", map[string]any{
		"repositoryName":     "storage-class-time-repo",
		"imageId":            map[string]any{"imageDigest": digest},
		"targetStorageClass": "ARCHIVE",
	})
	require.Equal(t, http.StatusOK, archiveRec.Code)

	// DescribeImages defaults to ACTIVE-only per AWS docs, so an archived image
	// needs an explicit ImageStatus filter to appear in the response.
	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "storage-class-time-repo",
		"filter":         map[string]any{"imageStatus": "ARCHIVED"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	detail := parseAccuracy(t, rec)["imageDetails"].([]any)[0].(map[string]any)
	archivedAt, ok := detail["lastArchivedAt"].(float64)
	require.True(t, ok, "lastArchivedAt must be a number after archiving")
	assert.Positive(t, archivedAt)
	assert.NotContains(t, detail, "lastActivatedAt",
		"an image that was only ever archived must omit lastActivatedAt")

	activateRec := doAccuracy(t, h, "UpdateImageStorageClass", map[string]any{
		"repositoryName":     "storage-class-time-repo",
		"imageId":            map[string]any{"imageDigest": digest},
		"targetStorageClass": "STANDARD",
	})
	require.Equal(t, http.StatusOK, activateRec.Code)

	rec2 := doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "storage-class-time-repo"})
	require.Equal(t, http.StatusOK, rec2.Code)
	detail2 := parseAccuracy(t, rec2)["imageDetails"].([]any)[0].(map[string]any)
	activatedAt, ok := detail2["lastActivatedAt"].(float64)
	require.True(t, ok, "lastActivatedAt must be a number after re-activating")
	assert.Positive(t, activatedAt)
}

func TestPutImage_IMMUTABLE_SameTag_DifferentDigest_Rejected(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-repo-3",
		"imageTagMutability": "IMMUTABLE",
	})

	// First push succeeds.
	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo-3",
		"imageManifest":  `{"schemaVersion":2,"v":"1"}`,
		"imageTag":       "stable",
	})
	require.Equal(t, http.StatusOK, rec1.Code, "first push must succeed")

	// Second push with same tag but different manifest (different digest) must fail.
	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo-3",
		"imageManifest":  `{"schemaVersion":2,"v":"2"}`,
		"imageTag":       "stable",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code,
		"retagging in an IMMUTABLE repo must return 400")

	out := parseAccuracy(t, rec2)
	errType, _ := out["__type"].(string)
	assert.Equal(t, "ImageTagAlreadyExistsException", errType)
}

func TestPutImage_IMMUTABLE_NewTag_Succeeds(t *testing.T) {
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

// TestPutImage_MUTABLE_SameTagSameDigest_ImageAlreadyExists locks that
// ImageAlreadyExistsException fires on a MUTABLE repository too: it is a
// distinct check from the IMMUTABLE-only ImageTagAlreadyExistsException
// retag guard (see PutImage in images.go), so a repo's tag mutability
// setting must not affect whether re-pushing an unchanged manifest+tag is
// rejected.
func TestPutImage_MUTABLE_SameTagSameDigest_ImageAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "mutable-idem")

	manifest := `{"schemaVersion":2,"idempotent":true}`

	rec1 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "mutable-idem",
		"imageManifest":  manifest,
		"imageTag":       "prod",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "mutable-idem",
		"imageManifest":  manifest,
		"imageTag":       "prod",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code,
		"re-pushing identical content to a MUTABLE repo must also be rejected as a no-op push")

	out := parseAccuracy(t, rec2)
	assert.Equal(t, "ImageAlreadyExistsException", out["__type"])
}

func TestPutImage_MUTABLE_Retag_Succeeds(t *testing.T) {
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

func TestPutImageTagMutability_Switch_IMMUTABLE_Enforced(t *testing.T) {
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

func TestListImages_MaxResults_LimitsResponse(t *testing.T) {
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

func TestListImages_NextToken_ContinuesPagination(t *testing.T) {
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

func TestECR_BackendPutImageReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	_, err := backend.CreateRepository(context.Background(), "copy-repo", "", false, "", "")
	require.NoError(t, err)

	img, err := backend.PutImage(context.Background(), "copy-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "latest"},
	})
	require.NoError(t, err)
	img.ImageStatus = "CORRUPTED"
	img.ImageID.ImageTag = "changed"

	stored, err := backend.DescribeImages(
		context.Background(),
		"copy-repo",
		[]ecr.ImageIdentifier{{ImageDigest: img.ImageDigest}},
	)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, "ACTIVE", stored[0].ImageStatus)
	assert.Equal(t, "latest", stored[0].ImageID.ImageTag)
}

func TestDescribeImages_FilterTagStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
		wantCount int
	}{
		{
			name:      "tagged_only_returns_only_tagged",
			tagStatus: "TAGGED",
			wantCount: 1,
		},
		{
			name:      "untagged_only_returns_only_untagged",
			tagStatus: "UNTAGGED",
			wantCount: 1,
		},
		{
			name:      "any_returns_all",
			tagStatus: "ANY",
			wantCount: 2,
		},
		{
			name:      "empty_filter_returns_all",
			tagStatus: "",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "filter-repo-2")

			// Push a tagged image.
			taggedDigest := mustPutImage(t, h, "filter-repo-2", "v1.0", `{"tagged":true}`)

			// Push an untagged image (no tag field).
			untaggedDigest := mustPutImage(t, h, "filter-repo-2", "", `{"untagged":true}`)
			require.NotEmpty(t, untaggedDigest)

			body := map[string]any{
				"repositoryName": "filter-repo-2",
			}
			if tt.tagStatus != "" {
				body["filter"] = map[string]any{"tagStatus": tt.tagStatus}
			}

			rec := doAccuracy(t, h, "DescribeImages", body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			details, _ := out["imageDetails"].([]any)
			assert.Len(t, details, tt.wantCount, "tagStatus=%q", tt.tagStatus)

			// Spot-check which digest shows up when filtering TAGGED.
			if tt.tagStatus == "TAGGED" {
				require.Len(t, details, 1)
				assert.Equal(t, taggedDigest, details[0].(map[string]any)["imageDigest"])
			}

			// Spot-check which digest shows up when filtering UNTAGGED.
			if tt.tagStatus == "UNTAGGED" {
				require.Len(t, details, 1)
				assert.Equal(t, untaggedDigest, details[0].(map[string]any)["imageDigest"])
			}
		})
	}
}

func TestDescribeImages_FilterIgnoredWhenImageIDsProvided(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "id-filter-repo")

	// Push an untagged image (no tag).
	digest := mustPutImage(t, h, "id-filter-repo", "", `{"content":"only"}`)

	// When imageIds are specified, filter.tagStatus is not applied.
	descRec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "id-filter-repo",
		"imageIds":       []map[string]any{{"imageDigest": digest}},
		"filter":         map[string]any{"tagStatus": "TAGGED"},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	out := parseAccuracy(t, descRec)
	details, _ := out["imageDetails"].([]any)
	assert.Len(t, details, 1,
		"untagged filter with explicit digest still returns image")
}

func TestDescribeImages_FilterTagStatus_EmptyRepo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
	}{
		{name: "tagged_on_empty_repo", tagStatus: "TAGGED"},
		{name: "untagged_on_empty_repo", tagStatus: "UNTAGGED"},
		{name: "any_on_empty_repo", tagStatus: "ANY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "empty-filter-repo-"+tt.tagStatus)

			rec := doAccuracy(t, h, "DescribeImages", map[string]any{
				"repositoryName": "empty-filter-repo-" + tt.tagStatus,
				"filter":         map[string]any{"tagStatus": tt.tagStatus},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			details, _ := out["imageDetails"].([]any)
			assert.Empty(t, details, "tagStatus=%q on empty repo must return empty list", tt.tagStatus)
		})
	}
}

// TestListImages_OpaqueNextToken verifies that the nextToken emitted
// by ListImages is base64-opaque and round-trips correctly.
func TestListImages_OpaqueNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		imageCount int
		maxResults int
		wantNext   bool
	}{
		{
			name:       "no_pagination_when_all_fit",
			imageCount: 2,
			maxResults: 10,
			wantNext:   false,
		},
		{
			name:       "token_emitted_and_round_trips",
			imageCount: 3,
			maxResults: 2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newHandlerWithBackend()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for i := range tt.imageCount {
				tag := "v1." + string(rune('0'+i))
				digest := "sha256:" + strings.Repeat("a", 63-i) + string(rune('0'+i))
				_, err = b.PutImage(ctx, "myrepo", ecr.Image{
					ImageDigest:    digest,
					ImageID:        ecr.ImageIdentifier{ImageDigest: digest, ImageTag: tag},
					RepositoryName: "myrepo",
					RegistryID:     "123456789012",
				})
				require.NoError(t, err)
			}

			rec := doAccuracy(t, h, "ListImages", map[string]any{
				"repositoryName": "myrepo",
				"maxResults":     tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			nextToken, _ := out["nextToken"].(string)

			if !tt.wantNext {
				assert.Empty(t, nextToken)

				return
			}

			require.NotEmpty(t, nextToken, "nextToken must be emitted when truncated")

			// Token must be valid base64.
			decoded, err := base64.StdEncoding.DecodeString(nextToken)
			require.NoError(t, err, "nextToken must be valid base64")
			assert.Contains(t, string(decoded), "sha256:", "decoded cursor must contain a digest")

			// Round-trip: use the token to get the next page.
			rec2 := doAccuracy(t, h, "ListImages", map[string]any{
				"repositoryName": "myrepo",
				"maxResults":     tt.maxResults,
				"nextToken":      nextToken,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseAccuracy(t, rec2)
			ids1, _ := out["imageIds"].([]any)
			ids2, _ := out2["imageIds"].([]any)
			assert.Equal(
				t,
				tt.imageCount,
				len(ids1)+len(ids2),
				"both pages together must cover all images",
			)
		})
	}
}

// TestPutImage_ImageView_OmitsInventedFields locks the PutImage "image" wire
// shape against the real AWS ecr.types.Image, which has exactly five fields —
// imageId, imageManifest, imageManifestMediaType, registryId, repositoryName
// (per awsAwsjson11_deserializeDocumentImage). gopherstack's internal Image
// domain struct additionally carries imageDigest, imagePushedAt, imageStatus,
// storageClass, and imageSizeInBytes for its own bookkeeping (used by the
// separate DescribeImages ImageDetail shape); those must never leak onto the
// PutImage/BatchGetImage "image" object.
func TestPutImage_ImageView_OmitsInventedFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "image-view-repo")

	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "image-view-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "v1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	img, ok := out["image"].(map[string]any)
	require.True(t, ok)

	for _, invented := range []string{"imageDigest", "imagePushedAt", "imageStatus", "storageClass", "imageSizeInBytes"} {
		_, present := img[invented]
		assert.False(t, present, "PutImage image object must not carry invented field %q", invented)
	}

	imageID, ok := img["imageId"].(map[string]any)
	require.True(t, ok, "image.imageId must be present")
	assert.NotEmpty(t, imageID["imageDigest"], "the digest belongs under imageId.imageDigest")
	assert.Equal(t, "v1", imageID["imageTag"])
}

// TestBatchGetImage_ImageView_OmitsInventedFields is the BatchGetImage
// counterpart of TestPutImage_ImageView_OmitsInventedFields: BatchGetImageOutput.Images
// is also []types.Image, the same thin shape as PutImage's response.
func TestBatchGetImage_ImageView_OmitsInventedFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "bgi-view-repo")
	digest := mustPutImage(t, h, "bgi-view-repo", "v1", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "BatchGetImage", map[string]any{
		"repositoryName": "bgi-view-repo",
		"imageIds":       []map[string]any{{"imageDigest": digest}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	images, ok := out["images"].([]any)
	require.True(t, ok)
	require.Len(t, images, 1)

	img, ok := images[0].(map[string]any)
	require.True(t, ok)

	for _, invented := range []string{"imageDigest", "imagePushedAt", "imageStatus", "storageClass", "imageSizeInBytes"} {
		_, present := img[invented]
		assert.False(t, present, "BatchGetImage image object must not carry invented field %q", invented)
	}
}
