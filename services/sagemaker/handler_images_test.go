package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageArn"], "my-image")
}

func TestHandler_DescribeImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-1", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "img-1", resp["ImageName"])
	assert.Equal(t, "CREATED", resp["ImageStatus"])
}

func TestHandler_DeleteImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-del", "RoleArn": "arn:test"})
	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListImages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-a", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-b", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "ListImages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Images"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

func TestHandler_CreateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-ver", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-ver", "BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageVersionArn"], "img-ver")
}

func TestHandler_DescribeImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-v", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-v", "BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})

	rec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{"ImageName": "img-v", "Version": 1})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InEpsilon(t, float64(1), resp["Version"], 0.001)
	assert.Equal(t, "CREATED", resp["ImageVersionStatus"])
}

func TestHandler_ListImageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-lv", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-lv", "BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:v1",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-lv", "BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:v2",
	})

	rec := doSageMakerRequest(t, h, "ListImageVersions", map[string]any{"ImageName": "img-lv"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ImageVersions"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// CompilationJob
// ---------------------------------------------------------------------------

func TestDeleteImage_WithVersions_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-has-ver",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-has-ver",
		"BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-has-ver",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteImage_AfterVersionsRemoved_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-ver-cleanup",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
		"BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})
	doSageMakerRequest(t, h, "DeleteImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
		"Version":   1,
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-ver-cleanup",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// MonitoringSchedule: stop/start state guards
// ---------------------------------------------------------------------------

func TestHandler_ListAliases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "alias-img", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "alias-img", "BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "alias-img", "AliasesToAdd": []string{"latest", "stable"},
	})

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "alias-img"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	aliases, _ := out["SageMakerImageVersionAliases"].([]any)
	assert.ElementsMatch(t, []any{"latest", "stable"}, aliases)
}

func TestHandler_ListAliases_ImageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "no-such-image"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

func TestHandler_UpdateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName":   "my-image",
		"RoleArn":     "arn:test",
		"Description": "original",
	})

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":   "my-image",
		"DisplayName": "My Image",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["Description"])
	assert.Equal(t, "My Image", resp["DisplayName"])

	// DeleteProperties clears Description.
	doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":        "my-image",
		"DeleteProperties": []string{"Description"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	assert.Empty(t, resp2["Description"])
}

func TestHandler_UpdateImage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "my-image",
		"BaseImage": "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag",
	})

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":    "my-image",
		"Version":      1,
		"MLFramework":  "PyTorch 2.0",
		"AliasesToAdd": []string{"latest", "stable"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "PyTorch 2.0", resp["MLFramework"])
	aliases, ok := resp["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	// Remove one alias.
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":       "my-image",
		"Version":         1,
		"AliasesToDelete": []string{"latest"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	aliases2, ok := resp2["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases2, 1)
	assert.Equal(t, "stable", aliases2[0])
}

func TestHandler_UpdateImageVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "does-not-exist",
		"Version":   1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_CreateImage_DisplayName_RealClient asserts DisplayName -- absent
// before this pass -- round-trips through Describe.
func TestHandler_CreateImage_DisplayName_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName:   aws.String("img-display"),
		RoleArn:     aws.String("arn:aws:iam::000000000000:role/test"),
		DisplayName: aws.String("Friendly Name"),
	})
	require.NoError(t, err)

	out, err := client.DescribeImage(t.Context(), &sagemakersdk.DescribeImageInput{
		ImageName: aws.String("img-display"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Friendly Name", aws.ToString(out.DisplayName))
}

// TestHandler_ListImages_FilterSortPage_RealClient asserts ListImagesInput's
// filter/sort/pagination fields -- absent before this pass except NextToken
// -- actually narrow, reorder (defaulting to Descending per
// api_op_ListImages.go:60, not the Ascending default most other List ops in
// this service use), and paginate the result set.
func TestHandler_ListImages_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-image", "beta-image", "gamma-widget"}
	for _, n := range names {
		_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
			ImageName: aws.String(n),
			RoleArn:   aws.String("arn:test"),
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListImages(t.Context(), &sagemakersdk.ListImagesInput{
			NameContains: aws.String("image"),
		})
		require.NoError(t, err)
		assert.Len(t, out.Images, 2)
	})

	t.Run("default sort order is descending by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListImages(t.Context(), &sagemakersdk.ListImagesInput{
			SortBy: smtypes.ImageSortByImageName,
		})
		require.NoError(t, err)
		require.Len(t, out.Images, 3)
		assert.Equal(t, "gamma-widget", aws.ToString(out.Images[0].ImageName))
		assert.Equal(t, "alpha-image", aws.ToString(out.Images[2].ImageName))
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListImages(t.Context(), &sagemakersdk.ListImagesInput{
			SortBy:    smtypes.ImageSortByImageName,
			SortOrder: smtypes.ImageSortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.Images, 3)
		assert.Equal(t, "alpha-image", aws.ToString(out.Images[0].ImageName))
		assert.Equal(t, "gamma-widget", aws.ToString(out.Images[2].ImageName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListImages(t.Context(), &sagemakersdk.ListImagesInput{
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ImageSortByImageName,
			SortOrder:  smtypes.ImageSortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.Images, 1)
		assert.Equal(t, "alpha-image", aws.ToString(out.Images[0].ImageName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})
}

// TestHandler_CreateImageVersion_FullFields_RealClient asserts
// CreateImageVersionInput's optional fields -- entirely absent before this
// pass, forcing every real client to make an immediate follow-up
// UpdateImageVersion call just to set them -- are now set at creation time,
// and that BaseImage (a real required field this backend previously didn't
// even declare) round-trips into both BaseImage and ContainerImage.
func TestHandler_CreateImageVersion_FullFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-full"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	_, err = client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
		ImageName:       aws.String("img-full"),
		BaseImage:       aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
		Aliases:         []string{"latest"},
		Horovod:         aws.Bool(true),
		JobType:         smtypes.JobTypeTraining,
		MLFramework:     aws.String("PyTorch 2.0"),
		Processor:       smtypes.ProcessorGpu,
		ProgrammingLang: aws.String("python:3.9"),
		ReleaseNotes:    aws.String("first cut"),
		VendorGuidance:  smtypes.VendorGuidanceStable,
	})
	require.NoError(t, err)

	out, err := client.DescribeImageVersion(t.Context(), &sagemakersdk.DescribeImageVersionInput{
		ImageName: aws.String("img-full"),
		Version:   aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag", aws.ToString(out.BaseImage))
	assert.Equal(t, "111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag", aws.ToString(out.ContainerImage))
	assert.True(t, aws.ToBool(out.Horovod))
	assert.Equal(t, smtypes.JobTypeTraining, out.JobType)
	assert.Equal(t, "PyTorch 2.0", aws.ToString(out.MLFramework))
	assert.Equal(t, smtypes.ProcessorGpu, out.Processor)
	assert.Equal(t, "python:3.9", aws.ToString(out.ProgrammingLang))
	assert.Equal(t, "first cut", aws.ToString(out.ReleaseNotes))
	assert.Equal(t, smtypes.VendorGuidanceStable, out.VendorGuidance)
}

// TestHandler_DescribeImageVersion_DefaultsToLatest_RealClient asserts
// DescribeImageVersion, called with no Version and no Alias, returns the
// highest version rather than failing outright (the pre-existing bug: the
// handler decoded a zero-value int as an ordinary version number and looked
// up a version that never exists, even though api_op_DescribeImageVersion.go:
// 824-825 documents "If not specified, the latest version is described").
func TestHandler_DescribeImageVersion_DefaultsToLatest_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-latest"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	for range 2 {
		_, verErr := client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
			ImageName: aws.String("img-latest"),
			BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
		})
		require.NoError(t, verErr)
	}

	out, err := client.DescribeImageVersion(t.Context(), &sagemakersdk.DescribeImageVersionInput{
		ImageName: aws.String("img-latest"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(out.Version))
}

// TestHandler_ImageVersionAlias_RealClient asserts Alias -- absent from
// DescribeImageVersion/DeleteImageVersion/UpdateImageVersion/ListAliases
// before this pass -- resolves to the version it is attached to across all
// four ops, matching the real SDK's Version-or-Alias identifier pattern.
func TestHandler_ImageVersionAlias_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-alias"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	_, err = client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
		ImageName: aws.String("img-alias"),
		BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:v1"),
		Aliases:   []string{"stable"},
	})
	require.NoError(t, err)

	t.Run("describe by alias", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeImageVersion(t.Context(), &sagemakersdk.DescribeImageVersionInput{
			ImageName: aws.String("img-alias"),
			Alias:     aws.String("stable"),
		})
		require.NoError(t, descErr)
		assert.Equal(t, int32(1), aws.ToInt32(out.Version))
	})

	t.Run("update by alias", func(t *testing.T) {
		t.Parallel()

		_, updateErr := client.UpdateImageVersion(t.Context(), &sagemakersdk.UpdateImageVersionInput{
			ImageName:   aws.String("img-alias"),
			Alias:       aws.String("stable"),
			MLFramework: aws.String("TensorFlow 2.0"),
		})
		require.NoError(t, updateErr)

		out, descErr := client.DescribeImageVersion(t.Context(), &sagemakersdk.DescribeImageVersionInput{
			ImageName: aws.String("img-alias"),
			Version:   aws.Int32(1),
		})
		require.NoError(t, descErr)
		assert.Equal(t, "TensorFlow 2.0", aws.ToString(out.MLFramework))
	})

	t.Run("unknown alias fails", func(t *testing.T) {
		t.Parallel()

		_, descErr := client.DescribeImageVersion(t.Context(), &sagemakersdk.DescribeImageVersionInput{
			ImageName: aws.String("img-alias"),
			Alias:     aws.String("no-such-alias"),
		})
		require.Error(t, descErr)
	})
}

// TestHandler_DeleteImageVersion_RequiresIdentifier_RealClient asserts
// DeleteImageVersion rejects a call with neither Version nor Alias, rather
// than silently defaulting to some version -- api_op_DeleteImageVersion.go:
// 950-964 documents no "if unspecified" default, unlike DescribeImageVersion.
func TestHandler_DeleteImageVersion_RequiresIdentifier_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-del-ver"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	_, err = client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
		ImageName: aws.String("img-del-ver"),
		BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
	})
	require.NoError(t, err)

	_, err = client.DeleteImageVersion(t.Context(), &sagemakersdk.DeleteImageVersionInput{
		ImageName: aws.String("img-del-ver"),
	})
	require.Error(t, err)

	_, err = client.DeleteImageVersion(t.Context(), &sagemakersdk.DeleteImageVersionInput{
		ImageName: aws.String("img-del-ver"),
		Alias:     aws.String("nonexistent"),
	})
	require.Error(t, err)

	_, err = client.DeleteImageVersion(t.Context(), &sagemakersdk.DeleteImageVersionInput{
		ImageName: aws.String("img-del-ver"),
		Version:   aws.Int32(1),
	})
	require.NoError(t, err)
}

// TestHandler_ListImageVersions_FilterSortPage_RealClient asserts
// ListImageVersionsInput's filter/sort/pagination fields -- absent before
// this pass except NextToken -- narrow, reorder (defaulting to Descending per
// api_op_ListImageVersions.go:61), and paginate; and that ImageArn -- a
// required ImageVersion-summary field the handler never emitted -- is now
// present on every summary.
func TestHandler_ListImageVersions_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	createOut, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-lv-rc"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	for range 3 {
		_, verErr := client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
			ImageName: aws.String("img-lv-rc"),
			BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
		})
		require.NoError(t, verErr)
	}

	t.Run("image arn present on every summary", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListImageVersions(t.Context(), &sagemakersdk.ListImageVersionsInput{
			ImageName: aws.String("img-lv-rc"),
		})
		require.NoError(t, listErr)
		require.Len(t, out.ImageVersions, 3)

		for _, iv := range out.ImageVersions {
			assert.Equal(t, aws.ToString(createOut.ImageArn), aws.ToString(iv.ImageArn))
		}
	})

	t.Run("default sort order is descending by version", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListImageVersions(t.Context(), &sagemakersdk.ListImageVersionsInput{
			ImageName: aws.String("img-lv-rc"),
			SortBy:    smtypes.ImageVersionSortByVersion,
		})
		require.NoError(t, listErr)
		require.Len(t, out.ImageVersions, 3)
		assert.Equal(t, int32(3), aws.ToInt32(out.ImageVersions[0].Version))
		assert.Equal(t, int32(1), aws.ToInt32(out.ImageVersions[2].Version))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListImageVersions(t.Context(), &sagemakersdk.ListImageVersionsInput{
			ImageName:  aws.String("img-lv-rc"),
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ImageVersionSortByVersion,
			SortOrder:  smtypes.ImageVersionSortOrderAscending,
		})
		require.NoError(t, listErr)
		require.Len(t, out.ImageVersions, 1)
		assert.Equal(t, int32(1), aws.ToInt32(out.ImageVersions[0].Version))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})
}

// TestHandler_ListAliases_AliasAndMaxResults_RealClient asserts ListAliases'
// Alias and MaxResults fields -- absent before this pass -- respectively
// narrow the aggregation to one version's aliases and cap the page.
func TestHandler_ListAliases_AliasAndMaxResults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateImage(t.Context(), &sagemakersdk.CreateImageInput{
		ImageName: aws.String("img-aliases-rc"),
		RoleArn:   aws.String("arn:test"),
	})
	require.NoError(t, err)

	_, err = client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
		ImageName: aws.String("img-aliases-rc"),
		BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:v1"),
		Aliases:   []string{"v1-alias"},
	})
	require.NoError(t, err)

	_, err = client.CreateImageVersion(t.Context(), &sagemakersdk.CreateImageVersionInput{
		ImageName: aws.String("img-aliases-rc"),
		BaseImage: aws.String("111111111111.dkr.ecr.us-east-1.amazonaws.com/repo:v2"),
		Aliases:   []string{"v2-alias-a", "v2-alias-b"},
	})
	require.NoError(t, err)

	t.Run("alias narrows to one version", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListAliases(t.Context(), &sagemakersdk.ListAliasesInput{
			ImageName: aws.String("img-aliases-rc"),
			Alias:     aws.String("v1-alias"),
		})
		require.NoError(t, listErr)
		assert.Equal(t, []string{"v1-alias"}, out.SageMakerImageVersionAliases)
	})

	t.Run("max results caps the page", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListAliases(t.Context(), &sagemakersdk.ListAliasesInput{
			ImageName:  aws.String("img-aliases-rc"),
			MaxResults: aws.Int32(1),
		})
		require.NoError(t, listErr)
		assert.Len(t, out.SageMakerImageVersionAliases, 1)
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})
}
