package ecr_test

// list_filter_params_test.go ratifies the gopherstack-6flj wrapper-key sweep's
// constrained-parameter fixes for ecr: DescribeImages, ListImages,
// DescribeRepositories, DescribePullThroughCacheRules, and
// DescribeRepositoryCreationTemplates all ignored their documented "100 by
// default" MaxResults (ecr@v1.60.4 api_op_*.go), returning every item
// unbounded when the client sent nothing; and DescribeImages/ListImages never
// read their Filter's ImageStatus member at all — an image moved to ARCHIVED
// storage (UpdateImageStorageClass) kept showing up even though both ops
// document "If not specified, only images with ACTIVE status are returned."

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultPageSizeSeed = 105

func TestDescribeImages_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "describe-images-default")

	for i := range defaultPageSizeSeed {
		mustPutImage(t, h, "describe-images-default", fmt.Sprintf("v%03d", i), fmt.Sprintf(`{"n":%d}`, i))
	}

	out, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String("describe-images-default"),
	})
	require.NoError(t, err)
	assert.Len(t, out.ImageDetails, 100, "no maxResults given: must default to the documented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "105 images > default page size of 100: a next page must exist")
}

func TestListImages_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "list-images-default")

	for i := range defaultPageSizeSeed {
		mustPutImage(t, h, "list-images-default", fmt.Sprintf("v%03d", i), fmt.Sprintf(`{"n":%d}`, i))
	}

	out, err := client.ListImages(t.Context(), &ecrsdk.ListImagesInput{
		RepositoryName: aws.String("list-images-default"),
	})
	require.NoError(t, err)
	assert.Len(t, out.ImageIds, 100, "no maxResults given: must default to the documented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "105 images > default page size of 100: a next page must exist")
}

func TestDescribeRepositories_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	for i := range defaultPageSizeSeed {
		mustCreateRepo(t, h, fmt.Sprintf("describe-repos-default-%03d", i))
	}

	out, err := client.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{})
	require.NoError(t, err)
	assert.Len(t, out.Repositories, 100, "no maxResults given: must default to the documented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "105 repos > default page size of 100: a next page must exist")
}

func TestDescribePullThroughCacheRules_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	for i := range defaultPageSizeSeed {
		_, err := client.CreatePullThroughCacheRule(t.Context(), &ecrsdk.CreatePullThroughCacheRuleInput{
			EcrRepositoryPrefix: aws.String(fmt.Sprintf("prefix-%03d", i)),
			UpstreamRegistryUrl: aws.String("public.ecr.aws"),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribePullThroughCacheRules(t.Context(), &ecrsdk.DescribePullThroughCacheRulesInput{})
	require.NoError(t, err)
	assert.Len(t, out.PullThroughCacheRules, 100, "no maxResults given: must default to the documented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "105 rules > default page size of 100: a next page must exist")
}

func TestDescribeRepositoryCreationTemplates_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	for i := range defaultPageSizeSeed {
		_, err := client.CreateRepositoryCreationTemplate(t.Context(), &ecrsdk.CreateRepositoryCreationTemplateInput{
			Prefix:     aws.String(fmt.Sprintf("tmpl-%03d", i)),
			AppliedFor: []types.RCTAppliedFor{types.RCTAppliedForPullThroughCache},
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeRepositoryCreationTemplates(
		t.Context(), &ecrsdk.DescribeRepositoryCreationTemplatesInput{},
	)
	require.NoError(t, err)
	assert.Len(t, out.RepositoryCreationTemplates, 100, "no maxResults given: must default to the documented 100")
	assert.NotEmpty(t, aws.ToString(out.NextToken), "105 templates > default page size of 100: a next page must exist")
}

func TestDescribeImages_ImageStatusFilter_DefaultsToActiveOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "describe-images-status")

	mustPutImage(t, h, "describe-images-status", "active-tag", `{"n":1}`)
	archivedDigest := mustPutImage(t, h, "describe-images-status", "archived-tag", `{"n":2}`)

	_, err := client.UpdateImageStorageClass(t.Context(), &ecrsdk.UpdateImageStorageClassInput{
		RepositoryName:     aws.String("describe-images-status"),
		ImageId:            &types.ImageIdentifier{ImageDigest: aws.String(archivedDigest)},
		TargetStorageClass: types.TargetStorageClassArchive,
	})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String("describe-images-status"),
	})
	require.NoError(t, err)
	require.Len(t, out.ImageDetails, 1, "no filter given: only the ACTIVE image, per documented default")
	assert.Equal(t, "active-tag", out.ImageDetails[0].ImageTags[0])
}

func TestDescribeImages_ImageStatusFilter_Explicit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "describe-images-status-explicit")

	mustPutImage(t, h, "describe-images-status-explicit", "active-tag", `{"n":1}`)
	archivedDigest := mustPutImage(t, h, "describe-images-status-explicit", "archived-tag", `{"n":2}`)

	_, err := client.UpdateImageStorageClass(t.Context(), &ecrsdk.UpdateImageStorageClassInput{
		RepositoryName:     aws.String("describe-images-status-explicit"),
		ImageId:            &types.ImageIdentifier{ImageDigest: aws.String(archivedDigest)},
		TargetStorageClass: types.TargetStorageClassArchive,
	})
	require.NoError(t, err)

	archived, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String("describe-images-status-explicit"),
		Filter:         &types.DescribeImagesFilter{ImageStatus: types.ImageStatusFilterArchived},
	})
	require.NoError(t, err)
	require.Len(t, archived.ImageDetails, 1, "explicit ARCHIVED filter: only the archived image")
	assert.Equal(t, "archived-tag", archived.ImageDetails[0].ImageTags[0])

	all, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String("describe-images-status-explicit"),
		Filter:         &types.DescribeImagesFilter{ImageStatus: types.ImageStatusFilterAny},
	})
	require.NoError(t, err)
	assert.Len(t, all.ImageDetails, 2, "explicit ANY filter: both images regardless of status")
}

func TestListImages_ImageStatusFilter_DefaultsToActiveOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "list-images-status")

	mustPutImage(t, h, "list-images-status", "active-tag", `{"n":1}`)
	archivedDigest := mustPutImage(t, h, "list-images-status", "archived-tag", `{"n":2}`)

	_, err := client.UpdateImageStorageClass(t.Context(), &ecrsdk.UpdateImageStorageClassInput{
		RepositoryName:     aws.String("list-images-status"),
		ImageId:            &types.ImageIdentifier{ImageDigest: aws.String(archivedDigest)},
		TargetStorageClass: types.TargetStorageClassArchive,
	})
	require.NoError(t, err)

	out, err := client.ListImages(t.Context(), &ecrsdk.ListImagesInput{
		RepositoryName: aws.String("list-images-status"),
	})
	require.NoError(t, err)
	require.Len(t, out.ImageIds, 1, "no filter given: only the ACTIVE image, per documented default")
	assert.Equal(t, "active-tag", aws.ToString(out.ImageIds[0].ImageTag))
}
