package ecr_test

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeImages_SDKRoundTrip_StaleCursorResumesPastDeletedItem drives
// DescribeImages through the real aws-sdk-go-v2/service/ecr client to prove
// the filterAndPaginateImages fix (services/ecr/handler_images.go): a
// nextToken naming an image deleted between calls must resume after that
// image's digest position, not silently reset to page one and re-return an
// image the caller already saw.
func TestDescribeImages_SDKRoundTrip_StaleCursorResumesPastDeletedItem(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	client := newTestECRClient(t, h)
	const repo = "describe-images-stale"
	mustCreateRepo(t, h, repo)

	for i := range 3 {
		mustPutManifest(t, h, repo, "v"+string(rune('a'+i)),
			`{"schemaVersion":2,"n":`+string(rune('0'+i))+`}`)
	}

	page1, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String(repo),
		MaxResults:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.ImageDetails, 1)
	require.NotNil(t, page1.NextToken)

	firstSeenDigest := aws.ToString(page1.ImageDetails[0].ImageDigest)
	staleToken := aws.ToString(page1.NextToken)

	decoded, err := base64.StdEncoding.DecodeString(staleToken)
	require.NoError(t, err)

	staleDigest := string(decoded)

	// Delete the image the cursor points at before the next page is
	// fetched -- the deletion trigger this bug class is named for.
	_, err = client.BatchDeleteImage(t.Context(), &ecrsdk.BatchDeleteImageInput{
		RepositoryName: aws.String(repo),
		ImageIds:       []types.ImageIdentifier{{ImageDigest: aws.String(staleDigest)}},
	})
	require.NoError(t, err)

	page2, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
		RepositoryName: aws.String(repo),
		MaxResults:     aws.Int32(10),
		NextToken:      aws.String(staleToken),
	})
	require.NoError(t, err)

	page2Digests := make([]string, 0, len(page2.ImageDetails))
	for _, d := range page2.ImageDetails {
		page2Digests = append(page2Digests, aws.ToString(d.ImageDigest))
	}

	assert.NotContains(t, page2Digests, firstSeenDigest,
		"a stale cursor must not re-return page1's image -- that means pagination reset to page one")
	assert.NotContains(t, page2Digests, staleDigest, "the deleted image itself must not reappear")
	assert.Len(t, page2Digests, 1, "exactly one surviving image remains after the deleted one")
}

// TestListImages_SDKRoundTrip_StaleCursorResumesPastDeletedItem is the
// ListImages analogue: its cursor logic is hand-rolled in handleListImages
// instead of routed through a shared helper, and had the identical Class B
// shape before the fix.
func TestListImages_SDKRoundTrip_StaleCursorResumesPastDeletedItem(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	client := newTestECRClient(t, h)
	const repo = "list-images-stale"
	mustCreateRepo(t, h, repo)

	for i := range 3 {
		mustPutManifest(t, h, repo, "v"+string(rune('a'+i)),
			`{"schemaVersion":2,"n":`+string(rune('0'+i))+`}`)
	}

	// Learn the server's (digest,tag)-sorted order up front, then target
	// the second entry as the deleted, stale-cursor item -- ListImages'
	// nextToken is base64(digest:tag), a composite key, so it can't be
	// derived from a single push's return value the way DescribeImages'
	// plain-digest cursor can above.
	full, err := client.ListImages(t.Context(), &ecrsdk.ListImagesInput{
		RepositoryName: aws.String(repo),
		MaxResults:     aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, full.ImageIds, 3)

	firstSeenDigest := aws.ToString(full.ImageIds[0].ImageDigest)
	staleTarget := full.ImageIds[1]
	staleKey := aws.ToString(staleTarget.ImageDigest) + ":" + aws.ToString(staleTarget.ImageTag)
	staleToken := base64.StdEncoding.EncodeToString([]byte(staleKey))

	_, err = client.BatchDeleteImage(t.Context(), &ecrsdk.BatchDeleteImageInput{
		RepositoryName: aws.String(repo),
		ImageIds:       []types.ImageIdentifier{{ImageDigest: staleTarget.ImageDigest}},
	})
	require.NoError(t, err)

	page2, err := client.ListImages(t.Context(), &ecrsdk.ListImagesInput{
		RepositoryName: aws.String(repo),
		MaxResults:     aws.Int32(10),
		NextToken:      aws.String(staleToken),
	})
	require.NoError(t, err)

	page2Digests := make([]string, 0, len(page2.ImageIds))
	for _, id := range page2.ImageIds {
		page2Digests = append(page2Digests, aws.ToString(id.ImageDigest))
	}

	assert.NotContains(t, page2Digests, firstSeenDigest,
		"a stale cursor must not re-return an earlier-sorted image -- that means pagination reset to page one")
	assert.NotContains(t, page2Digests, aws.ToString(staleTarget.ImageDigest),
		"the deleted image itself must not reappear")
	assert.Len(t, page2Digests, 1, "exactly one surviving image sorts after the deleted one")
}
