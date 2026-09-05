package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	macie2types "github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestSearchResources_FiltersByBucketCriteria drives SearchResources through
// a real SDK client with a SimpleCriterion on S3_BUCKET_NAME. Before this
// fix, SearchResources's backend signature discarded BucketCriteria (`_
// map[string]any`) and always returned an empty MatchingResources list --
// this asserts a real, decoded response containing exactly the matching
// bucket, not just err == nil.
func TestSearchResources_FiltersByBucketCriteria(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:  "000000000000",
		BucketArn:  "arn:aws:s3:::keep-me",
		BucketName: "keep-me",
		Region:     "us-east-1",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID:  "000000000000",
		BucketArn:  "arn:aws:s3:::drop-me",
		BucketName: "drop-me",
		Region:     "us-east-1",
	})

	client := newTestMacie2SDKClient(t, macie2.NewHandler(b))

	out, err := client.SearchResources(t.Context(), &macie2sdk.SearchResourcesInput{
		BucketCriteria: &macie2types.SearchResourcesBucketCriteria{
			Includes: &macie2types.SearchResourcesCriteriaBlock{
				And: []macie2types.SearchResourcesCriteria{
					{
						SimpleCriterion: &macie2types.SearchResourcesSimpleCriterion{
							Comparator: macie2types.SearchResourcesComparatorEq,
							Key:        macie2types.SearchResourcesSimpleCriterionKeyS3BucketName,
							Values:     []string{"keep-me"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.MatchingResources, 1, "only the bucket matching the SimpleCriterion should be returned")
	require.NotNil(t, out.MatchingResources[0].MatchingBucket)
	assert.Equal(t, "keep-me", aws.ToString(out.MatchingResources[0].MatchingBucket.BucketName))
}

// TestSearchResources_ExcludesByBucketCriteria asserts the Excludes half of
// BucketCriteria is honored, the complementary case to Includes above.
func TestSearchResources_ExcludesByBucketCriteria(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID: "111111111111", BucketArn: "arn:aws:s3:::a", BucketName: "a", Region: "us-east-1",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID: "222222222222", BucketArn: "arn:aws:s3:::b", BucketName: "b", Region: "us-east-1",
	})

	client := newTestMacie2SDKClient(t, macie2.NewHandler(b))

	out, err := client.SearchResources(t.Context(), &macie2sdk.SearchResourcesInput{
		BucketCriteria: &macie2types.SearchResourcesBucketCriteria{
			Excludes: &macie2types.SearchResourcesCriteriaBlock{
				And: []macie2types.SearchResourcesCriteria{
					{
						SimpleCriterion: &macie2types.SearchResourcesSimpleCriterion{
							Comparator: macie2types.SearchResourcesComparatorEq,
							Key:        macie2types.SearchResourcesSimpleCriterionKeyAccountId,
							Values:     []string{"111111111111"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.MatchingResources, 1)
	assert.Equal(t, "b", aws.ToString(out.MatchingResources[0].MatchingBucket.BucketName))
}

// TestSearchResources_SortCriteria asserts SortCriteria (a distinct request
// parameter from BucketCriteria, also previously discarded) is honored.
func TestSearchResources_SortCriteria(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID: "000000000000", BucketArn: "arn:aws:s3:::charlie", BucketName: "charlie", Region: "us-east-1",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID: "000000000000", BucketArn: "arn:aws:s3:::alpha", BucketName: "alpha", Region: "us-east-1",
	})
	macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
		AccountID: "000000000000", BucketArn: "arn:aws:s3:::bravo", BucketName: "bravo", Region: "us-east-1",
	})

	client := newTestMacie2SDKClient(t, macie2.NewHandler(b))

	out, err := client.SearchResources(t.Context(), &macie2sdk.SearchResourcesInput{
		SortCriteria: &macie2types.SearchResourcesSortCriteria{
			AttributeName: macie2types.SearchResourcesSortAttributeNameResourceName,
			OrderBy:       macie2types.OrderByDesc,
		},
	})
	require.NoError(t, err)
	require.Len(t, out.MatchingResources, 3)

	names := make([]string, len(out.MatchingResources))
	for i, r := range out.MatchingResources {
		names[i] = aws.ToString(r.MatchingBucket.BucketName)
	}
	assert.Equal(t, []string{"charlie", "bravo", "alpha"}, names, "DESC sort by RESOURCE_NAME")
}

// TestSearchResources_Pagination asserts MaxResults/NextToken are honored --
// the third parameter this op previously discarded.
func TestSearchResources_Pagination(t *testing.T) {
	t.Parallel()

	b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	for _, name := range []string{"bucket-a", "bucket-b", "bucket-c"} {
		macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
			AccountID: "000000000000", BucketArn: "arn:aws:s3:::" + name, BucketName: name, Region: "us-east-1",
		})
	}

	client := newTestMacie2SDKClient(t, macie2.NewHandler(b))

	page1, err := client.SearchResources(t.Context(), &macie2sdk.SearchResourcesInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.MatchingResources, 2, "page size must be capped at MaxResults")
	require.NotNil(t, page1.NextToken, "a further page must be signalled")

	page2, err := client.SearchResources(t.Context(), &macie2sdk.SearchResourcesInput{
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.MatchingResources, 1, "the remaining bucket must be returned on the next page")

	seen := map[string]bool{}
	for _, r := range append(page1.MatchingResources, page2.MatchingResources...) {
		seen[aws.ToString(r.MatchingBucket.BucketName)] = true
	}
	assert.Len(t, seen, 3, "all 3 buckets must be seen exactly once across both pages")
}
