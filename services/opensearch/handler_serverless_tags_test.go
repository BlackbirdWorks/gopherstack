package opensearch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tagsOf reduces a ListTagsForResourceOutput's Tags to a plain map for
// order-independent comparison.
func tagsOf(tags []aosstypes.Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	return out
}

// TestServerless_RealSDKClient_Tagging drives ListTagsForResource,
// TagResource and UntagResource through the real opensearchserverless
// client (gopherstack-3cijh). Before this fix these three ops were absent
// from serverlessJSONRPCOps(), so a real client got an
// "operation ... not implemented" ValidationException for all three.
func TestServerless_RealSDKClient_Tagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *opensearchserverless.Client, resourceArn string)
		name string
	}{
		{
			name: "create_collection_with_tags_then_list",
			run: func(t *testing.T, client *opensearchserverless.Client, resourceArn string) {
				t.Helper()

				out, err := client.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceArn),
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "test"}, tagsOf(out.Tags))
			},
		},
		{
			name: "tag_resource_adds_and_overwrites",
			run: func(t *testing.T, client *opensearchserverless.Client, resourceArn string) {
				t.Helper()

				_, err := client.TagResource(t.Context(), &opensearchserverless.TagResourceInput{
					ResourceArn: aws.String(resourceArn),
					Tags: []aosstypes.Tag{
						{Key: aws.String("env"), Value: aws.String("prod")},
						{Key: aws.String("owner"), Value: aws.String("search-team")},
					},
				})
				require.NoError(t, err)

				out, err := client.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceArn),
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "prod", "owner": "search-team"}, tagsOf(out.Tags))
			},
		},
		{
			name: "untag_resource_removes_by_key",
			run: func(t *testing.T, client *opensearchserverless.Client, resourceArn string) {
				t.Helper()

				_, err := client.UntagResource(t.Context(), &opensearchserverless.UntagResourceInput{
					ResourceArn: aws.String(resourceArn),
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				out, err := client.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
					ResourceArn: aws.String(resourceArn),
				})
				require.NoError(t, err)
				assert.Empty(t, out.Tags)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := testServerlessHandler(t)
			client := newTestServerlessClient(t, h)

			created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
				Name: aws.String("tag-scenario"),
				Type: aosstypes.CollectionTypeVectorsearch,
				Tags: []aosstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
			})
			require.NoError(t, err)

			tt.run(t, client, aws.ToString(created.CreateCollectionDetail.Arn))
		})
	}
}

// TestServerless_RealSDKClient_ListTagsForResource_NotFound confirms an
// unknown ARN maps to the real AOSS ResourceNotFoundException, asserted via
// the smithy APIError interface's ErrorCode() -- not just a generic error.
func TestServerless_RealSDKClient_ListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	_, err := client.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:aoss:us-east-1:000000000000:collection/does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestServerless_RealSDKClient_TagResource_TooManyTags confirms TagResource
// enforces the real 50-tag-per-resource cap with the exception the real
// deserializer actually declares for this op, ServiceQuotaExceededException
// (opensearchserverless v1.34.4 deserializers.go
// awsAwsjson10_deserializeOpErrorTagResource).
func TestServerless_RealSDKClient_TagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("too-many-tags"),
		Type: aosstypes.CollectionTypeVectorsearch,
	})
	require.NoError(t, err)

	tags := make([]aosstypes.Tag, 0, 51)
	for i := range 51 {
		tags = append(tags, aosstypes.Tag{Key: aws.String(fmt.Sprintf("k%d", i)), Value: aws.String("v")})
	}

	_, err = client.TagResource(t.Context(), &opensearchserverless.TagResourceInput{
		ResourceArn: created.CreateCollectionDetail.Arn,
		Tags:        tags,
	})
	require.Error(t, err)

	var quotaErr *aosstypes.ServiceQuotaExceededException
	require.ErrorAs(t, err, &quotaErr)
}

// TestServerless_RealSDKClient_TagResource_CollectionWireStillHasNoTags is
// the de493780d control: tagging a collection through the real
// TagResource op must not resurrect the tags key on CreateCollection or
// BatchGetCollection's raw wire body -- those still marshal through
// wireServerlessCollection, which the real CollectionDetail/
// CreateCollectionDetail shapes require (opensearchserverless v1.34.4
// types.go:115,349). Tags reach a real client only through
// ListTagsForResource.
func TestServerless_RealSDKClient_TagResource_CollectionWireStillHasNoTags(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client, capture := newCapturingTestServerlessClient(t, h)

	created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("wire-control"),
		Type: aosstypes.CollectionTypeVectorsearch,
	})
	require.NoError(t, err)
	assert.NotContains(t, string(capture.body), `"tags"`)

	resourceArn := aws.ToString(created.CreateCollectionDetail.Arn)
	id := aws.ToString(created.CreateCollectionDetail.Id)

	_, err = client.TagResource(t.Context(), &opensearchserverless.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        []aosstypes.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	_, err = client.BatchGetCollection(t.Context(), &opensearchserverless.BatchGetCollectionInput{
		Ids: []string{id},
	})
	require.NoError(t, err)
	assert.NotContains(t, string(capture.body), `"tags"`,
		"BatchGetCollection must not leak tags even after TagResource -- de493780d's wire twin must still hold")

	out, err := client.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform"}, tagsOf(out.Tags))
}

// TestServerless_RealSDKClient_TagsSurviveSnapshotRestore proves the tags
// applied via TagResource are the same persisted ServerlessCollection.Tags
// field a snapshot/restore round-trip already carries (gopherstack-3cijh:
// storage pre-existed, only the three dispatch ops were missing).
func TestServerless_RealSDKClient_TagsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("persist-tags"),
		Type: aosstypes.CollectionTypeVectorsearch,
	})
	require.NoError(t, err)
	resourceArn := aws.ToString(created.CreateCollectionDetail.Arn)

	_, err = client.TagResource(t.Context(), &opensearchserverless.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        []aosstypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	h2, snap := snapshotHandler(t, h)
	require.NoError(t, h2.Backend.Restore(t.Context(), snap))

	client2 := newTestServerlessClient(t, h2)

	out, err := client2.ListTagsForResource(t.Context(), &opensearchserverless.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tagsOf(out.Tags))
}
