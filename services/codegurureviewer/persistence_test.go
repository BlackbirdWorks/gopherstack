package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	arn, recs := completedReview(t, src)
	rec := aws.ToString(recs[0].RecommendationId)

	_, err := src.client.PutRecommendationFeedback(t.Context(), &sdk.PutRecommendationFeedbackInput{
		CodeReviewArn: aws.String(
			arn,
		),
		RecommendationId: aws.String(rec),
		Reactions:        []types.Reaction{types.ReactionThumbsUp},
	})
	require.NoError(t, err)

	assocs, err := src.client.ListRepositoryAssociations(t.Context(), &sdk.ListRepositoryAssociationsInput{})
	require.NoError(t, err)
	require.Len(t, assocs.RepositoryAssociationSummaries, 1)

	_, err = src.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: assocs.RepositoryAssociationSummaries[0].AssociationArn, Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	dst.clock.Store(src.clock.Load())
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	d, err := dst.client.DescribeCodeReview(t.Context(), &sdk.DescribeCodeReviewInput{CodeReviewArn: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, types.JobStateCompleted, d.CodeReview.State)

	restored, err := dst.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: aws.String(arn)},
	)
	require.NoError(t, err)
	assert.Equal(t, recs, restored.RecommendationSummaries)

	fb, err := dst.client.DescribeRecommendationFeedback(t.Context(), &sdk.DescribeRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationId: aws.String(rec),
	})
	require.NoError(t, err)
	assert.Equal(t, []types.Reaction{types.ReactionThumbsUp}, fb.RecommendationFeedback.Reactions)

	tags, err := dst.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{
		ResourceArn: assocs.RepositoryAssociationSummaries[0].AssociationArn,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)

	dst.handler.Reset()

	reviews, err := dst.client.ListCodeReviews(
		t.Context(),
		&sdk.ListCodeReviewsInput{Type: types.TypeRepositoryAnalysis},
	)
	require.NoError(t, err)
	assert.Empty(t, reviews.CodeReviewSummaries)
}

func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	require.Error(t, newFixture(t).handler.Restore(t.Context(), []byte("{")))
}
