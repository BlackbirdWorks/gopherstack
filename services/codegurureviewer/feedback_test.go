package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func completedReview(t *testing.T, f *fixture) (string, []types.RecommendationSummary) {
	t.Helper()

	f.commitRepo(t, "app", map[string]string{"main.go": badGo, "keys.py": badPy})
	assoc := f.associateCodeCommit(t, "app")
	cr := f.review(t, "fb", assoc, "main")
	f.settle()

	recs, err := f.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: cr.CodeReview.CodeReviewArn},
	)
	require.NoError(t, err)
	require.NotEmpty(t, recs.RecommendationSummaries)

	return aws.ToString(cr.CodeReview.CodeReviewArn), recs.RecommendationSummaries
}

func TestRecommendationFeedback(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	arn, recs := completedReview(t, f)
	rec := aws.ToString(recs[0].RecommendationId)
	other := aws.ToString(recs[1].RecommendationId)

	put := func(id string, reactions ...types.Reaction) error {
		if reactions == nil {
			reactions = []types.Reaction{}
		}

		_, err := f.client.PutRecommendationFeedback(t.Context(), &sdk.PutRecommendationFeedbackInput{
			CodeReviewArn: aws.String(arn), RecommendationId: aws.String(id), Reactions: reactions,
		})

		return err
	}

	require.NoError(t, put(rec, types.ReactionThumbsUp, types.ReactionThumbsUp))
	require.NoError(t, put(other, types.ReactionThumbsDown))

	got, err := f.client.DescribeRecommendationFeedback(t.Context(), &sdk.DescribeRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationId: aws.String(rec),
	})
	require.NoError(t, err)

	fb := got.RecommendationFeedback
	assert.Equal(t, []types.Reaction{types.ReactionThumbsUp}, fb.Reactions, "duplicate reactions collapse")
	assert.Equal(t, arn, aws.ToString(fb.CodeReviewArn))
	assert.Equal(t, rec, aws.ToString(fb.RecommendationId))
	assert.NotEmpty(t, aws.ToString(fb.UserId))
	assert.NotNil(t, fb.CreatedTimeStamp)

	require.NoError(t, put(rec, types.ReactionThumbsDown))

	got, err = f.client.DescribeRecommendationFeedback(t.Context(), &sdk.DescribeRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationId: aws.String(rec), UserId: fb.UserId,
	})
	require.NoError(t, err)
	assert.Equal(t, []types.Reaction{types.ReactionThumbsDown}, got.RecommendationFeedback.Reactions)

	all, err := f.client.ListRecommendationFeedback(
		t.Context(),
		&sdk.ListRecommendationFeedbackInput{CodeReviewArn: aws.String(arn)},
	)
	require.NoError(t, err)
	assert.Len(t, all.RecommendationFeedbackSummaries, 2)

	one, err := f.client.ListRecommendationFeedback(t.Context(), &sdk.ListRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationIds: []string{other},
	})
	require.NoError(t, err)
	require.Len(t, one.RecommendationFeedbackSummaries, 1)
	assert.Equal(t, other, aws.ToString(one.RecommendationFeedbackSummaries[0].RecommendationId))

	nobody, err := f.client.ListRecommendationFeedback(t.Context(), &sdk.ListRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), UserIds: []string{"someone-else"},
	})
	require.NoError(t, err)
	assert.Empty(t, nobody.RecommendationFeedbackSummaries)

	paged, err := f.client.ListRecommendationFeedback(t.Context(), &sdk.ListRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, paged.RecommendationFeedbackSummaries, 1)
	assert.NotNil(t, paged.NextToken)

	require.NoError(t, put(rec), "an empty reaction list clears the feedback")

	_, err = f.client.DescribeRecommendationFeedback(t.Context(), &sdk.DescribeRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationId: aws.String(rec),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestRecommendationFeedbackErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	arn, recs := completedReview(t, f)
	rec := aws.ToString(recs[0].RecommendationId)

	tests := []struct {
		name      string
		arn       string
		rec       string
		wantCode  string
		reactions []types.Reaction
	}{
		{name: "unknown review", arn: arn + "x", rec: rec, wantCode: "ResourceNotFoundException"},
		{name: "malformed review arn", arn: "nope", rec: rec, wantCode: "ValidationException"},
		{name: "unknown recommendation", arn: arn, rec: "0000", wantCode: "ResourceNotFoundException"},
		{name: "bad reaction", arn: arn, rec: rec, reactions: []types.Reaction{"Meh"}, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reactions := tt.reactions
			if reactions == nil {
				reactions = []types.Reaction{types.ReactionThumbsUp}
			}

			_, err := f.client.PutRecommendationFeedback(t.Context(), &sdk.PutRecommendationFeedbackInput{
				CodeReviewArn: aws.String(tt.arn), RecommendationId: aws.String(tt.rec), Reactions: reactions,
			})
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}

	_, err := f.client.ListRecommendationFeedback(t.Context(), &sdk.ListRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn + "x"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DescribeRecommendationFeedback(t.Context(), &sdk.DescribeRecommendationFeedbackInput{
		CodeReviewArn: aws.String(arn), RecommendationId: aws.String(rec), UserId: aws.String("ghost"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
