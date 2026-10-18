package codegurureviewer

import (
	"context"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	reactionThumbsUp   = "ThumbsUp"
	reactionThumbsDown = "ThumbsDown"
)

// PutFeedbackInput is the PutRecommendationFeedback request.
type PutFeedbackInput struct {
	CodeReviewArn    string   `json:"CodeReviewArn"`
	RecommendationID string   `json:"RecommendationId"`
	Reactions        []string `json:"Reactions"`
}

// DescribeFeedbackInput is the DescribeRecommendationFeedback request.
type DescribeFeedbackInput struct {
	CodeReviewArn    string `json:"-" path:"CodeReviewArn"`
	RecommendationID string `json:"-"                      query:"RecommendationId"`
	UserID           string `json:"-"                      query:"UserId"`
}

// DescribeFeedbackOutput is the DescribeRecommendationFeedback response.
type DescribeFeedbackOutput struct {
	RecommendationFeedback *RecommendationFeedback `json:"RecommendationFeedback"`
}

// ListFeedbackInput is the ListRecommendationFeedback request.
type ListFeedbackInput struct {
	CodeReviewArn     string   `json:"-" path:"CodeReviewArn"`
	NextToken         string   `json:"-"                      query:"NextToken"`
	UserIDs           []string `json:"-"                      query:"UserIds"`
	RecommendationIDs []string `json:"-"                      query:"RecommendationIds"`
	MaxResults        int32    `json:"-"                      query:"MaxResults"`
}

// ListFeedbackOutput is the ListRecommendationFeedback response.
type ListFeedbackOutput struct {
	NextToken                       string `json:",omitempty"`
	RecommendationFeedbackSummaries []RecommendationFeedbackSummary
}

func (r *reviewRecord) hasRecommendation(id string) bool {
	return slices.ContainsFunc(r.Recommendations, func(x Recommendation) bool { return x.RecommendationID == id })
}

// PutRecommendationFeedback records the caller's reactions to a recommendation; an empty list clears them.
func (b *InMemoryBackend) PutRecommendationFeedback(ctx context.Context, in *PutFeedbackInput) error {
	if in.RecommendationID == "" || len(in.RecommendationID) > maxTokenLen {
		return validationErr("RecommendationId must be 1-%d characters", maxTokenLen)
	}

	if err := validateEnums("Reactions", in.Reactions, []string{reactionThumbsUp, reactionThumbsDown}); err != nil {
		return err
	}

	user := callerUserID(ctx, b.accountID)

	b.mu.Lock("PutRecommendationFeedback")
	defer b.mu.Unlock()

	r, err := b.reviewLocked(in.CodeReviewArn)
	if err != nil {
		return err
	}

	if !r.hasRecommendation(in.RecommendationID) {
		return resourceNotFoundErr("recommendation %q does not exist in the code review", in.RecommendationID)
	}

	key := in.CodeReviewArn + "|" + in.RecommendationID + "|" + user
	reactions := dedupe(in.Reactions)

	if len(reactions) == 0 {
		b.feedback.Delete(key)

		return nil
	}

	now := awstime.NewEpochTime(b.now())
	if old, ok := b.feedback.Get(key); ok {
		old.Reactions, old.Updated = reactions, now

		return nil
	}

	b.feedback.Put(&feedbackRecord{
		CodeReviewARN: in.CodeReviewArn, RecommendationID: in.RecommendationID, UserID: user,
		Reactions: reactions, Created: now, Updated: now,
	})

	return nil
}

func dedupe(in []string) []string {
	out := make([]string, 0, len(in))

	for _, v := range in {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}

	return out
}

// DescribeRecommendationFeedback returns one user's feedback; the caller's when UserId is omitted.
func (b *InMemoryBackend) DescribeRecommendationFeedback(
	ctx context.Context, in *DescribeFeedbackInput,
) (*RecommendationFeedback, error) {
	if in.RecommendationID == "" {
		return nil, validationErr("RecommendationId is required")
	}

	user := in.UserID
	if user == "" {
		user = callerUserID(ctx, b.accountID)
	}

	b.mu.RLock("DescribeRecommendationFeedback")
	defer b.mu.RUnlock()

	if _, err := b.reviewLocked(in.CodeReviewArn); err != nil {
		return nil, err
	}

	f, ok := b.feedback.Get(in.CodeReviewArn + "|" + in.RecommendationID + "|" + user)
	if !ok {
		return nil, resourceNotFoundErr("no feedback from %q for recommendation %q", user, in.RecommendationID)
	}

	return &RecommendationFeedback{
		CodeReviewArn: f.CodeReviewARN, RecommendationID: f.RecommendationID, UserID: f.UserID,
		Reactions: slices.Clone(f.Reactions), CreatedTimeStamp: f.Created, LastUpdatedTimeStamp: f.Updated,
	}, nil
}

// ListRecommendationFeedback lists the feedback given on a review, filtered by user and recommendation.
func (b *InMemoryBackend) ListRecommendationFeedback(
	in *ListFeedbackInput,
) ([]RecommendationFeedbackSummary, string, error) {
	b.mu.RLock("ListRecommendationFeedback")
	defer b.mu.RUnlock()

	if _, err := b.reviewLocked(in.CodeReviewArn); err != nil {
		return nil, "", err
	}

	var recs []*feedbackRecord

	for _, f := range b.feedback.All() {
		if f.CodeReviewARN == in.CodeReviewArn && matchAny(in.UserIDs, f.UserID) &&
			matchAny(in.RecommendationIDs, f.RecommendationID) {
			recs = append(recs, f)
		}
	}

	slices.SortFunc(recs, func(x, y *feedbackRecord) int {
		return strings.Compare(x.RecommendationID+"|"+x.UserID, y.RecommendationID+"|"+y.UserID)
	})

	pg, err := restroute.Paginate(recs, in.NextToken, int(in.MaxResults), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, "", validationErr("%v", err)
	}

	out := make([]RecommendationFeedbackSummary, 0, len(pg.Data))
	for _, f := range pg.Data {
		out = append(out, RecommendationFeedbackSummary{
			RecommendationID: f.RecommendationID, UserID: f.UserID, Reactions: slices.Clone(f.Reactions),
		})
	}

	return out, pg.Next, nil
}
