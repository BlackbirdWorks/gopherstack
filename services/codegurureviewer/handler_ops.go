package codegurureviewer

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type (
	empty struct{}

	associationArnInput struct {
		AssociationArn string `json:"-" path:"AssociationArn"`
	}

	codeReviewArnInput struct {
		CodeReviewArn string `json:"-" path:"CodeReviewArn"`
	}

	listRecommendationsInput struct {
		CodeReviewArn string `json:"-" path:"CodeReviewArn"`
		NextToken     string `json:"-"                      query:"NextToken"`
		MaxResults    int32  `json:"-"                      query:"MaxResults"`
	}

	listRecommendationsOutput struct {
		NextToken               string `json:",omitempty"`
		RecommendationSummaries []Recommendation
	}

	tagResourceInput struct {
		Tags        map[string]string `json:"Tags"`
		ResourceArn string            `json:"-"    path:"resourceArn"`
	}

	untagResourceInput struct {
		ResourceArn string   `json:"-" path:"resourceArn"`
		TagKeys     []string `json:"-"                    query:"tagKeys"`
	}

	resourceArnInput struct {
		ResourceArn string `json:"-" path:"resourceArn"`
	}

	listTagsOutput struct {
		Tags map[string]string `json:"Tags"`
	}
)

func createOnce[T any](
	h *Handler, op, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)

	return res, idempotencyErr(err)
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"AssociateRepository":            restroute.Typed(h.associateRepository),
		"DescribeRepositoryAssociation":  restroute.Typed(h.describeRepositoryAssociation),
		"DisassociateRepository":         restroute.Typed(h.disassociateRepository),
		"ListRepositoryAssociations":     restroute.Typed(h.listRepositoryAssociations),
		"CreateCodeReview":               restroute.Typed(h.createCodeReview),
		"DescribeCodeReview":             restroute.Typed(h.describeCodeReview),
		"ListCodeReviews":                restroute.Typed(h.listCodeReviews),
		"ListRecommendations":            restroute.Typed(h.listRecommendations),
		"PutRecommendationFeedback":      restroute.Typed(h.putRecommendationFeedback),
		"DescribeRecommendationFeedback": restroute.Typed(h.describeRecommendationFeedback),
		"ListRecommendationFeedback":     restroute.Typed(h.listRecommendationFeedback),
		"TagResource": restroute.Typed(func(_ context.Context, in *tagResourceInput) (*empty, error) {
			return nil, h.Backend.TagResource(in.ResourceArn, in.Tags)
		}),
		"UntagResource": restroute.Typed(func(_ context.Context, in *untagResourceInput) (*empty, error) {
			return nil, h.Backend.UntagResource(in.ResourceArn, in.TagKeys)
		}),
		"ListTagsForResource": restroute.Typed(func(_ context.Context, in *resourceArnInput) (*listTagsOutput, error) {
			t, err := h.Backend.ListTagsForResource(in.ResourceArn)

			return &listTagsOutput{Tags: t}, err
		}),
	}
}

func (h *Handler) associateRepository(ctx context.Context, in *AssociateRepositoryInput) (*AssociationOutput, error) {
	rec, err := createOnce(h, "AssociateRepository", in.ClientRequestToken, in,
		func(a *associationRecord) string { return a.ARN }, h.Backend.DescribeRepositoryAssociation,
		func() (*associationRecord, error) { return h.Backend.AssociateRepository(ctx, in) })
	if err != nil {
		return nil, err
	}

	return rec.output(), nil
}

func (h *Handler) describeRepositoryAssociation(
	_ context.Context,
	in *associationArnInput,
) (*AssociationOutput, error) {
	rec, err := h.Backend.DescribeRepositoryAssociation(in.AssociationArn)
	if err != nil {
		return nil, err
	}

	return rec.output(), nil
}

func (h *Handler) disassociateRepository(_ context.Context, in *associationArnInput) (*AssociationOutput, error) {
	rec, err := h.Backend.DisassociateRepository(in.AssociationArn)
	if err != nil {
		return nil, err
	}

	return rec.output(), nil
}

func (h *Handler) listRepositoryAssociations(
	_ context.Context,
	in *ListAssociationsInput,
) (*ListAssociationsOutput, error) {
	items, next, err := h.Backend.ListRepositoryAssociations(in)
	if err != nil {
		return nil, err
	}

	return &ListAssociationsOutput{RepositoryAssociationSummaries: items, NextToken: next}, nil
}

func (h *Handler) createCodeReview(ctx context.Context, in *CreateCodeReviewInput) (*CodeReviewOutput, error) {
	rec, err := createOnce(h, "CreateCodeReview", in.ClientRequestToken, in,
		func(r *reviewRecord) string { return r.ARN }, h.Backend.GetCodeReview,
		func() (*reviewRecord, error) { return h.Backend.CreateCodeReview(ctx, in) })
	if err != nil {
		return nil, err
	}

	return &CodeReviewOutput{CodeReview: h.Backend.wireReview(rec)}, nil
}

func (h *Handler) describeCodeReview(_ context.Context, in *codeReviewArnInput) (*CodeReviewOutput, error) {
	rec, err := h.Backend.GetCodeReview(in.CodeReviewArn)
	if err != nil {
		return nil, err
	}

	return &CodeReviewOutput{CodeReview: h.Backend.wireReview(rec)}, nil
}

func (h *Handler) listCodeReviews(_ context.Context, in *ListCodeReviewsInput) (*ListCodeReviewsOutput, error) {
	items, next, err := h.Backend.ListCodeReviews(in)
	if err != nil {
		return nil, err
	}

	return &ListCodeReviewsOutput{CodeReviewSummaries: items, NextToken: next}, nil
}

func (h *Handler) listRecommendations(
	_ context.Context,
	in *listRecommendationsInput,
) (*listRecommendationsOutput, error) {
	items, next, err := h.Backend.ListRecommendations(in.CodeReviewArn, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listRecommendationsOutput{RecommendationSummaries: items, NextToken: next}, nil
}

func (h *Handler) putRecommendationFeedback(ctx context.Context, in *PutFeedbackInput) (*empty, error) {
	return nil, h.Backend.PutRecommendationFeedback(ctx, in)
}

func (h *Handler) describeRecommendationFeedback(
	ctx context.Context, in *DescribeFeedbackInput,
) (*DescribeFeedbackOutput, error) {
	f, err := h.Backend.DescribeRecommendationFeedback(ctx, in)
	if err != nil {
		return nil, err
	}

	return &DescribeFeedbackOutput{RecommendationFeedback: f}, nil
}

func (h *Handler) listRecommendationFeedback(_ context.Context, in *ListFeedbackInput) (*ListFeedbackOutput, error) {
	items, next, err := h.Backend.ListRecommendationFeedback(in)
	if err != nil {
		return nil, err
	}

	return &ListFeedbackOutput{RecommendationFeedbackSummaries: items, NextToken: next}, nil
}
