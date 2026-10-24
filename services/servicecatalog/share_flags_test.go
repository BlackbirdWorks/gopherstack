package servicecatalog_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

const sharedPattern = "arn:aws:iam::111111111111:role/dev-*"

func recipientCtx(t *testing.T) context.Context {
	t.Helper()

	return awsmeta.Set(t.Context(), &awsmeta.Metadata{
		Account: recipientKey, Region: "us-east-1",
		Principal: &awsmeta.Principal{Arn: "arn:aws:iam::111111111111:role/dev-a", AccountID: recipientKey},
	})
}

type recipientView struct {
	ownerOfTO   string
	principals  int
	tagOptions  int
	portfolioTO int
	searchHits  int
}

func viewOf(
	t *testing.T, f *fixture, recipient *sdk.Client, portfolioID string,
) recipientView {
	t.Helper()

	ctx := t.Context()

	pr, err := recipient.ListPrincipalsForPortfolio(ctx, &sdk.ListPrincipalsForPortfolioInput{
		PortfolioId: aws.String(portfolioID),
	})
	require.NoError(t, err)

	tos, err := recipient.ListTagOptions(ctx, &sdk.ListTagOptionsInput{})
	require.NoError(t, err)

	pf, err := recipient.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: aws.String(portfolioID)})
	require.NoError(t, err)

	found, err := f.backend.SearchProducts(recipientCtx(t), &servicecatalog.SearchProductsInput{})
	require.NoError(t, err)

	v := recipientView{
		principals: len(pr.Principals), tagOptions: len(tos.TagOptionDetails),
		portfolioTO: len(pf.TagOptions), searchHits: len(found.ProductViewSummaries),
	}

	if len(tos.TagOptionDetails) > 0 {
		v.ownerOfTO = aws.ToString(tos.TagOptionDetails[0].Owner)
	}

	return v
}

func TestShareFlagsReachRecipient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		start  [2]bool
		update [2]bool
	}{
		{name: "none then both", start: [2]bool{false, false}, update: [2]bool{true, true}},
		{name: "both then none", start: [2]bool{true, true}, update: [2]bool{false, false}},
		{name: "principals only", start: [2]bool{true, false}, update: [2]bool{false, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			owner := f.client(t)
			recipient := f.clientIn(t, "us-east-1", recipientKey)
			ctx := t.Context()
			cat := createCatalog(t, owner, templateURL)

			to, err := owner.CreateTagOption(ctx, &sdk.CreateTagOptionInput{
				Key: aws.String("env"), Value: aws.String("prod"),
			})
			require.NoError(t, err)

			_, err = owner.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
				ResourceId: aws.String(cat.portfolioID), TagOptionId: to.TagOptionDetail.Id,
			})
			require.NoError(t, err)

			_, err = owner.AssociatePrincipalWithPortfolio(ctx, &sdk.AssociatePrincipalWithPortfolioInput{
				PortfolioId: aws.String(cat.portfolioID), PrincipalARN: aws.String(sharedPattern),
				PrincipalType: sctypes.PrincipalTypeIamPattern,
			})
			require.NoError(t, err)

			_, err = owner.CreatePortfolioShare(ctx, &sdk.CreatePortfolioShareInput{
				PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey),
				SharePrincipals: tt.start[0], ShareTagOptions: tt.start[1],
			})
			require.NoError(t, err)

			_, err = recipient.AcceptPortfolioShare(ctx, &sdk.AcceptPortfolioShareInput{
				PortfolioId: aws.String(cat.portfolioID),
			})
			require.NoError(t, err)

			want := func(flags [2]bool) recipientView {
				v := recipientView{}
				if flags[0] {
					v.principals, v.searchHits = 1, 1
				}

				if flags[1] {
					v.tagOptions, v.portfolioTO, v.ownerOfTO = 1, 1, testAccount
				}

				return v
			}

			assert.Equal(t, want(tt.start), viewOf(t, f, recipient, cat.portfolioID), "after create")

			_, err = owner.UpdatePortfolioShare(ctx, &sdk.UpdatePortfolioShareInput{
				PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey),
				SharePrincipals: aws.Bool(tt.update[0]), ShareTagOptions: aws.Bool(tt.update[1]),
			})
			require.NoError(t, err)

			assert.Equal(t, want(tt.update), viewOf(t, f, recipient, cat.portfolioID), "after update")

			_, err = recipient.RejectPortfolioShare(ctx, &sdk.RejectPortfolioShareInput{
				PortfolioId: aws.String(cat.portfolioID),
			})
			require.NoError(t, err)

			_, err = recipient.DescribeTagOption(ctx, &sdk.DescribeTagOptionInput{Id: to.TagOptionDetail.Id})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err), "rejected share drops the shared tag option")
		})
	}
}
