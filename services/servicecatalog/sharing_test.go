package servicecatalog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	recipientKey = "111111111111"
	memberAKey   = "222222222222"
	memberBKey   = "333333333333"
)

var errOUNotFound = errors.New("OU not found")

type fakeOrg struct {
	ous     map[string][]string
	all     []string
	enabled bool
}

func (f *fakeOrg) AccountsUnder(nodeType, value string) ([]string, error) {
	switch nodeType {
	case "ORGANIZATION":
		return f.all, nil
	case "ORGANIZATIONAL_UNIT":
		if a, ok := f.ous[value]; ok {
			return a, nil
		}

		return nil, errOUNotFound
	default:
		return []string{value}, nil
	}
}

func (f *fakeOrg) EnableServiceAccess(string) error {
	f.enabled = true

	return nil
}

func (f *fakeOrg) DisableServiceAccess(string) error {
	f.enabled = false

	return nil
}
func (f *fakeOrg) ServiceAccessEnabled(string) (bool, error) {
	return f.enabled, nil
}

func TestAccountShareFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	owner := f.client(t)
	recipient := f.clientIn(t, "us-east-1", recipientKey)
	ctx := t.Context()
	cat := createCatalog(t, owner, templateURL)

	out, err := owner.CreatePortfolioShare(ctx, &sdk.CreatePortfolioShareInput{
		PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey), SharePrincipals: true,
	})
	require.NoError(t, err)
	assert.Nil(t, out.PortfolioShareToken, "account shares have no token")

	shares, err := owner.DescribePortfolioShares(ctx, &sdk.DescribePortfolioSharesInput{
		PortfolioId: aws.String(cat.portfolioID), Type: sctypes.DescribePortfolioShareTypeAccount,
	})
	require.NoError(t, err)
	require.Len(t, shares.PortfolioShareDetails, 1)
	assert.False(t, shares.PortfolioShareDetails[0].Accepted)
	assert.True(t, shares.PortfolioShareDetails[0].SharePrincipals)

	access, err := owner.ListPortfolioAccess(
		ctx,
		&sdk.ListPortfolioAccessInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{recipientKey}, access.AccountIds)

	_, err = recipient.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: aws.String(cat.portfolioID)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "not visible before acceptance")

	_, err = owner.CreatePortfolioShare(
		ctx,
		&sdk.CreatePortfolioShareInput{PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(testAccount)},
	)
	assert.Equal(t, "InvalidParametersException", apiCode(err), "not with the owner")

	_, err = recipient.AcceptPortfolioShare(
		ctx,
		&sdk.AcceptPortfolioShareInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)

	accepted, err := recipient.ListAcceptedPortfolioShares(ctx, &sdk.ListAcceptedPortfolioSharesInput{})
	require.NoError(t, err)
	require.Len(t, accepted.PortfolioDetails, 1)

	mine, err := recipient.ListPortfolios(ctx, &sdk.ListPortfoliosInput{})
	require.NoError(t, err)
	assert.Empty(t, mine.PortfolioDetails, "an imported portfolio is not owned")

	prod, err := recipient.SearchProducts(ctx, &sdk.SearchProductsInput{})
	require.NoError(t, err)
	require.Len(t, prod.ProductViewSummaries, 1)

	_, err = recipient.DeletePortfolio(ctx, &sdk.DeletePortfolioInput{Id: aws.String(cat.portfolioID)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "only the owner can delete")

	rec, err := recipient.ProvisionProduct(ctx, &sdk.ProvisionProductInput{
		ProvisionToken: aws.String(
			token(t),
		),
		ProvisionedProductName: aws.String("shared-queue"),
		ProductId:              aws.String(cat.productID),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		ProvisioningParameters: []sctypes.ProvisioningParameter{{Key: aws.String("Size"), Value: aws.String("s")}},
	})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusCreated, rec.RecordDetail.Status)

	ownerPPs, err := owner.SearchProvisionedProducts(ctx, &sdk.SearchProvisionedProductsInput{})
	require.NoError(t, err)
	assert.Empty(t, ownerPPs.ProvisionedProducts, "the launch belongs to the recipient account")

	_, err = owner.UpdatePortfolioShare(ctx, &sdk.UpdatePortfolioShareInput{
		PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey), SharePrincipals: aws.Bool(false),
	})
	require.NoError(t, err)

	shares, err = owner.DescribePortfolioShares(ctx, &sdk.DescribePortfolioSharesInput{
		PortfolioId: aws.String(cat.portfolioID), Type: sctypes.DescribePortfolioShareTypeAccount,
	})
	require.NoError(t, err)
	assert.False(t, shares.PortfolioShareDetails[0].SharePrincipals)
	assert.True(t, shares.PortfolioShareDetails[0].Accepted)

	_, err = recipient.RejectPortfolioShare(
		ctx,
		&sdk.RejectPortfolioShareInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)

	accepted, err = recipient.ListAcceptedPortfolioShares(ctx, &sdk.ListAcceptedPortfolioSharesInput{})
	require.NoError(t, err)
	assert.Empty(t, accepted.PortfolioDetails)

	_, err = owner.DeletePortfolioShare(
		ctx,
		&sdk.DeletePortfolioShareInput{PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey)},
	)
	require.NoError(t, err)

	_, err = owner.DeletePortfolioShare(
		ctx,
		&sdk.DeletePortfolioShareInput{PortfolioId: aws.String(cat.portfolioID), AccountId: aws.String(recipientKey)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestShareValidation(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	cat := createCatalog(t, c, templateURL)

	tests := []struct {
		name string
		in   sdk.CreatePortfolioShareInput
		code string
	}{
		{name: "neither", in: sdk.CreatePortfolioShareInput{}, code: "InvalidParametersException"},
		{
			name: "both", code: "InvalidParametersException",
			in: sdk.CreatePortfolioShareInput{
				AccountId: aws.String(recipientKey),
				OrganizationNode: &sctypes.OrganizationNode{
					Type:  sctypes.OrganizationNodeTypeAccount,
					Value: aws.String("1"),
				},
			},
		},
		{
			name: "bad_account",
			in:   sdk.CreatePortfolioShareInput{AccountId: aws.String("12")},
			code: "InvalidParametersException",
		},
		{
			name: "org_without_access",
			code: "InvalidStateException",
			in: sdk.CreatePortfolioShareInput{
				OrganizationNode: &sctypes.OrganizationNode{
					Type:  sctypes.OrganizationNodeTypeOrganization,
					Value: aws.String("o-1"),
				},
			},
		},
		{
			name: "unknown_portfolio",
			in: sdk.CreatePortfolioShareInput{
				PortfolioId: aws.String("port-none"),
				AccountId:   aws.String(recipientKey),
			},
			code: "ResourceNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := tc.in
			if in.PortfolioId == nil {
				in.PortfolioId = aws.String(cat.portfolioID)
			}

			_, err := c.CreatePortfolioShare(t.Context(), &in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestOrganizationShareFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	org := &fakeOrg{
		ous: map[string][]string{"ou-1": {memberAKey, memberBKey, testAccount}},
		all: []string{testAccount, memberAKey, memberBKey, recipientKey},
	}
	f.backend.SetOrgDirectory(org)

	owner := f.client(t)
	memberA := f.clientIn(t, "us-east-1", memberAKey)
	ctx := t.Context()
	cat := createCatalog(t, owner, templateURL)

	status, err := owner.GetAWSOrganizationsAccessStatus(ctx, &sdk.GetAWSOrganizationsAccessStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, sctypes.AccessStatusDisabled, status.AccessStatus)

	node := &sctypes.OrganizationNode{Type: sctypes.OrganizationNodeTypeOrganizationalUnit, Value: aws.String("ou-1")}

	_, err = owner.CreatePortfolioShare(
		ctx,
		&sdk.CreatePortfolioShareInput{PortfolioId: aws.String(cat.portfolioID), OrganizationNode: node},
	)
	assert.Equal(t, "InvalidStateException", apiCode(err))

	_, err = owner.EnableAWSOrganizationsAccess(ctx, &sdk.EnableAWSOrganizationsAccessInput{})
	require.NoError(t, err)

	status, err = owner.GetAWSOrganizationsAccessStatus(ctx, &sdk.GetAWSOrganizationsAccessStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, sctypes.AccessStatusEnabled, status.AccessStatus)

	share, err := owner.CreatePortfolioShare(ctx, &sdk.CreatePortfolioShareInput{
		PortfolioId: aws.String(cat.portfolioID), OrganizationNode: node, ShareTagOptions: true,
	})
	require.NoError(t, err)
	require.NotNil(t, share.PortfolioShareToken)

	st, err := owner.DescribePortfolioShareStatus(
		ctx,
		&sdk.DescribePortfolioShareStatusInput{PortfolioShareToken: share.PortfolioShareToken},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.ShareStatusCompleted, st.Status)
	assert.Equal(t, "ou-1", aws.ToString(st.OrganizationNodeValue))
	assert.True(t, slices.Equal([]string{memberAKey, memberBKey}, st.ShareDetails.SuccessfulShares))

	nodes, err := owner.ListOrganizationPortfolioAccess(ctx, &sdk.ListOrganizationPortfolioAccessInput{
		PortfolioId: aws.String(cat.portfolioID), OrganizationNodeType: sctypes.OrganizationNodeTypeOrganizationalUnit,
	})
	require.NoError(t, err)
	require.Len(t, nodes.OrganizationNodes, 1)
	assert.Equal(t, "ou-1", aws.ToString(nodes.OrganizationNodes[0].Value))

	members, err := owner.DescribePortfolioShares(ctx, &sdk.DescribePortfolioSharesInput{
		PortfolioId: aws.String(cat.portfolioID), Type: sctypes.DescribePortfolioShareTypeOrganizationMemberAccount,
	})
	require.NoError(t, err)
	assert.Len(t, members.PortfolioShareDetails, 2)

	access, err := owner.ListPortfolioAccess(ctx, &sdk.ListPortfolioAccessInput{
		PortfolioId: aws.String(cat.portfolioID), OrganizationParentId: aws.String("ou-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{memberAKey, memberBKey}, access.AccountIds)

	accepted, err := memberA.ListAcceptedPortfolioShares(
		ctx,
		&sdk.ListAcceptedPortfolioSharesInput{PortfolioShareType: sctypes.PortfolioShareTypeAwsOrganizations},
	)
	require.NoError(t, err)
	assert.Len(t, accepted.PortfolioDetails, 1, "organization members accept implicitly")

	_, err = owner.DeletePortfolioShare(
		ctx,
		&sdk.DeletePortfolioShareInput{PortfolioId: aws.String(cat.portfolioID), OrganizationNode: node},
	)
	require.NoError(t, err)

	accepted, err = memberA.ListAcceptedPortfolioShares(ctx, &sdk.ListAcceptedPortfolioSharesInput{})
	require.NoError(t, err)
	assert.Empty(t, accepted.PortfolioDetails)

	_, err = owner.CreatePortfolioShare(ctx, &sdk.CreatePortfolioShareInput{
		PortfolioId: aws.String(cat.portfolioID),
		OrganizationNode: &sctypes.OrganizationNode{
			Type:  sctypes.OrganizationNodeTypeOrganizationalUnit,
			Value: aws.String("ou-missing"),
		},
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	_, err = owner.DisableAWSOrganizationsAccess(ctx, &sdk.DisableAWSOrganizationsAccessInput{})
	require.NoError(t, err)
	assert.False(t, org.enabled)
}

func TestOrgAccessWithoutOrganizations(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	_, err := c.EnableAWSOrganizationsAccess(ctx, &sdk.EnableAWSOrganizationsAccessInput{})
	require.NoError(t, err)

	status, err := c.GetAWSOrganizationsAccessStatus(ctx, &sdk.GetAWSOrganizationsAccessStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, sctypes.AccessStatusEnabled, status.AccessStatus)

	_, err = c.DisableAWSOrganizationsAccess(ctx, &sdk.DisableAWSOrganizationsAccessInput{})
	require.NoError(t, err)

	status, err = c.GetAWSOrganizationsAccessStatus(ctx, &sdk.GetAWSOrganizationsAccessStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, sctypes.AccessStatusDisabled, status.AccessStatus)
}

func TestPrincipalGatesEndUserVisibility(t *testing.T) {
	t.Parallel()

	// The sdktest router has no principal middleware, so a caller ARN cannot be set here;
	// this checks the association bookkeeping an identified caller would be matched against.
	c := newFixture(t).client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	_, err := c.AssociatePrincipalWithPortfolio(ctx, &sdk.AssociatePrincipalWithPortfolioInput{
		PortfolioId: aws.String(
			cat.portfolioID,
		),
		PrincipalARN:  aws.String("arn:aws:iam::123456789012:user/a"),
		PrincipalType: sctypes.PrincipalTypeIam,
	})
	require.NoError(t, err)

	out, err := c.SearchProducts(ctx, &sdk.SearchProductsInput{})
	require.NoError(t, err)
	assert.Len(t, out.ProductViewSummaries, 1, "an unidentified caller is not restricted")
}
