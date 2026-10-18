package servicecatalog_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPortfolioLifecycle(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	created, err := c.CreatePortfolio(ctx, &sdk.CreatePortfolioInput{
		DisplayName:  aws.String("shared-services"),
		ProviderName: aws.String("platform"),
		Description:  aws.String("d"),
		IdempotencyToken: aws.String(
			token(t),
		),
		Tags: []sctypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	id := created.PortfolioDetail.Id
	assert.True(t, strings.HasPrefix(aws.ToString(id), "port-"))
	assert.Contains(t, aws.ToString(created.PortfolioDetail.ARN), ":catalog:us-east-1:"+testAccount+":portfolio/")

	got, err := c.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: id})
	require.NoError(t, err)
	assert.Equal(t, "shared-services", aws.ToString(got.PortfolioDetail.DisplayName))
	assert.Equal(t, "prod", aws.ToString(got.Tags[0].Value))

	upd, err := c.UpdatePortfolio(ctx, &sdk.UpdatePortfolioInput{
		Id:          id,
		DisplayName: aws.String("renamed"),
		AddTags:     []sctypes.Tag{{Key: aws.String("team"), Value: aws.String("x")}},
		RemoveTags:  []string{"env"},
		Description: aws.String(""),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(upd.PortfolioDetail.DisplayName))
	assert.Empty(t, aws.ToString(upd.PortfolioDetail.Description))
	require.Len(t, upd.Tags, 1)
	assert.Equal(t, "team", aws.ToString(upd.Tags[0].Key))

	list, err := c.ListPortfolios(ctx, &sdk.ListPortfoliosInput{})
	require.NoError(t, err)
	assert.Len(t, list.PortfolioDetails, 1)

	_, err = c.DeletePortfolio(ctx, &sdk.DeletePortfolioInput{Id: id})
	require.NoError(t, err)

	_, err = c.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestPortfolioValidationAndIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
		in   sdk.CreatePortfolioInput
	}{
		{
			name: "empty_name",
			in:   sdk.CreatePortfolioInput{DisplayName: aws.String(""), ProviderName: aws.String("p")},
			code: "InvalidParametersException",
		},
		{
			name: "long_name",
			in: sdk.CreatePortfolioInput{
				DisplayName:  aws.String(strings.Repeat("a", 101)),
				ProviderName: aws.String("p"),
			},
			code: "InvalidParametersException",
		},
		{
			name: "long_provider",
			in: sdk.CreatePortfolioInput{
				DisplayName:  aws.String("n"),
				ProviderName: aws.String(strings.Repeat("a", 51)),
			},
			code: "InvalidParametersException",
		},
		{
			name: "aws_tag", code: "InvalidParametersException",
			in: sdk.CreatePortfolioInput{
				DisplayName: aws.String(
					"n",
				),
				ProviderName: aws.String("p"),
				Tags:         []sctypes.Tag{{Key: aws.String("aws:x"), Value: aws.String("v")}},
			},
		},
		{name: "ok", in: sdk.CreatePortfolioInput{DisplayName: aws.String("n"), ProviderName: aws.String("p")}},
	}

	c := newFixture(t).client(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.in.IdempotencyToken = aws.String(token(t))
			_, err := c.CreatePortfolio(t.Context(), &tc.in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}

	first, err := c.CreatePortfolio(t.Context(), &sdk.CreatePortfolioInput{
		DisplayName: aws.String("idem"), ProviderName: aws.String("p"), IdempotencyToken: aws.String("same"),
	})
	require.NoError(t, err)

	again, err := c.CreatePortfolio(t.Context(), &sdk.CreatePortfolioInput{
		DisplayName: aws.String("idem"), ProviderName: aws.String("p"), IdempotencyToken: aws.String("same"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.PortfolioDetail.Id), aws.ToString(again.PortfolioDetail.Id))

	_, err = c.CreatePortfolio(t.Context(), &sdk.CreatePortfolioInput{
		DisplayName: aws.String("different"), ProviderName: aws.String("p"), IdempotencyToken: aws.String("same"),
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}

func TestPortfolioPagination(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)

	for range 5 {
		_, err := c.CreatePortfolio(t.Context(), &sdk.CreatePortfolioInput{
			DisplayName: aws.String("p"), ProviderName: aws.String("x"), IdempotencyToken: aws.String(token(t)),
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name string
		want []int
		size int32
	}{
		{name: "size_2", size: 2, want: []int{2, 2, 1}},
		{name: "size_5", size: 5, want: []int{5}},
		{name: "default", size: 0, want: []int{5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				got []int
				tok *string
			)

			for {
				out, err := c.ListPortfolios(t.Context(), &sdk.ListPortfoliosInput{PageSize: tc.size, PageToken: tok})
				require.NoError(t, err)

				got = append(got, len(out.PortfolioDetails))

				if tok = out.NextPageToken; tok == nil {
					break
				}
			}

			assert.Equal(t, tc.want, got)
		})
	}

	_, err := c.ListPortfolios(t.Context(), &sdk.ListPortfoliosInput{PageSize: 21})
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}

func TestProductLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	admin, err := c.DescribeProductAsAdmin(ctx, &sdk.DescribeProductAsAdminInput{Id: aws.String(cat.productID)})
	require.NoError(t, err)
	assert.Equal(t, "queue", aws.ToString(admin.ProductViewDetail.ProductViewSummary.Name))
	assert.Equal(t, sctypes.StatusAvailable, admin.ProductViewDetail.Status)
	require.Len(t, admin.ProvisioningArtifactSummaries, 1)
	assert.Equal(
		t,
		templateURL,
		admin.ProvisioningArtifactSummaries[0].ProvisioningArtifactMetadata["LoadTemplateFromURL"],
	)

	byName, err := c.DescribeProductAsAdmin(ctx, &sdk.DescribeProductAsAdminInput{Name: aws.String("queue")})
	require.NoError(t, err)
	assert.Equal(t, cat.productID, aws.ToString(byName.ProductViewDetail.ProductViewSummary.ProductId))

	upd, err := c.UpdateProduct(ctx, &sdk.UpdateProductInput{
		Id: aws.String(cat.productID), Owner: aws.String("team-b"), SupportEmail: aws.String("a@b.c"),
		AddTags: []sctypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)
	assert.Equal(t, "team-b", aws.ToString(upd.ProductViewDetail.ProductViewSummary.Owner))
	assert.Len(t, upd.Tags, 1)

	art, err := c.CreateProvisioningArtifact(ctx, &sdk.CreateProvisioningArtifactInput{
		ProductId: aws.String(cat.productID), IdempotencyToken: aws.String(token(t)),
		Parameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String(
				"v2",
			),
			Description: aws.String("second"),
			Info:        map[string]string{"LoadTemplateFromURL": templateURL},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, sctypes.StatusAvailable, art.Status)
	assert.Equal(t, templateURL, art.Info["TemplateUrl"])

	list, err := c.ListProvisioningArtifacts(
		ctx,
		&sdk.ListProvisioningArtifactsInput{ProductId: aws.String(cat.productID)},
	)
	require.NoError(t, err)
	assert.Len(t, list.ProvisioningArtifactDetails, 2)

	deact, err := c.UpdateProvisioningArtifact(ctx, &sdk.UpdateProvisioningArtifactInput{
		ProductId: aws.String(cat.productID), ProvisioningArtifactId: art.ProvisioningArtifactDetail.Id,
		Active: aws.Bool(false), Guidance: sctypes.ProvisioningArtifactGuidanceDeprecated,
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(deact.ProvisioningArtifactDetail.Active))
	assert.Equal(t, sctypes.ProvisioningArtifactGuidanceDeprecated, deact.ProvisioningArtifactDetail.Guidance)

	end, err := c.DescribeProduct(ctx, &sdk.DescribeProductInput{Id: aws.String(cat.productID)})
	require.NoError(t, err)
	assert.Len(t, end.ProvisioningArtifacts, 1, "inactive versions are hidden from end users")
	require.Len(t, end.LaunchPaths, 1)
	assert.Equal(t, cat.portfolioID, aws.ToString(end.LaunchPaths[0].Id))

	_, err = c.DeleteProvisioningArtifact(ctx, &sdk.DeleteProvisioningArtifactInput{
		ProductId: aws.String(cat.productID), ProvisioningArtifactId: art.ProvisioningArtifactDetail.Id,
	})
	require.NoError(t, err)

	_, err = c.DeleteProduct(ctx, &sdk.DeleteProductInput{Id: aws.String(cat.productID)})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "still in a portfolio")

	_, err = c.DeletePortfolio(ctx, &sdk.DeletePortfolioInput{Id: aws.String(cat.portfolioID)})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "still has a product")

	_, err = c.DisassociateProductFromPortfolio(ctx, &sdk.DisassociateProductFromPortfolioInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
	})
	require.NoError(t, err)

	_, err = c.DeleteProduct(ctx, &sdk.DeleteProductInput{Id: aws.String(cat.productID)})
	require.NoError(t, err)

	_, err = c.DeletePortfolio(ctx, &sdk.DeletePortfolioInput{Id: aws.String(cat.portfolioID)})
	require.NoError(t, err)
}

func TestCreateProductValidation(t *testing.T) {
	t.Parallel()

	good := func() *sctypes.ProvisioningArtifactProperties {
		return &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v1"), Info: map[string]string{"LoadTemplateFromURL": templateURL},
		}
	}

	tests := []struct {
		mut  func(in *sdk.CreateProductInput)
		name string
		code string
	}{
		{name: "ok", mut: func(*sdk.CreateProductInput) {}},
		{
			name: "marketplace_type",
			mut:  func(in *sdk.CreateProductInput) { in.ProductType = sctypes.ProductTypeMarketplace },
			code: "InvalidParametersException",
		},
		{
			name: "missing_url",
			mut:  func(in *sdk.CreateProductInput) { in.ProvisioningArtifactParameters.Info = map[string]string{} },
			code: "InvalidParametersException",
		},
		{
			name: "unreachable_template", code: "InvalidParametersException",
			mut: func(in *sdk.CreateProductInput) {
				in.ProvisioningArtifactParameters.Info = map[string]string{
					"LoadTemplateFromURL": "https://s3.amazonaws.com/x/missing.json",
				}
			},
		},
		{
			name: "skip_validation", code: "",
			mut: func(in *sdk.CreateProductInput) {
				in.ProvisioningArtifactParameters.DisableTemplateValidation = true
				in.ProvisioningArtifactParameters.Info = map[string]string{
					"LoadTemplateFromURL": "https://s3.amazonaws.com/x/missing.json",
				}
			},
		},
		{
			name: "wrong_artifact_type", code: "InvalidParametersException",
			mut: func(in *sdk.CreateProductInput) {
				in.ProvisioningArtifactParameters.Type = sctypes.ProvisioningArtifactTypeExternal
			},
		},
		{
			name: "long_name",
			mut:  func(in *sdk.CreateProductInput) { in.Name = aws.String(strings.Repeat("a", 8200)) },
			code: "InvalidParametersException",
		},
	}

	c := newFixture(t).client(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateProductInput{
				Name: aws.String("p"), Owner: aws.String("o"), ProductType: sctypes.ProductTypeCloudFormationTemplate,
				ProvisioningArtifactParameters: good(), IdempotencyToken: aws.String(token(t)),
			}
			tc.mut(in)

			_, err := c.CreateProduct(t.Context(), in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestSearchProducts(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()

	cat := createCatalog(t, c, templateURL)

	other, err := c.CreateProduct(ctx, &sdk.CreateProductInput{
		Name: aws.String("bucket"), Owner: aws.String("team-b"), Distributor: aws.String("acme"),
		ProductType: sctypes.ProductTypeCloudFormationTemplate, IdempotencyToken: aws.String(token(t)),
		ProvisioningArtifactParameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v1"), Info: map[string]string{"LoadTemplateFromURL": templateURL},
		},
	})
	require.NoError(t, err)

	_, err = c.AssociateProductWithPortfolio(ctx, &sdk.AssociateProductWithPortfolioInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: other.ProductViewDetail.ProductViewSummary.ProductId,
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters map[string][]string
		sort    sctypes.ProductViewSortBy
		order   sctypes.SortOrder
		want    []string
	}{
		{name: "all", want: []string{"bucket", "queue"}, sort: sctypes.ProductViewSortByTitle},
		{
			name:  "title_desc",
			want:  []string{"queue", "bucket"},
			sort:  sctypes.ProductViewSortByTitle,
			order: sctypes.SortOrderDescending,
		},
		{name: "owner", filters: map[string][]string{"Owner": {"team-a"}}, want: []string{"queue"}},
		{name: "full_text", filters: map[string][]string{"FullTextSearch": {"BUCK"}}, want: []string{"bucket"}},
		{
			name:    "type",
			filters: map[string][]string{"ProductType": {"CLOUD_FORMATION_TEMPLATE"}},
			want:    []string{"bucket", "queue"},
			sort:    sctypes.ProductViewSortByTitle,
		},
		{name: "none", filters: map[string][]string{"Owner": {"nobody"}}, want: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, serr := c.SearchProducts(
				ctx,
				&sdk.SearchProductsInput{Filters: tc.filters, SortBy: tc.sort, SortOrder: tc.order},
			)
			require.NoError(t, serr)

			names := []string{}
			for _, s := range out.ProductViewSummaries {
				names = append(names, aws.ToString(s.Name))
			}

			if tc.sort == "" {
				assert.ElementsMatch(t, tc.want, names)
			} else {
				assert.Equal(t, tc.want, names)
			}

			admin, aerr := c.SearchProductsAsAdmin(
				ctx,
				&sdk.SearchProductsAsAdminInput{Filters: tc.filters, PortfolioId: aws.String(cat.portfolioID)},
			)
			require.NoError(t, aerr)
			assert.Len(t, admin.ProductViewDetails, len(tc.want))
		})
	}

	out, err := c.SearchProducts(ctx, &sdk.SearchProductsInput{})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.ProductViewAggregations["Owner"][0].ApproximateCount)
	assert.Len(t, out.ProductViewAggregations["ProductType"], 1)

	_, err = c.SearchProducts(ctx, &sdk.SearchProductsInput{Filters: map[string][]string{"Bogus": {"x"}}})
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}

func TestPrincipalAssociationAndVisibility(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	const role = "arn:aws:iam::123456789012:role/dev"

	_, err := c.AssociatePrincipalWithPortfolio(ctx, &sdk.AssociatePrincipalWithPortfolioInput{
		PortfolioId: aws.String(
			cat.portfolioID,
		),
		PrincipalARN:  aws.String(role),
		PrincipalType: sctypes.PrincipalTypeIam,
	})
	require.NoError(t, err)

	_, err = c.AssociatePrincipalWithPortfolio(ctx, &sdk.AssociatePrincipalWithPortfolioInput{
		PortfolioId: aws.String(cat.portfolioID), PrincipalARN: aws.String("arn:aws:iam::123456789012:role/ops-*"),
		PrincipalType: sctypes.PrincipalTypeIamPattern,
	})
	require.NoError(t, err)

	list, err := c.ListPrincipalsForPortfolio(
		ctx,
		&sdk.ListPrincipalsForPortfolioInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)
	assert.Len(t, list.Principals, 2)

	_, err = c.AssociatePrincipalWithPortfolio(ctx, &sdk.AssociatePrincipalWithPortfolioInput{
		PortfolioId: aws.String(
			cat.portfolioID,
		),
		PrincipalARN:  aws.String("not-an-arn"),
		PrincipalType: sctypes.PrincipalTypeIam,
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	_, err = c.DisassociatePrincipalFromPortfolio(ctx, &sdk.DisassociatePrincipalFromPortfolioInput{
		PortfolioId: aws.String(cat.portfolioID), PrincipalARN: aws.String(role),
	})
	require.NoError(t, err)

	list, err = c.ListPrincipalsForPortfolio(
		ctx,
		&sdk.ListPrincipalsForPortfolioInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)
	assert.Len(t, list.Principals, 1)

	_, err = c.DeletePortfolio(ctx, &sdk.DeletePortfolioInput{Id: aws.String(cat.portfolioID)})
	assert.Equal(t, "ResourceInUseException", apiCode(err))
}

func TestConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		typ    string
		params string
		code   string
	}{
		{name: "launch_role", typ: "LAUNCH", params: `{"RoleArn":"arn:aws:iam::123456789012:role/launch"}`},
		{name: "launch_local", typ: "LAUNCH", params: `{"LocalRoleName":"launch"}`},
		{
			name:   "launch_both",
			typ:    "LAUNCH",
			params: `{"RoleArn":"arn:aws:iam::1:role/a","LocalRoleName":"a"}`,
			code:   "InvalidParametersException",
		},
		{name: "launch_neither", typ: "LAUNCH", params: `{}`, code: "InvalidParametersException"},
		{
			name:   "notification",
			typ:    "NOTIFICATION",
			params: `{"NotificationArns":["arn:aws:sns:us-east-1:123456789012:t"]}`,
		},
		{
			name:   "notification_empty",
			typ:    "NOTIFICATION",
			params: `{"NotificationArns":[]}`,
			code:   "InvalidParametersException",
		},
		{
			name:   "template",
			typ:    "TEMPLATE",
			params: `{"Rules":{"R":{"Assertions":[{"Assert":{"Fn::Equals":[{"Ref":"Env"},"dev"]}}]}}}`,
		},
		{name: "template_no_rules", typ: "TEMPLATE", params: `{}`, code: "InvalidParametersException"},
		{name: "resource_update", typ: "RESOURCE_UPDATE", params: `{"TagUpdateOnProvisionedProduct":"ALLOWED"}`},
		{
			name:   "resource_update_bad",
			typ:    "RESOURCE_UPDATE",
			params: `{"TagUpdateOnProvisionedProduct":"MAYBE"}`,
			code:   "InvalidParametersException",
		},
		{
			name:   "stackset",
			typ:    "STACKSET",
			params: stackSetParams,
		},
		{name: "bad_json", typ: "LAUNCH", params: `nope`, code: "InvalidParametersException"},
		{name: "bad_type", typ: "BOGUS", params: `{}`, code: "InvalidParametersException"},
	}

	f := newFixture(t)
	c := f.client(t)
	cat := createCatalog(t, c, templateURL)

	// Each case needs its own product-portfolio pair so LAUNCH/STACKSET uniqueness does not interfere.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			own := createCatalog(t, c, templateURL)
			out, err := c.CreateConstraint(t.Context(), &sdk.CreateConstraintInput{
				PortfolioId: aws.String(
					own.portfolioID,
				),
				ProductId:        aws.String(own.productID),
				Type:             aws.String(tc.typ),
				Parameters:       aws.String(tc.params),
				IdempotencyToken: aws.String(token(t)),
				Description:      aws.String("d"),
			})
			assert.Equal(t, tc.code, apiCode(err))

			if tc.code != "" {
				return
			}

			assert.Equal(t, sctypes.StatusAvailable, out.Status)
			assert.Equal(t, tc.params, aws.ToString(out.ConstraintParameters))
			assert.Equal(t, tc.typ, aws.ToString(out.ConstraintDetail.Type))
			assert.Equal(t, testAccount, aws.ToString(out.ConstraintDetail.Owner))
		})
	}

	ctx := t.Context()

	first, err := c.CreateConstraint(ctx, &sdk.CreateConstraintInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID), Type: aws.String("LAUNCH"),
		Parameters: aws.String(`{"LocalRoleName":"r"}`), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	_, err = c.CreateConstraint(ctx, &sdk.CreateConstraintInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID), Type: aws.String("LAUNCH"),
		Parameters: aws.String(`{"LocalRoleName":"r2"}`), IdempotencyToken: aws.String(token(t)),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	upd, err := c.UpdateConstraint(ctx, &sdk.UpdateConstraintInput{
		Id:          first.ConstraintDetail.ConstraintId,
		Parameters:  aws.String(`{"LocalRoleName":"r3"}`),
		Description: aws.String("new"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"LocalRoleName":"r3"}`, aws.ToString(upd.ConstraintParameters))

	_, err = c.UpdateConstraint(
		ctx,
		&sdk.UpdateConstraintInput{Id: first.ConstraintDetail.ConstraintId, Parameters: aws.String(`{}`)},
	)
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	list, err := c.ListConstraintsForPortfolio(
		ctx,
		&sdk.ListConstraintsForPortfolioInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)
	assert.Len(t, list.ConstraintDetails, 1)

	_, err = c.DisassociateProductFromPortfolio(ctx, &sdk.DisassociateProductFromPortfolioInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
	})
	assert.Equal(t, "ResourceInUseException", apiCode(err), "a constraint still applies")

	paths, err := c.ListLaunchPaths(ctx, &sdk.ListLaunchPathsInput{ProductId: aws.String(cat.productID)})
	require.NoError(t, err)
	require.Len(t, paths.LaunchPathSummaries, 1)
	require.Len(t, paths.LaunchPathSummaries[0].ConstraintSummaries, 1)
	assert.Equal(t, "LAUNCH", aws.ToString(paths.LaunchPathSummaries[0].ConstraintSummaries[0].Type))

	_, err = c.DeleteConstraint(ctx, &sdk.DeleteConstraintInput{Id: first.ConstraintDetail.ConstraintId})
	require.NoError(t, err)

	_, err = c.DescribeConstraint(ctx, &sdk.DescribeConstraintInput{Id: first.ConstraintDetail.ConstraintId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
