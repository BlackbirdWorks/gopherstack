package servicecatalog_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func provision(
	t *testing.T,
	c *sdk.Client,
	cat catalog,
	name string,
	params map[string]string,
	tags ...sctypes.Tag,
) *sdk.ProvisionProductOutput {
	t.Helper()

	in := &sdk.ProvisionProductInput{
		ProvisionToken: aws.String(
			token(t),
		),
		ProvisionedProductName: aws.String(name),
		ProductId:              aws.String(cat.productID),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		Tags:                   tags,
	}

	for k, v := range params {
		in.ProvisioningParameters = append(
			in.ProvisioningParameters,
			sctypes.ProvisioningParameter{Key: aws.String(k), Value: aws.String(v)},
		)
	}

	out, err := c.ProvisionProduct(t.Context(), in)
	require.NoError(t, err)

	return out
}

func TestProvisionLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)

	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	_, err := c.UpdateProduct(ctx, &sdk.UpdateProductInput{
		Id: aws.String(cat.productID), AddTags: []sctypes.Tag{{Key: aws.String("cost"), Value: aws.String("c1")}},
	})
	require.NoError(t, err)

	out := provision(
		t,
		c,
		cat,
		"my-queue",
		map[string]string{"Size": "big"},
		sctypes.Tag{Key: aws.String("owner"), Value: aws.String("me")},
	)
	rec := out.RecordDetail
	assert.Equal(t, sctypes.RecordStatusCreated, rec.Status)
	assert.Equal(t, "PROVISION_PRODUCT", aws.ToString(rec.RecordType))
	assert.Equal(t, "CFN_STACK", aws.ToString(rec.ProvisionedProductType))
	assert.Equal(t, cat.portfolioID, aws.ToString(rec.PathId))

	ppID := aws.ToString(rec.ProvisionedProductId)
	assert.Contains(t, ppID, "pp-")

	req := f.engine.lastRequest(t)
	assert.Equal(t, "SC-"+testAccount+"-"+ppID, req.Name)
	assert.Equal(t, map[string]string{"Size": "big"}, req.Parameters)
	assert.Equal(t, "me", req.Tags["owner"])
	assert.Equal(t, "c1", req.Tags["cost"], "product tags are applied")
	assert.Contains(t, req.Tags["aws:servicecatalog:productArn"], ":product/"+cat.productID)
	assert.Contains(t, req.Tags["aws:servicecatalog:portfolioArn"], ":portfolio/"+cat.portfolioID)

	desc, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: rec.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusInProgress, desc.RecordDetail.Status)
	assert.Empty(t, desc.RecordOutputs)

	pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Name: aws.String("my-queue")})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusUnderChange, pp.ProvisionedProductDetail.Status)

	f.clock.advance(time.Minute)

	desc, err = c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: rec.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusSucceeded, desc.RecordDetail.Status)
	require.Len(t, desc.RecordOutputs, 1)
	assert.Equal(t, "QueueName", aws.ToString(desc.RecordOutputs[0].OutputKey))
	assert.Equal(t, "q-1", aws.ToString(desc.RecordOutputs[0].OutputValue))
	assert.NotEmpty(t, desc.RecordDetail.RecordTags)

	pp, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: aws.String(ppID)})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)
	assert.Equal(
		t,
		aws.ToString(rec.RecordId),
		aws.ToString(pp.ProvisionedProductDetail.LastSuccessfulProvisioningRecordId),
	)
	assert.Equal(t, cat.artifactID, aws.ToString(pp.ProvisionedProductDetail.ProvisioningArtifactId))

	outs, err := c.GetProvisionedProductOutputs(
		ctx,
		&sdk.GetProvisionedProductOutputsInput{ProvisionedProductName: aws.String("my-queue")},
	)
	require.NoError(t, err)
	require.Len(t, outs.Outputs, 1)

	filtered, err := c.GetProvisionedProductOutputs(ctx, &sdk.GetProvisionedProductOutputsInput{
		ProvisionedProductId: aws.String(ppID), OutputKeys: []string{"Nope"},
	})
	require.NoError(t, err)
	assert.Empty(t, filtered.Outputs)

	hist, err := c.ListRecordHistory(ctx, &sdk.ListRecordHistoryInput{
		SearchFilter: &sctypes.ListRecordHistorySearchFilter{
			Key:   aws.String("provisionedproduct"),
			Value: aws.String("my-queue"),
		},
	})
	require.NoError(t, err)
	require.Len(t, hist.RecordDetails, 1)

	scan, err := c.ScanProvisionedProducts(ctx, &sdk.ScanProvisionedProductsInput{})
	require.NoError(t, err)
	assert.Len(t, scan.ProvisionedProducts, 1)

	v2, err := c.CreateProvisioningArtifact(ctx, &sdk.CreateProvisioningArtifactInput{
		ProductId: aws.String(cat.productID), IdempotencyToken: aws.String(token(t)),
		Parameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v2"), Info: map[string]string{"LoadTemplateFromURL": templateURL},
		},
	})
	require.NoError(t, err)

	upd, err := c.UpdateProvisionedProduct(ctx, &sdk.UpdateProvisionedProductInput{
		UpdateToken: aws.String(token(t)), ProvisionedProductId: aws.String(ppID), ProductId: aws.String(cat.productID),
		ProvisioningArtifactId: v2.ProvisioningArtifactDetail.Id,
		ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
			{Key: aws.String("Size"), UsePreviousValue: true}, {Key: aws.String("Env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_PROVISIONED_PRODUCT", aws.ToString(upd.RecordDetail.RecordType))

	ureq := f.engine.lastRequest(t)
	assert.True(t, ureq.Update)
	assert.Equal(t, map[string]string{"Size": "big", "Env": "prod"}, ureq.Parameters)

	f.clock.advance(time.Minute)

	pp, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: aws.String(ppID)})
	require.NoError(t, err)
	assert.Equal(t, v2.ProvisioningArtifactDetail.Id, pp.ProvisionedProductDetail.ProvisioningArtifactId)
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)

	term, err := c.TerminateProvisionedProduct(ctx, &sdk.TerminateProvisionedProductInput{
		TerminateToken: aws.String(token(t)), ProvisionedProductId: aws.String(ppID),
	})
	require.NoError(t, err)
	assert.Equal(t, "TERMINATE_PROVISIONED_PRODUCT", aws.ToString(term.RecordDetail.RecordType))

	f.clock.advance(time.Minute)

	_, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: aws.String(ppID)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	final, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: term.RecordDetail.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusSucceeded, final.RecordDetail.Status)

	hist, err = c.ListRecordHistory(ctx, &sdk.ListRecordHistoryInput{})
	require.NoError(t, err)
	assert.Len(t, hist.RecordDetails, 3, "provision, update and terminate records outlive the product")
	assert.Equal(t, "TERMINATE_PROVISIONED_PRODUCT", aws.ToString(hist.RecordDetails[0].RecordType), "newest first")
}

func TestProvisionValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	cat := createCatalog(t, c, templateURL)
	provision(t, c, cat, "taken", map[string]string{"Size": "s"})

	tests := []struct {
		mut  func(in *sdk.ProvisionProductInput)
		name string
		code string
	}{
		{name: "ok", mut: func(*sdk.ProvisionProductInput) {}},
		{
			name: "duplicate_name",
			mut:  func(in *sdk.ProvisionProductInput) { in.ProvisionedProductName = aws.String("taken") },
			code: "DuplicateResourceException",
		},
		{
			name: "bad_name",
			mut:  func(in *sdk.ProvisionProductInput) { in.ProvisionedProductName = aws.String("-bad name") },
			code: "InvalidParametersException",
		},
		{
			name: "unknown_product",
			mut:  func(in *sdk.ProvisionProductInput) { in.ProductId = aws.String("prod-missing") },
			code: "ResourceNotFoundException",
		},
		{
			name: "unknown_artifact",
			mut:  func(in *sdk.ProvisionProductInput) { in.ProvisioningArtifactId = aws.String("pa-missing") },
			code: "ResourceNotFoundException",
		},
		{
			name: "missing_required_param",
			mut:  func(in *sdk.ProvisionProductInput) { in.ProvisioningParameters = nil },
			code: "InvalidParametersException",
		},
		{
			name: "unknown_param", code: "InvalidParametersException",
			mut: func(in *sdk.ProvisionProductInput) {
				in.ProvisioningParameters = append(
					in.ProvisioningParameters,
					sctypes.ProvisioningParameter{Key: aws.String("Nope"), Value: aws.String("x")},
				)
			},
		},
		{
			name: "disallowed_value", code: "InvalidParametersException",
			mut: func(in *sdk.ProvisionProductInput) {
				in.ProvisioningParameters = append(
					in.ProvisioningParameters,
					sctypes.ProvisioningParameter{Key: aws.String("Env"), Value: aws.String("staging")},
				)
			},
		},
		{
			name: "wrong_path",
			mut:  func(in *sdk.ProvisionProductInput) { in.PathId = aws.String("port-other") },
			code: "InvalidParametersException",
		},
		{name: "path_by_name", mut: func(in *sdk.ProvisionProductInput) { in.PathName = aws.String("platform") }},
		{
			name: "too_many_notifications", code: "InvalidParametersException",
			mut: func(in *sdk.ProvisionProductInput) { in.NotificationArns = []string{"a", "b", "c", "d", "e", "f"} },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.ProvisionProductInput{
				ProvisionToken:         aws.String(token(t)),
				ProvisionedProductName: aws.String("pp-" + tc.name),
				ProductId:              aws.String(cat.productID),
				ProvisioningArtifactId: aws.String(cat.artifactID),
				ProvisioningParameters: []sctypes.ProvisioningParameter{
					{Key: aws.String("Size"), Value: aws.String("s")},
				},
			}
			tc.mut(in)

			_, err := c.ProvisionProduct(t.Context(), in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestProvisionMultiplePaths(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	second, err := c.CreatePortfolio(ctx, &sdk.CreatePortfolioInput{
		DisplayName: aws.String("second"), ProviderName: aws.String("p"), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	_, err = c.AssociateProductWithPortfolio(ctx, &sdk.AssociateProductWithPortfolioInput{
		PortfolioId: second.PortfolioDetail.Id, ProductId: aws.String(cat.productID),
	})
	require.NoError(t, err)

	in := &sdk.ProvisionProductInput{
		ProvisionToken: aws.String(
			token(t),
		),
		ProvisionedProductName: aws.String("p1"),
		ProductId:              aws.String(cat.productID),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		ProvisioningParameters: []sctypes.ProvisioningParameter{{Key: aws.String("Size"), Value: aws.String("s")}},
	}

	_, err = c.ProvisionProduct(ctx, in)
	assert.Equal(t, "InvalidParametersException", apiCode(err), "an ambiguous path must be named")

	in.ProvisionToken, in.PathId = aws.String(token(t)), second.PortfolioDetail.Id

	out, err := c.ProvisionProduct(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(second.PortfolioDetail.Id), aws.ToString(out.RecordDetail.PathId))

	paths, err := c.ListLaunchPaths(ctx, &sdk.ListLaunchPathsInput{ProductId: aws.String(cat.productID)})
	require.NoError(t, err)
	assert.Len(t, paths.LaunchPathSummaries, 2)
}

func TestProvisionFailure(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()

	cat := createCatalog(t, c, failURL)
	out := provision(t, c, cat, "doomed", nil)

	rec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: out.RecordDetail.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusFailed, rec.RecordDetail.Status)
	require.NotEmpty(t, rec.RecordDetail.RecordErrors)
	assert.Contains(t, aws.ToString(rec.RecordDetail.RecordErrors[0].Description), "FAIL")

	pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Name: aws.String("doomed")})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusError, pp.ProvisionedProductDetail.Status)
	assert.Contains(t, aws.ToString(pp.ProvisionedProductDetail.StatusMessage), "FAIL")

	_, err = c.TerminateProvisionedProduct(ctx, &sdk.TerminateProvisionedProductInput{
		TerminateToken: aws.String(token(t)), ProvisionedProductName: aws.String("doomed"), IgnoreErrors: true,
	})
	require.NoError(t, err)

	_, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Name: aws.String("doomed")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestUpdateFailureTaintsProduct(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	out := provision(t, c, cat, "web", map[string]string{"Size": "s"})
	ppID := out.RecordDetail.ProvisionedProductId

	bad, err := c.CreateProvisioningArtifact(ctx, &sdk.CreateProvisioningArtifactInput{
		ProductId:        aws.String(cat.productID),
		IdempotencyToken: aws.String(token(t)),
		Parameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("bad"),
			Info: map[string]string{"LoadTemplateFromURL": failURL},
		},
	})
	require.NoError(t, err)

	upd, err := c.UpdateProvisionedProduct(ctx, &sdk.UpdateProvisionedProductInput{
		UpdateToken: aws.String(
			token(t),
		),
		ProvisionedProductId:   ppID,
		ProvisioningArtifactId: bad.ProvisioningArtifactDetail.Id,
	})
	require.NoError(t, err)

	rec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: upd.RecordDetail.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusFailed, rec.RecordDetail.Status)

	pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: ppID})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusTainted, pp.ProvisionedProductDetail.Status)
}

func TestProvisionIdempotency(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	cat := createCatalog(t, c, templateURL)

	in := &sdk.ProvisionProductInput{
		ProvisionToken: aws.String(
			"once",
		),
		ProvisionedProductName: aws.String("only"),
		ProductId:              aws.String(cat.productID),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		ProvisioningParameters: []sctypes.ProvisioningParameter{{Key: aws.String("Size"), Value: aws.String("s")}},
	}

	first, err := c.ProvisionProduct(t.Context(), in)
	require.NoError(t, err)

	again, err := c.ProvisionProduct(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.RecordDetail.RecordId), aws.ToString(again.RecordDetail.RecordId))

	list, err := c.SearchProvisionedProducts(t.Context(), &sdk.SearchProvisionedProductsInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, list.TotalResultsCount)
}

func TestLaunchConstraintsApply(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		check  func(t *testing.T, f *fixture, c *sdk.Client, cat catalog)
		name   string
		typ    string
		params string
	}{
		{
			name: "launch_role", typ: "LAUNCH", params: `{"LocalRoleName":"sc-launch"}`,
			check: func(t *testing.T, f *fixture, c *sdk.Client, cat catalog) {
				t.Helper()

				out := provision(t, c, cat, "with-role", map[string]string{"Size": "s"})
				assert.Equal(
					t,
					"arn:aws:iam::"+testAccount+":role/sc-launch",
					aws.ToString(out.RecordDetail.LaunchRoleArn),
				)
				assert.Equal(t, "arn:aws:iam::"+testAccount+":role/sc-launch", f.engine.lastRequest(t).RoleARN)
			},
		},
		{
			name:   "notification",
			typ:    "NOTIFICATION",
			params: `{"NotificationArns":["arn:aws:sns:us-east-1:123456789012:t"]}`,
			check: func(t *testing.T, f *fixture, c *sdk.Client, cat catalog) {
				t.Helper()

				provision(t, c, cat, "with-notify", map[string]string{"Size": "s"})
				assert.Equal(
					t,
					[]string{"arn:aws:sns:us-east-1:123456789012:t"},
					f.engine.lastRequest(t).NotificationARNs,
				)
			},
		},
		{
			name:   "template_rule_blocks",
			typ:    "TEMPLATE",
			params: noProdRule,
			check: func(t *testing.T, _ *fixture, c *sdk.Client, cat catalog) {
				t.Helper()

				_, err := c.ProvisionProduct(t.Context(), &sdk.ProvisionProductInput{
					ProvisionToken: aws.String(
						token(t),
					),
					ProvisionedProductName: aws.String("prodq"),
					ProductId:              aws.String(cat.productID),
					ProvisioningArtifactId: aws.String(cat.artifactID),
					ProvisioningParameters: []sctypes.ProvisioningParameter{
						{
							Key:   aws.String("Size"),
							Value: aws.String("s"),
						},
						{Key: aws.String("Env"), Value: aws.String("prod")},
					},
				})
				require.Error(t, err)
				assert.Equal(t, "InvalidParametersException", apiCode(err))
				assert.Contains(t, err.Error(), "prod is not allowed")

				provision(t, c, cat, "devq", map[string]string{"Size": "s"})
			},
		},
		{
			name:   "resource_update_blocks_tags",
			typ:    "RESOURCE_UPDATE",
			params: `{"TagUpdateOnProvisionedProduct":"NOT_ALLOWED"}`,
			check: func(t *testing.T, _ *fixture, c *sdk.Client, cat catalog) {
				t.Helper()

				out := provision(t, c, cat, "tagged", map[string]string{"Size": "s"})

				_, err := c.UpdateProvisionedProduct(t.Context(), &sdk.UpdateProvisionedProductInput{
					UpdateToken:          aws.String(token(t)),
					ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
					ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
						{Key: aws.String("Size"), UsePreviousValue: true},
					},
					Tags: []sctypes.Tag{{Key: aws.String("new"), Value: aws.String("tag")}},
				})
				assert.Equal(t, "InvalidParametersException", apiCode(err))
			},
		},
		{
			name:   "stackset_unsupported",
			typ:    "STACKSET",
			params: stackSetParams,
			check: func(t *testing.T, _ *fixture, c *sdk.Client, cat catalog) {
				t.Helper()

				_, err := c.ProvisionProduct(t.Context(), &sdk.ProvisionProductInput{
					ProvisionToken: aws.String(
						token(t),
					),
					ProvisionedProductName: aws.String("ss"),
					ProductId:              aws.String(cat.productID),
					ProvisioningArtifactId: aws.String(cat.artifactID),
				})
				assert.Equal(t, "OperationNotSupportedException", apiCode(err))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			c := f.client(t)
			cat := createCatalog(t, c, templateURL)
			_, err := c.CreateConstraint(ctx, &sdk.CreateConstraintInput{
				PortfolioId: aws.String(
					cat.portfolioID,
				),
				ProductId:        aws.String(cat.productID),
				Type:             aws.String(tc.typ),
				Parameters:       aws.String(tc.params),
				IdempotencyToken: aws.String(token(t)),
			})
			require.NoError(t, err)

			tc.check(t, f, c, cat)
		})
	}
}

func TestDescribeProvisioningParameters(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	opt, err := c.CreateTagOption(ctx, &sdk.CreateTagOptionInput{Key: aws.String("env"), Value: aws.String("dev")})
	require.NoError(t, err)

	_, err = c.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
		ResourceId: aws.String(cat.portfolioID), TagOptionId: opt.TagOptionDetail.Id,
	})
	require.NoError(t, err)

	_, err = c.CreateConstraint(ctx, &sdk.CreateConstraintInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID), Type: aws.String("TEMPLATE"),
		Parameters:       aws.String(`{"Rules":{"R":{"Assertions":[{"Assert":{"Fn::Equals":["a","a"]}}]}}}`),
		IdempotencyToken: aws.String(token(t)), Description: aws.String("rules"),
	})
	require.NoError(t, err)

	out, err := c.DescribeProvisioningParameters(ctx, &sdk.DescribeProvisioningParametersInput{
		ProductId: aws.String(cat.productID), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	require.NoError(t, err)

	keys := map[string]sctypes.ProvisioningArtifactParameter{}
	for _, p := range out.ProvisioningArtifactParameters {
		keys[aws.ToString(p.ParameterKey)] = p
	}

	assert.Equal(t, "dev", aws.ToString(keys["Env"].DefaultValue))
	assert.Equal(t, []string{"dev", "prod"}, keys["Env"].ParameterConstraints.AllowedValues)
	assert.Contains(t, keys, "Size")
	require.Len(t, out.ProvisioningArtifactOutputKeys, 1)
	assert.Equal(t, "QueueName", aws.ToString(out.ProvisioningArtifactOutputKeys[0].Key))
	require.Len(t, out.TagOptions, 1)
	assert.Equal(t, []string{"dev"}, out.TagOptions[0].Values)
	require.Len(t, out.ConstraintSummaries, 1)
	assert.Equal(t, "rules", aws.ToString(out.ConstraintSummaries[0].Description))
	require.Len(t, out.UsageInstructions, 1)
	assert.Equal(t, "rules", aws.ToString(out.UsageInstructions[0].Type))

	art, err := c.DescribeProvisioningArtifact(ctx, &sdk.DescribeProvisioningArtifactInput{
		ProductId: aws.String(
			cat.productID,
		),
		ProvisioningArtifactName:              aws.String("v1"),
		IncludeProvisioningArtifactParameters: true,
	})
	require.NoError(t, err)
	assert.Len(t, art.ProvisioningArtifactParameters, 2)
	assert.Equal(t, templateURL, art.Info["TemplateUrl"])
}

func TestSearchProvisionedProductsQueries(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	cat := createCatalog(t, c, templateURL)

	provision(
		t,
		c,
		cat,
		"alpha",
		map[string]string{"Size": "s"},
		sctypes.Tag{Key: aws.String("team"), Value: aws.String("x")},
	)
	provision(t, c, cat, "beta", map[string]string{"Size": "s"})

	tests := []struct {
		name  string
		query string
		order sctypes.SortOrder
		want  []string
	}{
		{name: "all", query: "", want: []string{"alpha", "beta"}},
		{name: "free_text", query: "alph", want: []string{"alpha"}},
		{name: "name_field", query: "name:beta", want: []string{"beta"}},
		{name: "status_field", query: "status:AVAILABLE", want: []string{"alpha", "beta"}},
		{name: "product_field", query: "productId:" + cat.productID + " name:alpha", want: []string{"alpha"}},
		{name: "tag_field", query: "tags:team=x", want: []string{"alpha"}},
		{name: "no_match", query: "name:gamma", want: []string{}},
		{name: "descending", query: "", order: sctypes.SortOrderDescending, want: []string{"beta", "alpha"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.SearchProvisionedProductsInput{SortBy: aws.String("name"), SortOrder: tc.order}
			if tc.query != "" {
				in.Filters = map[string][]string{"SearchQuery": {tc.query}}
			}

			out, err := c.SearchProvisionedProducts(t.Context(), in)
			require.NoError(t, err)

			names := []string{}
			for _, p := range out.ProvisionedProducts {
				names = append(names, aws.ToString(p.Name))
			}

			assert.Equal(t, tc.want, names)
			assert.EqualValues(t, len(tc.want), out.TotalResultsCount)
		})
	}

	_, err := c.SearchProvisionedProducts(
		t.Context(),
		&sdk.SearchProvisionedProductsInput{Filters: map[string][]string{"Bogus": {"x"}}},
	)
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}

func TestExternalProductEngineWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     sctypes.EngineWorkflowStatus
		wantRecord sctypes.RecordStatus
		wantPP     sctypes.ProvisionedProductStatus
	}{
		{
			name:       "succeeds",
			status:     sctypes.EngineWorkflowStatusSucceeded,
			wantRecord: sctypes.RecordStatusSucceeded,
			wantPP:     sctypes.ProvisionedProductStatusAvailable,
		},
		{
			name:       "fails",
			status:     sctypes.EngineWorkflowStatusFailed,
			wantRecord: sctypes.RecordStatusFailed,
			wantPP:     sctypes.ProvisionedProductStatusError,
		},
	}

	c := newFixture(t).client(t)
	ctx := t.Context()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pf, err := c.CreatePortfolio(ctx, &sdk.CreatePortfolioInput{
				DisplayName: aws.String(
					"ext-" + tc.name,
				),
				ProviderName:     aws.String("p"),
				IdempotencyToken: aws.String(token(t)),
			})
			require.NoError(t, err)

			pr, err := c.CreateProduct(ctx, &sdk.CreateProductInput{
				Name: aws.String(
					"tf",
				),
				Owner:            aws.String("o"),
				ProductType:      sctypes.ProductTypeExternal,
				IdempotencyToken: aws.String(token(t)),
				ProvisioningArtifactParameters: &sctypes.ProvisioningArtifactProperties{
					Name: aws.String(
						"v1",
					),
					Type: sctypes.ProvisioningArtifactTypeExternal,
					Info: map[string]string{"k": "v"},
				},
			})
			require.NoError(t, err)

			prodID := pr.ProductViewDetail.ProductViewSummary.ProductId

			_, err = c.AssociateProductWithPortfolio(
				ctx,
				&sdk.AssociateProductWithPortfolioInput{PortfolioId: pf.PortfolioDetail.Id, ProductId: prodID},
			)
			require.NoError(t, err)

			out, err := c.ProvisionProduct(ctx, &sdk.ProvisionProductInput{
				ProvisionToken: aws.String(token(t)), ProvisionedProductName: aws.String("ext-" + tc.name),
				ProductId: prodID, ProvisioningArtifactId: pr.ProvisioningArtifactDetail.Id,
			})
			require.NoError(t, err)

			recID := out.RecordDetail.RecordId

			rec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: recID})
			require.NoError(t, err)
			assert.Equal(t, sctypes.RecordStatusInProgress, rec.RecordDetail.Status, "waits for the engine")

			_, err = c.NotifyProvisionProductEngineWorkflowResult(
				ctx,
				&sdk.NotifyProvisionProductEngineWorkflowResultInput{
					IdempotencyToken: aws.String(
						token(t),
					),
					RecordId:      recID,
					Status:        tc.status,
					WorkflowToken: aws.String("wrong"),
				},
			)
			assert.Equal(t, "InvalidParametersException", apiCode(err))

			// The emulator has no channel to hand the token to an engine; it is wf-<RecordId>.
			_, err = c.NotifyProvisionProductEngineWorkflowResult(
				ctx,
				&sdk.NotifyProvisionProductEngineWorkflowResultInput{
					IdempotencyToken: aws.String(token(t)), RecordId: recID, Status: tc.status,
					WorkflowToken: aws.String("wf-" + aws.ToString(recID)),
					FailureReason: aws.String("boom"),
					Outputs:       []sctypes.RecordOutput{{OutputKey: aws.String("k"), OutputValue: aws.String("v")}},
				},
			)
			require.NoError(t, err)

			rec, err = c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: recID})
			require.NoError(t, err)
			assert.Equal(t, tc.wantRecord, rec.RecordDetail.Status)

			pp, err := c.DescribeProvisionedProduct(
				ctx,
				&sdk.DescribeProvisionedProductInput{Name: aws.String("ext-" + tc.name)},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPP, pp.ProvisionedProductDetail.Status)
		})
	}
}
