package app

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	scSetTemplate = `{"Parameters":{"Env":{"Type":"String","Default":"dev"}},` +
		`"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
	scDashboardTemplate = `{"Resources":{"Dash":{"Type":"AWS::CloudWatch::Dashboard","Properties":` +
		`{"DashboardName":"sc-dash","DashboardBody":"{\"widgets\":[]}"}}}}`
	scSetConstraint = `{"Version":"1.0","Properties":{"AccountList":["111111111111","222222222222"],` +
		`"RegionList":["us-east-1"],"AdminRole":"arn:aws:iam::000000000000:role/admin","ExecutionRole":"exec"}}`
)

type scCatalog struct {
	sc        *servicecatalog.Client
	portfolio *string
	product   *string
	artifact  *string
}

func newSCCatalog(t *testing.T, fx *sfnFixture, template string) scCatalog {
	t.Helper()

	ctx := t.Context()
	s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	sc := servicecatalog.NewFromConfig(fx.cfg)

	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("sc-ext")})
	require.NoError(t, err)

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("sc-ext"), Key: aws.String("t.json"), Body: strings.NewReader(template),
	})
	require.NoError(t, err)

	pf, err := sc.CreatePortfolio(ctx, &servicecatalog.CreatePortfolioInput{
		DisplayName: aws.String("ext"), ProviderName: aws.String("it"), IdempotencyToken: aws.String("pf-" + t.Name()),
	})
	require.NoError(t, err)

	pr, err := sc.CreateProduct(ctx, &servicecatalog.CreateProductInput{
		Name: aws.String("ext"), Owner: aws.String("me"), ProductType: sctypes.ProductTypeCloudFormationTemplate,
		IdempotencyToken: aws.String("pr-" + t.Name()),
		ProvisioningArtifactParameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v1"),
			Info: map[string]string{"LoadTemplateFromURL": "https://s3.us-east-1.amazonaws.com/sc-ext/t.json"},
		},
	})
	require.NoError(t, err)

	prodID := pr.ProductViewDetail.ProductViewSummary.ProductId
	_, err = sc.AssociateProductWithPortfolio(
		ctx, &servicecatalog.AssociateProductWithPortfolioInput{PortfolioId: pf.PortfolioDetail.Id, ProductId: prodID},
	)
	require.NoError(t, err)

	return scCatalog{
		sc: sc, portfolio: pf.PortfolioDetail.Id, product: prodID, artifact: pr.ProvisioningArtifactDetail.Id,
	}
}

func (c scCatalog) constrain(t *testing.T, typ, params string) {
	t.Helper()

	_, err := c.sc.CreateConstraint(t.Context(), &servicecatalog.CreateConstraintInput{
		PortfolioId: c.portfolio, ProductId: c.product, Type: aws.String(typ), Parameters: aws.String(params),
		IdempotencyToken: aws.String(typ + "-" + t.Name()),
	})
	require.NoError(t, err)
}

func (c scCatalog) provision(
	t *testing.T, name string, prefs *sctypes.ProvisioningPreferences,
) (*servicecatalog.ProvisionProductOutput, error) {
	t.Helper()

	return c.sc.ProvisionProduct(t.Context(), &servicecatalog.ProvisionProductInput{
		ProvisionToken: aws.String("tok-" + name + t.Name()), ProvisionedProductName: aws.String(name),
		ProductId: c.product, ProvisioningArtifactId: c.artifact, ProvisioningPreferences: prefs,
	})
}

func TestServiceCatalogStackSetProductsUseCloudFormationStackSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefs        *sctypes.ProvisioningPreferences
		name         string
		wantInstance int
		wantTol      int32
	}{
		{name: "all_targets", wantInstance: 2},
		{
			name: "narrowed_with_preferences",
			prefs: &sctypes.ProvisioningPreferences{
				StackSetAccounts: []string{"222222222222"}, StackSetFailureToleranceCount: aws.Int32(1),
				StackSetMaxConcurrencyCount: aws.Int32(2),
			},
			wantInstance: 1, wantTol: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cfn := cloudformation.NewFromConfig(fx.cfg)
			ctx := t.Context()
			cat := newSCCatalog(t, fx, scSetTemplate)
			cat.constrain(t, "STACKSET", scSetConstraint)

			out, err := cat.provision(t, "ss-product", tc.prefs)
			require.NoError(t, err)

			rec, err := cat.sc.DescribeRecord(ctx, &servicecatalog.DescribeRecordInput{Id: out.RecordDetail.RecordId})
			require.NoError(t, err)
			require.Equal(
				t,
				sctypes.RecordStatusSucceeded,
				rec.RecordDetail.Status,
				"%+v",
				rec.RecordDetail.RecordErrors,
			)

			pp, err := cat.sc.DescribeProvisionedProduct(
				ctx, &servicecatalog.DescribeProvisionedProductInput{Name: aws.String("ss-product")},
			)
			require.NoError(t, err)
			assert.Equal(t, "CFN_STACKSET", aws.ToString(pp.ProvisionedProductDetail.Type))

			setName := "SC-000000000000-" + aws.ToString(pp.ProvisionedProductDetail.Id)
			ss, err := cfn.DescribeStackSet(
				ctx,
				&cloudformation.DescribeStackSetInput{StackSetName: aws.String(setName)},
			)
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:iam::000000000000:role/admin", aws.ToString(ss.StackSet.AdministrationRoleARN))
			assert.Equal(t, "exec", aws.ToString(ss.StackSet.ExecutionRoleName))

			cfnInsts, err := cfn.ListStackInstances(
				ctx,
				&cloudformation.ListStackInstancesInput{StackSetName: aws.String(setName)},
			)
			require.NoError(t, err)
			assert.Len(t, cfnInsts.Summaries, tc.wantInstance)

			scInsts, err := cat.sc.ListStackInstancesForProvisionedProduct(
				ctx, &servicecatalog.ListStackInstancesForProvisionedProductInput{
					ProvisionedProductId: pp.ProvisionedProductDetail.Id,
				},
			)
			require.NoError(t, err)
			require.Len(t, scInsts.StackInstances, tc.wantInstance)

			for _, i := range scInsts.StackInstances {
				assert.Equal(t, sctypes.StackInstanceStatusCurrent, i.StackInstanceStatus)
				assert.Equal(t, "us-east-1", aws.ToString(i.Region))
			}

			ops, err := cfn.ListStackSetOperations(
				ctx,
				&cloudformation.ListStackSetOperationsInput{StackSetName: aws.String(setName)},
			)
			require.NoError(t, err)
			require.NotEmpty(t, ops.Summaries)

			op, err := cfn.DescribeStackSetOperation(ctx, &cloudformation.DescribeStackSetOperationInput{
				StackSetName: aws.String(setName), OperationId: ops.Summaries[0].OperationId,
			})
			require.NoError(t, err)
			assert.Equal(t, cfntypes.StackSetOperationStatusSucceeded, op.StackSetOperation.Status)

			if tc.prefs != nil {
				require.NotNil(t, op.StackSetOperation.OperationPreferences)
				assert.Equal(
					t,
					tc.wantTol,
					aws.ToInt32(op.StackSetOperation.OperationPreferences.FailureToleranceCount),
				)
			}

			_, err = cat.sc.TerminateProvisionedProduct(ctx, &servicecatalog.TerminateProvisionedProductInput{
				TerminateToken: aws.String("end-" + t.Name()), ProvisionedProductId: pp.ProvisionedProductDetail.Id,
			})
			require.NoError(t, err)

			_, err = cfn.DescribeStackSet(ctx, &cloudformation.DescribeStackSetInput{StackSetName: aws.String(setName)})
			require.Error(t, err, "terminating the product deleted its stack set")
		})
	}
}

func TestServiceCatalogReportsCloudWatchDashboards(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cat := newSCCatalog(t, fx, scDashboardTemplate)

	_, err := cat.provision(t, "dash-product", nil)
	require.NoError(t, err)

	pp, err := cat.sc.DescribeProvisionedProduct(
		t.Context(), &servicecatalog.DescribeProvisionedProductInput{Name: aws.String("dash-product")},
	)
	require.NoError(t, err)
	require.Len(t, pp.CloudWatchDashboards, 1)
	assert.Equal(t, "sc-dash", aws.ToString(pp.CloudWatchDashboards[0].Name))
}

func TestServiceCatalogTagOptionsTagTheStack(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cfn := cloudformation.NewFromConfig(fx.cfg)
	cat := newSCCatalog(t, fx, scSetTemplate)

	opt, err := cat.sc.CreateTagOption(t.Context(), &servicecatalog.CreateTagOptionInput{
		Key: aws.String("cost-center"), Value: aws.String("cc-42"),
	})
	require.NoError(t, err)

	_, err = cat.sc.AssociateTagOptionWithResource(t.Context(), &servicecatalog.AssociateTagOptionWithResourceInput{
		ResourceId: cat.portfolio, TagOptionId: opt.TagOptionDetail.Id,
	})
	require.NoError(t, err)

	_, err = cat.provision(t, "tagged", nil)
	require.NoError(t, err)

	stacks, err := cfn.DescribeStacks(t.Context(), &cloudformation.DescribeStacksInput{})
	require.NoError(t, err)

	tags := map[string]string{}

	for _, s := range stacks.Stacks {
		if strings.HasPrefix(aws.ToString(s.StackName), "SC-") {
			for _, tg := range s.Tags {
				tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
		}
	}

	assert.Equal(t, "cc-42", tags["cost-center"])
	assert.Contains(t, tags, "aws:servicecatalog:provisionedProductArn")
}

func TestServiceCatalogLaunchRoleMustTrustServiceCatalog(t *testing.T) {
	t.Parallel()

	const trust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":%q},` +
		`"Action":"sts:AssumeRole"}]}`

	tests := []struct {
		name      string
		principal string
		wantErr   bool
	}{
		{name: "trusted", principal: "servicecatalog.amazonaws.com"},
		{name: "wrong_principal", principal: "lambda.amazonaws.com", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, true)
			iamc := iam.NewFromConfig(fx.cfg)

			role, err := iamc.CreateRole(t.Context(), &iam.CreateRoleInput{
				RoleName:                 aws.String("launch"),
				AssumeRolePolicyDocument: aws.String(strings.Replace(trust, "%q", `"`+tc.principal+`"`, 1)),
			})
			require.NoError(t, err)

			cat := newSCCatalog(t, fx, scSetTemplate)
			cat.constrain(t, "LAUNCH", `{"RoleArn":"`+aws.ToString(role.Role.Arn)+`"}`)

			_, err = cat.provision(t, "launch-product", nil)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParametersException")
				assert.Contains(t, err.Error(), "cannot assume the launch role")

				return
			}

			require.NoError(t, err)
		})
	}
}
