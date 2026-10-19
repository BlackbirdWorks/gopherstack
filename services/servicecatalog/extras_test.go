package servicecatalog_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

func TestTagOptions(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	opt, err := c.CreateTagOption(ctx, &sdk.CreateTagOptionInput{Key: aws.String("env"), Value: aws.String("dev")})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(opt.TagOptionDetail.Active))

	_, err = c.CreateTagOption(ctx, &sdk.CreateTagOptionInput{Key: aws.String("env"), Value: aws.String("dev")})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = c.CreateTagOption(ctx, &sdk.CreateTagOptionInput{Key: aws.String("aws:x"), Value: aws.String("v")})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	prod, err := c.CreateTagOption(ctx, &sdk.CreateTagOptionInput{Key: aws.String("env"), Value: aws.String("prod")})
	require.NoError(t, err)

	for _, resource := range []string{cat.portfolioID, cat.productID} {
		_, err = c.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
			ResourceId: aws.String(resource), TagOptionId: opt.TagOptionDetail.Id,
		})
		require.NoError(t, err)
	}

	_, err = c.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
		ResourceId: aws.String(cat.portfolioID), TagOptionId: prod.TagOptionDetail.Id,
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err), "one value per key per resource")

	_, err = c.AssociateTagOptionWithResource(ctx, &sdk.AssociateTagOptionWithResourceInput{
		ResourceId: aws.String("nothing"), TagOptionId: opt.TagOptionDetail.Id,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	resources, err := c.ListResourcesForTagOption(
		ctx,
		&sdk.ListResourcesForTagOptionInput{TagOptionId: opt.TagOptionDetail.Id},
	)
	require.NoError(t, err)
	assert.Len(t, resources.ResourceDetails, 2)

	onlyProducts, err := c.ListResourcesForTagOption(
		ctx,
		&sdk.ListResourcesForTagOptionInput{TagOptionId: opt.TagOptionDetail.Id, ResourceType: aws.String("Product")},
	)
	require.NoError(t, err)
	require.Len(t, onlyProducts.ResourceDetails, 1)
	assert.Equal(t, cat.productID, aws.ToString(onlyProducts.ResourceDetails[0].Id))

	desc, err := c.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: aws.String(cat.portfolioID)})
	require.NoError(t, err)
	assert.Len(t, desc.TagOptions, 1)

	_, err = c.DeleteTagOption(ctx, &sdk.DeleteTagOptionInput{Id: opt.TagOptionDetail.Id})
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	upd, err := c.UpdateTagOption(
		ctx,
		&sdk.UpdateTagOptionInput{Id: prod.TagOptionDetail.Id, Active: aws.Bool(false), Value: aws.String("qa")},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(upd.TagOptionDetail.Active))
	assert.Equal(t, "qa", aws.ToString(upd.TagOptionDetail.Value))

	active, err := c.ListTagOptions(
		ctx,
		&sdk.ListTagOptionsInput{Filters: &sctypes.ListTagOptionsFilters{Active: aws.Bool(true)}},
	)
	require.NoError(t, err)
	assert.Len(t, active.TagOptionDetails, 1)

	byKey, err := c.ListTagOptions(
		ctx,
		&sdk.ListTagOptionsInput{Filters: &sctypes.ListTagOptionsFilters{Key: aws.String("env")}},
	)
	require.NoError(t, err)
	assert.Len(t, byKey.TagOptionDetails, 2)

	for _, resource := range []string{cat.portfolioID, cat.productID} {
		_, err = c.DisassociateTagOptionFromResource(ctx, &sdk.DisassociateTagOptionFromResourceInput{
			ResourceId: aws.String(resource), TagOptionId: opt.TagOptionDetail.Id,
		})
		require.NoError(t, err)
	}

	_, err = c.DeleteTagOption(ctx, &sdk.DeleteTagOptionInput{Id: opt.TagOptionDetail.Id})
	require.NoError(t, err)
}

type fakeBudgets map[string]bool

func (f fakeBudgets) BudgetExists(_ context.Context, name string) bool { return f[name] }

func TestBudgets(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetBudgetChecker(fakeBudgets{"monthly": true})

	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	_, err := c.AssociateBudgetWithResource(
		ctx,
		&sdk.AssociateBudgetWithResourceInput{BudgetName: aws.String("ghost"), ResourceId: aws.String(cat.productID)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = c.AssociateBudgetWithResource(
		ctx,
		&sdk.AssociateBudgetWithResourceInput{BudgetName: aws.String("monthly"), ResourceId: aws.String(cat.productID)},
	)
	require.NoError(t, err)

	_, err = c.AssociateBudgetWithResource(
		ctx,
		&sdk.AssociateBudgetWithResourceInput{BudgetName: aws.String("monthly"), ResourceId: aws.String(cat.productID)},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	list, err := c.ListBudgetsForResource(ctx, &sdk.ListBudgetsForResourceInput{ResourceId: aws.String(cat.productID)})
	require.NoError(t, err)
	require.Len(t, list.Budgets, 1)

	admin, err := c.DescribeProductAsAdmin(ctx, &sdk.DescribeProductAsAdminInput{Id: aws.String(cat.productID)})
	require.NoError(t, err)
	assert.Len(t, admin.Budgets, 1)

	end, err := c.DescribeProduct(ctx, &sdk.DescribeProductInput{Id: aws.String(cat.productID)})
	require.NoError(t, err)
	assert.Len(t, end.Budgets, 1)

	_, err = c.DisassociateBudgetFromResource(
		ctx,
		&sdk.DisassociateBudgetFromResourceInput{
			BudgetName: aws.String("monthly"),
			ResourceId: aws.String(cat.productID),
		},
	)
	require.NoError(t, err)

	_, err = c.DisassociateBudgetFromResource(
		ctx,
		&sdk.DisassociateBudgetFromResourceInput{
			BudgetName: aws.String("monthly"),
			ResourceId: aws.String(cat.productID),
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

var errThrottled = errors.New("throttled")

type fakeRunner struct {
	docs  map[string]bool
	calls []map[string][]string
	names []string
	mu    sync.Mutex
	fail  bool
}

func (f *fakeRunner) DocumentExists(_ context.Context, name string) bool { return f.docs[name] }

func (f *fakeRunner) StartAutomation(_ context.Context, doc, _ string, params map[string][]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fail {
		return "", errThrottled
	}

	f.names = append(f.names, doc)
	f.calls = append(f.calls, params)

	return "auto-1", nil
}

func TestServiceActions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	runner := &fakeRunner{docs: map[string]bool{"AWS-RestartEC2Instance": true}}
	f.backend.SetAutomationRunner(runner)

	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	def := map[string]string{
		"Name": "AWS-RestartEC2Instance", "Version": "1", "AssumeRole": "arn:aws:iam::123456789012:role/ssm",
		"Parameters": `[{"Name":"InstanceId","Type":"TARGET"}]`,
	}

	bad, err := c.CreateServiceAction(ctx, &sdk.CreateServiceActionInput{
		Name: aws.String(
			"bad",
		),
		DefinitionType:   sctypes.ServiceActionDefinitionTypeSsmAutomation,
		Definition:       map[string]string{"Name": "x"},
		IdempotencyToken: aws.String(token(t)),
	})
	require.Error(t, err)
	assert.Nil(t, bad)
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	created, err := c.CreateServiceAction(ctx, &sdk.CreateServiceActionInput{
		Name: aws.String("restart"), DefinitionType: sctypes.ServiceActionDefinitionTypeSsmAutomation, Definition: def,
		Description: aws.String("restarts"), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	actID := created.ServiceActionDetail.ServiceActionSummary.Id
	assert.Equal(t, def, created.ServiceActionDetail.Definition)

	upd, err := c.UpdateServiceAction(
		ctx,
		&sdk.UpdateServiceActionInput{Id: actID, Name: aws.String("restart-2"), Description: aws.String("d2")},
	)
	require.NoError(t, err)
	assert.Equal(t, "restart-2", aws.ToString(upd.ServiceActionDetail.ServiceActionSummary.Name))

	_, err = c.AssociateServiceActionWithProvisioningArtifact(
		ctx,
		&sdk.AssociateServiceActionWithProvisioningArtifactInput{
			ProductId: aws.String(
				cat.productID,
			),
			ProvisioningArtifactId: aws.String(cat.artifactID),
			ServiceActionId:        actID,
			IdempotencyToken:       aws.String(token(t)),
		},
	)
	require.NoError(t, err)

	_, err = c.AssociateServiceActionWithProvisioningArtifact(
		ctx,
		&sdk.AssociateServiceActionWithProvisioningArtifactInput{
			ProductId: aws.String(
				cat.productID,
			),
			ProvisioningArtifactId: aws.String(cat.artifactID),
			ServiceActionId:        actID,
			IdempotencyToken:       aws.String(token(t)),
		},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	batch, err := c.BatchAssociateServiceActionWithProvisioningArtifact(
		ctx,
		&sdk.BatchAssociateServiceActionWithProvisioningArtifactInput{
			ServiceActionAssociations: []sctypes.ServiceActionAssociation{
				{
					ProductId:              aws.String(cat.productID),
					ProvisioningArtifactId: aws.String(cat.artifactID),
					ServiceActionId:        actID,
				},
				{
					ProductId:              aws.String(cat.productID),
					ProvisioningArtifactId: aws.String("pa-none"),
					ServiceActionId:        actID,
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, batch.FailedServiceActionAssociations, 2)
	assert.Equal(
		t,
		sctypes.ServiceActionAssociationErrorCodeDuplicateResourceException,
		batch.FailedServiceActionAssociations[0].ErrorCode,
	)
	assert.Equal(
		t,
		sctypes.ServiceActionAssociationErrorCodeResourceNotFoundException,
		batch.FailedServiceActionAssociations[1].ErrorCode,
	)

	forArt, err := c.ListServiceActionsForProvisioningArtifact(ctx, &sdk.ListServiceActionsForProvisioningArtifactInput{
		ProductId: aws.String(cat.productID), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	require.NoError(t, err)
	assert.Len(t, forArt.ServiceActionSummaries, 1)

	views, err := c.ListProvisioningArtifactsForServiceAction(
		ctx,
		&sdk.ListProvisioningArtifactsForServiceActionInput{ServiceActionId: actID},
	)
	require.NoError(t, err)
	require.Len(t, views.ProvisioningArtifactViews, 1)
	assert.Equal(t, cat.artifactID, aws.ToString(views.ProvisioningArtifactViews[0].ProvisioningArtifact.Id))

	_, err = c.DeleteServiceAction(
		ctx,
		&sdk.DeleteServiceActionInput{Id: actID, IdempotencyToken: aws.String(token(t))},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	out := provision(t, c, cat, "box", map[string]string{"Size": "s"})
	ppID := out.RecordDetail.ProvisionedProductId

	params, err := c.DescribeServiceActionExecutionParameters(ctx, &sdk.DescribeServiceActionExecutionParametersInput{
		ProvisionedProductId: ppID, ServiceActionId: actID,
	})
	require.NoError(t, err)
	require.Len(t, params.ServiceActionParameters, 1)
	assert.Equal(t, "InstanceId", aws.ToString(params.ServiceActionParameters[0].Name))
	assert.Equal(t, []string{"i-0abc"}, params.ServiceActionParameters[0].DefaultValues)

	_, err = c.ExecuteProvisionedProductServiceAction(ctx, &sdk.ExecuteProvisionedProductServiceActionInput{
		ProvisionedProductId: ppID, ServiceActionId: actID, ExecuteToken: aws.String(token(t)),
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err), "the TARGET parameter is required")

	exec, err := c.ExecuteProvisionedProductServiceAction(ctx, &sdk.ExecuteProvisionedProductServiceActionInput{
		ProvisionedProductId: ppID, ServiceActionId: actID, ExecuteToken: aws.String(token(t)),
		Parameters: map[string][]string{"InstanceId": {"i-0abc"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "EXECUTE_PROFILE_SERVICE_ACTION", aws.ToString(exec.RecordDetail.RecordType))
	require.Len(t, runner.calls, 1)
	assert.Equal(t, []string{"i-0abc"}, runner.calls[0]["InstanceId"])
	assert.Equal(t, []string{"arn:aws:iam::123456789012:role/ssm"}, runner.calls[0]["AutomationAssumeRole"])

	runner.fail = true

	_, err = c.ExecuteProvisionedProductServiceAction(ctx, &sdk.ExecuteProvisionedProductServiceActionInput{
		ProvisionedProductId: ppID, ServiceActionId: actID, ExecuteToken: aws.String(token(t)),
		Parameters: map[string][]string{"InstanceId": {"i-0abc"}},
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	list, err := c.ListServiceActions(ctx, &sdk.ListServiceActionsInput{})
	require.NoError(t, err)
	assert.Len(t, list.ServiceActionSummaries, 1)

	dis, err := c.BatchDisassociateServiceActionFromProvisioningArtifact(
		ctx,
		&sdk.BatchDisassociateServiceActionFromProvisioningArtifactInput{
			ServiceActionAssociations: []sctypes.ServiceActionAssociation{
				{
					ProductId:              aws.String(cat.productID),
					ProvisioningArtifactId: aws.String(cat.artifactID),
					ServiceActionId:        actID,
				},
			},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, dis.FailedServiceActionAssociations)

	_, err = c.DeleteServiceAction(
		ctx,
		&sdk.DeleteServiceActionInput{Id: actID, IdempotencyToken: aws.String(token(t))},
	)
	require.NoError(t, err)

	_, err = c.DescribeServiceAction(ctx, &sdk.DescribeServiceActionInput{Id: actID})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestExecuteServiceActionRequiresRunnerAndDocument(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	created, err := c.CreateServiceAction(ctx, &sdk.CreateServiceActionInput{
		Name: aws.String(
			"a",
		),
		DefinitionType:   sctypes.ServiceActionDefinitionTypeSsmAutomation,
		IdempotencyToken: aws.String(token(t)),
		Definition:       map[string]string{"Name": "Missing-Doc", "Version": "1"},
	})
	require.NoError(t, err)

	actID := created.ServiceActionDetail.ServiceActionSummary.Id

	_, err = c.AssociateServiceActionWithProvisioningArtifact(
		ctx,
		&sdk.AssociateServiceActionWithProvisioningArtifactInput{
			ProductId: aws.String(
				cat.productID,
			),
			ProvisioningArtifactId: aws.String(cat.artifactID),
			ServiceActionId:        actID,
			IdempotencyToken:       aws.String(token(t)),
		},
	)
	require.NoError(t, err)

	out := provision(t, c, cat, "box", map[string]string{"Size": "s"})

	_, err = c.ExecuteProvisionedProductServiceAction(ctx, &sdk.ExecuteProvisionedProductServiceActionInput{
		ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
		ServiceActionId:      actID,
		ExecuteToken:         aws.String(token(t)),
	})
	assert.Equal(t, "OperationNotSupportedException", apiCode(err), "no SSM wired")

	f.backend.SetAutomationRunner(&fakeRunner{docs: map[string]bool{}})

	_, err = c.ExecuteProvisionedProductServiceAction(ctx, &sdk.ExecuteProvisionedProductServiceActionInput{
		ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
		ServiceActionId:      actID,
		ExecuteToken:         aws.String(token(t)),
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err), "the document does not exist")
}

func TestProvisionedProductPlans(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	plan, err := c.CreateProvisionedProductPlan(ctx, &sdk.CreateProvisionedProductPlanInput{
		PlanName: aws.String(
			"launch",
		),
		PlanType:               sctypes.ProvisionedProductPlanTypeCloudformation,
		ProductId:              aws.String(cat.productID),
		ProvisionedProductName: aws.String("planned"),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		IdempotencyToken:       aws.String(token(t)),
		ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
			{Key: aws.String("Size"), Value: aws.String("s")},
		},
	})
	require.NoError(t, err)

	pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: plan.ProvisionProductId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusPlanInProgress, pp.ProvisionedProductDetail.Status)

	desc, err := c.DescribeProvisionedProductPlan(ctx, &sdk.DescribeProvisionedProductPlanInput{PlanId: plan.PlanId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductPlanStatusCreateSuccess, desc.ProvisionedProductPlanDetails.Status)
	require.Len(t, desc.ResourceChanges, 1)
	assert.Equal(t, sctypes.ChangeActionAdd, desc.ResourceChanges[0].Action)
	assert.Equal(t, "AWS::SQS::Queue", aws.ToString(desc.ResourceChanges[0].ResourceType))

	list, err := c.ListProvisionedProductPlans(
		ctx,
		&sdk.ListProvisionedProductPlansInput{ProvisionProductId: plan.ProvisionProductId},
	)
	require.NoError(t, err)
	assert.Len(t, list.ProvisionedProductPlans, 1)

	exec, err := c.ExecuteProvisionedProductPlan(
		ctx,
		&sdk.ExecuteProvisionedProductPlanInput{PlanId: plan.PlanId, IdempotencyToken: aws.String(token(t))},
	)
	require.NoError(t, err)
	assert.Equal(t, "PROVISION_PRODUCT", aws.ToString(exec.RecordDetail.RecordType))

	pp, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: plan.ProvisionProductId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)

	desc, err = c.DescribeProvisionedProductPlan(ctx, &sdk.DescribeProvisionedProductPlanInput{PlanId: plan.PlanId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductPlanStatusExecuteSuccess, desc.ProvisionedProductPlanDetails.Status)

	_, err = c.ExecuteProvisionedProductPlan(
		ctx,
		&sdk.ExecuteProvisionedProductPlanInput{PlanId: plan.PlanId, IdempotencyToken: aws.String(token(t))},
	)
	assert.Equal(t, "InvalidStateException", apiCode(err), "a plan runs once")

	upPlan, err := c.CreateProvisionedProductPlan(ctx, &sdk.CreateProvisionedProductPlanInput{
		PlanName: aws.String(
			"change",
		),
		PlanType:               sctypes.ProvisionedProductPlanTypeCloudformation,
		ProductId:              aws.String(cat.productID),
		ProvisionedProductName: aws.String("planned"),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		IdempotencyToken:       aws.String(token(t)),
		ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
			{Key: aws.String("Size"), UsePreviousValue: true},
			{Key: aws.String("Env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(plan.ProvisionProductId),
		aws.ToString(upPlan.ProvisionProductId),
		"the plan targets the existing product",
	)

	upDesc, err := c.DescribeProvisionedProductPlan(
		ctx,
		&sdk.DescribeProvisionedProductPlanInput{PlanId: upPlan.PlanId},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.ChangeActionModify, upDesc.ResourceChanges[0].Action)

	_, err = c.DeleteProvisionedProductPlan(ctx, &sdk.DeleteProvisionedProductPlanInput{PlanId: upPlan.PlanId})
	require.NoError(t, err)

	_, err = c.DescribeProvisionedProductPlan(ctx, &sdk.DescribeProvisionedProductPlanInput{PlanId: upPlan.PlanId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	unexec, err := c.CreateProvisionedProductPlan(ctx, &sdk.CreateProvisionedProductPlanInput{
		PlanName: aws.String(
			"never",
		),
		PlanType:               sctypes.ProvisionedProductPlanTypeCloudformation,
		ProductId:              aws.String(cat.productID),
		ProvisionedProductName: aws.String("abandoned"),
		ProvisioningArtifactId: aws.String(cat.artifactID),
		IdempotencyToken:       aws.String(token(t)),
		ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
			{Key: aws.String("Size"), Value: aws.String("s")},
		},
	})
	require.NoError(t, err)

	_, err = c.DeleteProvisionedProductPlan(ctx, &sdk.DeleteProvisionedProductPlanInput{PlanId: unexec.PlanId})
	require.NoError(t, err)

	_, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: unexec.ProvisionProductId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "the placeholder of an abandoned launch is removed")
}

func TestCopyProduct(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(60_000_000_000)

	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	_, err := c.UpdateProduct(
		ctx,
		&sdk.UpdateProductInput{
			Id:      aws.String(cat.productID),
			AddTags: []sctypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
		},
	)
	require.NoError(t, err)

	src := "arn:aws:catalog:us-east-1:" + testAccount + ":product/" + cat.productID

	copied, err := c.CopyProduct(ctx, &sdk.CopyProductInput{
		SourceProductArn: aws.String(
			src,
		),
		TargetProductName: aws.String("queue-copy"),
		IdempotencyToken:  aws.String(token(t)),
		CopyOptions:       []sctypes.CopyOption{sctypes.CopyOptionCopyTags},
	})
	require.NoError(t, err)

	st, err := c.DescribeCopyProductStatus(
		ctx,
		&sdk.DescribeCopyProductStatusInput{CopyProductToken: copied.CopyProductToken},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.CopyProductStatusInProgress, st.CopyProductStatus)

	f.clock.advance(2 * 60_000_000_000)

	st, err = c.DescribeCopyProductStatus(
		ctx,
		&sdk.DescribeCopyProductStatusInput{CopyProductToken: copied.CopyProductToken},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.CopyProductStatusSucceeded, st.CopyProductStatus)

	admin, err := c.DescribeProductAsAdmin(ctx, &sdk.DescribeProductAsAdminInput{Id: st.TargetProductId})
	require.NoError(t, err)
	assert.Equal(t, "queue-copy", aws.ToString(admin.ProductViewDetail.ProductViewSummary.Name))
	assert.Len(t, admin.Tags, 1)
	assert.Len(t, admin.ProvisioningArtifactSummaries, 1)
	assert.NotEqual(t, cat.artifactID, aws.ToString(admin.ProvisioningArtifactSummaries[0].Id))

	into, err := c.CopyProduct(ctx, &sdk.CopyProductInput{
		SourceProductArn: aws.String(src), TargetProductId: st.TargetProductId, IdempotencyToken: aws.String(token(t)),
		SourceProvisioningArtifactIdentifiers: []map[string]string{{"Id": cat.artifactID}},
	})
	require.NoError(t, err)

	st2, err := c.DescribeCopyProductStatus(
		ctx,
		&sdk.DescribeCopyProductStatusInput{CopyProductToken: into.CopyProductToken},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(st.TargetProductId), aws.ToString(st2.TargetProductId))

	again, err := c.DescribeProductAsAdmin(ctx, &sdk.DescribeProductAsAdminInput{Id: st.TargetProductId})
	require.NoError(t, err)
	assert.Len(t, again.ProvisioningArtifactSummaries, 2)

	_, err = c.CopyProduct(
		ctx,
		&sdk.CopyProductInput{SourceProductArn: aws.String("nonsense"), IdempotencyToken: aws.String(token(t))},
	)
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	_, err = c.CopyProduct(ctx, &sdk.CopyProductInput{
		SourceProductArn: aws.String(
			"arn:aws:catalog:us-east-1:" + testAccount + ":product/prod-none",
		),
		IdempotencyToken: aws.String(token(t)),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestImportAsProvisionedProduct(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)

	f.engine.mu.Lock()
	f.engine.stacks["legacy"] = &servicecatalog.StackInfo{
		ID:      "arn:aws:cloudformation:us-east-1:" + testAccount + ":stack/legacy/1",
		Name:    "legacy",
		Status:  "CREATE_COMPLETE",
		Outputs: []servicecatalog.Output{{Key: "Url", Value: "http://x"}},
	}
	f.engine.stacks["broken"] = &servicecatalog.StackInfo{ID: "b", Name: "broken", Status: "ROLLBACK_COMPLETE"}
	f.engine.mu.Unlock()

	_, err := c.ImportAsProvisionedProduct(ctx, &sdk.ImportAsProvisionedProductInput{
		IdempotencyToken: aws.String(token(t)), PhysicalId: aws.String("missing"), ProductId: aws.String(cat.productID),
		ProvisionedProductName: aws.String("imp"), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	_, err = c.ImportAsProvisionedProduct(ctx, &sdk.ImportAsProvisionedProductInput{
		IdempotencyToken: aws.String(token(t)), PhysicalId: aws.String("broken"), ProductId: aws.String(cat.productID),
		ProvisionedProductName: aws.String("imp"), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err), "only healthy stacks import")

	out, err := c.ImportAsProvisionedProduct(ctx, &sdk.ImportAsProvisionedProductInput{
		IdempotencyToken: aws.String(token(t)), PhysicalId: aws.String("legacy"), ProductId: aws.String(cat.productID),
		ProvisionedProductName: aws.String("imp"), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	require.NoError(t, err)
	assert.Equal(t, "IMPORT_PROVISIONED_PRODUCT", aws.ToString(out.RecordDetail.RecordType))

	outs, err := c.GetProvisionedProductOutputs(
		ctx,
		&sdk.GetProvisionedProductOutputsInput{ProvisionedProductName: aws.String("imp")},
	)
	require.NoError(t, err)
	require.Len(t, outs.Outputs, 1)
	assert.Equal(t, "Url", aws.ToString(outs.Outputs[0].OutputKey))

	_, err = c.ImportAsProvisionedProduct(ctx, &sdk.ImportAsProvisionedProductInput{
		IdempotencyToken: aws.String(token(t)), PhysicalId: aws.String("legacy"), ProductId: aws.String(cat.productID),
		ProvisionedProductName: aws.String("imp2"), ProvisioningArtifactId: aws.String(cat.artifactID),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err), "a stack is imported once")
}

func TestUpdateProvisionedProductProperties(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)
	out := provision(t, c, cat, "box", map[string]string{"Size": "s"})

	res, err := c.UpdateProvisionedProductProperties(ctx, &sdk.UpdateProvisionedProductPropertiesInput{
		IdempotencyToken: aws.String(token(t)), ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
		ProvisionedProductProperties: map[string]string{"OWNER": "arn:aws:iam::123456789012:user/bob"},
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:user/bob", res.ProvisionedProductProperties["OWNER"])

	rec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: res.RecordId})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_PROVISIONED_PRODUCT_PROPERTIES", aws.ToString(rec.RecordDetail.RecordType))
	assert.Equal(t, sctypes.RecordStatusSucceeded, rec.RecordDetail.Status)

	_, err = c.UpdateProvisionedProductProperties(ctx, &sdk.UpdateProvisionedProductPropertiesInput{
		IdempotencyToken: aws.String(token(t)), ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
		ProvisionedProductProperties: map[string]string{"BOGUS": "arn:aws:iam::1:user/x"},
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}

func TestTenancyIsolation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	east, west := f.client(t), f.clientIn(t, "us-west-2", "test")
	other := f.clientIn(t, "us-east-1", recipientKey)
	ctx := t.Context()
	cat := createCatalog(t, east, templateURL)

	tests := []struct {
		client *sdk.Client
		name   string
	}{
		{name: "other_region", client: west},
		{name: "other_account", client: other},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.client.DescribePortfolio(ctx, &sdk.DescribePortfolioInput{Id: aws.String(cat.portfolioID)})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = tc.client.DescribeProductAsAdmin(
				ctx,
				&sdk.DescribeProductAsAdminInput{Id: aws.String(cat.productID)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			list, err := tc.client.ListPortfolios(ctx, &sdk.ListPortfoliosInput{})
			require.NoError(t, err)
			assert.Empty(t, list.PortfolioDetails)
		})
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()
	cat := createCatalog(t, c, templateURL)
	out := provision(t, c, cat, "kept", map[string]string{"Size": "s"})

	_, err := c.CreateConstraint(ctx, &sdk.CreateConstraintInput{
		PortfolioId: aws.String(
			cat.portfolioID,
		),
		ProductId:        aws.String(cat.productID),
		Type:             aws.String("RESOURCE_UPDATE"),
		Parameters:       aws.String(`{"TagUpdateOnProvisionedProduct":"ALLOWED"}`),
		IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	snap := f.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := servicecatalog.NewInMemoryBackend("000000000000", "us-east-1")
	restored.SetStackEngine(f.engine)
	require.NoError(t, restored.Restore(ctx, snap))

	r := &fixture{backend: restored, engine: f.engine, clock: f.clock, url: serve(t, restored)}
	rc := r.client(t)

	pp, err := rc.DescribeProvisionedProduct(
		ctx,
		&sdk.DescribeProvisionedProductInput{Id: out.RecordDetail.ProvisionedProductId},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)

	cons, err := rc.ListConstraintsForPortfolio(
		ctx,
		&sdk.ListConstraintsForPortfolioInput{PortfolioId: aws.String(cat.portfolioID)},
	)
	require.NoError(t, err)
	assert.Len(t, cons.ConstraintDetails, 1)

	hist, err := rc.ListRecordHistory(ctx, &sdk.ListRecordHistoryInput{})
	require.NoError(t, err)
	assert.Len(t, hist.RecordDetails, 1)
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	createCatalog(t, c, templateURL)
	f.backend.Reset()

	list, err := c.ListPortfolios(t.Context(), &sdk.ListPortfoliosInput{})
	require.NoError(t, err)
	assert.Empty(t, list.PortfolioDetails)
}

func TestUnroutedOperation(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)

	_, err := c.ListStackInstancesForProvisionedProduct(
		t.Context(),
		&sdk.ListStackInstancesForProvisionedProductInput{ProvisionedProductId: aws.String("pp-1")},
	)
	require.Error(t, err)
	assert.Equal(t, "InvalidParametersException", apiCode(err))
}
