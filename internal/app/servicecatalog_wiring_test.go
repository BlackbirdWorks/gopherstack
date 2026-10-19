package app

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const scQueueTemplate = `{"Parameters":{"QueueName":{"Type":"String"}},` +
	`"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Ref":"QueueName"}}}},` +
	`"Outputs":{"QueueUrl":{"Description":"url","Value":{"Ref":"Q"}}}}`

func TestServiceCatalogWiringProvisionsThroughCloudFormation(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()

	s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	sc := servicecatalog.NewFromConfig(fx.cfg)
	sqsC := sqs.NewFromConfig(fx.cfg)
	cfn := cloudformation.NewFromConfig(fx.cfg)

	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("sc-templates")})
	require.NoError(t, err)

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("sc-templates"), Key: aws.String("queue.json"), Body: strings.NewReader(scQueueTemplate),
	})
	require.NoError(t, err)

	pf, err := sc.CreatePortfolio(ctx, &servicecatalog.CreatePortfolioInput{
		DisplayName: aws.String("wired"), ProviderName: aws.String("it"), IdempotencyToken: aws.String("pf-1"),
	})
	require.NoError(t, err)

	pr, err := sc.CreateProduct(ctx, &servicecatalog.CreateProductInput{
		Name: aws.String("queue"), Owner: aws.String("me"), ProductType: sctypes.ProductTypeCloudFormationTemplate,
		IdempotencyToken: aws.String("pr-1"), Tags: []sctypes.Tag{{Key: aws.String("cost"), Value: aws.String("c1")}},
		ProvisioningArtifactParameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v1"),
			Info: map[string]string{
				"LoadTemplateFromURL": "https://s3.us-east-1.amazonaws.com/sc-templates/queue.json",
			},
		},
	})
	require.NoError(t, err)

	prodID := pr.ProductViewDetail.ProductViewSummary.ProductId

	_, err = sc.AssociateProductWithPortfolio(
		ctx,
		&servicecatalog.AssociateProductWithPortfolioInput{PortfolioId: pf.PortfolioDetail.Id, ProductId: prodID},
	)
	require.NoError(t, err)

	params, err := sc.DescribeProvisioningParameters(ctx, &servicecatalog.DescribeProvisioningParametersInput{
		ProductId: prodID, ProvisioningArtifactId: pr.ProvisioningArtifactDetail.Id,
	})
	require.NoError(t, err)
	require.Len(t, params.ProvisioningArtifactParameters, 1)
	assert.Equal(t, "QueueName", aws.ToString(params.ProvisioningArtifactParameters[0].ParameterKey))
	require.Len(t, params.ProvisioningArtifactOutputKeys, 1)

	out, err := sc.ProvisionProduct(ctx, &servicecatalog.ProvisionProductInput{
		ProvisionToken: aws.String("run-1"), ProvisionedProductName: aws.String("wired-queue"), ProductId: prodID,
		ProvisioningArtifactId: pr.ProvisioningArtifactDetail.Id,
		ProvisioningParameters: []sctypes.ProvisioningParameter{
			{Key: aws.String("QueueName"), Value: aws.String("sc-made-queue")},
		},
	})
	require.NoError(t, err)

	rec, err := sc.DescribeRecord(ctx, &servicecatalog.DescribeRecordInput{Id: out.RecordDetail.RecordId})
	require.NoError(t, err)
	require.Equal(
		t,
		sctypes.RecordStatusSucceeded,
		rec.RecordDetail.Status,
		"record errors: %+v",
		rec.RecordDetail.RecordErrors,
	)
	require.Len(t, rec.RecordOutputs, 1)
	assert.Contains(t, aws.ToString(rec.RecordOutputs[0].OutputValue), "sc-made-queue")

	queues, err := sqsC.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("sc-made-queue")})
	require.NoError(t, err)
	assert.Len(t, queues.QueueUrls, 1, "the product launched a real queue through CloudFormation")

	stacks, err := cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{})
	require.NoError(t, err)

	var stackTags map[string]string

	for _, s := range stacks.Stacks {
		if strings.HasPrefix(aws.ToString(s.StackName), "SC-") {
			stackTags = map[string]string{}
			for _, tg := range s.Tags {
				stackTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
		}
	}

	require.NotNil(t, stackTags, "an SC- stack exists")
	assert.Equal(t, "c1", stackTags["cost"])
	assert.Contains(t, stackTags["aws:servicecatalog:productArn"], aws.ToString(prodID))

	pp, err := sc.DescribeProvisionedProduct(
		ctx,
		&servicecatalog.DescribeProvisionedProductInput{Name: aws.String("wired-queue")},
	)
	require.NoError(t, err)
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)

	_, err = sc.TerminateProvisionedProduct(ctx, &servicecatalog.TerminateProvisionedProductInput{
		TerminateToken: aws.String("end-1"), ProvisionedProductName: aws.String("wired-queue"),
	})
	require.NoError(t, err)

	queues, err = sqsC.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("sc-made-queue")})
	require.NoError(t, err)
	assert.Empty(t, queues.QueueUrls, "terminating the product deleted its stack and queue")
}
