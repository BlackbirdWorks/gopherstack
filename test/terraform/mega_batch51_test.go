package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsvc51 "github.com/aws/aws-sdk-go-v2/service/batch"
	cloudtrailsvc51 "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	codeartifactsvc51 "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	codedeploysvc51 "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/dax"
	iamsvc51 "github.com/aws/aws-sdk-go-v2/service/iam"
	memorydbsvc51 "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	resourcegroupssvc51 "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	schedulersvc51 "github.com/aws/aws-sdk-go-v2/service/scheduler"
	secretssvc51 "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	sfnsvc51 "github.com/aws/aws-sdk-go-v2/service/sfn"
	snssvc51 "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc51 "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes51 "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch51 provisions: a Batch fair-share scheduling policy;
// a DAX parameter group and (VPC-backed) subnet group; IAM STS preferences;
// a Scheduler schedule group; a Step Functions activity; a CodeDeploy
// deployment config; an MQ configuration; a Timestream database + table; a
// MemoryDB parameter group + password-auth user; a Secrets Manager secret +
// resource policy; an SQS queue + policy; an SNS topic + policy; a
// ResourceGroups group + resource membership; CodeArtifact domain/repository
// permissions policies; and a CloudTrail Lake event data store -- via
// Terraform, verifying each through its own SDK client.
func TestTerraform_MegaBatch51(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-51",
			// aws_iam_security_token_service_preferences needs a real account ID
			// (see macie2ProviderBlock's doc comment); skip_requesting_account_id=true
			// makes the provider drop it from state right after creation.
			providerFn: macie2ProviderBlock,
			setup:      setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch51Batch(ctx, t)
				verifyMegaBatch51DAX(ctx, t)
				verifyMegaBatch51IAM(ctx, t)
				verifyMegaBatch51Scheduler(ctx, t)
				verifyMegaBatch51SFN(ctx, t)
				verifyMegaBatch51CodeDeploy(ctx, t)
				verifyMegaBatch51MQ(ctx, t)
				verifyMegaBatch51Timestream(ctx, t)
				verifyMegaBatch51MemoryDB(ctx, t)
				verifyMegaBatch51SecretsManager(ctx, t)
				verifyMegaBatch51SQS(ctx, t)
				verifyMegaBatch51SNS(ctx, t)
				verifyMegaBatch51ResourceGroups(ctx, t)
				verifyMegaBatch51CodeArtifact(ctx, t)
				verifyMegaBatch51CloudTrail(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch51Batch(ctx context.Context, t *testing.T) {
	t.Helper()
	client := batchsvc51.NewFromConfig(megaConfig(t), func(o *batchsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	listOut, err := client.ListSchedulingPolicies(ctx, &batchsvc51.ListSchedulingPoliciesInput{})
	require.NoError(t, err, "ListSchedulingPolicies should succeed")

	var policyARN string

	for _, sp := range listOut.SchedulingPolicies {
		if containsSuffix51(aws.ToString(sp.Arn), "mega-batch-51-sched-policy") {
			policyARN = aws.ToString(sp.Arn)
		}
	}

	require.NotEmpty(t, policyARN, "scheduling policy mega-batch-51-sched-policy should be listed")

	descOut, err := client.DescribeSchedulingPolicies(ctx, &batchsvc51.DescribeSchedulingPoliciesInput{
		Arns: []string{policyARN},
	})
	require.NoError(t, err, "DescribeSchedulingPolicies should succeed")
	require.Len(t, descOut.SchedulingPolicies, 1)
	require.NotNil(t, descOut.SchedulingPolicies[0].FairsharePolicy)
	assert.EqualValues(t, 1, aws.ToInt32(descOut.SchedulingPolicies[0].FairsharePolicy.ComputeReservation))
}

func verifyMegaBatch51DAX(ctx context.Context, t *testing.T) {
	t.Helper()
	client := dax.NewFromConfig(megaConfig(t), func(o *dax.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	pgOut, err := client.DescribeParameterGroups(ctx, &dax.DescribeParameterGroupsInput{
		ParameterGroupNames: []string{"mega-batch-51-dax-pg"},
	})
	require.NoError(t, err, "DescribeParameterGroups should succeed")
	require.Len(t, pgOut.ParameterGroups, 1)

	sgOut, err := client.DescribeSubnetGroups(ctx, &dax.DescribeSubnetGroupsInput{
		SubnetGroupNames: []string{"mega-batch-51-dax-sg"},
	})
	require.NoError(t, err, "DescribeSubnetGroups should succeed")
	require.Len(t, sgOut.SubnetGroups, 1)
	assert.Len(t, sgOut.SubnetGroups[0].Subnets, 2)
}

func verifyMegaBatch51IAM(ctx context.Context, t *testing.T) {
	t.Helper()
	client := iamsvc51.NewFromConfig(megaConfig(t), func(o *iamsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetAccountSummary(ctx, &iamsvc51.GetAccountSummaryInput{})
	require.NoError(t, err, "GetAccountSummary should succeed")
	assert.Contains(t, out.SummaryMap, "GlobalEndpointTokenVersion")
}

func verifyMegaBatch51Scheduler(ctx context.Context, t *testing.T) {
	t.Helper()
	client := schedulersvc51.NewFromConfig(megaConfig(t), func(o *schedulersvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetScheduleGroup(ctx, &schedulersvc51.GetScheduleGroupInput{
		Name: aws.String("mega-batch-51-sched-group"),
	})
	require.NoError(t, err, "GetScheduleGroup should succeed")
	assert.Equal(t, "mega-batch-51-sched-group", aws.ToString(out.Name))
}

func verifyMegaBatch51SFN(ctx context.Context, t *testing.T) {
	t.Helper()
	client := sfnsvc51.NewFromConfig(megaConfig(t), func(o *sfnsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListActivities(ctx, &sfnsvc51.ListActivitiesInput{})
	require.NoError(t, err, "ListActivities should succeed")

	found := false

	for _, a := range out.Activities {
		if aws.ToString(a.Name) == "mega-batch-51-activity" {
			found = true
		}
	}

	assert.True(t, found, "activity mega-batch-51-activity should be listed")
}

func verifyMegaBatch51CodeDeploy(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codedeploysvc51.NewFromConfig(megaConfig(t), func(o *codedeploysvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetDeploymentConfig(ctx, &codedeploysvc51.GetDeploymentConfigInput{
		DeploymentConfigName: aws.String("mega-batch-51-deploy-config"),
	})
	require.NoError(t, err, "GetDeploymentConfig should succeed")
	require.NotNil(t, out.DeploymentConfigInfo.MinimumHealthyHosts)
	assert.EqualValues(t, 1, out.DeploymentConfigInfo.MinimumHealthyHosts.Value)
}

func verifyMegaBatch51MQ(ctx context.Context, t *testing.T) {
	t.Helper()
	client := mq.NewFromConfig(megaConfig(t), func(o *mq.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListConfigurations(ctx, &mq.ListConfigurationsInput{})
	require.NoError(t, err, "ListConfigurations should succeed")

	found := false

	for _, c := range out.Configurations {
		if aws.ToString(c.Name) == "mega-batch-51-mq-config" {
			found = true
		}
	}

	assert.True(t, found, "configuration mega-batch-51-mq-config should be listed")
}

func verifyMegaBatch51Timestream(ctx context.Context, t *testing.T) {
	t.Helper()
	client := timestreamwrite.NewFromConfig(megaConfig(t), func(o *timestreamwrite.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	dbOut, err := client.DescribeDatabase(ctx, &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String("mega-batch-51-timestream-db"),
	})
	require.NoError(t, err, "DescribeDatabase should succeed")
	assert.Equal(t, "mega-batch-51-timestream-db", aws.ToString(dbOut.Database.DatabaseName))

	tblOut, err := client.DescribeTable(ctx, &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String("mega-batch-51-timestream-db"),
		TableName:    aws.String("mega-batch-51-timestream-table"),
	})
	require.NoError(t, err, "DescribeTable should succeed")
	assert.Equal(t, "mega-batch-51-timestream-table", aws.ToString(tblOut.Table.TableName))
}

func verifyMegaBatch51MemoryDB(ctx context.Context, t *testing.T) {
	t.Helper()
	client := memorydbsvc51.NewFromConfig(megaConfig(t), func(o *memorydbsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	pgOut, err := client.DescribeParameterGroups(ctx, &memorydbsvc51.DescribeParameterGroupsInput{
		ParameterGroupName: aws.String("mega-batch-51-memorydb-pg"),
	})
	require.NoError(t, err, "DescribeParameterGroups should succeed")
	require.Len(t, pgOut.ParameterGroups, 1)
	assert.Equal(t, "memorydb_redis7", aws.ToString(pgOut.ParameterGroups[0].Family))

	userOut, err := client.DescribeUsers(ctx, &memorydbsvc51.DescribeUsersInput{
		UserName: aws.String("mega-batch-51-memorydb-user"),
	})
	require.NoError(t, err, "DescribeUsers should succeed")
	require.Len(t, userOut.Users, 1)
	assert.Equal(t, "on ~* &* +@all", aws.ToString(userOut.Users[0].AccessString))
}

func verifyMegaBatch51SecretsManager(ctx context.Context, t *testing.T) {
	t.Helper()
	client := secretssvc51.NewFromConfig(megaConfig(t), func(o *secretssvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetResourcePolicy(ctx, &secretssvc51.GetResourcePolicyInput{
		SecretId: aws.String("mega-batch-51-secret"),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(out.ResourcePolicy), "secretsmanager:GetSecretValue")
}

func verifyMegaBatch51SQS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := sqssvc51.NewFromConfig(megaConfig(t), func(o *sqssvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	urlOut, err := client.GetQueueUrl(ctx, &sqssvc51.GetQueueUrlInput{
		QueueName: aws.String("mega-batch-51-queue"),
	})
	require.NoError(t, err, "GetQueueUrl should succeed")

	attrOut, err := client.GetQueueAttributes(ctx, &sqssvc51.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []sqstypes51.QueueAttributeName{sqstypes51.QueueAttributeNamePolicy},
	})
	require.NoError(t, err, "GetQueueAttributes should succeed")
	assert.Contains(t, attrOut.Attributes["Policy"], "sqs:SendMessage")
}

func verifyMegaBatch51SNS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := snssvc51.NewFromConfig(megaConfig(t), func(o *snssvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListTopics(ctx, &snssvc51.ListTopicsInput{})
	require.NoError(t, err, "ListTopics should succeed")

	var topicArn string

	for _, tpc := range out.Topics {
		if arn := aws.ToString(tpc.TopicArn); len(arn) > 0 && containsSuffix51(arn, "mega-batch-51-topic") {
			topicArn = arn
		}
	}

	require.NotEmpty(t, topicArn, "topic mega-batch-51-topic should be listed")

	attrOut, err := client.GetTopicAttributes(ctx, &snssvc51.GetTopicAttributesInput{
		TopicArn: aws.String(topicArn),
	})
	require.NoError(t, err, "GetTopicAttributes should succeed")
	assert.Contains(t, attrOut.Attributes["Policy"], "SNS:Publish")
}

func containsSuffix51(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func verifyMegaBatch51ResourceGroups(ctx context.Context, t *testing.T) {
	t.Helper()
	client := resourcegroupssvc51.NewFromConfig(megaConfig(t), func(o *resourcegroupssvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListGroupResources(ctx, &resourcegroupssvc51.ListGroupResourcesInput{
		Group: aws.String("mega-batch-51-rg"),
	})
	require.NoError(t, err, "ListGroupResources should succeed")
	assert.NotEmpty(t, out.Resources, "group should have at least one grouped resource")
}

func verifyMegaBatch51CodeArtifact(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codeartifactsvc51.NewFromConfig(megaConfig(t), func(o *codeartifactsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	domPolicyOut, err := client.GetDomainPermissionsPolicy(ctx, &codeartifactsvc51.GetDomainPermissionsPolicyInput{
		Domain: aws.String("mega-batch-51-domain"),
	})
	require.NoError(t, err, "GetDomainPermissionsPolicy should succeed")
	assert.Contains(t, aws.ToString(domPolicyOut.Policy.Document), "codeartifact:CreateRepository")

	repoPolicyOut, err := client.GetRepositoryPermissionsPolicy(
		ctx, &codeartifactsvc51.GetRepositoryPermissionsPolicyInput{
			Domain:     aws.String("mega-batch-51-domain"),
			Repository: aws.String("mega-batch-51-repo"),
		},
	)
	require.NoError(t, err, "GetRepositoryPermissionsPolicy should succeed")
	assert.Contains(t, aws.ToString(repoPolicyOut.Policy.Document), "codeartifact:ReadFromRepository")
}

func verifyMegaBatch51CloudTrail(ctx context.Context, t *testing.T) {
	t.Helper()
	client := cloudtrailsvc51.NewFromConfig(megaConfig(t), func(o *cloudtrailsvc51.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListEventDataStores(ctx, &cloudtrailsvc51.ListEventDataStoresInput{})
	require.NoError(t, err, "ListEventDataStores should succeed")

	found := false

	for _, eds := range out.EventDataStores {
		if aws.ToString(eds.Name) == "mega-batch-51-eds" {
			found = true
		}
	}

	assert.True(t, found, "event data store mega-batch-51-eds should be listed")
}
