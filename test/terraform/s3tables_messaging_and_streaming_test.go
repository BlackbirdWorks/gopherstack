package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysvc52 "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigatewaytypes52 "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	applicationautoscalingsvc52 "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	applicationautoscalingtypes52 "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	bedrockagentsvc52 "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	bedrockagenttypes52 "github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	codepipelinesvc52 "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	codepipelinetypes52 "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	ecrsvc52 "github.com/aws/aws-sdk-go-v2/service/ecr"
	elbsvc52 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	glaciersvc52 "github.com/aws/aws-sdk-go-v2/service/glacier"
	kinesissvc52 "github.com/aws/aws-sdk-go-v2/service/kinesis"
	lakeformationsvc52 "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lakeformationtypes52 "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	mediastoresvc52 "github.com/aws/aws-sdk-go-v2/service/mediastore"
	quicksightsvc52 "github.com/aws/aws-sdk-go-v2/service/quicksight"
	rekognitionsvc52 "github.com/aws/aws-sdk-go-v2/service/rekognition"
	rolesanywheresvc52 "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	rolesanywheretypes52 "github.com/aws/aws-sdk-go-v2/service/rolesanywhere/types"
	s3tablessvc52 "github.com/aws/aws-sdk-go-v2/service/s3tables"
	servicediscoverysvc52 "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	servicediscoverytypes52 "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	snssvc52 "github.com/aws/aws-sdk-go-v2/service/sns"
	sqssvc52 "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes52 "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	transcribesvc52 "github.com/aws/aws-sdk-go-v2/service/transcribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch52 provisions 24 previously-uncovered class-A
// terraform resource types (Application Auto Scaling, Bedrock Agents,
// ECR registry scanning, Glacier vault lock, Roles Anywhere, Rekognition,
// QuickSight account subscription/settings, Kinesis resource policy/stream
// consumer, Lake Formation resource LF-tag, MediaStore container policy,
// S3 Tables bucket/table policies, Service Discovery public/private DNS
// namespaces, SNS SMS preferences/data protection policy, SQS redrive
// policy, Transcribe vocabulary filter, CodePipeline custom action type,
// API Gateway PutRestApi, and a classic ELB attachment) via Terraform,
// verifying each through its own SDK client.
func TestTerraform_MegaBatch52(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-52",
			// aws_quicksight_account_subscription needs a real account ID (see
			// macie2ProviderBlock's doc comment); skip_requesting_account_id
			// omitted here for the same reason.
			providerFn: macie2ProviderBlock,
			setup:      setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch52ApplicationAutoScaling(ctx, t)
				verifyMegaBatch52BedrockAgent(ctx, t)
				verifyMegaBatch52ECR(ctx, t)
				verifyMegaBatch52Glacier(ctx, t)
				verifyMegaBatch52RolesAnywhere(ctx, t)
				verifyMegaBatch52Rekognition(ctx, t)
				verifyMegaBatch52QuickSight(ctx, t)
				verifyMegaBatch52Kinesis(ctx, t)
				verifyMegaBatch52LakeFormation(ctx, t)
				verifyMegaBatch52MediaStore(ctx, t)
				verifyMegaBatch52S3Tables(ctx, t)
				verifyMegaBatch52ServiceDiscovery(ctx, t)
				verifyMegaBatch52SNS(ctx, t)
				verifyMegaBatch52SQS(ctx, t)
				verifyMegaBatch52Transcribe(ctx, t)
				verifyMegaBatch52CodePipeline(ctx, t)
				verifyMegaBatch52APIGateway(ctx, t)
				verifyMegaBatch52ELB(ctx, t)
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

func verifyMegaBatch52ApplicationAutoScaling(ctx context.Context, t *testing.T) {
	t.Helper()
	client := applicationautoscalingsvc52.NewFromConfig(megaConfig(t), func(o *applicationautoscalingsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	policyOut, err := client.DescribeScalingPolicies(ctx, &applicationautoscalingsvc52.DescribeScalingPoliciesInput{
		ServiceNamespace: applicationautoscalingtypes52.ServiceNamespaceDynamodb,
		ResourceId:       aws.String("table/mega-batch-52-ddb"),
	})
	require.NoError(t, err, "DescribeScalingPolicies should succeed")
	policy := findBy(t, policyOut.ScalingPolicies, func(p applicationautoscalingtypes52.ScalingPolicy) bool {
		return aws.ToString(p.PolicyName) == "mega-batch-52-scaling-policy"
	}, "scaling policy mega-batch-52-scaling-policy")
	assert.Equal(t, applicationautoscalingtypes52.PolicyTypeTargetTrackingScaling, policy.PolicyType)

	actionOut, err := client.DescribeScheduledActions(ctx, &applicationautoscalingsvc52.DescribeScheduledActionsInput{
		ServiceNamespace: applicationautoscalingtypes52.ServiceNamespaceDynamodb,
		ResourceId:       aws.String("table/mega-batch-52-ddb"),
	})
	require.NoError(t, err, "DescribeScheduledActions should succeed")
	findBy(t, actionOut.ScheduledActions, func(a applicationautoscalingtypes52.ScheduledAction) bool {
		return aws.ToString(a.ScheduledActionName) == "mega-batch-52-scheduled-action"
	}, "scheduled action mega-batch-52-scheduled-action")
}

func verifyMegaBatch52BedrockAgent(ctx context.Context, t *testing.T) {
	t.Helper()
	client := bedrockagentsvc52.NewFromConfig(megaConfig(t), func(o *bedrockagentsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	listOut, err := client.ListPrompts(ctx, &bedrockagentsvc52.ListPromptsInput{})
	require.NoError(t, err, "ListPrompts should succeed")
	prompt := findBy(t, listOut.PromptSummaries, func(p bedrockagenttypes52.PromptSummary) bool {
		return aws.ToString(p.Name) == "mega-batch-52-prompt"
	}, "prompt mega-batch-52-prompt")

	getOut, err := client.GetPrompt(ctx, &bedrockagentsvc52.GetPromptInput{
		PromptIdentifier: prompt.Id,
	})
	require.NoError(t, err, "GetPrompt should succeed")
	assert.Equal(t, "mega-batch-52-prompt", aws.ToString(getOut.Name))
}

func verifyMegaBatch52ECR(ctx context.Context, t *testing.T) {
	t.Helper()
	client := ecrsvc52.NewFromConfig(megaConfig(t), func(o *ecrsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetRegistryScanningConfiguration(ctx, &ecrsvc52.GetRegistryScanningConfigurationInput{})
	require.NoError(t, err, "GetRegistryScanningConfiguration should succeed")
	require.NotNil(t, out.ScanningConfiguration)
	assert.Equal(t, "BASIC", string(out.ScanningConfiguration.ScanType))
	require.Len(t, out.ScanningConfiguration.Rules, 1)
	assert.Equal(t, "SCAN_ON_PUSH", string(out.ScanningConfiguration.Rules[0].ScanFrequency))
}

func verifyMegaBatch52Glacier(ctx context.Context, t *testing.T) {
	t.Helper()
	client := glaciersvc52.NewFromConfig(megaConfig(t), func(o *glaciersvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	descOut, err := client.DescribeVault(ctx, &glaciersvc52.DescribeVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String("mega-batch-52-vault"),
	})
	require.NoError(t, err, "DescribeVault should succeed")
	assert.Equal(t, "mega-batch-52-vault", aws.ToString(descOut.VaultName))

	lockOut, err := client.GetVaultLock(ctx, &glaciersvc52.GetVaultLockInput{
		AccountId: aws.String("-"),
		VaultName: aws.String("mega-batch-52-vault"),
	})
	require.NoError(t, err, "GetVaultLock should succeed")
	assert.Equal(t, "InProgress", aws.ToString(lockOut.State))
}

func verifyMegaBatch52RolesAnywhere(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rolesanywheresvc52.NewFromConfig(megaConfig(t), func(o *rolesanywheresvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListProfiles(ctx, &rolesanywheresvc52.ListProfilesInput{})
	require.NoError(t, err, "ListProfiles should succeed")
	profile := findBy(t, out.Profiles, func(p rolesanywheretypes52.ProfileDetail) bool {
		return aws.ToString(p.Name) == "mega-batch-52-profile"
	}, "profile mega-batch-52-profile")
	assert.Contains(t, profile.RoleArns, "arn:aws:iam::000000000000:role/mega-batch-52-rolesanywhere-role")
}

func verifyMegaBatch52Rekognition(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rekognitionsvc52.NewFromConfig(megaConfig(t), func(o *rekognitionsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeProjects(ctx, &rekognitionsvc52.DescribeProjectsInput{
		ProjectNames: []string{"mega-batch-52-project"},
	})
	require.NoError(t, err, "DescribeProjects should succeed")
	require.Len(t, out.ProjectDescriptions, 1)
	assert.Equal(t, "CREATED", string(out.ProjectDescriptions[0].Status))
}

func verifyMegaBatch52QuickSight(ctx context.Context, t *testing.T) {
	t.Helper()
	client := quicksightsvc52.NewFromConfig(megaConfig(t), func(o *quicksightsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	subOut, err := client.DescribeAccountSubscription(ctx, &quicksightsvc52.DescribeAccountSubscriptionInput{
		AwsAccountId: aws.String("000000000000"),
	})
	require.NoError(t, err, "DescribeAccountSubscription should succeed")
	require.NotNil(t, subOut.AccountInfo)
	assert.Equal(t, "mega-batch-52-qs", aws.ToString(subOut.AccountInfo.AccountName))

	settingsOut, err := client.DescribeAccountSettings(ctx, &quicksightsvc52.DescribeAccountSettingsInput{
		AwsAccountId: aws.String("000000000000"),
	})
	require.NoError(t, err, "DescribeAccountSettings should succeed")
	require.NotNil(t, settingsOut.AccountSettings)
	assert.False(t, settingsOut.AccountSettings.TerminationProtectionEnabled)
}

func verifyMegaBatch52Kinesis(ctx context.Context, t *testing.T) {
	t.Helper()
	client := kinesissvc52.NewFromConfig(megaConfig(t), func(o *kinesissvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/mega-batch-52-stream"

	policyOut, err := client.GetResourcePolicy(ctx, &kinesissvc52.GetResourcePolicyInput{
		ResourceARN: aws.String(streamARN),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "kinesis:GetRecords")

	consumersOut, err := client.ListStreamConsumers(ctx, &kinesissvc52.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err, "ListStreamConsumers should succeed")
	assert.NotEmpty(t, consumersOut.Consumers, "consumer mega-batch-52-consumer should be listed")
}

func verifyMegaBatch52LakeFormation(ctx context.Context, t *testing.T) {
	t.Helper()
	client := lakeformationsvc52.NewFromConfig(megaConfig(t), func(o *lakeformationsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	tagOut, err := client.GetLFTag(ctx, &lakeformationsvc52.GetLFTagInput{
		CatalogId: aws.String("000000000000"),
		TagKey:    aws.String("mega-batch-52-tag"),
	})
	require.NoError(t, err, "GetLFTag should succeed")
	assert.ElementsMatch(t, []string{"blue", "green"}, tagOut.TagValues)

	resOut, err := client.GetResourceLFTags(ctx, &lakeformationsvc52.GetResourceLFTagsInput{
		Resource: &lakeformationtypes52.Resource{
			Database: &lakeformationtypes52.DatabaseResource{Name: aws.String("mega_batch_52_db")},
		},
	})
	require.NoError(t, err, "GetResourceLFTags should succeed")
	require.NotEmpty(t, resOut.LFTagOnDatabase)
	assert.Equal(t, "mega-batch-52-tag", aws.ToString(resOut.LFTagOnDatabase[0].TagKey))
}

func verifyMegaBatch52MediaStore(ctx context.Context, t *testing.T) {
	t.Helper()
	client := mediastoresvc52.NewFromConfig(megaConfig(t), func(o *mediastoresvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetContainerPolicy(ctx, &mediastoresvc52.GetContainerPolicyInput{
		ContainerName: aws.String("mega_batch_52_container"),
	})
	require.NoError(t, err, "GetContainerPolicy should succeed")
	assert.Contains(t, aws.ToString(out.Policy), "mediastore:GetObject")
}

func verifyMegaBatch52S3Tables(ctx context.Context, t *testing.T) {
	t.Helper()
	client := s3tablessvc52.NewFromConfig(megaConfig(t), func(o *s3tablessvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/mega-batch-52-tb"

	bucketPolicyOut, err := client.GetTableBucketPolicy(ctx, &s3tablessvc52.GetTableBucketPolicyInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.NoError(t, err, "GetTableBucketPolicy should succeed")
	assert.Contains(t, aws.ToString(bucketPolicyOut.ResourcePolicy), "s3tables:GetTableBucket")

	tablePolicyOut, err := client.GetTablePolicy(ctx, &s3tablessvc52.GetTablePolicyInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("mega_batch_52_ns"),
		Name:           aws.String("mega_batch_52_table"),
	})
	require.NoError(t, err, "GetTablePolicy should succeed")
	assert.Contains(t, aws.ToString(tablePolicyOut.ResourcePolicy), "s3tables:GetTable")
}

func verifyMegaBatch52ServiceDiscovery(ctx context.Context, t *testing.T) {
	t.Helper()
	client := servicediscoverysvc52.NewFromConfig(megaConfig(t), func(o *servicediscoverysvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListNamespaces(ctx, &servicediscoverysvc52.ListNamespacesInput{})
	require.NoError(t, err, "ListNamespaces should succeed")

	pub := findBy(t, out.Namespaces, func(ns servicediscoverytypes52.NamespaceSummary) bool {
		return aws.ToString(ns.Name) == "mega-batch-52.example.com"
	}, "public namespace mega-batch-52.example.com")
	assert.Equal(t, servicediscoverytypes52.NamespaceTypeDnsPublic, pub.Type)

	priv := findBy(t, out.Namespaces, func(ns servicediscoverytypes52.NamespaceSummary) bool {
		return aws.ToString(ns.Name) == "mega-batch-52.private"
	}, "private namespace mega-batch-52.private")
	assert.Equal(t, servicediscoverytypes52.NamespaceTypeDnsPrivate, priv.Type)
}

func verifyMegaBatch52SNS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := snssvc52.NewFromConfig(megaConfig(t), func(o *snssvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	smsOut, err := client.GetSMSAttributes(ctx, &snssvc52.GetSMSAttributesInput{})
	require.NoError(t, err, "GetSMSAttributes should succeed")
	assert.Equal(t, "Transactional", smsOut.Attributes["DefaultSMSType"])

	dppOut, err := client.GetDataProtectionPolicy(ctx, &snssvc52.GetDataProtectionPolicyInput{
		ResourceArn: aws.String("arn:aws:sns:us-east-1:000000000000:mega-batch-52-topic"),
	})
	require.NoError(t, err, "GetDataProtectionPolicy should succeed")
	assert.Contains(t, aws.ToString(dppOut.DataProtectionPolicy), "mega-batch-52-dpp")
}

func verifyMegaBatch52SQS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := sqssvc52.NewFromConfig(megaConfig(t), func(o *sqssvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	urlOut, err := client.GetQueueUrl(ctx, &sqssvc52.GetQueueUrlInput{
		QueueName: aws.String("mega-batch-52-src"),
	})
	require.NoError(t, err, "GetQueueUrl should succeed")

	attrOut, err := client.GetQueueAttributes(ctx, &sqssvc52.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []sqstypes52.QueueAttributeName{sqstypes52.QueueAttributeNameRedrivePolicy},
	})
	require.NoError(t, err, "GetQueueAttributes should succeed")
	assert.Contains(t, attrOut.Attributes["RedrivePolicy"], "mega-batch-52-dlq")
}

func verifyMegaBatch52Transcribe(ctx context.Context, t *testing.T) {
	t.Helper()
	client := transcribesvc52.NewFromConfig(megaConfig(t), func(o *transcribesvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetVocabularyFilter(ctx, &transcribesvc52.GetVocabularyFilterInput{
		VocabularyFilterName: aws.String("mega-batch-52-filter"),
	})
	require.NoError(t, err, "GetVocabularyFilter should succeed")
	assert.Equal(t, "en-US", string(out.LanguageCode))
}

func verifyMegaBatch52CodePipeline(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codepipelinesvc52.NewFromConfig(megaConfig(t), func(o *codepipelinesvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListActionTypes(ctx, &codepipelinesvc52.ListActionTypesInput{
		ActionOwnerFilter: codepipelinetypes52.ActionOwnerCustom,
	})
	require.NoError(t, err, "ListActionTypes should succeed")
	action := findBy(t, out.ActionTypes, func(a codepipelinetypes52.ActionType) bool {
		return a.Id != nil && aws.ToString(a.Id.Provider) == "mega-batch-52-provider"
	}, "custom action type mega-batch-52-provider")
	assert.Equal(t, codepipelinetypes52.ActionCategoryBuild, action.Id.Category)
	assert.Equal(t, "1", aws.ToString(action.Id.Version))
}

func verifyMegaBatch52APIGateway(ctx context.Context, t *testing.T) {
	t.Helper()
	client := apigatewaysvc52.NewFromConfig(megaConfig(t), func(o *apigatewaysvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	apisOut, err := client.GetRestApis(ctx, &apigatewaysvc52.GetRestApisInput{})
	require.NoError(t, err, "GetRestApis should succeed")
	api := findBy(t, apisOut.Items, func(a apigatewaytypes52.RestApi) bool {
		return aws.ToString(a.Name) == "mega-batch-52-api"
	}, "rest api mega-batch-52-api")

	resOut, err := client.GetResources(ctx, &apigatewaysvc52.GetResourcesInput{
		RestApiId: api.Id,
	})
	require.NoError(t, err, "GetResources should succeed")
	findBy(t, resOut.Items, func(r apigatewaytypes52.Resource) bool {
		return aws.ToString(r.Path) == "/mb52"
	}, "resource /mb52")
}

func verifyMegaBatch52ELB(ctx context.Context, t *testing.T) {
	t.Helper()
	client := elbsvc52.NewFromConfig(megaConfig(t), func(o *elbsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeLoadBalancers(ctx, &elbsvc52.DescribeLoadBalancersInput{
		LoadBalancerNames: []string{"mega-batch-52-lb"},
	})
	require.NoError(t, err, "DescribeLoadBalancers should succeed")
	require.Len(t, out.LoadBalancerDescriptions, 1)
	assert.Len(t, out.LoadBalancerDescriptions[0].Instances, 1)
}
