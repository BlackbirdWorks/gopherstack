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

// TestTerraform_S3tablesMessagingAndStreaming provisions 24 previously-uncovered class-A
// terraform resource types (Application Auto Scaling, Bedrock Agents,
// ECR registry scanning, Glacier vault lock, Roles Anywhere, Rekognition,
// QuickSight account subscription/settings, Kinesis resource policy/stream
// consumer, Lake Formation resource LF-tag, MediaStore container policy,
// S3 Tables bucket/table policies, Service Discovery public/private DNS
// namespaces, SNS SMS preferences/data protection policy, SQS redrive
// policy, Transcribe vocabulary filter, CodePipeline custom action type,
// API Gateway PutRestApi, and a classic ELB attachment) via Terraform,
// verifying each through its own SDK client.
func TestTerraform_S3tablesMessagingAndStreaming(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "s3tables-messaging-and-streaming",
			// aws_quicksight_account_subscription needs a real account ID (see
			// macie2ProviderBlock's doc comment); skip_requesting_account_id
			// omitted here for the same reason.
			providerFn: macie2ProviderBlock,
			setup:      setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyS3tablesMessagingAndStreamingApplicationAutoScaling(ctx, t)
				verifyS3tablesMessagingAndStreamingBedrockAgent(ctx, t)
				verifyS3tablesMessagingAndStreamingECR(ctx, t)
				verifyS3tablesMessagingAndStreamingGlacier(ctx, t)
				verifyS3tablesMessagingAndStreamingRolesAnywhere(ctx, t)
				verifyS3tablesMessagingAndStreamingRekognition(ctx, t)
				verifyS3tablesMessagingAndStreamingQuickSight(ctx, t)
				verifyS3tablesMessagingAndStreamingKinesis(ctx, t)
				verifyS3tablesMessagingAndStreamingLakeFormation(ctx, t)
				verifyS3tablesMessagingAndStreamingMediaStore(ctx, t)
				verifyS3tablesMessagingAndStreamingS3Tables(ctx, t)
				verifyS3tablesMessagingAndStreamingServiceDiscovery(ctx, t)
				verifyS3tablesMessagingAndStreamingSNS(ctx, t)
				verifyS3tablesMessagingAndStreamingSQS(ctx, t)
				verifyS3tablesMessagingAndStreamingTranscribe(ctx, t)
				verifyS3tablesMessagingAndStreamingCodePipeline(ctx, t)
				verifyS3tablesMessagingAndStreamingAPIGateway(ctx, t)
				verifyS3tablesMessagingAndStreamingELB(ctx, t)
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

func verifyS3tablesMessagingAndStreamingApplicationAutoScaling(ctx context.Context, t *testing.T) {
	t.Helper()
	client := applicationautoscalingsvc52.NewFromConfig(megaConfig(t), func(o *applicationautoscalingsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	policyOut, err := client.DescribeScalingPolicies(ctx, &applicationautoscalingsvc52.DescribeScalingPoliciesInput{
		ServiceNamespace: applicationautoscalingtypes52.ServiceNamespaceDynamodb,
		ResourceId:       aws.String("table/s3ms-ddb"),
	})
	require.NoError(t, err, "DescribeScalingPolicies should succeed")
	policy := findBy(t, policyOut.ScalingPolicies, func(p applicationautoscalingtypes52.ScalingPolicy) bool {
		return aws.ToString(p.PolicyName) == "s3ms-scaling-policy"
	}, "scaling policy s3ms-scaling-policy")
	assert.Equal(t, applicationautoscalingtypes52.PolicyTypeTargetTrackingScaling, policy.PolicyType)

	actionOut, err := client.DescribeScheduledActions(ctx, &applicationautoscalingsvc52.DescribeScheduledActionsInput{
		ServiceNamespace: applicationautoscalingtypes52.ServiceNamespaceDynamodb,
		ResourceId:       aws.String("table/s3ms-ddb"),
	})
	require.NoError(t, err, "DescribeScheduledActions should succeed")
	findBy(t, actionOut.ScheduledActions, func(a applicationautoscalingtypes52.ScheduledAction) bool {
		return aws.ToString(a.ScheduledActionName) == "s3ms-scheduled-action"
	}, "scheduled action s3ms-scheduled-action")
}

func verifyS3tablesMessagingAndStreamingBedrockAgent(ctx context.Context, t *testing.T) {
	t.Helper()
	client := bedrockagentsvc52.NewFromConfig(megaConfig(t), func(o *bedrockagentsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	listOut, err := client.ListPrompts(ctx, &bedrockagentsvc52.ListPromptsInput{})
	require.NoError(t, err, "ListPrompts should succeed")
	prompt := findBy(t, listOut.PromptSummaries, func(p bedrockagenttypes52.PromptSummary) bool {
		return aws.ToString(p.Name) == "s3ms-prompt"
	}, "prompt s3ms-prompt")

	getOut, err := client.GetPrompt(ctx, &bedrockagentsvc52.GetPromptInput{
		PromptIdentifier: prompt.Id,
	})
	require.NoError(t, err, "GetPrompt should succeed")
	assert.Equal(t, "s3ms-prompt", aws.ToString(getOut.Name))
}

func verifyS3tablesMessagingAndStreamingECR(ctx context.Context, t *testing.T) {
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

func verifyS3tablesMessagingAndStreamingGlacier(ctx context.Context, t *testing.T) {
	t.Helper()
	client := glaciersvc52.NewFromConfig(megaConfig(t), func(o *glaciersvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	descOut, err := client.DescribeVault(ctx, &glaciersvc52.DescribeVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String("s3ms-vault"),
	})
	require.NoError(t, err, "DescribeVault should succeed")
	assert.Equal(t, "s3ms-vault", aws.ToString(descOut.VaultName))

	lockOut, err := client.GetVaultLock(ctx, &glaciersvc52.GetVaultLockInput{
		AccountId: aws.String("-"),
		VaultName: aws.String("s3ms-vault"),
	})
	require.NoError(t, err, "GetVaultLock should succeed")
	assert.Equal(t, "InProgress", aws.ToString(lockOut.State))
}

func verifyS3tablesMessagingAndStreamingRolesAnywhere(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rolesanywheresvc52.NewFromConfig(megaConfig(t), func(o *rolesanywheresvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListProfiles(ctx, &rolesanywheresvc52.ListProfilesInput{})
	require.NoError(t, err, "ListProfiles should succeed")
	profile := findBy(t, out.Profiles, func(p rolesanywheretypes52.ProfileDetail) bool {
		return aws.ToString(p.Name) == "s3ms-profile"
	}, "profile s3ms-profile")
	assert.Contains(t, profile.RoleArns, "arn:aws:iam::000000000000:role/s3ms-rolesanywhere-role")
}

func verifyS3tablesMessagingAndStreamingRekognition(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rekognitionsvc52.NewFromConfig(megaConfig(t), func(o *rekognitionsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeProjects(ctx, &rekognitionsvc52.DescribeProjectsInput{
		ProjectNames: []string{"s3ms-project"},
	})
	require.NoError(t, err, "DescribeProjects should succeed")
	require.Len(t, out.ProjectDescriptions, 1)
	assert.Equal(t, "CREATED", string(out.ProjectDescriptions[0].Status))
}

func verifyS3tablesMessagingAndStreamingQuickSight(ctx context.Context, t *testing.T) {
	t.Helper()
	client := quicksightsvc52.NewFromConfig(megaConfig(t), func(o *quicksightsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	subOut, err := client.DescribeAccountSubscription(ctx, &quicksightsvc52.DescribeAccountSubscriptionInput{
		AwsAccountId: aws.String("000000000000"),
	})
	require.NoError(t, err, "DescribeAccountSubscription should succeed")
	require.NotNil(t, subOut.AccountInfo)
	assert.Equal(t, "s3ms-qs", aws.ToString(subOut.AccountInfo.AccountName))

	settingsOut, err := client.DescribeAccountSettings(ctx, &quicksightsvc52.DescribeAccountSettingsInput{
		AwsAccountId: aws.String("000000000000"),
	})
	require.NoError(t, err, "DescribeAccountSettings should succeed")
	require.NotNil(t, settingsOut.AccountSettings)
	assert.False(t, settingsOut.AccountSettings.TerminationProtectionEnabled)
}

func verifyS3tablesMessagingAndStreamingKinesis(ctx context.Context, t *testing.T) {
	t.Helper()
	client := kinesissvc52.NewFromConfig(megaConfig(t), func(o *kinesissvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/s3ms-stream"

	policyOut, err := client.GetResourcePolicy(ctx, &kinesissvc52.GetResourcePolicyInput{
		ResourceARN: aws.String(streamARN),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "kinesis:GetRecords")

	consumersOut, err := client.ListStreamConsumers(ctx, &kinesissvc52.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err, "ListStreamConsumers should succeed")
	assert.NotEmpty(t, consumersOut.Consumers, "consumer s3ms-consumer should be listed")
}

func verifyS3tablesMessagingAndStreamingLakeFormation(ctx context.Context, t *testing.T) {
	t.Helper()
	client := lakeformationsvc52.NewFromConfig(megaConfig(t), func(o *lakeformationsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	tagOut, err := client.GetLFTag(ctx, &lakeformationsvc52.GetLFTagInput{
		CatalogId: aws.String("000000000000"),
		TagKey:    aws.String("s3ms-tag"),
	})
	require.NoError(t, err, "GetLFTag should succeed")
	assert.ElementsMatch(t, []string{"blue", "green"}, tagOut.TagValues)

	resOut, err := client.GetResourceLFTags(ctx, &lakeformationsvc52.GetResourceLFTagsInput{
		Resource: &lakeformationtypes52.Resource{
			Database: &lakeformationtypes52.DatabaseResource{Name: aws.String("s3ms_db")},
		},
	})
	require.NoError(t, err, "GetResourceLFTags should succeed")
	require.NotEmpty(t, resOut.LFTagOnDatabase)
	assert.Equal(t, "s3ms-tag", aws.ToString(resOut.LFTagOnDatabase[0].TagKey))
}

func verifyS3tablesMessagingAndStreamingMediaStore(ctx context.Context, t *testing.T) {
	t.Helper()
	client := mediastoresvc52.NewFromConfig(megaConfig(t), func(o *mediastoresvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetContainerPolicy(ctx, &mediastoresvc52.GetContainerPolicyInput{
		ContainerName: aws.String("s3ms_container"),
	})
	require.NoError(t, err, "GetContainerPolicy should succeed")
	assert.Contains(t, aws.ToString(out.Policy), "mediastore:GetObject")
}

func verifyS3tablesMessagingAndStreamingS3Tables(ctx context.Context, t *testing.T) {
	t.Helper()
	client := s3tablessvc52.NewFromConfig(megaConfig(t), func(o *s3tablessvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/s3ms-tb"

	bucketPolicyOut, err := client.GetTableBucketPolicy(ctx, &s3tablessvc52.GetTableBucketPolicyInput{
		TableBucketARN: aws.String(bucketARN),
	})
	require.NoError(t, err, "GetTableBucketPolicy should succeed")
	assert.Contains(t, aws.ToString(bucketPolicyOut.ResourcePolicy), "s3tables:GetTableBucket")

	tablePolicyOut, err := client.GetTablePolicy(ctx, &s3tablessvc52.GetTablePolicyInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String("s3ms_ns"),
		Name:           aws.String("s3ms_table"),
	})
	require.NoError(t, err, "GetTablePolicy should succeed")
	assert.Contains(t, aws.ToString(tablePolicyOut.ResourcePolicy), "s3tables:GetTable")
}

func verifyS3tablesMessagingAndStreamingServiceDiscovery(ctx context.Context, t *testing.T) {
	t.Helper()
	client := servicediscoverysvc52.NewFromConfig(megaConfig(t), func(o *servicediscoverysvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListNamespaces(ctx, &servicediscoverysvc52.ListNamespacesInput{})
	require.NoError(t, err, "ListNamespaces should succeed")

	pub := findBy(t, out.Namespaces, func(ns servicediscoverytypes52.NamespaceSummary) bool {
		return aws.ToString(ns.Name) == "s3ms.example.com"
	}, "public namespace s3ms.example.com")
	assert.Equal(t, servicediscoverytypes52.NamespaceTypeDnsPublic, pub.Type)

	priv := findBy(t, out.Namespaces, func(ns servicediscoverytypes52.NamespaceSummary) bool {
		return aws.ToString(ns.Name) == "s3ms.private"
	}, "private namespace s3ms.private")
	assert.Equal(t, servicediscoverytypes52.NamespaceTypeDnsPrivate, priv.Type)
}

func verifyS3tablesMessagingAndStreamingSNS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := snssvc52.NewFromConfig(megaConfig(t), func(o *snssvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	smsOut, err := client.GetSMSAttributes(ctx, &snssvc52.GetSMSAttributesInput{})
	require.NoError(t, err, "GetSMSAttributes should succeed")
	assert.Equal(t, "Transactional", smsOut.Attributes["DefaultSMSType"])

	dppOut, err := client.GetDataProtectionPolicy(ctx, &snssvc52.GetDataProtectionPolicyInput{
		ResourceArn: aws.String("arn:aws:sns:us-east-1:000000000000:s3ms-topic"),
	})
	require.NoError(t, err, "GetDataProtectionPolicy should succeed")
	assert.Contains(t, aws.ToString(dppOut.DataProtectionPolicy), "s3ms-dpp")
}

func verifyS3tablesMessagingAndStreamingSQS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := sqssvc52.NewFromConfig(megaConfig(t), func(o *sqssvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	urlOut, err := client.GetQueueUrl(ctx, &sqssvc52.GetQueueUrlInput{
		QueueName: aws.String("s3ms-src"),
	})
	require.NoError(t, err, "GetQueueUrl should succeed")

	attrOut, err := client.GetQueueAttributes(ctx, &sqssvc52.GetQueueAttributesInput{
		QueueUrl:       urlOut.QueueUrl,
		AttributeNames: []sqstypes52.QueueAttributeName{sqstypes52.QueueAttributeNameRedrivePolicy},
	})
	require.NoError(t, err, "GetQueueAttributes should succeed")
	assert.Contains(t, attrOut.Attributes["RedrivePolicy"], "s3ms-dlq")
}

func verifyS3tablesMessagingAndStreamingTranscribe(ctx context.Context, t *testing.T) {
	t.Helper()
	client := transcribesvc52.NewFromConfig(megaConfig(t), func(o *transcribesvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.GetVocabularyFilter(ctx, &transcribesvc52.GetVocabularyFilterInput{
		VocabularyFilterName: aws.String("s3ms-filter"),
	})
	require.NoError(t, err, "GetVocabularyFilter should succeed")
	assert.Equal(t, "en-US", string(out.LanguageCode))
}

func verifyS3tablesMessagingAndStreamingCodePipeline(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codepipelinesvc52.NewFromConfig(megaConfig(t), func(o *codepipelinesvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListActionTypes(ctx, &codepipelinesvc52.ListActionTypesInput{
		ActionOwnerFilter: codepipelinetypes52.ActionOwnerCustom,
	})
	require.NoError(t, err, "ListActionTypes should succeed")
	action := findBy(t, out.ActionTypes, func(a codepipelinetypes52.ActionType) bool {
		return a.Id != nil && aws.ToString(a.Id.Provider) == "s3ms-provider"
	}, "custom action type s3ms-provider")
	assert.Equal(t, codepipelinetypes52.ActionCategoryBuild, action.Id.Category)
	assert.Equal(t, "1", aws.ToString(action.Id.Version))
}

func verifyS3tablesMessagingAndStreamingAPIGateway(ctx context.Context, t *testing.T) {
	t.Helper()
	client := apigatewaysvc52.NewFromConfig(megaConfig(t), func(o *apigatewaysvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	apisOut, err := client.GetRestApis(ctx, &apigatewaysvc52.GetRestApisInput{})
	require.NoError(t, err, "GetRestApis should succeed")
	api := findBy(t, apisOut.Items, func(a apigatewaytypes52.RestApi) bool {
		return aws.ToString(a.Name) == "s3ms-api"
	}, "rest api s3ms-api")

	resOut, err := client.GetResources(ctx, &apigatewaysvc52.GetResourcesInput{
		RestApiId: api.Id,
	})
	require.NoError(t, err, "GetResources should succeed")
	findBy(t, resOut.Items, func(r apigatewaytypes52.Resource) bool {
		return aws.ToString(r.Path) == "/s3ms"
	}, "resource /s3ms")
}

func verifyS3tablesMessagingAndStreamingELB(ctx context.Context, t *testing.T) {
	t.Helper()
	client := elbsvc52.NewFromConfig(megaConfig(t), func(o *elbsvc52.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeLoadBalancers(ctx, &elbsvc52.DescribeLoadBalancersInput{
		LoadBalancerNames: []string{"s3ms-lb"},
	})
	require.NoError(t, err, "DescribeLoadBalancers should succeed")
	require.Len(t, out.LoadBalancerDescriptions, 1)
	assert.Len(t, out.LoadBalancerDescriptions[0].Instances, 1)
}
