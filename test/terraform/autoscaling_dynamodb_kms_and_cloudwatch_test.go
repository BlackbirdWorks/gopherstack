package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	autoscalingsdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes2 "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes2 "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_AutoscalingDynamodbKmsAndCloudwatch provisions an Auto Scaling group with an
// attachment, traffic-source attachment, group tag, lifecycle hook,
// notification, scaling policy and scheduled action; a DynamoDB table with
// contributor insights, a Kinesis streaming destination, a resource policy,
// a table item, a tag and a table export; a KMS key with a ciphertext
// grant, key policy and an external key; and CloudWatch composite alarm,
// insight rule, managed insight rule and metric stream resources, verifying
// each through its own SDK client.
func TestTerraform_AutoscalingDynamodbKmsAndCloudwatch(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "autoscaling-dynamodb-kms-and-cloudwatch",
			providerFn: macie2ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				vars := vpcCIDRVars(t)
				vars["Endpoint"] = endpoint

				return vars
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyAutoscalingDynamodbKmsAndCloudwatchAutoScaling(ctx, t)
				verifyAutoscalingDynamodbKmsAndCloudwatchDynamoDB(ctx, t)
				verifyAutoscalingDynamodbKmsAndCloudwatchKMS(ctx, t)
				verifyAutoscalingDynamodbKmsAndCloudwatchCloudWatch(ctx, t)
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

func verifyAutoscalingDynamodbKmsAndCloudwatchAutoScaling(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := autoscalingsdk.NewFromConfig(cfg, func(o *autoscalingsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	groupsOut, err := client.DescribeAutoScalingGroups(ctx, &autoscalingsdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"adkc-asg"},
	})
	require.NoError(t, err, "DescribeAutoScalingGroups should succeed")
	require.Len(t, groupsOut.AutoScalingGroups, 1)

	group := groupsOut.AutoScalingGroups[0]
	require.Len(t, group.TargetGroupARNs, 1, "attachment should register a target group")

	foundTrafficSource := false

	for _, ts := range group.TrafficSources {
		if aws.ToString(ts.Type) == "elbv2" {
			foundTrafficSource = true
		}
	}

	assert.True(t, foundTrafficSource, "traffic source attachment should be listed")

	foundTag := false

	for _, tag := range group.Tags {
		if aws.ToString(tag.Key) == "adkc-tag" {
			foundTag = true

			assert.Equal(t, "true", aws.ToString(tag.Value))
		}
	}

	assert.True(t, foundTag, "group tag should be listed")

	hooksOut, err := client.DescribeLifecycleHooks(ctx, &autoscalingsdk.DescribeLifecycleHooksInput{
		AutoScalingGroupName: aws.String("adkc-asg"),
	})
	require.NoError(t, err, "DescribeLifecycleHooks should succeed")
	require.NotEmpty(t, hooksOut.LifecycleHooks)
	assert.Equal(t, "adkc-hook", aws.ToString(hooksOut.LifecycleHooks[0].LifecycleHookName))

	notifOut, err := client.DescribeNotificationConfigurations(
		ctx, &autoscalingsdk.DescribeNotificationConfigurationsInput{
			AutoScalingGroupNames: []string{"adkc-asg"},
		},
	)
	require.NoError(t, err, "DescribeNotificationConfigurations should succeed")
	require.NotEmpty(t, notifOut.NotificationConfigurations)

	policiesOut, err := client.DescribePolicies(ctx, &autoscalingsdk.DescribePoliciesInput{
		AutoScalingGroupName: aws.String("adkc-asg"),
	})
	require.NoError(t, err, "DescribePolicies should succeed")
	require.NotEmpty(t, policiesOut.ScalingPolicies)
	assert.Equal(t, "adkc-policy", aws.ToString(policiesOut.ScalingPolicies[0].PolicyName))

	schedOut, err := client.DescribeScheduledActions(ctx, &autoscalingsdk.DescribeScheduledActionsInput{
		AutoScalingGroupName: aws.String("adkc-asg"),
	})
	require.NoError(t, err, "DescribeScheduledActions should succeed")
	require.NotEmpty(t, schedOut.ScheduledUpdateGroupActions)
}

func verifyAutoscalingDynamodbKmsAndCloudwatchDynamoDB(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := ddbsdk.NewFromConfig(cfg, func(o *ddbsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	descOut, err := client.DescribeContributorInsights(ctx, &ddbsdk.DescribeContributorInsightsInput{
		TableName: aws.String("adkc-table"),
	})
	require.NoError(t, err, "DescribeContributorInsights should succeed")
	assert.NotEmpty(t, descOut.ContributorInsightsStatus)

	kinOut, err := client.DescribeKinesisStreamingDestination(ctx, &ddbsdk.DescribeKinesisStreamingDestinationInput{
		TableName: aws.String("adkc-table"),
	})
	require.NoError(t, err, "DescribeKinesisStreamingDestination should succeed")
	require.NotEmpty(t, kinOut.KinesisDataStreamDestinations)

	descTable, err := client.DescribeTable(ctx, &ddbsdk.DescribeTableInput{
		TableName: aws.String("adkc-table"),
	})
	require.NoError(t, err, "DescribeTable should succeed")
	tableArn := aws.ToString(descTable.Table.TableArn)

	policyOut, err := client.GetResourcePolicy(ctx, &ddbsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(tableArn),
	})
	require.NoError(t, err, "GetResourcePolicy should succeed")
	assert.NotEmpty(t, aws.ToString(policyOut.Policy))

	itemOut, err := client.GetItem(ctx, &ddbsdk.GetItemInput{
		TableName: aws.String("adkc-table"),
		Key: map[string]ddbtypes2.AttributeValue{
			"id": &ddbtypes2.AttributeValueMemberS{Value: "adkc-item"},
		},
	})
	require.NoError(t, err, "GetItem should succeed")
	require.NotEmpty(t, itemOut.Item)
	assert.Equal(t, "widget", itemOut.Item["name"].(*ddbtypes2.AttributeValueMemberS).Value)

	tagsOut, err := client.ListTagsOfResource(ctx, &ddbsdk.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	})
	require.NoError(t, err, "ListTagsOfResource should succeed")

	foundTag := false

	for _, tag := range tagsOut.Tags {
		if aws.ToString(tag.Key) == "adkc-tag" {
			foundTag = true
		}
	}

	assert.True(t, foundTag, "table tag should be listed")

	exportsOut, err := client.ListExports(ctx, &ddbsdk.ListExportsInput{
		TableArn: aws.String(tableArn),
	})
	require.NoError(t, err, "ListExports should succeed")
	require.NotEmpty(t, exportsOut.ExportSummaries)
}

func verifyAutoscalingDynamodbKmsAndCloudwatchKMS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	keysOut, err := client.ListKeys(ctx, &kmssdk.ListKeysInput{})
	require.NoError(t, err, "ListKeys should succeed")
	require.NotEmpty(t, keysOut.Keys)

	var keyID string

	for _, k := range keysOut.Keys {
		descOut, descErr := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: k.KeyId})
		require.NoError(t, descErr, "DescribeKey should succeed")

		if aws.ToString(descOut.KeyMetadata.Description) == "adkc kms key" {
			keyID = aws.ToString(k.KeyId)
		}
	}

	require.NotEmpty(t, keyID, "kms key should be listed")

	grantsOut, err := client.ListGrants(ctx, &kmssdk.ListGrantsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err, "ListGrants should succeed")
	require.NotEmpty(t, grantsOut.Grants)
	assert.Equal(t, "adkc-grant", aws.ToString(grantsOut.Grants[0].Name))

	policyOut, err := client.GetKeyPolicy(ctx, &kmssdk.GetKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
	})
	require.NoError(t, err, "GetKeyPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "adkc-key-policy")

	externalFound := false

	for _, k := range keysOut.Keys {
		descOut, descErr := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: k.KeyId})
		require.NoError(t, descErr, "DescribeKey should succeed")

		if aws.ToString(descOut.KeyMetadata.Description) == "adkc external key" {
			externalFound = true

			assert.Equal(t, "EXTERNAL", string(descOut.KeyMetadata.Origin))
		}
	}

	assert.True(t, externalFound, "external key should be listed")

	cksOut, err := client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
		CustomKeyStoreName: aws.String("adkc-cks"),
	})
	require.NoError(t, err, "DescribeCustomKeyStores should succeed")
	require.Len(t, cksOut.CustomKeyStores, 1)
	assert.Equal(t, "cluster-adkc", aws.ToString(cksOut.CustomKeyStores[0].CloudHsmClusterId))

	var replicaPrimaryKeyID, replicaExternalPrimaryKeyID string

	for _, k := range keysOut.Keys {
		descOut, descErr := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: k.KeyId})
		require.NoError(t, descErr, "DescribeKey should succeed")

		switch aws.ToString(descOut.KeyMetadata.Description) {
		case "adkc replica primary key":
			replicaPrimaryKeyID = aws.ToString(k.KeyId)
		case "adkc replica primary external key":
			replicaExternalPrimaryKeyID = aws.ToString(k.KeyId)
		}
	}

	require.NotEmpty(t, replicaPrimaryKeyID, "replica primary key should be listed")
	require.NotEmpty(t, replicaExternalPrimaryKeyID, "replica primary external key should be listed")

	primaryDesc, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(replicaPrimaryKeyID)})
	require.NoError(t, err, "DescribeKey on replica primary should succeed")
	require.NotNil(t, primaryDesc.KeyMetadata.MultiRegionConfiguration)
	require.Len(t, primaryDesc.KeyMetadata.MultiRegionConfiguration.ReplicaKeys, 1)

	replicaClient := kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Region = "us-west-2"
	})

	replicaKeyArn := aws.ToString(primaryDesc.KeyMetadata.MultiRegionConfiguration.ReplicaKeys[0].Arn)

	replicaDesc, err := replicaClient.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(replicaKeyArn)})
	require.NoError(t, err, "DescribeKey on replica should succeed")
	assert.Equal(t, "adkc replica key", aws.ToString(replicaDesc.KeyMetadata.Description))

	extPrimaryDesc, err := client.DescribeKey(
		ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(replicaExternalPrimaryKeyID)},
	)
	require.NoError(t, err, "DescribeKey on replica external primary should succeed")
	require.NotNil(t, extPrimaryDesc.KeyMetadata.MultiRegionConfiguration)
	require.Len(t, extPrimaryDesc.KeyMetadata.MultiRegionConfiguration.ReplicaKeys, 1)

	extReplicaKeyArn := aws.ToString(extPrimaryDesc.KeyMetadata.MultiRegionConfiguration.ReplicaKeys[0].Arn)

	extReplicaDesc, err := replicaClient.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(extReplicaKeyArn)})
	require.NoError(t, err, "DescribeKey on replica external key should succeed")
	assert.Equal(t, "adkc replica external key", aws.ToString(extReplicaDesc.KeyMetadata.Description))
	assert.Equal(t, "Enabled", string(extReplicaDesc.KeyMetadata.KeyState))
}

func verifyAutoscalingDynamodbKmsAndCloudwatchCloudWatch(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	alarmsOut, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{
		AlarmNames: []string{"adkc-composite-alarm"},
		AlarmTypes: []cwtypes2.AlarmType{cwtypes2.AlarmTypeCompositeAlarm},
	})
	require.NoError(t, err, "DescribeAlarms should succeed")
	require.Len(t, alarmsOut.CompositeAlarms, 1)
	assert.Contains(t, aws.ToString(alarmsOut.CompositeAlarms[0].AlarmRule), "adkc-leaf-a")

	rulesOut, err := client.DescribeInsightRules(ctx, &cwsdk.DescribeInsightRulesInput{})
	require.NoError(t, err, "DescribeInsightRules should succeed")

	foundRule := false

	for _, r := range rulesOut.InsightRules {
		if aws.ToString(r.Name) == "adkc-insight-rule" {
			foundRule = true
		}
	}

	assert.True(t, foundRule, "contributor insight rule should be listed")

	managedOut, err := client.ListManagedInsightRules(ctx, &cwsdk.ListManagedInsightRulesInput{
		ResourceARN: descTableArn(ctx, t),
	})
	require.NoError(t, err, "ListManagedInsightRules should succeed")
	require.NotEmpty(t, managedOut.ManagedRules)

	streamsOut, err := client.ListMetricStreams(ctx, &cwsdk.ListMetricStreamsInput{})
	require.NoError(t, err, "ListMetricStreams should succeed")

	foundStream := false

	for _, s := range streamsOut.Entries {
		if aws.ToString(s.Name) == "adkc-metric-stream" {
			foundStream = true
		}
	}

	assert.True(t, foundStream, "metric stream should be listed")
}

// descTableArn resolves the adkc DynamoDB table's ARN for the
// CloudWatch managed insight rule lookup.
func descTableArn(ctx context.Context, t *testing.T) *string {
	t.Helper()
	cfg := megaConfig(t)

	client := ddbsdk.NewFromConfig(cfg, func(o *ddbsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeTable(ctx, &ddbsdk.DescribeTableInput{
		TableName: aws.String("adkc-table"),
	})
	require.NoError(t, err, "DescribeTable should succeed")

	return out.Table.TableArn
}
