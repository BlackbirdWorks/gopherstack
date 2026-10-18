package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstanceLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)

	out, err := f.client.CreateInstance(t.Context(), &sdk.CreateInstanceInput{
		IdentityManagementType: types.DirectoryTypeConnectManaged,
		InstanceAlias:          aws.String("acme"),
		InboundCallsEnabled:    aws.Bool(true),
		OutboundCallsEnabled:   aws.Bool(false),
		Tags:                   map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:connect:us-east-1:123456789012:instance/"+aws.ToString(out.Id), aws.ToString(out.Arn))

	got, err := f.client.DescribeInstance(t.Context(), &sdk.DescribeInstanceInput{InstanceId: out.Id})
	require.NoError(t, err)
	assert.Equal(t, types.InstanceStatusCreationInProgress, got.Instance.InstanceStatus)
	assert.Equal(t, "https://acme.my.connect.aws", aws.ToString(got.Instance.InstanceAccessUrl))
	assert.True(t, aws.ToBool(got.Instance.InboundCallsEnabled))
	assert.False(t, aws.ToBool(got.Instance.OutboundCallsEnabled))
	assert.Equal(t, map[string]string{"env": "test"}, got.Instance.Tags)

	f.clock.advance(2 * time.Minute)

	got, err = f.client.DescribeInstance(t.Context(), &sdk.DescribeInstanceInput{InstanceId: out.Arn})
	require.NoError(t, err)
	assert.Equal(t, types.InstanceStatusActive, got.Instance.InstanceStatus)

	list, err := f.client.ListInstances(t.Context(), &sdk.ListInstancesInput{})
	require.NoError(t, err)
	require.Len(t, list.InstanceSummaryList, 1)
	assert.Equal(t, "acme", aws.ToString(list.InstanceSummaryList[0].InstanceAlias))

	_, err = f.client.DeleteInstance(t.Context(), &sdk.DeleteInstanceInput{InstanceId: out.Id})
	require.NoError(t, err)

	_, err = f.client.DescribeInstance(t.Context(), &sdk.DescribeInstanceInput{InstanceId: out.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestCreateInstanceValidation(t *testing.T) {
	t.Parallel()

	base := func() *sdk.CreateInstanceInput {
		return &sdk.CreateInstanceInput{
			IdentityManagementType: types.DirectoryTypeConnectManaged, InstanceAlias: aws.String("ok-alias"),
			InboundCallsEnabled: aws.Bool(true), OutboundCallsEnabled: aws.Bool(true),
		}
	}

	tests := []struct {
		mutate func(in *sdk.CreateInstanceInput)
		name   string
		code   string
	}{
		{
			name:   "no alias",
			mutate: func(in *sdk.CreateInstanceInput) { in.InstanceAlias = nil },
			code:   "InvalidParameterException",
		},
		{
			name:   "bad alias",
			mutate: func(in *sdk.CreateInstanceInput) { in.InstanceAlias = aws.String("Bad_Alias") },
			code:   "InvalidParameterException",
		},
		{name: "directory needed", mutate: func(in *sdk.CreateInstanceInput) {
			in.IdentityManagementType = types.DirectoryTypeExistingDirectory
		}, code: "InvalidParameterException"},
		{
			name:   "bad type",
			mutate: func(in *sdk.CreateInstanceInput) { in.IdentityManagementType = "LDAP" },
			code:   "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := base()
			tt.mutate(in)
			_, err := newFixture(t).client.CreateInstance(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}

	f := newFixture(t)
	f.newInstance(t, "taken")
	_, err := f.client.CreateInstance(t.Context(), func() *sdk.CreateInstanceInput {
		in := base()
		in.InstanceAlias = aws.String("taken")

		return in
	}())
	assert.Equal(t, "DuplicateResourceException", apiCode(err))
}

func TestInstanceDefaults(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "defaults")

	profiles, err := f.client.ListSecurityProfiles(
		t.Context(),
		&sdk.ListSecurityProfilesInput{InstanceId: aws.String(inst)},
	)
	require.NoError(t, err)

	names := make([]string, 0, len(profiles.SecurityProfileSummaryList))
	for _, p := range profiles.SecurityProfileSummaryList {
		names = append(names, aws.ToString(p.Name))
	}

	assert.ElementsMatch(t, []string{"Admin", "Agent", "CallCenterManager", "QualityAnalyst"}, names)

	statuses, err := f.client.ListAgentStatuses(t.Context(), &sdk.ListAgentStatusesInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	assert.Len(t, statuses.AgentStatusSummaryList, 2)

	queueID, _, profileID := f.defaultQueue(t, inst)
	rpq, err := f.client.ListRoutingProfileQueues(
		t.Context(),
		&sdk.ListRoutingProfileQueuesInput{InstanceId: aws.String(inst), RoutingProfileId: aws.String(profileID)},
	)
	require.NoError(t, err)
	assert.Len(t, rpq.RoutingProfileQueueConfigSummaryList, 3)
	assert.Equal(t, queueID, aws.ToString(rpq.RoutingProfileQueueConfigSummaryList[0].QueueId))

	rp, err := f.client.DescribeRoutingProfile(
		t.Context(),
		&sdk.DescribeRoutingProfileInput{InstanceId: aws.String(inst), RoutingProfileId: aws.String(profileID)},
	)
	require.NoError(t, err)
	assert.True(t, rp.RoutingProfile.IsDefault)
	assert.EqualValues(t, 1, aws.ToInt64(rp.RoutingProfile.NumberOfAssociatedQueues))
}

func TestInstanceAttributes(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "attrs"))

	got, err := f.client.DescribeInstanceAttribute(t.Context(), &sdk.DescribeInstanceAttributeInput{
		InstanceId: inst, AttributeType: types.InstanceAttributeTypeContactflowLogs,
	})
	require.NoError(t, err)
	assert.Equal(t, "false", aws.ToString(got.Attribute.Value))

	_, err = f.client.UpdateInstanceAttribute(t.Context(), &sdk.UpdateInstanceAttributeInput{
		InstanceId: inst, AttributeType: types.InstanceAttributeTypeContactflowLogs, Value: aws.String("true"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateInstanceAttribute(t.Context(), &sdk.UpdateInstanceAttributeInput{
		InstanceId: inst, AttributeType: types.InstanceAttributeTypeOutboundCalls, Value: aws.String("false"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateInstanceAttribute(t.Context(), &sdk.UpdateInstanceAttributeInput{
		InstanceId: inst, AttributeType: types.InstanceAttributeTypeEarlyMedia, Value: aws.String("maybe"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	list, err := f.client.ListInstanceAttributes(t.Context(), &sdk.ListInstanceAttributesInput{InstanceId: inst})
	require.NoError(t, err)

	values := make(map[types.InstanceAttributeType]string)
	for _, a := range list.Attributes {
		values[a.AttributeType] = aws.ToString(a.Value)
	}

	assert.Equal(t, "true", values[types.InstanceAttributeTypeContactflowLogs])
	assert.Equal(t, "false", values[types.InstanceAttributeTypeOutboundCalls])

	inst2, err := f.client.DescribeInstance(t.Context(), &sdk.DescribeInstanceInput{InstanceId: inst})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(inst2.Instance.OutboundCallsEnabled))
}

func TestStorageConfigs(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetResolvers(fakeResolvers{buckets: map[string]bool{"rec-bucket": true}})
	inst := aws.String(f.newInstance(t, "storage"))

	s3 := func(bucket string) *types.InstanceStorageConfig {
		return &types.InstanceStorageConfig{
			StorageType: types.StorageTypeS3,
			S3Config:    &types.S3Config{BucketName: aws.String(bucket), BucketPrefix: aws.String("calls")},
		}
	}

	_, err := f.client.AssociateInstanceStorageConfig(t.Context(), &sdk.AssociateInstanceStorageConfigInput{
		InstanceId: inst, ResourceType: types.InstanceStorageResourceTypeCallRecordings, StorageConfig: s3("missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.AssociateInstanceStorageConfig(t.Context(), &sdk.AssociateInstanceStorageConfigInput{
		InstanceId: inst, ResourceType: types.InstanceStorageResourceTypeMediaStreams, StorageConfig: s3("rec-bucket"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	assoc, err := f.client.AssociateInstanceStorageConfig(t.Context(), &sdk.AssociateInstanceStorageConfigInput{
		InstanceId:    inst,
		ResourceType:  types.InstanceStorageResourceTypeCallRecordings,
		StorageConfig: s3("rec-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, aws.ToString(assoc.AssociationId), 64)

	_, err = f.client.AssociateInstanceStorageConfig(t.Context(), &sdk.AssociateInstanceStorageConfigInput{
		InstanceId:    inst,
		ResourceType:  types.InstanceStorageResourceTypeCallRecordings,
		StorageConfig: s3("rec-bucket"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.DescribeInstanceStorageConfig(t.Context(), &sdk.DescribeInstanceStorageConfigInput{
		InstanceId:    inst,
		AssociationId: assoc.AssociationId,
		ResourceType:  types.InstanceStorageResourceTypeCallRecordings,
	})
	require.NoError(t, err)
	assert.Equal(t, "rec-bucket", aws.ToString(got.StorageConfig.S3Config.BucketName))
	assert.Equal(t, assoc.AssociationId, got.StorageConfig.AssociationId)

	cfg := s3("rec-bucket")
	cfg.S3Config.BucketPrefix = aws.String("v2")
	_, err = f.client.UpdateInstanceStorageConfig(t.Context(), &sdk.UpdateInstanceStorageConfigInput{
		InstanceId:    inst,
		AssociationId: assoc.AssociationId,
		ResourceType:  types.InstanceStorageResourceTypeCallRecordings,
		StorageConfig: cfg,
	})
	require.NoError(t, err)

	list, err := f.client.ListInstanceStorageConfigs(t.Context(), &sdk.ListInstanceStorageConfigsInput{
		InstanceId: inst, ResourceType: types.InstanceStorageResourceTypeCallRecordings,
	})
	require.NoError(t, err)
	require.Len(t, list.StorageConfigs, 1)
	assert.Equal(t, "v2", aws.ToString(list.StorageConfigs[0].S3Config.BucketPrefix))

	_, err = f.client.DisassociateInstanceStorageConfig(t.Context(), &sdk.DisassociateInstanceStorageConfigInput{
		InstanceId:    inst,
		AssociationId: assoc.AssociationId,
		ResourceType:  types.InstanceStorageResourceTypeCallRecordings,
	})
	require.NoError(t, err)

	list, err = f.client.ListInstanceStorageConfigs(t.Context(), &sdk.ListInstanceStorageConfigsInput{
		InstanceId: inst, ResourceType: types.InstanceStorageResourceTypeCallRecordings,
	})
	require.NoError(t, err)
	assert.Empty(t, list.StorageConfigs)
}

func TestInstanceAssociations(t *testing.T) {
	t.Parallel()

	const fn = "arn:aws:lambda:us-east-1:123456789012:function:hello"

	f := newFixture(t)
	f.backend.SetResolvers(fakeResolvers{lambdas: map[string]bool{fn: true}})
	inst := aws.String(f.newInstance(t, "assoc"))

	_, err := f.client.AssociateLambdaFunction(t.Context(), &sdk.AssociateLambdaFunctionInput{
		InstanceId: inst, FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:ghost"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.AssociateLambdaFunction(
		t.Context(),
		&sdk.AssociateLambdaFunctionInput{InstanceId: inst, FunctionArn: aws.String(fn)},
	)
	require.NoError(t, err)

	fns, err := f.client.ListLambdaFunctions(t.Context(), &sdk.ListLambdaFunctionsInput{InstanceId: inst})
	require.NoError(t, err)
	assert.Equal(t, []string{fn}, fns.LambdaFunctions)

	_, err = f.client.DisassociateLambdaFunction(
		t.Context(),
		&sdk.DisassociateLambdaFunctionInput{InstanceId: inst, FunctionArn: aws.String(fn)},
	)
	require.NoError(t, err)

	_, err = f.client.AssociateApprovedOrigin(
		t.Context(),
		&sdk.AssociateApprovedOriginInput{InstanceId: inst, Origin: aws.String("http://insecure.example.com")},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.AssociateApprovedOrigin(
		t.Context(),
		&sdk.AssociateApprovedOriginInput{InstanceId: inst, Origin: aws.String("https://app.example.com")},
	)
	require.NoError(t, err)

	origins, err := f.client.ListApprovedOrigins(t.Context(), &sdk.ListApprovedOriginsInput{InstanceId: inst})
	require.NoError(t, err)
	assert.Equal(t, []string{"https://app.example.com"}, origins.Origins)

	_, err = f.client.AssociateLexBot(
		t.Context(),
		&sdk.AssociateLexBotInput{
			InstanceId: inst,
			LexBot:     &types.LexBot{Name: aws.String("Orders"), LexRegion: aws.String("us-east-1")},
		},
	)
	require.NoError(t, err)

	_, err = f.client.AssociateBot(t.Context(), &sdk.AssociateBotInput{
		InstanceId: inst,
		LexV2Bot: &types.LexV2Bot{
			AliasArn: aws.String("arn:aws:lex:us-east-1:123456789012:bot-alias/ABCDEFGHIJ/KLMNOPQRST"),
		},
	})
	require.NoError(t, err)

	v1, err := f.client.ListLexBots(t.Context(), &sdk.ListLexBotsInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, v1.LexBots, 1)
	assert.Equal(t, "Orders", aws.ToString(v1.LexBots[0].Name))

	v2, err := f.client.ListBots(t.Context(), &sdk.ListBotsInput{InstanceId: inst, LexVersion: types.LexVersionV2})
	require.NoError(t, err)
	require.Len(t, v2.LexBots, 1)
	assert.NotNil(t, v2.LexBots[0].LexV2Bot)

	_, err = f.client.DisassociateLexBot(
		t.Context(),
		&sdk.DisassociateLexBotInput{
			InstanceId: inst,
			BotName:    aws.String("Orders"),
			LexRegion:  aws.String("us-east-1"),
		},
	)
	require.NoError(t, err)

	key := "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----"
	sk, err := f.client.AssociateSecurityKey(
		t.Context(),
		&sdk.AssociateSecurityKeyInput{InstanceId: inst, Key: aws.String(key)},
	)
	require.NoError(t, err)

	keys, err := f.client.ListSecurityKeys(t.Context(), &sdk.ListSecurityKeysInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, keys.SecurityKeys, 1)
	assert.NotNil(t, keys.SecurityKeys[0].CreationTime)

	_, err = f.client.DisassociateSecurityKey(
		t.Context(),
		&sdk.DisassociateSecurityKeyInput{InstanceId: inst, AssociationId: sk.AssociationId},
	)
	require.NoError(t, err)
}
