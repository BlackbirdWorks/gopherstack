package replicator_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

func TestReplicateExtras(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(*testing.T, pair)
		check func(*testing.T, pair, *replicator.Report)
		name  string
		types []string
	}{
		{
			name:  "lambda versions and aliases",
			types: []string{"lambda"},
			seed:  seedLambdaVersions,
			check: checkLambdaVersions,
		},
		{
			name: "lambda event source mappings", types: []string{"sqs", "kinesis", "dynamodb", "lambda"},
			seed: seedESM, check: checkESM,
		},
		{name: "lambda vpc missing in target", types: []string{"lambda"}, seed: seedVPCMissing, check: checkVPCMissing},
		{name: "lambda vpc present in target", types: []string{"lambda"}, seed: seedVPCPresent, check: checkVPCPresent},
		{name: "lambda container image", types: []string{"lambda"}, seed: seedImage, check: checkImage},
		{name: "iam users groups profiles", types: []string{"iam"}, seed: seedIAMPrincipals, check: checkIAMPrincipals},
		{
			name:  "secrets versions and kms",
			types: []string{"kms", "secretsmanager"},
			seed:  seedSecretVersions,
			check: checkSecretVersions,
		},
		{name: "eventbridge ecs and batch", types: []string{"eventbridge"}, seed: seedEBParams, check: checkEBParams},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newPair(t)
			tt.seed(t, p)

			cfg := p.config(tt.types...)

			rep, err := replicator.Run(t.Context(), cfg)
			require.NoError(t, err)
			require.False(t, rep.Failed(), "%+v", rep.Results)
			tt.check(t, p, rep)

			rerun, err := replicator.Run(t.Context(), cfg)
			require.NoError(t, err)
			require.False(t, rerun.Failed(), "rerun: %+v", rerun.Results)
			assert.Zero(t, rerun.Counts[replicator.ActionCreated], "rerun created: %+v", rerun.Results)
		})
	}
}

func putCode(t *testing.T, p pair) {
	t.Helper()

	c := s3Client(p, true)
	_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("code")})
	require.NoError(t, err)

	_, err = c.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("code"), Key: aws.String("fn.zip"), Body: strings.NewReader(fnZip),
	})
	require.NoError(t, err)
}

func createFn(t *testing.T, p pair, name string, mutate func(*lambda.CreateFunctionInput)) {
	t.Helper()

	in := &lambda.CreateFunctionInput{
		FunctionName: aws.String(name), Runtime: lambdatypes.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::" + srcAccount + ":role/app-role"),
		Code: &lambdatypes.FunctionCode{S3Bucket: aws.String("code"), S3Key: aws.String("fn.zip")},
	}
	if mutate != nil {
		mutate(in)
	}

	_, err := lambda.NewFromConfig(p.srcCfg).CreateFunction(t.Context(), in)
	require.NoError(t, err)
}

func seedLambdaVersions(t *testing.T, p pair) {
	t.Helper()
	putCode(t, p)
	createFn(t, p, "worker", nil)

	c := lambda.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	_, err := c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("worker")})
	require.NoError(t, err)

	_, err = c.UpdateFunctionConfiguration(ctx, &lambda.UpdateFunctionConfigurationInput{
		FunctionName: aws.String("worker"), Timeout: aws.Int32(30),
	})
	require.NoError(t, err)

	_, err = c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("worker")})
	require.NoError(t, err)

	_, err = c.CreateAlias(ctx, &lambda.CreateAliasInput{
		FunctionName: aws.String("worker"), Name: aws.String("live"), FunctionVersion: aws.String("2"),
		RoutingConfig: &lambdatypes.AliasRoutingConfiguration{AdditionalVersionWeights: map[string]float64{"1": 0.25}},
	})
	require.NoError(t, err)
}

func checkLambdaVersions(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	a := actions(rep)
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker:1"])
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker:2"])
	assert.Equal(t, replicator.ActionCreated, a["lambda/worker:live"])

	c := lambda.NewFromConfig(p.dstCfg)

	vs, err := c.ListVersionsByFunction(
		t.Context(),
		&lambda.ListVersionsByFunctionInput{FunctionName: aws.String("worker")},
	)
	require.NoError(t, err)

	got := map[string]int32{}
	for _, v := range vs.Versions {
		got[aws.ToString(v.Version)] = aws.ToInt32(v.Timeout)
	}

	assert.Contains(t, got, "1")
	assert.EqualValues(t, 30, got["2"])
	assert.EqualValues(t, 30, got[latest], "$LATEST restored")

	al, err := c.GetAlias(
		t.Context(),
		&lambda.GetAliasInput{FunctionName: aws.String("worker"), Name: aws.String("live")},
	)
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(al.FunctionVersion))
	assert.InDelta(t, 0.25, al.RoutingConfig.AdditionalVersionWeights["1"], 0.0001)
}

const latest = "$LATEST"

func seedESM(t *testing.T, p pair) {
	t.Helper()
	putCode(t, p)
	createFn(t, p, "consumer", nil)

	ctx := t.Context()
	c := lambda.NewFromConfig(p.srcCfg)

	q, err := sqs.NewFromConfig(p.srcCfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("events-q")})
	require.NoError(t, err)

	attrs, err := sqs.NewFromConfig(p.srcCfg).GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	_, err = c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{
		FunctionName: aws.String("consumer"), EventSourceArn: aws.String(attrs.Attributes["QueueArn"]),
		BatchSize: aws.Int32(5), Enabled: aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = kinesis.NewFromConfig(p.srcCfg).CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String("feed"), ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	ks, err := kinesis.NewFromConfig(p.srcCfg).DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String("feed"),
	})
	require.NoError(t, err)

	_, err = c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{
		FunctionName: aws.String("consumer"), EventSourceArn: ks.StreamDescriptionSummary.StreamARN,
		StartingPosition: lambdatypes.EventSourcePositionLatest, BatchSize: aws.Int32(50),
	})
	require.NoError(t, err)

	seedStreamTable(t, p, c)
}

func seedStreamTable(t *testing.T, p pair, c *lambda.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := dynamodb.NewFromConfig(p.srcCfg).CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("events"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	desc, err := dynamodb.NewFromConfig(p.srcCfg).
		DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("events")})
	require.NoError(t, err)
	require.NotNil(t, desc.Table.LatestStreamArn)

	_, err = c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{
		FunctionName: aws.String("consumer"), EventSourceArn: desc.Table.LatestStreamArn,
		StartingPosition: lambdatypes.EventSourcePositionTrimHorizon,
	})
	require.NoError(t, err)
}

func checkESM(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	out, err := lambda.NewFromConfig(p.dstCfg).
		ListEventSourceMappings(t.Context(), &lambda.ListEventSourceMappingsInput{
			FunctionName: aws.String("consumer"),
		})
	require.NoError(t, err)
	require.Len(t, out.EventSourceMappings, 3)

	for _, m := range out.EventSourceMappings {
		src := aws.ToString(m.EventSourceArn)
		assert.NotContains(t, src, srcAccount)

		if strings.Contains(src, ":sqs:") {
			assert.Contains(t, src, dstAccount)
			assert.EqualValues(t, 5, aws.ToInt32(m.BatchSize))
			assert.Equal(t, "Disabled", aws.ToString(m.State))
		}
	}
}

func seedVPCMissing(t *testing.T, p pair) {
	t.Helper()
	putCode(t, p)
	createFn(t, p, "netfn", func(in *lambda.CreateFunctionInput) {
		in.VpcConfig = &lambdatypes.VpcConfig{SubnetIds: []string{"subnet-0abc"}, SecurityGroupIds: []string{"sg-0abc"}}
	})
}

func checkVPCMissing(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.Equal(t, replicator.ActionSkipped, actions(rep)["lambda/netfn:vpc"])

	got, err := lambda.NewFromConfig(p.dstCfg).
		GetFunction(t.Context(), &lambda.GetFunctionInput{FunctionName: aws.String("netfn")})
	require.NoError(t, err)

	if got.Configuration.VpcConfig != nil {
		assert.Empty(t, got.Configuration.VpcConfig.SubnetIds)
	}
}

func seedVPCPresent(t *testing.T, p pair) {
	t.Helper()
	putCode(t, p)

	e := ec2.NewFromConfig(p.dstCfg)
	vpc, err := e.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)

	sn, err := e.CreateSubnet(t.Context(), &ec2.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)

	sg, err := e.CreateSecurityGroup(t.Context(), &ec2.CreateSecurityGroupInput{
		GroupName: aws.String("fn"), Description: aws.String("fn"), VpcId: vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	createFn(t, p, "netfn", func(in *lambda.CreateFunctionInput) {
		in.VpcConfig = &lambdatypes.VpcConfig{
			SubnetIds: []string{aws.ToString(sn.Subnet.SubnetId)}, SecurityGroupIds: []string{aws.ToString(sg.GroupId)},
		}
	})
}

func checkVPCPresent(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.NotContains(t, actions(rep), "lambda/netfn:vpc")

	got, err := lambda.NewFromConfig(p.dstCfg).
		GetFunction(t.Context(), &lambda.GetFunctionInput{FunctionName: aws.String("netfn")})
	require.NoError(t, err)
	require.NotNil(t, got.Configuration.VpcConfig)
	assert.Len(t, got.Configuration.VpcConfig.SubnetIds, 1)
	assert.Len(t, got.Configuration.VpcConfig.SecurityGroupIds, 1)
}

func seedImage(t *testing.T, p pair) {
	t.Helper()
	createFn(t, p, "imgfn", func(in *lambda.CreateFunctionInput) {
		in.PackageType = lambdatypes.PackageTypeImage
		in.Runtime = ""
		in.Handler = nil
		in.Code = &lambdatypes.FunctionCode{ImageUri: aws.String("123456789012.dkr.ecr.us-east-1.amazonaws.com/app:1")}
	})
}

func checkImage(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.Equal(t, replicator.ActionSkipped, actions(rep)["lambda/imgfn"])

	for _, r := range rep.Results {
		if r.ID == "imgfn" {
			assert.Contains(t, r.Reason, "ECR")
		}
	}

	_, err := lambda.NewFromConfig(p.dstCfg).
		GetFunction(t.Context(), &lambda.GetFunctionInput{FunctionName: aws.String("imgfn")})
	require.Error(t, err)
}

func seedIAMPrincipals(t *testing.T, p pair) {
	t.Helper()
	seedIAM(t, p)

	c := iam.NewFromConfig(p.srcCfg)
	ctx := t.Context()
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*",` +
		`"Resource":"arn:aws:sqs:us-east-1:` + srcAccount + `:q"}]}`

	_, err := c.CreateGroup(ctx, &iam.CreateGroupInput{GroupName: aws.String("devs")})
	require.NoError(t, err)

	_, err = c.PutGroupPolicy(ctx, &iam.PutGroupPolicyInput{
		GroupName: aws.String("devs"), PolicyName: aws.String("g-inline"), PolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)

	_, err = c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	_, err = c.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName: aws.String("alice"), PolicyName: aws.String("u-inline"), PolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)

	_, err = c.AddUserToGroup(
		ctx,
		&iam.AddUserToGroupInput{UserName: aws.String("alice"), GroupName: aws.String("devs")},
	)
	require.NoError(t, err)

	_, err = c.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	_, err = c.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{InstanceProfileName: aws.String("app-prof")})
	require.NoError(t, err)

	_, err = c.AddRoleToInstanceProfile(ctx, &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String("app-prof"), RoleName: aws.String("app-role"),
	})
	require.NoError(t, err)
}

func checkIAMPrincipals(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	a := actions(rep)
	assert.Equal(t, replicator.ActionCreated, a["iam/user/alice"])
	assert.Equal(t, replicator.ActionCreated, a["iam/group/devs"])
	assert.Equal(t, replicator.ActionCreated, a["iam/instance-profile/app-prof"])

	c := iam.NewFromConfig(p.dstCfg)
	ctx := t.Context()

	up, err := c.GetUserPolicy(
		ctx,
		&iam.GetUserPolicyInput{UserName: aws.String("alice"), PolicyName: aws.String("u-inline")},
	)
	require.NoError(t, err)
	upDoc, err := url.QueryUnescape(aws.ToString(up.PolicyDocument))
	require.NoError(t, err)
	assert.Contains(t, upDoc, dstAccount)

	gp, err := c.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)
	require.Len(t, gp.Groups, 1)
	assert.Equal(t, "devs", aws.ToString(gp.Groups[0].GroupName))

	_, err = c.GetGroupPolicy(
		ctx,
		&iam.GetGroupPolicyInput{GroupName: aws.String("devs"), PolicyName: aws.String("g-inline")},
	)
	require.NoError(t, err)

	ip, err := c.GetInstanceProfile(ctx, &iam.GetInstanceProfileInput{InstanceProfileName: aws.String("app-prof")})
	require.NoError(t, err)
	require.Len(t, ip.InstanceProfile.Roles, 1)

	keys, err := c.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String("alice")})
	require.NoError(t, err)
	assert.Empty(t, keys.AccessKeyMetadata, "access keys must never be copied")
}

func seedSecretVersions(t *testing.T, p pair) {
	t.Helper()
	seedKMS(t, p)

	k, err := kms.NewFromConfig(p.srcCfg).
		DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: aws.String("alias/app")})
	require.NoError(t, err)

	c := secretsmanager.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	first, err := c.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name: aws.String("db/pass"), SecretString: aws.String("v1"), KmsKeyId: k.KeyMetadata.KeyId,
	})
	require.NoError(t, err)

	for _, v := range []string{"v2", "v3"} {
		_, err = c.PutSecretValue(
			ctx,
			&secretsmanager.PutSecretValueInput{SecretId: aws.String("db/pass"), SecretString: aws.String(v)},
		)
		require.NoError(t, err)
	}

	_, err = c.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
		SecretId: aws.String("db/pass"), VersionStage: aws.String("blue"), MoveToVersionId: first.VersionId,
	})
	require.NoError(t, err)
}

func checkSecretVersions(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	c := secretsmanager.NewFromConfig(p.dstCfg)
	ctx := t.Context()

	want := map[string]string{"AWSCURRENT": "v3", "AWSPREVIOUS": "v2", "blue": "v1"}

	for stage, val := range want {
		got, err := c.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
			SecretId: aws.String("db/pass"), VersionStage: aws.String(stage),
		})
		require.NoError(t, err, stage)
		assert.Equal(t, val, aws.ToString(got.SecretString), stage)
	}

	d, err := c.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String("db/pass")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(d.KmsKeyId), dstKeyID(t, p))
}

func dstKeyID(t *testing.T, p pair) string {
	t.Helper()

	k, err := kms.NewFromConfig(p.dstCfg).
		DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: aws.String("alias/app")})
	require.NoError(t, err)

	return aws.ToString(k.KeyMetadata.KeyId)
}

func seedEBParams(t *testing.T, p pair) {
	t.Helper()

	c := eventbridge.NewFromConfig(p.srcCfg)
	ctx := t.Context()
	role := "arn:aws:iam::" + srcAccount + ":role/events"

	_, err := c.PutRule(ctx, &eventbridge.PutRuleInput{
		Name: aws.String("jobs"), ScheduleExpression: aws.String("rate(1 hour)"), State: ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = c.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule: aws.String("jobs"),
		Targets: []ebtypes.Target{
			{
				Id: aws.String(
					"ecs",
				), Arn: aws.String("arn:aws:ecs:us-east-1:" + srcAccount + ":cluster/c1"), RoleArn: &role,
				EcsParameters: &ebtypes.EcsParameters{
					TaskDefinitionArn: aws.String("arn:aws:ecs:us-east-1:" + srcAccount + ":task-definition/web:3"),
					TaskCount:         aws.Int32(2), LaunchType: ebtypes.LaunchTypeFargate,
					NetworkConfiguration: &ebtypes.NetworkConfiguration{
						AwsvpcConfiguration: &ebtypes.AwsVpcConfiguration{
							Subnets: []string{"subnet-1"}, AssignPublicIp: ebtypes.AssignPublicIpEnabled,
						},
					},
				},
			},
			{
				Id: aws.String(
					"batch",
				), Arn: aws.String("arn:aws:batch:us-east-1:" + srcAccount + ":job-queue/q1"), RoleArn: &role,
				BatchParameters: &ebtypes.BatchParameters{
					JobDefinition:   aws.String("arn:aws:batch:us-east-1:" + srcAccount + ":job-definition/jd:1"),
					JobName:         aws.String("nightly"),
					ArrayProperties: &ebtypes.BatchArrayProperties{Size: 4},
				},
			},
		},
	})
	require.NoError(t, err)
}

func checkEBParams(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	out, err := eventbridge.NewFromConfig(p.dstCfg).ListTargetsByRule(t.Context(), &eventbridge.ListTargetsByRuleInput{
		Rule: aws.String("jobs"),
	})
	require.NoError(t, err)

	byID := map[string]ebtypes.Target{}
	for _, tg := range out.Targets {
		byID[aws.ToString(tg.Id)] = tg
	}

	require.Len(t, byID, 2)

	ecs := byID["ecs"].EcsParameters
	require.NotNil(t, ecs)
	assert.Equal(t, "arn:aws:ecs:us-east-1:"+dstAccount+":task-definition/web:3", aws.ToString(ecs.TaskDefinitionArn))
	assert.EqualValues(t, 2, aws.ToInt32(ecs.TaskCount))
	assert.Equal(t, ebtypes.LaunchTypeFargate, ecs.LaunchType)
	require.NotNil(t, ecs.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-1"}, ecs.NetworkConfiguration.AwsvpcConfiguration.Subnets)

	batch := byID["batch"].BatchParameters
	require.NotNil(t, batch)
	assert.Equal(t, "arn:aws:batch:us-east-1:"+dstAccount+":job-definition/jd:1", aws.ToString(batch.JobDefinition))
	assert.Equal(t, "nightly", aws.ToString(batch.JobName))
	assert.EqualValues(t, 4, batch.ArrayProperties.Size)
}
