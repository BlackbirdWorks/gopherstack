package replicator_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

const trustDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func TestReplicateTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(*testing.T, pair)
		check func(*testing.T, pair, *replicator.Report)
		tweak func(*replicator.Config)
		name  string
		types []string
	}{
		{name: "sqs with dlq and tags", types: []string{"sqs"}, seed: seedSQS, check: checkSQS},
		{name: "sns with remapped subscription", types: []string{"sqs", "sns"}, seed: seedSNS, check: checkSNS},
		{
			name: "s3 with data", types: []string{"s3"}, seed: seedS3, check: checkS3,
			tweak: func(c *replicator.Config) { c.IncludeData = true; c.MaxObjectBytes = 16 },
		},
		{
			name:  "dynamodb",
			types: []string{"dynamodb"},
			seed:  seedDDB,
			check: checkDDB,
			tweak: func(c *replicator.Config) { c.IncludeData = true },
		},
		{name: "iam", types: []string{"iam"}, seed: seedIAM, check: checkIAM},
		{
			name: "ssm", types: []string{"ssm"}, seed: seedSSM, check: checkSSM,
			tweak: func(c *replicator.Config) { c.WithDecryption = true },
		},
		{name: "secretsmanager", types: []string{"secretsmanager"}, seed: seedSecrets, check: checkSecrets},
		{name: "kms", types: []string{"kms"}, seed: seedKMS, check: checkKMS},
		{name: "eventbridge", types: []string{"sqs", "eventbridge"}, seed: seedEB, check: checkEB},
		{name: "kinesis", types: []string{"kinesis"}, seed: seedKinesis, check: checkKinesis},
		{name: "lambda", types: []string{"lambda"}, seed: seedLambda, check: checkLambda},
		{name: "logs", types: []string{"logs"}, seed: seedLogs, check: checkLogs},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newPair(t)
			tt.seed(t, p)

			cfg := p.config(tt.types...)
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}

			rep, err := replicator.Run(t.Context(), cfg)
			require.NoError(t, err)
			require.False(t, rep.Failed(), "%+v", rep.Results)
			assert.Equal(t, srcAccount, rep.SourceAccount)
			assert.Equal(t, dstAccount, rep.TargetAccount)
			tt.check(t, p, rep)

			rerun, err := replicator.Run(t.Context(), cfg)
			require.NoError(t, err)
			require.False(t, rerun.Failed(), "rerun: %+v", rerun.Results)
			assert.Zero(t, rerun.Counts[replicator.ActionCreated], "rerun created: %+v", rerun.Results)
		})
	}
}

func TestReplicateDryRunWritesNothing(t *testing.T) {
	t.Parallel()

	p := newPair(t)
	seedSQS(t, p)
	seedS3(t, p)
	seedIAM(t, p)

	cfg := p.config("iam", "s3", "sqs")
	cfg.DryRun = true

	rep, err := replicator.Run(t.Context(), cfg)
	require.NoError(t, err)
	require.False(t, rep.Failed(), "%+v", rep.Results)
	assert.True(t, rep.DryRun)
	assert.Equal(t, replicator.ActionCreated, actions(rep)["sqs/orders"])

	q, err := sqs.NewFromConfig(p.dstCfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, q.QueueUrls)

	b, err := s3.NewFromConfig(p.dstCfg, func(o *s3.Options) { o.UsePathStyle = true }).
		ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Empty(t, b.Buckets)

	roles, err := iam.NewFromConfig(p.dstCfg).ListRoles(t.Context(), &iam.ListRolesInput{})
	require.NoError(t, err)
	assert.Empty(t, roles.Roles)
}

func TestReplicateIdentifierFilter(t *testing.T) {
	t.Parallel()

	p := newPair(t)
	seedSQS(t, p)

	cfg := p.config("sqs")
	cfg.Identifier = "orders-dlq"

	rep, err := replicator.Run(t.Context(), cfg)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"sqs/orders-dlq": replicator.ActionCreated}, actions(rep))
}

func TestReplicateRejectsUnknownType(t *testing.T) {
	t.Parallel()

	_, err := replicator.Run(t.Context(), replicator.Config{Types: []string{"ec2"}})
	require.ErrorIs(t, err, replicator.ErrUnknownType)
}

func seedSQS(t *testing.T, p pair) {
	t.Helper()

	c := sqs.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("orders-dlq")})
	require.NoError(t, err)

	rp := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:` + srcAccount + `:orders-dlq","maxReceiveCount":3}`
	_, err = c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("orders"),
		Attributes: map[string]string{
			"VisibilityTimeout": "45", "DelaySeconds": "2", "RedrivePolicy": rp,
		},
		Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
}

func checkSQS(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.Equal(t, replicator.ActionCreated, actions(rep)["sqs/orders"])

	c := sqs.NewFromConfig(p.dstCfg)
	u, err := c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("orders")})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: u.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)
	assert.Equal(t, "45", attrs.Attributes["VisibilityTimeout"])
	assert.Equal(t, "2", attrs.Attributes["DelaySeconds"])
	assert.Contains(t, attrs.Attributes["RedrivePolicy"], dstAccount+":orders-dlq")

	tags, err := c.ListQueueTags(t.Context(), &sqs.ListQueueTagsInput{QueueUrl: u.QueueUrl})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags.Tags)
}

func seedSNS(t *testing.T, p pair) {
	t.Helper()

	ctx := t.Context()
	_, err := sqs.NewFromConfig(p.srcCfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sink")})
	require.NoError(t, err)

	c := sns.NewFromConfig(p.srcCfg)
	topic, err := c.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("events"), Attributes: map[string]string{"DisplayName": "Events"},
	})
	require.NoError(t, err)

	_, err = c.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
		Endpoint:   aws.String("arn:aws:sqs:us-east-1:" + srcAccount + ":sink"),
		Attributes: map[string]string{"RawMessageDelivery": "true"}, ReturnSubscriptionArn: true,
	})
	require.NoError(t, err)
}

func checkSNS(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.Equal(t, replicator.ActionCreated, actions(rep)["sns/events"])

	c := sns.NewFromConfig(p.dstCfg)
	arn := "arn:aws:sns:us-east-1:" + dstAccount + ":events"

	attrs, err := c.GetTopicAttributes(t.Context(), &sns.GetTopicAttributesInput{TopicArn: &arn})
	require.NoError(t, err)
	assert.Equal(t, "Events", attrs.Attributes["DisplayName"])

	subs, err := c.ListSubscriptionsByTopic(t.Context(), &sns.ListSubscriptionsByTopicInput{TopicArn: &arn})
	require.NoError(t, err)
	require.Len(t, subs.Subscriptions, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:"+dstAccount+":sink", aws.ToString(subs.Subscriptions[0].Endpoint))

	sa, err := c.GetSubscriptionAttributes(t.Context(), &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: subs.Subscriptions[0].SubscriptionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "true", sa.Attributes["RawMessageDelivery"])
}

func s3Client(cfgOf pair, src bool) *s3.Client {
	c := cfgOf.dstCfg
	if src {
		c = cfgOf.srcCfg
	}

	return s3.NewFromConfig(c, func(o *s3.Options) { o.UsePathStyle = true })
}

func seedS3(t *testing.T, p pair) {
	t.Helper()

	c := s3Client(p, true)
	ctx := t.Context()

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("assets")})
	require.NoError(t, err)

	_, err = c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String("assets"),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	_, err = c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket:  aws.String("assets"),
		Tagging: &s3types.Tagging{TagSet: []s3types.Tag{{Key: aws.String("team"), Value: aws.String("core")}}},
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::` +
		srcAccount + `:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::assets/*"}]}`
	_, err = c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String("assets"), Policy: &policy})
	require.NoError(t, err)

	for k, v := range map[string]string{"small.txt": "hello", "big.bin": strings.Repeat("x", 64)} {
		_, err = c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("assets"), Key: aws.String(k), Body: strings.NewReader(v),
		})
		require.NoError(t, err)
	}
}

func checkS3(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	var detail string

	for _, r := range rep.Results {
		if r.ID == "assets" {
			detail = r.Reason
		}
	}

	assert.Equal(t, "1 objects copied, 1 skipped", detail)

	c := s3Client(p, false)
	ctx := t.Context()

	v, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("assets")})
	require.NoError(t, err)
	assert.Equal(t, s3types.BucketVersioningStatusEnabled, v.Status)

	tg, err := c.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String("assets")})
	require.NoError(t, err)
	require.Len(t, tg.TagSet, 1)

	pol, err := c.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String("assets")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(pol.Policy), dstAccount)
	assert.NotContains(t, aws.ToString(pol.Policy), srcAccount)

	obj, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("assets"), Key: aws.String("small.txt")})
	require.NoError(t, err)

	defer obj.Body.Close()

	b := new(strings.Builder)
	_, err = io.Copy(b, obj.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello", b.String())

	_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("assets"), Key: aws.String("big.bin")})
	require.Error(t, err)
}

func seedDDB(t *testing.T, p pair) {
	t.Helper()

	c := dynamodb.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("users"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("email"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-email"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("email"), KeyType: ddbtypes.KeyTypeHash},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
		Tags: []ddbtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	_, err = c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String("users"),
		TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
			Enabled: aws.Bool(true), AttributeName: aws.String("expires"),
		},
	})
	require.NoError(t, err)

	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("users"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{
				Value: "u1",
			},
			"email": &ddbtypes.AttributeValueMemberS{Value: "a@b.c"},
		},
	})
	require.NoError(t, err)
}

func checkDDB(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	c := dynamodb.NewFromConfig(p.dstCfg)
	ctx := t.Context()

	d, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("users")})
	require.NoError(t, err)
	require.Len(t, d.Table.GlobalSecondaryIndexes, 1)
	assert.Equal(t, "by-email", aws.ToString(d.Table.GlobalSecondaryIndexes[0].IndexName))
	require.NotNil(t, d.Table.StreamSpecification)
	assert.Equal(t, ddbtypes.StreamViewTypeNewAndOldImages, d.Table.StreamSpecification.StreamViewType)

	ttl, err := c.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String("users")})
	require.NoError(t, err)
	assert.Equal(t, "expires", aws.ToString(ttl.TimeToLiveDescription.AttributeName))

	got, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("users"),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "u1"}},
	})
	require.NoError(t, err)
	assert.Len(t, got.Item, 2)
}

func seedIAM(t *testing.T, p pair) {
	t.Helper()

	c := iam.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	pol, err := c.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName: aws.String("app-policy"),
		PolicyDocument: aws.String(
			`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"arn:aws:sqs:us-east-1:` +
				srcAccount + `:*"}]}`,
		),
	})
	require.NoError(t, err)

	_, err = c.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName: aws.String("app-role"), AssumeRolePolicyDocument: aws.String(trustDoc),
		Tags: []iamtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	_, err = c.UpdateRole(ctx, &iam.UpdateRoleInput{RoleName: aws.String("app-role"), Description: aws.String("app")})
	require.NoError(t, err)

	_, err = c.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
		RoleName: aws.String("app-role"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = c.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:   aws.String("app-role"),
		PolicyName: aws.String("inline"),
		PolicyDocument: aws.String(
			`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		),
	})
	require.NoError(t, err)
}

func checkIAM(t *testing.T, p pair, rep *replicator.Report) {
	t.Helper()

	assert.Equal(t, replicator.ActionCreated, actions(rep)["iam/app-role"])
	assert.Equal(t, replicator.ActionCreated, actions(rep)["iam/policy/app-policy"])

	c := iam.NewFromConfig(p.dstCfg)
	ctx := t.Context()

	r, err := c.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("app-role")})
	require.NoError(t, err)
	assert.Equal(t, "app", aws.ToString(r.Role.Description))

	att, err := c.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String("app-role")})
	require.NoError(t, err)
	require.Len(t, att.AttachedPolicies, 1)
	assert.Contains(t, aws.ToString(att.AttachedPolicies[0].PolicyArn), dstAccount)

	inl, err := c.GetRolePolicy(
		ctx,
		&iam.GetRolePolicyInput{RoleName: aws.String("app-role"), PolicyName: aws.String("inline")},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(inl.PolicyDocument))
}

func seedSSM(t *testing.T, p pair) {
	t.Helper()

	c := ssm.NewFromConfig(p.srcCfg)
	ctx := t.Context()

	for _, in := range []*ssm.PutParameterInput{
		{
			Name: aws.String("/app/plain"), Value: aws.String("v1"),
			Type: ssmtypes.ParameterTypeString, Description: aws.String("d"),
		},
		{Name: aws.String("/app/secret"), Value: aws.String("s3cret"), Type: ssmtypes.ParameterTypeSecureString},
	} {
		_, err := c.PutParameter(ctx, in)
		require.NoError(t, err)
	}
}

func checkSSM(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	c := ssm.NewFromConfig(p.dstCfg)

	got, err := c.GetParameter(
		t.Context(),
		&ssm.GetParameterInput{Name: aws.String("/app/secret"), WithDecryption: aws.Bool(true)},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3cret", aws.ToString(got.Parameter.Value))
	assert.Equal(t, ssmtypes.ParameterTypeSecureString, got.Parameter.Type)

	plain, err := c.GetParameter(t.Context(), &ssm.GetParameterInput{Name: aws.String("/app/plain")})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(plain.Parameter.Value))
}

func TestReplicateSSMSecureStringNeedsFlag(t *testing.T) {
	t.Parallel()

	p := newPair(t)
	seedSSM(t, p)

	rep, err := replicator.Run(t.Context(), p.config("ssm"))
	require.NoError(t, err)

	a := actions(rep)
	assert.Equal(t, replicator.ActionSkipped, a["ssm//app/secret"])
	assert.Equal(t, replicator.ActionCreated, a["ssm//app/plain"])
}

func seedSecrets(t *testing.T, p pair) {
	t.Helper()

	_, err := secretsmanager.NewFromConfig(p.srcCfg).CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name: aws.String("db/pass"), SecretString: aws.String("hunter2"), Description: aws.String("db"),
		Tags: []smtypes.Tag{{Key: aws.String("a"), Value: aws.String("b")}},
	})
	require.NoError(t, err)
}

func checkSecrets(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	got, err := secretsmanager.NewFromConfig(p.dstCfg).GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("db/pass"),
	})
	require.NoError(t, err)
	assert.Equal(t, "hunter2", aws.ToString(got.SecretString))
}

func seedKMS(t *testing.T, p pair) {
	t.Helper()

	c := kms.NewFromConfig(p.srcCfg)

	k, err := c.CreateKey(t.Context(), &kms.CreateKeyInput{Description: aws.String("app key")})
	require.NoError(t, err)

	_, err = c.CreateAlias(t.Context(), &kms.CreateAliasInput{
		AliasName: aws.String("alias/app"), TargetKeyId: k.KeyMetadata.KeyId,
	})
	require.NoError(t, err)
}

func checkKMS(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	d, err := kms.NewFromConfig(p.dstCfg).
		DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: aws.String("alias/app")})
	require.NoError(t, err)
	assert.Equal(t, "app key", aws.ToString(d.KeyMetadata.Description))
	assert.Contains(t, aws.ToString(d.KeyMetadata.Arn), dstAccount)
}

func seedEB(t *testing.T, p pair) {
	t.Helper()

	ctx := t.Context()
	_, err := sqs.NewFromConfig(p.srcCfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("target-q")})
	require.NoError(t, err)

	c := eventbridge.NewFromConfig(p.srcCfg)

	_, err = c.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String("custom")})
	require.NoError(t, err)

	_, err = c.PutRule(ctx, &eventbridge.PutRuleInput{
		Name: aws.String("r1"), EventBusName: aws.String("custom"),
		EventPattern: aws.String(`{"source":["app"]}`), State: ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = c.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule:         aws.String("r1"),
		EventBusName: aws.String("custom"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String("arn:aws:sqs:us-east-1:" + srcAccount + ":target-q")},
		},
	})
	require.NoError(t, err)
}

func checkEB(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	c := eventbridge.NewFromConfig(p.dstCfg)

	d, err := c.DescribeRule(
		t.Context(),
		&eventbridge.DescribeRuleInput{Name: aws.String("r1"), EventBusName: aws.String("custom")},
	)
	require.NoError(t, err)
	assert.JSONEq(t, `{"source":["app"]}`, aws.ToString(d.EventPattern))

	ts, err := c.ListTargetsByRule(t.Context(), &eventbridge.ListTargetsByRuleInput{
		Rule: aws.String("r1"), EventBusName: aws.String("custom"),
	})
	require.NoError(t, err)
	require.Len(t, ts.Targets, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:"+dstAccount+":target-q", aws.ToString(ts.Targets[0].Arn))
}

func seedKinesis(t *testing.T, p pair) {
	t.Helper()

	c := kinesis.NewFromConfig(p.srcCfg)

	_, err := c.CreateStream(
		t.Context(),
		&kinesis.CreateStreamInput{StreamName: aws.String("clicks"), ShardCount: aws.Int32(2)},
	)
	require.NoError(t, err)

	_, err = c.AddTagsToStream(t.Context(), &kinesis.AddTagsToStreamInput{
		StreamName: aws.String("clicks"), Tags: map[string]string{"a": "b"},
	})
	require.NoError(t, err)
}

func checkKinesis(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	c := kinesis.NewFromConfig(p.dstCfg)

	d, err := c.DescribeStreamSummary(
		t.Context(),
		&kinesis.DescribeStreamSummaryInput{StreamName: aws.String("clicks")},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 2, aws.ToInt32(d.StreamDescriptionSummary.OpenShardCount))

	tags, err := c.ListTagsForStream(t.Context(), &kinesis.ListTagsForStreamInput{StreamName: aws.String("clicks")})
	require.NoError(t, err)
	assert.Len(t, tags.Tags, 1)
}

func seedLogs(t *testing.T, p pair) {
	t.Helper()

	c := cloudwatchlogs.NewFromConfig(p.srcCfg)

	_, err := c.CreateLogGroup(t.Context(), &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("/app/web")})
	require.NoError(t, err)

	_, err = c.PutRetentionPolicy(t.Context(), &cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName: aws.String("/app/web"), RetentionInDays: aws.Int32(14),
	})
	require.NoError(t, err)
}

func checkLogs(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	out, err := cloudwatchlogs.NewFromConfig(p.dstCfg).
		DescribeLogGroups(t.Context(), &cloudwatchlogs.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/app/web"),
		})
	require.NoError(t, err)
	require.Len(t, out.LogGroups, 1)
	assert.EqualValues(t, 14, aws.ToInt32(out.LogGroups[0].RetentionInDays))
}

func seedLambda(t *testing.T, p pair) {
	t.Helper()

	ctx := t.Context()
	c := s3Client(p, true)

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("code")})
	require.NoError(t, err)

	_, err = c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("code"), Key: aws.String("fn.zip"), Body: strings.NewReader(fnZip),
	})
	require.NoError(t, err)

	_, err = lambda.NewFromConfig(p.srcCfg).CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String("worker"), Runtime: lambdatypes.RuntimePython312, Handler: aws.String("app.handler"),
		Role:        aws.String("arn:aws:iam::" + srcAccount + ":role/app-role"),
		Code:        &lambdatypes.FunctionCode{S3Bucket: aws.String("code"), S3Key: aws.String("fn.zip")},
		Timeout:     aws.Int32(12),
		Environment: &lambdatypes.Environment{Variables: map[string]string{"MODE": "prod"}},
	})
	require.NoError(t, err)
}

func checkLambda(t *testing.T, p pair, _ *replicator.Report) {
	t.Helper()

	got, err := lambda.NewFromConfig(p.dstCfg).
		GetFunction(t.Context(), &lambda.GetFunctionInput{FunctionName: aws.String("worker")})
	require.NoError(t, err)

	cfg := got.Configuration
	assert.Equal(t, "arn:aws:iam::"+dstAccount+":role/app-role", aws.ToString(cfg.Role))
	assert.EqualValues(t, 12, aws.ToInt32(cfg.Timeout))
	assert.Equal(t, "prod", cfg.Environment.Variables["MODE"])
}

const fnZip = "PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
