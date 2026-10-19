package app

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	agenttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func agentCoreErrCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestAgentCoreWiringChecksOtherServices(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	ac := bedrockagentcorecontrol.NewFromConfig(fx.cfg)

	_, err := ecr.NewFromConfig(fx.cfg).
		CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("agent")})
	require.NoError(t, err)

	artifact := func(repo string) agenttypes.AgentRuntimeArtifact {
		return &agenttypes.AgentRuntimeArtifactMemberContainerConfiguration{Value: agenttypes.ContainerConfiguration{
			ContainerUri: aws.String("000000000000.dkr.ecr.us-east-1.amazonaws.com/" + repo + ":latest"),
		}}
	}
	role := aws.String("arn:aws:iam::000000000000:role/agent")

	_, err = ac.CreateAgentRuntime(ctx, &bedrockagentcorecontrol.CreateAgentRuntimeInput{
		AgentRuntimeName: aws.String("wired"), AgentRuntimeArtifact: artifact("agent"), RoleArn: role,
	})
	require.NoError(t, err, "an image in a real ECR repository is accepted")

	_, err = ac.CreateAgentRuntime(ctx, &bedrockagentcorecontrol.CreateAgentRuntimeInput{
		AgentRuntimeName: aws.String("unwired"), AgentRuntimeArtifact: artifact("missing"), RoleArn: role,
	})
	assert.Equal(
		t,
		"ValidationException",
		agentCoreErrCode(err),
		"an image in a repository ECR does not hold is refused",
	)

	secret, err := secretsmanager.NewFromConfig(fx.cfg).CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name: aws.String("agent-key"), SecretString: aws.String("{\"k\":\"v\"}"),
	})
	require.NoError(t, err)

	secretTests := []struct {
		name     string
		secretID string
		wantCode string
	}{
		{name: "existing", secretID: aws.ToString(secret.ARN), wantCode: ""},
		{
			name:     "missing",
			secretID: "arn:aws:secretsmanager:us-east-1:000000000000:secret:ghost-AbCdEf",
			wantCode: "ValidationException",
		},
	}

	for _, tc := range secretTests {
		_, cerr := ac.CreateApiKeyCredentialProvider(ctx, &bedrockagentcorecontrol.CreateApiKeyCredentialProviderInput{
			Name: aws.String("p-" + tc.name),
			ApiKeySecretConfig: &agenttypes.SecretReference{
				SecretId: aws.String(tc.secretID), JsonKey: aws.String("k"),
			},
		})
		assert.Equal(t, tc.wantCode, agentCoreErrCode(cerr), tc.name)
	}

	s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("datasets")})
	require.NoError(t, err)
	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(
			"datasets",
		),
		Key:  aws.String("ex.jsonl"),
		Body: strings.NewReader("{\"input\":\"a\"}\n{\"input\":\"b\"}\n"),
	})
	require.NoError(t, err)

	ds, err := ac.CreateDataset(ctx, &bedrockagentcorecontrol.CreateDatasetInput{
		DatasetName: aws.String("fromS3"),
		SchemaType:  agenttypes.DatasetSchemaTypeThirdPartyEvaluationV1,
		Source: &agenttypes.DataSourceTypeMemberS3Source{
			Value: agenttypes.S3Source{S3Uri: aws.String("s3://datasets/ex.jsonl")},
		},
	})
	require.NoError(t, err)

	got, err := ac.GetDataset(ctx, &bedrockagentcorecontrol.GetDatasetInput{DatasetId: ds.DatasetId})
	require.NoError(t, err)
	assert.Equal(t, agenttypes.DatasetStatusActive, got.Status)
	assert.Equal(t, int64(2), aws.ToInt64(got.ExampleCount), "the JSONL object was read from the S3 backend")
}
