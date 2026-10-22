package app

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aisdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	aitypes "github.com/aws/aws-sdk-go-v2/service/appintegrations/types"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	connecttypes "github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type connectWiringEnv struct {
	connect      *connectsdk.Client
	integrations *aisdk.Client
	events       *eventbridge.Client
	objects      *s3.Client
	instance     *connectsdk.CreateInstanceOutput
}

func connectErrCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestInitializeServices_ConnectWiring(t *testing.T) {
	t.Parallel()

	cfg := newWiredSDKConfig(t, CLI{})
	base := connectWiringEnv{
		connect:      connectsdk.NewFromConfig(cfg),
		integrations: aisdk.NewFromConfig(cfg),
		events:       eventbridge.NewFromConfig(cfg),
		objects:      s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }),
	}

	tests := []struct {
		run  func(t *testing.T, e *connectWiringEnv)
		name string
	}{
		{name: "event association owns an EventBridge rule", run: connectEventRuleCase},
		{name: "event integration needs an EventBridge bus", run: func(t *testing.T, e *connectWiringEnv) {
			t.Helper()

			_, err := e.integrations.CreateEventIntegration(t.Context(), &aisdk.CreateEventIntegrationInput{
				Name: aws.String("nobus"), EventBridgeBus: aws.String("missing-bus"),
				EventFilter: &aitypes.EventFilter{Source: aws.String("aws.partner/example")},
			})
			assert.Equal(t, "InvalidRequestException", connectErrCode(err))
		}},
		{name: "prompts read S3 objects", run: connectPromptCase},
		{name: "storage config needs its bucket", run: func(t *testing.T, e *connectWiringEnv) {
			t.Helper()

			_, err := e.connect.AssociateInstanceStorageConfig(
				t.Context(),
				&connectsdk.AssociateInstanceStorageConfigInput{
					InstanceId: e.instance.Id, ResourceType: connecttypes.InstanceStorageResourceTypeCallRecordings,
					StorageConfig: &connecttypes.InstanceStorageConfig{
						StorageType: connecttypes.StorageTypeS3,
						S3Config: &connecttypes.S3Config{
							BucketName:   aws.String("no-such-bucket"),
							BucketPrefix: aws.String("p"),
						},
					},
				},
			)
			assert.Equal(t, "ResourceNotFoundException", connectErrCode(err))
		}},
		{name: "lambda functions must exist", run: func(t *testing.T, e *connectWiringEnv) {
			t.Helper()

			_, err := e.connect.AssociateLambdaFunction(t.Context(), &connectsdk.AssociateLambdaFunctionInput{
				InstanceId:  e.instance.Id,
				FunctionArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:ghost"),
			})
			assert.Equal(t, "ResourceNotFoundException", connectErrCode(err))
		}},
		{name: "unrouted operations answer as unknown", run: func(t *testing.T, e *connectWiringEnv) {
			t.Helper()

			_, err := e.connect.DismissUserContact(t.Context(), &connectsdk.DismissUserContactInput{
				InstanceId: e.instance.Id, UserId: aws.String("u"), ContactId: aws.String("c"),
			})
			assert.Equal(t, "UnknownOperationException", connectErrCode(err))
		}},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := base

			inst, err := env.connect.CreateInstance(t.Context(), &connectsdk.CreateInstanceInput{
				IdentityManagementType: connecttypes.DirectoryTypeConnectManaged,
				InstanceAlias:          aws.String("wired-" + string(rune('a'+i))),
				InboundCallsEnabled:    aws.Bool(true), OutboundCallsEnabled: aws.Bool(true),
			})
			require.NoError(t, err)

			env.instance = inst
			tt.run(t, &env)
		})
	}
}

func connectEventRuleCase(t *testing.T, e *connectWiringEnv) {
	t.Helper()

	created, err := e.integrations.CreateEventIntegration(t.Context(), &aisdk.CreateEventIntegrationInput{
		Name: aws.String("partner"), EventBridgeBus: aws.String("default"),
		EventFilter: &aitypes.EventFilter{Source: aws.String("aws.partner/example")},
	})
	require.NoError(t, err)

	assoc, err := e.connect.CreateIntegrationAssociation(t.Context(), &connectsdk.CreateIntegrationAssociationInput{
		InstanceId:      e.instance.Id,
		IntegrationType: connecttypes.IntegrationTypeEvent,
		IntegrationArn:  created.EventIntegrationArn,
	})
	require.NoError(t, err)

	list, err := e.integrations.ListEventIntegrationAssociations(
		t.Context(),
		&aisdk.ListEventIntegrationAssociationsInput{
			EventIntegrationName: aws.String("partner"),
		},
	)
	require.NoError(t, err)
	require.Len(t, list.EventIntegrationAssociations, 1)
	assert.Equal(t, aws.ToString(e.instance.Arn), aws.ToString(list.EventIntegrationAssociations[0].ClientId))

	rule := aws.ToString(list.EventIntegrationAssociations[0].EventBridgeRuleName)

	described, err := e.events.DescribeRule(t.Context(), &eventbridge.DescribeRuleInput{Name: aws.String(rule)})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(described.EventPattern), "aws.partner/example")

	_, err = e.connect.DeleteIntegrationAssociation(t.Context(), &connectsdk.DeleteIntegrationAssociationInput{
		InstanceId: e.instance.Id, IntegrationAssociationId: assoc.IntegrationAssociationId,
	})
	require.NoError(t, err)

	_, err = e.events.DescribeRule(t.Context(), &eventbridge.DescribeRuleInput{Name: aws.String(rule)})
	require.Error(t, err, "the rule is removed with the association")

	list, err = e.integrations.ListEventIntegrationAssociations(
		t.Context(),
		&aisdk.ListEventIntegrationAssociationsInput{
			EventIntegrationName: aws.String("partner"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, list.EventIntegrationAssociations)
}

func connectPromptCase(t *testing.T, e *connectWiringEnv) {
	t.Helper()

	prompt := &connectsdk.CreatePromptInput{
		InstanceId: e.instance.Id, Name: aws.String("hello"), S3Uri: aws.String("s3://audio-bucket/hello.wav"),
	}

	_, err := e.connect.CreatePrompt(t.Context(), prompt)
	assert.Equal(t, "ResourceNotFoundException", connectErrCode(err))

	_, err = e.objects.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("audio-bucket")})
	require.NoError(t, err)

	_, err = e.objects.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("audio-bucket"), Key: aws.String("hello.wav"), Body: strings.NewReader("RIFF"),
	})
	require.NoError(t, err)

	_, err = e.connect.CreatePrompt(t.Context(), prompt)
	require.NoError(t, err)
}
