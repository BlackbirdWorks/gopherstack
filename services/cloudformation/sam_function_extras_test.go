package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestSAMTransform_FunctionExtras(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantIDs   map[string]string
		check     func(t *testing.T, env *samEventEnv, fn string)
		name      string
		props     string
		qualifier string
	}{
		{
			name: "event_invoke_config",
			props: "      EventInvokeConfig:\n        MaximumRetryAttempts: 1\n        MaximumEventAgeInSeconds: 120\n" +
				"        DestinationConfig:\n          OnFailure:\n            Type: SQS\n" +
				"            Destination: arn:aws:sqs:us-east-1:000000000000:dlq\n",
			wantIDs: map[string]string{
				"FEventInvokeConfig": "AWS::Lambda::EventInvokeConfig",
				"FRole":              "AWS::IAM::Role",
			},
			check: func(t *testing.T, env *samEventEnv, fn string) {
				t.Helper()

				cfg, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).
					GetFunctionEventInvokeConfig(fn)
				require.NoError(t, err)
				assert.Equal(t, 1, aws.ToInt(cfg.MaximumRetryAttempts))
				assert.Equal(t, 120, aws.ToInt(cfg.MaximumEventAgeInSeconds))
				require.NotNil(t, cfg.DestinationConfig.OnFailure)
				assert.Contains(t, cfg.DestinationConfig.OnFailure.Destination, ":dlq")
			},
		},
		{
			name:    "event_invoke_config_alias",
			props:   "      AutoPublishAlias: live\n      EventInvokeConfig:\n        MaximumRetryAttempts: 0\n",
			wantIDs: map[string]string{"FEventInvokeConfig": "AWS::Lambda::EventInvokeConfig"},
			check: func(t *testing.T, env *samEventEnv, fn string) {
				t.Helper()

				cfg, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).
					GetFunctionEventInvokeConfigQualified(fn, "live")
				require.NoError(t, err)
				assert.Equal(t, 0, aws.ToInt(cfg.MaximumRetryAttempts))
			},
		},
		{
			name: "function_url_none",
			props: "      FunctionUrlConfig:\n        AuthType: NONE\n        Cors:\n          AllowOrigins: ['*']\n" +
				"          AllowMethods: [GET]\n          MaxAge: 60\n",
			wantIDs: map[string]string{
				"FUrl": "AWS::Lambda::Url", "FUrlPublicPermissions": "AWS::Lambda::Permission",
			},
			check: func(t *testing.T, env *samEventEnv, fn string) {
				t.Helper()

				cfg, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetFunctionURLConfig(fn)
				require.NoError(t, err)
				assert.Equal(t, "NONE", cfg.AuthType)
				require.NotNil(t, cfg.Cors)
				assert.Equal(t, []string{"GET"}, cfg.Cors.AllowMethods)
				assert.Equal(t, 60, cfg.Cors.MaxAge)
			},
		},
		{
			name:    "function_url_iam",
			props:   "      FunctionUrlConfig:\n        AuthType: AWS_IAM\n",
			wantIDs: map[string]string{"FUrl": "AWS::Lambda::Url"},
			check: func(t *testing.T, env *samEventEnv, fn string) {
				t.Helper()

				cfg, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetFunctionURLConfig(fn)
				require.NoError(t, err)
				assert.Equal(t, "AWS_IAM", cfg.AuthType)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(
				t,
				env.client,
				samHeader+"Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n"+
					samFnHeader+tc.props,
			)

			got := logicalIDs(t, env.client)
			for id, typ := range tc.wantIDs {
				assert.Equal(t, typ, got[id], id)
			}

			fn := env.fnName(t, "F")
			tc.check(t, env, fn)
			assertDeleteClean(t, env.client)

			_, err := env.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend).GetFunctionURLConfig(fn)
			if _, isURL := tc.wantIDs["FUrl"]; isURL {
				require.Error(t, err, "URL config must be removed with the stack")
			}
		})
	}
}

func TestSAMTransform_FunctionExtrasFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, props, reason string
	}{
		{
			name:   "url_bad_auth",
			props:  "      FunctionUrlConfig:\n        AuthType: BASIC\n",
			reason: "AuthType",
		},
		{
			name:   "destination_without_arn",
			props:  "      EventInvokeConfig:\n        DestinationConfig:\n          OnSuccess:\n            Type: SNS\n",
			reason: "Destination",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			_, err := env.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
				StackName:     aws.String("bad"),
				ChangeSetName: aws.String("cs"),
				ChangeSetType: types.ChangeSetTypeCreate,
				TemplateBody: aws.String(
					samHeader + "Resources:\n  F:\n    Type: AWS::Serverless::Function\n    Properties:\n" +
						samFnHeader + tc.props,
				),
				Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)

			cs, err := env.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ChangeSetStatusFailed, cs.Status)
			assert.Contains(t, aws.ToString(cs.StatusReason), tc.reason)
		})
	}
}
