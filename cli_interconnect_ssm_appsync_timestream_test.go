package main

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	apptypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	ptypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	tswtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/require"
)

const (
	icAccount   = "000000000000"
	icRunbook   = `{"schemaVersion":"0.3","mainSteps":[{"name":"s","action":"aws:sleep","inputs":{"Duration":"PT1S"}}]}`
	icCommand   = `{"schemaVersion":"2.2","mainSteps":[{"action":"aws:runShellScript","name":"s"}]}`
	icSchema    = "type Query { q: String }\ntype Mutation { push(id: ID!): String }"
	icPutItemVT = `{"operation":"PutItem","item":{"id":{"S":"$ctx.args.id"}}}`
)

func icCreateDoc(t *testing.T, fx *sfnFixture, name, content string, typ ssmtypes.DocumentType) {
	t.Helper()

	_, err := ssm.NewFromConfig(fx.cfg).CreateDocument(t.Context(), &ssm.CreateDocumentInput{
		Name: aws.String(name), Content: aws.String(content),
		DocumentType: typ, DocumentFormat: ssmtypes.DocumentFormatJson,
	})
	require.NoError(t, err)
}

func icSetupAppSync(t *testing.T, fx *sfnFixture, authType apptypes.AuthenticationType) string {
	t.Helper()

	_, err := dynamodb.NewFromConfig(fx.cfg).CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String("ic-events"), BillingMode: ddbtypes.BillingModePayPerRequest,
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	ac := appsync.NewFromConfig(fx.cfg)
	api, err := ac.CreateGraphqlApi(t.Context(), &appsync.CreateGraphqlApiInput{
		Name: aws.String("ic-api"), AuthenticationType: authType,
	})
	require.NoError(t, err)

	apiID := api.GraphqlApi.ApiId

	_, err = ac.StartSchemaCreation(t.Context(), &appsync.StartSchemaCreationInput{
		ApiId: apiID, Definition: []byte(icSchema),
	})
	require.NoError(t, err)

	_, err = ac.CreateDataSource(t.Context(), &appsync.CreateDataSourceInput{
		ApiId: apiID, Name: aws.String("ddb"), Type: apptypes.DataSourceTypeAmazonDynamodb,
		DynamodbConfig: &apptypes.DynamodbDataSourceConfig{
			TableName: aws.String("ic-events"), AwsRegion: aws.String("us-east-1"),
		},
		ServiceRoleArn: aws.String("arn:aws:iam::" + icAccount + ":role/appsync"),
	})
	require.NoError(t, err)

	_, err = ac.CreateResolver(t.Context(), &appsync.CreateResolverInput{
		ApiId: apiID, TypeName: aws.String("Mutation"), FieldName: aws.String("push"),
		DataSourceName: aws.String("ddb"), RequestMappingTemplate: aws.String(icPutItemVT),
		ResponseMappingTemplate: aws.String("$util.toJson($context.result)"),
	})
	require.NoError(t, err)

	return "arn:aws:appsync:us-east-1:" + icAccount + ":apis/" + aws.ToString(apiID)
}

func icPutRuleTarget(t *testing.T, fx *sfnFixture, target ebtypes.Target) {
	t.Helper()

	ebc := eventbridge.NewFromConfig(fx.cfg)
	_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
		Name: aws.String("r"), EventPattern: aws.String(`{"source":["ic.targets"]}`),
	})
	require.NoError(t, err)

	target.Id = aws.String("t")
	_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
		Rule: aws.String("r"), Targets: []ebtypes.Target{target},
	})
	require.NoError(t, err)

	_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
		Source: aws.String("ic.targets"), DetailType: aws.String("t"), Detail: aws.String(`{"id":"evt-1"}`),
	}}})
	require.NoError(t, err)
}

func TestEventBridgeSSMAndAppSyncTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, fx *sfnFixture) ebtypes.Target
		landed func(t *testing.T, fx *sfnFixture) bool
		name   string
	}{
		{
			name: "run_command",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				icCreateDoc(t, fx, "ic-cmd", icCommand, ssmtypes.DocumentTypeCommand)

				return ebtypes.Target{
					Arn:   aws.String("arn:aws:ssm:us-east-1:" + icAccount + ":document/ic-cmd"),
					Input: aws.String(`{"Parameters":{"commands":["echo hi"]}}`),
					RunCommandParameters: &ebtypes.RunCommandParameters{RunCommandTargets: []ebtypes.RunCommandTarget{
						{Key: aws.String("InstanceIds"), Values: []string{"i-0123456789abcdef0"}},
					}},
				}
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				out, err := ssm.NewFromConfig(fx.cfg).ListCommands(t.Context(), &ssm.ListCommandsInput{})
				if err != nil {
					return false
				}

				for _, c := range out.Commands {
					if aws.ToString(c.DocumentName) == "ic-cmd" && len(c.Parameters["commands"]) == 1 &&
						c.Parameters["commands"][0] == "echo hi" {
						return true
					}
				}

				return false
			},
		},
		{
			name: "run_command_document_in_input",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				icCreateDoc(t, fx, "ic-cmd2", icCommand, ssmtypes.DocumentTypeCommand)

				return ebtypes.Target{
					Arn:   aws.String("arn:aws:ssm:us-east-1:" + icAccount + ":document/AWS-RunShellScript"),
					Input: aws.String(`{"DocumentName":"ic-cmd2"}`),
					RunCommandParameters: &ebtypes.RunCommandParameters{RunCommandTargets: []ebtypes.RunCommandTarget{
						{Key: aws.String("InstanceIds"), Values: []string{"i-0123456789abcdef0"}},
					}},
				}
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				out, err := ssm.NewFromConfig(fx.cfg).ListCommands(t.Context(), &ssm.ListCommandsInput{})

				return err == nil && len(out.Commands) == 1 && aws.ToString(out.Commands[0].DocumentName) == "ic-cmd2"
			},
		},
		{
			name: "automation",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				icCreateDoc(t, fx, "ic-auto", icRunbook, ssmtypes.DocumentTypeAutomation)

				return ebtypes.Target{
					Arn:   aws.String("arn:aws:ssm:us-east-1:" + icAccount + ":automation-definition/ic-auto:$DEFAULT"),
					Input: aws.String(`{"InstanceId":["i-0123456789abcdef0"]}`),
				}
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				out, err := ssm.NewFromConfig(fx.cfg).DescribeAutomationExecutions(
					t.Context(), &ssm.DescribeAutomationExecutionsInput{})
				if err != nil || len(out.AutomationExecutionMetadataList) == 0 {
					return false
				}

				md := out.AutomationExecutionMetadataList[0]
				if aws.ToString(md.DocumentName) != "ic-auto" {
					return false
				}

				got, gerr := ssm.NewFromConfig(fx.cfg).GetAutomationExecution(
					t.Context(), &ssm.GetAutomationExecutionInput{AutomationExecutionId: md.AutomationExecutionId})

				return gerr == nil && len(got.AutomationExecution.Parameters["InstanceId"]) == 1 &&
					got.AutomationExecution.Parameters["InstanceId"][0] == "i-0123456789abcdef0"
			},
		},
		{
			name: "appsync",
			setup: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				return ebtypes.Target{
					Arn: aws.String(icSetupAppSync(t, fx, apptypes.AuthenticationTypeAwsIam)),
					AppSyncParameters: &ebtypes.AppSyncParameters{
						GraphQLOperation: aws.String(`mutation Push($id: ID!) { push(id: $id) }`),
					},
					InputTransformer: &ebtypes.InputTransformer{
						InputPathsMap: map[string]string{"id": "$.detail.id"},
						InputTemplate: aws.String(`{"id": <id>}`),
					},
				}
			},
			landed: func(t *testing.T, fx *sfnFixture) bool {
				t.Helper()

				out, err := dynamodb.NewFromConfig(fx.cfg).GetItem(t.Context(), &dynamodb.GetItemInput{
					TableName: aws.String("ic-events"),
					Key: map[string]ddbtypes.AttributeValue{
						"id": &ddbtypes.AttributeValueMemberS{Value: "evt-1"},
					},
				})

				return err == nil && len(out.Item) > 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "EventBridge")

			icPutRuleTarget(t, fx, tt.setup(t, fx))

			require.Eventually(t, func() bool { return tt.landed(t, fx) }, authzDeadline, authzTick)
		})
	}
}

func TestEventBridgeSSMAndAppSyncTargetFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target func(t *testing.T, fx *sfnFixture) ebtypes.Target
		name   string
	}{
		{
			name: "run_command_missing_document",
			target: func(*testing.T, *sfnFixture) ebtypes.Target {
				return ebtypes.Target{
					Arn: aws.String("arn:aws:ssm:us-east-1:" + icAccount + ":document/absent"),
					RunCommandParameters: &ebtypes.RunCommandParameters{RunCommandTargets: []ebtypes.RunCommandTarget{
						{Key: aws.String("InstanceIds"), Values: []string{"i-0123456789abcdef0"}},
					}},
				}
			},
		},
		{
			name: "appsync_api_key_auth",
			target: func(t *testing.T, fx *sfnFixture) ebtypes.Target {
				t.Helper()

				return ebtypes.Target{
					Arn: aws.String(icSetupAppSync(t, fx, apptypes.AuthenticationTypeApiKey)),
					AppSyncParameters: &ebtypes.AppSyncParameters{
						GraphQLOperation: aws.String(`mutation Push($id: ID!) { push(id: $id) }`),
					},
					Input: aws.String(`{"id":"evt-1"}`),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "EventBridge")

			dlqURL, dlqARN := authzQueue(t, fx, "ic-dlq")
			target := tt.target(t, fx)
			target.DeadLetterConfig = &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)}
			target.RetryPolicy = &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)}

			icPutRuleTarget(t, fx, target)

			require.Eventually(t, func() bool {
				return authzDLQErrorCode(t, fx, dlqURL) != ""
			}, authzDeadline, authzTick)
		})
	}
}

func icPipeTimestream() *ptypes.PipeTargetTimestreamParameters {
	return &ptypes.PipeTargetTimestreamParameters{
		TimeValue:     aws.String("$.body.ts"),
		VersionValue:  aws.String("1"),
		EpochTimeUnit: ptypes.EpochTimeUnitMilliseconds,
		DimensionMappings: []ptypes.DimensionMapping{{
			DimensionName: aws.String("host"), DimensionValue: aws.String("$.body.host"),
			DimensionValueType: ptypes.DimensionValueTypeVarchar,
		}},
		SingleMeasureMappings: []ptypes.SingleMeasureMapping{{
			MeasureName: aws.String("cpu"), MeasureValue: aws.String("$.body.cpu"),
			MeasureValueType: ptypes.MeasureValueTypeDouble,
		}},
		MultiMeasureMappings: []ptypes.MultiMeasureMapping{{
			MultiMeasureName: aws.String("usage"),
			MultiMeasureAttributeMappings: []ptypes.MultiMeasureAttributeMapping{{
				MeasureValue: aws.String("$.body.mem"), MeasureValueType: ptypes.MeasureValueTypeBigint,
				MultiMeasureAttributeName: aws.String("mem"),
			}},
		}},
	}
}

// icRewriteRejected re-writes a record the pipe should already have written; Timestream rejects a
// same-key record whose version is not higher.
func icRewriteRejected(t *testing.T, fx *sfnFixture, now time.Time, measure string, multi bool) bool {
	t.Helper()

	rec := tswtypes.Record{
		MeasureName: aws.String(measure), Time: aws.String(strconv.FormatInt(now.UnixMilli(), 10)),
		TimeUnit: tswtypes.TimeUnitMilliseconds, Version: aws.Int64(1),
		Dimensions: []tswtypes.Dimension{{Name: aws.String("host"), Value: aws.String("h1")}},
	}

	if multi {
		rec.MeasureValueType = tswtypes.MeasureValueTypeMulti
		rec.MeasureValues = []tswtypes.MeasureValue{{
			Name: aws.String("mem"), Value: aws.String("7"), Type: tswtypes.MeasureValueTypeBigint,
		}}
	} else {
		rec.MeasureValueType = tswtypes.MeasureValueTypeDouble
		rec.MeasureValue = aws.String("42.5")
	}

	_, err := timestreamwrite.NewFromConfig(fx.cfg).WriteRecords(t.Context(), &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String("ic-db"), TableName: aws.String("ic-table"), Records: []tswtypes.Record{rec},
	})

	var rejected *tswtypes.RejectedRecordsException

	return errors.As(err, &rejected)
}

func TestPipesTimestreamTarget(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "Pipes")

	tw := timestreamwrite.NewFromConfig(fx.cfg)
	_, err := tw.CreateDatabase(t.Context(), &timestreamwrite.CreateDatabaseInput{DatabaseName: aws.String("ic-db")})
	require.NoError(t, err)

	_, err = tw.CreateTable(t.Context(), &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String("ic-db"), TableName: aws.String("ic-table"),
	})
	require.NoError(t, err)

	srcURL, srcARN := authzQueue(t, fx, "ic-pipe-src")
	now := time.Now().UTC().Truncate(time.Millisecond)

	_, err = pipes.NewFromConfig(fx.cfg).CreatePipe(t.Context(), &pipes.CreatePipeInput{
		Name: aws.String("p"), RoleArn: aws.String("arn:aws:iam::" + icAccount + ":role/pipe"),
		Source:           aws.String(srcARN),
		Target:           aws.String("arn:aws:timestream:us-east-1:" + icAccount + ":database/ic-db/table/ic-table"),
		TargetParameters: &ptypes.PipeTargetParameters{TimestreamParameters: icPipeTimestream()},
	})
	require.NoError(t, err)

	body := `{"ts":"` + strconv.FormatInt(now.UnixMilli(), 10) + `","host":"h1","cpu":42.5,"mem":7}`
	_, err = sqs.NewFromConfig(fx.cfg).SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(srcURL), MessageBody: aws.String(body),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return icRewriteRejected(t, fx, now, "cpu", false) && icRewriteRejected(t, fx, now, "usage", true)
	},
		authzDeadline, authzTick)
}

func TestPipesTimestreamTargetFailure(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "Pipes")

	srcURL, srcARN := authzQueue(t, fx, "ic-pipe-fail-src")
	now := time.Now().UTC()

	_, err := pipes.NewFromConfig(fx.cfg).CreatePipe(t.Context(), &pipes.CreatePipeInput{
		Name: aws.String("p"), RoleArn: aws.String("arn:aws:iam::" + icAccount + ":role/pipe"),
		Source:           aws.String(srcARN),
		Target:           aws.String("arn:aws:timestream:us-east-1:" + icAccount + ":database/absent/table/absent"),
		TargetParameters: &ptypes.PipeTargetParameters{TimestreamParameters: icPipeTimestream()},
	})
	require.NoError(t, err)

	_, err = sqs.NewFromConfig(fx.cfg).SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(srcURL),
		MessageBody: aws.String(`{"ts":"` + strconv.FormatInt(now.UnixMilli(), 10) +
			`","host":"h1","cpu":1,"mem":2}`),
	})
	require.NoError(t, err)

	sqsc := sqs.NewFromConfig(fx.cfg)

	require.Eventually(t, func() bool {
		out, aerr := sqsc.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(srcURL),
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})

		return aerr == nil &&
			out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "1"
	}, authzDeadline, authzTick)
}
