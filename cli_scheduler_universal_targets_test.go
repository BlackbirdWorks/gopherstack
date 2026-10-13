package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/stretchr/testify/require"
)

const universalPrefix = "arn:aws:scheduler:::aws-sdk:"

func putUniversalSchedule(t *testing.T, fx *sfnFixture, action, input string) {
	t.Helper()

	_, err := scheduler.NewFromConfig(fx.cfg).CreateSchedule(t.Context(), &scheduler.CreateScheduleInput{
		Name:               aws.String("universal"),
		ScheduleExpression: aws.String("rate(1 minute)"),
		FlexibleTimeWindow: &schedtypes.FlexibleTimeWindow{Mode: schedtypes.FlexibleTimeWindowModeOff},
		Target: &schedtypes.Target{
			Arn:     aws.String(universalPrefix + action),
			RoleArn: aws.String(authzAccount + "sched"),
			Input:   aws.String(input),
		},
	})
	require.NoError(t, err)
}

func TestSchedulerUniversalTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, fx *sfnFixture) (string, string)
		landed func(t *testing.T, fx *sfnFixture, arg string) bool
		name   string
		action string
	}{
		{
			name:   "sqs_send_message",
			action: "sqs:sendMessage",
			setup: func(t *testing.T, fx *sfnFixture) (string, string) {
				t.Helper()

				url, _ := authzQueue(t, fx, "universal-q")
				in, err := json.Marshal(map[string]string{"QueueUrl": url, "MessageBody": "hello"})
				require.NoError(t, err)

				return string(in), url
			},
			landed: func(t *testing.T, fx *sfnFixture, url string) bool {
				t.Helper()

				return authzReceived(t, fx, url)
			},
		},
		{
			name:   "dynamodb_put_item",
			action: "dynamodb:putItem",
			setup: func(t *testing.T, fx *sfnFixture) (string, string) {
				t.Helper()

				_, err := dynamodb.NewFromConfig(fx.cfg).CreateTable(t.Context(), &dynamodb.CreateTableInput{
					TableName:   aws.String("universal"),
					BillingMode: ddbtypes.BillingModePayPerRequest,
					KeySchema: []ddbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []ddbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
					},
				})
				require.NoError(t, err)

				return `{"TableName":"universal","Item":{"id":{"S":"k1"}}}`, ""
			},
			landed: func(t *testing.T, fx *sfnFixture, _ string) bool {
				t.Helper()

				out, err := dynamodb.NewFromConfig(fx.cfg).GetItem(t.Context(), &dynamodb.GetItemInput{
					TableName: aws.String("universal"),
					Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "k1"}},
				})

				return err == nil && len(out.Item) > 0
			},
		},
		{
			name:   "batch_submit_job",
			action: "batch:submitJob",
			setup: func(t *testing.T, fx *sfnFixture) (string, string) {
				t.Helper()

				jd := setupBatchQueue(t, fx)
				in, err := json.Marshal(map[string]string{
					"JobName": "universal-job", "JobQueue": "eb-queue", "JobDefinition": jd,
				})
				require.NoError(t, err)

				return string(in), ""
			},
			landed: func(t *testing.T, fx *sfnFixture, _ string) bool {
				t.Helper()

				return batchJobExists(t, fx)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "Scheduler")

			input, handle := tt.setup(t, fx)
			putUniversalSchedule(t, fx, tt.action, input)

			require.Eventually(t, func() bool { return tt.landed(t, fx, handle) }, authzDeadline, authzTick)
		})
	}
}
