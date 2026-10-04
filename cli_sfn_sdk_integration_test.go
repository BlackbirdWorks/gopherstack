package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

var errFakeServiceException = errors.New("ServiceException")

const sfnTestRole = "arn:aws:iam::000000000000:role/sfn-role"

type sfnFixture struct {
	sfn      *sfnbackend.InMemoryBackend
	handler  http.Handler
	cfg      aws.Config
	services []service.Registerable
}

func newSFNFixture(t *testing.T) *sfnFixture {
	t.Helper()

	return newSFNFixtureIAM(t, false)
}

func newSFNFixtureIAM(t *testing.T, enforceIAM bool) *sfnFixture {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1", EnforceIAM: enforceIAM}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	addr := srv.Listener.Addr().String()
	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
		awscfg.WithHTTPClient(&http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, addr)
			},
		}}),
	)
	require.NoError(t, err)

	sfnH, ok := serviceByName(services)["StepFunctions"].(*sfnbackend.Handler)
	require.True(t, ok)

	bk, ok := sfnH.Backend.(*sfnbackend.InMemoryBackend)
	require.True(t, ok)

	return &sfnFixture{sfn: bk, handler: e, services: services, cfg: cfg}
}

type fakeLambda struct {
	fn    func(attempt int64, payload []byte) ([]byte, error)
	calls atomic.Int64
}

func (f *fakeLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	out, err := f.fn(f.calls.Add(1), payload)

	return out, http.StatusOK, err
}

// flakyHandler answers the first n DynamoDB GetItem calls with a throttling
// error before delegating to next.
type flakyHandler struct {
	next  http.Handler
	calls atomic.Int64
	n     int64
}

func (f *flakyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".GetItem") && f.calls.Add(1) <= f.n {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",` +
			`"message":"throttled"}`))

		return
	}

	f.next.ServeHTTP(w, r)
}

func runSyncSFN(t *testing.T, fx *sfnFixture, name, definition, input string) *sfn.StartSyncExecutionOutput {
	t.Helper()

	return runSyncSFNAs(t, fx, sfnTestRole, name, definition, input)
}

func runSyncSFNAs(
	t *testing.T, fx *sfnFixture, role, name, definition, input string,
) *sfn.StartSyncExecutionOutput {
	t.Helper()

	client := sfn.NewFromConfig(fx.cfg)

	created, err := client.CreateStateMachine(t.Context(), &sfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(role),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	out, err := client.StartSyncExecution(t.Context(), &sfn.StartSyncExecutionInput{
		StateMachineArn: created.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	return out
}

func jsonAt(t *testing.T, raw *string, path ...string) any {
	t.Helper()

	var cur any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(raw)), &cur))

	for _, p := range path {
		m, ok := cur.(map[string]any)
		require.True(t, ok, "path %v: %v is not an object in %s", path, cur, aws.ToString(raw))

		cur = m[p]
	}

	return cur
}

func catchTo(errName string) string {
	return `"Catch":[{"ErrorEquals":["` + errName + `"],"ResultPath":"$.err","Next":"Caught"}]`
}

const caughtState = `"Caught":{"Type":"Succeed"}`

func TestStepFunctionsSDKIntegration(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()

	ddb := dynamodb.NewFromConfig(fx.cfg)
	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("sfn-sdk-table"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	q, err := sqs.NewFromConfig(fx.cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sfn-sdk-q")})
	require.NoError(t, err)

	jq, err := sqs.NewFromConfig(fx.cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sfn-sdk-jq")})
	require.NoError(t, err)

	topic, err := sns.NewFromConfig(fx.cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("sfn-sdk-topic")})
	require.NoError(t, err)

	_, err = s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).
		CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("sfn-sdk-bucket")})
	require.NoError(t, err)

	child, err := sfn.NewFromConfig(fx.cfg).CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name: aws.String("sfn-sdk-child"),
		Definition: aws.String(
			`{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"MyKey":"MyValue"},"End":true}}}`,
		),
		RoleArn: aws.String(sfnTestRole),
	})
	require.NoError(t, err)

	failChild, err := sfn.NewFromConfig(fx.cfg).CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name:       aws.String("sfn-sdk-failchild"),
		Definition: aws.String(`{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"nested"}}}`),
		RoleArn:    aws.String(sfnTestRole),
	})
	require.NoError(t, err)

	tests := []struct {
		check      func(t *testing.T, out *sfn.StartSyncExecutionOutput)
		name       string
		definition string
		input      string
	}{
		{
			name: "dynamodb_optimized_put_get",
			definition: `{"StartAt":"Put","States":{` +
				`"Put":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:putItem","Parameters":{` +
				`"TableName":"sfn-sdk-table","Item":{"id":{"S":"a"},"v":{"N":"5"}}},"ResultPath":null,"Next":"Get"},` +
				`"Get":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:getItem","Parameters":{` +
				`"TableName":"sfn-sdk-table","Key":{"id":{"S":"a"}}},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "5", jsonAt(t, out.Output, "Item", "v", "N"))
				assert.Equal(t, "a", jsonAt(t, out.Output, "Item", "id", "S"))
			},
		},
		{
			name: "dynamodb_sdk_missing_table_caught",
			definition: `{"StartAt":"Get","States":{"Get":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::aws-sdk:dynamodb:getItem","Parameters":{` +
				`"TableName":"no-such-table","Key":{"id":{"S":"a"}}},` + catchTo("DynamoDb.ResourceNotFoundException") +
				`,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "DynamoDb.ResourceNotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "dynamodb_optimized_missing_table_caught",
			definition: `{"StartAt":"Get","States":{"Get":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::dynamodb:getItem","Parameters":{` +
				`"TableName":"no-such-table","Key":{"id":{"S":"a"}}},` + catchTo("DynamoDB.ResourceNotFoundException") +
				`,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "DynamoDB.ResourceNotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "sqs_optimized_send_sdk_receive",
			definition: `{"StartAt":"Send","States":{` +
				`"Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{` +
				`"QueueUrl.$":"$.q","MessageBody":"hello"},"ResultPath":"$.sent","Next":"Recv"},` +
				`"Recv":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sqs:receiveMessage","Parameters":{` +
				`"QueueUrl.$":"$.q"},"ResultPath":"$.recv","End":true}}}`,
			input: `{"q":"` + aws.ToString(q.QueueUrl) + `"}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotEmpty(t, jsonAt(t, out.Output, "sent", "MessageId"))
				msgs, ok := jsonAt(t, out.Output, "recv", "Messages").([]any)
				require.True(t, ok)
				require.Len(t, msgs, 1)
				assert.Equal(t, "hello", msgs[0].(map[string]any)["Body"])
			},
		},
		{
			name: "sqs_sdk_missing_queue_caught",
			definition: `{"StartAt":"U","States":{"U":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::aws-sdk:sqs:getQueueUrl","Parameters":{"QueueName":"no-such-q"},` +
				catchTo("Sqs.QueueDoesNotExistException") + `,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "Sqs.QueueDoesNotExistException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "sns_optimized_publish",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Task","Resource":"arn:aws:states:::sns:publish",` +
				`"Parameters":{"TopicArn.$":"$.t","Message":"m"},"End":true}}}`,
			input: `{"t":"` + aws.ToString(topic.TopicArn) + `"}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotEmpty(t, jsonAt(t, out.Output, "MessageId"))
			},
		},
		{
			name: "sns_sdk_missing_topic_caught",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sns:publish",` +
				`"Parameters":{"TopicArn":"arn:aws:sns:us-east-1:000000000000:no-such-topic","Message":"m"},` +
				catchTo(
					"Sns.NotFoundException",
				) + `,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "Sns.NotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "s3_put_get_and_missing_key",
			definition: `{"StartAt":"Put","States":{` +
				`"Put":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:putObject","Parameters":{` +
				`"Bucket":"sfn-sdk-bucket","Key":"k","Body":"payload"},"ResultPath":null,"Next":"Get"},` +
				`"Get":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject","Parameters":{` +
				`"Bucket":"sfn-sdk-bucket","Key":"k"},"ResultPath":"$.got","Next":"Miss"},` +
				`"Miss":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject","Parameters":{` +
				`"Bucket":"sfn-sdk-bucket","Key":"absent"},` + catchTo("S3.NoSuchKeyException") + `,"End":true},` +
				caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "payload", jsonAt(t, out.Output, "got", "Body"))
				assert.Equal(t, "S3.NoSuchKeyException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "ssm_put_get_and_missing",
			definition: `{"StartAt":"Put","States":{` +
				`"Put":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:ssm:putParameter","Parameters":{` +
				`"Name":"/sfn/p","Value":"v1","Type":"String","Overwrite":true},"ResultPath":null,"Next":"Get"},` +
				`"Get":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:ssm:getParameter","Parameters":{` +
				`"Name":"/sfn/p"},"ResultPath":"$.got","Next":"Miss"},` +
				`"Miss":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:ssm:getParameter","Parameters":{` +
				`"Name":"/sfn/absent"},` + catchTo("Ssm.ParameterNotFoundException") + `,"End":true},` +
				caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "v1", jsonAt(t, out.Output, "got", "Parameter", "Value"))
				assert.Equal(t, "Ssm.ParameterNotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "secretsmanager_create_get_and_missing",
			definition: `{"StartAt":"Create","States":{` +
				`"Create":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:secretsmanager:createSecret","Parameters":{` +
				`"Name":"sfn/secret","SecretString":"s3cr3t"},"ResultPath":null,"Next":"Get"},` +
				`"Get":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:secretsmanager:getSecretValue","Parameters":{` +
				`"SecretId":"sfn/secret"},"ResultPath":"$.got","Next":"Miss"},` +
				`"Miss":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:secretsmanager:getSecretValue","Parameters":{` +
				`"SecretId":"sfn/absent"},` + catchTo("SecretsManager.ResourceNotFoundException") + `,"End":true},` +
				caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "s3cr3t", jsonAt(t, out.Output, "got", "SecretString"))
				assert.Equal(t, "SecretsManager.ResourceNotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "kms_create_describe_and_missing",
			definition: `{"StartAt":"Create","States":{` +
				`"Create":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:kms:createKey","Parameters":{},` +
				`"ResultPath":"$.created","Next":"Miss"},` +
				`"Miss":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:kms:describeKey","Parameters":{` +
				`"KeyId":"11111111-2222-3333-4444-555555555555"},` + catchTo("Kms.NotFoundException") + `,"End":true},` +
				caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotEmpty(t, jsonAt(t, out.Output, "created", "KeyMetadata", "KeyId"))
				assert.Equal(t, "Kms.NotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "eventbridge_optimized_put_events",
			definition: `{"StartAt":"E","States":{"E":{"Type":"Task","Resource":"arn:aws:states:::events:putEvents",` +
				`"Parameters":{"Entries":[{"Source":"sfn.test","DetailType":"t","Detail":"{}"}]},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.InDelta(t, 0, jsonAt(t, out.Output, "FailedEntryCount"), 0)
				entries, ok := jsonAt(t, out.Output, "Entries").([]any)
				require.True(t, ok)
				require.Len(t, entries, 1)
				assert.NotEmpty(t, entries[0].(map[string]any)["EventId"])
			},
		},
		{
			name: "states_start_execution_sync_v2_json_output",
			definition: `{"StartAt":"N","States":{"N":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::states:startExecution.sync:2","Parameters":{` +
				`"StateMachineArn":"` + aws.ToString(child.StateMachineArn) + `","Input":{"x":1}},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "MyValue", jsonAt(t, out.Output, "Output", "MyKey"))
				assert.Equal(t, "SUCCEEDED", jsonAt(t, out.Output, "Status"))
			},
		},
		{
			name: "states_start_execution_sync_string_output",
			definition: `{"StartAt":"N","States":{"N":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::states:startExecution.sync","Parameters":{` +
				`"StateMachineArn":"` + aws.ToString(child.StateMachineArn) + `"},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.JSONEq(t, `{"MyKey":"MyValue"}`, jsonAt(t, out.Output, "Output").(string))
			},
		},
		{
			name: "states_start_execution_async_returns_arn",
			definition: `{"StartAt":"N","States":{"N":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::states:startExecution","Parameters":{` +
				`"StateMachineArn":"` + aws.ToString(child.StateMachineArn) + `"},"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Contains(t, jsonAt(t, out.Output, "ExecutionArn"), ":execution:sfn-sdk-child:")
			},
		},
		{
			name: "states_start_execution_sync_child_failure_caught",
			definition: `{"StartAt":"N","States":{"N":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::states:startExecution.sync:2","Parameters":{` +
				`"StateMachineArn":"` + aws.ToString(failChild.StateMachineArn) + `"},` +
				catchTo("States.TaskFailed") + `,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "States.TaskFailed", jsonAt(t, out.Output, "err", "Error"))
				assert.Contains(t, jsonAt(t, out.Output, "err", "Cause"), "Boom")
			},
		},
		{
			name: "lambda_sdk_missing_function_caught",
			definition: `{"StartAt":"L","States":{"L":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:lambda:getFunction",` +
				`"Parameters":{"FunctionName":"no-such-fn"},` + catchTo(
				"Lambda.ResourceNotFoundException",
			) +
				`,"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "Lambda.ResourceNotFoundException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "athena_start_query_sync",
			definition: `{"StartAt":"A","States":{"A":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::athena:startQueryExecution.sync","Parameters":{` +
				`"QueryString":"SELECT 1","ResultConfiguration":{"OutputLocation":"s3://sfn-sdk-bucket/athena/"}},` +
				`"End":true}}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "SUCCEEDED", jsonAt(t, out.Output, "QueryExecution", "Status", "State"))
			},
		},
		{
			name: "jsonata_sdk_catch_output",
			definition: `{"QueryLanguage":"JSONata","StartAt":"G","States":{"G":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::aws-sdk:dynamodb:getItem","Arguments":{` +
				`"TableName":"no-such-table","Key":{"id":{"S":"a"}}},` +
				`"Catch":[{"ErrorEquals":["DynamoDb.ResourceNotFoundException"],"Output":"{% $states.errorOutput %}",` +
				`"Next":"Caught"}],"End":true},` + caughtState + `}}`,
			input: `{}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "DynamoDb.ResourceNotFoundException", jsonAt(t, out.Output, "Error"))
			},
		},
		{
			name: "jsonata_sdk_arguments_output",
			definition: `{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Task",` +
				`"Resource":"arn:aws:states:::aws-sdk:sqs:sendMessage","Arguments":{` +
				`"QueueUrl":"{% $states.input.q %}","MessageBody":"jsonata"},` +
				`"Output":"{% $states.result.MessageId %}","End":true}}}`,
			input: `{"q":"` + aws.ToString(jq.QueueUrl) + `"}`,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Regexp(t, `^"[0-9a-f-]{36}"$`, aws.ToString(out.Output))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := runSyncSFN(t, fx, "sm-"+tt.name, tt.definition, tt.input)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status,
				"error=%s cause=%s", aws.ToString(out.Error), aws.ToString(out.Cause))
			tt.check(t, out)
		})
	}
}

func TestStepFunctionsSDKIntegration_Faults(t *testing.T) {
	t.Parallel()

	const lambdaFn = "arn:aws:lambda:us-east-1:000000000000:function:fake"

	tests := []struct {
		setup      func(fx *sfnFixture) func() int64
		check      func(t *testing.T, out *sfn.StartSyncExecutionOutput)
		name       string
		definition string
		wantCalls  int64
	}{
		{
			name: "lambda_invoke_success_parses_payload",
			setup: func(fx *sfnFixture) func() int64 {
				f := &fakeLambda{fn: func(_ int64, p []byte) ([]byte, error) {
					return append([]byte(`{"echo":`), append(p, '}')...), nil
				}}
				fx.sfn.SetLambdaInvoker(f)

				return f.calls.Load
			},
			definition: `{"StartAt":"L","States":{"L":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke",` +
				`"Parameters":{"FunctionName":"` + lambdaFn + `","Payload":{"a":1}},"End":true}}}`,
			wantCalls: 1,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.InDelta(t, 1, jsonAt(t, out.Output, "Payload", "echo", "a"), 0)
				assert.InDelta(t, 200, jsonAt(t, out.Output, "StatusCode"), 0)
				assert.Equal(t, "$LATEST", jsonAt(t, out.Output, "ExecutedVersion"))
			},
		},
		{
			name: "lambda_function_error_caught_by_error_type",
			setup: func(fx *sfnFixture) func() int64 {
				f := &fakeLambda{fn: func(int64, []byte) ([]byte, error) {
					return []byte(`{"errorType":"CustomError","errorMessage":"bad"}`), nil
				}}
				fx.sfn.SetLambdaInvoker(f)

				return f.calls.Load
			},
			definition: `{"StartAt":"L","States":{"L":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke",` +
				`"Parameters":{"FunctionName":"` + lambdaFn + `"},` + catchTo("CustomError") + `,"End":true},` +
				caughtState + `}}`,
			wantCalls: 1,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "CustomError", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "lambda_service_exception_retried",
			setup: func(fx *sfnFixture) func() int64 {
				f := &fakeLambda{fn: func(attempt int64, _ []byte) ([]byte, error) {
					if attempt < 3 {
						return nil, errFakeServiceException
					}

					return []byte(`"ok"`), nil
				}}
				fx.sfn.SetLambdaInvoker(f)

				return f.calls.Load
			},
			definition: `{"StartAt":"L","States":{"L":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke",` +
				`"Parameters":{"FunctionName":"` + lambdaFn + `"},"Retry":[{"ErrorEquals":["Lambda.ServiceException"],` +
				`"IntervalSeconds":1,"MaxAttempts":3,"BackoffRate":1}],"End":true}}}`,
			wantCalls: 3,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "ok", jsonAt(t, out.Output, "Payload"))
			},
		},
		{
			name: "dynamodb_sdk_throttle_retried_then_succeeds",
			setup: func(fx *sfnFixture) func() int64 {
				flaky := &flakyHandler{next: fx.handler, n: 2}
				fx.sfn.SetSDKIntegration(sfnbackend.NewSDKIntegration(flaky, "us-east-1"))

				return flaky.calls.Load
			},
			definition: `{"StartAt":"G","States":{"G":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:dynamodb:getItem",` +
				`"Parameters":{"TableName":"retry-table","Key":{"id":{"S":"a"}}},` +
				`"Retry":[{"ErrorEquals":["DynamoDb.ProvisionedThroughputExceededException"],` +
				`"IntervalSeconds":1,"MaxAttempts":3,"BackoffRate":1}],"End":true}}}`,
			wantCalls: 3,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotNil(t, out.Output)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)

			_, err := dynamodb.NewFromConfig(fx.cfg).CreateTable(t.Context(), &dynamodb.CreateTableInput{
				TableName:   aws.String("retry-table"),
				BillingMode: ddbtypes.BillingModePayPerRequest,
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			calls := tt.setup(fx)

			out := runSyncSFN(t, fx, "sm-"+tt.name, tt.definition, `{}`)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status,
				"error=%s cause=%s", aws.ToString(out.Error), aws.ToString(out.Cause))
			assert.Equal(t, tt.wantCalls, calls())
			tt.check(t, out)
		})
	}
}

func TestStepFunctionsSDKIntegration_ExecutionRole(t *testing.T) {
	t.Parallel()

	const (
		sqsAllowPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:SendMessage","Resource":"*"}]}`
		trustStates    = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
		trustLambda = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	)

	sendDef := func(queueURL string) string {
		return `{"StartAt":"S","States":{"S":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sqs:sendMessage",` +
			`"Parameters":{"QueueUrl":"` + queueURL + `","MessageBody":"x"},"End":true}}}`
	}

	getDef := func(errName string) string {
		return `{"StartAt":"G","States":{"G":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:dynamodb:listTables",` +
			`"Parameters":{},` + catchTo(
			errName,
		) + `,"End":true},` + caughtState + `}}`
	}

	tests := []struct {
		check      func(t *testing.T, out *sfn.StartSyncExecutionOutput)
		name       string
		trust      string
		policy     string
		definition func(queueURL string) string
		role       string
		enforce    bool
	}{
		{
			name: "enforced_allowed_action", enforce: true, trust: trustStates, policy: sqsAllowPolicy,
			role: "sfn-allowed", definition: sendDef,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotEmpty(t, jsonAt(t, out.Output, "MessageId"))
			},
		},
		{
			name: "enforced_denied_action_caught", enforce: true, trust: trustStates, policy: sqsAllowPolicy,
			role:       "sfn-denied",
			definition: func(string) string { return getDef("DynamoDb.AccessDeniedException") },
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "DynamoDb.AccessDeniedException", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "enforced_untrusted_role_is_permissions_error", enforce: true, trust: trustLambda,
			policy: sqsAllowPolicy, role: "sfn-untrusted",
			definition: func(string) string { return getDef("States.Permissions") },
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "States.Permissions", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "enforced_missing_role_is_permissions_error", enforce: true, role: "sfn-absent",
			definition: func(string) string { return getDef("States.Permissions") },
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.Equal(t, "States.Permissions", jsonAt(t, out.Output, "err", "Error"))
			},
		},
		{
			name: "not_enforced_ignores_role", role: "sfn-absent-unenforced", definition: sendDef,
			check: func(t *testing.T, out *sfn.StartSyncExecutionOutput) {
				t.Helper()
				assert.NotEmpty(t, jsonAt(t, out.Output, "MessageId"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			ctx := t.Context()

			if tt.trust != "" {
				iamc := iam.NewFromConfig(fx.cfg)
				_, err := iamc.CreateRole(ctx, &iam.CreateRoleInput{
					RoleName: aws.String(tt.role), AssumeRolePolicyDocument: aws.String(tt.trust),
				})
				require.NoError(t, err)

				_, err = iamc.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
					RoleName: aws.String(tt.role), PolicyName: aws.String("p"), PolicyDocument: aws.String(tt.policy),
				})
				require.NoError(t, err)
			}

			q, err := sqs.NewFromConfig(fx.cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("role-q")})
			require.NoError(t, err)

			out := runSyncSFNAs(
				t, fx, "arn:aws:iam::000000000000:role/"+tt.role, "sm-"+tt.name,
				tt.definition(aws.ToString(q.QueueUrl)), `{}`,
			)
			require.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status,
				"error=%s cause=%s", aws.ToString(out.Error), aws.ToString(out.Cause))
			tt.check(t, out)
		})
	}
}
