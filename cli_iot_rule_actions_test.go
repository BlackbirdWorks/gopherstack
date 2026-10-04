package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

type iotRuleFixture struct {
	fx     *sfnFixture
	broker *iotbackend.Broker
	role   string
	port   int
}

func newIoTRuleFixture(t *testing.T, enforce bool, rolePerm string) *iotRuleFixture {
	t.Helper()

	fx := newSFNFixtureIAM(t, enforce)

	iotH, ok := serviceByName(fx.services)["IoT"].(*iotbackend.Handler)
	require.True(t, ok)

	iotBk, ok := iotH.Backend.(*iotbackend.InMemoryBackend)
	require.True(t, ok)

	port := freeTCPPort(t)
	broker := iotbackend.NewBroker(iotBk, port)

	go broker.Run(t.Context())

	return &iotRuleFixture{
		fx: fx, broker: broker, port: port,
		role: authzRole(t, fx, "iot-action-role", "iot.amazonaws.com", rolePerm),
	}
}

func (f *iotRuleFixture) createRule(
	t *testing.T, name, sql string, errAction *iottypes.Action, acts ...iottypes.Action,
) {
	t.Helper()

	_, err := iot.NewFromConfig(f.fx.cfg).CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
		RuleName: aws.String(name),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql: aws.String(sql), Actions: acts, ErrorAction: errAction,
		},
	})
	require.NoError(t, err)
}

func (f *iotRuleFixture) publish(t *testing.T, topic, payload string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return f.broker.Publish(topic, []byte(payload), false, 0) == nil
	}, authzDeadline, authzTick)
}

func (f *iotRuleFixture) bodies(t *testing.T, queueURL string, want int) []string {
	t.Helper()

	var out []string

	require.Eventually(t, func() bool {
		res, err := sqs.NewFromConfig(f.fx.cfg).ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10,
		})
		if err != nil {
			return false
		}

		for _, m := range res.Messages {
			out = append(out, aws.ToString(m.Body))
		}

		return len(out) >= want
	}, authzDeadline, authzTick)

	return out
}

func (f *iotRuleFixture) errAction(t *testing.T) (*iottypes.Action, string) {
	t.Helper()

	url, _ := authzQueue(t, f.fx, "iot-error")
	role := authzRole(t, f.fx, "iot-error-role", "iot.amazonaws.com", "sqs:SendMessage")

	return &iottypes.Action{Sqs: &iottypes.SqsAction{QueueUrl: aws.String(url), RoleArn: aws.String(role)}}, url
}

func iotTable(t *testing.T, f *iotRuleFixture, name string, withRange bool) {
	t.Helper()

	keys := []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}
	attrs := []ddbtypes.AttributeDefinition{{
		AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS,
	}}

	if withRange {
		keys = append(keys, ddbtypes.KeySchemaElement{AttributeName: aws.String("seq"), KeyType: ddbtypes.KeyTypeRange})
		attrs = append(attrs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String("seq"), AttributeType: ddbtypes.ScalarAttributeTypeN,
		})
	}

	_, err := dynamodb.NewFromConfig(f.fx.cfg).CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(name), KeySchema: keys, AttributeDefinitions: attrs,
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

func iotGetItem(
	t *testing.T, f *iotRuleFixture, table string, key map[string]ddbtypes.AttributeValue,
) map[string]ddbtypes.AttributeValue {
	t.Helper()

	var item map[string]ddbtypes.AttributeValue

	require.Eventually(t, func() bool {
		out, err := dynamodb.NewFromConfig(f.fx.cfg).GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(table), Key: key, ConsistentRead: aws.Bool(true),
		})
		item = out.Item

		return err == nil && len(item) > 0
	}, authzDeadline, authzTick)

	return item
}

func TestIoTRuleActionsDeliver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *iotRuleFixture)
		name string
	}{
		{iotCaseKinesis, "kinesis_partition_key_template"},
		{iotCaseSNSRaw, "sns_raw"},
		{iotCaseSNSJSON, "sns_json"},
		{iotCaseSNSJSONInvalid, "sns_json_without_default_runs_error_action"},
		{iotCaseFirehose, "firehose_batch_separator"},
		{iotCaseDynamoInsert, "dynamodb_insert"},
		{iotCaseDynamoUpdate, "dynamodb_update_payload_field"},
		{iotCaseDynamoDelete, "dynamodb_delete"},
		{iotCaseDynamoV2, "dynamodbv2_put_item"},
		{iotCaseS3, "s3_key_template"},
		{iotCaseMetric, "cloudwatch_metric"},
		{iotCaseAlarm, "cloudwatch_alarm"},
		{iotCaseLogsSingle, "cloudwatch_logs"},
		{iotCaseLogsBatch, "cloudwatch_logs_batch"},
		{iotCaseStepFunctions, "step_functions_prefix"},
		{iotCaseAnalytics, "iot_analytics_channel"},
		{iotCaseRepublish, "republish_template"},
		{iotCaseRepublishLoop, "republish_loop_ends"},
		{iotCaseTemplateError, "template_error_runs_error_action"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newIoTRuleFixture(t, false, "*"))
		})
	}
}

func iotCaseKinesis(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	kinesisStreamARN(t, f.fx, "iot-stream", true)
	f.createRule(t, "r", "SELECT * FROM 'dev/+'", nil, iottypes.Action{Kinesis: &iottypes.KinesisAction{
		RoleArn: aws.String(f.role), StreamName: aws.String("iot-stream"),
		PartitionKey: aws.String("${deviceId}-${topic(2)}"),
	}})
	f.publish(t, "dev/a1", `{"deviceId":"d7"}`)

	require.Eventually(t, func() bool {
		return len(kinesisRecords(t, f.fx, "iot-stream")) > 0
	}, authzDeadline, authzTick)

	rec := kinesisRecords(t, f.fx, "iot-stream")[0]
	assert.Equal(t, "d7-a1", aws.ToString(rec.PartitionKey))
	assert.JSONEq(t, `{"deviceId":"d7"}`, string(rec.Data))
}

func iotCaseSNSRaw(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotCaseSNS(t, f, "RAW", `{"k":"v"}`, `{"k":"v"}`)
}

func iotCaseSNSJSON(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotCaseSNS(t, f, "JSON", `{"default":"hello"}`, "hello")
}

func iotCaseLogsSingle(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotCaseLogs(t, f, false)
}

func iotCaseLogsBatch(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotCaseLogs(t, f, true)
}

func iotCaseSNS(t *testing.T, f *iotRuleFixture, format, payload, want string) {
	t.Helper()

	url, arn := authzQueue(t, f.fx, "iot-sns-q")
	snsc := sns.NewFromConfig(f.fx.cfg)

	topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("iot-topic")})
	require.NoError(t, err)

	_, err = snsc.Subscribe(t.Context(), &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(arn),
	})
	require.NoError(t, err)

	f.createRule(t, "r", "SELECT * FROM 'sns/topic'", nil, iottypes.Action{Sns: &iottypes.SnsAction{
		RoleArn: aws.String(f.role), TargetArn: topic.TopicArn, MessageFormat: iottypes.MessageFormat(format),
	}})
	f.publish(t, "sns/topic", payload)

	var env struct {
		Message string `json:"Message"`
	}

	require.NoError(t, json.Unmarshal([]byte(f.bodies(t, url, 1)[0]), &env))
	assert.Contains(t, env.Message, want)
}

func iotCaseSNSJSONInvalid(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	snsc := sns.NewFromConfig(f.fx.cfg)

	topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("iot-bad-topic")})
	require.NoError(t, err)

	errAct, errURL := f.errAction(t)
	f.createRule(t, "r", "SELECT * FROM 'sns/bad'", errAct, iottypes.Action{Sns: &iottypes.SnsAction{
		RoleArn: aws.String(f.role), TargetArn: topic.TopicArn, MessageFormat: iottypes.MessageFormatJson,
	}})
	f.publish(t, "sns/bad", `{"k":"v"}`)

	assert.Contains(t, f.bodies(t, errURL, 1)[0], "SnsAction")
}

func iotCaseFirehose(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	s3c := s3.NewFromConfig(f.fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("iot-fh-bucket")})
	require.NoError(t, err)

	fhRole := authzRole(t, f.fx, "iot-fh-dest", "firehose.amazonaws.com", "s3:PutObject")
	authzFirehoseStream(t, f.fx, "iot-fh", "iot-fh-bucket", fhRole, false)

	f.createRule(t, "r", "SELECT * FROM 'fh/topic'", nil, iottypes.Action{Firehose: &iottypes.FirehoseAction{
		RoleArn: aws.String(f.role), DeliveryStreamName: aws.String("iot-fh"),
		Separator: aws.String("\n"), BatchMode: aws.Bool(true),
	}})
	f.publish(t, "fh/topic", `[{"a":1},{"a":2}]`)

	fhH, ok := serviceByName(f.fx.services)["Firehose"].(*firehosebackend.Handler)
	require.True(t, ok)

	fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
	require.True(t, ok)

	var body string

	require.Eventually(t, func() bool {
		fhBk.FlushAll(t.Context())

		listed, lerr := s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String("iot-fh-bucket")})
		if lerr != nil || len(listed.Contents) == 0 {
			return false
		}

		obj, gerr := s3c.GetObject(t.Context(), &s3.GetObjectInput{
			Bucket: aws.String("iot-fh-bucket"), Key: listed.Contents[0].Key,
		})
		if gerr != nil {
			return false
		}

		defer obj.Body.Close()

		data, rerr := io.ReadAll(obj.Body)
		body = string(data)

		return rerr == nil
	}, authzDeadline, authzTick)

	assert.Equal(t, "{\"a\":1}\n{\"a\":2}\n", body)
}

func iotCaseDynamoInsert(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotTable(t, f, "iot-ddb", true)
	f.createRule(t, "r", "SELECT * FROM 'ddb/+'", nil, iottypes.Action{DynamoDB: &iottypes.DynamoDBAction{
		RoleArn:      aws.String(f.role),
		TableName:    aws.String("iot-ddb"),
		HashKeyField: aws.String("id"),
		HashKeyValue: aws.String("${topic(2)}"),
		RangeKeyField: aws.String(
			"seq",
		),
		RangeKeyValue: aws.String("${seq}"),
		RangeKeyType:  iottypes.DynamoKeyTypeNumber,
	}})
	f.publish(t, "ddb/dev1", `{"seq":5,"temp":21.5}`)

	item := iotGetItem(t, f, "iot-ddb", map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "dev1"}, "seq": &ddbtypes.AttributeValueMemberN{Value: "5"},
	})

	payload, ok := item["payload"].(*ddbtypes.AttributeValueMemberM)
	require.True(t, ok)
	assert.Equal(t, "21.5", payload.Value["temp"].(*ddbtypes.AttributeValueMemberN).Value)
}

func iotCaseDynamoUpdate(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotTable(t, f, "iot-ddb-u", false)
	f.createRule(t, "r", "SELECT * FROM 'upd/+'", nil, iottypes.Action{DynamoDB: &iottypes.DynamoDBAction{
		RoleArn: aws.String(f.role), TableName: aws.String("iot-ddb-u"), HashKeyField: aws.String("id"),
		HashKeyValue: aws.String("${topic(2)}"), PayloadField: aws.String("latest"), Operation: aws.String("UPDATE"),
	}})
	f.publish(t, "upd/dev2", `{"temp":1}`)

	item := iotGetItem(t, f, "iot-ddb-u", map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "dev2"},
	})
	assert.Contains(t, item, "latest")
}

func iotCaseDynamoDelete(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotTable(t, f, "iot-ddb-d", false)

	ddbc := dynamodb.NewFromConfig(f.fx.cfg)
	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "dev3"}}
	_, err := ddbc.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("iot-ddb-d"), Item: key})
	require.NoError(t, err)

	f.createRule(t, "r", "SELECT * FROM 'del/+'", nil, iottypes.Action{DynamoDB: &iottypes.DynamoDBAction{
		RoleArn: aws.String(f.role), TableName: aws.String("iot-ddb-d"), HashKeyField: aws.String("id"),
		HashKeyValue: aws.String("${topic(2)}"), Operation: aws.String("DELETE"),
	}})
	f.publish(t, "del/dev3", `{}`)

	require.Eventually(t, func() bool {
		out, gerr := ddbc.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String("iot-ddb-d"), Key: key, ConsistentRead: aws.Bool(true),
		})

		return gerr == nil && len(out.Item) == 0
	}, authzDeadline, authzTick)
}

func iotCaseDynamoV2(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iotTable(t, f, "iot-ddb-v2", false)
	f.createRule(t, "r", "SELECT * FROM 'v2/topic'", nil, iottypes.Action{DynamoDBv2: &iottypes.DynamoDBv2Action{
		RoleArn: aws.String(f.role), PutItem: &iottypes.PutItemInput{TableName: aws.String("iot-ddb-v2")},
	}})
	f.publish(t, "v2/topic", `{"id":"x1","n":3,"flag":true}`)

	item := iotGetItem(t, f, "iot-ddb-v2", map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "x1"},
	})
	assert.Equal(t, "3", item["n"].(*ddbtypes.AttributeValueMemberN).Value)
	assert.True(t, item["flag"].(*ddbtypes.AttributeValueMemberBOOL).Value)
}

func iotCaseS3(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	s3c := s3.NewFromConfig(f.fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("iot-s3-bucket")})
	require.NoError(t, err)

	f.createRule(t, "r", "SELECT * FROM 's3/+'", nil, iottypes.Action{S3: &iottypes.S3Action{
		RoleArn: aws.String(f.role), BucketName: aws.String("iot-s3-bucket"),
		Key: aws.String("${topic(2)}/${deviceId}.json"), CannedAcl: iottypes.CannedAccessControlListPrivate,
	}})
	f.publish(t, "s3/room1", `{"deviceId":"d9"}`)

	var body string

	require.Eventually(t, func() bool {
		obj, gerr := s3c.GetObject(t.Context(), &s3.GetObjectInput{
			Bucket: aws.String("iot-s3-bucket"), Key: aws.String("room1/d9.json"),
		})
		if gerr != nil {
			return false
		}

		defer obj.Body.Close()

		data, _ := io.ReadAll(obj.Body)
		body = string(data)

		return true
	}, authzDeadline, authzTick)

	assert.JSONEq(t, `{"deviceId":"d9"}`, body)
}

func iotCaseMetric(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	f.createRule(t, "r", "SELECT * FROM 'm/+'", nil, iottypes.Action{CloudwatchMetric: &iottypes.CloudwatchMetricAction{
		RoleArn: aws.String(f.role), MetricNamespace: aws.String("IoT/${topic(2)}"), MetricName: aws.String("temp"),
		MetricValue: aws.String("${temp}"), MetricUnit: aws.String("Count"),
	}})
	f.publish(t, "m/dev1", `{"temp":42}`)

	require.Eventually(t, func() bool {
		out, err := cloudwatch.NewFromConfig(f.fx.cfg).ListMetrics(t.Context(), &cloudwatch.ListMetricsInput{
			Namespace: aws.String("IoT/dev1"),
		})

		return err == nil && len(out.Metrics) == 1 && aws.ToString(out.Metrics[0].MetricName) == "temp"
	}, authzDeadline, authzTick)
}

func iotCaseAlarm(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	cwc := cloudwatch.NewFromConfig(f.fx.cfg)
	_, err := cwc.PutMetricAlarm(t.Context(), &cloudwatch.PutMetricAlarmInput{
		AlarmName: aws.String("iot-alarm"), MetricName: aws.String("m"), Namespace: aws.String("n"),
		Statistic: cwtypes.StatisticAverage, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1),
		Threshold: aws.Float64(1), ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
	})
	require.NoError(t, err)

	f.createRule(
		t,
		"r",
		"SELECT * FROM 'alarm/topic'",
		nil,
		iottypes.Action{CloudwatchAlarm: &iottypes.CloudwatchAlarmAction{
			RoleArn: aws.String(f.role), AlarmName: aws.String("iot-alarm"),
			StateValue: aws.String("ALARM"), StateReason: aws.String("device ${deviceId} tripped"),
		}},
	)
	f.publish(t, "alarm/topic", `{"deviceId":"d1"}`)

	require.Eventually(t, func() bool {
		out, derr := cwc.DescribeAlarms(t.Context(), &cloudwatch.DescribeAlarmsInput{
			AlarmNames: []string{"iot-alarm"},
		})

		return derr == nil && len(out.MetricAlarms) == 1 &&
			out.MetricAlarms[0].StateValue == cwtypes.StateValueAlarm &&
			aws.ToString(out.MetricAlarms[0].StateReason) == "device d1 tripped"
	}, authzDeadline, authzTick)
}

func iotCaseLogs(t *testing.T, f *iotRuleFixture, batch bool) {
	t.Helper()

	logc := cloudwatchlogs.NewFromConfig(f.fx.cfg)
	_, err := logc.CreateLogGroup(
		t.Context(),
		&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("/iot/logs")},
	)
	require.NoError(t, err)

	logsAct := iottypes.Action{CloudwatchLogs: &iottypes.CloudwatchLogsAction{
		RoleArn: aws.String(f.role), LogGroupName: aws.String("/iot/logs"), BatchMode: aws.Bool(batch),
	}}
	f.createRule(t, "logrule", "SELECT * FROM 'log/topic'", nil, logsAct)

	payload, want := `{"k":"v"}`, []string{`{"k":"v"}`}
	if batch {
		now := time.Now().UnixMilli()
		payload = fmt.Sprintf(`[{"timestamp":%d,"message":"one"},{"timestamp":%d,"message":"two"}]`, now-2, now-1)
		want = []string{"one", "two"}
	}

	f.publish(t, "log/topic", payload)

	var got []string

	require.Eventually(t, func() bool {
		out, gerr := logc.GetLogEvents(t.Context(), &cloudwatchlogs.GetLogEventsInput{
			LogGroupName: aws.String("/iot/logs"), LogStreamName: aws.String("logrule"),
		})
		if gerr != nil {
			return false
		}

		got = got[:0]
		for _, e := range out.Events {
			got = append(got, aws.ToString(e.Message))
		}

		return len(got) >= len(want)
	}, authzDeadline, authzTick)

	assert.Equal(t, want, got)
}

func iotCaseStepFunctions(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	sfnc := sfn.NewFromConfig(f.fx.cfg)
	sm, err := sfnc.CreateStateMachine(t.Context(), &sfn.CreateStateMachineInput{
		Name:       aws.String("iot-sm"),
		Definition: aws.String(`{"StartAt":"p","States":{"p":{"Type":"Pass","End":true}}}`),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
	})
	require.NoError(t, err)

	f.createRule(t, "r", "SELECT * FROM 'sfn/topic'", nil, iottypes.Action{StepFunctions: &iottypes.StepFunctionsAction{
		RoleArn: aws.String(
			f.role,
		),
		StateMachineName:    aws.String("iot-sm"),
		ExecutionNamePrefix: aws.String("iot-run-"),
	}})
	f.publish(t, "sfn/topic", `{"x":1}`)

	require.Eventually(t, func() bool {
		out, lerr := sfnc.ListExecutions(t.Context(), &sfn.ListExecutionsInput{StateMachineArn: sm.StateMachineArn})

		return lerr == nil && len(out.Executions) == 1 &&
			strings.HasPrefix(aws.ToString(out.Executions[0].Name), "iot-run-")
	}, authzDeadline, authzTick)
}

func iotCaseAnalytics(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	iaH, ok := serviceByName(f.fx.services)["IoTAnalytics"].(*iotanalyticsbackend.Handler)
	require.True(t, ok)

	iaBk, ok := iaH.Backend.(*iotanalyticsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := iaBk.CreateChannel(t.Context(), "iot_chan", nil, nil, nil)
	require.NoError(t, err)

	f.createRule(t, "r", "SELECT * FROM 'ia/topic'", nil, iottypes.Action{IotAnalytics: &iottypes.IotAnalyticsAction{
		RoleArn: aws.String(f.role), ChannelName: aws.String("iot_chan"),
	}})
	f.publish(t, "ia/topic", `{"v":1}`)

	require.Eventually(t, func() bool {
		out, serr := iaBk.SampleChannelData("iot_chan", 10, false, 0, false, 0)

		return serr == nil && len(out) == 1 && string(out[0]) == `{"v":1}`
	}, authzDeadline, authzTick)
}

func iotCaseRepublish(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	url, _ := authzQueue(t, f.fx, "iot-republish-q")

	f.createRule(t, "fwd", "SELECT * FROM 'in/+'", nil, iottypes.Action{Republish: &iottypes.RepublishAction{
		RoleArn: aws.String(f.role), Topic: aws.String("out/${topic(2)}/${deviceId}"), Qos: aws.Int32(1),
	}})
	f.createRule(t, "sink", "SELECT * FROM 'out/#'", nil, iottypes.Action{Sqs: &iottypes.SqsAction{
		QueueUrl: aws.String(url), RoleArn: aws.String(f.role),
	}})
	f.publish(t, "in/dev5", `{"deviceId":"d5"}`)

	assert.JSONEq(t, `{"deviceId":"d5"}`, f.bodies(t, url, 1)[0])
}

func iotCaseRepublishLoop(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	url, _ := authzQueue(t, f.fx, "iot-loop-q")

	f.createRule(
		t,
		"loop",
		"SELECT * FROM 'loop/#'",
		nil,
		iottypes.Action{Sqs: &iottypes.SqsAction{QueueUrl: aws.String(url), RoleArn: aws.String(f.role)}},
		iottypes.Action{
			Republish: &iottypes.RepublishAction{RoleArn: aws.String(f.role), Topic: aws.String("loop/again")},
		},
	)
	f.publish(t, "loop/start", `{}`)

	const hopsPlusOriginal = 9

	assert.Len(t, f.bodies(t, url, hopsPlusOriginal), hopsPlusOriginal)
	assert.False(t, authzReceived(t, f.fx, url))
}

func iotCaseTemplateError(t *testing.T, f *iotRuleFixture) {
	t.Helper()

	kinesisStreamARN(t, f.fx, "iot-tpl-stream", true)

	errAct, errURL := f.errAction(t)
	f.createRule(t, "r", "SELECT * FROM 'tpl/topic'", errAct, iottypes.Action{Kinesis: &iottypes.KinesisAction{
		RoleArn: aws.String(f.role), StreamName: aws.String("iot-tpl-stream"), PartitionKey: aws.String("${absent}"),
	}})
	f.publish(t, "tpl/topic", `{"present":1}`)

	var env map[string]string
	require.NoError(t, json.Unmarshal([]byte(f.bodies(t, errURL, 1)[0]), &env))
	assert.Equal(t, "KinesisAction", env["failedAction"])
	assert.Equal(t, "r", env["ruleName"])
	assert.Empty(t, kinesisRecords(t, f.fx, "iot-tpl-stream"))
}

func TestIoTRuleActionsEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kind      string
		perm      string
		wantError bool
		enforce   bool
	}{
		{name: "kinesis_allowed", kind: "kinesis", perm: "kinesis:PutRecord", enforce: true},
		{name: "kinesis_denied", kind: "kinesis", perm: "sqs:SendMessage", enforce: true, wantError: true},
		{name: "kinesis_enforcement_off", kind: "kinesis", perm: "sqs:SendMessage"},
		{name: "s3_allowed", kind: "s3", perm: "s3:PutObject", enforce: true},
		{name: "s3_denied", kind: "s3", perm: "sqs:SendMessage", enforce: true, wantError: true},
		{name: "s3_enforcement_off", kind: "s3", perm: "sqs:SendMessage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, tt.enforce, tt.perm)
			errAct, errURL := f.errAction(t)

			var (
				act       iottypes.Action
				delivered func() bool
			)

			if tt.kind == "kinesis" {
				kinesisStreamARN(t, f.fx, "authz-stream", true)

				act = iottypes.Action{Kinesis: &iottypes.KinesisAction{
					RoleArn: aws.String(f.role), StreamName: aws.String("authz-stream"),
				}}
				delivered = func() bool { return len(kinesisRecords(t, f.fx, "authz-stream")) > 0 }
			} else {
				s3c := s3.NewFromConfig(f.fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
				_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("authz-bucket")})
				require.NoError(t, err)

				act = iottypes.Action{S3: &iottypes.S3Action{
					RoleArn: aws.String(f.role), BucketName: aws.String("authz-bucket"), Key: aws.String("k"),
				}}
				delivered = func() bool {
					_, gerr := s3c.HeadObject(t.Context(), &s3.HeadObjectInput{
						Bucket: aws.String("authz-bucket"), Key: aws.String("k"),
					})

					return gerr == nil
				}
			}

			f.createRule(t, "authz", "SELECT * FROM 'authz/topic'", errAct, act)
			f.publish(t, "authz/topic", `{"k":"v"}`)

			if tt.wantError {
				require.Eventually(t, func() bool { return authzReceived(t, f.fx, errURL) }, authzDeadline, authzTick)
				assert.False(t, delivered())

				return
			}

			require.Eventually(t, delivered, authzDeadline, authzTick)
			assert.False(t, authzReceived(t, f.fx, errURL))
		})
	}
}
