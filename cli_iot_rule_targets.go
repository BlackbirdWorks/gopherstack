package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ddbmodels "github.com/blackbirdworks/gopherstack/services/dynamodb/models"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	iotdataplanebackend "github.com/blackbirdworks/gopherstack/services/iotdataplane"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// wireIoTActionTargets connects the IoT rule actions beyond SQS and Lambda to their service backends.
func wireIoTActionTargets(byName map[string]service.Registerable) {
	iotH, ok := byName["IoT"].(*iotbackend.Handler)
	if !ok {
		return
	}

	iotBk, bkOk := iotH.Backend.(*iotbackend.InMemoryBackend)
	if !bkOk {
		return
	}

	t := &iotbackend.ActionTargets{}

	wireIoTStreamTargets(t, byName)
	wireIoTStoreTargets(t, byName)
	wireIoTOpsTargets(t, byName)
	wireIoTLookupTargets(t, byName)

	iotBk.SetActionTargets(t)
}

func wireIoTStreamTargets(t *iotbackend.ActionTargets, byName map[string]service.Registerable) {
	if h, ok := byName["SNS"].(*snsbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*snsbackend.InMemoryBackend); bkOk {
			t.SNS = &iotSNSTarget{backend: bk}
		}
	}

	if h, ok := byName["Kinesis"].(*kinesisbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*kinesisbackend.InMemoryBackend); bkOk {
			t.Kinesis = &iotKinesisTarget{backend: bk}
		}
	}

	if h, ok := byName["Firehose"].(*firehosebackend.Handler); ok {
		if bk, bkOk := h.Backend.(*firehosebackend.InMemoryBackend); bkOk {
			t.Firehose = &iotFirehoseTarget{backend: bk}
		}
	}

	if h, ok := byName["IoTAnalytics"].(*iotanalyticsbackend.Handler); ok {
		t.Analytics = &iotAnalyticsTarget{handler: h}
	}
}

func wireIoTStoreTargets(t *iotbackend.ActionTargets, byName map[string]service.Registerable) {
	if h, ok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); ok {
		if bk, bkOk := h.Backend.(*ddbbackend.InMemoryDB); bkOk {
			t.DynamoDB = &iotDynamoTarget{db: bk}
		}
	}

	if h, ok := byName["S3"].(*s3backend.S3Handler); ok {
		t.S3 = &iotS3Target{backend: h.Backend}
	}
}

func wireIoTOpsTargets(t *iotbackend.ActionTargets, byName map[string]service.Registerable) {
	if h, ok := byName["CloudWatch"].(*cwbackend.Handler); ok {
		cw := &iotCloudWatchTarget{handler: h}
		t.Metrics, t.Alarms = cw, cw
	}

	if h, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*cwlogsbackend.InMemoryBackend); bkOk {
			t.Logs = &iotLogsTarget{backend: bk}
		}
	}

	if h, ok := byName["StepFunctions"].(*sfnbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*sfnbackend.InMemoryBackend); bkOk {
			t.StepFunctions = &iotStepFunctionsTarget{backend: bk}
		}
	}
}

type iotSNSTarget struct{ backend *snsbackend.InMemoryBackend }

func (a *iotSNSTarget) PublishToTopic(_ context.Context, _, topicARN, message string, jsonFormat bool) error {
	structure := ""
	if jsonFormat {
		structure = "json"
	}

	_, err := a.backend.Publish(topicARN, message, "", structure, nil)

	return err
}

type iotKinesisTarget struct {
	backend *kinesisbackend.InMemoryBackend
}

func (a *iotKinesisTarget) PutRecord(ctx context.Context, _, stream, partitionKey string, data []byte) error {
	_, err := a.backend.PutRecord(ctx, &kinesisbackend.PutRecordInput{
		StreamName: stream, PartitionKey: partitionKey, Data: data,
	})

	return err
}

type iotFirehoseTarget struct {
	backend *firehosebackend.InMemoryBackend
}

func (a *iotFirehoseTarget) PutRecords(ctx context.Context, region, stream string, records [][]byte) error {
	ctx = inRegion(ctx, region)

	if len(records) == 1 {
		return a.backend.PutRecord(ctx, stream, records[0])
	}

	failed, err := a.backend.PutRecordBatch(ctx, stream, records)
	if err != nil {
		return err
	}

	if failed > 0 {
		return fmt.Errorf("%w: %d of %d records", errIoTBatchRejected, failed, len(records))
	}

	return nil
}

var errIoTBatchRejected = errors.New("firehose rejected batch records")

type iotDynamoTarget struct{ db *ddbbackend.InMemoryDB }

func (a *iotDynamoTarget) PutItem(ctx context.Context, region, table string, item map[string]any) error {
	sdkItem, err := ddbmodels.ToSDKItem(item)
	if err != nil {
		return fmt.Errorf("iot dynamodb target: item: %w", err)
	}

	_, err = a.db.PutItem(ddbbackend.WithRegion(ctx, region), &dynamodb.PutItemInput{
		TableName: aws.String(table), Item: sdkItem,
	})

	return err
}

func (a *iotDynamoTarget) SetAttribute(
	ctx context.Context, region, table string, key map[string]any, attr string, val map[string]any,
) error {
	sdkKey, err := ddbmodels.ToSDKItem(key)
	if err != nil {
		return fmt.Errorf("iot dynamodb target: key: %w", err)
	}

	sdkVal, err := ddbmodels.ToSDKAttributeValue(val)
	if err != nil {
		return fmt.Errorf("iot dynamodb target: value: %w", err)
	}

	_, err = a.db.UpdateItem(ddbbackend.WithRegion(ctx, region), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       sdkKey,
		UpdateExpression:          aws.String("SET #a = :v"),
		ExpressionAttributeNames:  map[string]string{"#a": attr},
		ExpressionAttributeValues: map[string]ddbsdktypes.AttributeValue{":v": sdkVal},
	})

	return err
}

func (a *iotDynamoTarget) DeleteItem(ctx context.Context, region, table string, key map[string]any) error {
	sdkKey, err := ddbmodels.ToSDKItem(key)
	if err != nil {
		return fmt.Errorf("iot dynamodb target: key: %w", err)
	}

	_, err = a.db.DeleteItem(ddbbackend.WithRegion(ctx, region), &dynamodb.DeleteItemInput{
		TableName: aws.String(table), Key: sdkKey,
	})

	return err
}

type iotS3Target struct{ backend s3backend.StorageBackend }

func (a *iotS3Target) PutObject(ctx context.Context, _, bucket, key string, data []byte, cannedACL string) error {
	_, err := a.backend.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		ACL: s3types.ObjectCannedACL(cannedACL),
	})

	return err
}

type iotCloudWatchTarget struct{ handler *cwbackend.Handler }

func (a *iotCloudWatchTarget) PutMetric(
	_ context.Context, region, namespace, name, unit string, value float64, ts time.Time,
) error {
	return a.handler.BackendFor(region).PutMetricData(namespace, []cwbackend.MetricDatum{{
		MetricName: name, Namespace: namespace, Unit: unit, Value: value, Count: 1, Sum: value, Min: value,
		Max: value, Timestamp: ts,
	}})
}

func (a *iotCloudWatchTarget) SetAlarmState(ctx context.Context, region, alarm, state, reason string) error {
	return a.handler.BackendFor(region).SetAlarmState(ctx, alarm, state, reason, "")
}

type iotLogsTarget struct {
	backend *cwlogsbackend.InMemoryBackend
}

func (a *iotLogsTarget) PutLogEvents(
	ctx context.Context, region, group, stream string, events []iotbackend.LogEvent,
) error {
	ctx = cwlogsbackend.WithRegion(ctx, region)

	if _, err := a.backend.CreateLogStream(ctx, group, stream); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return err
	}

	in := make([]cwlogsbackend.InputLogEvent, len(events))
	for i, e := range events {
		in[i] = cwlogsbackend.InputLogEvent{Message: e.Message, Timestamp: e.Timestamp}
	}

	_, err := a.backend.PutLogEvents(ctx, group, stream, "", in)

	return err
}

type iotStepFunctionsTarget struct{ backend *sfnbackend.InMemoryBackend }

func (a *iotStepFunctionsTarget) StartExecution(
	_ context.Context, region, account, stateMachine, execName, input string,
) error {
	smARN := arn.Build("states", region, account, "stateMachine:"+stateMachine)
	_, err := a.backend.StartExecution(smARN, execName, input)

	return err
}

var errIoTAnalyticsRegionUnavailable = errors.New("iotanalytics backend unavailable for region")

type iotAnalyticsTarget struct {
	handler *iotanalyticsbackend.Handler
}

func (a *iotAnalyticsTarget) PutChannelMessages(_ context.Context, region, channel string, payloads [][]byte) error {
	bk, ok := regionpeers.Backend[*iotanalyticsbackend.InMemoryBackend](a.handler.BackendFor, region)
	if !ok {
		return errIoTAnalyticsRegionUnavailable
	}

	return bk.PutChannelMessages(channel, payloads)
}

func wireIoTLookupTargets(t *iotbackend.ActionTargets, byName map[string]service.Registerable) {
	if h, ok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); ok {
		if bk, bkOk := h.Backend.(*ddbbackend.InMemoryDB); bkOk {
			t.DynamoReader = &iotDynamoTarget{db: bk}
		}
	}

	if h, ok := byName["SecretsManager"].(*secretsmanagerbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*secretsmanagerbackend.InMemoryBackend); bkOk {
			t.Secrets = &iotSecretsTarget{backend: bk}
		}
	}

	if h, ok := byName["IoTDataPlane"].(*iotdataplanebackend.Handler); ok {
		t.Shadows = &iotShadowTarget{handler: h}
	}

	if h, ok := byName["Lambda"].(*lambdabackend.Handler); ok {
		if bk, bkOk := h.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			t.Lambda = &iotLambdaTarget{backend: bk}
		}
	}

	if h, ok := byName["STS"].(*stsbackend.Handler); ok {
		if bk, bkOk := h.Backend.(*stsbackend.InMemoryBackend); bkOk {
			t.Credentials = &iotRoleCredentials{sts: bk}
		}
	}
}

func (a *iotDynamoTarget) GetItem(
	ctx context.Context, region, table string, key map[string]any,
) (map[string]any, error) {
	sdkKey, err := ddbmodels.ToSDKItem(key)
	if err != nil {
		return nil, fmt.Errorf("iot dynamodb target: key: %w", err)
	}

	out, err := a.db.GetItem(ddbbackend.WithRegion(ctx, region), &dynamodb.GetItemInput{
		TableName: aws.String(table), Key: sdkKey,
	})
	if err != nil || len(out.Item) == 0 {
		return nil, err
	}

	return ddbmodels.FromSDKItem(out.Item), nil
}

type iotSecretsTarget struct {
	backend *secretsmanagerbackend.InMemoryBackend
}

func (a *iotSecretsTarget) GetSecretValue(
	ctx context.Context,
	region, secretID string,
) (iotbackend.SecretValue, error) {
	out, err := a.backend.GetSecretValue(secretsmanagerbackend.WithRegion(ctx, region),
		&secretsmanagerbackend.GetSecretValueInput{SecretID: secretID})
	if err != nil {
		return iotbackend.SecretValue{}, err
	}

	return iotbackend.SecretValue{ARN: out.ARN, String: out.SecretString, Binary: out.SecretBinary}, nil
}

type iotShadowTarget struct {
	handler *iotdataplanebackend.Handler
}

func (a *iotShadowTarget) GetThingShadow(_ context.Context, region, thingName, shadowName string) ([]byte, error) {
	return a.handler.BackendFor(region).GetThingShadow(thingName, shadowName)
}

var errIoTLambdaFunction = errors.New("lambda function returned an error")

type iotLambdaTarget struct {
	backend *lambdabackend.InMemoryBackend
}

func (a *iotLambdaTarget) RequestLambda(ctx context.Context, _, functionARN string, payload []byte) ([]byte, error) {
	out, _, fnErr, _, err := a.backend.InvokeFunctionWithQualifier(
		ctx, functionARN, "", "", "", lambdabackend.InvocationTypeRequestResponse, payload)
	if err != nil {
		return nil, err
	}

	if fnErr != "" {
		return nil, errIoTLambdaFunction
	}

	return out, nil
}

type iotRoleCredentials struct{ sts *stsbackend.InMemoryBackend }

func (a *iotRoleCredentials) IssueRoleCredentials(roleARN string) (aws.Credentials, error) {
	out, err := a.sts.AssumeRoleForService("iot.amazonaws.com", roleARN, "iot-rules-engine")
	if err != nil {
		return aws.Credentials{}, err
	}

	c := out.AssumeRoleResult.Credentials

	return aws.Credentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken,
	}, nil
}
