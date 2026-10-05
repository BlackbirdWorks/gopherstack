package main

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const metricWaitFor = 10 * time.Second

// metricWant is one expected CloudWatch series: exact dimensions, unit, and either an exact sum or, when
// sum is negative, only that at least one datapoint exists.
type metricWant struct {
	dims      map[string]string
	namespace string
	name      string
	unit      cwtypes.StandardUnit
	sum       float64
	minSum    bool
}

func (w metricWant) dimensions() []cwtypes.Dimension {
	out := make([]cwtypes.Dimension, 0, len(w.dims))
	for k, v := range w.dims {
		out = append(out, cwtypes.Dimension{Name: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (w metricWant) label() string {
	parts := make([]string, 0, 2+len(w.dims))
	parts = append(parts, w.namespace, w.name)

	for _, k := range slices.Sorted(maps.Keys(w.dims)) {
		parts = append(parts, k+"="+w.dims[k])
	}

	return strings.Join(parts, "/")
}

func assertMetricsEmitted(t *testing.T, fx *sfnFixture, wants []metricWant) {
	t.Helper()

	cw := cwsdk.NewFromConfig(fx.cfg)

	for _, w := range wants {
		t.Run(w.label(), func(t *testing.T) {
			t.Parallel()

			var (
				sum   float64
				count float64
				units []cwtypes.StandardUnit
			)

			require.Eventually(t, func() bool {
				now := time.Now()

				stats, err := cw.GetMetricStatistics(t.Context(), &cwsdk.GetMetricStatisticsInput{
					Namespace: aws.String(w.namespace), MetricName: aws.String(w.name),
					Dimensions: w.dimensions(),
					StartTime:  aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now.Add(time.Hour)),
					Period:     aws.Int32(3600),
					Statistics: []cwtypes.Statistic{cwtypes.StatisticSum, cwtypes.StatisticSampleCount},
				})
				if err != nil {
					return false
				}

				sum, count, units = 0, 0, units[:0]

				for _, dp := range stats.Datapoints {
					sum += aws.ToFloat64(dp.Sum)
					count += aws.ToFloat64(dp.SampleCount)
					units = append(units, dp.Unit)
				}

				if count == 0 {
					return false
				}

				if w.sum < 0 {
					return true
				}

				if w.minSum {
					return sum >= w.sum
				}

				return sum == w.sum
			}, metricWaitFor, 10*time.Millisecond, "series %s sum=%v count=%v", w.label(), sum, count)

			for _, u := range units {
				assert.Equal(t, w.unit, u)
			}

			list, err := cw.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{
				Namespace: aws.String(w.namespace), MetricName: aws.String(w.name),
				Dimensions: dimensionFilters(w.dims),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, list.Metrics)
		})
	}
}

func dimensionFilters(dims map[string]string) []cwtypes.DimensionFilter {
	out := make([]cwtypes.DimensionFilter, 0, len(dims))
	for k, v := range dims {
		out = append(out, cwtypes.DimensionFilter{Name: aws.String(k), Value: aws.String(v)})
	}

	return out
}

// dataPlane sends a request straight to the in-process router and returns the status.
func dataPlane(t *testing.T, fx *sfnFixture, path string) int {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	fx.handler.ServeHTTP(rec, req)

	return rec.Code
}

func TestServiceMetrics_Kinesis(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	kc := kinesis.NewFromConfig(fx.cfg)
	arn := kinesisStreamARN(t, fx, "mt-stream", true)
	require.NotEmpty(t, arn)

	_, err := kc.PutRecord(t.Context(), &kinesis.PutRecordInput{
		StreamName: aws.String("mt-stream"), PartitionKey: aws.String("pk"), Data: []byte("0123456789"),
	})
	require.NoError(t, err)

	rec := func() kintypes.PutRecordsRequestEntry {
		return kintypes.PutRecordsRequestEntry{PartitionKey: aws.String("k"), Data: []byte("abcd")}
	}
	_, err = kc.PutRecords(t.Context(), &kinesis.PutRecordsInput{
		StreamName: aws.String("mt-stream"), Records: []kintypes.PutRecordsRequestEntry{rec(), rec(), rec()},
	})
	require.NoError(t, err)

	require.Len(t, kinesisRecords(t, fx, "mt-stream"), 4)

	dims := map[string]string{"StreamName": "mt-stream"}
	const ns = "AWS/Kinesis"

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "IncomingRecords", dims: dims, unit: cwtypes.StandardUnitCount, sum: 4},
		{namespace: ns, name: "IncomingBytes", dims: dims, unit: cwtypes.StandardUnitBytes, sum: 27},
		{namespace: ns, name: "PutRecord.Success", dims: dims, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "PutRecord.Bytes", dims: dims, unit: cwtypes.StandardUnitBytes, sum: 12},
		{namespace: ns, name: "PutRecord.Latency", dims: dims, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "PutRecords.Success", dims: dims, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "PutRecords.Records", dims: dims, unit: cwtypes.StandardUnitCount, sum: 3},
		{namespace: ns, name: "PutRecords.Bytes", dims: dims, unit: cwtypes.StandardUnitBytes, sum: 15},
		{namespace: ns, name: "PutRecords.FailedRecords", dims: dims, unit: cwtypes.StandardUnitCount, sum: 0},
		{namespace: ns, name: "PutRecords.Latency", dims: dims, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "GetRecords.Success", dims: dims, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "GetRecords.Records", dims: dims, unit: cwtypes.StandardUnitCount, sum: 4},
		{namespace: ns, name: "GetRecords.Bytes", dims: dims, unit: cwtypes.StandardUnitBytes, sum: 22},
		{
			namespace: ns, name: "GetRecords.IteratorAgeMilliseconds", dims: dims,
			unit: cwtypes.StandardUnitMilliseconds, sum: -1,
		},
		{namespace: ns, name: "GetRecords.Latency", dims: dims, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
	})
}

func TestServiceMetrics_DynamoDB(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ddb := dynamodb.NewFromConfig(fx.cfg)

	_, err := ddb.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String("mt-table"), BillingMode: ddbtypes.BillingModePayPerRequest,
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	item := func(id string) map[string]ddbtypes.AttributeValue {
		return map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: id}}
	}

	for _, id := range []string{"a", "b"} {
		_, err = ddb.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("mt-table"), Item: item(id)})
		require.NoError(t, err)
	}

	_, err = ddb.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("mt-table"), Key: item("a")})
	require.NoError(t, err)

	_, err = ddb.Scan(t.Context(), &dynamodb.ScanInput{TableName: aws.String("mt-table")})
	require.NoError(t, err)

	_, err = ddb.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String("mt-table"), Item: item("a"), ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	require.Error(t, err)

	_, err = ddb.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("absent"), Key: item("a")})
	require.Error(t, err)

	table := map[string]string{"TableName": "mt-table"}
	op := func(name string) map[string]string {
		return map[string]string{"TableName": "mt-table", "Operation": name}
	}
	const ns = "AWS/DynamoDB"

	assertMetricsEmitted(t, fx, []metricWant{
		{
			namespace: ns, name: "ConsumedWriteCapacityUnits", dims: table,
			unit: cwtypes.StandardUnitCount, sum: 3, minSum: true,
		},
		{
			namespace: ns, name: "ConsumedReadCapacityUnits", dims: table,
			unit: cwtypes.StandardUnitCount, sum: 0.5, minSum: true,
		},
		{
			namespace: ns, name: "SuccessfulRequestLatency", dims: op("PutItem"),
			unit: cwtypes.StandardUnitMilliseconds, sum: -1,
		},
		{
			namespace: ns, name: "SuccessfulRequestLatency", dims: op("GetItem"),
			unit: cwtypes.StandardUnitMilliseconds, sum: -1,
		},
		{namespace: ns, name: "ReturnedItemCount", dims: op("Scan"), unit: cwtypes.StandardUnitCount, sum: 2},
		{namespace: ns, name: "ConditionalCheckFailedRequests", dims: table, unit: cwtypes.StandardUnitCount, sum: 1},
		{
			namespace: ns, name: "UserErrors", dims: map[string]string{},
			unit: cwtypes.StandardUnitCount, sum: 2, minSum: true,
		},
	})
}

func TestServiceMetrics_StepFunctions(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	sc := sfn.NewFromConfig(fx.cfg)

	create := func(name, def string) string {
		out, err := sc.CreateStateMachine(t.Context(), &sfn.CreateStateMachineInput{
			Name: aws.String(name), Definition: aws.String(def), RoleArn: aws.String(sfnTestRole),
		})
		require.NoError(t, err)

		return aws.ToString(out.StateMachineArn)
	}

	start := func(arn string) string {
		out, err := sc.StartExecution(t.Context(), &sfn.StartExecutionInput{StateMachineArn: aws.String(arn)})
		require.NoError(t, err)

		return aws.ToString(out.ExecutionArn)
	}

	okARN := create("mt-ok", `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`)
	failARN := create("mt-fail", `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"E","Cause":"c"}}}`)
	waitARN := create("mt-wait", `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":300,"End":true}}}`)

	start(okARN)
	start(failARN)

	execARN := start(waitARN)
	_, err := sc.StopExecution(t.Context(), &sfn.StopExecutionInput{ExecutionArn: aws.String(execARN)})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, derr := sc.DescribeExecution(t.Context(), &sfn.DescribeExecutionInput{ExecutionArn: aws.String(execARN)})

		return derr == nil && out.Status == sfntypes.ExecutionStatusAborted
	}, metricWaitFor, 10*time.Millisecond)

	d := func(arn string) map[string]string { return map[string]string{"StateMachineArn": arn} }
	const ns = "AWS/States"

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "ExecutionsStarted", dims: d(okARN), unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "ExecutionsSucceeded", dims: d(okARN), unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "ExecutionTime", dims: d(okARN), unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "ExecutionsFailed", dims: d(failARN), unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "ExecutionsAborted", dims: d(waitARN), unit: cwtypes.StandardUnitCount, sum: 1},
	})
}

func TestServiceMetrics_SNS(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	snsc := sns.NewFromConfig(fx.cfg)
	sqsc := sqs.NewFromConfig(fx.cfg)

	topic, err := snsc.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("mt-topic")})
	require.NoError(t, err)

	subscribe := func(queue string, attrs map[string]string) {
		q, qerr := sqsc.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(queue)})
		require.NoError(t, qerr)

		qa, qerr := sqsc.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		require.NoError(t, qerr)

		_, qerr = snsc.Subscribe(t.Context(), &sns.SubscribeInput{
			TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
			Endpoint: aws.String(qa.Attributes["QueueArn"]), Attributes: attrs,
		})
		require.NoError(t, qerr)
	}

	subscribe("mt-all", nil)
	subscribe("mt-red", map[string]string{"FilterPolicy": `{"color":["red"]}`})

	_, err = snsc.Publish(t.Context(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("hello")})
	require.NoError(t, err)

	d := map[string]string{"TopicName": "mt-topic"}
	const ns = "AWS/SNS"

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "NumberOfMessagesPublished", dims: d, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "PublishSize", dims: d, unit: cwtypes.StandardUnitBytes, sum: 5},
		{namespace: ns, name: "NumberOfNotificationsDelivered", dims: d, unit: cwtypes.StandardUnitCount, sum: 1},
		{
			namespace: ns, name: "NumberOfNotificationsFilteredOut-NoMessageAttributes", dims: d,
			unit: cwtypes.StandardUnitCount, sum: 1,
		},
	})
}

func TestServiceMetrics_Firehose(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	fh := firehose.NewFromConfig(fx.cfg)

	_, err := fh.CreateDeliveryStream(t.Context(), &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("mt-fh"),
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::mt-fh-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/fh"),
		},
	})
	require.NoError(t, err)

	_, err = fh.PutRecord(t.Context(), &firehose.PutRecordInput{
		DeliveryStreamName: aws.String("mt-fh"), Record: &fhtypes.Record{Data: []byte("0123")},
	})
	require.NoError(t, err)

	_, err = fh.PutRecordBatch(t.Context(), &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("mt-fh"),
		Records:            []fhtypes.Record{{Data: []byte("ab")}, {Data: []byte("cde")}},
	})
	require.NoError(t, err)

	d := map[string]string{"DeliveryStreamName": "mt-fh"}
	const ns = "AWS/Firehose"

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "IncomingRecords", dims: d, unit: cwtypes.StandardUnitCount, sum: 3},
		{namespace: ns, name: "IncomingBytes", dims: d, unit: cwtypes.StandardUnitBytes, sum: 9},
	})
}

func TestServiceMetrics_S3(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })

	_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("mt-bucket")})
	require.NoError(t, err)

	_, err = s3c.PutBucketMetricsConfiguration(t.Context(), &s3.PutBucketMetricsConfigurationInput{
		Bucket: aws.String("mt-bucket"), Id: aws.String("EntireBucket"),
		MetricsConfiguration: &s3types.MetricsConfiguration{Id: aws.String("EntireBucket")},
	})
	require.NoError(t, err)

	_, err = s3c.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("mt-bucket"), Key: aws.String("k"), Body: strings.NewReader("12345"),
	})
	require.NoError(t, err)

	got, err := s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("mt-bucket"), Key: aws.String("k")})
	require.NoError(t, err)
	require.NoError(t, got.Body.Close())

	_, err = s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String("mt-bucket")})
	require.NoError(t, err)

	_, err = s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("mt-bucket"), Key: aws.String("absent")})
	require.Error(t, err)

	h, ok := serviceByName(fx.services)["S3"].(*s3backend.S3Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok)

	bk.EmitStorageMetrics()

	req := map[string]string{"BucketName": "mt-bucket", "FilterId": "EntireBucket"}
	const ns = "AWS/S3"

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "AllRequests", dims: req, unit: cwtypes.StandardUnitCount, sum: 5, minSum: true},
		{namespace: ns, name: "PutRequests", dims: req, unit: cwtypes.StandardUnitCount, sum: 2, minSum: true},
		{namespace: ns, name: "GetRequests", dims: req, unit: cwtypes.StandardUnitCount, sum: 2},
		{namespace: ns, name: "ListRequests", dims: req, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "4xxErrors", dims: req, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "BytesDownloaded", dims: req, unit: cwtypes.StandardUnitBytes, sum: 5, minSum: true},
		{namespace: ns, name: "BytesUploaded", dims: req, unit: cwtypes.StandardUnitBytes, sum: 5, minSum: true},
		{namespace: ns, name: "TotalRequestLatency", dims: req, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{
			namespace: ns, name: "BucketSizeBytes",
			dims: map[string]string{"BucketName": "mt-bucket", "StorageType": "StandardStorage"},
			unit: cwtypes.StandardUnitBytes, sum: 5,
		},
		{
			namespace: ns, name: "NumberOfObjects",
			dims: map[string]string{"BucketName": "mt-bucket", "StorageType": "AllStorageTypes"},
			unit: cwtypes.StandardUnitCount, sum: 1,
		},
	})
}

func TestServiceMetrics_EC2(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ec2c := ec2.NewFromConfig(fx.cfg)

	run, err := ec2c.RunInstances(t.Context(), &ec2.RunInstancesInput{
		ImageId: aws.String("ami-12345678"), InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)

	id := aws.ToString(run.Instances[0].InstanceId)

	require.Eventually(t, func() bool {
		out, derr := ec2c.DescribeInstances(t.Context(), &ec2.DescribeInstancesInput{InstanceIds: []string{id}})

		return derr == nil && len(out.Reservations) == 1 &&
			out.Reservations[0].Instances[0].State.Name == ec2types.InstanceStateNameRunning
	}, metricWaitFor, 10*time.Millisecond)

	h, ok := serviceByName(fx.services)["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	bk.EmitStatusCheckMetrics()

	d := map[string]string{"InstanceId": id}

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: "AWS/EC2", name: "StatusCheckFailed", dims: d, unit: cwtypes.StandardUnitCount, sum: 0},
		{namespace: "AWS/EC2", name: "StatusCheckFailed_Instance", dims: d, unit: cwtypes.StandardUnitCount, sum: 0},
		{namespace: "AWS/EC2", name: "StatusCheckFailed_System", dims: d, unit: cwtypes.StandardUnitCount, sum: 0},
	})
}

func TestServiceMetrics_APIGatewayREST(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ag := apigateway.NewFromConfig(fx.cfg)

	api, err := ag.CreateRestApi(t.Context(), &apigateway.CreateRestApiInput{Name: aws.String("mt-api")})
	require.NoError(t, err)

	res, err := ag.CreateResource(t.Context(), &apigateway.CreateResourceInput{
		RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("pets"),
	})
	require.NoError(t, err)

	method := func(keyRequired bool, resourceID *string) {
		_, merr := ag.PutMethod(t.Context(), &apigateway.PutMethodInput{
			RestApiId: api.Id, ResourceId: resourceID, HttpMethod: aws.String("GET"),
			AuthorizationType: aws.String("NONE"), ApiKeyRequired: keyRequired,
		})
		require.NoError(t, merr)

		_, merr = ag.PutIntegration(t.Context(), &apigateway.PutIntegrationInput{
			RestApiId: api.Id, ResourceId: resourceID, HttpMethod: aws.String("GET"),
			Type: apigwtypes.IntegrationTypeMock,
		})
		require.NoError(t, merr)
	}

	secure, err := ag.CreateResource(t.Context(), &apigateway.CreateResourceInput{
		RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("secure"),
	})
	require.NoError(t, err)

	method(false, res.Id)
	method(true, secure.Id)

	dep, err := ag.CreateDeployment(t.Context(), &apigateway.CreateDeploymentInput{
		RestApiId: api.Id, StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	require.NotNil(t, dep.Id)

	_, err = ag.UpdateStage(t.Context(), &apigateway.UpdateStageInput{
		RestApiId: api.Id, StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{{
			Op: apigwtypes.OpReplace, Path: aws.String("/*/*/metrics/enabled"), Value: aws.String("true"),
		}},
	})
	require.NoError(t, err)

	base := "/restapis/" + aws.ToString(api.Id) + "/prod/_user_request_"
	require.Equal(t, http.StatusOK, dataPlane(t, fx, base+"/pets"))
	require.Equal(t, http.StatusForbidden, dataPlane(t, fx, base+"/secure"))

	const ns = "AWS/ApiGateway"

	api1 := map[string]string{"ApiName": "mt-api"}
	stage := map[string]string{"ApiName": "mt-api", "Stage": "prod"}
	detail := map[string]string{"ApiName": "mt-api", "Stage": "prod", "Method": "GET", "Resource": "/pets"}

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "Count", dims: api1, unit: cwtypes.StandardUnitCount, sum: 2},
		{namespace: ns, name: "Count", dims: stage, unit: cwtypes.StandardUnitCount, sum: 2},
		{namespace: ns, name: "4XXError", dims: stage, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "5XXError", dims: stage, unit: cwtypes.StandardUnitCount, sum: 0},
		{namespace: ns, name: "Latency", dims: stage, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "IntegrationLatency", dims: stage, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "Count", dims: detail, unit: cwtypes.StandardUnitCount, sum: 1},
	})
}

func TestServiceMetrics_APIGatewayV2(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ag := apigatewayv2.NewFromConfig(fx.cfg)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	httpAPI, err := ag.CreateApi(t.Context(), &apigatewayv2.CreateApiInput{
		Name: aws.String("mt-http"), ProtocolType: apigwv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	route := func(key, path string) {
		integ, ierr := ag.CreateIntegration(t.Context(), &apigatewayv2.CreateIntegrationInput{
			ApiId: httpAPI.ApiId, IntegrationType: apigwv2types.IntegrationTypeHttpProxy,
			IntegrationMethod: aws.String("GET"), IntegrationUri: aws.String(upstream.URL + path),
			PayloadFormatVersion: aws.String("1.0"),
		})
		require.NoError(t, ierr)

		_, ierr = ag.CreateRoute(t.Context(), &apigatewayv2.CreateRouteInput{
			ApiId: httpAPI.ApiId, RouteKey: aws.String(key),
			Target: aws.String("integrations/" + aws.ToString(integ.IntegrationId)),
		})
		require.NoError(t, ierr)
	}

	route("GET /ok", "/ok")
	route("GET /boom", "/boom")

	_, err = ag.CreateStage(t.Context(), &apigatewayv2.CreateStageInput{
		ApiId: httpAPI.ApiId, StageName: aws.String("prod"), AutoDeploy: aws.Bool(true),
	})
	require.NoError(t, err)

	base := "/v2proxy/" + aws.ToString(httpAPI.ApiId) + "/prod"
	require.Equal(t, http.StatusOK, dataPlane(t, fx, base+"/ok"))
	require.Equal(t, http.StatusInternalServerError, dataPlane(t, fx, base+"/boom"))
	require.Equal(t, http.StatusNotFound, dataPlane(t, fx, base+"/missing"))

	wsID := websocketMetricsAPI(t, fx, ag)

	const ns = "AWS/ApiGateway"

	hs := map[string]string{"ApiId": aws.ToString(httpAPI.ApiId), "Stage": "prod"}
	ha := map[string]string{"ApiId": aws.ToString(httpAPI.ApiId)}
	ws := map[string]string{"ApiId": wsID, "Stage": "prod"}

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "Count", dims: hs, unit: cwtypes.StandardUnitCount, sum: 3},
		{namespace: ns, name: "Count", dims: ha, unit: cwtypes.StandardUnitCount, sum: 3},
		{namespace: ns, name: "4xx", dims: hs, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "5xx", dims: hs, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "Latency", dims: hs, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "IntegrationLatency", dims: hs, unit: cwtypes.StandardUnitMilliseconds, sum: -1},
		{namespace: ns, name: "ConnectCount", dims: ws, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "MessageCount", dims: ws, unit: cwtypes.StandardUnitCount, sum: 2},
		{namespace: ns, name: "ExecutionError", dims: ws, unit: cwtypes.StandardUnitCount, sum: 1},
	})
}

// websocketMetricsAPI creates a WebSocket API with a mock $default route, drives one connection that
// sends a routable and an unroutable message, and returns the API id.
func websocketMetricsAPI(t *testing.T, fx *sfnFixture, ag *apigatewayv2.Client) string {
	t.Helper()

	api, err := ag.CreateApi(t.Context(), &apigatewayv2.CreateApiInput{
		Name: aws.String("mt-ws"), ProtocolType: apigwv2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action"),
	})
	require.NoError(t, err)

	integ, err := ag.CreateIntegration(t.Context(), &apigatewayv2.CreateIntegrationInput{
		ApiId: api.ApiId, IntegrationType: apigwv2types.IntegrationTypeMock,
	})
	require.NoError(t, err)

	_, err = ag.CreateRoute(t.Context(), &apigatewayv2.CreateRouteInput{
		ApiId: api.ApiId, RouteKey: aws.String("$default"),
		Target: aws.String("integrations/" + aws.ToString(integ.IntegrationId)),
	})
	require.NoError(t, err)

	_, err = ag.CreateStage(t.Context(), &apigatewayv2.CreateStageInput{
		ApiId: api.ApiId, StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	url := "ws" + strings.TrimPrefix(aws.ToString(fx.cfg.BaseEndpoint), "http") +
		"/v2proxy/" + aws.ToString(api.ApiId) + "/prod"

	conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), url, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":"world"}`)))
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"nope"}`)))
	require.NoError(t, conn.Close())

	return aws.ToString(api.ApiId)
}

func TestServiceMetrics_EventBridge(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	authzStartWorkers(t, fx, "EventBridge")

	eb := eventbridge.NewFromConfig(fx.cfg)
	sqsc := sqs.NewFromConfig(fx.cfg)

	queue := func(name string) string {
		q, err := sqsc.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
		require.NoError(t, err)

		qa, err := sqsc.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		require.NoError(t, err)

		return qa.Attributes["QueueArn"]
	}

	rule := func(name, bus string, target ebtypes.Target) {
		_, err := eb.PutRule(t.Context(), &eventbridge.PutRuleInput{
			Name: aws.String(name), EventBusName: aws.String(bus), EventPattern: aws.String(`{"source":["mt.test"]}`),
		})
		require.NoError(t, err)

		_, err = eb.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
			Rule: aws.String(name), EventBusName: aws.String(bus), Targets: []ebtypes.Target{target},
		})
		require.NoError(t, err)
	}

	_, err := eb.CreateEventBus(t.Context(), &eventbridge.CreateEventBusInput{Name: aws.String("mt-bus")})
	require.NoError(t, err)

	rule("mt-default", "default", ebtypes.Target{Id: aws.String("t"), Arn: aws.String(queue("mt-q1"))})
	rule("mt-custom", "mt-bus", ebtypes.Target{Id: aws.String("t"), Arn: aws.String(queue("mt-q2"))})
	rule("mt-dead", "default", ebtypes.Target{
		Id: aws.String("t"), Arn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:absent"),
		RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
		DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(queue("mt-dlq"))},
	})

	for _, bus := range []string{"default", "mt-bus"} {
		_, err = eb.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
			Source: aws.String(
				"mt.test",
			), DetailType: aws.String("t"), Detail: aws.String(`{}`), EventBusName: aws.String(bus),
		}}})
		require.NoError(t, err)
	}

	const ns = "AWS/Events"

	def := map[string]string{"RuleName": "mt-default"}
	custom := map[string]string{"RuleName": "mt-custom", "EventBusName": "mt-bus"}
	dead := map[string]string{"RuleName": "mt-dead"}

	assertMetricsEmitted(t, fx, []metricWant{
		{namespace: ns, name: "MatchedEvents", dims: def, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "TriggeredRules", dims: def, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "Invocations", dims: def, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "Invocations", dims: custom, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "MatchedEvents", dims: custom, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "FailedInvocations", dims: dead, unit: cwtypes.StandardUnitCount, sum: 1},
		{namespace: ns, name: "DeadLetterInvocations", dims: dead, unit: cwtypes.StandardUnitCount, sum: 1},
	})
}
