package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func newKinesisMoreTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newExtendedServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_KinesisStreamConsumer(t *testing.T) {
	t.Parallel()

	_, client := newKinesisMoreTestClient(t)

	tmpl := `{
"Resources": {
  "Stream": {"Type": "AWS::Kinesis::Stream", "Properties": {"Name": "my-stream", "ShardCount": 1}},
  "Consumer": {
    "Type": "AWS::Kinesis::StreamConsumer",
    "Properties": {
      "StreamARN": {"Ref": "Stream"},
      "ConsumerName": "my-consumer"
    }
  }
},
"Outputs": {
  "StreamRef": {"Value": {"Ref": "Stream"}},
  "Ref": {"Value": {"Ref": "Consumer"}},
  "ConsumerArn": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerARN"]}},
  "ConsumerName": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerName"]}},
  "StreamArn": {"Value": {"Fn::GetAtt": ["Consumer", "StreamARN"]}},
  "ConsumerStatus": {"Value": {"Fn::GetAtt": ["Consumer", "ConsumerStatus"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kinesis-consumer-stack", tmpl)

	assert.Equal(t, outputs["Ref"], outputs["ConsumerArn"])
	assert.Contains(t, outputs["Ref"], "stream/my-stream/consumer/my-consumer:")
	assert.Equal(t, "my-consumer", outputs["ConsumerName"])
	assert.Equal(t, outputs["StreamRef"], outputs["StreamArn"])
	assert.Equal(t, "ACTIVE", outputs["ConsumerStatus"])

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kinesis-consumer-stack")})
	require.NoError(t, err)
}

func TestCreateStack_KinesisFirehoseDeliveryStream_RealTypeName(t *testing.T) {
	t.Parallel()

	backends := newAdditionalServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	tmpl := `{
"Resources": {
  "Stream": {
    "Type": "AWS::KinesisFirehose::DeliveryStream",
    "Properties": {"DeliveryStreamName": "my-delivery-stream"}
  }
},
"Outputs": {"Name": {"Value": {"Ref": "Stream"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "firehose-real-name-stack", tmpl)
	assert.Equal(t, "my-delivery-stream", outputs["Name"])

	_, err := backends.Firehose.Backend.DescribeDeliveryStream(t.Context(), "my-delivery-stream")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("firehose-real-name-stack")},
	)
	require.NoError(t, err)

	_, err = backends.Firehose.Backend.DescribeDeliveryStream(t.Context(), "my-delivery-stream")
	require.Error(t, err)
}
