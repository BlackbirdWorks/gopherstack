package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	kinesisvideobackend "github.com/blackbirdworks/gopherstack/services/kinesisvideo"
)

// newKinesisVideoTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with KinesisVideo (among the other backends
// newMoreTypesServiceBackends already wires) set to a real in-memory service
// backend.
func newKinesisVideoTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.KinesisVideo = kinesisvideobackend.NewHandler(kinesisvideobackend.NewInMemoryBackend())

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_KinesisVideoTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testKinesisVideoStream, "stream"},
		{testKinesisVideoSignalingChannel, "signaling_channel"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testKinesisVideoStream(t *testing.T) {
	t.Helper()

	backends, client := newKinesisVideoTestClient(t)

	tmpl := `{
"Resources": {"Stream": {"Type": "AWS::KinesisVideo::Stream", "Properties": {
  "Name": "test-stream",
  "DataRetentionInHours": 24,
  "MediaType": "video/h264"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Stream"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Stream", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kvs-stream-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Arn"], "test-stream")

	s, err := backends.KinesisVideo.Backend.DescribeStream("test-stream", "")
	require.NoError(t, err)
	assert.Equal(t, int32(24), s.DataRetentionInHours)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kvs-stream-stack")})
	require.NoError(t, err)

	_, err = backends.KinesisVideo.Backend.DescribeStream("test-stream", "")
	require.Error(t, err)
}

func testKinesisVideoSignalingChannel(t *testing.T) {
	t.Helper()

	backends, client := newKinesisVideoTestClient(t)

	tmpl := `{
"Resources": {"Channel": {"Type": "AWS::KinesisVideo::SignalingChannel", "Properties": {
  "Name": "test-channel",
  "Type": "SINGLE_MASTER"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Channel"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Channel", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kvs-channel-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Arn"], "test-channel")

	_, err := backends.KinesisVideo.Backend.DescribeSignalingChannel("test-channel", "")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kvs-channel-stack")})
	require.NoError(t, err)

	_, err = backends.KinesisVideo.Backend.DescribeSignalingChannel("test-channel", "")
	require.Error(t, err)
}
