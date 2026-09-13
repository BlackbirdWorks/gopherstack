package awsconfig_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// recorderStatusCaptureTransport records the raw bytes of the last HTTP
// response, then replays them so the real SDK deserializer still sees the
// full body.
type recorderStatusCaptureTransport struct {
	body []byte
}

func (c *recorderStatusCaptureTransport) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	resp.Body.Close()

	c.body = b
	resp.Body = io.NopCloser(bytes.NewReader(b))

	return resp, nil
}

// newCapturingWireTestAWSConfigClient is newTestAWSConfigSDKClient plus a
// transport that stashes each response's raw bytes on capture.body, so a
// test can assert on the wire JSON while also proving the real client
// decodes it.
func newCapturingWireTestAWSConfigClient(
	t *testing.T, h *awsconfig.Handler,
) (*configservicesdk.Client, *recorderStatusCaptureTransport) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	capture := &recorderStatusCaptureTransport{}

	client := configservicesdk.NewFromConfig(cfg, func(o *configservicesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = capture
	})

	return client, capture
}

// TestConfigurationRecorderWire_StatusStripped covers every wire path that
// marshals ConfigurationRecorder directly (gopherstack-nginj): real
// types.ConfigurationRecorder (configservice v1.68.4 types/types.go:
// 1024-1146) has no status member, so the raw body must not carry the key
// even though the recorder has a non-empty Status persisted. Each case also
// decodes the response through the real typed client to prove the strip
// didn't break the shape.
func TestConfigurationRecorderWire_StatusStripped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *configservicesdk.Client)
		name string
	}{
		{
			name: "describe_configuration_recorders",
			run: func(t *testing.T, client *configservicesdk.Client) {
				t.Helper()

				out, err := client.DescribeConfigurationRecorders(
					t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{},
				)
				require.NoError(t, err)
				require.Len(t, out.ConfigurationRecorders, 1)
				assert.Equal(t, "default", aws.ToString(out.ConfigurationRecorders[0].Name))
			},
		},
		{
			name: "associate_resource_types",
			run: func(t *testing.T, client *configservicesdk.Client) {
				t.Helper()

				describeOut, err := client.DescribeConfigurationRecorders(
					t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{},
				)
				require.NoError(t, err)
				require.Len(t, describeOut.ConfigurationRecorders, 1)
				arn := aws.ToString(describeOut.ConfigurationRecorders[0].Arn)

				out, err := client.AssociateResourceTypes(t.Context(), &configservicesdk.AssociateResourceTypesInput{
					ConfigurationRecorderArn: aws.String(arn),
					ResourceTypes:            []types.ResourceType{types.ResourceTypeInstance},
				})
				require.NoError(t, err)
				require.NotNil(t, out.ConfigurationRecorder)
				assert.Equal(t, "default", aws.ToString(out.ConfigurationRecorder.Name))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
			client, capture := newCapturingWireTestAWSConfigClient(t, h)

			_, err := client.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
				ConfigurationRecorder: &types.ConfigurationRecorder{
					Name:    aws.String("default"),
					RoleARN: aws.String("arn:aws:iam::123456789012:role/r"),
				},
			})
			require.NoError(t, err)

			_, err = client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
				DeliveryChannel: &types.DeliveryChannel{
					Name:         aws.String("default"),
					S3BucketName: aws.String("my-bucket"),
				},
			})
			require.NoError(t, err)

			_, err = client.StartConfigurationRecorder(
				t.Context(),
				&configservicesdk.StartConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")},
			)
			require.NoError(t, err)

			tt.run(t, client)

			require.NotEmpty(t, capture.body)
			assert.NotContains(t, string(capture.body), `"status"`,
				"response must not carry the persisted-only Status field on the wire")
		})
	}
}

// TestDescribeConfigurationRecorderStatus_StillReturnsStatus is the control:
// Status stays persisted on ConfigurationRecorder and DescribeConfigurationRecorderStatus
// -- the real, separate operation for recorder status
// (types.ConfigurationRecorderStatus, types/types.go:1178-1213) -- must
// still return it.
func TestDescribeConfigurationRecorderStatus_StillReturnsStatus(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client, capture := newCapturingWireTestAWSConfigClient(t, h)

	_, err := client.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
		ConfigurationRecorder: &types.ConfigurationRecorder{
			Name:    aws.String("default"),
			RoleARN: aws.String("arn:aws:iam::123456789012:role/r"),
		},
	})
	require.NoError(t, err)

	_, err = client.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{
			Name:         aws.String("default"),
			S3BucketName: aws.String("my-bucket"),
		},
	})
	require.NoError(t, err)

	_, err = client.StartConfigurationRecorder(
		t.Context(),
		&configservicesdk.StartConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")},
	)
	require.NoError(t, err)

	out, err := client.DescribeConfigurationRecorderStatus(
		t.Context(), &configservicesdk.DescribeConfigurationRecorderStatusInput{},
	)
	require.NoError(t, err)
	require.Len(t, out.ConfigurationRecordersStatus, 1)
	assert.True(t, out.ConfigurationRecordersStatus[0].Recording)
	assert.NotEmpty(t, out.ConfigurationRecordersStatus[0].LastStatus)

	require.NotEmpty(t, capture.body)
	assert.Contains(t, string(capture.body), `"lastStatus"`,
		"DescribeConfigurationRecorderStatus must still carry lastStatus on the wire")
}
