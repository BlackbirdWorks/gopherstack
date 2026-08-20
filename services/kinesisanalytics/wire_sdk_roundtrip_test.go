package kinesisanalytics_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kasdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	katypes "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// newTestKASDKClient stands up the real aws-sdk-go-v2 kinesisanalytics client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- so a shape is
// verified by the real client's own deserializer, not gopherstack's own JSON
// tags.
func newTestKASDKClient(t *testing.T, h *kinesisanalytics.Handler) *kasdk.Client {
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

	return kasdk.NewFromConfig(cfg, func(o *kasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDescribeApplication_InputLambdaProcessorDescription_SDKRoundTrip proves
// that an input's processing-configuration description round-trips through
// the real SDK client. types.InputProcessingConfigurationDescription's own
// member is InputLambdaProcessorDescription (aws-sdk-go-v2/service/
// kinesisanalytics/types/types.go), a distinct wire key from the
// request-side InputProcessingConfiguration.InputLambdaProcessor -- reusing
// the request key on the description leaves the real client's deserializer
// (deserializers.go's awsAwsjson11_deserializeDocumentInputProcessingConfigurationDescription)
// unable to find the field, silently dropping the Lambda preprocessor ARN.
func TestDescribeApplication_InputLambdaProcessorDescription_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesisanalytics.NewInMemoryBackend("us-east-1", "000000000000")
	h := kinesisanalytics.NewHandler(backend)
	h.AccountID = "000000000000"
	h.DefaultRegion = "us-east-1"

	client := newTestKASDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kasdk.CreateApplicationInput{
		ApplicationName: aws.String("proc-roundtrip-app"),
	})
	require.NoError(t, err)

	_, err = client.AddApplicationInput(ctx, &kasdk.AddApplicationInputInput{
		ApplicationName:             aws.String("proc-roundtrip-app"),
		CurrentApplicationVersionId: aws.Int64(1),
		Input: &katypes.Input{
			NamePrefix: aws.String("SOURCE_SQL_STREAM"),
			KinesisStreamsInput: &katypes.KinesisStreamsInput{
				ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/test"),
				RoleARN:     aws.String("arn:aws:iam::000000000000:role/role"),
			},
			InputSchema: &katypes.SourceSchema{
				RecordFormat: &katypes.RecordFormat{RecordFormatType: katypes.RecordFormatTypeJson},
				RecordColumns: []katypes.RecordColumn{
					{Name: aws.String("COL1"), SqlType: aws.String("VARCHAR(4)")},
				},
			},
			InputProcessingConfiguration: &katypes.InputProcessingConfiguration{
				InputLambdaProcessor: &katypes.InputLambdaProcessor{
					ResourceARN: aws.String("arn:aws:lambda:us-east-1:000000000000:function:fn"),
					RoleARN:     aws.String("arn:aws:iam::000000000000:role/lambda-role"),
				},
			},
		},
	})
	require.NoError(t, err)

	describeOut, err := client.DescribeApplication(ctx, &kasdk.DescribeApplicationInput{
		ApplicationName: aws.String("proc-roundtrip-app"),
	})
	require.NoError(t, err)
	require.Len(t, describeOut.ApplicationDetail.InputDescriptions, 1)

	inputDesc := describeOut.ApplicationDetail.InputDescriptions[0]
	require.NotNil(t, inputDesc.InputProcessingConfigurationDescription)
	require.NotNil(t, inputDesc.InputProcessingConfigurationDescription.InputLambdaProcessorDescription)
	require.Equal(
		t,
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		aws.ToString(inputDesc.InputProcessingConfigurationDescription.InputLambdaProcessorDescription.ResourceARN),
	)
	require.Equal(
		t,
		"arn:aws:iam::000000000000:role/lambda-role",
		aws.ToString(inputDesc.InputProcessingConfigurationDescription.InputLambdaProcessorDescription.RoleARN),
	)
}
