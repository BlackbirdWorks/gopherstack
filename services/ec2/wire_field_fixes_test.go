package ec2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newTestEC2Client stands up the real aws-sdk-go-v2 EC2 client (ec2@v1.320.0)
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestEC2Client(t *testing.T, h *ec2.Handler) *ec2sdk.Client {
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

	return ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateNetworkInsightsPath_RealWireKeys drives CreateNetworkInsightsPath
// through the real SDK client. The request's required fields serialize as
// "Source"/"Destination" (serializers.go:71549, api_op_CreateNetworkInsightsPath.go
// Source/Destination), but the handler read "SourceId"/"DestinationId" -- names
// that don't exist on the wire at all, so a real client's Source was always
// empty and the backend's own required-field check rejected every real call.
func TestCreateNetworkInsightsPath_RealWireKeys(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateNetworkInsightsPath(t.Context(), &ec2sdk.CreateNetworkInsightsPathInput{
		Source:      aws.String("eni-0123456789abcdef0"),
		Destination: aws.String("eni-0fedcba9876543210"),
		Protocol:    types.ProtocolTcp,
	})
	require.NoError(t, err)
	assert.Equal(t, "eni-0123456789abcdef0", aws.ToString(out.NetworkInsightsPath.Source))
	assert.Equal(t, "eni-0fedcba9876543210", aws.ToString(out.NetworkInsightsPath.Destination))
}

// TestCreateRouteServer_RealWireKeys drives CreateRouteServer through the
// real SDK client. The real request field is "PersistRoutes"
// (serializers.go:72100), but the handler read "PersistRoutesState" -- the
// name of a different, response-only field (RouteServer.PersistRoutesState,
// botocore ec2 2016-11-15 service-2.json) -- so a client's requested
// PersistRoutes action was always discarded.
func TestCreateRouteServer_RealWireKeys(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateRouteServer(t.Context(), &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(4200000000),
		PersistRoutes: types.RouteServerPersistRoutesActionEnable,
	})
	require.NoError(t, err)
	assert.Equal(t,
		string(types.RouteServerPersistRoutesActionEnable),
		string(out.RouteServer.PersistRoutesState),
	)
}

// TestCreateInstanceExportTask_RealWireKeys drives CreateInstanceExportTask
// through the real SDK client. The nested settings serialize under the
// parent key "ExportToS3" (serializers.go:70438, matching response field
// ExportTask.ExportToS3Task -> wire "exportToS3"), but the handler read
// "ExportToS3Task.*" -- a prefix that doesn't exist on the wire -- so a real
// client's format/bucket/prefix were always discarded.
func TestCreateInstanceExportTask_RealWireKeys(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	out, err := client.CreateInstanceExportTask(t.Context(), &ec2sdk.CreateInstanceExportTaskInput{
		InstanceId:        aws.String(instanceID),
		TargetEnvironment: types.ExportEnvironmentVmware,
		ExportToS3Task: &types.ExportToS3TaskSpecification{
			ContainerFormat: types.ContainerFormatOva,
			DiskImageFormat: types.DiskImageFormatVmdk,
			S3Bucket:        aws.String("my-export-bucket"),
			S3Prefix:        aws.String("exports/"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ExportTask.ExportToS3Task)
	assert.Equal(t, "my-export-bucket", aws.ToString(out.ExportTask.ExportToS3Task.S3Bucket))
	assert.Equal(t, types.ContainerFormatOva, out.ExportTask.ExportToS3Task.ContainerFormat)
}
