package codedeploy_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

const rtTestRegion = "us-east-1"

// newTestCodeDeployClient stands up the real aws-sdk-go-v2 CodeDeploy client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer (rather than decoding the
// raw JSON body with ad-hoc structs, as most other tests in this package do)
// is what actually proves a response is wire-compatible: the awsjson1.1
// deserializer parses Timestamp shapes with smithytime.ParseEpochSeconds,
// which reads the wire value as seconds -- a handler emitting
// UnixMilli()-scaled numbers would silently decode as a wildly wrong
// timestamp (off by 1000x) rather than failing outright, so only a real
// client round-trip catches it.
func newTestCodeDeployClient(t *testing.T, h *codedeploy.Handler) *codedeploysdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return codedeploysdk.NewFromConfig(cfg, func(o *codedeploysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_CreateTime_EpochSeconds proves that Application, Deployment,
// DeploymentConfig, and OnPremisesInstance timestamps decode to a sane, current
// time through the real SDK client. Before the fix, the handler serialized
// these as UnixMilli() (an int64 ~1000x larger than the epoch-seconds value
// the wire format expects), which the real deserializer's
// smithytime.ParseEpochSeconds would interpret as a time far in the future.
func Test_SDKRoundTrip_CreateTime_EpochSeconds(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	before := time.Now().Add(-time.Minute)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("rt-epoch-app"),
		ComputePlatform: types.ComputePlatformServer,
	})
	require.NoError(t, err)

	appOut, err := client.GetApplication(t.Context(), &codedeploysdk.GetApplicationInput{
		ApplicationName: aws.String("rt-epoch-app"),
	})
	require.NoError(t, err)
	require.NotNil(t, appOut.Application.CreateTime)
	assertRecentTime(t, *appOut.Application.CreateTime, before, "Application.CreateTime")

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("rt-epoch-app"),
		DeploymentGroupName: aws.String("rt-epoch-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	deployOut, err := client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("rt-epoch-app"),
		DeploymentGroupName: aws.String("rt-epoch-dg"),
	})
	require.NoError(t, err)

	getOut, err := client.GetDeployment(t.Context(), &codedeploysdk.GetDeploymentInput{
		DeploymentId: deployOut.DeploymentId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DeploymentInfo.CreateTime)
	assertRecentTime(t, *getOut.DeploymentInfo.CreateTime, before, "DeploymentInfo.CreateTime")
	require.NotNil(t, getOut.DeploymentInfo.CompleteTime)
	assertRecentTime(t, *getOut.DeploymentInfo.CompleteTime, before, "DeploymentInfo.CompleteTime")

	cfgOut, err := client.GetDeploymentConfig(t.Context(), &codedeploysdk.GetDeploymentConfigInput{
		DeploymentConfigName: aws.String("CodeDeployDefault.AllAtOnce"),
	})
	require.NoError(t, err)
	require.NotNil(t, cfgOut.DeploymentConfigInfo.CreateTime)
	assertRecentTime(t, *cfgOut.DeploymentConfigInfo.CreateTime, before, "DeploymentConfigInfo.CreateTime")

	_, err = client.RegisterOnPremisesInstance(t.Context(), &codedeploysdk.RegisterOnPremisesInstanceInput{
		InstanceName: aws.String("rt-epoch-instance"),
		IamUserArn:   aws.String("arn:aws:iam::000000000000:user/instance"),
	})
	require.NoError(t, err)

	instOut, err := client.GetOnPremisesInstance(t.Context(), &codedeploysdk.GetOnPremisesInstanceInput{
		InstanceName: aws.String("rt-epoch-instance"),
	})
	require.NoError(t, err)
	require.NotNil(t, instOut.InstanceInfo.RegisterTime)
	assertRecentTime(t, *instOut.InstanceInfo.RegisterTime, before, "OnPremisesInstanceInfo.RegisterTime")
}

// assertRecentTime fails the test unless got falls within a sane window of
// "now" -- specifically after lowerBound and no more than an hour in the
// future. A UnixMilli-as-seconds wire bug decodes to a timestamp tens of
// thousands of years away, which this window rejects outright.
func assertRecentTime(t *testing.T, got, lowerBound time.Time, field string) {
	t.Helper()

	assert.Truef(t, got.After(lowerBound), "%s = %v, want after %v", field, got, lowerBound)
	assert.Truef(t, got.Before(time.Now().Add(time.Hour)), "%s = %v, want within an hour of now", field, got)
}

// Test_SDKRoundTrip_StopDeployment_StatusEnum proves StopDeploymentOutput.Status
// decodes as the real StopStatus enum ("Succeeded"), and that the deployment's
// own resulting status ("Stopped") is a separate field on GetDeployment. Before
// the fix, the handler emitted the deployment's status ("Stopped") for the stop
// operation's status too, which is not a valid StopStatus value.
func Test_SDKRoundTrip_StopDeployment_StatusEnum(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("rt-stop-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("rt-stop-app"),
		DeploymentGroupName: aws.String("rt-stop-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	deployOut, err := client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("rt-stop-app"),
		DeploymentGroupName: aws.String("rt-stop-dg"),
	})
	require.NoError(t, err)

	stopOut, err := client.StopDeployment(t.Context(), &codedeploysdk.StopDeploymentInput{
		DeploymentId: deployOut.DeploymentId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.StopStatusSucceeded, stopOut.Status)

	getOut, err := client.GetDeployment(t.Context(), &codedeploysdk.GetDeploymentInput{
		DeploymentId: deployOut.DeploymentId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.DeploymentStatusStopped, getOut.DeploymentInfo.Status)
}

// Test_SDKRoundTrip_ListDeployments_CreateTimeRange proves the request-side
// createTimeRange filter is parsed as epoch seconds, matching how the real
// client's serializer (smithytime.FormatEpochSeconds) encodes it. Before the
// fix, the handler parsed the wire value with time.UnixMilli, which collapses
// any real epoch-seconds value into a date in January 1970 -- far earlier than
// any deployment's CreateTime, so a "start" filter set in the future would
// never actually exclude anything.
func Test_SDKRoundTrip_ListDeployments_CreateTimeRange(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("rt-range-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("rt-range-app"),
		DeploymentGroupName: aws.String("rt-range-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("rt-range-app"),
		DeploymentGroupName: aws.String("rt-range-dg"),
	})
	require.NoError(t, err)

	// A start bound one hour in the future must exclude the deployment just
	// created "now".
	future := time.Now().Add(time.Hour)
	listOut, err := client.ListDeployments(t.Context(), &codedeploysdk.ListDeploymentsInput{
		ApplicationName:     aws.String("rt-range-app"),
		DeploymentGroupName: aws.String("rt-range-dg"),
		CreateTimeRange: &types.TimeRange{
			Start: &future,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.Deployments,
		"a createTimeRange.start an hour in the future must exclude a deployment created now")

	// A start bound one hour in the past must include it.
	past := time.Now().Add(-time.Hour)
	listOut, err = client.ListDeployments(t.Context(), &codedeploysdk.ListDeploymentsInput{
		ApplicationName:     aws.String("rt-range-app"),
		DeploymentGroupName: aws.String("rt-range-dg"),
		CreateTimeRange: &types.TimeRange{
			Start: &past,
		},
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Deployments, 1)
}
