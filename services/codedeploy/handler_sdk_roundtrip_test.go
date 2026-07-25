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

// Test_SDKRoundTrip_ApplicationRevision_EpochSeconds proves the newly-added
// ApplicationRevision family's GenericRevisionInfo timestamps (registerTime,
// firstUsedTime, lastUsedTime) decode correctly as epoch-seconds through the
// real SDK client -- the same 1000x UnixMilli-vs-epoch-seconds bug class
// documented at the top of this file for every other Timestamp shape in this
// service. It also proves RegisterApplicationRevision/GetApplicationRevision
// round-trip a real revision (not an echo of the request) and that
// CreateDeployment auto-registers and touches a revision's usage timestamps.
func Test_SDKRoundTrip_ApplicationRevision_EpochSeconds(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	before := time.Now().Add(-time.Minute)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("rt-revision-app"),
		ComputePlatform: types.ComputePlatformServer,
	})
	require.NoError(t, err)

	revision := &types.RevisionLocation{
		RevisionType: types.RevisionLocationTypeS3,
		S3Location: &types.S3Location{
			Bucket:     aws.String("rt-bucket"),
			Key:        aws.String("rt-key"),
			BundleType: types.BundleTypeZip,
		},
	}

	_, err = client.RegisterApplicationRevision(t.Context(), &codedeploysdk.RegisterApplicationRevisionInput{
		ApplicationName: aws.String("rt-revision-app"),
		Revision:        revision,
		Description:     aws.String("rt description"),
	})
	require.NoError(t, err)

	getOut, err := client.GetApplicationRevision(t.Context(), &codedeploysdk.GetApplicationRevisionInput{
		ApplicationName: aws.String("rt-revision-app"),
		Revision:        revision,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RevisionInfo)
	assert.Equal(t, "rt description", aws.ToString(getOut.RevisionInfo.Description))
	require.NotNil(t, getOut.RevisionInfo.RegisterTime)
	assertRecentTime(t, *getOut.RevisionInfo.RegisterTime, before, "RevisionInfo.RegisterTime")
	assert.Nil(t, getOut.RevisionInfo.FirstUsedTime, "never deployed, so FirstUsedTime must stay unset")

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("rt-revision-app"),
		DeploymentGroupName: aws.String("rt-revision-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("rt-revision-app"),
		DeploymentGroupName: aws.String("rt-revision-dg"),
		Revision:            revision,
	})
	require.NoError(t, err)

	getOut, err = client.GetApplicationRevision(t.Context(), &codedeploysdk.GetApplicationRevisionInput{
		ApplicationName: aws.String("rt-revision-app"),
		Revision:        revision,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RevisionInfo.FirstUsedTime)
	assertRecentTime(t, *getOut.RevisionInfo.FirstUsedTime, before, "RevisionInfo.FirstUsedTime")
	require.NotNil(t, getOut.RevisionInfo.LastUsedTime)
	assertRecentTime(t, *getOut.RevisionInfo.LastUsedTime, before, "RevisionInfo.LastUsedTime")
	assert.Contains(t, getOut.RevisionInfo.DeploymentGroups, "rt-revision-dg")
}

// Test_SDKRoundTrip_DeploymentTarget_EpochSeconds proves GetDeploymentTarget's
// computed instanceTarget.lastUpdatedAt decodes correctly as epoch-seconds
// through the real SDK client, and that the target is resolved from a real
// registered on-premises instance matched by the deployment group's tag
// filters rather than fabricated for an arbitrary requested ID.
func Test_SDKRoundTrip_DeploymentTarget_EpochSeconds(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	before := time.Now().Add(-time.Minute)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("rt-target-app"),
		ComputePlatform: types.ComputePlatformServer,
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("rt-target-app"),
		DeploymentGroupName: aws.String("rt-target-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
		OnPremisesInstanceTagFilters: []types.TagFilter{
			{Key: aws.String("env"), Value: aws.String("prod"), Type: types.TagFilterTypeKeyAndValue},
		},
	})
	require.NoError(t, err)

	_, err = client.RegisterOnPremisesInstance(t.Context(), &codedeploysdk.RegisterOnPremisesInstanceInput{
		InstanceName: aws.String("rt-target-instance"),
		IamUserArn:   aws.String("arn:aws:iam::000000000000:user/instance"),
	})
	require.NoError(t, err)

	_, err = client.AddTagsToOnPremisesInstances(t.Context(), &codedeploysdk.AddTagsToOnPremisesInstancesInput{
		InstanceNames: []string{"rt-target-instance"},
		Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	deployOut, err := client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("rt-target-app"),
		DeploymentGroupName: aws.String("rt-target-dg"),
	})
	require.NoError(t, err)

	targetOut, err := client.GetDeploymentTarget(t.Context(), &codedeploysdk.GetDeploymentTargetInput{
		DeploymentId: deployOut.DeploymentId,
		TargetId:     aws.String("rt-target-instance"),
	})
	require.NoError(t, err)
	require.NotNil(t, targetOut.DeploymentTarget)
	require.NotNil(t, targetOut.DeploymentTarget.InstanceTarget)
	assert.Equal(t, "rt-target-instance", aws.ToString(targetOut.DeploymentTarget.InstanceTarget.TargetId))
	assert.Equal(t, types.TargetStatusSucceeded, targetOut.DeploymentTarget.InstanceTarget.Status)
	require.NotNil(t, targetOut.DeploymentTarget.InstanceTarget.LastUpdatedAt)
	assertRecentTime(
		t, *targetOut.DeploymentTarget.InstanceTarget.LastUpdatedAt, before, "InstanceTarget.LastUpdatedAt",
	)
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
