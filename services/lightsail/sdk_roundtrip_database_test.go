package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestRelationalDatabaseRoundTrip exercises family N+O+P end to end.
func TestRelationalDatabaseRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("db-1"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
	})
	require.NoError(t, err)

	getOut, err := client.GetRelationalDatabase(
		ctx,
		&lightsailsdk.GetRelationalDatabaseInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)
	require.Equal(t, "mysql", aws.ToString(getOut.RelationalDatabase.Engine))

	require.Eventually(t, func() bool {
		out, getErr := client.GetRelationalDatabase(
			ctx,
			&lightsailsdk.GetRelationalDatabaseInput{RelationalDatabaseName: aws.String("db-1")},
		)

		return getErr == nil && aws.ToString(out.RelationalDatabase.State) == "available"
	}, defaultAsyncWait, defaultAsyncPoll, "database never reached available")

	listOut, err := client.GetRelationalDatabases(ctx, &lightsailsdk.GetRelationalDatabasesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.RelationalDatabases, 1)

	paramsOut, err := client.GetRelationalDatabaseParameters(
		ctx,
		&lightsailsdk.GetRelationalDatabaseParametersInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, paramsOut.Parameters)

	_, err = client.UpdateRelationalDatabaseParameters(ctx, &lightsailsdk.UpdateRelationalDatabaseParametersInput{
		RelationalDatabaseName: aws.String("db-1"),
		Parameters: []lightsailtypes.RelationalDatabaseParameter{
			{ParameterName: aws.String("max_connections"), ParameterValue: aws.String("200")},
		},
	})
	require.NoError(t, err)

	streamsOut, err := client.GetRelationalDatabaseLogStreams(
		ctx,
		&lightsailsdk.GetRelationalDatabaseLogStreamsInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, streamsOut.LogStreams)

	pwOut, err := client.GetRelationalDatabaseMasterUserPassword(
		ctx,
		&lightsailsdk.GetRelationalDatabaseMasterUserPasswordInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(pwOut.MasterUserPassword))

	_, err = client.CreateRelationalDatabaseSnapshot(ctx, &lightsailsdk.CreateRelationalDatabaseSnapshotInput{
		RelationalDatabaseName: aws.String("db-1"), RelationalDatabaseSnapshotName: aws.String("db-1-snap"),
	})
	require.NoError(t, err)

	snapOut, err := client.GetRelationalDatabaseSnapshot(
		ctx,
		&lightsailsdk.GetRelationalDatabaseSnapshotInput{RelationalDatabaseSnapshotName: aws.String("db-1-snap")},
	)
	require.NoError(t, err)
	require.Equal(t, "db-1-snap", aws.ToString(snapOut.RelationalDatabaseSnapshot.Name))

	_, err = client.RebootRelationalDatabase(
		ctx,
		&lightsailsdk.RebootRelationalDatabaseInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)

	eventsOut, err := client.GetRelationalDatabaseEvents(
		ctx,
		&lightsailsdk.GetRelationalDatabaseEventsInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, eventsOut.RelationalDatabaseEvents)

	_, err = client.StopRelationalDatabase(
		ctx,
		&lightsailsdk.StopRelationalDatabaseInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)

	_, err = client.StartRelationalDatabase(
		ctx,
		&lightsailsdk.StartRelationalDatabaseInput{RelationalDatabaseName: aws.String("db-1")},
	)
	require.NoError(t, err)

	_, err = client.DeleteRelationalDatabaseSnapshot(
		ctx,
		&lightsailsdk.DeleteRelationalDatabaseSnapshotInput{RelationalDatabaseSnapshotName: aws.String("db-1-snap")},
	)
	require.NoError(t, err)

	_, err = client.DeleteRelationalDatabase(ctx, &lightsailsdk.DeleteRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("db-1"), SkipFinalSnapshot: aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestContainerServiceRoundTrip exercises family Q+R end to end, including
// the full PENDING->READY (no-deployment) and PENDING->DEPLOYING->RUNNING
// (with-deployment) state machines.
func TestContainerServiceRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateContainerService(ctx, &lightsailsdk.CreateContainerServiceInput{
		ServiceName: aws.String("svc-1"), Power: lightsailtypes.ContainerServicePowerNameNano, Scale: aws.Int32(1),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetContainerServices(
			ctx,
			&lightsailsdk.GetContainerServicesInput{ServiceName: aws.String("svc-1")},
		)

		return getErr == nil && len(out.ContainerServices) == 1 &&
			out.ContainerServices[0].State == lightsailtypes.ContainerServiceStateReady
	}, defaultAsyncWait, defaultAsyncPoll, "container service never reached READY")

	regOut, err := client.RegisterContainerImage(ctx, &lightsailsdk.RegisterContainerImageInput{
		ServiceName: aws.String("svc-1"), Label: aws.String("app"), Digest: aws.String("sha256:deadbeef"),
	})
	require.NoError(t, err)
	require.Contains(t, aws.ToString(regOut.ContainerImage.Image), "svc-1.app.1")

	imagesOut, err := client.GetContainerImages(
		ctx,
		&lightsailsdk.GetContainerImagesInput{ServiceName: aws.String("svc-1")},
	)
	require.NoError(t, err)
	require.Len(t, imagesOut.ContainerImages, 1)

	_, err = client.CreateContainerServiceDeployment(ctx, &lightsailsdk.CreateContainerServiceDeploymentInput{
		ServiceName: aws.String("svc-1"),
		Containers: map[string]lightsailtypes.Container{
			"app": {Image: aws.String(aws.ToString(regOut.ContainerImage.Image))},
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetContainerServices(
			ctx,
			&lightsailsdk.GetContainerServicesInput{ServiceName: aws.String("svc-1")},
		)

		return getErr == nil && len(out.ContainerServices) == 1 &&
			out.ContainerServices[0].State == lightsailtypes.ContainerServiceStateRunning
	}, defaultAsyncWait, defaultAsyncPoll, "container service never reached RUNNING")

	deploymentsOut, err := client.GetContainerServiceDeployments(
		ctx,
		&lightsailsdk.GetContainerServiceDeploymentsInput{ServiceName: aws.String("svc-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, deploymentsOut.Deployments)
	require.Equal(
		t,
		lightsailtypes.ContainerServiceDeploymentStateActive,
		deploymentsOut.Deployments[len(deploymentsOut.Deployments)-1].State,
	)

	loginOut, err := client.CreateContainerServiceRegistryLogin(
		ctx,
		&lightsailsdk.CreateContainerServiceRegistryLoginInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(loginOut.RegistryLogin.Username))

	_, err = client.DeleteContainerImage(ctx, &lightsailsdk.DeleteContainerImageInput{
		ServiceName: aws.String("svc-1"), Image: aws.String(aws.ToString(regOut.ContainerImage.Image)),
	})
	require.NoError(t, err)

	_, err = client.DeleteContainerService(
		ctx,
		&lightsailsdk.DeleteContainerServiceInput{ServiceName: aws.String("svc-1")},
	)
	require.NoError(t, err)
}
