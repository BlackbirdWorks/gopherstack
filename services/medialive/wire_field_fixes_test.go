package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestSignalMap_StatusIsLegalEnumMember drives CreateSignalMap and
// StartUpdateSignalMap through the real aws-sdk-go-v2 client.
// CreateSignalMapOutput.Status/StartUpdateSignalMapOutput.Status are
// types.SignalMapStatus (CREATE_IN_PROGRESS/CREATE_COMPLETE/CREATE_FAILED/
// UPDATE_IN_PROGRESS/UPDATE_COMPLETE/UPDATE_REVERTED/UPDATE_FAILED/READY/
// NOT_READY -- medialive@v1.101.4 types/enums.go); the backend previously
// set the bare string "SUCCEEDED" on both create and update, which is not a
// member of SignalMapStatus, so a real client's waiter for a signal map
// would never match any case and poll until timeout.
func TestSignalMap_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("my-signal-map"),
		DiscoveryEntryPointArn: aws.String("arn:aws:medialive:us-east-1:000000000000:input:1234567"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalMapStatusCreateComplete, created.Status)

	updated, err := client.StartUpdateSignalMap(ctx, &medialivesdk.StartUpdateSignalMapInput{
		Identifier:  created.Id,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.SignalMapStatusUpdateComplete, updated.Status)
}

// TestSignalMap_MonitorDeploymentStatusIsLegalEnumMember drives
// StartMonitorDeployment/StartDeleteMonitorDeployment through the real
// aws-sdk-go-v2 client. StartMonitorDeploymentOutput/
// StartDeleteMonitorDeploymentOutput nest their status under a
// "monitorDeployment" object (types.MonitorDeployment.Status --
// medialive@v1.101.4 types/types.go:5679); the backend previously emitted a
// flat top-level "monitorDeploymentStatus" key instead, which a real client
// silently discards, decoding MonitorDeployment as nil.
func TestSignalMap_MonitorDeploymentStatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("my-signal-map-2"),
		DiscoveryEntryPointArn: aws.String("arn:aws:medialive:us-east-1:000000000000:input:1234567"),
	})
	require.NoError(t, err)

	deployed, err := client.StartMonitorDeployment(ctx, &medialivesdk.StartMonitorDeploymentInput{
		Identifier: created.Id,
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		deployed.MonitorDeployment,
		"MonitorDeployment must nest under monitorDeployment, not a flat monitorDeploymentStatus key",
	)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusDeploymentComplete, deployed.MonitorDeployment.Status)

	deleted, err := client.StartDeleteMonitorDeployment(ctx, &medialivesdk.StartDeleteMonitorDeploymentInput{
		Identifier: created.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.MonitorDeployment)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusDeleteComplete, deleted.MonitorDeployment.Status)
}

// TestCreateSignalMap_MonitorDeploymentNested covers the same
// monitorDeployment nesting bug on CreateSignalMap/GetSignalMap/
// StartUpdateSignalMap. Real Create/Get/StartUpdateSignalMapOutput nest the
// monitor deployment status under a "monitorDeployment" object
// (types.MonitorDeployment.Status -- medialive@v1.101.4
// deserializers.go:4687-4690), not a flat "monitorDeploymentStatus" key.
func TestCreateSignalMap_MonitorDeploymentNested(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("my-signal-map-3"),
		DiscoveryEntryPointArn: aws.String("arn:aws:medialive:us-east-1:000000000000:input:1234567"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.MonitorDeployment)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusNotDeployed, created.MonitorDeployment.Status)

	got, err := client.GetSignalMap(ctx, &medialivesdk.GetSignalMapInput{Identifier: created.Id})
	require.NoError(t, err)
	require.NotNil(t, got.MonitorDeployment)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusNotDeployed, got.MonitorDeployment.Status)

	updated, err := client.StartUpdateSignalMap(ctx, &medialivesdk.StartUpdateSignalMapInput{
		Identifier:  created.Id,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MonitorDeployment)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusNotDeployed, updated.MonitorDeployment.Status)
}
