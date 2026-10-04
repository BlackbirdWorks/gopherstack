package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	lightsailbackend "github.com/blackbirdworks/gopherstack/services/lightsail"
)

// TestInitializeServices_LightsailCloudFormationWiring proves the real
// composition root wires CreateCloudFormationStack to a real Stack.
func TestInitializeServices_LightsailCloudFormationWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19300, 19400)
	require.NoError(t, err)

	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
		PortAlloc:  portAlloc,
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	lsH, ok := byName["Lightsail"].(*lightsailbackend.Handler)
	require.True(t, ok, "Lightsail handler must be registered")

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok, "CloudFormation handler must be registered")

	_, err = lsH.Backend.CreateInstances(lightsailbackend.CreateInstancesRequest{
		Names: []string{"wiring-instance"}, AvailabilityZone: "us-east-1a",
		BlueprintID: "amazon_linux_2023", BundleID: "nano_3_0",
	})
	require.NoError(t, err)

	_, err = lsH.Backend.CreateInstanceSnapshot("wiring-instance", "wiring-snap", nil)
	require.NoError(t, err)

	_, err = lsH.Backend.ExportSnapshot("wiring-snap")
	require.NoError(t, err)

	exportRecords, err := lsH.Backend.GetExportSnapshotRecords("")
	require.NoError(t, err)
	require.Len(t, exportRecords.Data, 1)

	before := len(cfnH.Backend.ListAll())

	_, err = lsH.Backend.CreateCloudFormationStack([]lightsailbackend.InstanceEntry{
		{SourceName: exportRecords.Data[0].Name, AvailabilityZone: "us-east-1a", InstanceType: "nano"},
	})
	require.NoError(t, err)

	after := cfnH.Backend.ListAll()
	require.Len(t, after, before+1,
		"a real cli.go composition root must wire Lightsail's CreateCloudFormationStack "+
			"to a real cloudformation Stack via wireLightsailCloudFormation")

	recordsOut, err := lsH.Backend.GetCloudFormationStackRecords("")
	require.NoError(t, err)
	require.Len(t, recordsOut.Data, 1)
	require.Equal(t, "SUCCEEDED", recordsOut.Data[0].State,
		"State must be SUCCEEDED once the wired cloudformation backend creates a real stack")
	require.NotEmpty(t, recordsOut.Data[0].DestinationInfoID)
}
