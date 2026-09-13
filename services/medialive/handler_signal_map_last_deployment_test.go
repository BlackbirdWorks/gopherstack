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

// TestSignalMap_LastSuccessfulMonitorDeployment_RealClient drives
// CreateSignalMap -> StartMonitorDeployment -> StartDeleteMonitorDeployment
// through the real SDK client. GetSignalMapOutput.LastSuccessfulMonitorDeployment
// is optional but, once a deployment has actually succeeded, its
// DetailsUri/Status members are both "This member is required"
// (medialive@v1.101.4 types/types.go:8320, 8325). The pre-fix handler never
// modeled the field at all, so it was always absent even after a successful
// deployment -- gopherstack-mven required-output nested-domain-struct sweep.
func TestSignalMap_LastSuccessfulMonitorDeployment_RealClient(t *testing.T) {
	t.Parallel()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMediaLiveClient(t, medialive.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateSignalMap(ctx, &medialivesdk.CreateSignalMapInput{
		Name:                   aws.String("last-deploy-signal-map"),
		DiscoveryEntryPointArn: aws.String("arn:aws:medialive:us-east-1:000000000000:input:1234567"),
	})
	require.NoError(t, err)
	assert.Nil(t, created.LastSuccessfulMonitorDeployment,
		"a freshly created signal map has never deployed, so this must stay absent")

	deployed, err := client.StartMonitorDeployment(ctx, &medialivesdk.StartMonitorDeploymentInput{
		Identifier: created.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, deployed.LastSuccessfulMonitorDeployment,
		"a successful StartMonitorDeployment must populate LastSuccessfulMonitorDeployment")
	assert.NotEmpty(t, aws.ToString(deployed.LastSuccessfulMonitorDeployment.DetailsUri))
	assert.Equal(t,
		types.SignalMapMonitorDeploymentStatusDeploymentComplete,
		deployed.LastSuccessfulMonitorDeployment.Status,
	)

	got, err := client.GetSignalMap(ctx, &medialivesdk.GetSignalMapInput{Identifier: created.Id})
	require.NoError(t, err)
	require.NotNil(t, got.LastSuccessfulMonitorDeployment)
	assert.Equal(t, aws.ToString(deployed.LastSuccessfulMonitorDeployment.DetailsUri),
		aws.ToString(got.LastSuccessfulMonitorDeployment.DetailsUri))

	deleted, err := client.StartDeleteMonitorDeployment(ctx, &medialivesdk.StartDeleteMonitorDeploymentInput{
		Identifier: created.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.MonitorDeployment)
	assert.Equal(t, types.SignalMapMonitorDeploymentStatusDeleteComplete, deleted.MonitorDeployment.Status)
	require.NotNil(t, deleted.LastSuccessfulMonitorDeployment,
		"tearing down the current deployment must not erase the record of the last successful one")
	assert.Equal(t,
		types.SignalMapMonitorDeploymentStatusDeploymentComplete,
		deleted.LastSuccessfulMonitorDeployment.Status,
	)
}
