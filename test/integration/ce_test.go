package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cesdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CostExplorer_AnomalyMonitorLifecycle exercises the
// AnomalyMonitor portion of the AWS Cost Explorer API via the AWS SDK v2.
// Primary integration coverage for the Cost Explorer JSON-RPC handler.
func TestIntegration_CostExplorer_AnomalyMonitorLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCostExplorerClient(t)
	ctx := t.Context()

	// CreateAnomalyMonitor.
	createOut, err := client.CreateAnomalyMonitor(ctx, &cesdk.CreateAnomalyMonitorInput{
		AnomalyMonitor: &cetypes.AnomalyMonitor{
			MonitorName: aws.String("it-ce-monitor"),
			MonitorType: cetypes.MonitorTypeDimensional,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.MonitorArn))

	arn := aws.ToString(createOut.MonitorArn)

	t.Cleanup(func() {
		_, _ = client.DeleteAnomalyMonitor(ctx, &cesdk.DeleteAnomalyMonitorInput{
			MonitorArn: aws.String(arn),
		})
	})

	// GetAnomalyMonitors should include the new monitor.
	getOut, err := client.GetAnomalyMonitors(ctx, &cesdk.GetAnomalyMonitorsInput{
		MonitorArnList: []string{arn},
	})
	require.NoError(t, err)
	require.Len(t, getOut.AnomalyMonitors, 1)
	assert.Equal(t, arn, aws.ToString(getOut.AnomalyMonitors[0].MonitorArn))
}
