package route53recoverycluster_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster/types"
	rccfg "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// topology is a deployed cluster with two routing controls in its default panel.
type topology struct {
	client     *route53recoverycluster.Client
	clusterArn string
	panelArn   string
	controlA   string
	controlB   string
	endpoint   string
}

func (f *fixture) build(t *testing.T) topology {
	t.Helper()

	ctx := t.Context()
	cl, err := f.ccClient.CreateCluster(ctx, &rccfg.CreateClusterInput{ClusterName: aws.String("c1")})
	require.NoError(t, err)

	desc, err := f.ccClient.DescribeCluster(ctx, &rccfg.DescribeClusterInput{ClusterArn: cl.Cluster.ClusterArn})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Cluster.ClusterEndpoints)

	panels, err := f.ccClient.ListControlPanels(ctx, &rccfg.ListControlPanelsInput{ClusterArn: cl.Cluster.ClusterArn})
	require.NoError(t, err)
	require.Len(t, panels.ControlPanels, 1)

	mk := func(name string) string {
		out, cErr := f.ccClient.CreateRoutingControl(ctx, &rccfg.CreateRoutingControlInput{
			ClusterArn: cl.Cluster.ClusterArn, RoutingControlName: aws.String(name),
		})
		require.NoError(t, cErr)

		return aws.ToString(out.RoutingControl.RoutingControlArn)
	}

	ep := desc.Cluster.ClusterEndpoints[0]

	return topology{
		clusterArn: aws.ToString(
			cl.Cluster.ClusterArn,
		),
		panelArn: aws.ToString(panels.ControlPanels[0].ControlPanelArn),
		controlA: mk(
			"a",
		),
		controlB: mk("b"),
		endpoint: aws.ToString(ep.Endpoint),
		client:   f.dataPlane(aws.ToString(ep.Endpoint)),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestClusterEndpointsServeDataPlane(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)
	ctx := t.Context()

	assert.Contains(t, topo.endpoint, f.url)

	got, err := topo.client.GetRoutingControlState(ctx, &route53recoverycluster.GetRoutingControlStateInput{
		RoutingControlArn: aws.String(topo.controlA),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RoutingControlStateOff, got.RoutingControlState)
	assert.Equal(t, "a", aws.ToString(got.RoutingControlName))

	_, err = topo.client.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
		RoutingControlArn: aws.String(topo.controlA), RoutingControlState: types.RoutingControlStateOn,
	})
	require.NoError(t, err)

	got, err = topo.client.GetRoutingControlState(ctx, &route53recoverycluster.GetRoutingControlStateInput{
		RoutingControlArn: aws.String(topo.controlA),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RoutingControlStateOn, got.RoutingControlState)

	list, err := topo.client.ListRoutingControls(ctx, &route53recoverycluster.ListRoutingControlsInput{
		ControlPanelArn: aws.String(topo.panelArn),
	})
	require.NoError(t, err)
	require.Len(t, list.RoutingControls, 2)
	assert.Equal(t, "a", aws.ToString(list.RoutingControls[0].RoutingControlName))
	assert.Equal(t, types.RoutingControlStateOn, list.RoutingControls[0].RoutingControlState)
	assert.Equal(t, "DefaultControlPanel", aws.ToString(list.RoutingControls[0].ControlPanelName))
}
