package gcpcompute_test

import (
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpansionStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.v6Network(t)
	require.NoError(t, src.v6Subnet(t, "sub", "10.10.0.0/24", "IPV4_IPV6", "INTERNAL"))
	require.NoError(t, src.vmOn(t, "vm1", "sub", &computepb.NetworkInterface{StackType: s("IPV4_IPV6")}))
	src.legacyHC(t, "lhc")
	require.NoError(t, src.poolInsert(t, &computepb.TargetPool{
		Name: s("pool"), Instances: []string{vm1Ref}, HealthChecks: []string{lhcRef},
	}))
	src.haGateway(t, "gw-a")
	src.haGateway(t, "gw-b")
	require.NoError(t, src.insertTunnel(t, haTunnel("ta", gwA, gwB, 0)))
	require.NoError(t, src.insertTunnel(t, haTunnel("tb", gwB, gwA, 0)))
	src.externalGateway(t)

	src.webMIGOnSub(t)
	require.NoError(
		t,
		src.resizeInsert(t, &computepb.InstanceGroupManagerResizeRequest{Name: s("rr"), ResizeBy: new(int32(1))}),
	)
	src.settle()

	cfgName := src.migInstances(t)[0]
	require.NoError(t, src.upsertConfig(t, false, &computepb.PerInstanceConfig{
		Name: s(cfgName), PreservedState: meta(map[string]string{"k": "v"}),
	}))

	data := src.backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), data))
	dst.clock.Advance(src.clock.Now().Sub(dst.clock.Now()) + time.Minute)

	assert.NotEmpty(t, dst.getVMNic(t, "vm1").GetIpv6Address())
	assert.Len(t, dst.getPool(t).GetInstances(), 1)
	assert.Equal(t, "ESTABLISHED", dst.tunnel(t, "ta").GetStatus())
	assert.Equal(t, "SUCCEEDED", dst.resizeGet(t, "rr").GetState())
	assert.Len(t, dst.listConfigs(t), 1)

	got, err := dst.legacy(t).HttpHealthChecks.Get(project, "lhc").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.EqualValues(t, 80, got.Port)
}
