package gcpcompute_test

import (
	"net/http"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyAndPeeringStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.peerNet(t, "a", "10.1.0.0/24")
	src.peerNet(t, "b", "10.2.0.0/24")
	require.NoError(
		t,
		src.addPeering(t, "a", "a-to-b", "b", func(p *computepb.NetworkPeering) { p.ExportCustomRoutes = new(true) }),
	)
	require.NoError(t, src.addPeering(t, "b", "b-to-a", "a", nil))

	src.lbStack(t)
	require.NoError(t, src.policy(t, &computepb.SecurityPolicy{
		Name: s(
			"pol",
		),
		Rules: []*computepb.SecurityPolicyRule{
			srcRule(10, "deny(403)", "198.51.100.0/24"), srcRule(defaultPrio, "allow", "*"),
		},
	}))

	bsc := src.backendServices(t)
	op, err := bsc.SetSecurityPolicy(t.Context(), &computepb.SetSecurityPolicyBackendServiceRequest{
		Project: project, BackendService: "bs",
		SecurityPolicyReferenceResource: &computepb.SecurityPolicyReference{
			SecurityPolicy: s("global/securityPolicies/pol"),
		},
	})
	src.done(t, op, err)

	src.disk(t, "d1")
	require.NoError(t, src.resPolicy(t, named(daily("00:00"), "snap")))
	require.NoError(t, src.changeDisk(t, "d1", true, rpRef("snap")))

	src.baseNetVM(t)
	require.NoError(t, src.resPolicy(t, named(instanceSchedule("0 8 * * *"), "sched")))
	require.NoError(t, src.changeVM(t, true, rpRef("sched")))

	suspend, err := src.instances(t).
		Suspend(t.Context(), &computepb.SuspendInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	src.done(t, suspend, err)

	data := src.backend.Snapshot(t.Context())

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), data))
	dst.clock.Advance(src.clock.Now().Sub(dst.clock.Now()) + time.Minute)

	assert.Equal(t, "ACTIVE", dst.peerings(t, "a")["a-to-b"].GetState())
	assert.True(t, dst.peerings(t, "a")["a-to-b"].GetExportCustomRoutes())
	assert.Contains(t, dst.peeringRoutes(t, "b", "b-to-a", "INCOMING"), "10.1.0.0/24")

	p := dst.getPolicy(t)
	require.Len(t, p.GetRules(), 2)
	assert.Equal(t, int32(10), p.GetRules()[0].GetPriority())
	assert.Equal(t, "deny(403)", p.GetRules()[0].GetAction())

	bs, err := dst.backendServices(t).
		Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Contains(t, bs.GetSecurityPolicy(), "/securityPolicies/pol")

	_, err = dst.armor(t).
		Delete(t.Context(), &computepb.DeleteSecurityPolicyRequest{Project: project, SecurityPolicy: "pol"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = dst.resPolicies(t).
		Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{Project: project, Region: region, ResourcePolicy: "snap"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	vm := dst.getVM(t, "vm1")
	assert.Equal(t, "SUSPENDED", vm.GetStatus())
	assert.Len(t, vm.GetResourcePolicies(), 1)

	assert.Contains(t, dst.routeList(t), "10.1.0.0/24")
	assert.Equal(t, "SUBNET", dst.routeList(t)["10.2.0.0/24"].GetRouteType())
}

func (e *env) backendServices(t *testing.T) *compute.BackendServicesClient {
	t.Helper()

	return client(t, e, compute.NewBackendServicesRESTClient)
}

func (e *env) baseNetVM(t *testing.T) {
	t.Helper()
	e.runningVM(t)
}
