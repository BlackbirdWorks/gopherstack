package gcpcompute_test

import (
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const epsilon = 0.001

func quotaMap(qs []*computepb.Quota) map[string]*computepb.Quota {
	out := make(map[string]*computepb.Quota, len(qs))
	for _, q := range qs {
		out[q.GetMetric()] = q
	}

	return out
}

func (e *env) regionQuotas(t *testing.T, name string) map[string]*computepb.Quota {
	t.Helper()

	r, err := e.regions(t).Get(t.Context(), &computepb.GetRegionRequest{Project: project, Region: name})
	require.NoError(t, err)

	return quotaMap(r.GetQuotas())
}

func (e *env) projectQuotas(t *testing.T) map[string]*computepb.Quota {
	t.Helper()

	p, err := e.projects(t).Get(t.Context(), &computepb.GetProjectRequest{Project: project})
	require.NoError(t, err)

	return quotaMap(p.GetQuotas())
}

func TestRegionQuotasReflectUsage(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	before := e.regionQuotas(t, region)
	require.Contains(t, before, "CPUS")
	assert.Positive(t, before["CPUS"].GetLimit())
	assert.InDelta(t, 0, before["CPUS"].GetUsage(), epsilon)
	assert.InDelta(t, 0, before["INSTANCES"].GetUsage(), epsilon)

	assert.InDelta(t, 1, before["SUBNETWORKS"].GetUsage(), epsilon)

	spec := vmSpec("vm1")
	spec.MachineType = s("n1-standard-4")
	spec.Scheduling = &computepb.Scheduling{OnHostMaintenance: s("TERMINATE")}
	spec.GuestAccelerators = []*computepb.AcceleratorConfig{
		{AcceleratorType: s("nvidia-tesla-t4"), AcceleratorCount: proto.Int32(2)},
	}
	e.vm(t, spec)

	e.disk(t, "d1")

	got := e.regionQuotas(t, region)
	assert.InDelta(t, 4, got["CPUS"].GetUsage(), epsilon)
	assert.InDelta(t, 1, got["INSTANCES"].GetUsage(), epsilon)
	assert.InDelta(t, 30, got["DISKS_TOTAL_GB"].GetUsage(), epsilon, "20 GB boot disk and a 10 GB disk")
	assert.InDelta(t, 2, got["NVIDIA_T4_GPUS"].GetUsage(), epsilon)
	assert.InDelta(t, 1, got["IN_USE_ADDRESSES"].GetUsage(), epsilon)
	assert.InDelta(t, 0, got["E2_CPUS"].GetUsage(), epsilon)

	other := e.regionQuotas(t, "europe-west1")
	assert.InDelta(t, 0, other["CPUS"].GetUsage(), epsilon)
	assert.InDelta(t, 0, other["DISKS_TOTAL_GB"].GetUsage(), epsilon)

	proj := e.projectQuotas(t)
	assert.InDelta(t, 4, proj["CPUS_ALL_REGIONS"].GetUsage(), epsilon)
	assert.InDelta(t, 2, proj["GPUS_ALL_REGIONS"].GetUsage(), epsilon)
	assert.InDelta(t, 1, proj["NETWORKS"].GetUsage(), epsilon)

	e.stopVM1(t)

	stopped := e.regionQuotas(t, region)
	assert.InDelta(t, 0, stopped["CPUS"].GetUsage(), epsilon, "a stopped instance holds no CPUs")
	assert.InDelta(t, 1, stopped["INSTANCES"].GetUsage(), epsilon)
	assert.InDelta(t, 0, stopped["NVIDIA_T4_GPUS"].GetUsage(), epsilon)
	assert.InDelta(t, 0, e.projectQuotas(t)["CPUS_ALL_REGIONS"].GetUsage(), epsilon)
}

func TestQuotaCountsFollowResources(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)

	assert.InDelta(t, 1, e.projectQuotas(t)["NETWORKS"].GetUsage(), epsilon)

	op, err := e.firewalls(t).Insert(t.Context(), &computepb.InsertFirewallRequest{
		Project: project, FirewallResource: &computepb.Firewall{
			Name: s("fw"), Network: s("global/networks/vpc"), SourceRanges: []string{"10.0.0.0/8"},
			Allowed: []*computepb.Allowed{{IPProtocol: s("tcp")}},
		},
	})
	e.done(t, op, err)

	op, err = e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("ip")},
	})
	e.done(t, op, err)

	require.NoError(t, e.regionPolicy(t, &computepb.SecurityPolicy{Name: s("rp")}))
	require.NoError(t, e.policy(t, &computepb.SecurityPolicy{Name: s("gp")}))
	require.NoError(t, e.interconnect(t, dedicated("ic")))
	require.NoError(t, e.attach(t, &computepb.InterconnectAttachment{
		Name: s("att"), Interconnect: s("global/interconnects/ic"),
	}, false))

	proj := e.projectQuotas(t)
	assert.InDelta(t, 1, proj["FIREWALLS"].GetUsage(), epsilon)
	assert.InDelta(t, 1, proj["SECURITY_POLICIES"].GetUsage(), epsilon)
	assert.InDelta(t, 1, proj["INTERCONNECTS"].GetUsage(), epsilon)

	reg := e.regionQuotas(t, region)
	assert.InDelta(t, 1, reg["STATIC_ADDRESSES"].GetUsage(), epsilon)
	assert.InDelta(t, 0, reg["IN_USE_ADDRESSES"].GetUsage(), epsilon, "a reserved address nothing uses is not in use")
	assert.InDelta(t, 1, reg["SECURITY_POLICIES_PER_REGION"].GetUsage(), epsilon)
	assert.InDelta(t, 1, reg["INTERCONNECT_ATTACHMENTS_PER_REGION"].GetUsage(), epsilon)
}
