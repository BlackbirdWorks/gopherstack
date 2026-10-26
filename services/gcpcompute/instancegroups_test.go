package gcpcompute_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func (e *env) group(t *testing.T, name string) {
	t.Helper()

	op, err := e.groups(t).Insert(t.Context(), &computepb.InsertInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroupResource: &computepb.InstanceGroup{Name: s(name)},
	})
	e.done(t, op, err)
}

func (e *env) groupMembers(t *testing.T, name, state string) []*computepb.InstanceWithNamedPorts {
	t.Helper()

	it := e.groups(t).ListInstances(t.Context(), &computepb.ListInstancesInstanceGroupsRequest{
		Project:       project,
		Zone:          zone,
		InstanceGroup: name,
		InstanceGroupsListInstancesRequestResource: &computepb.InstanceGroupsListInstancesRequest{
			InstanceState: s(state),
		},
	})

	var out []*computepb.InstanceWithNamedPorts

	for {
		m, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, m)
	}
}

func (e *env) addToGroup(t *testing.T, group string, vms ...string) error {
	t.Helper()

	refs := make([]*computepb.InstanceReference, len(vms))
	for i, v := range vms {
		refs[i] = &computepb.InstanceReference{Instance: s("zones/" + zone + "/instances/" + v)}
	}

	op, err := e.groups(t).AddInstances(t.Context(), &computepb.AddInstancesInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: group,
		InstanceGroupsAddInstancesRequestResource: &computepb.InstanceGroupsAddInstancesRequest{Instances: refs},
	})
	if err == nil {
		e.settle()
		err = op.Wait(t.Context())
	}

	return err
}

func TestUnmanagedInstanceGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))
	e.vm(t, vmSpec("vm2"))
	e.settle()
	e.group(t, "ug")

	g, err := e.groups(t).
		Get(t.Context(), &computepb.GetInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ug"})
	require.NoError(t, err)
	assert.Equal(t, int32(0), g.GetSize())
	assert.Contains(t, g.GetSelfLink(), "/zones/"+zone+"/instanceGroups/ug")

	require.NoError(t, e.addToGroup(t, "ug", "vm1", "vm2"))

	g, err = e.groups(t).
		Get(t.Context(), &computepb.GetInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ug"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), g.GetSize())
	assert.Contains(t, g.GetNetwork(), "/global/networks/vpc")

	members := e.groupMembers(t, "ug", "ALL")
	require.Len(t, members, 2)
	assert.Equal(t, "RUNNING", members[0].GetStatus())

	assertAPIError(t, e.addToGroup(t, "ug", "vm1"), http.StatusBadRequest, "invalid")
	assertAPIError(t, e.addToGroup(t, "ug", "ghost"), http.StatusNotFound, "notFound")

	op, err := e.instances(t).
		Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm2"})
	e.done(t, op, err)
	assert.Len(t, e.groupMembers(t, "ug", "ALL"), 2)

	listPath := "/compute/v1/projects/" + project + "/zones/" + zone + "/instanceGroups/ug/listInstances"

	for _, c := range []struct {
		state string
		want  int
		code  int
	}{
		{"RUNNING", 1, http.StatusOK}, {"ALL", 2, http.StatusOK}, {"", 2, http.StatusOK},
		{"BOGUS", 0, http.StatusBadRequest},
	} {
		code, body := rawDo(t, e, http.MethodPost, listPath, `{"instanceState":"`+c.state+`"}`)
		assert.Equal(t, c.code, code, c.state)
		assert.Equal(t, c.want, strings.Count(body, `"instance":`), c.state)
	}

	op, err = e.groups(t).SetNamedPorts(t.Context(), &computepb.SetNamedPortsInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: "ug",
		InstanceGroupsSetNamedPortsRequestResource: &computepb.InstanceGroupsSetNamedPortsRequest{
			NamedPorts: []*computepb.NamedPort{{Name: s("http"), Port: proto.Int32(80)}}, Fingerprint: g.Fingerprint,
		},
	})
	e.done(t, op, err)

	_, err = e.groups(t).SetNamedPorts(t.Context(), &computepb.SetNamedPortsInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: "ug",
		InstanceGroupsSetNamedPortsRequestResource: &computepb.InstanceGroupsSetNamedPortsRequest{
			NamedPorts: []*computepb.NamedPort{{Name: s("http"), Port: proto.Int32(81)}}, Fingerprint: g.Fingerprint,
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	members = e.groupMembers(t, "ug", "ALL")
	require.NotEmpty(t, members[0].GetNamedPorts())
	assert.Equal(t, int32(80), members[0].GetNamedPorts()[0].GetPort())

	op, err = e.groups(t).RemoveInstances(t.Context(), &computepb.RemoveInstancesInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: "ug",
		InstanceGroupsRemoveInstancesRequestResource: &computepb.InstanceGroupsRemoveInstancesRequest{
			Instances: []*computepb.InstanceReference{{Instance: s("zones/" + zone + "/instances/vm1")}},
		},
	})
	e.done(t, op, err)
	assert.Len(t, e.groupMembers(t, "ug", "ALL"), 1)

	_, err = e.groups(t).RemoveInstances(t.Context(), &computepb.RemoveInstancesInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: "ug",
		InstanceGroupsRemoveInstancesRequestResource: &computepb.InstanceGroupsRemoveInstancesRequest{
			Instances: []*computepb.InstanceReference{{Instance: s("zones/" + zone + "/instances/vm1")}},
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = e.instances(t).
		Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "vm2"})
	e.done(t, op, err)
	assert.Empty(t, e.groupMembers(t, "ug", "ALL"), "deleting an instance removes it from its groups")

	op, err = e.groups(t).
		Delete(t.Context(), &computepb.DeleteInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ug"})
	e.done(t, op, err)

	_, err = e.groups(t).
		Get(t.Context(), &computepb.GetInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ug"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestInstanceGroupNetworkAndNameRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.network(t, "other", false)
	e.subnet(t, "sub2", "10.30.0.0/24")

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s("osub"), Network: s("global/networks/other"), IpCidrRange: s("10.40.0.0/24"),
		},
	})
	e.done(t, op, err)

	spec := vmSpec("vm-other")
	spec.NetworkInterfaces[0].Subnetwork = s("regions/" + region + "/subnetworks/osub")
	e.vm(t, vmSpec("vm1"))
	e.vm(t, spec)
	e.group(t, "ug")

	require.NoError(t, e.addToGroup(t, "ug", "vm1"))
	assertAPIError(t, e.addToGroup(t, "ug", "vm-other"), http.StatusBadRequest, "invalid")

	_, err = e.groups(t).Insert(t.Context(), &computepb.InsertInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroupResource: &computepb.InstanceGroup{Name: s("ug")},
	})
	assertAPIError(t, err, http.StatusConflict, "alreadyExists")

	e.mig(t, "managed", e.template(t, "tpl"), 1)

	_, err = e.groups(t).Insert(t.Context(), &computepb.InsertInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroupResource: &computepb.InstanceGroup{Name: s("managed")},
	})
	assertAPIError(t, err, http.StatusConflict, "alreadyExists")

	_, err = e.groups(t).
		Delete(t.Context(), &computepb.DeleteInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "managed"})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
	assertAPIError(t, e.addToGroup(t, "managed", "vm1"), http.StatusBadRequest, "invalid")
}

func TestManagedInstanceGroupView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		regional bool
	}{
		{name: "zonal"},
		{name: "regional", regional: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			tmpl := e.template(t, "tpl")

			var size int32

			if tt.regional {
				op, err := e.regionMigs(t).Insert(t.Context(), &computepb.InsertRegionInstanceGroupManagerRequest{
					Project: project, Region: region, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
						Name: s("m"), BaseInstanceName: s("m"), InstanceTemplate: s(tmpl), TargetSize: proto.Int32(4),
					},
				})
				e.done(t, op, err)
				e.settle()

				g, gerr := e.regionGroups(t).Get(t.Context(), &computepb.GetRegionInstanceGroupRequest{
					Project: project, Region: region, InstanceGroup: "m",
				})
				require.NoError(t, gerr)

				size = g.GetSize()

				it := e.regionGroups(t).ListInstances(t.Context(), &computepb.ListInstancesRegionInstanceGroupsRequest{
					Project: project, Region: region, InstanceGroup: "m",
				})
				n := 0

				for {
					_, nerr := it.Next()
					if errors.Is(nerr, iterator.Done) {
						break
					}

					require.NoError(t, nerr)

					n++
				}

				assert.Equal(t, 4, n)

				op, err = e.regionGroups(t).
					SetNamedPorts(t.Context(), &computepb.SetNamedPortsRegionInstanceGroupRequest{
						Project:       project,
						Region:        region,
						InstanceGroup: "m",
						RegionInstanceGroupsSetNamedPortsRequestResource: &computepb.RegionInstanceGroupsSetNamedPortsRequest{
							NamedPorts: []*computepb.NamedPort{{Name: s("http"), Port: proto.Int32(80)}},
						},
					})
				e.done(t, op, err)

				mg, err := e.regionMigs(t).Get(t.Context(), &computepb.GetRegionInstanceGroupManagerRequest{
					Project: project, Region: region, InstanceGroupManager: "m",
				})
				require.NoError(t, err)
				require.Len(t, mg.GetNamedPorts(), 1)
			} else {
				e.mig(t, "m", tmpl, 4)
				e.settle()

				g, err := e.groups(t).
					Get(t.Context(), &computepb.GetInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "m"})
				require.NoError(t, err)

				size = g.GetSize()

				assert.Len(t, e.groupMembers(t, "m", "RUNNING"), 4)
			}

			assert.Equal(t, int32(4), size)
		})
	}
}
