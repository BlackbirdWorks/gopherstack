package gcpcompute_test

import (
	"errors"
	"strings"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

const (
	globalTmplPath = "projects/" + project + "/global/instanceTemplates/"
	subnetFull     = "regions/" + region + "/subnetworks/sub"
)

func (e *env) templates(t *testing.T) *compute.InstanceTemplatesClient {
	t.Helper()

	return client(t, e, compute.NewInstanceTemplatesRESTClient)
}

func (e *env) regionTemplates(t *testing.T) *compute.RegionInstanceTemplatesClient {
	t.Helper()

	return client(t, e, compute.NewRegionInstanceTemplatesRESTClient)
}

func (e *env) migs(t *testing.T) *compute.InstanceGroupManagersClient {
	t.Helper()

	return client(t, e, compute.NewInstanceGroupManagersRESTClient)
}

func (e *env) regionMigs(t *testing.T) *compute.RegionInstanceGroupManagersClient {
	t.Helper()

	return client(t, e, compute.NewRegionInstanceGroupManagersRESTClient)
}

func (e *env) groups(t *testing.T) *compute.InstanceGroupsClient {
	t.Helper()

	return client(t, e, compute.NewInstanceGroupsRESTClient)
}

func (e *env) regionGroups(t *testing.T) *compute.RegionInstanceGroupsClient {
	t.Helper()

	return client(t, e, compute.NewRegionInstanceGroupsRESTClient)
}

func (e *env) healthChecks(t *testing.T) *compute.HealthChecksClient {
	t.Helper()

	return client(t, e, compute.NewHealthChecksRESTClient)
}

func (e *env) regionHealthChecks(t *testing.T) *compute.RegionHealthChecksClient {
	t.Helper()

	return client(t, e, compute.NewRegionHealthChecksRESTClient)
}

func (e *env) autoscalers(t *testing.T) *compute.AutoscalersClient {
	t.Helper()

	return client(t, e, compute.NewAutoscalersRESTClient)
}

func (e *env) regionAutoscalers(t *testing.T) *compute.RegionAutoscalersClient {
	t.Helper()

	return client(t, e, compute.NewRegionAutoscalersRESTClient)
}

func tmplSpec(name string) *computepb.InstanceTemplate {
	return &computepb.InstanceTemplate{
		Name: s(name),
		Properties: &computepb.InstanceProperties{
			MachineType: s("e2-medium"),
			Labels:      map[string]string{"env": "test"},
			Tags:        &computepb.Tags{Items: []string{"web"}},
			Disks: []*computepb.AttachedDisk{{
				Boot: new(true), AutoDelete: new(true),
				InitializeParams: &computepb.AttachedDiskInitializeParams{
					SourceImage: s(debianFam), DiskSizeGb: proto.Int64(20),
				},
			}},
			NetworkInterfaces: []*computepb.NetworkInterface{{Subnetwork: s(subnetFull)}},
		},
	}
}

func (e *env) template(t *testing.T, name string) string {
	t.Helper()

	op, err := e.templates(t).
		Insert(t.Context(), &computepb.InsertInstanceTemplateRequest{
			Project: project, InstanceTemplateResource: tmplSpec(name),
		})
	e.done(t, op, err)

	return globalTmplPath + name
}

func (e *env) healthCheck(t *testing.T, name string) string {
	t.Helper()

	op, err := e.healthChecks(t).Insert(t.Context(), &computepb.InsertHealthCheckRequest{
		Project: project, HealthCheckResource: &computepb.HealthCheck{
			Name: s(name), Type: s("TCP"), TcpHealthCheck: &computepb.TCPHealthCheck{Port: proto.Int32(80)},
		},
	})
	e.done(t, op, err)

	return "projects/" + project + "/global/healthChecks/" + name
}

func (e *env) mig(t *testing.T, name, tmpl string, size int32) {
	t.Helper()

	op, err := e.migs(t).Insert(t.Context(), &computepb.InsertInstanceGroupManagerRequest{
		Project: project, Zone: zone, InstanceGroupManagerResource: &computepb.InstanceGroupManager{
			Name: s(name), BaseInstanceName: s(name), InstanceTemplate: s(tmpl), TargetSize: new(size),
		},
	})
	e.done(t, op, err)
}

func (e *env) getMig(t *testing.T) *computepb.InstanceGroupManager {
	t.Helper()

	m, err := e.migs(t).Get(t.Context(),
		&computepb.GetInstanceGroupManagerRequest{Project: project, Zone: zone, InstanceGroupManager: "web"})
	require.NoError(t, err)

	return m
}

func (e *env) managed(t *testing.T) []*computepb.ManagedInstance {
	t.Helper()

	it := e.migs(t).ListManagedInstances(t.Context(),
		&computepb.ListManagedInstancesInstanceGroupManagersRequest{
			Project:              project,
			Zone:                 zone,
			InstanceGroupManager: "web",
		})

	var out []*computepb.ManagedInstance

	for {
		mi, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, mi)
	}
}

func (e *env) vmNames(t *testing.T) []string {
	t.Helper()

	it := e.instances(t).List(t.Context(), &computepb.ListInstancesRequest{Project: project, Zone: zone})

	var out []string

	for {
		vm, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, vm.GetName())
	}
}

func hasPrefixAll(names []string, prefix string) bool {
	for _, n := range names {
		if !strings.HasPrefix(n, prefix) {
			return false
		}
	}

	return true
}
