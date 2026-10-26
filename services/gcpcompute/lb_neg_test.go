package gcpcompute_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func zonalNEG(name string) *computepb.NetworkEndpointGroup {
	return &computepb.NetworkEndpointGroup{
		Name: s(name), Network: s(gl + "networks/vpc"), Subnetwork: s(subnetRef), DefaultPort: proto.Int32(8080),
	}
}

func TestZonalNEG(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(n *computepb.NetworkEndpointGroup)
		name   string
		status int
	}{
		{name: "defaults", mutate: func(*computepb.NetworkEndpointGroup) {}},
		{name: "no network anywhere", status: http.StatusNotFound, mutate: func(n *computepb.NetworkEndpointGroup) { n.Network = nil }},
		{name: "missing subnet", status: http.StatusBadRequest, mutate: func(n *computepb.NetworkEndpointGroup) { n.Subnetwork = nil }},
		{name: "bad type", status: http.StatusBadRequest, mutate: func(n *computepb.NetworkEndpointGroup) { n.NetworkEndpointType = s("INTERNET_IP_PORT") }},
		{name: "bad port", status: http.StatusBadRequest, mutate: func(n *computepb.NetworkEndpointGroup) { n.DefaultPort = proto.Int32(70000) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			negs := client(t, e, compute.NewNetworkEndpointGroupsRESTClient)
			n := zonalNEG("neg")
			tt.mutate(n)

			op, err := negs.Insert(t.Context(), &computepb.InsertNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroupResource: n,
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := negs.Get(t.Context(), &computepb.GetNetworkEndpointGroupRequest{Project: project, Zone: zone, NetworkEndpointGroup: "neg"})
			require.NoError(t, gerr)
			assert.Equal(t, "GCE_VM_IP_PORT", got.GetNetworkEndpointType())
			assert.Equal(t, int32(0), got.GetSize())
			assert.Contains(t, got.GetZone(), zone)
		})
	}
}

func listNEGEndpoints(t *testing.T, e *env, show string) []*computepb.NetworkEndpointWithHealthStatus {
	t.Helper()

	it := client(t, e, compute.NewNetworkEndpointGroupsRESTClient).ListNetworkEndpoints(t.Context(),
		&computepb.ListNetworkEndpointsNetworkEndpointGroupsRequest{
			Project: project, Zone: zone, NetworkEndpointGroup: "neg",
			NetworkEndpointGroupsListEndpointsRequestResource: &computepb.NetworkEndpointGroupsListEndpointsRequest{HealthStatus: s(show)},
		})

	var out []*computepb.NetworkEndpointWithHealthStatus

	for {
		v, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, v)
	}
}

func TestNEGEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		eps    []*computepb.NetworkEndpoint
		name   string
		want   int
		status int
	}{
		{name: "instance default ip and port", eps: []*computepb.NetworkEndpoint{{Instance: s("vm1")}}, want: 1},
		{name: "explicit port", eps: []*computepb.NetworkEndpoint{{Instance: s("vm1"), Port: proto.Int32(9000)}}, want: 1},
		{name: "unknown instance", eps: []*computepb.NetworkEndpoint{{Instance: s("ghost")}}, status: http.StatusNotFound},
		{name: "wrong ip", eps: []*computepb.NetworkEndpoint{{Instance: s("vm1"), IpAddress: s("10.99.0.1")}}, status: http.StatusBadRequest},
		{name: "no instance", eps: []*computepb.NetworkEndpoint{{IpAddress: s("10.10.0.5")}}, status: http.StatusBadRequest},
		{name: "duplicate in request", eps: []*computepb.NetworkEndpoint{{Instance: s("vm1")}, {Instance: s("vm1")}}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.vm(t, vmSpec("vm1"))

			negs := client(t, e, compute.NewNetworkEndpointGroupsRESTClient)
			op, err := negs.Insert(t.Context(), &computepb.InsertNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroupResource: zonalNEG("neg"),
			})
			e.done(t, op, err)

			op, err = negs.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroup: "neg",
				NetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.NetworkEndpointGroupsAttachEndpointsRequest{NetworkEndpoints: tt.eps},
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")
				assert.Empty(t, listNEGEndpoints(t, e, "SKIP"))

				return
			}

			e.done(t, op, err)

			items := listNEGEndpoints(t, e, "SKIP")
			require.Len(t, items, tt.want)
			assert.Equal(t, "vm1", items[0].GetNetworkEndpoint().GetInstance())
			assert.Regexp(t, `^10\.10\.0\.`, items[0].GetNetworkEndpoint().GetIpAddress())

			got, gerr := negs.Get(t.Context(), &computepb.GetNetworkEndpointGroupRequest{Project: project, Zone: zone, NetworkEndpointGroup: "neg"})
			require.NoError(t, gerr)
			assert.Equal(t, int32(tt.want), got.GetSize())

			_, err = negs.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroup: "neg",
				NetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.NetworkEndpointGroupsAttachEndpointsRequest{NetworkEndpoints: tt.eps[:1]},
			})
			assertAPIError(t, err, http.StatusBadRequest, "invalid")

			op, err = negs.DetachNetworkEndpoints(t.Context(), &computepb.DetachNetworkEndpointsNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroup: "neg",
				NetworkEndpointGroupsDetachEndpointsRequestResource: &computepb.NetworkEndpointGroupsDetachEndpointsRequest{NetworkEndpoints: tt.eps[:1]},
			})
			e.done(t, op, err)
			assert.Empty(t, listNEGEndpoints(t, e, "SKIP"))

			_, err = negs.DetachNetworkEndpoints(t.Context(), &computepb.DetachNetworkEndpointsNetworkEndpointGroupRequest{
				Project: project, Zone: zone, NetworkEndpointGroup: "neg",
				NetworkEndpointGroupsDetachEndpointsRequestResource: &computepb.NetworkEndpointGroupsDetachEndpointsRequest{NetworkEndpoints: tt.eps[:1]},
			})
			assertAPIError(t, err, http.StatusBadRequest, "invalid")
		})
	}
}

func TestInternetAndServerlessNEG(t *testing.T) {
	t.Parallel()

	t.Run("internet", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		gn := client(t, e, compute.NewGlobalNetworkEndpointGroupsRESTClient)

		op, err := gn.Insert(t.Context(), &computepb.InsertGlobalNetworkEndpointGroupRequest{
			Project: project, NetworkEndpointGroupResource: &computepb.NetworkEndpointGroup{
				Name: s("ineg"), NetworkEndpointType: s("INTERNET_FQDN_PORT"), DefaultPort: proto.Int32(443),
			},
		})
		e.done(t, op, err)

		op, err = gn.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsGlobalNetworkEndpointGroupRequest{
			Project: project, NetworkEndpointGroup: "ineg",
			GlobalNetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.GlobalNetworkEndpointGroupsAttachEndpointsRequest{
				NetworkEndpoints: []*computepb.NetworkEndpoint{{Fqdn: s("origin.example.com")}},
			},
		})
		e.done(t, op, err)

		it := gn.ListNetworkEndpoints(t.Context(), &computepb.ListNetworkEndpointsGlobalNetworkEndpointGroupsRequest{
			Project: project, NetworkEndpointGroup: "ineg",
		})
		v, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, "origin.example.com", v.GetNetworkEndpoint().GetFqdn())
		assert.Equal(t, int32(443), v.GetNetworkEndpoint().GetPort())

		_, err = gn.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsGlobalNetworkEndpointGroupRequest{
			Project: project, NetworkEndpointGroup: "ineg",
			GlobalNetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.GlobalNetworkEndpointGroupsAttachEndpointsRequest{
				NetworkEndpoints: []*computepb.NetworkEndpoint{{IpAddress: s("1.2.3.4")}},
			},
		})
		assertAPIError(t, err, http.StatusBadRequest, "invalid")
	})

	tests := []struct {
		neg    *computepb.NetworkEndpointGroup
		name   string
		status int
	}{
		{name: "cloud run", neg: &computepb.NetworkEndpointGroup{CloudRun: &computepb.NetworkEndpointGroupCloudRun{Service: s("my-svc")}}},
		{name: "cloud run url mask", neg: &computepb.NetworkEndpointGroup{CloudRun: &computepb.NetworkEndpointGroupCloudRun{UrlMask: s("<service>.example.com")}}},
		{name: "no target", neg: &computepb.NetworkEndpointGroup{}, status: http.StatusBadRequest},
		{name: "two targets", status: http.StatusBadRequest, neg: &computepb.NetworkEndpointGroup{
			CloudRun:      &computepb.NetworkEndpointGroupCloudRun{Service: s("a")},
			CloudFunction: &computepb.NetworkEndpointGroupCloudFunction{Function: s("f")},
		}},
		{name: "empty cloud run", neg: &computepb.NetworkEndpointGroup{CloudRun: &computepb.NetworkEndpointGroupCloudRun{}}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run("serverless "+tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			rn := client(t, e, compute.NewRegionNetworkEndpointGroupsRESTClient)
			tt.neg.Name, tt.neg.NetworkEndpointType = s("sneg"), s("SERVERLESS")

			op, err := rn.Insert(t.Context(), &computepb.InsertRegionNetworkEndpointGroupRequest{
				Project: project, Region: region, NetworkEndpointGroupResource: tt.neg,
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := rn.Get(t.Context(), &computepb.GetRegionNetworkEndpointGroupRequest{
				Project: project, Region: region, NetworkEndpointGroup: "sneg",
			})
			require.NoError(t, gerr)
			assert.Equal(t, "SERVERLESS", got.GetNetworkEndpointType())
			assert.Contains(t, got.GetRegion(), region)
		})
	}
}

func TestNEGAsBackend(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.lbHC(t, "hc")

	negs := client(t, e, compute.NewNetworkEndpointGroupsRESTClient)
	op, err := negs.Insert(t.Context(), &computepb.InsertNetworkEndpointGroupRequest{
		Project: project, Zone: zone, NetworkEndpointGroupResource: zonalNEG("neg"),
	})
	e.done(t, op, err)

	negRef := "zones/" + zone + "/networkEndpointGroups/neg"
	require.NoError(t, e.lbBS(t, &computepb.BackendService{
		Name: s("bs"), HealthChecks: []string{hcRef},
		Backends: []*computepb.Backend{{Group: s(negRef), MaxRatePerEndpoint: proto.Float32(100)}},
	}))

	bs, err := client(t, e, compute.NewBackendServicesRESTClient).Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	require.NoError(t, err)
	assert.Equal(t, "RATE", bs.GetBackends()[0].GetBalancingMode())

	_, err = negs.Delete(t.Context(), &computepb.DeleteNetworkEndpointGroupRequest{Project: project, Zone: zone, NetworkEndpointGroup: "neg"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	e.vm(t, vmSpec("vm1"))

	op, err = negs.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsNetworkEndpointGroupRequest{
		Project: project, Zone: zone, NetworkEndpointGroup: "neg",
		NetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.NetworkEndpointGroupsAttachEndpointsRequest{
			NetworkEndpoints: []*computepb.NetworkEndpoint{{Instance: s("vm1")}},
		},
	})
	e.done(t, op, err)

	h, err := client(t, e, compute.NewBackendServicesRESTClient).GetHealth(t.Context(), &computepb.GetHealthBackendServiceRequest{
		Project: project, BackendService: "bs",
		ResourceGroupReferenceResource: &computepb.ResourceGroupReference{Group: s(negRef)},
	})
	require.NoError(t, err)
	require.Len(t, h.GetHealthStatus(), 1)
	assert.Equal(t, int32(8080), h.GetHealthStatus()[0].GetPort())
	assert.Equal(t, "HEALTHY", h.GetHealthStatus()[0].GetHealthState())

	code, body := rawDo(t, e, http.MethodPost,
		"/compute/v1/projects/"+project+"/zones/"+zone+"/networkEndpointGroups/neg/listNetworkEndpoints",
		`{"healthStatus":"SHOW"}`)
	require.Equal(t, http.StatusOK, code)

	var listed struct {
		Items []struct {
			Healths []struct {
				HealthState string `json:"healthState"`
			} `json:"healths"`
		} `json:"items"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &listed))
	require.Len(t, listed.Items, 1)
	require.Len(t, listed.Items[0].Healths, 1)
	assert.Equal(t, "HEALTHY", listed.Items[0].Healths[0].HealthState)
}
