package ociloadbalancer_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetCompartmentChecker(func(id string) bool { return id == testCompartment })

	tests := []struct {
		mutate func(d *loadbalancer.CreateLoadBalancerDetails)
		name   string
		code   string
		status int
	}{
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.ShapeDetails = nil },
			"flexible needs details",
			"MissingParameter",
			400,
		},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.ShapeDetails = &loadbalancer.ShapeDetails{
				MinimumBandwidthInMbps: new(50),
				MaximumBandwidthInMbps: new(10),
			}
		}, "min above max", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.ShapeDetails = &loadbalancer.ShapeDetails{
				MinimumBandwidthInMbps: new(10),
				MaximumBandwidthInMbps: new(9000),
			}
		}, "max above limit", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.ShapeDetails = &loadbalancer.ShapeDetails{MinimumBandwidthInMbps: new(5), MaximumBandwidthInMbps: new(10)}
		}, "min below limit", "InvalidParameter", 400},
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.ShapeName = new("100Mbps") },
			"fixed shape with details",
			"InvalidParameter",
			400,
		},
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.ShapeName = new("huge") },
			"unknown shape",
			"InvalidParameter",
			400,
		},
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.SubnetIds = []string{} },
			"no subnets",
			"MissingParameter",
			400,
		},
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.SubnetIds = []string{"a", "b", "c"} },
			"three subnets",
			"InvalidParameter",
			400,
		},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.IsPrivate = new(true)
			d.SubnetIds = []string{"a", "b"}
		}, "private two subnets", "InvalidParameter", 400},
		{
			func(d *loadbalancer.CreateLoadBalancerDetails) { d.CompartmentId = new(otherCompart) },
			"unknown compartment",
			"NotAuthorizedOrNotFound",
			404,
		},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.ReservedIps = []loadbalancer.ReservedIp{{Id: new("x")}}
		}, "reserved ips", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{
				"bad name": {Policy: new("ROUND_ROBIN"), HealthChecker: hc()},
			}
		}, "backend set name with space", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{
				"bs": {Policy: new("RANDOM"), HealthChecker: hc()},
			}
		}, "unknown policy", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": {Policy: new("ROUND_ROBIN"),
				HealthChecker: &loadbalancer.HealthCheckerDetails{Protocol: new("ICMP")}}}
		}, "unknown health protocol", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
				DefaultBackendSetName: new("missing"), Port: new(80), Protocol: new("HTTP"),
			}}
		}, "listener unknown backend set", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{
				"bs": {Policy: new("ROUND_ROBIN"), HealthChecker: hc()},
			}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
				DefaultBackendSetName: new("bs"), Port: new(80), Protocol: new("HTTP2"),
			}}
		}, "http2 needs tls", "InvalidParameter", 400},
		{func(d *loadbalancer.CreateLoadBalancerDetails) {
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{
				"bs": {Policy: new("ROUND_ROBIN"), HealthChecker: hc()},
			}
			d.Listeners = map[string]loadbalancer.ListenerDetails{
				"a": {DefaultBackendSetName: new("bs"), Port: new(80), Protocol: new("HTTP")},
				"b": {DefaultBackendSetName: new("bs"), Port: new(80), Protocol: new("HTTP")},
			}
		}, "duplicate listener port", "Conflict", 409},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := baseDetails(tt.name)
			tt.mutate(&d)

			_, err := e.lb.CreateLoadBalancer(
				t.Context(),
				loadbalancer.CreateLoadBalancerRequest{CreateLoadBalancerDetails: d},
			)
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestLifecycleAndWorkRequests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetSubnetResolver(func(id string) (string, bool) { return "10.0.1.0/24", id == testSubnet })

	d := baseDetails("lifecycle")
	d.IsPrivate = new(true)
	d.FreeformTags = map[string]string{"env": "test"}
	id, wrID := e.create(t, d)

	got := e.get(t, id)
	assert.Equal(t, loadbalancer.LoadBalancerLifecycleStateCreating, got.LifecycleState)
	require.Len(t, got.IpAddresses, 1)
	assert.Contains(t, *got.IpAddresses[0].IpAddress, "10.0.1.")
	assert.False(t, *got.IpAddresses[0].IsPublic)
	assert.NotNil(t, got.ETag)

	wr, err := e.lb.GetWorkRequest(t.Context(), loadbalancer.GetWorkRequestRequest{WorkRequestId: &wrID})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.WorkRequestLifecycleStateInProgress, wr.LifecycleState)
	assert.Equal(t, "CreateLoadBalancer", *wr.Type)
	assert.Equal(t, id, *wr.LoadBalancerId)

	_, err = e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
		LoadBalancerId: &id, DisplayName: new("x"),
	})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	e.settle()

	got = e.get(t, id)
	assert.Equal(t, loadbalancer.LoadBalancerLifecycleStateActive, got.LifecycleState)

	wr, err = e.lb.GetWorkRequest(t.Context(), loadbalancer.GetWorkRequestRequest{WorkRequestId: &wrID})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.WorkRequestLifecycleStateSucceeded, wr.LifecycleState)
	assert.NotNil(t, wr.TimeFinished)

	{ // update
		_, uerr := e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
			LoadBalancerId: &id, IfMatch: new("stale"),
			DisplayName: new("renamed"),
		})
		require.Error(t, uerr)
		assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, uerr).GetHTTPStatusCode())

		resp, uerr := e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
			LoadBalancerId: &id, IfMatch: got.ETag,
			DisplayName: new("renamed"), IsDeleteProtectionEnabled: new(true),
		})
		require.NoError(t, uerr)
		assert.NotEmpty(t, *resp.OpcWorkRequestId)

		_, uerr = e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
			LoadBalancerId: &id, DisplayName: new("again"),
		})
		assert.Equal(t, "Conflict", svcErr(t, uerr).GetCode())

		e.settle()

		after := e.get(t, id)
		assert.Equal(t, "renamed", *after.DisplayName)
		assert.NotEqual(t, *got.ETag, *after.ETag)
		assert.Equal(t, "test", after.FreeformTags["env"])
	}

	{ // delete protection then delete
		_, derr := e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{LoadBalancerId: &id})
		assert.Equal(t, http.StatusConflict, svcErr(t, derr).GetHTTPStatusCode())

		_, derr = e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
			LoadBalancerId: &id, IsDeleteProtectionEnabled: new(false),
		})
		require.NoError(t, derr)
		e.settle()

		_, derr = e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{LoadBalancerId: &id})
		require.NoError(t, derr)
		assert.Equal(t, loadbalancer.LoadBalancerLifecycleStateDeleting, e.get(t, id).LifecycleState)

		e.settle()
		assert.Equal(t, loadbalancer.LoadBalancerLifecycleStateDeleted, e.get(t, id).LifecycleState)

		list, lerr := e.lb.ListLoadBalancers(
			t.Context(),
			loadbalancer.ListLoadBalancersRequest{CompartmentId: new(testCompartment)},
		)
		require.NoError(t, lerr)
		assert.Empty(t, list.Items)

		list, lerr = e.lb.ListLoadBalancers(t.Context(), loadbalancer.ListLoadBalancersRequest{
			CompartmentId: new(testCompartment), LifecycleState: loadbalancer.LoadBalancerLifecycleStateDeleted,
		})
		require.NoError(t, lerr)
		assert.Len(t, list.Items, 1)

		wrs, lerr := e.lb.ListWorkRequests(t.Context(), loadbalancer.ListWorkRequestsRequest{LoadBalancerId: &id})
		require.NoError(t, lerr)
		assert.GreaterOrEqual(t, len(wrs.Items), 4)
	}
}

func TestListFiltersAndCatalogs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkLB(t, baseDetails("alpha"))
	e.mkLB(t, baseDetails("beta"))

	other := baseDetails("gamma")
	other.CompartmentId = new(otherCompart)
	e.mkLB(t, other)

	t.Run("by compartment", func(t *testing.T) {
		t.Parallel()

		resp, err := e.lb.ListLoadBalancers(t.Context(), loadbalancer.ListLoadBalancersRequest{
			CompartmentId: new(testCompartment), SortBy: loadbalancer.ListLoadBalancersSortByDisplayname,
			SortOrder: loadbalancer.ListLoadBalancersSortOrderAsc,
		})
		require.NoError(t, err)
		require.Len(t, resp.Items, 2)
		assert.Equal(t, "alpha", *resp.Items[0].DisplayName)
		assert.Equal(t, "beta", *resp.Items[1].DisplayName)
	})

	t.Run("by name and paging", func(t *testing.T) {
		t.Parallel()

		resp, err := e.lb.ListLoadBalancers(t.Context(), loadbalancer.ListLoadBalancersRequest{
			CompartmentId: new(testCompartment), DisplayName: new("beta"),
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 1)

		resp, err = e.lb.ListLoadBalancers(t.Context(), loadbalancer.ListLoadBalancersRequest{
			CompartmentId: new(testCompartment), Limit: new(int64(1)),
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 1)
		assert.NotNil(t, resp.OpcNextPage)
	})

	t.Run("missing compartment", func(t *testing.T) {
		t.Parallel()

		status, body := rawDo(t, e.base+"/20170115/loadBalancers")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, body, "MissingParameter")
	})

	t.Run("catalogs", func(t *testing.T) {
		t.Parallel()

		shapes, err := e.lb.ListShapes(t.Context(), loadbalancer.ListShapesRequest{CompartmentId: new(testCompartment)})
		require.NoError(t, err)

		names := make([]string, 0, len(shapes.Items))
		for _, s := range shapes.Items {
			names = append(names, *s.Name)
		}

		assert.Contains(t, names, "flexible")

		pol, err := e.lb.ListPolicies(
			t.Context(),
			loadbalancer.ListPoliciesRequest{CompartmentId: new(testCompartment)},
		)
		require.NoError(t, err)
		assert.Len(t, pol.Items, 3)

		proto, err := e.lb.ListProtocols(
			t.Context(),
			loadbalancer.ListProtocolsRequest{CompartmentId: new(testCompartment)},
		)
		require.NoError(t, err)
		assert.Len(t, proto.Items, 3)
	})

	t.Run("unknown load balancer", func(t *testing.T) {
		t.Parallel()

		_, err := e.lb.GetLoadBalancer(t.Context(), loadbalancer.GetLoadBalancerRequest{LoadBalancerId: new("nope")})
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	})
}

func TestShapeAndNSGUpdates(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := e.mkLB(t, baseDetails("shape"))

	tests := []struct {
		details loadbalancer.UpdateLoadBalancerShapeDetails
		name    string
		wantErr bool
	}{
		{name: "widen flexible", details: loadbalancer.UpdateLoadBalancerShapeDetails{
			ShapeName:    new("flexible"),
			ShapeDetails: &loadbalancer.ShapeDetails{MinimumBandwidthInMbps: new(20), MaximumBandwidthInMbps: new(500)},
		}},
		{name: "bad flexible", wantErr: true, details: loadbalancer.UpdateLoadBalancerShapeDetails{
			ShapeName:    new("flexible"),
			ShapeDetails: &loadbalancer.ShapeDetails{MinimumBandwidthInMbps: new(20), MaximumBandwidthInMbps: new(10)},
		}},
		{name: "fixed shape", details: loadbalancer.UpdateLoadBalancerShapeDetails{ShapeName: new("400Mbps")}},
	}

	for _, tt := range tests {
		e.settle()

		_, err := e.lb.UpdateLoadBalancerShape(t.Context(), loadbalancer.UpdateLoadBalancerShapeRequest{
			LoadBalancerId: &id, UpdateLoadBalancerShapeDetails: tt.details,
		})
		if tt.wantErr {
			require.Error(t, err, tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		e.settle()
		assert.Equal(t, *tt.details.ShapeName, *e.get(t, id).ShapeName, tt.name)
	}

	e.settle()

	_, err := e.lb.UpdateNetworkSecurityGroups(t.Context(), loadbalancer.UpdateNetworkSecurityGroupsRequest{
		LoadBalancerId:          &id,
		NetworkSecurityGroupIds: []string{"nsg1"},
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, []string{"nsg1"}, e.get(t, id).NetworkSecurityGroupIds)

	e.settle()

	_, err = e.lb.ChangeLoadBalancerCompartment(t.Context(), loadbalancer.ChangeLoadBalancerCompartmentRequest{
		LoadBalancerId: &id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)
	e.settle()
	assert.Equal(t, otherCompart, *e.get(t, id).CompartmentId)
}

func TestAuthEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opts     envOpts
		wantCode int
	}{
		{"registered key accepted", envOpts{enforce: true, register: true}, http.StatusOK},
		{"unregistered key rejected", envOpts{enforce: true}, http.StatusUnauthorized},
		{"enforcement off accepts any", envOpts{}, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)
			_, err := e.lb.ListLoadBalancers(
				t.Context(),
				loadbalancer.ListLoadBalancersRequest{CompartmentId: new(testCompartment)},
			)

			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	t.Run("unsigned", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{enforce: true, register: true})
		status, body := rawDo(t, e.base+"/20170115/loadBalancers?compartmentId=x")
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Contains(t, body, "NotAuthenticated")
	})

	t.Run("unknown route", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		status, body := rawDo(t, e.base+"/20170115/nothing")
		assert.Equal(t, http.StatusNotFound, status)
		assert.Contains(t, body, "NotFound")
	})
}
