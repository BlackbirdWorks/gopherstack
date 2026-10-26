package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lbChain builds hc, ig, bs, um, px, cert and policy and a forwarding rule on top.
func lbChain(t *testing.T, e *env) {
	t.Helper()
	e.lbStack(t)
	e.lbCert(t, "cert")
	require.NoError(t, e.lbProxy(t, "px", umRef))

	op, err := client(t, e, compute.NewSslPoliciesRESTClient).Insert(t.Context(), &computepb.InsertSslPolicyRequest{
		Project: project, SslPolicyResource: &computepb.SslPolicy{Name: s("pol")},
	})
	e.done(t, op, err)

	op, err = client(
		t,
		e,
		compute.NewTargetHttpsProxiesRESTClient,
	).Insert(t.Context(), &computepb.InsertTargetHttpsProxyRequest{
		Project: project, TargetHttpsProxyResource: &computepb.TargetHttpsProxy{
			Name: s("spx"), UrlMap: s(umRef), SslCertificates: []string{certRef}, SslPolicy: s(gl + "sslPolicies/pol"),
		},
	})
	e.done(t, op, err)

	require.NoError(t, e.lbRule(t, &computepb.ForwardingRule{Name: s("fr"), Target: s(proxyRef), PortRange: s("80")}))
}

func lbDeleters(t *testing.T, e *env) map[string]func() (*compute.Operation, error) {
	t.Helper()

	del := map[string]func() (*compute.Operation, error){
		"healthCheck": func() (*compute.Operation, error) {
			return e.healthChecks(t).
				Delete(t.Context(), &computepb.DeleteHealthCheckRequest{Project: project, HealthCheck: "hc"})
		},
		"instanceGroup": func() (*compute.Operation, error) {
			return e.groups(t).
				Delete(t.Context(), &computepb.DeleteInstanceGroupRequest{Project: project, Zone: zone, InstanceGroup: "ig"})
		},
		"backendService": func() (*compute.Operation, error) {
			return client(
				t,
				e,
				compute.NewBackendServicesRESTClient,
			).Delete(t.Context(), &computepb.DeleteBackendServiceRequest{Project: project, BackendService: "bs"})
		},
		"urlMap": func() (*compute.Operation, error) {
			return client(
				t,
				e,
				compute.NewUrlMapsRESTClient,
			).Delete(t.Context(), &computepb.DeleteUrlMapRequest{Project: project, UrlMap: "um"})
		},
		"httpProxy": func() (*compute.Operation, error) {
			return client(
				t,
				e,
				compute.NewTargetHttpProxiesRESTClient,
			).Delete(t.Context(), &computepb.DeleteTargetHttpProxyRequest{Project: project, TargetHttpProxy: "px"})
		},
		"cert": func() (*compute.Operation, error) {
			return client(
				t,
				e,
				compute.NewSslCertificatesRESTClient,
			).Delete(t.Context(), &computepb.DeleteSslCertificateRequest{Project: project, SslCertificate: "cert"})
		},
		"policy": func() (*compute.Operation, error) {
			return client(
				t,
				e,
				compute.NewSslPoliciesRESTClient,
			).Delete(t.Context(), &computepb.DeleteSslPolicyRequest{Project: project, SslPolicy: "pol"})
		},
	}

	rules := client(t, e, compute.NewGlobalForwardingRulesRESTClient)
	https := client(t, e, compute.NewTargetHttpsProxiesRESTClient)

	del["rule"] = func() (*compute.Operation, error) {
		return rules.Delete(
			t.Context(),
			&computepb.DeleteGlobalForwardingRuleRequest{Project: project, ForwardingRule: "fr"},
		)
	}
	del["httpsProxy"] = func() (*compute.Operation, error) {
		return https.Delete(
			t.Context(),
			&computepb.DeleteTargetHttpsProxyRequest{Project: project, TargetHttpsProxy: "spx"},
		)
	}

	return del
}

func TestLoadBalancerInUseProtection(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	lbChain(t, e)

	for _, name := range []string{
		"healthCheck", "instanceGroup", "backendService", "urlMap", "httpProxy", "cert", "policy",
	} {
		fn := lbDeleters(t, e)[name]

		t.Run("blocked "+name, func(t *testing.T) {
			t.Parallel()

			_, err := fn()
			assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
		})
	}
}

func TestLoadBalancerDeleteOrder(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	lbChain(t, e)

	del := lbDeleters(t, e)

	order := []string{
		"rule",
		"httpsProxy",
		"policy",
		"cert",
		"httpProxy",
		"urlMap",
		"backendService",
		"healthCheck",
		"instanceGroup",
	}
	for _, name := range order {
		op, err := del[name]()
		e.done(t, op, err)
	}

	_, err := client(t, e, compute.NewBackendServicesRESTClient).
		Get(t.Context(), &computepb.GetBackendServiceRequest{Project: project, BackendService: "bs"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestLoadBalancerSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.baseNet(t)
	lbChain(t, src)

	negs := client(t, src, compute.NewNetworkEndpointGroupsRESTClient)
	op, err := negs.Insert(t.Context(), &computepb.InsertNetworkEndpointGroupRequest{
		Project: project, Zone: zone, NetworkEndpointGroupResource: zonalNEG(),
	})
	src.done(t, op, err)
	src.vm(t, vmSpec("vm1"))

	op, err = negs.AttachNetworkEndpoints(t.Context(), &computepb.AttachNetworkEndpointsNetworkEndpointGroupRequest{
		Project: project, Zone: zone, NetworkEndpointGroup: "neg",
		NetworkEndpointGroupsAttachEndpointsRequestResource: &computepb.NetworkEndpointGroupsAttachEndpointsRequest{
			NetworkEndpoints: []*computepb.NetworkEndpoint{{Instance: s("vm1")}},
		},
	})
	src.done(t, op, err)

	before, err := client(t, src, compute.NewGlobalForwardingRulesRESTClient).
		Get(t.Context(), &computepb.GetGlobalForwardingRuleRequest{Project: project, ForwardingRule: "fr"})
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	after, err := client(t, dst, compute.NewGlobalForwardingRulesRESTClient).
		Get(t.Context(), &computepb.GetGlobalForwardingRuleRequest{Project: project, ForwardingRule: "fr"})
	require.NoError(t, err)
	assert.Equal(t, before.GetIPAddress(), after.GetIPAddress())
	assert.Equal(t, before.GetId(), after.GetId())

	assert.Len(t, listNEGEndpoints(t, dst), 1)

	_, err = client(t, dst, compute.NewTargetHttpsProxiesRESTClient).
		Get(t.Context(), &computepb.GetTargetHttpsProxyRequest{Project: project, TargetHttpsProxy: "spx"})
	require.NoError(t, err)

	_, err = client(t, dst, compute.NewBackendServicesRESTClient).Delete(t.Context(),
		&computepb.DeleteBackendServiceRequest{Project: project, BackendService: "bs"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}
