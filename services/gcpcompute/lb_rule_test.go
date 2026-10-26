package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalForwardingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule   *computepb.ForwardingRule
		check  func(t *testing.T, e *env, r *computepb.ForwardingRule)
		name   string
		status int
	}{
		{
			name: "ephemeral address", rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("80")},
			check: func(t *testing.T, _ *env, r *computepb.ForwardingRule) {
				t.Helper()
				assert.Equal(t, "80-80", r.GetPortRange())
				assert.Equal(t, "TCP", r.GetIPProtocol())
				assert.Equal(t, "EXTERNAL", r.GetLoadBalancingScheme())
				assert.Regexp(t, `^34\.120\.`, r.GetIPAddress())
				assert.Contains(t, r.GetTarget(), "/targetHttpProxies/px")
			},
		},
		{
			name: "reserved address by name", rule: &computepb.ForwardingRule{
				Target: s(proxyRef), PortRange: s("8080"), IPAddress: s(gl + "addresses/vip"),
			},
			check: func(t *testing.T, e *env, r *computepb.ForwardingRule) {
				t.Helper()

				a, err := e.globalAddresses(t).Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "vip"})
				require.NoError(t, err)
				assert.Equal(t, a.GetAddress(), r.GetIPAddress())
				assert.Equal(t, "IN_USE", a.GetStatus())
				assert.Contains(t, a.GetUsers()[0], "/forwardingRules/fr")
			},
		},
		{
			name: "no port", rule: &computepb.ForwardingRule{Target: s(proxyRef)}, status: http.StatusBadRequest,
		},
		{
			name: "http proxy on 443", rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("443")}, status: http.StatusBadRequest,
		},
		{
			name: "target and backend service", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(proxyRef), BackendService: s(bsRef), PortRange: s("80")},
		},
		{
			name: "missing proxy", status: http.StatusNotFound,
			rule: &computepb.ForwardingRule{Target: s(gl + "targetHttpProxies/none"), PortRange: s("80")},
		},
		{
			name: "target pool unsupported", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(regBase + "targetPools/tp"), PortRange: s("80")},
		},
		{
			name: "udp with proxy", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("80"), IPProtocol: s("UDP")},
		},
		{
			name: "scheme mismatch with backend", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("80"), LoadBalancingScheme: s("EXTERNAL_MANAGED")},
		},
		{
			name: "unreserved ip", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("80"), IPAddress: s("203.0.113.9")},
		},
		{
			name: "internal scheme is regional", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{Target: s(proxyRef), PortRange: s("80"), LoadBalancingScheme: s("INTERNAL")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.lbStack(t)
			require.NoError(t, e.lbProxy(t, "px", umRef))

			op, err := e.globalAddresses(t).Insert(t.Context(), &computepb.InsertGlobalAddressRequest{
				Project: project, AddressResource: &computepb.Address{Name: s("vip")},
			})
			e.done(t, op, err)

			tt.rule.Name = s("fr")
			err = e.lbRule(t, tt.rule)

			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)

			got, gerr := client(t, e, compute.NewGlobalForwardingRulesRESTClient).Get(t.Context(),
				&computepb.GetGlobalForwardingRuleRequest{Project: project, ForwardingRule: "fr"})
			require.NoError(t, gerr)
			tt.check(t, e, got)
		})
	}
}

func TestForwardingRuleConflictsAndMutations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.lbStack(t)
	require.NoError(t, e.lbProxy(t, "px", umRef))
	require.NoError(t, e.lbProxy(t, "px2", umRef))

	op, err := e.globalAddresses(t).Insert(t.Context(), &computepb.InsertGlobalAddressRequest{
		Project: project, AddressResource: &computepb.Address{Name: s("vip")},
	})
	e.done(t, op, err)

	vip := s(gl + "addresses/vip")
	require.NoError(t, e.lbRule(t, &computepb.ForwardingRule{Name: s("a"), Target: s(proxyRef), PortRange: s("80"), IPAddress: vip}))

	t.Run("same ip and port conflicts", func(t *testing.T) {
		assertAPIError(t, e.lbRule(t, &computepb.ForwardingRule{
			Name: s("b"), Target: s(gl + "targetHttpProxies/px2"), PortRange: s("80"), IPAddress: vip,
		}), http.StatusBadRequest, "invalid")
	})

	t.Run("same ip different port shares", func(t *testing.T) {
		require.NoError(t, e.lbRule(t, &computepb.ForwardingRule{
			Name: s("c"), Target: s(gl + "targetHttpProxies/px2"), PortRange: s("8080"), IPAddress: vip,
		}))
	})

	rules := client(t, e, compute.NewGlobalForwardingRulesRESTClient)

	t.Run("set target", func(t *testing.T) {
		op, err := rules.SetTarget(t.Context(), &computepb.SetTargetGlobalForwardingRuleRequest{
			Project: project, ForwardingRule: "a",
			TargetReferenceResource: &computepb.TargetReference{Target: s(gl + "targetHttpProxies/px2")},
		})
		e.done(t, op, err)

		got, err := rules.Get(t.Context(), &computepb.GetGlobalForwardingRuleRequest{Project: project, ForwardingRule: "a"})
		require.NoError(t, err)
		assert.Contains(t, got.GetTarget(), "/px2")

		_, err = rules.SetTarget(t.Context(), &computepb.SetTargetGlobalForwardingRuleRequest{
			Project: project, ForwardingRule: "a",
			TargetReferenceResource: &computepb.TargetReference{Target: s(gl + "targetHttpProxies/none")},
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})

	t.Run("address held while in use", func(t *testing.T) {
		_, err := e.globalAddresses(t).Delete(t.Context(), &computepb.DeleteGlobalAddressRequest{Project: project, Address: "vip"})
		assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
	})
}

func TestRegionalForwardingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule   *computepb.ForwardingRule
		check  func(t *testing.T, r *computepb.ForwardingRule)
		name   string
		status int
	}{
		{
			name: "internal passthrough", rule: &computepb.ForwardingRule{
				LoadBalancingScheme: s("INTERNAL"), Subnetwork: s(subnetRef), Ports: []string{"80", "443"},
			},
			check: func(t *testing.T, r *computepb.ForwardingRule) {
				t.Helper()
				assert.Equal(t, "10.10.0.2", r.GetIPAddress())
				assert.Contains(t, r.GetNetwork(), "/networks/vpc")
			},
		},
		{
			name: "external passthrough port range", rule: &computepb.ForwardingRule{PortRange: s("1000-2000")},
			check: func(t *testing.T, r *computepb.ForwardingRule) {
				t.Helper()
				assert.Regexp(t, `^34\.1\d\d\.`, r.GetIPAddress())
			},
		},
		{
			name: "internal needs subnetwork", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{LoadBalancingScheme: s("INTERNAL"), AllPorts: new(true)},
		},
		{
			name: "internal rejects portRange", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{LoadBalancingScheme: s("INTERNAL"), Subnetwork: s(subnetRef), PortRange: s("80")},
		},
		{
			name: "too many ports", status: http.StatusBadRequest,
			rule: &computepb.ForwardingRule{
				LoadBalancingScheme: s("INTERNAL"), Subnetwork: s(subnetRef), Ports: []string{"1", "2", "3", "4", "5", "6"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			rhc := client(t, e, compute.NewRegionHealthChecksRESTClient)
			rbs := client(t, e, compute.NewRegionBackendServicesRESTClient)
			rules := client(t, e, compute.NewForwardingRulesRESTClient)

			op, err := rhc.Insert(t.Context(), &computepb.InsertRegionHealthCheckRequest{
				Project: project, Region: region,
				HealthCheckResource: &computepb.HealthCheck{Name: s("rhc"), TcpHealthCheck: &computepb.TCPHealthCheck{}},
			})
			e.done(t, op, err)

			scheme := "EXTERNAL"
			if tt.rule.GetLoadBalancingScheme() == "INTERNAL" {
				scheme = "INTERNAL"
			}

			op, err = rbs.Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
				Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
					Name: s("rbs"), Protocol: s("TCP"), LoadBalancingScheme: s(scheme),
					HealthChecks: []string{regBase + "healthChecks/rhc"},
				},
			})
			e.done(t, op, err)

			tt.rule.Name, tt.rule.BackendService = s("rfr"), s(regBase+"backendServices/rbs")

			op, err = rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
				Project: project, Region: region, ForwardingRuleResource: tt.rule,
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := rules.Get(t.Context(), &computepb.GetForwardingRuleRequest{Project: project, Region: region, ForwardingRule: "rfr"})
			require.NoError(t, gerr)
			assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/forwardingRules/rfr")
			tt.check(t, got)
		})
	}
}
