package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) externalRuleStack(t *testing.T) {
	t.Helper()
	e.baseNet(t)

	rhc := client(t, e, compute.NewRegionHealthChecksRESTClient)
	op, err := rhc.Insert(t.Context(), &computepb.InsertRegionHealthCheckRequest{
		Project: project, Region: region,
		HealthCheckResource: &computepb.HealthCheck{Name: s("rhc"), TcpHealthCheck: &computepb.TCPHealthCheck{}},
	})
	e.done(t, op, err)

	rbs := client(t, e, compute.NewRegionBackendServicesRESTClient)
	op, err = rbs.Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
		Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
			Name: s("rbs"), Protocol: s("TCP"), LoadBalancingScheme: s("EXTERNAL"),
			HealthChecks: []string{regBase + "healthChecks/rhc"},
		},
	})
	e.done(t, op, err)
}

func TestForwardingRuleTierFollowsAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		addrTier  string
		ruleTier  string
		wantTier  string
		wantError int
	}{
		{name: "no address defaults to premium", wantTier: "PREMIUM"},
		{name: "standard address", addrTier: "STANDARD", wantTier: "STANDARD"},
		{name: "premium address", addrTier: "PREMIUM", wantTier: "PREMIUM"},
		{name: "matching explicit tier", addrTier: "STANDARD", ruleTier: "STANDARD", wantTier: "STANDARD"},
		{
			name: "premium rule on standard address", addrTier: "STANDARD", ruleTier: "PREMIUM",
			wantError: http.StatusBadRequest,
		},
		{
			name: "standard rule on premium address", addrTier: "PREMIUM", ruleTier: "STANDARD",
			wantError: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.externalRuleStack(t)

			rule := &computepb.ForwardingRule{
				Name: s("rfr"), BackendService: s(regBase + "backendServices/rbs"), PortRange: s("80"),
			}

			if tt.addrTier != "" {
				op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
					Project: project, Region: region,
					AddressResource: &computepb.Address{Name: s("vip"), NetworkTier: s(tt.addrTier)},
				})
				e.done(t, op, err)

				rule.IPAddress = s(regBase + "addresses/vip")
			}

			if tt.ruleTier != "" {
				rule.NetworkTier = s(tt.ruleTier)
			}

			rules := client(t, e, compute.NewForwardingRulesRESTClient)
			op, err := rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
				Project: project, Region: region, ForwardingRuleResource: rule,
			})

			if tt.wantError != 0 {
				assertAPIError(t, err, tt.wantError, "")

				return
			}

			e.done(t, op, err)

			got, gerr := rules.Get(t.Context(),
				&computepb.GetForwardingRuleRequest{Project: project, Region: region, ForwardingRule: "rfr"})
			require.NoError(t, gerr)
			assert.Equal(t, tt.wantTier, got.GetNetworkTier())
		})
	}
}

func TestForwardingRulePatchTierMustMatchAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		patchTier string
		wantError int
	}{
		{name: "same tier", patchTier: "STANDARD"},
		{name: "other tier", patchTier: "PREMIUM", wantError: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.externalRuleStack(t)

			op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
				Project: project, Region: region,
				AddressResource: &computepb.Address{Name: s("vip"), NetworkTier: s("STANDARD")},
			})
			e.done(t, op, err)

			rules := client(t, e, compute.NewForwardingRulesRESTClient)
			op, err = rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
				Project: project, Region: region, ForwardingRuleResource: &computepb.ForwardingRule{
					Name: s("rfr"), BackendService: s(regBase + "backendServices/rbs"), PortRange: s("80"),
					IPAddress: s(regBase + "addresses/vip"),
				},
			})
			e.done(t, op, err)

			op, err = rules.Patch(t.Context(), &computepb.PatchForwardingRuleRequest{
				Project: project, Region: region, ForwardingRule: "rfr",
				ForwardingRuleResource: &computepb.ForwardingRule{NetworkTier: s(tt.patchTier)},
			})

			if tt.wantError != 0 {
				assertAPIError(t, err, tt.wantError, "")

				return
			}

			e.done(t, op, err)
		})
	}
}
