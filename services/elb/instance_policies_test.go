package elb_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

func TestBackendServerPoliciesSetAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "bsd-desc-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-desc-lb"},
		"PolicyName":       {"proxy-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-desc-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"proxy-pol"},
	})

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"bsd-desc-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)
	require.Len(t, lbs[0].BackendServerDescriptions, 1)
	assert.Equal(t, int32(8080), lbs[0].BackendServerDescriptions[0].InstancePort)
	assert.Equal(t, []string{"proxy-pol"}, lbs[0].BackendServerDescriptions[0].PolicyNames)
}

func TestBackendServerPoliciesClearByEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "bsd-clear-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-clear-lb"},
		"PolicyName":       {"proxy-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-clear-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"proxy-pol"},
	})

	// Clear policies by setting empty list.
	doELB(t, h, url.Values{
		"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-clear-lb"},
		"InstancePort":     {"8080"},
	})

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"bsd-clear-lb"})
	require.NoError(t, err)
	bsd := lbs[0].BackendServerDescriptions
	// Either removed or has empty policy list.
	for _, d := range bsd {
		if d.InstancePort == 8080 {
			assert.Empty(t, d.PolicyNames)
		}
	}
}

func TestBackendServerPoliciesUnknownPolicyRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "bsd-unknown-lb")

	rec := doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-unknown-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"no-such-policy"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// describeBSDs is a helper that calls DescribeLoadBalancers and extracts the
// BackendServerDescriptions list.
func describeBSDs(t *testing.T, h *elb.Handler, lbName string) []struct {
	PolicyNames  string `xml:"PolicyNames"`
	InstancePort int32  `xml:"InstancePort"`
} {
	t.Helper()

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {lbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LBs struct {
				Members []struct {
					BSDs struct {
						Members []struct {
							PolicyNames  string `xml:"PolicyNames"`
							InstancePort int32  `xml:"InstancePort"`
						} `xml:"member"`
					} `xml:"BackendServerDescriptions"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LBs.Members, 1)

	return resp.Result.LBs.Members[0].BSDs.Members
}

// TestBSDCleanupOnEmptyPolicies verifies that a BackendServerDescription
// entry is removed when all policies are cleared from that backend port.
func TestBSDCleanupOnEmptyPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "bsd-lb")

	// Create a proxy-protocol policy.
	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-lb"},
		"PolicyName":       {"pp-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	// Attach policy to backend port 8080.
	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"pp-pol"},
	})

	// Verify BSD entry is present.
	bsdsAfterAdd := describeBSDs(t, h, "bsd-lb")
	require.Len(t, bsdsAfterAdd, 1, "BSD entry should exist after policy attach")

	// Clear all policies from backend port 8080 (empty list).
	doELB(t, h, url.Values{
		"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-lb"},
		"InstancePort":     {"8080"},
	})

	// BSD entry must be removed.
	bsdsAfterClear := describeBSDs(t, h, "bsd-lb")
	assert.Empty(t, bsdsAfterClear, "BSD entry should be removed when no policies remain")
}

func TestSetLoadBalancerPoliciesForBackendServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elb.Handler)
		vals         url.Values
		name         string
		wantPolicies []string
		wantStatus   int
		wantPort     int32
	}{
		{
			name: "sets_policies_on_backend_server",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "bsd-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"bsd-lb"},
					"PolicyName":       {"proxy-pol"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
			},
			vals: url.Values{
				"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
				"Version":              {"2012-06-01"},
				"LoadBalancerName":     {"bsd-lb"},
				"InstancePort":         {"8080"},
				"PolicyNames.member.1": {"proxy-pol"},
			},
			wantStatus:   http.StatusOK,
			wantPort:     8080,
			wantPolicies: []string{"proxy-pol"},
		},
		{
			name: "updates_existing_backend_server_policies",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "bsd-update-lb")

				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"bsd-update-lb"},
					"PolicyName":       {"old-pol"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"bsd-update-lb"},
					"PolicyName":       {"new-pol"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
				doELB(t, h, url.Values{
					"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
					"Version":              {"2012-06-01"},
					"LoadBalancerName":     {"bsd-update-lb"},
					"InstancePort":         {"8080"},
					"PolicyNames.member.1": {"old-pol"},
				})
			},
			vals: url.Values{
				"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
				"Version":              {"2012-06-01"},
				"LoadBalancerName":     {"bsd-update-lb"},
				"InstancePort":         {"8080"},
				"PolicyNames.member.1": {"new-pol"},
			},
			wantStatus:   http.StatusOK,
			wantPort:     8080,
			wantPolicies: []string{"new-pol"},
		},
		{
			name: "missing_lb_name_returns_400",
			vals: url.Values{
				"Action":       {"SetLoadBalancerPoliciesForBackendServer"},
				"Version":      {"2012-06-01"},
				"InstancePort": {"8080"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_instance_port_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "bsd-noport-lb")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"bsd-noport-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "lb_not_found_returns_404",
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"InstancePort":     {"8080"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantPort > 0 {
				lbName := tt.vals.Get("LoadBalancerName")
				lbs, err := h.Backend.DescribeLoadBalancers(context.Background(), []string{lbName})
				require.NoError(t, err)
				require.Len(t, lbs, 1)

				var gotPolicies []string
				for _, bsd := range lbs[0].BackendServerDescriptions {
					if bsd.InstancePort == tt.wantPort {
						gotPolicies = bsd.PolicyNames
					}
				}

				assert.Equal(t, tt.wantPolicies, gotPolicies)
			}
		})
	}
}
