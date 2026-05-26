package elb_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// --- DetachLoadBalancerFromSubnets ---

func TestDetachLoadBalancerFromSubnets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "detaches_existing_subnet",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"detach-lb"},
					"Subnets.member.1":                    {"subnet-aaa"},
					"Subnets.member.2":                    {"subnet-bbb"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			vals: url.Values{
				"Action":           {"DetachLoadBalancerFromSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"detach-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":           {"DetachLoadBalancerFromSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_name_returns_400",
			vals: url.Values{
				"Action":  {"DetachLoadBalancerFromSubnets"},
				"Version": {"2012-06-01"},
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

			if tt.wantStatus == http.StatusOK && tt.wantCount > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DetachLoadBalancerFromSubnetsResponse"`
					Result  struct {
						Subnets struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"Subnets"`
					} `xml:"DetachLoadBalancerFromSubnetsResult"`
				}

				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.Subnets.Members, tt.wantCount)
			}
		})
	}
}

// --- EnableAvailabilityZonesForLoadBalancer ---

func TestEnableAvailabilityZonesForLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantAZs    []string
		wantStatus int
	}{
		{
			name: "adds_new_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "enable-az-lb")
			},
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"enable-az-lb"},
				"AvailabilityZones.member.1": {"us-east-1b"},
			},
			wantStatus: http.StatusOK,
			wantAZs:    []string{"us-east-1a", "us-east-1b"},
		},
		{
			name: "idempotent_for_existing_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "enable-az-idem")
			},
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"enable-az-idem"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
			wantAZs:    []string{"us-east-1a"},
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"no-lb"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantStatus == http.StatusOK && len(tt.wantAZs) > 0 {
				var resp struct {
					XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
					Result  struct {
						AvailabilityZones struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"AvailabilityZones"`
					} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
				}

				parseXMLBody(t, rec, &resp)
				gotAZs := make([]string, 0, len(resp.Result.AvailabilityZones.Members))
				for _, m := range resp.Result.AvailabilityZones.Members {
					gotAZs = append(gotAZs, m.Value)
				}

				for _, az := range tt.wantAZs {
					assert.Contains(t, gotAZs, az)
				}
			}
		})
	}
}

// --- DisableAvailabilityZonesForLoadBalancer ---

func TestDisableAvailabilityZonesForLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "removes_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"disable-az-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"AvailabilityZones.member.2":          {"us-east-1b"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			vals: url.Values{
				"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"disable-az-lb"},
				"AvailabilityZones.member.1": {"us-east-1b"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"no-lb"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantStatus == http.StatusOK && tt.wantCount > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
					Result  struct {
						AvailabilityZones struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"AvailabilityZones"`
					} `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
				}

				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.AvailabilityZones.Members, tt.wantCount)
			}
		})
	}
}

// --- SetLoadBalancerListenerSSLCertificate ---

func TestSetLoadBalancerListenerSSLCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantCertID string
		wantStatus int
	}{
		{
			name: "sets_ssl_certificate",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				rec := doELB(t, h, url.Values{
					"Action":                                        {"CreateLoadBalancer"},
					"Version":                                       {"2012-06-01"},
					"LoadBalancerName":                              {"ssl-lb"},
					"AvailabilityZones.member.1":                    {"us-east-1a"},
					"Listeners.member.1.Protocol":                   {"HTTPS"},
					"Listeners.member.1.LoadBalancerPort":           {"443"},
					"Listeners.member.1.InstancePort":               {"8443"},
					"Listeners.member.1.SSLCertificateId":           {"arn:aws:acm:us-east-1:123456789012:certificate/initial"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusOK,
			wantCertID: "arn:aws:acm:us-east-1:123456789012:certificate/abc-123",
		},
		{
			name: "missing_lb_name_returns_400",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_port_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-missing-port")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-missing-port"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_cert_id_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-missing-cert")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-missing-cert"},
				"LoadBalancerPort": {"80"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "port_not_found_returns_404",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-bad-port")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-bad-port"},
				"LoadBalancerPort": {"9999"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "lb_not_found_returns_404",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantStatus == http.StatusOK && tt.wantCertID != "" {
				lbName := tt.vals.Get("LoadBalancerName")
				lbs, err := h.Backend.DescribeLoadBalancers([]string{lbName})
				require.NoError(t, err)
				require.Len(t, lbs, 1)

				found := false
				for _, l := range lbs[0].Listeners {
					if l.SSLCertificateID == tt.wantCertID {
						found = true
					}
				}

				assert.True(t, found, "SSL certificate ID not set on listener")
			}
		})
	}
}

// --- SetLoadBalancerPoliciesOfListener ---

func TestSetLoadBalancerPoliciesOfListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elb.Handler)
		vals         url.Values
		name         string
		wantPolicies []string
		wantStatus   int
	}{
		{
			name: "sets_policies_on_listener",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lpol-lb")

				doELB(t, h, url.Values{
					"Action":           {"CreateAppCookieStickinessPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"lpol-lb"},
					"PolicyName":       {"sticky-pol"},
					"CookieName":       {"SID"},
				})
			},
			vals: url.Values{
				"Action":               {"SetLoadBalancerPoliciesOfListener"},
				"Version":              {"2012-06-01"},
				"LoadBalancerName":     {"lpol-lb"},
				"LoadBalancerPort":     {"80"},
				"PolicyNames.member.1": {"sticky-pol"},
			},
			wantStatus:   http.StatusOK,
			wantPolicies: []string{"sticky-pol"},
		},
		{
			name: "clears_policies_with_empty_list",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lpol-clear")

				doELB(t, h, url.Values{
					"Action":               {"SetLoadBalancerPoliciesOfListener"},
					"Version":              {"2012-06-01"},
					"LoadBalancerName":     {"lpol-clear"},
					"LoadBalancerPort":     {"80"},
					"PolicyNames.member.1": {"pol-a"},
				})
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesOfListener"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lpol-clear"},
				"LoadBalancerPort": {"80"},
			},
			wantStatus:   http.StatusOK,
			wantPolicies: []string{},
		},
		{
			name: "missing_lb_name_returns_400",
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesOfListener"},
				"Version":          {"2012-06-01"},
				"LoadBalancerPort": {"80"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_port_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lpol-noportlb")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesOfListener"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lpol-noportlb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "listener_not_found_returns_404",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lpol-noport")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerPoliciesOfListener"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lpol-noport"},
				"LoadBalancerPort": {"9999"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantStatus == http.StatusOK {
				lbName := tt.vals.Get("LoadBalancerName")
				lbs, err := h.Backend.DescribeLoadBalancers([]string{lbName})
				require.NoError(t, err)
				require.Len(t, lbs, 1)

				var gotPolicies []string
				for _, l := range lbs[0].Listeners {
					gotPolicies = l.PolicyNames
				}

				if gotPolicies == nil {
					gotPolicies = []string{}
				}

				assert.Equal(t, tt.wantPolicies, gotPolicies)
			}
		})
	}
}

// --- SetLoadBalancerPoliciesForBackendServer ---

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
			wantStatus: http.StatusNotFound,
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
				lbs, err := h.Backend.DescribeLoadBalancers([]string{lbName})
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
