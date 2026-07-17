package elbv2_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProtocolValidationPerLBType verifies protocol enforcement per load balancer type.
func TestProtocolValidationPerLBType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lbType     string
		protocol   string
		wantStatus int
	}{
		{name: "alb_http_ok", lbType: "application", protocol: "HTTP", wantStatus: http.StatusOK},
		{name: "alb_https_ok", lbType: "application", protocol: "HTTPS", wantStatus: http.StatusOK},
		{name: "alb_tcp_rejected", lbType: "application", protocol: "TCP", wantStatus: http.StatusBadRequest},
		{name: "alb_udp_rejected", lbType: "application", protocol: "UDP", wantStatus: http.StatusBadRequest},
		{name: "nlb_tcp_ok", lbType: "network", protocol: "TCP", wantStatus: http.StatusOK},
		{name: "nlb_udp_ok", lbType: "network", protocol: "UDP", wantStatus: http.StatusOK},
		{name: "nlb_tls_ok", lbType: "network", protocol: "TLS", wantStatus: http.StatusOK},
		{name: "nlb_http_rejected", lbType: "network", protocol: "HTTP", wantStatus: http.StatusBadRequest},
		{name: "nlb_https_rejected", lbType: "network", protocol: "HTTPS", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"proto-val-lb"},
				"Type":    {tt.lbType},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var lbResp struct {
				Result struct {
					LoadBalancers struct {
						Members []struct {
							LoadBalancerArn string `xml:"LoadBalancerArn"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"CreateLoadBalancerResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &lbResp))
			require.Len(t, lbResp.Result.LoadBalancers.Members, 1)
			lbArn := lbResp.Result.LoadBalancers.Members[0].LoadBalancerArn

			tgArn := mustCreateTG(t, h, "proto-val-tg-"+tt.name)

			listenerVals := url.Values{
				"Action":                                 {"CreateListener"},
				"Version":                                {"2015-12-01"},
				"LoadBalancerArn":                        {lbArn},
				"Protocol":                               {tt.protocol},
				"Port":                                   {"80"},
				"DefaultActions.member.1.Type":           {"forward"},
				"DefaultActions.member.1.TargetGroupArn": {tgArn},
			}

			// HTTPS requires at least one cert.
			if tt.protocol == "HTTPS" && tt.wantStatus == http.StatusOK {
				listenerVals["Certificates.member.1.CertificateArn"] = []string{
					"arn:aws:acm:us-east-1:123:certificate/abc",
				}
			}
			// TLS for NLB also requires cert.
			if tt.protocol == "TLS" && tt.wantStatus == http.StatusOK {
				listenerVals["Certificates.member.1.CertificateArn"] = []string{
					"arn:aws:acm:us-east-1:123:certificate/abc",
				}
			}

			rec2 := doELBv2(t, h, listenerVals)
			assert.Equal(t, tt.wantStatus, rec2.Code, "body: %s", rec2.Body.String())
		})
	}
}

// TestLBTypeValidation tests load balancer type validation.
func TestLBTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lbType     string
		wantStatus int
	}{
		{"type_application", "application", http.StatusOK},
		{"type_network", "network", http.StatusOK},
		{"type_gateway", "gateway", http.StatusOK},
		{"type_empty_defaults_to_application", "", http.StatusOK},
		{"type_invalid", "invalid-type", http.StatusBadRequest},
		{"type_classic", "classic", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {fmt.Sprintf("lb-type-%d", i)},
			}

			if tt.lbType != "" {
				vals.Set("Type", tt.lbType)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code, "type=%s", tt.lbType)
		})
	}
}

// TestNLBDNSFormat verifies that NLB uses the correct DNS format (elb before region).
func TestNLBDNSFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"my-nlb"},
		"Scheme":  {"internet-facing"},
		"Type":    {"network"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					DNSName               string `xml:"DNSName"`
					CanonicalHostedZoneID string `xml:"CanonicalHostedZoneId"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	lb := resp.Result.LoadBalancers.Members[0]
	// NLB DNS should be: {name}-{id}.elb.{region}.amazonaws.com
	assert.Contains(t, lb.DNSName, ".elb.us-east-1.amazonaws.com", "NLB DNS must include elb before region")
	// NLB hosted zone should differ from ALB.
	assert.NotEqual(t, "Z35SXDOTRQ7X7K", lb.CanonicalHostedZoneID, "NLB must not use ALB hosted zone ID")
}

// TestResourceNameValidation verifies naming rules for LBs and TGs.
func TestResourceNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lbName     string
		wantStatus int
	}{
		{"valid_alphanumeric", "validlb1", http.StatusOK},
		{"valid_with_hyphens", "valid-lb-1", http.StatusOK},
		{"invalid_starts_with_hyphen", "-badlb", http.StatusBadRequest},
		{"invalid_ends_with_hyphen", "badlb-", http.StatusBadRequest},
		{"invalid_special_chars", "bad@lb!", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {tt.lbName},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "name=%s", tt.lbName)
		})
	}
}

// TestModifyListenerProtocolValidated verifies that changing to an incompatible protocol fails.
func TestModifyListenerProtocolValidated(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Create an NLB.
	nlbRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"proto-nlb"},
		"Type":    {"network"},
	})
	require.Equal(t, http.StatusOK, nlbRec.Code)

	var nlbResp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(nlbRec.Body.Bytes(), &nlbResp))
	nlbArn := nlbResp.Result.LoadBalancers.Members[0].LoadBalancerArn

	// Create a TCP listener on the NLB.
	tgArn := mustCreateTG(t, h, "proto-nlb-tg")
	listRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {nlbArn},
		"Protocol":                               {"TCP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))
	listenerArn := listResp.Result.Listeners.Members[0].ListenerArn

	// Attempt to change to HTTP (invalid for NLB).
	rec := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
		"Protocol":    {"HTTP"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestNameTooLong verifies that LB and TG names longer than 32 chars are rejected.
func TestNameTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	longName := strings.Repeat("a", 33)

	t.Run("lb_name_too_long", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":  {"CreateLoadBalancer"},
			"Version": {"2015-12-01"},
			"Name":    {longName},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("tg_name_too_long", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":   {"CreateTargetGroup"},
			"Version":  {"2015-12-01"},
			"Name":     {longName},
			"Protocol": {"HTTP"},
			"Port":     {"80"},
			"VpcId":    {"vpc-00000000"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("exactly_32_chars_ok", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler()
		rec := doELBv2(t, h2, url.Values{
			"Action":  {"CreateLoadBalancer"},
			"Version": {"2015-12-01"},
			"Name":    {strings.Repeat("a", 32)},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestCreateLB_InvalidType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"bad-type-lb"},
		"Type":    {"invalid"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateLB_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	b1CreateLB(t, h, "dup-lb-batch1")
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"dup-lb-batch1"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNLB_DNS_Format(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"my-nlb"},
		"Type":    {"network"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					DNSName string `xml:"DNSName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	dns := resp.Result.LoadBalancers.Members[0].DNSName
	assert.Contains(t, dns, "my-nlb")
	assert.Contains(t, dns, "elb.")
}

func TestCreateLB_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"this-name-is-way-too-long-for-elb"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestLBName_Validation verifies that LB names reject underscores and
// require at least 2 characters, unlike target group names.
func TestLBName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lbName   string
		wantCode int
	}{
		{
			name:     "valid_name_accepted",
			lbName:   "my-lb",
			wantCode: http.StatusOK,
		},
		{
			name:     "underscore_rejected",
			lbName:   "my_lb",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "single_char_rejected",
			lbName:   "x",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "two_chars_accepted",
			lbName:   "lb",
			wantCode: http.StatusOK,
		},
		{
			name:     "leading_hyphen_rejected",
			lbName:   "-lb",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {tc.lbName},
				"Type":    {"application"},
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}
