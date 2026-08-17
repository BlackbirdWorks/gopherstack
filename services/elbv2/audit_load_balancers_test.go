package elbv2_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// TestAuditELBv2_DNSName_ALBAndNLBFormat verifies that ALB and NLB get correctly
// formatted DNS names following the AWS convention.
func TestAuditELBv2_DNSName_ALBAndNLBFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lbType     string
		lbName     string
		wantSuffix string
	}{
		{
			name:       "alb_dns_contains_region",
			lbType:     "application",
			lbName:     "my-alb",
			wantSuffix: config.DefaultRegion + ".elb.amazonaws.com",
		},
		{
			name:       "nlb_dns_contains_elb_prefix",
			lbType:     "network",
			lbName:     "my-nlb",
			wantSuffix: "elb." + config.DefaultRegion + ".amazonaws.com",
		},
		{
			name:       "gateway_lb_gets_dns",
			lbType:     "gateway",
			lbName:     "my-gwlb",
			wantSuffix: ".elb.amazonaws.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)

			var resp struct {
				Result struct {
					LoadBalancers struct {
						Members []struct {
							DNSName               string `xml:"DNSName"`
							CanonicalHostedZoneID string `xml:"CanonicalHostedZoneId"`
							Type                  string `xml:"Type"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"CreateLoadBalancerResult"`
			}
			auditDo(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {tc.lbName},
				"Type":    {tc.lbType},
			}).into(&resp)

			require.Len(t, resp.Result.LoadBalancers.Members, 1)
			lb := resp.Result.LoadBalancers.Members[0]
			assert.Contains(t, lb.DNSName, tc.lbName, "DNS name should contain the LB name")
			assert.Contains(t, lb.DNSName, tc.wantSuffix, "DNS name suffix mismatch")
			assert.NotEmpty(t, lb.CanonicalHostedZoneID, "CanonicalHostedZoneId must be set")
		})
	}
}

// TestAuditELBv2_LBAttributes_ALBDefaults verifies that an ALB is created with
// the correct default attribute set (idle_timeout, routing.http2.enabled, etc.).
func TestAuditELBv2_LBAttributes_ALBDefaults(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "attr-alb")

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	auditDo(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	}).into(&resp)

	attrs := map[string]string{}
	for _, m := range resp.Result.Attributes.Members {
		attrs[m.Key] = m.Value
	}
	assert.Equal(t, "60", attrs["idle_timeout.timeout_seconds"])
	assert.Equal(t, "true", attrs["routing.http2.enabled"])
	assert.Equal(t, "false", attrs["deletion_protection.enabled"])
	assert.Equal(t, "true", attrs["load_balancing.cross_zone.enabled"])
}

// TestAuditELBv2_LBAttributes_NLBDefaults verifies NLB-specific default attributes.
func TestAuditELBv2_LBAttributes_NLBDefaults(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateNLB(t, h, "attr-nlb")

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	auditDo(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	}).into(&resp)

	attrs := map[string]string{}
	for _, m := range resp.Result.Attributes.Members {
		attrs[m.Key] = m.Value
	}
	assert.Equal(t, "false", attrs["load_balancing.cross_zone.enabled"])
	assert.Equal(t, "false", attrs["deletion_protection.enabled"])
}

// TestAuditELBv2_LBScheme_InternetFacingAndInternal verifies that ALBs can be
// created with internet-facing and internal schemes.
func TestAuditELBv2_LBScheme_InternetFacingAndInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scheme     string
		wantScheme string
	}{
		{name: "internet-facing", scheme: "internet-facing", wantScheme: "internet-facing"},
		{name: "internal", scheme: "internal", wantScheme: "internal"},
		{name: "default-facing", scheme: "", wantScheme: "internet-facing"},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)
			vals := url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {fmt.Sprintf("scheme-lb-%d", i)},
				"Type":    {"application"},
			}
			if tc.scheme != "" {
				vals.Set("Scheme", tc.scheme)
			}

			var resp struct {
				Result struct {
					LoadBalancers struct {
						Members []struct {
							Scheme string `xml:"Scheme"`
							State  struct {
								Code string `xml:"Code"`
							} `xml:"State"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"CreateLoadBalancerResult"`
			}
			auditDo(t, h, vals).into(&resp)
			require.Len(t, resp.Result.LoadBalancers.Members, 1)
			lb := resp.Result.LoadBalancers.Members[0]
			assert.Equal(t, tc.wantScheme, lb.Scheme)
			assert.Equal(t, "active", lb.State.Code, "new LB must be active immediately")
		})
	}
}

// TestAuditELBv2_Pagination_DescribeLoadBalancers verifies marker-based pagination
// for DescribeLoadBalancers.
func TestAuditELBv2_Pagination_DescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)

	// Create 5 LBs.
	for i := range 5 {
		auditCreateLB(t, h, fmt.Sprintf("page-lb-%02d", i))
	}

	// Page 1: PageSize=2.
	var page1Resp struct {
		Result struct { //nolint:govet // field order is chosen for readability
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	auditDo(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	}).into(&page1Resp)

	require.Len(t, page1Resp.Result.LoadBalancers.Members, 2)
	assert.NotEmpty(t, page1Resp.Result.NextMarker, "page 1 must return NextMarker")

	// Page 2 with marker.
	var page2Resp struct {
		Result struct { //nolint:govet // field order is chosen for readability
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	auditDo(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page1Resp.Result.NextMarker},
	}).into(&page2Resp)

	require.Len(t, page2Resp.Result.LoadBalancers.Members, 2)

	// Collect all names to ensure no duplicates.
	seen := map[string]bool{}
	for _, m := range page1Resp.Result.LoadBalancers.Members {
		seen[m.LoadBalancerName] = true
	}
	for _, m := range page2Resp.Result.LoadBalancers.Members {
		assert.False(
			t,
			seen[m.LoadBalancerName],
			"duplicate LB across pages: %s",
			m.LoadBalancerName,
		)
	}
}

// TestAuditELBv2_SecurityGroups_ALBOnly verifies that SetSecurityGroups rejects NLBs.
func TestAuditELBv2_SecurityGroups_ALBOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lbType   string
		wantCode int
	}{
		{name: "alb-accepts-sgs", lbType: "application", wantCode: http.StatusOK},
		{name: "nlb-rejects-sgs", lbType: "network", wantCode: http.StatusBadRequest},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)
			var resp struct {
				Result struct {
					LoadBalancers struct {
						Members []struct {
							LoadBalancerArn string `xml:"LoadBalancerArn"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"CreateLoadBalancerResult"`
			}
			auditDo(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {fmt.Sprintf("sg-lb-%d", i)},
				"Type":    {tc.lbType},
			}).into(&resp)
			lbArn := resp.Result.LoadBalancers.Members[0].LoadBalancerArn

			rec := doELBv2(t, h, url.Values{
				"Action":                  {"SetSecurityGroups"},
				"Version":                 {"2015-12-01"},
				"LoadBalancerArn":         {lbArn},
				"SecurityGroups.member.1": {"sg-00000001"},
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
