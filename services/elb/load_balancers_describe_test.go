package elb_test

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestDescribeLoadBalancers tests describe operations.
func TestDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lb-a")
				mustCreateLB(t, h, "lb-b")
			},
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "describe_by_name",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "named-lb")
			},
			vals: url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"named-lb"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "describe_not_found",
			vals: url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"missing-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "describe_empty",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
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
				var resp struct {
					XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
					Result  struct {
						LoadBalancerDescriptions struct {
							Members []struct {
								Name string `xml:"LoadBalancerName"`
							} `xml:"member"`
						} `xml:"LoadBalancerDescriptions"`
					} `xml:"DescribeLoadBalancersResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.LoadBalancerDescriptions.Members, tt.wantCount)
			}
		})
	}
}

// TestDescribeLoadBalancersWithHealthCheck tests that health check is included in describe.
func TestDescribeLoadBalancersWithHealthCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-describe-lb")

	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-describe-lb"},
		"HealthCheck.Target":             {"TCP:80"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"3"},
		"HealthCheck.HealthyThreshold":   {"2"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"hc-describe-lb"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					HealthCheck struct {
						Target string `xml:"Target"`
					} `xml:"HealthCheck"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	parseXMLBody(t, rec, &resp)
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	assert.Equal(t, "TCP:80", resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck.Target)
}

// TestDescribeLoadBalancersHealthCheckAlwaysPresent verifies that the HealthCheck
// XML element is always included in DescribeLoadBalancers responses, even when no
// health check is configured. This prevents a nil-pointer panic in the Terraform
// AWS provider which accesses lb.HealthCheck.Target without a nil guard.
func TestDescribeLoadBalancersHealthCheckAlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "no-hc-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"no-hc-lb"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					HealthCheck *struct {
						Target string `xml:"Target"`
					} `xml:"HealthCheck"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	parseXMLBody(t, rec, &resp)
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	assert.NotNil(
		t,
		resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck,
		"HealthCheck element must always be present",
	)
	assert.Empty(t, resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck.Target)
}

// TestDescribeAccountLimits tests returning account limits.
func TestDescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.NotEmpty(t, resp.Result.Limits.Members)

	names := make([]string, 0, len(resp.Result.Limits.Members))
	for _, m := range resp.Result.Limits.Members {
		names = append(names, m.Name)
	}

	assert.Contains(t, names, "classic-load-balancers")
	assert.Contains(t, names, "classic-listeners")
}

// TestDescribeAccountLimitsPagination proves Marker/PageSize genuinely
// paginate the (small, fixed) limit catalog instead of being parsed and
// ignored (reverting the pagination logic in handleDescribeAccountLimits
// makes this fail: page1 would contain all 3 limits and NextMarker would be
// empty).
func TestDescribeAccountLimitsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	type limitsResp struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			NextMarker string `xml:"NextMarker"`
			Limits     struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}

	rec1 := doELB(t, h, url.Values{
		"Action":   {"DescribeAccountLimits"},
		"Version":  {"2012-06-01"},
		"PageSize": {"1"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 limitsResp
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.Limits.Members, 1)
	require.NotEmpty(t, page1.Result.NextMarker)

	rec2 := doELB(t, h, url.Values{
		"Action":   {"DescribeAccountLimits"},
		"Version":  {"2012-06-01"},
		"PageSize": {"1"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 limitsResp
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.Limits.Members, 1)
	assert.NotEqual(t, page1.Result.Limits.Members[0].Name, page2.Result.Limits.Members[0].Name)

	// Requesting all 3 in one page must not carry a NextMarker.
	recAll := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
	})
	require.Equal(t, http.StatusOK, recAll.Code)

	var pageAll limitsResp
	require.NoError(t, xml.Unmarshal(recAll.Body.Bytes(), &pageAll))
	assert.Empty(t, pageAll.Result.NextMarker)
	assert.Len(t, pageAll.Result.Limits.Members, 3)
}

// TestDescribeAccountLimitsInvalidMarker verifies a malformed Marker is rejected.
func TestDescribeAccountLimitsInvalidMarker(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
		"Marker":  {"not-valid-base64!!"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSourceSecurityGroupAlwaysPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *elb.Handler) string
		name  string
	}{
		{
			name: "classic_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-classic-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-classic-lb"
			},
		},
		{
			name: "vpc_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-vpc-lb"},
					"Subnets.member.1":                    {"subnet-abc123"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-vpc-lb"
			},
		},
		{
			name: "internal_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-internal-lb"},
					"Scheme":                              {"internal"},
					"Subnets.member.1":                    {"subnet-def456"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-internal-lb"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := tt.setup(t, h)

			rec := doELB(t, h, url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {lbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LoadBalancerDescriptions struct {
						Members []struct {
							SourceSecurityGroup struct {
								GroupName  string `xml:"GroupName"`
								OwnerAlias string `xml:"OwnerAlias"`
							} `xml:"SourceSecurityGroup"`
						} `xml:"member"`
					} `xml:"LoadBalancerDescriptions"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)

			ssg := resp.Result.LoadBalancerDescriptions.Members[0].SourceSecurityGroup
			assert.NotEmpty(t, ssg.GroupName, "GroupName must be set")
			assert.NotEmpty(t, ssg.OwnerAlias, "OwnerAlias must be set")
		})
	}
}

// TestSourceSecurityGroupEC2Classic verifies that an EC2-Classic
// (no-subnet, no-VPC) LB uses the Amazon-managed security-group alias
// "amazon-elb" / "amazon-elb-sg", matching real AWS behaviour.
func TestSourceSecurityGroupEC2Classic(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("999888777666", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "ssg-acct-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"ssg-acct-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					SourceSecurityGroup struct {
						GroupName  string `xml:"GroupName"`
						OwnerAlias string `xml:"OwnerAlias"`
					} `xml:"SourceSecurityGroup"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)

	ssg := resp.Result.LoadBalancerDescriptions.Members[0].SourceSecurityGroup
	assert.Equal(t, "amazon-elb-sg", ssg.GroupName)
	assert.Equal(t, "amazon-elb", ssg.OwnerAlias)
}

func TestDescribeLoadBalancersMultipleNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "multi-a")
	mustCreateLB(t, h, "multi-b")
	mustCreateLB(t, h, "multi-c")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"multi-a"},
		"LoadBalancerNames.member.2": {"multi-c"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 2)
}

func TestDescribeLoadBalancersUnknownNameReturns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "exist-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"exist-lb"},
		"LoadBalancerNames.member.2": {"no-such-lb"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDescribeLoadBalancersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for i := range 5 {
		name := [5]string{"lb-aaa", "lb-bbb", "lb-ccc", "lb-ddd", "lb-eee"}[i]
		mustCreateLB(t, h, name)
	}

	// First page: 3 items.
	rec := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker               string `xml:"NextMarker"`
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &page1))
	require.Len(t, page1.Result.LoadBalancerDescriptions.Members, 3)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Second page using marker.
	rec2 := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"3"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker               string `xml:"NextMarker"`
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.LoadBalancerDescriptions.Members, 2)
	assert.Empty(t, page2.Result.NextMarker, "last page must have no NextMarker")
}

func TestAccountLimitsStructure(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.GreaterOrEqual(t, len(resp.Result.Limits.Members), 3)

	names := make([]string, 0, len(resp.Result.Limits.Members))
	for _, l := range resp.Result.Limits.Members {
		names = append(names, l.Name)
		assert.NotEmpty(t, l.Max, "limit Max must not be empty")
	}
	assert.Contains(t, names, "classic-load-balancers")
}

// TestRegionHostedZoneID verifies that known AWS regions return their
// documented CanonicalHostedZoneNameID values.
func TestRegionHostedZoneID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region         string
		wantHostedZone string
	}{
		{"us-east-1", "Z35SXDOTRQ7X7K"},
		{"us-west-2", "Z1H1FL5HABSF5"},
		{"eu-west-1", "Z32O12XQLNTSW2"},
		{"ap-southeast-1", "Z1LMS91P8CMLE5"},
		{"ap-northeast-1", "Z14GRHDCWA56QT"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			b := elb.NewInMemoryBackend("123456789012", tt.region)
			h := elb.NewHandler(b)

			mustCreateLB(t, h, "zone-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"zone-lb"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LBs struct {
						Members []struct {
							CanonicalHostedZoneNameID string `xml:"CanonicalHostedZoneNameID"`
						} `xml:"member"`
					} `xml:"LoadBalancerDescriptions"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.LBs.Members, 1)
			assert.Equal(t, tt.wantHostedZone, resp.Result.LBs.Members[0].CanonicalHostedZoneNameID)
		})
	}
}

// TestDNSNameFormat verifies that DNS names include a hash suffix and
// the internal scheme uses the "internal-" prefix.
func TestDNSNameFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantPrefix   string
		wantSuffix   string
		wantContains string
	}{
		{
			name: "internet_facing_has_hash_suffix",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"inet-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantPrefix:   "inet-lb-",
			wantSuffix:   ".us-east-1.elb.amazonaws.com",
			wantContains: "us-east-1.elb.amazonaws.com",
		},
		{
			name: "internal_scheme_has_internal_prefix",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"int-lb"},
				"Scheme":                              {"internal"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantPrefix:   "internal-int-lb-",
			wantSuffix:   ".us-east-1.elb.amazonaws.com",
			wantContains: "internal-int-lb-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
				Result  struct {
					DNSName string `xml:"DNSName"`
				} `xml:"CreateLoadBalancerResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			dns := resp.Result.DNSName

			if tt.wantPrefix != "" {
				assert.True(t, strings.HasPrefix(dns, tt.wantPrefix),
					"DNS %q should start with %q", dns, tt.wantPrefix)
			}
			if tt.wantSuffix != "" {
				assert.True(t, strings.HasSuffix(dns, tt.wantSuffix),
					"DNS %q should end with %q", dns, tt.wantSuffix)
			}
			if tt.wantContains != "" {
				assert.Contains(t, dns, tt.wantContains)
			}
		})
	}
}

// TestSourceSecurityGroupVPC verifies that a VPC LB returns the
// account ID as OwnerAlias and "default" as GroupName.
func TestSourceSecurityGroupVPC(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("111222333444", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateVPCLB(t, h, "vpc-ssg-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"vpc-ssg-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LBs struct {
				Members []struct {
					SSG struct {
						GroupName  string `xml:"GroupName"`
						OwnerAlias string `xml:"OwnerAlias"`
					} `xml:"SourceSecurityGroup"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LBs.Members, 1)

	ssg := resp.Result.LBs.Members[0].SSG
	assert.Equal(t, "default", ssg.GroupName)
	assert.Equal(t, "111222333444", ssg.OwnerAlias)
}

// TestAccountLimitMaxLoadBalancers verifies that creating more than 20
// load balancers returns a LimitExceeded error.
func TestAccountLimitMaxLoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 20 LBs (the max).
	for i := range 20 {
		rec := doELB(t, h, url.Values{
			"Action":                              {"CreateLoadBalancer"},
			"Version":                             {"2012-06-01"},
			"LoadBalancerName":                    {fmt.Sprintf("max-lbs-%02d", i)},
			"Listeners.member.1.Protocol":         {"HTTP"},
			"Listeners.member.1.LoadBalancerPort": {"80"},
			"Listeners.member.1.InstancePort":     {"8080"},
			"AvailabilityZones.member.1":          {"us-east-1a"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "LB %d creation should succeed", i+1)
	}

	// 21st LB must fail.
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"lb-over-limit"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "TooManyLoadBalancers", errResp.Error.Code)
}

// TestPaginationOpaqueMarker verifies that the NextMarker is base64
// encoded and that sending a non-base64 Marker returns an error.
func TestPaginationOpaqueMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		marker     string
		wantStatus int
	}{
		// A valid base64-encoded integer offset is accepted.
		{"valid_base64_marker", base64.StdEncoding.EncodeToString([]byte("0")), http.StatusOK},
		// Plain LB name (old behaviour) is now invalid.
		{"plain_name_marker_rejected", "some-lb-name", http.StatusBadRequest},
		// Totally invalid string is rejected.
		{"garbage_marker_rejected", "!!!not-base64!!!", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "pag-lb")

			rec := doELB(t, h, url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
				"Marker":  {tt.marker},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestPaginationNextMarkerIsBase64 verifies that when a NextMarker is
// returned it is valid base64 encoding an integer.
func TestPaginationNextMarkerIsBase64(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 5 LBs.
	for i := range 5 {
		mustCreateLB(t, h, fmt.Sprintf("pag-lb-%d", i))
	}

	// Request page of size 2.
	rec := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	marker := resp.Result.NextMarker
	require.NotEmpty(t, marker, "NextMarker should be set when more pages exist")

	// Marker must be valid base64.
	decoded, err := base64.StdEncoding.DecodeString(marker)
	require.NoError(t, err, "NextMarker must be base64: %q", marker)
	assert.NotEmpty(t, decoded)
}

// TestDeepCopyListeners verifies that the slice returned by DescribeLoadBalancers
// is a deep copy — mutations do not affect the stored state.
func TestDeepCopyListeners(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "dc-lb")

	// First describe: copy has 1 listener (from mustCreateLB).
	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"dc-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	originalCount := len(lbs[0].Listeners)

	// Append a listener to the returned copy — must not affect the stored LB.
	lbs[0].Listeners = append(lbs[0].Listeners, elb.Listener{Protocol: "TCP", LoadBalancerPort: 443, InstancePort: 443})
	require.Len(t, lbs[0].Listeners, originalCount+1)

	// Second describe: stored state must still have only originalCount listeners.
	lbs2, err := b.DescribeLoadBalancers(context.Background(), []string{"dc-lb"})
	require.NoError(t, err)
	assert.Len(t, lbs2[0].Listeners, originalCount, "mutation of returned copy must not modify stored state")
}

// TestSortedDescribeLoadBalancers verifies alphabetical ordering of DescribeLoadBalancers.
func TestSortedDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	for _, name := range []string{"z-lb", "a-lb", "m-lb"} {
		mustCreateLB(t, h, name)
	}

	lbs, err := b.DescribeLoadBalancers(context.Background(), nil)
	require.NoError(t, err)

	names := make([]string, len(lbs))
	for i, lb := range lbs {
		names[i] = lb.LoadBalancerName
	}

	assert.Equal(t, []string{"a-lb", "m-lb", "z-lb"}, names)
}

// TestDescribeAccountLimitsLocked verifies DescribeAccountLimits succeeds
// and returns exactly 3 limits.
func TestDescribeAccountLimitsLocked(t *testing.T) {
	t.Parallel()

	b := newBackend()
	limits, err := b.DescribeAccountLimits(context.Background())
	require.NoError(t, err)
	assert.Len(t, limits, 3)
}

// TestVPCIdInXML verifies that VPCId appears in XML response for subnet-attached LBs.
func TestVPCIdInXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, h *elb.Handler)
		name      string
		lbName    string
		wantEmpty bool
	}{
		{
			name:   "vpc_lb_has_vpc_id",
			lbName: "vpc-test-lb",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"vpc-test-lb"},
					"Subnets.member.1":                    {"subnet-abc123"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})
			},
			wantEmpty: false,
		},
		{
			name:   "classic_lb_no_vpc_id",
			lbName: "classic-test-lb",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"classic-test-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})
			},
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			tt.setup(t, h)

			rec := doELB(t, h, url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {tt.lbName},
			})

			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LoadBalancerDescriptions struct {
						Members []struct {
							VPCId string `xml:"VPCId"`
						} `xml:"member"`
					} `xml:"LoadBalancerDescriptions"`
				} `xml:"DescribeLoadBalancersResult"`
			}

			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)

			vpcID := resp.Result.LoadBalancerDescriptions.Members[0].VPCId
			if tt.wantEmpty {
				assert.Empty(t, vpcID)
			} else {
				assert.NotEmpty(t, vpcID)
			}
		})
	}
}

// TestSourceSecurityGroupInXML verifies SourceSecurityGroup appears in XML.
func TestSourceSecurityGroupInXML(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "sg-test-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"sg-test-lb"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					SourceSecurityGroup struct {
						GroupName  string `xml:"GroupName"`
						OwnerAlias string `xml:"OwnerAlias"`
					} `xml:"SourceSecurityGroup"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)

	ssg := resp.Result.LoadBalancerDescriptions.Members[0].SourceSecurityGroup
	assert.NotEmpty(t, ssg.GroupName)
	assert.NotEmpty(t, ssg.OwnerAlias)
}

// TestDescribeLoadBalancersInvalidMarkerParam verifies that an invalid Marker returns an error.
func TestDescribeLoadBalancersInvalidMarkerParam(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "marker-lb")

	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeLoadBalancers"},
		"Version": {"2012-06-01"},
		"Marker":  {"no-such-lb"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancersInvalidMarkerToken verifies InvalidNextMarker error.
func TestDescribeLoadBalancersInvalidMarkerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "unknown_marker_returns_error",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
				"Marker":  {"nonexistent-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no_marker_ok",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			got := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, got.Code)
		})
	}
}
