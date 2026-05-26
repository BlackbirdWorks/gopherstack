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

// TestRefinement2_VPCIdInXML verifies that VPCId appears in XML response for subnet-attached LBs.
func TestRefinement2_VPCIdInXML(t *testing.T) {
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

// TestRefinement2_SourceSecurityGroupInXML verifies SourceSecurityGroup appears in XML.
func TestRefinement2_SourceSecurityGroupInXML(t *testing.T) {
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

// TestRefinement2_PolicyNameValidation verifies policy name validation in create policy handlers.
func TestRefinement2_PolicyNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "empty_policy_name_rejected",
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"pol-val-lb"},
				"PolicyName":       {""},
				"CookieName":       {"SESS"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "policy_name_too_long_rejected",
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"pol-val-lb"},
				"PolicyName":       {"a12345678901234567890123456789012"},
				"CookieName":       {"SESS"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid_policy_name_accepted",
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"pol-val-lb"},
				"PolicyName":       {"valid-policy-1"},
				"CookieName":       {"SESS"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "pol-val-lb")

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_CookieNameRequired verifies AppCookieStickinessPolicy requires CookieName.
func TestRefinement2_CookieNameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cookieName string
		wantStatus int
	}{
		{
			name:       "empty_cookie_name_rejected",
			cookieName: "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non_empty_cookie_name_accepted",
			cookieName: "JSESSIONID",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "cookie-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"cookie-lb"},
				"PolicyName":       {"my-cookie-pol"},
				"CookieName":       {tt.cookieName},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_SetPoliciesOfListener_RejectsUnknownPolicy verifies that
// SetLoadBalancerPoliciesOfListener returns an error for unknown policy names.
func TestRefinement2_SetPoliciesOfListener_RejectsUnknownPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		name       string
		policyName string
		wantCode   string
		wantStatus int
	}{
		{
			name: "unknown_policy_rejected",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "set-pol-lb")
			},
			policyName: "no-such-policy",
			wantStatus: http.StatusNotFound,
			wantCode:   "PolicyNotFound",
		},
		{
			name: "known_policy_accepted",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "set-pol-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLBCookieStickinessPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"set-pol-lb"},
					"PolicyName":       {"lb-cookie-pol"},
				})
			},
			policyName: "lb-cookie-pol",
			wantStatus: http.StatusOK,
			wantCode:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			tt.setup(t, h)

			rec := doELB(t, h, url.Values{
				"Action":               {"SetLoadBalancerPoliciesOfListener"},
				"Version":              {"2012-06-01"},
				"LoadBalancerName":     {"set-pol-lb"},
				"LoadBalancerPort":     {"80"},
				"PolicyNames.member.1": {tt.policyName},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				var errResp struct {
					XMLName xml.Name `xml:"ErrorResponse"`
					Error   struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			}
		})
	}
}

// TestRefinement2_ModifyLBAttributes_IdleTimeout verifies idle timeout validation.
func TestRefinement2_ModifyLBAttributes_IdleTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		timeout    string
		wantStatus int
	}{
		{name: "zero_rejected", timeout: "0", wantStatus: http.StatusBadRequest},
		{name: "negative_rejected", timeout: "-1", wantStatus: http.StatusBadRequest},
		{name: "over_max_rejected", timeout: "3601", wantStatus: http.StatusBadRequest},
		{name: "min_accepted", timeout: "1", wantStatus: http.StatusOK},
		{name: "max_accepted", timeout: "3600", wantStatus: http.StatusOK},
		{name: "typical_accepted", timeout: "60", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "idle-to-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"idle-to-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {tt.timeout},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_ModifyLBAttributes_DrainingTimeout verifies connection draining timeout validation.
func TestRefinement2_ModifyLBAttributes_DrainingTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		timeout    string
		wantStatus int
	}{
		{name: "over_max_rejected", timeout: "3601", wantStatus: http.StatusBadRequest},
		{name: "zero_rejected", timeout: "0", wantStatus: http.StatusBadRequest},
		{name: "max_accepted", timeout: "3600", wantStatus: http.StatusOK},
		{name: "min_accepted", timeout: "1", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "drain-to-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"drain-to-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
				"LoadBalancerAttributes.ConnectionDraining.Enabled":     {"true"},
				"LoadBalancerAttributes.ConnectionDraining.Timeout":     {tt.timeout},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_ModifyLBAttributes_DesyncMode verifies desync mitigation mode validation.
func TestRefinement2_ModifyLBAttributes_DesyncMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       string
		wantStatus int
	}{
		{name: "invalid_mode_rejected", mode: "invalid-mode", wantStatus: http.StatusBadRequest},
		{name: "defensive_accepted", mode: "defensive", wantStatus: http.StatusOK},
		{name: "strictest_accepted", mode: "strictest", wantStatus: http.StatusOK},
		{name: "monitor_accepted", mode: "monitor", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "desync-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"desync-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout":      {"60"},
				"LoadBalancerAttributes.AdditionalAttributes.member.1.Key":   {"elb.http.desyncmitigationmode"},
				"LoadBalancerAttributes.AdditionalAttributes.member.1.Value": {tt.mode},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_DisableAZs_EmptyListNoOp verifies that DisableAZs with empty list is a no-op.
func TestRefinement2_DisableAZs_EmptyListNoOp(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"az-noop-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"AvailabilityZones.member.2":          {"us-east-1b"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"az-noop-lb"},
		// No AZs provided → no-op
	})

	require.Equal(t, http.StatusOK, rec.Code)

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

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.AvailabilityZones.Members, 2, "both AZs must remain after no-op disable")
}

// TestRefinement2_DetachSubnets_SortedResult verifies that DetachLoadBalancerFromSubnets
// returns subnets in sorted order.
func TestRefinement2_DetachSubnets_SortedResult(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sorted-detach-lb"},
		"Subnets.member.1":                    {"subnet-zzz"},
		"Subnets.member.2":                    {"subnet-aaa"},
		"Subnets.member.3":                    {"subnet-mmm"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DetachLoadBalancerFromSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"sorted-detach-lb"},
		"Subnets.member.1": {"subnet-mmm"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

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

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Subnets.Members, 2)

	subnets := []string{
		resp.Result.Subnets.Members[0].Value,
		resp.Result.Subnets.Members[1].Value,
	}
	assert.Equal(t, []string{"subnet-aaa", "subnet-zzz"}, subnets, "subnets must be sorted")
}

// TestRefinement2_AddTags_RejectsAwsPrefix verifies that tags with 'aws:' prefix are rejected.
func TestRefinement2_AddTags_RejectsAwsPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		wantStatus int
	}{
		{
			name:       "aws_prefix_rejected",
			tagKey:     "aws:stack",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "regular_key_accepted",
			tagKey:     "Environment",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "tag-prefix-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"tag-prefix-lb"},
				"Tags.member.1.Key":          {tt.tagKey},
				"Tags.member.1.Value":        {"val"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_RemoveTags_RequiresAtLeastOneName verifies RemoveTags needs at least one LB name.
func TestRefinement2_RemoveTags_RequiresAtLeastOneName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "no_lb_names_rejected",
			vals: url.Values{
				"Action":            {"RemoveTags"},
				"Version":           {"2012-06-01"},
				"Tags.member.1.Key": {"Env"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_lb_name_accepted",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"remove-tags-lb"},
				"Tags.member.1.Key":          {"Env"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)

			if tt.wantStatus == http.StatusOK {
				mustCreateLB(t, h, "remove-tags-lb")
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_DescribePolicies_AllPoliciesWhenNoName verifies that
// DescribeLoadBalancerPolicies with no LoadBalancerName returns the built-in
// sample SSL policies (AWS behaviour: customer policies are NOT included).
func TestRefinement2_DescribePolicies_AllPoliciesWhenNoName(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	// Create some customer policies — these should NOT appear in the global result.
	mustCreateLB(t, h, "multi-pol-lb1")
	mustCreateLB(t, h, "multi-pol-lb2")

	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"multi-pol-lb1"},
		"PolicyName":       {"pol-lb1"},
	})

	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"multi-pol-lb2"},
		"PolicyName":       {"pol-lb2"},
	})

	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeLoadBalancerPolicies"},
		"Version": {"2012-06-01"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
		Result  struct {
			PolicyDescriptions struct {
				Members []struct {
					PolicyName string `xml:"PolicyName"`
				} `xml:"member"`
			} `xml:"PolicyDescriptions"`
		} `xml:"DescribeLoadBalancerPoliciesResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	// Exactly 4 built-in sample policies are returned; customer policies are excluded.
	assert.Len(t, resp.Result.PolicyDescriptions.Members, 4)

	names := make([]string, 0, 4)
	for _, m := range resp.Result.PolicyDescriptions.Members {
		names = append(names, m.PolicyName)
	}

	assert.Contains(t, names, "ELBSample-ELBDefaultNegotiationPolicy")
	assert.Contains(t, names, "ELBSample-OpenSSLDefaultCipherPolicy")
	assert.Contains(t, names, "ELBSecurityPolicy-2016-08")
	assert.Contains(t, names, "ELBSecurityPolicy-TLS-1-2-2017-01")
}

// TestRefinement2_CreateListeners_RejectsEmptyList verifies that CreateLoadBalancerListeners
// rejects an empty listener list.
func TestRefinement2_CreateListeners_RejectsEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "empty-listeners-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerListeners"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"empty-listeners-lb"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_InvalidMarker verifies that an invalid Marker returns an error.
func TestRefinement2_InvalidMarker(t *testing.T) {
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

// TestRefinement2_ListenerNotFound_Returns404 verifies ErrListenerNotFound maps to HTTP 404.
func TestRefinement2_ListenerNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantCode string
	}{
		{
			name: "ssl_cert_no_listener",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lnf-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:iam::123456789012:server-certificate/my-cert"},
			},
			wantCode: "ListenerNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "lnf-lb")

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			var errResp struct {
				XMLName xml.Name `xml:"ErrorResponse"`
				Error   struct {
					Code string `xml:"Code"`
				} `xml:"Error"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// TestRefinement2_RegisterInstances_InvalidIDFormat verifies that invalid instance IDs are rejected.
func TestRefinement2_RegisterInstances_InvalidIDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		wantStatus int
	}{
		{name: "invalid_format_rejected", instanceID: "invalid-id", wantStatus: http.StatusBadRequest},
		{name: "uppercase_rejected", instanceID: "i-ABCDEF01", wantStatus: http.StatusBadRequest},
		{name: "too_short_rejected", instanceID: "i-abc123", wantStatus: http.StatusBadRequest},
		{name: "valid_8_char_accepted", instanceID: "i-abcdef01", wantStatus: http.StatusOK},
		{name: "valid_17_char_accepted", instanceID: "i-abcdef0123456789a", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "inst-id-lb")

			rec := doELB(t, h, url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"inst-id-lb"},
				"Instances.member.1.InstanceId": {tt.instanceID},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_DescribeInstanceHealth_InvalidInstance verifies ErrInvalidInstance
// is returned when requesting health for an instance not registered with the LB.
func TestRefinement2_DescribeInstanceHealth_InvalidInstance(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "health-inv-lb")

	rec := doELB(t, h, url.Values{
		"Action":                        {"DescribeInstanceHealth"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"health-inv-lb"},
		"Instances.member.1.InstanceId": {"i-abcdef01"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidInstance", errResp.Error.Code)
}
