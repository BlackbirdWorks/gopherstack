package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func newTestHandler() *elbv2.Handler {
	backend := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return elbv2.NewHandler(backend)
}

// doELBv2 sends a form-encoded POST to the ELBv2 handler and returns the recorder.
func doELBv2(t *testing.T, h *elbv2.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()

	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// mustCreateLB creates a load balancer and asserts success.
func mustCreateLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Scheme":  {"internet-facing"},
		"Type":    {"application"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	return resp.Result.LoadBalancers.Members[0].LoadBalancerArn
}

// mustCreateTG creates a target group and asserts success.
func mustCreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()

	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateTargetGroupResponse"`
		Result  struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	return resp.Result.TargetGroups.Members[0].TargetGroupArn
}

// parseXMLBody parses raw XML from a recorder body into dst.
func parseXMLBody(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), dst))
}

// TestCreateLoadBalancer tests load balancer creation.
func TestCreateLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantDNS    string
		wantStatus int
	}{
		{
			name: "creates_successfully",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"my-alb"},
				"Scheme":  {"internet-facing"},
				"Type":    {"application"},
			},
			wantStatus: http.StatusOK,
			wantDNS:    "my-alb-us-east-1.us-east-1.elb.amazonaws.com",
		},
		{
			name: "duplicate_returns_conflict",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-alb")
			},
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"dup-alb"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "missing_name_returns_bad_request",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_internal_scheme",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"internal-alb"},
				"Scheme":  {"internal"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDNS != "" {
				var resp struct {
					XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
					Result  struct {
						LoadBalancers struct {
							Members []struct {
								DNSName string `xml:"DNSName"`
							} `xml:"member"`
						} `xml:"LoadBalancers"`
					} `xml:"CreateLoadBalancerResult"`
				}
				parseXMLBody(t, rec, &resp)
				require.Len(t, resp.Result.LoadBalancers.Members, 1)
				assert.Equal(t, tt.wantDNS, resp.Result.LoadBalancers.Members[0].DNSName)
			}
		})
	}
}

// TestDeleteLoadBalancer tests delete operations.
func TestDeleteLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				lbArn := mustCreateLB(t, h, "delete-me")
				_ = lbArn
			},
			vals: url.Values{
				"Action":  {"DeleteLoadBalancer"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest, // LoadBalancerArn is required
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"DeleteLoadBalancer"},
				"Version": {"2015-12-01"},
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

			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeleteLoadBalancerByARN tests that providing a valid ARN deletes the LB.
func TestDeleteLoadBalancerByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "to-delete")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.LoadBalancers.Members)
}

// TestDescribeLoadBalancers tests describe operations.
func TestDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lb-a")
				mustCreateLB(t, h, "lb-b")
			},
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "describe_empty",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "filter-lb")
				mustCreateLB(t, h, "other-lb")
			},
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"filter-lb"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELBv2(t, h, tt.vals)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LoadBalancers struct {
						Members []struct {
							LoadBalancerArn string `xml:"LoadBalancerArn"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			parseXMLBody(t, rec, &resp)
			assert.Len(t, resp.Result.LoadBalancers.Members, tt.wantCount)
		})
	}
}

// TestCreateTargetGroup tests target group creation.
func TestCreateTargetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantArn    bool
	}{
		{
			name: "creates_successfully",
			vals: url.Values{
				"Action":   {"CreateTargetGroup"},
				"Version":  {"2015-12-01"},
				"Name":     {"my-tg"},
				"Protocol": {"HTTP"},
				"Port":     {"80"},
				"VpcId":    {"vpc-12345"},
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name: "duplicate_returns_conflict",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateTG(t, h, "dup-tg")
			},
			vals: url.Values{
				"Action":   {"CreateTargetGroup"},
				"Version":  {"2015-12-01"},
				"Name":     {"dup-tg"},
				"Protocol": {"HTTP"},
				"Port":     {"80"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "missing_name_returns_bad_request",
			vals: url.Values{
				"Action":  {"CreateTargetGroup"},
				"Version": {"2015-12-01"},
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

			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				var resp struct {
					XMLName xml.Name `xml:"CreateTargetGroupResponse"`
					Result  struct {
						TargetGroups struct {
							Members []struct {
								TargetGroupArn string `xml:"TargetGroupArn"`
							} `xml:"member"`
						} `xml:"TargetGroups"`
					} `xml:"CreateTargetGroupResult"`
				}
				parseXMLBody(t, rec, &resp)
				require.Len(t, resp.Result.TargetGroups.Members, 1)
				assert.NotEmpty(t, resp.Result.TargetGroups.Members[0].TargetGroupArn)
			}
		})
	}
}

// TestDescribeTargetGroups tests describe target groups operations.
func TestDescribeTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateTG(t, h, "tg-a")
				mustCreateTG(t, h, "tg-b")
			},
			vals: url.Values{
				"Action":  {"DescribeTargetGroups"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateTG(t, h, "filter-tg")
				mustCreateTG(t, h, "other-tg")
			},
			vals: url.Values{
				"Action":         {"DescribeTargetGroups"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"filter-tg"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELBv2(t, h, tt.vals)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				Result struct {
					TargetGroups struct {
						Members []struct {
							TargetGroupArn string `xml:"TargetGroupArn"`
						} `xml:"member"`
					} `xml:"TargetGroups"`
				} `xml:"DescribeTargetGroupsResult"`
			}
			parseXMLBody(t, rec, &resp)
			assert.Len(t, resp.Result.TargetGroups.Members, tt.wantCount)
		})
	}
}

// TestDeleteTargetGroup tests target group deletion.
func TestDeleteTargetGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "delete-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deletion
	rec2 := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.TargetGroups.Members)
}

// TestRegisterAndDeregisterTargets tests target registration.
func TestRegisterAndDeregisterTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "targets-tg")

	// Register targets
	rec := doELBv2(t, h, url.Values{
		"Action":              {"RegisterTargets"},
		"Version":             {"2015-12-01"},
		"TargetGroupArn":      {tgArn},
		"Targets.member.1.Id": {"i-0123456789abcdef0"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe target health
	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var healthResp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &healthResp))
	require.Len(t, healthResp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-0123456789abcdef0", healthResp.Result.TargetHealthDescriptions.Members[0].Target.ID)

	// Deregister targets
	rec3 := doELBv2(t, h, url.Values{
		"Action":              {"DeregisterTargets"},
		"Version":             {"2015-12-01"},
		"TargetGroupArn":      {tgArn},
		"Targets.member.1.Id": {"i-0123456789abcdef0"},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestCreateListener tests listener creation.
func TestCreateListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "creates_successfully",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "listener-lb")
				tgArn := mustCreateTG(t, h, "listener-tg")

				return url.Values{
					"Action":                                 {"CreateListener"},
					"Version":                                {"2015-12-01"},
					"LoadBalancerArn":                        {lbArn},
					"Protocol":                               {"HTTP"},
					"Port":                                   {"80"},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateListener"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "nonexistent_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateListener"},
					"Version": {"2015-12-01"},
					"LoadBalancerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no-such/0",
					},
					"Protocol": {"HTTP"},
					"Port":     {"80"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)
			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCreateRule tests rule creation.
func TestCreateRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-lb")
	tgArn := mustCreateTG(t, h, "rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	require.Len(t, listenerResp.Result.Listeners.Members, 1)
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	parseXMLBody(t, ruleRec, &ruleResp)
	require.Len(t, ruleResp.Result.Rules.Members, 1)
	assert.NotEmpty(t, ruleResp.Result.Rules.Members[0].RuleArn)
}

// TestAddAndDescribeTags tests tag operations.
func TestAddAndDescribeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "tagged-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"env"},
		"Tags.member.1.Value":   {"test"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TagDescriptions struct {
				Members []struct {
					ResourceArn string `xml:"ResourceArn"`
					Tags        struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	parseXMLBody(t, rec2, &resp)
	require.Len(t, resp.Result.TagDescriptions.Members, 1)
	assert.Equal(t, lbArn, resp.Result.TagDescriptions.Members[0].ResourceArn)

	found := false
	for _, tag := range resp.Result.TagDescriptions.Members[0].Tags.Members {
		if tag.Key == "env" && tag.Value == "test" {
			found = true
		}
	}
	assert.True(t, found, "expected tag env=test to be present")
}

// TestRemoveTags tests tag removal.
func TestRemoveTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "untag-lb")

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"remove-me"},
		"Tags.member.1.Value":   {"yes"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"TagKeys.member.1":      {"remove-me"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUnknownAction tests that an unknown action returns a bad request.
func TestUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"UnknownActionFoo"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestMissingAction tests that a request without Action returns bad request.
func TestMissingAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancerAttributes tests describe LB attributes.
func TestDescribeLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "attrs-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestModifyLoadBalancerAttributes tests modify LB attributes.
func TestModifyLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-attrs-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"ModifyLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSetSecurityGroups tests setting security groups.
func TestSetSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sg-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-00000001"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerName tests that the handler returns the correct name.
func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, "ELBv2", h.Name())
}

// TestHandlerSupportedOperations tests GetSupportedOperations.
func TestHandlerSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateLoadBalancer")
	assert.Contains(t, ops, "CreateTargetGroup")
	assert.Contains(t, ops, "CreateListener")
}

// TestDeleteListener tests listener deletion.
func TestDeleteListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "del-listener-lb")
				tgArn := mustCreateTG(t, h, "del-listener-tg")

				rec := doELBv2(t, h, url.Values{
					"Action":                                 {"CreateListener"},
					"Version":                                {"2015-12-01"},
					"LoadBalancerArn":                        {lbArn},
					"Protocol":                               {"HTTP"},
					"Port":                                   {"80"},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp struct {
					Result struct {
						Listeners struct {
							Members []struct {
								ListenerArn string `xml:"ListenerArn"`
							} `xml:"member"`
						} `xml:"Listeners"`
					} `xml:"CreateListenerResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				listenerArn := resp.Result.Listeners.Members[0].ListenerArn

				return url.Values{
					"Action":      {"DeleteListener"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {listenerArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteListener"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":      {"DeleteListener"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)
			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeListeners tests listener describe.
func TestDescribeListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "desc-listener-lb")
	tgArn := mustCreateTG(t, h, "desc-listener-tg")

	doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.Len(t, resp.Result.Listeners.Members, 1)
}

// TestModifyListener tests listener modification.
func TestModifyListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-listener-lb")
	tgArn := mustCreateTG(t, h, "mod-listener-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	rec := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Test not found case
	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// Test missing arn
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyListener"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// TestDeleteRule tests rule deletion.
func TestDeleteRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "del-rule-lb")
	tgArn := mustCreateTG(t, h, "del-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	// Delete the rule
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Test missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// Test not found
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {"arn:aws:no-such-rule"},
	})
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

// TestDescribeRules tests rule describe operations.
func TestDescribeRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "desc-rule-lb")
	tgArn := mustCreateTG(t, h, "desc-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.Len(t, resp.Result.Rules.Members, 1)
}

// TestModifyRule tests rule modification.
func TestModifyRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-rule-lb")
	tgArn := mustCreateTG(t, h, "mod-rule-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	rec := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found case
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {"arn:aws:no-such-rule"},
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// Missing arn
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// TestSetSubnets tests subnet setting.
func TestSetSubnets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "subnet-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":           {"SetSubnets"},
		"Version":          {"2015-12-01"},
		"LoadBalancerArn":  {lbArn},
		"Subnets.member.1": {"subnet-00000001"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"SetSubnets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestSetIpAddressType tests IP address type setting.
func TestSetIpAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "iptype-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"ipv4"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"SetIpAddressType"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestModifyTargetGroup tests target group modification.
func TestModifyTargetGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "mod-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"ModifyTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyTargetGroup"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestDeleteTargetGroupMissingARN tests missing ARN for delete.
func TestDeleteTargetGroupMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteTargetGroup"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteTargetGroupNotFound tests not found for delete.
func TestDeleteTargetGroupNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/no-such/0"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRegisterTargetsMissingARN tests missing ARN for register targets.
func TestRegisterTargetsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"RegisterTargets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeregisterTargetsMissingARN tests missing ARN for deregister targets.
func TestDeregisterTargetsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DeregisterTargets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeTargetHealthMissingARN tests missing ARN for describe target health.
func TestDescribeTargetHealthMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeTargetHealth"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAddTagsMissingResourceArns tests AddTags with no resource ARNs.
func TestAddTagsMissingResourceArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"AddTags"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRemoveTagsMissingResourceArns tests RemoveTags with no resource ARNs.
func TestRemoveTagsMissingResourceArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"RemoveTags"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeTagsMissingResourceArns tests DescribeTags with no resource ARNs.
func TestDescribeTagsMissingResourceArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeTags"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSetSecurityGroupsMissingARN tests missing ARN for SetSecurityGroups.
func TestSetSecurityGroupsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"SetSecurityGroups"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateRuleMissingListenerARN tests missing listener ARN for CreateRule.
func TestCreateRuleMissingListenerARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateRule"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateRuleListenerNotFound tests CreateRule with nonexistent listener.
func TestCreateRuleListenerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":      {"CreateRule"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestDescribeTagsForTargetGroupAndListener tests describe tags for TG and listener ARNs.
func TestDescribeTagsForTargetGroupAndListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tag-tg")

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
		"Tags.member.1.Key":     {"service"},
		"Tags.member.1.Value":   {"web"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
		"ResourceArns.member.2": {"arn:aws:doesnotexist"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TagDescriptions struct {
				Members []struct {
					ResourceArn string `xml:"ResourceArn"`
					Tags        struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.Len(t, resp.Result.TagDescriptions.Members, 2)
}

// TestRemoveTagsFromTG tests removing tags from a target group.
func TestRemoveTagsFromTG(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "rm-tag-tg")

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
		"Tags.member.1.Key":     {"to-remove"},
		"Tags.member.1.Value":   {"yes"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
		"TagKeys.member.1":      {"to-remove"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRemoveTagsFromListener tests removing tags from a listener.
func TestRemoveTagsFromListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rm-tag-listener-lb")
	tgArn := mustCreateTG(t, h, "rm-tag-listener-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {listenerArn},
		"Tags.member.1.Key":     {"listener-tag"},
		"Tags.member.1.Value":   {"yes"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {listenerArn},
		"TagKeys.member.1":      {"listener-tag"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestChaosHandlerMethods tests Chaos interface methods.
func TestChaosHandlerMethods(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, "elasticloadbalancingv2", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher tests the RouteMatcher function.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		body        string
		want        bool
	}{
		{
			name:        "matches_elbv2_post",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateLoadBalancer&Version=2015-12-01",
			want:        true,
		},
		{
			name:        "no_match_classic_elb_version",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateLoadBalancer&Version=2012-06-01",
			want:        false,
		},
		{
			name:        "no_match_get_request",
			method:      http.MethodGet,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Version=2015-12-01",
			want:        false,
		},
		{
			name:        "no_match_dashboard_path",
			method:      http.MethodPost,
			path:        "/dashboard/elbv2",
			contentType: "application/x-www-form-urlencoded",
			body:        "Version=2015-12-01",
			want:        false,
		},
		{
			name:        "no_match_json_content_type",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/json",
			body:        "Version=2015-12-01",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

// TestExtractOperation tests ExtractOperation.
func TestExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateLoadBalancer&Version=2015-12-01"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	assert.Equal(t, "CreateLoadBalancer", h.ExtractOperation(c))
}

// TestExtractResource tests ExtractResource.
func TestExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Name=my-alb&Version=2015-12-01"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	assert.Equal(t, "my-alb", h.ExtractResource(c))
}

// TestDeleteLoadBalancerNotFound tests delete with non-existent ARN.
func TestDeleteLoadBalancerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no-such/0"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestModifyLoadBalancerAttributesMissing tests missing ARN.
func TestModifyLoadBalancerAttributesMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"ModifyLoadBalancerAttributes"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancerAttributesMissing tests missing ARN.
func TestDescribeLoadBalancerAttributesMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeLoadBalancerAttributes"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
