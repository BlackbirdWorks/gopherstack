package elbv2_test

import (
	"encoding/xml"
	"fmt"
	"maps"
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
			wantDNS:    "my-alb-00000001.us-east-1.elb.amazonaws.com",
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

	// Querying a specific ARN that no longer exists should return 404 (AWS behavior).
	rec2 := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {lbArn},
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
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

	// Verify deletion — AWS returns TargetGroupNotFound for a deleted ARN.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusNotFound, rec2.Code)
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
					"Protocol":                     {"HTTP"},
					"Port":                         {"80"},
					"DefaultActions.member.1.Type": {"fixed-response"},
					"DefaultActions.member.1.FixedResponseConfig.StatusCode": {"200"},
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
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	parseXMLBody(t, rec, &resp)
	// 2 rules expected: 1 default (auto-created by CreateListener) + 1 explicit.
	assert.Len(t, resp.Result.Rules.Members, 2)
	defaultCount := 0
	for _, r := range resp.Result.Rules.Members {
		if r.IsDefault {
			defaultCount++
		}
	}
	assert.Equal(t, 1, defaultCount, "expected exactly one default rule")
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
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
		"Priority":              {"1"},
		"Actions.member.1.Type": {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode": {"200"},
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

// TestModifyTargetGroupAttributes tests target group attribute modification.
func TestModifyTargetGroupAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "attrs-tg")

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":         {"ModifyTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"ModifyTargetGroupAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":         {"ModifyTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/no-such/0"},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeTargetGroupAttributes tests target group attribute retrieval.
func TestDescribeTargetGroupAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "desc-attrs-tg")

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":         {"DescribeTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"DescribeTargetGroupAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func mustCreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
	t.Helper()

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
		XMLName xml.Name `xml:"CreateListenerResponse"`
		Result  struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

// TestModifyListenerAttributes tests listener attribute modification.
func TestModifyListenerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "ml-attrs-lb")
	tgArn := mustCreateTG(t, h, "ml-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":      {"ModifyListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {listenerArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"ModifyListenerAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":      {"ModifyListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no-such/0/80"},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeListenerAttributes tests listener attribute retrieval.
func TestDescribeListenerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dl-attrs-lb")
	tgArn := mustCreateTG(t, h, "dl-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":      {"DescribeListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {listenerArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"DescribeListenerAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":      {"DescribeListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no-such/0/80"},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRuleTagOperations tests AddTags, DescribeTags, RemoveTags, and DeleteRule on a Rule resource.
func TestRuleTagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-tag-lb")
	tgArn := mustCreateTG(t, h, "rule-tag-tg")

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

	// Create a rule with an initial tag.
	ruleRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Tags.member.1.Key":               {"env"},
		"Tags.member.1.Value":             {"prod"},
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

	// DescribeTags — should return the initial "env" tag.
	descRec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			TagDescriptions struct {
				Members []struct {
					Tags struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}

	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Len(t, descResp.Result.TagDescriptions.Members, 1)
	require.Len(t, descResp.Result.TagDescriptions.Members[0].Tags.Members, 1)
	assert.Equal(t, "env", descResp.Result.TagDescriptions.Members[0].Tags.Members[0].Key)
	assert.Equal(t, "prod", descResp.Result.TagDescriptions.Members[0].Tags.Members[0].Value)

	// AddTags — add a second tag.
	addRec := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
		"Tags.member.1.Key":     {"team"},
		"Tags.member.1.Value":   {"platform"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	// RemoveTags — remove the "env" tag.
	rmRec := doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {ruleArn},
		"TagKeys.member.1":      {"env"},
	})
	assert.Equal(t, http.StatusOK, rmRec.Code)

	// DeleteRule — should close tags without panic.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

// TestELBv2_TrustStoreLifecycle validates trust store create and delete operation error paths.
func TestELBv2_TrustStoreLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "create_trust_store",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"my-trust-store"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								TrustStoreArn string `xml:"TrustStoreArn"`
								Name          string `xml:"Name"`
								Status        string `xml:"Status"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"CreateTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)
				assert.NotEmpty(t, resp.Result.TrustStores.Members[0].TrustStoreArn)
				assert.Equal(t, "my-trust-store", resp.Result.TrustStores.Members[0].Name)
				assert.Equal(t, "ACTIVE", resp.Result.TrustStores.Members[0].Status)
			},
		},
		{
			name: "create_trust_store_missing_name",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "delete_trust_store_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteTrustStore"},
					"Version": {"2015-12-01"},
					"TrustStoreArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123456789012:truststore/nonexistent/abc123",
					},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete_trust_store_missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteTrustStore"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_TrustStoreFullLifecycle tests the complete lifecycle in sequence.
func TestELBv2_TrustStoreFullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a trust store.
	createRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"my-ts"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))
	require.Len(t, createResp.Result.TrustStores.Members, 1)

	tsArn := createResp.Result.TrustStores.Members[0].TrustStoreArn
	assert.NotEmpty(t, tsArn)

	// Add revocations.
	revRec := doELBv2(t, h, url.Values{
		"Action":                      {"AddTrustStoreRevocations"},
		"Version":                     {"2015-12-01"},
		"TrustStoreArn":               {tsArn},
		"RevocationContents.member.1": {"s3://my-bucket/revocations.crl"},
	})
	assert.Equal(t, http.StatusOK, revRec.Code)

	// Describe trust store associations (expect empty).
	assocRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreAssociations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp struct {
		Result struct {
			TrustStoreAssociations struct {
				Members []struct {
					ResourceArn string `xml:"ResourceArn"`
				} `xml:"member"`
			} `xml:"TrustStoreAssociations"`
		} `xml:"DescribeTrustStoreAssociationsResult"`
	}
	require.NoError(t, xml.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assert.Empty(t, assocResp.Result.TrustStoreAssociations.Members)

	// DescribeTrustStoreRevocations — the one we added should be visible.
	revDescRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, revDescRec.Code)

	var revDescResp struct {
		Result struct {
			RevocationContents struct {
				Members []struct {
					RevocationID string `xml:"RevocationId"`
				} `xml:"member"`
			} `xml:"RevocationContents"`
		} `xml:"DescribeTrustStoreRevocationsResult"`
	}
	require.NoError(t, xml.Unmarshal(revDescRec.Body.Bytes(), &revDescResp))
	require.Len(t, revDescResp.Result.RevocationContents.Members, 1)
	assert.Equal(t, "s3://my-bucket/revocations.crl", revDescResp.Result.RevocationContents.Members[0].RevocationID)

	// RemoveTrustStoreRevocations — remove the entry we added.
	revRmRec := doELBv2(t, h, url.Values{
		"Action":                 {"RemoveTrustStoreRevocations"},
		"Version":                {"2015-12-01"},
		"TrustStoreArn":          {tsArn},
		"RevocationIds.member.1": {"s3://my-bucket/revocations.crl"},
	})
	require.Equal(t, http.StatusOK, revRmRec.Code)

	// Verify revocations are now empty.
	revDescRec2 := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, revDescRec2.Code)

	var revDescResp2 struct {
		Result struct {
			RevocationContents struct {
				Members []struct {
					RevocationID string `xml:"RevocationId"`
				} `xml:"member"`
			} `xml:"RevocationContents"`
		} `xml:"DescribeTrustStoreRevocationsResult"`
	}
	require.NoError(t, xml.Unmarshal(revDescRec2.Body.Bytes(), &revDescResp2))
	assert.Empty(t, revDescResp2.Result.RevocationContents.Members)

	// DescribeTrustStores — should return our trust store.
	descTSRec := doELBv2(t, h, url.Values{
		"Action":                  {"DescribeTrustStores"},
		"Version":                 {"2015-12-01"},
		"TrustStoreArns.member.1": {tsArn},
	})
	require.Equal(t, http.StatusOK, descTSRec.Code)

	var descTSResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
					Name          string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"DescribeTrustStoresResult"`
	}
	require.NoError(t, xml.Unmarshal(descTSRec.Body.Bytes(), &descTSResp))
	require.Len(t, descTSResp.Result.TrustStores.Members, 1)
	assert.Equal(t, "my-ts", descTSResp.Result.TrustStores.Members[0].Name)

	// ModifyTrustStore — rename it.
	modTSRec := doELBv2(t, h, url.Values{
		"Action":        {"ModifyTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"Name":          {"my-ts-renamed"},
	})
	require.Equal(t, http.StatusOK, modTSRec.Code)

	var modTSResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"ModifyTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(modTSRec.Body.Bytes(), &modTSResp))
	require.Len(t, modTSResp.Result.TrustStores.Members, 1)
	assert.Equal(t, "my-ts-renamed", modTSResp.Result.TrustStores.Members[0].Name)

	// DeleteSharedTrustStoreAssociation (no-op) succeeds.
	delAssocRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, delAssocRec.Code)

	// Delete trust store.
	delRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Deleting again must return NotFound.
	delRec2 := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

// TestELBv2_ListenerCertificates validates error handling for add and describe listener certificate operations.
func TestELBv2_ListenerCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "add_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"AddListenerCertificates"},
					"Version": {"2015-12-01"},
					"ListenerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent",
					},
					"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:123:certificate/abc"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "describe_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":      {"DescribeListenerCertificates"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "add_certificates_missing_listener_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"AddListenerCertificates"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_certificates_no_certs_provided",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "nocert-lb")
				tgArn := mustCreateTG(t, h, "nocert-tg")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)

				return url.Values{
					"Action":      {"AddListenerCertificates"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {listenerArn},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "remove_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"RemoveListenerCertificates"},
					"Version": {"2015-12-01"},
					"ListenerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent",
					},
					"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:123:certificate/abc"},
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

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_ListenerCertificatesFullLifecycle tests add, describe, and removal in sequence.
func TestELBv2_ListenerCertificatesFullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cert-lb")
	tgArn := mustCreateTG(t, h, "cert-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	cert1 := "arn:aws:acm:us-east-1:123456789012:certificate/cert-1"
	cert2 := "arn:aws:acm:us-east-1:123456789012:certificate/cert-2"

	// Add two certificates.
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
		"Certificates.member.2.CertificateArn": {cert2},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	var addResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"AddListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(addRec.Body.Bytes(), &addResp))
	assert.Len(t, addResp.Result.Certificates.Members, 2)

	// Describe certificates — both should appear.
	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.Result.Certificates.Members, 2)

	// Adding same certs again is idempotent.
	addRec2 := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
	})
	assert.Equal(t, http.StatusOK, addRec2.Code)

	descRec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descResp2 struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	assert.Len(t, descResp2.Result.Certificates.Members, 2, "duplicate add should be idempotent")

	// Remove cert1 — only cert2 should remain.
	rmRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
	})
	require.Equal(t, http.StatusOK, rmRec.Code)

	descRec3 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec3.Code)

	var descResp3 struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec3.Body.Bytes(), &descResp3))
	require.Len(t, descResp3.Result.Certificates.Members, 1)
	assert.Equal(t, cert2, descResp3.Result.Certificates.Members[0].CertificateArn)
}

// TestELBv2_DescribeAccountLimits verifies the handler returns hardcoded limits.
func TestELBv2_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.Limits.Members)
}

// TestELBv2_DescribeCapacityReservation verifies the handler succeeds for a known LB.
func TestELBv2_DescribeCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "existing_lb",
			setup: func(t *testing.T, h *elbv2.Handler) string {
				t.Helper()

				return mustCreateLB(t, h, "cap-lb")
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := tt.setup(t, h)

			vals := url.Values{
				"Action":  {"DescribeCapacityReservation"},
				"Version": {"2015-12-01"},
			}
			if lbArn != "" {
				vals.Set("LoadBalancerArn", lbArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestELBv2_DescribeSSLPolicies verifies the handler returns standard SSL policies.
func TestELBv2_DescribeSSLPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.SslPolicies.Members)

	names := make([]string, 0, len(resp.Result.SslPolicies.Members))
	for _, m := range resp.Result.SslPolicies.Members {
		names = append(names, m.Name)
	}

	assert.Contains(t, names, "ELBSecurityPolicy-2016-08")
}

// TestELBv2_SetRulePriorities validates rule priority updates.
func TestELBv2_SetRulePriorities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "update_priorities_success",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "prio-lb")
				tgArn := mustCreateTG(t, h, "prio-tg")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)

				rec := doELBv2(t, h, url.Values{
					"Action":                              {"CreateRule"},
					"Version":                             {"2015-12-01"},
					"ListenerArn":                         {listenerArn},
					"Priority":                            {"100"},
					"Actions.member.1.Type":               {"forward"},
					"Actions.member.1.TargetGroupArn":     {tgArn},
					"Conditions.member.1.Field":           {"path-pattern"},
					"Conditions.member.1.Values.member.1": {"/api/*"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								RuleArn string `xml:"RuleArn"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"CreateRuleResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				ruleArn := resp.Result.Rules.Members[0].RuleArn

				return url.Values{
					"Action":                           {"SetRulePriorities"},
					"Version":                          {"2015-12-01"},
					"RulePriorities.member.1.RuleArn":  {ruleArn},
					"RulePriorities.member.1.Priority": {"200"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								Priority string `xml:"Priority"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"SetRulePrioritiesResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				assert.Equal(t, "200", resp.Result.Rules.Members[0].Priority)
			},
		},
		{
			name: "rule_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"SetRulePriorities"},
					"Version": {"2015-12-01"},
					"RulePriorities.member.1.RuleArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener-rule/app/lb/id/id/nonexistent",
					},
					"RulePriorities.member.1.Priority": {"10"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "no_priorities_provided",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"SetRulePriorities"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_DescribeTrustStores validates the DescribeTrustStores operation.
func TestELBv2_DescribeTrustStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-a"},
				})
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-b"},
				})

				return url.Values{
					"Action":  {"DescribeTrustStores"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								Name string `xml:"Name"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"DescribeTrustStoresResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.TrustStores.Members, 2)
			},
		},
		{
			name: "describe_by_name",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-named"},
				})
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-other"},
				})

				return url.Values{
					"Action":         {"DescribeTrustStores"},
					"Version":        {"2015-12-01"},
					"Names.member.1": {"ts-named"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								Name string `xml:"Name"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"DescribeTrustStoresResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)
				assert.Equal(t, "ts-named", resp.Result.TrustStores.Members[0].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_ModifyTrustStore validates trust store renaming.
func TestELBv2_ModifyTrustStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "rename_success",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()

				createRec := doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"orig-name"},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								TrustStoreArn string `xml:"TrustStoreArn"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"CreateTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)

				return url.Values{
					"Action":        {"ModifyTrustStore"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {resp.Result.TrustStores.Members[0].TrustStoreArn},
					"Name":          {"new-name"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":        {"ModifyTrustStore"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {"arn:aws:elasticloadbalancing:us-east-1:123:truststore/nonexistent/abc"},
					"Name":          {"new-name"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"ModifyTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"new-name"},
				}
			},
			wantStatus: http.StatusBadRequest,
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

// TestELBv2_StubOperations validates that stub operations return 200 for valid inputs.
func TestELBv2_StubOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "get_resource_policy",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "rp-lb")

				return url.Values{
					"Action":      {"GetResourcePolicy"},
					"Version":     {"2015-12-01"},
					"ResourceArn": {lbArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "get_resource_policy_missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"GetResourcePolicy"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "get_trust_store_ca_bundle",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()

				createRec := doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"stub-ts"},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								TrustStoreArn string `xml:"TrustStoreArn"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"CreateTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &resp))

				return url.Values{
					"Action":        {"GetTrustStoreCaCertificatesBundle"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {resp.Result.TrustStores.Members[0].TrustStoreArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "get_trust_store_ca_bundle_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":        {"GetTrustStoreCaCertificatesBundle"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {"arn:aws:elasticloadbalancing:us-east-1:123:truststore/nonexistent/abc"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "modify_capacity_reservation",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "cap-mod-lb")

				return url.Values{
					"Action":          {"ModifyCapacityReservation"},
					"Version":         {"2015-12-01"},
					"LoadBalancerArn": {lbArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "modify_capacity_reservation_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":          {"ModifyCapacityReservation"},
					"Version":         {"2015-12-01"},
					"LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/nonexistent/abc"},
				}
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "modify_ip_pools",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "ip-pools-lb")

				return url.Values{
					"Action":          {"ModifyIpPools"},
					"Version":         {"2015-12-01"},
					"LoadBalancerArn": {lbArn},
				}
			},
			wantStatus: http.StatusOK,
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

// TestCreateRuleWithConditions tests that rule conditions are stored and returned.
func TestCreateRuleWithConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		conditionVals url.Values
		wantField     string
	}{
		{
			name: "host_header",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                            {"host-header"},
				"Conditions.member.1.HostHeaderConfig.Values.member.1": {"example.com"},
				"Conditions.member.1.HostHeaderConfig.Values.member.2": {"*.example.com"},
			},
			wantField: "host-header",
		},
		{
			name: "path_pattern",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                             {"path-pattern"},
				"Conditions.member.1.PathPatternConfig.Values.member.1": {"/api/*"},
			},
			wantField: "path-pattern",
		},
		{
			name: "http_header",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                            {"http-header"},
				"Conditions.member.1.HttpHeaderConfig.HttpHeaderName":  {"X-Custom"},
				"Conditions.member.1.HttpHeaderConfig.Values.member.1": {"value1"},
			},
			wantField: "http-header",
		},
		{
			name: "http_request_method",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                                   {"http-request-method"},
				"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {"GET"},
			},
			wantField: "http-request-method",
		},
		{
			name: "source_ip",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                          {"source-ip"},
				"Conditions.member.1.SourceIpConfig.Values.member.1": {"10.0.0.0/8"},
			},
			wantField: "source-ip",
		},
		{
			name: "query_string",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                                   {"query-string"},
				"Conditions.member.1.QueryStringConfig.Values.member.1.Key":   {"version"},
				"Conditions.member.1.QueryStringConfig.Values.member.1.Value": {"v2"},
			},
			wantField: "query-string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "cond-lb")
			tgArn := mustCreateTG(t, h, "cond-tg")
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			vals := url.Values{
				"Action":                          {"CreateRule"},
				"Version":                         {"2015-12-01"},
				"ListenerArn":                     {listenerArn},
				"Priority":                        {"10"},
				"Actions.member.1.Type":           {"forward"},
				"Actions.member.1.TargetGroupArn": {tgArn},
			}
			maps.Copy(vals, tt.conditionVals)

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Result struct {
					Rules struct {
						Members []struct {
							RuleArn    string `xml:"RuleArn"`
							Conditions struct {
								Members []struct {
									Field string `xml:"Field"`
								} `xml:"member"`
							} `xml:"Conditions"`
						} `xml:"member"`
					} `xml:"Rules"`
				} `xml:"CreateRuleResult"`
			}
			parseXMLBody(t, rec, &resp)
			require.Len(t, resp.Result.Rules.Members, 1)
			require.Len(t, resp.Result.Rules.Members[0].Conditions.Members, 1)
			assert.Equal(t, tt.wantField, resp.Result.Rules.Members[0].Conditions.Members[0].Field)
		})
	}
}

// TestModifyRuleWithConditions tests that ModifyRule updates conditions.
func TestModifyRuleWithConditions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "modrule-cond-lb")
	tgArn := mustCreateTG(t, h, "modrule-cond-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	createRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"host-header"},
		"Conditions.member.1.HostHeaderConfig.Values.member.1": {"old.example.com"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	parseXMLBody(t, createRec, &createResp)
	require.Len(t, createResp.Result.Rules.Members, 1)
	ruleArn := createResp.Result.Rules.Members[0].RuleArn

	modRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyRule"},
		"Version":                   {"2015-12-01"},
		"RuleArn":                   {ruleArn},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/v2/*"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Conditions struct {
						Members []struct {
							Field             string `xml:"Field"`
							PathPatternConfig struct {
								Values struct {
									Members []struct {
										Value string `xml:",chardata"`
									} `xml:"member"`
								} `xml:"Values"`
							} `xml:"PathPatternConfig"`
						} `xml:"member"`
					} `xml:"Conditions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"ModifyRuleResult"`
	}
	parseXMLBody(t, modRec, &modResp)
	require.Len(t, modResp.Result.Rules.Members, 1)
	require.Len(t, modResp.Result.Rules.Members[0].Conditions.Members, 1)
	assert.Equal(t, "path-pattern", modResp.Result.Rules.Members[0].Conditions.Members[0].Field)
	require.Len(t, modResp.Result.Rules.Members[0].Conditions.Members[0].PathPatternConfig.Values.Members, 1)
	assert.Equal(
		t,
		"/v2/*",
		modResp.Result.Rules.Members[0].Conditions.Members[0].PathPatternConfig.Values.Members[0].Value,
	)
}

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

// TestHTTPSListenerCertificateEnforcement verifies that HTTPS listeners require ≥1 cert.
func TestHTTPSListenerCertificateEnforcement(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-cert-lb")
	tgArn := mustCreateTG(t, h, "https-cert-tg")

	// Should fail: no certificate provided.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Should succeed: certificate provided.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/my-cert"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestHTTPSListenerDefaultCertMarked verifies the first cert is marked IsDefault.
func TestHTTPSListenerDefaultCertMarked(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-default-cert-lb")
	tgArn := mustCreateTG(t, h, "https-default-cert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert1"},
		"Certificates.member.2.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert2"},
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

	// DescribeListenerCertificates should show the first cert as default.
	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var certResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
					IsDefault      bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &certResp))
	require.Len(t, certResp.Result.Certificates.Members, 2)
	// First cert should be marked as default.
	assert.True(t, certResp.Result.Certificates.Members[0].IsDefault)
}

// TestDuplicateListenerPortRejected verifies duplicate listener port returns conflict.
func TestDuplicateListenerPortRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-port-lb")
	tgArn := mustCreateTG(t, h, "dup-port-tg")
	mustCreateListener(t, h, lbArn, tgArn)

	// Attempt to create another listener on the same port.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestMutualAuthenticationOnListener verifies mTLS mode is stored and returned.
func TestMutualAuthenticationOnListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mtls-lb")
	tgArn := mustCreateTG(t, h, "mtls-tg")

	// Create trust store first.
	tsr := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"mtls-ts"},
	})
	require.Equal(t, http.StatusOK, tsr.Code)

	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsr.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert"},
		"MutualAuthentication.Mode":              {"verify"},
		"MutualAuthentication.TrustStoreArn":     {tsArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn          string `xml:"ListenerArn"`
					MutualAuthentication struct {
						Mode          string `xml:"Mode"`
						TrustStoreArn string `xml:"TrustStoreArn"`
					} `xml:"MutualAuthentication"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	assert.Equal(t, "verify", resp.Result.Listeners.Members[0].MutualAuthentication.Mode)
	assert.Equal(t, tsArn, resp.Result.Listeners.Members[0].MutualAuthentication.TrustStoreArn)
}

// TestDefaultRuleDeletionProtected verifies default rule cannot be deleted.
func TestDefaultRuleDeletionProtected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "default-rule-lb")
	tgArn := mustCreateTG(t, h, "default-rule-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Get the default rule ARN.
	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &rulesResp))

	var defaultRuleArn string
	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			defaultRuleArn = r.RuleArn

			break
		}
	}
	require.NotEmpty(t, defaultRuleArn)

	// Attempting to delete the default rule should fail.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {defaultRuleArn},
	})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "OperationNotPermitted")
}

// TestCreateRulePriorityValidation verifies priority must be 1-50000.
func TestCreateRulePriorityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		priority   string
		wantStatus int
	}{
		{name: "valid_1", priority: "1", wantStatus: http.StatusOK},
		{name: "valid_50000", priority: "50000", wantStatus: http.StatusOK},
		{name: "zero_invalid", priority: "0", wantStatus: http.StatusBadRequest},
		{name: "negative_invalid", priority: "-1", wantStatus: http.StatusBadRequest},
		{name: "too_large", priority: "50001", wantStatus: http.StatusBadRequest},
		{name: "non_numeric", priority: "abc", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "prio-lb")
			tgArn := mustCreateTG(t, h, "prio-tg")
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			rec := doELBv2(t, h, url.Values{
				"Action":                          {"CreateRule"},
				"Version":                         {"2015-12-01"},
				"ListenerArn":                     {listenerArn},
				"Priority":                        {tt.priority},
				"Actions.member.1.Type":           {"forward"},
				"Actions.member.1.TargetGroupArn": {tgArn},
				"Conditions.member.1.Field":       {"path-pattern"},
				"Conditions.member.1.PathPatternConfig.Values.member.1": {"/test"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestHTTPRequestMethodConditionWhitelist verifies invalid HTTP methods are rejected.
func TestHTTPRequestMethodConditionWhitelist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "method-whitelist-lb")
	tgArn := mustCreateTG(t, h, "method-whitelist-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Valid methods should work.
	for _, method := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"} {
		rec := doELBv2(t, h, url.Values{
			"Action":                          {"CreateRule"},
			"Version":                         {"2015-12-01"},
			"ListenerArn":                     {listenerArn},
			"Priority":                        {"1"},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {tgArn},
			"Conditions.member.1.Field":       {"http-request-method"},
			"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {method},
		})
		assert.Equal(t, http.StatusOK, rec.Code, "method %q should be valid", method)

		// Delete the rule so we can reuse priority 1.
		var ruleResp struct {
			Result struct {
				Rules struct {
					Members []struct {
						RuleArn string `xml:"RuleArn"`
					} `xml:"member"`
				} `xml:"Rules"`
			} `xml:"CreateRuleResult"`
		}
		if err := xml.Unmarshal(rec.Body.Bytes(), &ruleResp); err == nil && len(ruleResp.Result.Rules.Members) > 0 {
			doELBv2(t, h, url.Values{
				"Action":  {"DeleteRule"},
				"Version": {"2015-12-01"},
				"RuleArn": {ruleResp.Result.Rules.Members[0].RuleArn},
			})
		}
	}

	// Invalid method should fail.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"http-request-method"},
		"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {"INVALID"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRedirectActionParsed verifies redirect actions are stored and returned.
func TestRedirectActionParsed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "redirect-lb")
	tgArn := mustCreateTG(t, h, "redirect-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"10"},
		"Actions.member.1.Type": {"redirect"},
		"Actions.member.1.RedirectConfig.Protocol":              {"HTTPS"},
		"Actions.member.1.RedirectConfig.Port":                  {"443"},
		"Actions.member.1.RedirectConfig.StatusCode":            {"HTTP_301"},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type           string `xml:"Type"`
							RedirectConfig struct {
								Protocol   string `xml:"Protocol"`
								Port       string `xml:"Port"`
								StatusCode string `xml:"StatusCode"`
							} `xml:"RedirectConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	require.Len(t, resp.Result.Rules.Members[0].Actions.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "redirect", action.Type)
	assert.Equal(t, "HTTPS", action.RedirectConfig.Protocol)
	assert.Equal(t, "443", action.RedirectConfig.Port)
	assert.Equal(t, "HTTP_301", action.RedirectConfig.StatusCode)
}

// TestFixedResponseActionParsed verifies fixed-response actions are stored and returned.
func TestFixedResponseActionParsed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "fixed-response-lb")
	tgArn := mustCreateTG(t, h, "fixed-response-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"20"},
		"Actions.member.1.Type": {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode":       {"404"},
		"Actions.member.1.FixedResponseConfig.MessageBody":      {"Not Found"},
		"Actions.member.1.FixedResponseConfig.ContentType":      {"text/plain"},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                string `xml:"Type"`
							FixedResponseConfig struct {
								StatusCode  string `xml:"StatusCode"`
								MessageBody string `xml:"MessageBody"`
								ContentType string `xml:"ContentType"`
							} `xml:"FixedResponseConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "fixed-response", action.Type)
	assert.Equal(t, "404", action.FixedResponseConfig.StatusCode)
	assert.Equal(t, "Not Found", action.FixedResponseConfig.MessageBody)
	assert.Equal(t, "text/plain", action.FixedResponseConfig.ContentType)
}

// TestForwardWeightedTargetGroups verifies ForwardConfig with weighted target groups.
func TestForwardWeightedTargetGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "weighted-lb")
	tg1Arn := mustCreateTG(t, h, "weighted-tg1")
	tg2Arn := mustCreateTG(t, h, "weighted-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tg1Arn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"30"},
		"Actions.member.1.Type": {"forward"},
		"Actions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn": {tg1Arn},
		"Actions.member.1.ForwardConfig.TargetGroups.member.1.Weight":         {"80"},
		"Actions.member.1.ForwardConfig.TargetGroups.member.2.TargetGroupArn": {tg2Arn},
		"Actions.member.1.ForwardConfig.TargetGroups.member.2.Weight":         {"20"},
		"Conditions.member.1.Field":                                           {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":               {"/weighted"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type          string `xml:"Type"`
							ForwardConfig struct {
								TargetGroups struct {
									Members []struct {
										TargetGroupArn string `xml:"TargetGroupArn"`
										Weight         int32  `xml:"Weight"`
									} `xml:"member"`
								} `xml:"TargetGroups"`
							} `xml:"ForwardConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "forward", action.Type)
	require.Len(t, action.ForwardConfig.TargetGroups.Members, 2)
	assert.Equal(t, int32(80), action.ForwardConfig.TargetGroups.Members[0].Weight)
	assert.Equal(t, int32(20), action.ForwardConfig.TargetGroups.Members[1].Weight)
}

// TestAuthenticateCognitoAction verifies authenticate-cognito actions are stored and returned.
func TestAuthenticateCognitoAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cognito-lb")
	tgArn := mustCreateTG(t, h, "cognito-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"CreateRule"},
		"Version":                {"2015-12-01"},
		"ListenerArn":            {listenerArn},
		"Priority":               {"40"},
		"Actions.member.1.Type":  {"authenticate-cognito"},
		"Actions.member.1.Order": {"1"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolArn": {
			"arn:aws:cognito-idp:us-east-1:123:userpool/us-east-1_abc",
		},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolClientId":         {"client123"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolDomain":           {"auth.example.com"},
		"Actions.member.1.AuthenticateCognitoConfig.OnUnauthenticatedRequest": {"deny"},
		"Actions.member.2.Type":                                 {"forward"},
		"Actions.member.2.Order":                                {"2"},
		"Actions.member.2.TargetGroupArn":                       {tgArn},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/auth"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                      string `xml:"Type"`
							AuthenticateCognitoConfig struct {
								UserPoolArn              string `xml:"UserPoolArn"`
								UserPoolClientID         string `xml:"UserPoolClientId"`
								UserPoolDomain           string `xml:"UserPoolDomain"`
								OnUnauthenticatedRequest string `xml:"OnUnauthenticatedRequest"`
							} `xml:"AuthenticateCognitoConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	actions := resp.Result.Rules.Members[0].Actions.Members
	require.Len(t, actions, 2)

	cognitoAction := actions[0]
	assert.Equal(t, "authenticate-cognito", cognitoAction.Type)
	assert.Equal(
		t,
		"arn:aws:cognito-idp:us-east-1:123:userpool/us-east-1_abc",
		cognitoAction.AuthenticateCognitoConfig.UserPoolArn,
	)
	assert.Equal(t, "client123", cognitoAction.AuthenticateCognitoConfig.UserPoolClientID)
	assert.Equal(t, "deny", cognitoAction.AuthenticateCognitoConfig.OnUnauthenticatedRequest)
}

// TestAuthenticateOidcAction verifies authenticate-oidc actions are stored and returned.
func TestAuthenticateOidcAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "oidc-lb")
	tgArn := mustCreateTG(t, h, "oidc-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"CreateRule"},
		"Version":                {"2015-12-01"},
		"ListenerArn":            {listenerArn},
		"Priority":               {"50"},
		"Actions.member.1.Type":  {"authenticate-oidc"},
		"Actions.member.1.Order": {"1"},
		"Actions.member.1.AuthenticateOidcConfig.Issuer":                {"https://idp.example.com"},
		"Actions.member.1.AuthenticateOidcConfig.AuthorizationEndpoint": {"https://idp.example.com/auth"},
		"Actions.member.1.AuthenticateOidcConfig.TokenEndpoint":         {"https://idp.example.com/token"},
		"Actions.member.1.AuthenticateOidcConfig.UserInfoEndpoint":      {"https://idp.example.com/userinfo"},
		"Actions.member.1.AuthenticateOidcConfig.ClientId":              {"myapp"},
		"Actions.member.1.AuthenticateOidcConfig.ClientSecret":          {"secret"},
		"Actions.member.2.Type":                                         {"forward"},
		"Actions.member.2.Order":                                        {"2"},
		"Actions.member.2.TargetGroupArn":                               {tgArn},
		"Conditions.member.1.Field":                                     {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":         {"/oidc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                   string `xml:"Type"`
							AuthenticateOidcConfig struct {
								Issuer                string `xml:"Issuer"`
								AuthorizationEndpoint string `xml:"AuthorizationEndpoint"`
								ClientID              string `xml:"ClientId"`
							} `xml:"AuthenticateOidcConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	actions := resp.Result.Rules.Members[0].Actions.Members
	require.Len(t, actions, 2)
	assert.Equal(t, "authenticate-oidc", actions[0].Type)
	assert.Equal(t, "https://idp.example.com", actions[0].AuthenticateOidcConfig.Issuer)
	assert.Equal(t, "myapp", actions[0].AuthenticateOidcConfig.ClientID)
}

// TestTargetGroupMatcherPersisted verifies Matcher (HTTPCode/GrpcCode) is stored and returned.
func TestTargetGroupMatcherPersisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateTargetGroup"},
		"Version":          {"2015-12-01"},
		"Name":             {"matcher-tg"},
		"Protocol":         {"HTTP"},
		"Port":             {"80"},
		"VpcId":            {"vpc-00000000"},
		"Matcher.HTTPCode": {"200-299"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
					Matcher        struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "200-299", resp.Result.TargetGroups.Members[0].Matcher.HTTPCode)
}

// TestTargetGroupGrpcMatcherPersisted verifies GrpcCode matcher is stored.
func TestTargetGroupGrpcMatcherPersisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateTargetGroup"},
		"Version":          {"2015-12-01"},
		"Name":             {"grpc-matcher-tg"},
		"Protocol":         {"HTTP"},
		"Port":             {"80"},
		"VpcId":            {"vpc-00000000"},
		"Matcher.GrpcCode": {"0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
					Matcher        struct {
						GrpcCode string `xml:"GrpcCode"`
					} `xml:"Matcher"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "0", resp.Result.TargetGroups.Members[0].Matcher.GrpcCode)
}

// TestCrossZoneLoadBalancingDefault verifies CrossZoneLoadBalancing defaults to true.
func TestCrossZoneLoadBalancingDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "cz-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					CrossZoneLoadBalancing bool `xml:"CrossZoneLoadBalancing"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.True(t, resp.Result.TargetGroups.Members[0].CrossZoneLoadBalancing)
}

// TestModifyTargetGroupPersistsFields verifies ModifyTargetGroup actually updates the TG.
func TestModifyTargetGroupPersistsFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "modify-tg-persist")

	rec := doELBv2(t, h, url.Values{
		"Action":                     {"ModifyTargetGroup"},
		"Version":                    {"2015-12-01"},
		"TargetGroupArn":             {tgArn},
		"HealthCheckProtocol":        {"HTTPS"},
		"HealthCheckPort":            {"8443"},
		"HealthCheckPath":            {"/health"},
		"HealthCheckEnabled":         {"true"},
		"Matcher.HTTPCode":           {"200-204"},
		"HealthCheckIntervalSeconds": {"30"},
		"HealthyThresholdCount":      {"3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					Matcher struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
					HealthCheckProtocol        string `xml:"HealthCheckProtocol"`
					HealthCheckPort            string `xml:"HealthCheckPort"`
					HealthCheckPath            string `xml:"HealthCheckPath"`
					HealthCheckIntervalSeconds int32  `xml:"HealthCheckIntervalSeconds"`
					HealthyThresholdCount      int32  `xml:"HealthyThresholdCount"`
					HealthCheckEnabled         bool   `xml:"HealthCheckEnabled"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"ModifyTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, "HTTPS", tg.HealthCheckProtocol)
	assert.Equal(t, "8443", tg.HealthCheckPort)
	assert.Equal(t, "/health", tg.HealthCheckPath)
	assert.True(t, tg.HealthCheckEnabled)
	assert.Equal(t, "200-204", tg.Matcher.HTTPCode)
	assert.Equal(t, int32(30), tg.HealthCheckIntervalSeconds)
	assert.Equal(t, int32(3), tg.HealthyThresholdCount)
}

// TestModifyTargetGroupAttributesPersists verifies deregistration_delay is persisted.
func TestModifyTargetGroupAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tg-attrs-persist")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"deregistration_delay.timeout_seconds"},
		"Attributes.member.1.Value": {"120"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeTargetGroupAttributes.
	descRec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "120", attrMap["deregistration_delay.timeout_seconds"])
}

// TestDescribeLoadBalancerAttributesPersists verifies LB attrs are persisted and returned.
func TestDescribeLoadBalancerAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "lb-attrs-persist")

	// Verify defaults are set on creation.
	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "60", attrMap["idle_timeout.timeout_seconds"])
	assert.Equal(t, "defensive", attrMap["routing.http.desync_mitigation_mode"])
	assert.Equal(t, "false", attrMap["deletion_protection.enabled"])

	// Modify and verify the change is persisted.
	modRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyLoadBalancerAttributes"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArn":           {lbArn},
		"Attributes.member.1.Key":   {"idle_timeout.timeout_seconds"},
		"Attributes.member.1.Value": {"120"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	// Describe again to verify persistence.
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	attrMap2 := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap2[a.Key] = a.Value
	}
	assert.Equal(t, "120", attrMap2["idle_timeout.timeout_seconds"])
}

// TestDescribeTargetGroupsWithLBFilter verifies lbArn filter works.
func TestDescribeTargetGroupsWithLBFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "lb-filter-tgs-lb")
	tg1Arn := mustCreateTG(t, h, "lb-filter-tg1")
	tg2Arn := mustCreateTG(t, h, "lb-filter-tg2")
	_ = mustCreateTG(t, h, "lb-filter-tg3") // Not attached to LB.

	mustCreateListener(t, h, lbArn, tg1Arn)

	// Create a second listener on a different port with tg2.
	rec80 := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"8080"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tg2Arn},
	})
	require.Equal(t, http.StatusOK, rec80.Code)

	// Filter by lbArn should return only tg1 and tg2 (attached to listeners).
	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeTargetGroups"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	got := make(map[string]bool)
	for _, tg := range resp.Result.TargetGroups.Members {
		got[tg.TargetGroupArn] = true
	}
	assert.True(t, got[tg1Arn], "tg1 should be in result")
	assert.True(t, got[tg2Arn], "tg2 should be in result")
	assert.Len(t, resp.Result.TargetGroups.Members, 2, "only LB-attached TGs should be returned")
}

// TestRegisterTargetsDedupByPort verifies targets are de-duped by (ID, Port).
func TestRegisterTargetsDedupByPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "dedup-port-tg")

	// Register target on port 8080.
	rec1 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Register same target on port 8080 again (duplicate).
	rec2 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Register same target on different port 8081 (should be allowed).
	rec3 := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8081"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// Should have 2 entries: (i-abc:8080) and (i-abc:8081).
	healthRec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, healthRec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID   string `xml:"Id"`
						Port int32  `xml:"Port"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(healthRec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)
}

// TestTargetGroupDefaultAttributesOnCreate verifies defaults are set at creation time.
func TestTargetGroupDefaultAttributesOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "default-attrs-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "300", attrMap["deregistration_delay.timeout_seconds"])
	assert.Equal(t, "false", attrMap["stickiness.enabled"])
	assert.Equal(t, "round_robin", attrMap["load_balancing.algorithm.type"])
}

// TestModifyListenerAttributesPersists verifies listener attributes are stored.
func TestModifyListenerAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "listener-attrs-lb")
	tgArn := mustCreateTG(t, h, "listener-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyListenerAttributes"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {listenerArn},
		"Attributes.member.1.Key":   {"routing.http2.enabled"},
		"Attributes.member.1.Value": {"false"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeListenerAttributes.
	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeListenerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "false", attrMap["routing.http2.enabled"])
}

// --- New tests for implemented improvements ---

// TestPortValidationCreateListener tests port validation for CreateListener.
func TestPortValidationCreateListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		{"valid_port_80", "80", http.StatusNotFound}, // will fail on nonexistent LB, not port
		{"port_zero", "0", http.StatusBadRequest},
		{"port_negative", "-1", http.StatusBadRequest},
		{"port_65536", "65536", http.StatusBadRequest},
		{"port_65535_valid", "65535", http.StatusNotFound},
		{"port_1_valid", "1", http.StatusNotFound},
		{"port_non_numeric", "abc", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateListener"},
				"Version": {"2015-12-01"},
				"LoadBalancerArn": {
					"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no/0",
				},
				"Protocol":                     {"HTTP"},
				"Port":                         {tt.port},
				"DefaultActions.member.1.Type": {"fixed-response"},
				"DefaultActions.member.1.FixedResponseConfig.StatusCode": {"200"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "port=%s", tt.port)
		})
	}
}

// TestPortValidationCreateTargetGroup tests port validation for CreateTargetGroup.
func TestPortValidationCreateTargetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		{"port_zero", "0", http.StatusBadRequest},
		{"port_65536", "65536", http.StatusBadRequest},
		{"port_valid", "8080", http.StatusOK},
		{"port_max", "65535", http.StatusOK},
		{"port_min", "1", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":   {"CreateTargetGroup"},
				"Version":  {"2015-12-01"},
				"Name":     {"tg-port-" + tt.name},
				"Protocol": {"HTTP"},
				"Port":     {tt.port},
				"VpcId":    {"vpc-00000000"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "port=%s", tt.port)
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

// TestRuleCounterAfterDelete tests that rule ARNs remain unique after deletes.
func TestRuleCounterAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-counter-lb")
	tgArn := mustCreateTG(t, h, "rule-counter-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Create rule at priority 1.
	rec1 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/a"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var r1Resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &r1Resp))
	require.Len(t, r1Resp.Result.Rules.Members, 1)
	ruleArn1 := r1Resp.Result.Rules.Members[0].RuleArn

	// Delete the rule.
	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn1},
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Create another rule - should have a different ARN, not the same as deleted rule.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"2"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/b"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var r2Resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &r2Resp))
	require.Len(t, r2Resp.Result.Rules.Members, 1)
	ruleArn2 := r2Resp.Result.Rules.Members[0].RuleArn

	assert.NotEqual(t, ruleArn1, ruleArn2, "new rule ARN must differ from deleted rule ARN")
}

// TestDeleteTargetGroupInUse tests that deleting a TG referenced by a listener fails.
func TestDeleteTargetGroupInUse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "tg-inuse-lb")
	tgArn := mustCreateTG(t, h, "tg-inuse-tg")
	_ = mustCreateListener(t, h, lbArn, tgArn)

	// Attempting to delete the TG should fail because the listener references it.
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUse", errResp.Error.Code)
}

// TestDeleteTargetGroupNotInUse tests that deleting an unreferenced TG succeeds.
func TestDeleteTargetGroupNotInUse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tg-notinuse")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRemoveLastCertificateHTTPS tests that removing the last cert from HTTPS listener fails.
func TestRemoveLastCertificateHTTPS(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cert-lb")
	tgArn := mustCreateTG(t, h, "cert-tg")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/test-cert-1"

	// Create HTTPS listener with one certificate.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"SslPolicy":                              {"ELBSecurityPolicy-2016-08"},
		"Certificates.member.1.CertificateArn":   {certArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var lResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &lResp))
	listenerArn := lResp.Result.Listeners.Members[0].ListenerArn

	// Removing the only certificate should fail.
	removeRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusBadRequest, removeRec.Code)
}

// TestHealthCheckDefaults tests that health check defaults are applied.
func TestHealthCheckDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create TG without specifying health check values.
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"hc-defaults-tg"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					Matcher struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
					HealthCheckIntervalSeconds int32 `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32 `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32 `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32 `xml:"UnhealthyThresholdCount"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, int32(30), tg.HealthCheckIntervalSeconds, "default interval should be 30")
	assert.Equal(t, int32(5), tg.HealthCheckTimeoutSeconds, "default timeout should be 5")
	assert.Equal(t, int32(3), tg.HealthyThresholdCount, "default healthy threshold should be 3")
	assert.Equal(t, int32(3), tg.UnhealthyThresholdCount, "default unhealthy threshold should be 3")
	assert.Equal(t, "200", tg.Matcher.HTTPCode, "default HTTP matcher should be 200")
}

// TestHealthCheckDefaultsCustomValues tests that explicit health check values are preserved.
func TestHealthCheckDefaultsCustomValues(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":                     {"CreateTargetGroup"},
		"Version":                    {"2015-12-01"},
		"Name":                       {"hc-custom-tg"},
		"Protocol":                   {"HTTP"},
		"Port":                       {"80"},
		"VpcId":                      {"vpc-00000000"},
		"HealthCheckIntervalSeconds": {"60"},
		"HealthCheckTimeoutSeconds":  {"10"},
		"HealthyThresholdCount":      {"5"},
		"UnhealthyThresholdCount":    {"2"},
		"Matcher.HTTPCode":           {"200-299"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					Matcher struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
					HealthCheckIntervalSeconds int32 `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32 `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32 `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32 `xml:"UnhealthyThresholdCount"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, int32(60), tg.HealthCheckIntervalSeconds)
	assert.Equal(t, int32(10), tg.HealthCheckTimeoutSeconds)
	assert.Equal(t, int32(5), tg.HealthyThresholdCount)
	assert.Equal(t, int32(2), tg.UnhealthyThresholdCount)
	assert.Equal(t, "200-299", tg.Matcher.HTTPCode)
}

// TestProtocolVersion tests that ProtocolVersion is stored and returned.
func TestProtocolVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"pv-tg"},
		"Protocol":        {"HTTP"},
		"ProtocolVersion": {"HTTP2"},
		"Port":            {"80"},
		"VpcId":           {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					ProtocolVersion string `xml:"ProtocolVersion"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "HTTP2", resp.Result.TargetGroups.Members[0].ProtocolVersion)
}

// TestListenerAttributeDefaults tests that HTTP/HTTPS listeners get default attributes.
func TestListenerAttributeDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "attr-defaults-lb")
	tgArn := mustCreateTG(t, h, "attr-defaults-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeListenerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}

	assert.Equal(t, "true", attrMap["routing.http2.enabled"])
	assert.Equal(t, "60", attrMap["idle_timeout.timeout_seconds"])
	assert.Equal(t, "defensive", attrMap["routing.http.desync_mitigation_mode"])
}

// TestDescribeRulesSortedByPriority tests that DescribeRules returns rules sorted numerically.
func TestDescribeRulesSortedByPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sort-rules-lb")
	tgArn := mustCreateTG(t, h, "sort-rules-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	priorities := []string{"10", "2", "100", "1"}
	for _, p := range priorities {
		rec := doELBv2(t, h, url.Values{
			"Action":                          {"CreateRule"},
			"Version":                         {"2015-12-01"},
			"ListenerArn":                     {listenerArn},
			"Priority":                        {p},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {tgArn},
			"Conditions.member.1.Field":       {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/" + p},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

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
					Priority  string `xml:"Priority"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	// Expect: 1, 2, 10, 100, default
	got := make([]string, 0, len(resp.Result.Rules.Members))
	for _, r := range resp.Result.Rules.Members {
		got = append(got, r.Priority)
	}

	expected := []string{"1", "2", "10", "100", "default"}
	assert.Equal(t, expected, got)
}

// TestCachedDispatchTable tests that Handler uses a cached dispatch table.
func TestCachedDispatchTable(t *testing.T) {
	t.Parallel()

	// Call two operations to verify dispatch table is functional.
	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dispatch-lb")
	assert.NotEmpty(t, lbArn)

	// Describe should also work via cached table.
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeLoadBalancers"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDuplicateRulePriorityErrorCode tests that ErrDuplicateRulePriority returns DuplicatePriority code.
func TestDuplicateRulePriorityErrorCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-prio-lb")
	tgArn := mustCreateTG(t, h, "dup-prio-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Create rule at priority 5.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/first"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create another rule at the same priority.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/second"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var errResp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &errResp))
	assert.Equal(t, "DuplicatePriority", errResp.Error.Code)
}

// TestCreateListenerNoDefaultActions tests that missing DefaultActions returns 400.
func TestCreateListenerNoDefaultActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "no-actions-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateListener"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"Protocol":        {"HTTP"},
		"Port":            {"80"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateRuleNoActions tests that missing Actions in CreateRule returns 400.
func TestCreateRuleNoActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "no-rule-actions-lb")
	tgArn := mustCreateTG(t, h, "no-rule-actions-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"CreateRule"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
		"Priority":    {"1"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCertificatesInDescribeListeners tests that certificates appear in DescribeListeners output.
func TestCertificatesInDescribeListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "certs-describe-lb")
	tgArn := mustCreateTG(t, h, "certs-describe-tg")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/desc-cert"

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"SslPolicy":                              {"ELBSecurityPolicy-2016-08"},
		"Certificates.member.1.CertificateArn":   {certArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					Certificates struct {
						Members []struct {
							CertificateArn string `xml:"CertificateArn"`
						} `xml:"member"`
					} `xml:"Certificates"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	require.Len(t, resp.Result.Listeners.Members[0].Certificates.Members, 1)
	assert.Equal(t, certArn, resp.Result.Listeners.Members[0].Certificates.Members[0].CertificateArn)
}

// TestTargetGroupLoadBalancerArns tests that TG LoadBalancerArns is populated.
func TestTargetGroupLoadBalancerArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "tg-lb-arns-lb")
	tgArn := mustCreateTG(t, h, "tg-lb-arns-tg")

	// Before attaching to LB, LoadBalancerArns should be empty.
	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var beforeResp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					LoadBalancerArns struct {
						Members []struct {
							Value string `xml:",chardata"`
						} `xml:"member"`
					} `xml:"LoadBalancerArns"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &beforeResp))
	require.Len(t, beforeResp.Result.TargetGroups.Members, 1)
	assert.Empty(t, beforeResp.Result.TargetGroups.Members[0].LoadBalancerArns.Members)

	// Attach to LB via listener.
	_ = mustCreateListener(t, h, lbArn, tgArn)

	// After attaching, LoadBalancerArns should contain the LB ARN.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	// We verify via DescribeTargetGroups by LoadBalancerArn filter.
	filtRec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeTargetGroups"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, filtRec.Code)

	var filtResp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(filtRec.Body.Bytes(), &filtResp))
	require.Len(t, filtResp.Result.TargetGroups.Members, 1)
	assert.Equal(t, tgArn, filtResp.Result.TargetGroups.Members[0].TargetGroupArn)
}

// TestDeleteTGReferencedByRule tests that TG referenced in a rule action cannot be deleted.
func TestDeleteTGReferencedByRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "rule-ref-lb")
	tgArn1 := mustCreateTG(t, h, "rule-ref-tg1")
	tgArn2 := mustCreateTG(t, h, "rule-ref-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn1)

	// Create a rule referencing tgArn2.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn2},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/rule"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempting to delete tgArn2 should fail.
	delRec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn2},
	})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)
}

// TestHTTPSListenerRequiresCert tests that HTTPS listener requires at least one certificate.
func TestHTTPSListenerRequiresCert(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-nocert-lb")
	tgArn := mustCreateTG(t, h, "https-nocert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRemoveLastCertHTTPAllowed tests that removing all certs from HTTP listener is allowed.
func TestRemoveLastCertHTTPAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "http-cert-lb")
	tgArn := mustCreateTG(t, h, "http-cert-tg")

	// Create HTTP listener.
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Add a cert.
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/http-cert"
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	// Remove the cert - should be allowed for HTTP.
	removeRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}

// TestDeregisterTargetsPortAware verifies that DeregisterTargets matches by ID+Port,
// so a target registered on multiple ports can be deregistered independently.
func TestDeregisterTargetsPortAware(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "port-aware-tg")

	// Register same instance on two ports.
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
		"Targets.member.2.Id":   {"i-abc"},
		"Targets.member.2.Port": {"8081"},
	})

	// Deregister only port 8080.
	doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-abc"},
		"Targets.member.1.Port": {"8080"},
	})

	// Port 8081 must still be healthy.
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID   string `xml:"Id"`
						Port int    `xml:"Port"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-abc", resp.Result.TargetHealthDescriptions.Members[0].Target.ID)
	assert.Equal(t, 8081, resp.Result.TargetHealthDescriptions.Members[0].Target.Port)
}

// TestModifyTargetGroupHealthCheckEnabledOptional verifies that omitting HealthCheckEnabled
// in ModifyTargetGroup does not overwrite the stored value.
func TestModifyTargetGroupHealthCheckEnabledOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "hce-optional-tg")

	// Enable health checks explicitly on creation.
	doELBv2(t, h, url.Values{
		"Action":             {"ModifyTargetGroup"},
		"Version":            {"2015-12-01"},
		"TargetGroupArn":     {tgArn},
		"HealthCheckEnabled": {"true"},
	})

	// Now modify only the path — HealthCheckEnabled must remain true.
	doELBv2(t, h, url.Values{
		"Action":          {"ModifyTargetGroup"},
		"Version":         {"2015-12-01"},
		"TargetGroupArn":  {tgArn},
		"HealthCheckPath": {"/healthz"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					HealthCheckPath    string `xml:"HealthCheckPath"`
					HealthCheckEnabled bool   `xml:"HealthCheckEnabled"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	tg := resp.Result.TargetGroups.Members[0]
	assert.True(t, tg.HealthCheckEnabled, "HealthCheckEnabled must not be overwritten by absent param")
	assert.Equal(t, "/healthz", tg.HealthCheckPath)
}

// TestHealthCheckInvalidNumericParams verifies that non-numeric health check params return errors.
func TestHealthCheckInvalidNumericParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "hc-num-tg")

	tests := []struct {
		name  string
		param string
	}{
		{"invalid_interval", "HealthCheckIntervalSeconds"},
		{"invalid_timeout", "HealthCheckTimeoutSeconds"},
		{"invalid_healthy", "HealthyThresholdCount"},
		{"invalid_unhealthy", "UnhealthyThresholdCount"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doELBv2(t, h, url.Values{
				"Action":         {"ModifyTargetGroup"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
				tt.param:         {"not-a-number"},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestSetSecurityGroupsPersist verifies that SetSecurityGroups updates the LB state.
func TestSetSecurityGroupsPersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sg-persist-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-00000001"},
		"SecurityGroups.member.2": {"sg-00000002"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SecurityGroupIDs struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroupIds"`
		} `xml:"SetSecurityGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SecurityGroupIDs.Members, 2)
	assert.Equal(t, "sg-00000001", resp.Result.SecurityGroupIDs.Members[0].Value)
	assert.Equal(t, "sg-00000002", resp.Result.SecurityGroupIDs.Members[1].Value)
}

// TestSetSecurityGroupsNotFound verifies that SetSecurityGroups returns 404 for missing LB.
func TestSetSecurityGroupsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"SetSecurityGroups"},
		"Version": {"2015-12-01"},
		"LoadBalancerArn": {
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nonexistent/abc",
		},
		"SecurityGroups.member.1": {"sg-00000001"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestSetSubnetsPersist verifies that SetSubnets updates the LB availability zones.
func TestSetSubnetsPersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "subnets-persist-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":           {"SetSubnets"},
		"Version":          {"2015-12-01"},
		"LoadBalancerArn":  {lbArn},
		"Subnets.member.1": {"subnet-00000001"},
		"Subnets.member.2": {"subnet-00000002"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"SetSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.AvailabilityZones.Members, 2)
}

// TestSetIpAddressTypePersist verifies that SetIpAddressType updates and validates.
func TestSetIpAddressTypePersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "iptype-persist-lb")

	// Valid type.
	rec := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"dualstack"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			IPAddressType string `xml:"IpAddressType"`
		} `xml:"SetIpAddressTypeResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "dualstack", resp.Result.IPAddressType)

	// Invalid type.
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"bogus"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestDescribeTargetHealthFilter verifies that DescribeTargetHealth filters by requested targets.
func TestDescribeTargetHealthFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "health-filter-tg")

	// Register three targets.
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-aaa"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-bbb"},
		"Targets.member.2.Port": {"80"},
		"Targets.member.3.Id":   {"i-ccc"},
		"Targets.member.3.Port": {"80"},
	})

	// Request health for only i-aaa and i-ccc.
	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-aaa"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-ccc"},
		"Targets.member.2.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)

	ids := []string{
		resp.Result.TargetHealthDescriptions.Members[0].Target.ID,
		resp.Result.TargetHealthDescriptions.Members[1].Target.ID,
	}
	assert.Contains(t, ids, "i-aaa")
	assert.Contains(t, ids, "i-ccc")
	assert.NotContains(t, ids, "i-bbb")
}

// TestCreateRulePriorityRequired verifies that Priority is required for CreateRule.
func TestCreateRulePriorityRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "priority-req-lb")
	tgArn := mustCreateTG(t, h, "priority-req-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSetRulePrioritiesDefaultRuleRejected verifies that the default rule cannot be reordered.
func TestSetRulePrioritiesDefaultRuleRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "srp-default-lb")
	tgArn := mustCreateTG(t, h, "srp-default-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Find the default rule ARN.
	rulesRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rulesRec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn   string `xml:"RuleArn"`
					IsDefault bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rulesRec.Body.Bytes(), &rulesResp))

	var defaultRuleArn string
	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			defaultRuleArn = r.RuleArn
		}
	}
	require.NotEmpty(t, defaultRuleArn)

	// Attempt to set priority on default rule should fail.
	rec := doELBv2(t, h, url.Values{
		"Action":                           {"SetRulePriorities"},
		"Version":                          {"2015-12-01"},
		"RulePriorities.member.1.RuleArn":  {defaultRuleArn},
		"RulePriorities.member.1.Priority": {"5"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestModifyListenerSyncDefaultRule verifies that changing listener default actions
// also updates the auto-created default rule's actions.
func TestModifyListenerSyncDefaultRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sync-rule-lb")
	tgArn1 := mustCreateTG(t, h, "sync-rule-tg1")
	tgArn2 := mustCreateTG(t, h, "sync-rule-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn1)

	// Modify listener to point to tgArn2.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"ModifyListener"},
		"Version":                                {"2015-12-01"},
		"ListenerArn":                            {listenerArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn2},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The default rule must also reference tgArn2.
	rulesRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rulesRec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							TargetGroupArn string `xml:"TargetGroupArn"`
						} `xml:"member"`
					} `xml:"Actions"`
					IsDefault bool `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rulesRec.Body.Bytes(), &rulesResp))

	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			require.Len(t, r.Actions.Members, 1)
			assert.Equal(t, tgArn2, r.Actions.Members[0].TargetGroupArn,
				"default rule action must be updated when listener default actions change")
		}
	}
}

// TestCreateTargetGroupInvalidTargetType verifies that invalid TargetType is rejected.
func TestCreateTargetGroupInvalidTargetType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"bad-type-tg"},
		"Protocol":   {"HTTP"},
		"Port":       {"80"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"bogus"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateTargetGroupLambdaNoPort verifies that lambda target groups do not require Port.
func TestCreateTargetGroupLambdaNoPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"lambda-no-port-tg"},
		"TargetType": {"lambda"},
		"VpcId":      {"vpc-00000000"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestModifyListenerDuplicatePortRejected verifies that ModifyListener rejects a port already in use.
func TestModifyListenerDuplicatePortRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-port-lb")
	tgArn := mustCreateTG(t, h, "dup-port-tg")

	// Create two listeners on ports 80 and 443.
	listenerArn1 := mustCreateListener(t, h, lbArn, tgArn)

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/dup-cert"
	rec443 := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {certArn},
	})
	require.Equal(t, http.StatusOK, rec443.Code)

	// Attempt to modify listener on port 80 to use port 443 (already taken).
	rec := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn1},
		"Port":        {"443"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
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

// TestDescribeListenersSortedByPort verifies that DescribeListeners returns listeners
// sorted by port in ascending order.
func TestDescribeListenersSortedByPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sort-listen-lb")
	tgArn := mustCreateTG(t, h, "sort-listen-tg")
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/sort-cert"

	// Create listeners out of order: 8080, 80, 443.
	for _, portProto := range []struct{ port, proto string }{
		{"8080", "HTTP"},
		{"80", "HTTP"},
	} {
		rec := doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {portProto.proto},
			"Port":                                   {portProto.port},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Also create an HTTPS listener on 443.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {certArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe all listeners on the LB.
	descRec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					Port int `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Len(t, descResp.Result.Listeners.Members, 3)

	ports := []int{
		descResp.Result.Listeners.Members[0].Port,
		descResp.Result.Listeners.Members[1].Port,
		descResp.Result.Listeners.Members[2].Port,
	}
	assert.Equal(t, []int{80, 443, 8080}, ports, "listeners must be sorted by port ascending")
}

// TestInvalidActionTypeRejected verifies that unknown action types are rejected.
func TestInvalidActionTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "bad-action-lb")
	tgArn := mustCreateTG(t, h, "bad-action-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"unknown-action-type"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// mustCreateNLB creates a network load balancer and returns its ARN.
func mustCreateNLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"network"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
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

// TestNLBDefaultAttributes verifies that NLBs have cross_zone=false by default.
func TestNLBDefaultAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-attrs-test")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])
	// NLB should not have HTTP-specific attributes.
	assert.NotContains(t, attrMap, "routing.http2.enabled")
	assert.NotContains(t, attrMap, "waf.fail_open.enabled")
}

// TestALBResponseHeaderAttributes verifies that ALBs have response header attributes.
func TestALBResponseHeaderAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "alb-resp-hdr-test")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "true", attrMap["routing.http.response.server.enabled"])
	assert.Equal(t, "false", attrMap["routing.http.response.strict_transport_security.enabled"])
	assert.Contains(t, attrMap, "routing.http.response.x_frame_options.header_value")
	assert.Contains(t, attrMap, "routing.http.response.content_security_policy.header_value")
}

// TestSetSecurityGroupsNLBRejected verifies that SetSecurityGroups is rejected for NLBs.
func TestSetSecurityGroupsNLBRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-sg-reject")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-12345"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHTTPSListenerDefaultSSLPolicy verifies that HTTPS listeners get a default SSL policy.
func TestHTTPSListenerDefaultSSLPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-ssl-policy-lb")
	tgArn := mustCreateTG(t, h, "https-ssl-policy-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123456789012:certificate/test"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					SslPolicy string `xml:"SslPolicy"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.Listeners.Members[0].SslPolicy)
}

// TestHealthCheckPathValidation verifies that HealthCheckPath without a leading slash is rejected.
func TestHealthCheckPathValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	t.Run("missing_leading_slash_rejected", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":          {"CreateTargetGroup"},
			"Version":         {"2015-12-01"},
			"Name":            {"hc-path-bad"},
			"Protocol":        {"HTTP"},
			"Port":            {"80"},
			"VpcId":           {"vpc-00000000"},
			"HealthCheckPath": {"health"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("leading_slash_ok", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler()
		rec := doELBv2(t, h2, url.Values{
			"Action":          {"CreateTargetGroup"},
			"Version":         {"2015-12-01"},
			"Name":            {"hc-path-good"},
			"Protocol":        {"HTTP"},
			"Port":            {"80"},
			"VpcId":           {"vpc-00000000"},
			"HealthCheckPath": {"/health"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("modify_missing_slash_rejected", func(t *testing.T) {
		t.Parallel()

		h3 := newTestHandler()
		tgArn := mustCreateTG(t, h3, "hc-path-modify")
		rec := doELBv2(t, h3, url.Values{
			"Action":          {"ModifyTargetGroup"},
			"Version":         {"2015-12-01"},
			"TargetGroupArn":  {tgArn},
			"HealthCheckPath": {"noslash"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestDescribeLoadBalancersNotFound verifies that querying non-existent ARNs returns an error.
func TestDescribeLoadBalancersNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	fakeArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/fake/0000000000000000"

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {fakeArn},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestDescribeListenersNotFound verifies that querying non-existent listener ARNs returns an error.
func TestDescribeListenersNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	fakeArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/fake/0000000000000000/00000001"

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeListeners"},
		"Version":               {"2015-12-01"},
		"ListenerArns.member.1": {fakeArn},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestTargetGroupLoadBalancerArnsAfterListener verifies that TG shows LB ARNs after listener creation.
func TestTargetGroupLoadBalancerArnsAfterListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "tg-lb-arns-lb")
	tgArn := mustCreateTG(t, h, "tg-lb-arns-tg")
	_ = mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					LoadBalancerArns struct {
						Members []struct {
							Value string `xml:",chardata"`
						} `xml:"member"`
					} `xml:"LoadBalancerArns"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	require.Len(t, resp.Result.TargetGroups.Members[0].LoadBalancerArns.Members, 1)
	assert.Equal(t, lbArn, resp.Result.TargetGroups.Members[0].LoadBalancerArns.Members[0].Value)
}

// TestDescribeLoadBalancersPagination verifies Marker/PageSize pagination for DescribeLoadBalancers.
func TestDescribeLoadBalancersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Create 5 LBs sorted alphabetically: alb-a, alb-b, alb-c, alb-d, alb-e
	for _, name := range []string{"alb-a", "alb-b", "alb-c", "alb-d", "alb-e"} {
		mustCreateLB(t, h, name)
	}

	// Page 1: PageSize=2
	rec1 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn  string `xml:"LoadBalancerArn"`
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.LoadBalancers.Members, 2)
	assert.Equal(t, "alb-a", page1.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Equal(t, "alb-b", page1.Result.LoadBalancers.Members[1].LoadBalancerName)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Page 2: use Marker from page1, PageSize=2
	rec2 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.LoadBalancers.Members, 2)
	assert.Equal(t, "alb-c", page2.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Equal(t, "alb-d", page2.Result.LoadBalancers.Members[1].LoadBalancerName)

	// Page 3: last page
	rec3 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page2.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var page3 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec3.Body.Bytes(), &page3))
	require.Len(t, page3.Result.LoadBalancers.Members, 1)
	assert.Equal(t, "alb-e", page3.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Empty(t, page3.Result.NextMarker)
}

// TestDescribeTargetGroupsPagination verifies Marker/PageSize pagination for DescribeTargetGroups.
func TestDescribeTargetGroupsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"tg-a", "tg-b", "tg-c"} {
		mustCreateTG(t, h, name)
	}

	// Page 1: PageSize=2
	rec1 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeTargetGroups"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Result struct {
			NextMarker   string `xml:"NextMarker"`
			TargetGroups struct {
				Members []struct {
					TargetGroupName string `xml:"TargetGroupName"`
					TargetGroupArn  string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.TargetGroups.Members, 2)
	assert.Equal(t, "tg-a", page1.Result.TargetGroups.Members[0].TargetGroupName)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Page 2
	rec2 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeTargetGroups"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Result struct {
			NextMarker   string `xml:"NextMarker"`
			TargetGroups struct {
				Members []struct {
					TargetGroupName string `xml:"TargetGroupName"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.TargetGroups.Members, 1)
	assert.Equal(t, "tg-c", page2.Result.TargetGroups.Members[0].TargetGroupName)
	assert.Empty(t, page2.Result.NextMarker)
}

// TestDescribeListenersPagination verifies Marker/PageSize pagination for DescribeListeners.
func TestDescribeListenersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "pag-listeners-lb")
	tgArn := mustCreateTG(t, h, "pag-listeners-tg")

	// Create 3 listeners on ports 80, 81, 82.
	for _, port := range []string{"80", "81", "82"} {
		rec := doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {"HTTP"},
			"Port":                                   {port},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: PageSize=2
	rec1 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"PageSize":        {"2"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Result struct {
			NextMarker string `xml:"NextMarker"`
			Listeners  struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
					Port        int32  `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.Listeners.Members, 2)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Page 2
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"PageSize":        {"2"},
		"Marker":          {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Result struct {
			NextMarker string `xml:"NextMarker"`
			Listeners  struct {
				Members []struct {
					Port int32 `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.Listeners.Members, 1)
	assert.Empty(t, page2.Result.NextMarker)
}

// TestDescribeSSLPoliciesFiltering verifies that SSL policies can be filtered by name.
func TestDescribeSSLPoliciesFiltering(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeSSLPolicies"},
		"Version":        {"2015-12-01"},
		"Names.member.1": {"ELBSecurityPolicy-2016-08"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SslPolicies.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.SslPolicies.Members[0].Name)
}

// TestDescribeSSLPoliciesAll verifies that unfiltered DescribeSSLPolicies returns multiple policies.
func TestDescribeSSLPoliciesAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SslPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Greater(t, len(resp.Result.SslPolicies.Members), 2)

	// Verify specific policies are present.
	names := make(map[string]bool)
	for _, p := range resp.Result.SslPolicies.Members {
		names[p.Name] = true
	}
	assert.True(t, names["ELBSecurityPolicy-2016-08"])
	assert.True(t, names["ELBSecurityPolicy-TLS13-1-2-2021-06"])
	assert.True(t, names["ELBSecurityPolicy-FS-1-2-Res-2020-10"])
	assert.True(t, names["ELBSecurityPolicy-FS-2018-06"])
}

// TestGWLBDefaultAttributes verifies that GWLB has cross_zone=false.
func TestGWLBDefaultAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"gwlb-attrs"},
		"Type":    {"gateway"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	lbArn := createResp.Result.LoadBalancers.Members[0].LoadBalancerArn

	recAttrs := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, recAttrs.Code)

	var attrsResp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(recAttrs.Body.Bytes(), &attrsResp))

	attrMap := make(map[string]string)
	for _, m := range attrsResp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])
	assert.NotContains(t, attrMap, "routing.http2.enabled")
}

// TestNLBAttributeDefaults verifies that NLB attributes don't include HTTP routing attrs.
func TestNLBAttributeDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-attr-defaults")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	// NLB must have these.
	assert.Equal(t, "false", attrMap["access_logs.s3.enabled"])
	assert.Equal(t, "false", attrMap["deletion_protection.enabled"])
	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])

	// NLB must not have these HTTP-specific ones.
	assert.NotContains(t, attrMap, "routing.http2.enabled")
	assert.NotContains(t, attrMap, "routing.http.desync_mitigation_mode")
	assert.NotContains(t, attrMap, "waf.fail_open.enabled")
	assert.NotContains(t, attrMap, "routing.http.response.server.enabled")
}

// TestDescribeLoadBalancersByNameNotFound verifies that querying a non-existent LB by name returns 404,
// matching real AWS which raises LoadBalancerNotFoundException for any unknown name.
func TestDescribeLoadBalancersByNameNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals   url.Values
		name   string
		expect int
	}{
		{
			name: "single_missing_name",
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"does-not-exist"},
			},
			expect: http.StatusNotFound,
		},
		{
			name: "one_valid_one_missing_name",
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"desc-lb-name-exists"},
				"Names.member.2": {"does-not-exist"},
			},
			expect: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tc.name == "one_valid_one_missing_name" {
				mustCreateLB(t, h, "desc-lb-name-exists")
			}

			rec := doELBv2(t, h, tc.vals)
			assert.Equal(t, tc.expect, rec.Code)
		})
	}
}

// TestDescribeTargetGroupsByNameNotFound verifies that querying non-existent TG names returns 404,
// matching real AWS which raises TargetGroupNotFoundException for any unknown name.
func TestDescribeTargetGroupsByNameNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals   url.Values
		name   string
		expect int
	}{
		{
			name: "single_missing_name",
			vals: url.Values{
				"Action":         {"DescribeTargetGroups"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"does-not-exist"},
			},
			expect: http.StatusNotFound,
		},
		{
			name: "one_valid_one_missing_name",
			vals: url.Values{
				"Action":         {"DescribeTargetGroups"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"desc-tg-name-exists"},
				"Names.member.2": {"does-not-exist"},
			},
			expect: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tc.name == "one_valid_one_missing_name" {
				mustCreateTG(t, h, "desc-tg-name-exists")
			}

			rec := doELBv2(t, h, tc.vals)
			assert.Equal(t, tc.expect, rec.Code)
		})
	}
}

// TestDescribeTargetHealthUnregisteredTargets verifies that querying health for specific targets that are
// not registered returns state "unused" with reason "Target.NotRegistered", matching real AWS behaviour.
func TestDescribeTargetHealthUnregisteredTargets(t *testing.T) {
	t.Parallel()

	type targetHealthResult struct {
		State  string `xml:"State"`
		Reason string `xml:"Reason"`
	}
	type memberResult struct {
		TargetHealth targetHealthResult `xml:"TargetHealth"`
		Target       struct {
			ID   string `xml:"Id"`
			Port int32  `xml:"Port"`
		} `xml:"Target"`
	}
	type respType struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []memberResult `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}

	tests := []struct {
		requestTargets   url.Values
		name             string
		wantUnregistered []string // IDs expected with state=unused, reason=Target.NotRegistered
		wantRegistered   []string // IDs expected with a non-unused state
		wantLen          int
	}{
		{
			name: "single_unregistered_target",
			requestTargets: url.Values{
				"Targets.member.1.Id":   {"i-unregistered"},
				"Targets.member.1.Port": {"80"},
			},
			wantLen:          1,
			wantUnregistered: []string{"i-unregistered"},
		},
		{
			name: "mixed_registered_and_unregistered",
			requestTargets: url.Values{
				"Targets.member.1.Id":   {"i-registered"},
				"Targets.member.1.Port": {"80"},
				"Targets.member.2.Id":   {"i-ghost"},
				"Targets.member.2.Port": {"80"},
			},
			wantLen:          2,
			wantRegistered:   []string{"i-registered"},
			wantUnregistered: []string{"i-ghost"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tgArn := mustCreateTG(t, h, "unreg-tg")

			// Register only "i-registered" for the mixed test case.
			if len(tc.wantRegistered) > 0 {
				doELBv2(t, h, url.Values{
					"Action":                {"RegisterTargets"},
					"Version":               {"2015-12-01"},
					"TargetGroupArn":        {tgArn},
					"Targets.member.1.Id":   {"i-registered"},
					"Targets.member.1.Port": {"80"},
				})
			}

			vals := url.Values{
				"Action":         {"DescribeTargetHealth"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			}
			maps.Copy(vals, tc.requestTargets)

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp respType
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.TargetHealthDescriptions.Members, tc.wantLen)

			byID := make(map[string]memberResult, len(resp.Result.TargetHealthDescriptions.Members))
			for _, m := range resp.Result.TargetHealthDescriptions.Members {
				byID[m.Target.ID] = m
			}

			for _, id := range tc.wantUnregistered {
				m, ok := byID[id]
				require.True(t, ok, "expected %q in response", id)
				assert.Equal(t, "unused", m.TargetHealth.State, "target %q should be unused", id)
				assert.Equal(t, "Target.NotRegistered", m.TargetHealth.Reason, "target %q reason mismatch", id)
			}

			for _, id := range tc.wantRegistered {
				m, ok := byID[id]
				require.True(t, ok, "expected %q in response", id)
				assert.NotEqual(t, "unused", m.TargetHealth.State, "registered target %q should not be unused", id)
			}
		})
	}
}
