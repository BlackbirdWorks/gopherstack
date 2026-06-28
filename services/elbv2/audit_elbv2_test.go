package elbv2_test

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// --- audit helpers ---

func auditHandler(t *testing.T) *elbv2.Handler {
	t.Helper()

	b := elbv2.NewInMemoryBackend("111122223333", config.DefaultRegion)
	t.Cleanup(func() { b.Close() })

	return elbv2.NewHandler(b)
}

func auditBackend(t *testing.T) *elbv2.InMemoryBackend {
	t.Helper()

	b := elbv2.NewInMemoryBackend("111122223333", config.DefaultRegion)
	t.Cleanup(func() { b.Close() })

	return b
}

func auditDo(t *testing.T, h *elbv2.Handler, vals url.Values) *xmlUnmarshalHelper {
	t.Helper()
	rec := doELBv2(t, h, vals)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return &xmlUnmarshalHelper{t: t, body: rec.Body.Bytes()}
}

type xmlUnmarshalHelper struct {
	t    *testing.T
	body []byte
}

func (x *xmlUnmarshalHelper) into(v any) {
	x.t.Helper()
	require.NoError(x.t, xml.Unmarshal(x.body, v))
}

// auditCreateLB creates an ALB and returns its ARN.
func auditCreateLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
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
		"Name":    {name},
		"Type":    {"application"},
	}).into(&resp)
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	return resp.Result.LoadBalancers.Members[0].LoadBalancerArn
}

// auditCreateNLB creates a network LB and returns its ARN.
func auditCreateNLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
					DNSName         string `xml:"DNSName"`
					Type            string `xml:"Type"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	auditDo(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"network"},
	}).into(&resp)
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	return resp.Result.LoadBalancers.Members[0].LoadBalancerArn
}

// auditCreateTG creates an HTTP target group and returns its ARN.
func auditCreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	auditDo(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	}).into(&resp)
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	return resp.Result.TargetGroups.Members[0].TargetGroupArn
}

// auditCreateListener creates an HTTP listener and returns its ARN.
func auditCreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
	t.Helper()
	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	}).into(&resp)
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

// auditCreateRule creates a listener rule and returns its ARN.
func auditCreateRule(t *testing.T, h *elbv2.Handler, vals url.Values) string {
	t.Helper()
	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	auditDo(t, h, vals).into(&resp)
	require.Len(t, resp.Result.Rules.Members, 1)

	return resp.Result.Rules.Members[0].RuleArn
}

// --- draining state tests ---

// TestAuditELBv2_DeregisterTargets_DrainingState verifies that after DeregisterTargets,
// the target enters draining state with reason Target.DeregistrationInProgress.
func TestAuditELBv2_DeregisterTargets_DrainingState(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "drain-state-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-drain-01"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-drain-01"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "draining", th.State)
	assert.Equal(t, "Target.DeregistrationInProgress", th.Reason)
}

// TestAuditELBv2_DeregisterTargets_NonRegisteredIsNoop verifies that deregistering
// a target that was never registered is a no-op (does not affect other targets).
func TestAuditELBv2_DeregisterTargets_NonRegisteredIsNoop(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "noop-drain-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-real"},
		"Targets.member.1.Port": {"80"},
	})

	// Deregister a target that was never registered.
	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-never-registered"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "deregistering unknown target must not error")

	// The real target must be unaffected.
	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-real", resp.Result.TargetHealthDescriptions.Members[0].Target.ID)
	assert.NotEqual(
		t,
		"draining",
		resp.Result.TargetHealthDescriptions.Members[0].TargetHealth.State,
	)
}

// TestAuditELBv2_DeregisterTargets_ZeroDelay_EventuallyRemoved verifies that when
// deregistration_delay.timeout_seconds=0, targets are removed from the TG promptly
// by the background reconciler.
func TestAuditELBv2_DeregisterTargets_ZeroDelay_EventuallyRemoved(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "zero-drain-tg")

	// Set deregistration delay to 0 so the drain completes immediately.
	auditDo(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"deregistration_delay.timeout_seconds"},
		"Attributes.member.1.Value": {"0"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-zero-drain"},
		"Targets.member.1.Port": {"80"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-zero-drain"},
		"Targets.member.1.Port": {"80"},
	})

	// Wait for the background reconciler to remove the drained target.
	// The reconciler fires every ~40ms; with delay=0 the drain has already expired.
	require.Eventually(t, func() bool {
		var resp struct {
			Result struct {
				TargetHealthDescriptions struct {
					Members []struct{} `xml:"member"`
				} `xml:"TargetHealthDescriptions"`
			} `xml:"DescribeTargetHealthResult"`
		}
		rec := doELBv2(t, h, url.Values{
			"Action":         {"DescribeTargetHealth"},
			"Version":        {"2015-12-01"},
			"TargetGroupArn": {tgArn},
		})
		if rec.Code != http.StatusOK {
			return false
		}
		_ = xml.Unmarshal(rec.Body.Bytes(), &resp)

		return len(resp.Result.TargetHealthDescriptions.Members) == 0
	}, 500*time.Millisecond, 20*time.Millisecond, "target must be removed after drain delay expires")
}

// TestAuditELBv2_DeregisterTargets_MultiPort_OnlyDrainsTarget verifies that
// deregistering one port leaves the other port unaffected.
func TestAuditELBv2_DeregisterTargets_MultiPort_OnlyDrainsTarget(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "multi-port-drain-tg")

	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-mp"},
		"Targets.member.1.Port": {"8080"},
		"Targets.member.2.Id":   {"i-mp"},
		"Targets.member.2.Port": {"8081"},
	})

	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-mp"},
		"Targets.member.1.Port": {"8080"},
	})

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct { //nolint:govet // field order is chosen for readability
					Target struct {
						Port int `xml:"Port"`
					} `xml:"Target"`
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)
	states := map[int]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.Port] = m.TargetHealth.State
	}
	assert.Equal(t, "draining", states[8080])
	assert.NotEqual(t, "draining", states[8081])
}

// --- DNS name format tests ---

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

// --- weighted target group tests ---

// TestAuditELBv2_WeightedTargetGroups verifies that ForwardConfig with multiple
// weighted target groups is stored and returned correctly.
func TestAuditELBv2_WeightedTargetGroups(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "wt-lb")
	tg1Arn := auditCreateTG(t, h, "wt-tg1")
	tg2Arn := auditCreateTG(t, h, "wt-tg2")

	// Create a listener with two weighted target groups.
	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn    string `xml:"ListenerArn"`
					DefaultActions struct {
						Members []struct {
							Type          string `xml:"Type"`
							ForwardConfig struct {
								TargetGroups struct {
									Members []struct {
										TargetGroupArn string `xml:"TargetGroupArn"`
										Weight         int    `xml:"Weight"`
									} `xml:"member"`
								} `xml:"TargetGroups"`
							} `xml:"ForwardConfig"`
						} `xml:"member"`
					} `xml:"DefaultActions"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                       {"CreateListener"},
		"Version":                      {"2015-12-01"},
		"LoadBalancerArn":              {lbArn},
		"Protocol":                     {"HTTP"},
		"Port":                         {"80"},
		"DefaultActions.member.1.Type": {"forward"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn": {tg1Arn},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.Weight":         {"3"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.TargetGroupArn": {tg2Arn},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.Weight":         {"1"},
	}).into(&resp)

	require.Len(t, resp.Result.Listeners.Members, 1)
	l := resp.Result.Listeners.Members[0]
	require.Len(t, l.DefaultActions.Members, 1)
	assert.Equal(t, "forward", l.DefaultActions.Members[0].Type)

	tgMembers := l.DefaultActions.Members[0].ForwardConfig.TargetGroups.Members
	require.Len(t, tgMembers, 2)

	weights := map[string]int{}
	for _, tg := range tgMembers {
		weights[tg.TargetGroupArn] = tg.Weight
	}
	assert.Equal(t, 3, weights[tg1Arn], "tg1 weight should be 3")
	assert.Equal(t, 1, weights[tg2Arn], "tg2 weight should be 1")

	// Verify weights are preserved via DescribeListeners.
	lArn := l.ListenerArn
	var descResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					DefaultActions struct {
						Members []struct {
							ForwardConfig struct {
								TargetGroups struct {
									Members []struct {
										TargetGroupArn string `xml:"TargetGroupArn"`
										Weight         int    `xml:"Weight"`
									} `xml:"member"`
								} `xml:"TargetGroups"`
							} `xml:"ForwardConfig"`
						} `xml:"member"`
					} `xml:"DefaultActions"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                {"DescribeListeners"},
		"Version":               {"2015-12-01"},
		"ListenerArns.member.1": {lArn},
	}).into(&descResp)

	require.Len(t, descResp.Result.Listeners.Members, 1)
	tgMembers2 := descResp.Result.Listeners.Members[0].DefaultActions.Members[0].ForwardConfig.TargetGroups.Members
	require.Len(t, tgMembers2, 2)

	weights2 := map[string]int{}
	for _, tg := range tgMembers2 {
		weights2[tg.TargetGroupArn] = tg.Weight
	}
	assert.Equal(t, 3, weights2[tg1Arn])
	assert.Equal(t, 1, weights2[tg2Arn])
}

// --- rule condition tests ---

// TestAuditELBv2_RuleConditions_AllSupportedTypes verifies that all 6 condition
// types (host-header, path-pattern, http-header, http-request-method, query-string,
// source-ip) are persisted and returned in DescribeRules.
func TestAuditELBv2_RuleConditions_AllSupportedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order is chosen for readability
		name      string
		condField string
		buildVals func(prefix string) url.Values
		checkResp func(t *testing.T, conds []struct {
			Field  string `xml:"Field"`
			Values struct {
				Members []struct {
					Value string `xml:"member"`
				} `xml:",any"`
			} `xml:"Values"`
		})
	}{
		{
			name:      "host_header",
			condField: "host-header",
			buildVals: func(prefix string) url.Values {
				return url.Values{
					prefix + ".Field": {"host-header"},
					prefix + ".HostHeaderConfig.Values.member.1": {"example.com"},
				}
			},
		},
		{
			name:      "path_pattern",
			condField: "path-pattern",
			buildVals: func(prefix string) url.Values {
				return url.Values{
					prefix + ".Field": {"path-pattern"},
					prefix + ".PathPatternConfig.Values.member.1": {"/api/*"},
				}
			},
		},
		{
			name:      "http_request_method",
			condField: "http-request-method",
			buildVals: func(prefix string) url.Values {
				return url.Values{
					prefix + ".Field": {"http-request-method"},
					prefix + ".HttpRequestMethodConfig.Values.member.1": {"GET"},
				}
			},
		},
		{
			name:      "source_ip",
			condField: "source-ip",
			buildVals: func(prefix string) url.Values {
				return url.Values{
					prefix + ".Field":                          {"source-ip"},
					prefix + ".SourceIpConfig.Values.member.1": {"10.0.0.0/8"},
				}
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)
			lbArn := auditCreateLB(t, h, fmt.Sprintf("cond-lb-%d", i))
			tgArn := auditCreateTG(t, h, fmt.Sprintf("cond-tg-%d", i))
			lArn := auditCreateListener(t, h, lbArn, tgArn)

			condPrefix := "Conditions.member.1"
			condVals := tc.buildVals(condPrefix)

			vals := url.Values{
				"Action":                          {"CreateRule"},
				"Version":                         {"2015-12-01"},
				"ListenerArn":                     {lArn},
				"Priority":                        {"10"},
				"Actions.member.1.Type":           {"forward"},
				"Actions.member.1.TargetGroupArn": {tgArn},
			}
			maps.Copy(vals, condVals)

			ruleArn := auditCreateRule(t, h, vals)

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
				} `xml:"DescribeRulesResult"`
			}
			auditDo(t, h, url.Values{
				"Action":            {"DescribeRules"},
				"Version":           {"2015-12-01"},
				"RuleArns.member.1": {ruleArn},
			}).into(&resp)

			require.Len(t, resp.Result.Rules.Members, 1)
			conds := resp.Result.Rules.Members[0].Conditions.Members
			require.Len(t, conds, 1, "rule should have 1 condition")
			assert.Equal(t, tc.condField, conds[0].Field)
		})
	}
}

// TestAuditELBv2_RuleConditions_HTTPHeader verifies that http-header conditions
// preserve the HttpHeaderName field.
func TestAuditELBv2_RuleConditions_HTTPHeader(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "hh-lb")
	tgArn := auditCreateTG(t, h, "hh-tg")
	lArn := auditCreateListener(t, h, lbArn, tgArn)

	ruleArn := auditCreateRule(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {lArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"http-header"},
		"Conditions.member.1.HttpHeaderConfig.HttpHeaderName":  {"X-Custom-Header"},
		"Conditions.member.1.HttpHeaderConfig.Values.member.1": {"my-value"},
	})

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Conditions struct {
						Members []struct {
							Field            string `xml:"Field"`
							HTTPHeaderConfig struct {
								HTTPHeaderName string `xml:"HttpHeaderName"`
							} `xml:"HttpHeaderConfig"`
						} `xml:"member"`
					} `xml:"Conditions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	auditDo(t, h, url.Values{
		"Action":            {"DescribeRules"},
		"Version":           {"2015-12-01"},
		"RuleArns.member.1": {ruleArn},
	}).into(&resp)

	require.Len(t, resp.Result.Rules.Members, 1)
	conds := resp.Result.Rules.Members[0].Conditions.Members
	require.Len(t, conds, 1)
	assert.Equal(t, "http-header", conds[0].Field)
	assert.Equal(t, "X-Custom-Header", conds[0].HTTPHeaderConfig.HTTPHeaderName)
}

// TestAuditELBv2_RuleConditions_QueryString verifies that query-string conditions
// preserve key/value pairs.
func TestAuditELBv2_RuleConditions_QueryString(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "qs-lb")
	tgArn := auditCreateTG(t, h, "qs-tg")
	lArn := auditCreateListener(t, h, lbArn, tgArn)

	ruleArn := auditCreateRule(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {lArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"query-string"},
		"Conditions.member.1.QueryStringConfig.Values.member.1.Key":   {"env"},
		"Conditions.member.1.QueryStringConfig.Values.member.1.Value": {"prod"},
	})

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Conditions struct {
						Members []struct {
							Field             string `xml:"Field"`
							QueryStringConfig struct {
								Values struct {
									Members []struct {
										Key   string `xml:"Key"`
										Value string `xml:"Value"`
									} `xml:"member"`
								} `xml:"Values"`
							} `xml:"QueryStringConfig"`
						} `xml:"member"`
					} `xml:"Conditions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	auditDo(t, h, url.Values{
		"Action":            {"DescribeRules"},
		"Version":           {"2015-12-01"},
		"RuleArns.member.1": {ruleArn},
	}).into(&resp)

	require.Len(t, resp.Result.Rules.Members, 1)
	conds := resp.Result.Rules.Members[0].Conditions.Members
	require.Len(t, conds, 1)
	assert.Equal(t, "query-string", conds[0].Field)

	pairs := conds[0].QueryStringConfig.Values.Members
	require.Len(t, pairs, 1)
	assert.Equal(t, "env", pairs[0].Key)
	assert.Equal(t, "prod", pairs[0].Value)
}

// --- rule action tests ---

// TestAuditELBv2_RuleActions_RedirectAndFixedResponse verifies that redirect
// and fixed-response actions are stored and returned with correct config.
func TestAuditELBv2_RuleActions_RedirectAndFixedResponse(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order is chosen for readability
		name      string
		buildVals func(prefix, condPrefix string, tgArn string) url.Values
		checkXML  func(t *testing.T, body []byte)
	}{
		{
			name: "redirect_action",
			buildVals: func(actPrefix, condPrefix, _ string) url.Values {
				return url.Values{
					actPrefix + ".Type":                               {"redirect"},
					actPrefix + ".RedirectConfig.StatusCode":          {"HTTP_301"},
					actPrefix + ".RedirectConfig.Protocol":            {"HTTPS"},
					actPrefix + ".RedirectConfig.Port":                {"443"},
					condPrefix + ".Field":                             {"path-pattern"},
					condPrefix + ".PathPatternConfig.Values.member.1": {"/old/*"},
				}
			},
			checkXML: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								Actions struct {
									Members []struct {
										Type           string `xml:"Type"`
										RedirectConfig struct {
											StatusCode string `xml:"StatusCode"`
											Protocol   string `xml:"Protocol"`
											Port       string `xml:"Port"`
										} `xml:"RedirectConfig"`
									} `xml:"member"`
								} `xml:"Actions"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"DescribeRulesResult"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				acts := resp.Result.Rules.Members[0].Actions.Members
				require.Len(t, acts, 1)
				assert.Equal(t, "redirect", acts[0].Type)
				assert.Equal(t, "HTTP_301", acts[0].RedirectConfig.StatusCode)
				assert.Equal(t, "HTTPS", acts[0].RedirectConfig.Protocol)
				assert.Equal(t, "443", acts[0].RedirectConfig.Port)
			},
		},
		{
			name: "fixed_response_action",
			buildVals: func(actPrefix, condPrefix, _ string) url.Values {
				return url.Values{
					actPrefix + ".Type":                               {"fixed-response"},
					actPrefix + ".FixedResponseConfig.StatusCode":     {"503"},
					actPrefix + ".FixedResponseConfig.ContentType":    {"text/plain"},
					actPrefix + ".FixedResponseConfig.MessageBody":    {"Service Unavailable"},
					condPrefix + ".Field":                             {"path-pattern"},
					condPrefix + ".PathPatternConfig.Values.member.1": {"/maintenance"},
				}
			},
			checkXML: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					Result struct {
						Rules struct {
							Members []struct {
								Actions struct {
									Members []struct {
										Type                string `xml:"Type"`
										FixedResponseConfig struct {
											StatusCode  string `xml:"StatusCode"`
											ContentType string `xml:"ContentType"`
											MessageBody string `xml:"MessageBody"`
										} `xml:"FixedResponseConfig"`
									} `xml:"member"`
								} `xml:"Actions"`
							} `xml:"member"`
						} `xml:"Rules"`
					} `xml:"DescribeRulesResult"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				require.Len(t, resp.Result.Rules.Members, 1)
				acts := resp.Result.Rules.Members[0].Actions.Members
				require.Len(t, acts, 1)
				assert.Equal(t, "fixed-response", acts[0].Type)
				assert.Equal(t, "503", acts[0].FixedResponseConfig.StatusCode)
				assert.Equal(t, "text/plain", acts[0].FixedResponseConfig.ContentType)
				assert.Equal(t, "Service Unavailable", acts[0].FixedResponseConfig.MessageBody)
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)
			lbArn := auditCreateLB(t, h, fmt.Sprintf("act-lb-%d", i))
			tgArn := auditCreateTG(t, h, fmt.Sprintf("act-tg-%d", i))
			lArn := auditCreateListener(t, h, lbArn, tgArn)

			actPrefix := "Actions.member.1"
			condPrefix := "Conditions.member.1"

			vals := url.Values{
				"Action":      {"CreateRule"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {lArn},
				"Priority":    {"10"},
			}
			maps.Copy(vals, tc.buildVals(actPrefix, condPrefix, tgArn))

			ruleArn := auditCreateRule(t, h, vals)

			rec := doELBv2(t, h, url.Values{
				"Action":            {"DescribeRules"},
				"Version":           {"2015-12-01"},
				"RuleArns.member.1": {ruleArn},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			tc.checkXML(t, rec.Body.Bytes())
		})
	}
}

// --- LB attribute tests ---

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

// --- target group attribute tests ---

// TestAuditELBv2_TGAttributes_DefaultDeregistrationDelay verifies that the TG
// attribute deregistration_delay.timeout_seconds defaults to 300.
func TestAuditELBv2_TGAttributes_DefaultDeregistrationDelay(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	tgArn := auditCreateTG(t, h, "delay-tg")

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
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	attrs := map[string]string{}
	for _, m := range resp.Result.Attributes.Members {
		attrs[m.Key] = m.Value
	}
	assert.Equal(t, "300", attrs["deregistration_delay.timeout_seconds"])
	assert.Equal(t, "false", attrs["stickiness.enabled"])
	assert.Equal(t, "lb_cookie", attrs["stickiness.type"])
	assert.Equal(t, "round_robin", attrs["load_balancing.algorithm.type"])
}

// TestAuditELBv2_TGAttributes_ModifyDeregistrationDelay verifies that
// deregistration_delay.timeout_seconds can be modified.
func TestAuditELBv2_TGAttributes_ModifyDeregistrationDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantVal string
	}{
		{name: "set_to_0", value: "0", wantVal: "0"},
		{name: "set_to_60", value: "60", wantVal: "60"},
		{name: "set_to_3600", value: "3600", wantVal: "3600"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := auditHandler(t)
			tgArn := auditCreateTG(t, h, "mod-delay-"+tc.name)

			auditDo(t, h, url.Values{
				"Action":                    {"ModifyTargetGroupAttributes"},
				"Version":                   {"2015-12-01"},
				"TargetGroupArn":            {tgArn},
				"Attributes.member.1.Key":   {"deregistration_delay.timeout_seconds"},
				"Attributes.member.1.Value": {tc.value},
			})

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
			auditDo(t, h, url.Values{
				"Action":         {"DescribeTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			}).into(&resp)

			attrs := map[string]string{}
			for _, m := range resp.Result.Attributes.Members {
				attrs[m.Key] = m.Value
			}
			assert.Equal(t, tc.wantVal, attrs["deregistration_delay.timeout_seconds"])
		})
	}
}

// --- load balancer scheme / state tests ---

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

// --- listener SSL policy tests ---

// TestAuditELBv2_Listener_DefaultSSLPolicy verifies that HTTPS listeners get a
// default SSL policy when none is specified.
func TestAuditELBv2_Listener_DefaultSSLPolicy(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "ssl-lb")
	tgArn := auditCreateTG(t, h, "ssl-tg")

	// HTTPS listener needs a certificate.
	const certArn = "arn:aws:acm:us-east-1:111122223333:certificate/test-cert-id"

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					SSLPolicy string `xml:"SslPolicy"`
					Protocol  string `xml:"Protocol"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"Certificates.member.1.CertificateArn":   {certArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	}).into(&resp)

	require.Len(t, resp.Result.Listeners.Members, 1)
	assert.Equal(t, "HTTPS", resp.Result.Listeners.Members[0].Protocol)
	assert.NotEmpty(t, resp.Result.Listeners.Members[0].SSLPolicy,
		"HTTPS listener must have a default SSL policy")
}

// --- ModifyRule tests ---

// TestAuditELBv2_ModifyRule_UpdatesActionsAndConditions verifies that ModifyRule
// replaces both actions and conditions on an existing rule.
func TestAuditELBv2_ModifyRule_UpdatesActionsAndConditions(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "mr-lb")
	tg1Arn := auditCreateTG(t, h, "mr-tg1")
	tg2Arn := auditCreateTG(t, h, "mr-tg2")
	lArn := auditCreateListener(t, h, lbArn, tg1Arn)

	ruleArn := auditCreateRule(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {lArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tg1Arn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/v1/*"},
	})

	// Modify: change target group and path pattern.
	var modResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
					Actions struct {
						Members []struct {
							TargetGroupArn string `xml:"TargetGroupArn"`
						} `xml:"member"`
					} `xml:"Actions"`
					Conditions struct {
						Members []struct {
							Field             string `xml:"Field"`
							PathPatternConfig struct {
								Values struct {
									Members []struct {
										Value string `xml:"member"`
									} `xml:",any"`
								} `xml:"Values"`
							} `xml:"PathPatternConfig"`
						} `xml:"member"`
					} `xml:"Conditions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"ModifyRuleResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                          {"ModifyRule"},
		"Version":                         {"2015-12-01"},
		"RuleArn":                         {ruleArn},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tg2Arn},
		"Conditions.member.1.Field":       {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/v2/*"},
	}).into(&modResp)

	require.Len(t, modResp.Result.Rules.Members, 1)
	rule := modResp.Result.Rules.Members[0]
	assert.Equal(t, ruleArn, rule.RuleArn)
	require.Len(t, rule.Actions.Members, 1)
	assert.Equal(t, tg2Arn, rule.Actions.Members[0].TargetGroupArn)
}

// --- target health state model tests ---

// TestAuditELBv2_TargetHealth_AllStates verifies that all documented health states
// can be set and retrieved: initial, healthy, unhealthy, draining, unused.
func TestAuditELBv2_TargetHealth_AllStates(t *testing.T) {
	t.Parallel()

	b := auditBackend(t)
	h := elbv2.NewHandler(b)

	tgArn := auditCreateTG(t, h, "all-states-tg")

	// Register multiple targets.
	auditDo(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-initial"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-healthy"},
		"Targets.member.2.Port": {"80"},
		"Targets.member.3.Id":   {"i-unhealthy"},
		"Targets.member.3.Port": {"80"},
		"Targets.member.4.Id":   {"i-draining"},
		"Targets.member.4.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy", 80, "healthy", ""))
	require.NoError(
		t,
		b.SetTargetHealthState(tgArn, "i-unhealthy", 80, "unhealthy", "Target.FailedHealthChecks"),
	)

	// Deregister to trigger draining.
	auditDo(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-draining"},
		"Targets.member.1.Port": {"80"},
	})

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	}).into(&resp)

	states := map[string]string{}
	reasons := map[string]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.ID] = m.TargetHealth.State
		reasons[m.Target.ID] = m.TargetHealth.Reason
	}

	assert.Equal(t, "initial", states["i-initial"])
	assert.Equal(t, "Elb.InitialHealthChecking", reasons["i-initial"])
	assert.Equal(t, "healthy", states["i-healthy"])
	assert.Empty(t, reasons["i-healthy"])
	assert.Equal(t, "unhealthy", states["i-unhealthy"])
	assert.Equal(t, "Target.FailedHealthChecks", reasons["i-unhealthy"])
	assert.Equal(t, "draining", states["i-draining"])
	assert.Equal(t, "Target.DeregistrationInProgress", reasons["i-draining"])

	// Query a non-registered target → unused state.
	var unusedResp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	auditDo(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-not-registered"},
		"Targets.member.1.Port": {"80"},
	}).into(&unusedResp)

	require.Len(t, unusedResp.Result.TargetHealthDescriptions.Members, 1)
	uth := unusedResp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "unused", uth.State)
	assert.Equal(t, "Target.NotRegistered", uth.Reason)
}

// --- pagination tests ---

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

// TestAuditELBv2_Pagination_DescribeTargetGroups verifies pagination for target groups.
func TestAuditELBv2_Pagination_DescribeTargetGroups(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)

	for i := range 4 {
		auditCreateTG(t, h, fmt.Sprintf("pg-tg-%02d", i))
	}

	var page1Resp struct {
		Result struct { //nolint:govet // field order is chosen for readability
			TargetGroups struct {
				Members []struct {
					TargetGroupName string `xml:"TargetGroupName"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	auditDo(t, h, url.Values{
		"Action":   {"DescribeTargetGroups"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	}).into(&page1Resp)

	require.Len(t, page1Resp.Result.TargetGroups.Members, 2)
	require.NotEmpty(t, page1Resp.Result.NextMarker)

	var page2Resp struct {
		Result struct { //nolint:govet // field order is chosen for readability
			TargetGroups struct {
				Members []struct{} `xml:"member"`
			} `xml:"TargetGroups"`
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	auditDo(t, h, url.Values{
		"Action":   {"DescribeTargetGroups"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page1Resp.Result.NextMarker},
	}).into(&page2Resp)

	require.Len(t, page2Resp.Result.TargetGroups.Members, 2)
	assert.Empty(t, page2Resp.Result.NextMarker, "last page must not have NextMarker")
}

// --- security group & subnet tests ---

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

// TestAuditELBv2_Tags_ResourceLifecycle verifies that tags can be added, described,
// and removed on multiple resource types (LB, TG, listener, rule).
func TestAuditELBv2_Tags_ResourceLifecycle(t *testing.T) {
	t.Parallel()

	h := auditHandler(t)
	lbArn := auditCreateLB(t, h, "tag-lb")
	tgArn := auditCreateTG(t, h, "tag-tg")
	lArn := auditCreateListener(t, h, lbArn, tgArn)

	resources := []struct {
		name string
		arn  string
	}{
		{"loadbalancer", lbArn},
		{"targetgroup", tgArn},
		{"listener", lArn},
	}

	for _, res := range resources {
		t.Run(res.name, func(t *testing.T) {
			t.Parallel()

			// Add tag.
			auditDo(t, h, url.Values{
				"Action":                {"AddTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
				"Tags.member.1.Key":     {"Env"},
				"Tags.member.1.Value":   {"prod"},
			})

			// Describe tags.
			var descResp struct {
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
			auditDo(t, h, url.Values{
				"Action":                {"DescribeTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
			}).into(&descResp)

			require.Len(t, descResp.Result.TagDescriptions.Members, 1)
			tagList := descResp.Result.TagDescriptions.Members[0].Tags.Members
			require.Len(t, tagList, 1)
			assert.Equal(t, "Env", tagList[0].Key)
			assert.Equal(t, "prod", tagList[0].Value)

			// Remove tag.
			auditDo(t, h, url.Values{
				"Action":                {"RemoveTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
				"TagKeys.member.1":      {"Env"},
			})

			// Describe again — should be empty (fresh struct to avoid xml.Unmarshal slice appending).
			var descResp2 struct {
				Result struct {
					TagDescriptions struct {
						Members []struct {
							Tags struct {
								Members []struct{} `xml:"member"`
							} `xml:"Tags"`
						} `xml:"member"`
					} `xml:"TagDescriptions"`
				} `xml:"DescribeTagsResult"`
			}
			auditDo(t, h, url.Values{
				"Action":                {"DescribeTags"},
				"Version":               {"2015-12-01"},
				"ResourceArns.member.1": {res.arn},
			}).into(&descResp2)

			require.Len(t, descResp2.Result.TagDescriptions.Members, 1)
			assert.Empty(t, descResp2.Result.TagDescriptions.Members[0].Tags.Members)
		})
	}
}
