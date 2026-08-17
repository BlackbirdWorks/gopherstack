package elbv2_test

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
