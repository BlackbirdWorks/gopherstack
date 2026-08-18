package elbv2_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
