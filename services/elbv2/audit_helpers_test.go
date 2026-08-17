package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

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
