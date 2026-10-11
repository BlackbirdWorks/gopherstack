package elbv2_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataPlane_RuleConditions(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	l := f.listener(t, fixedAction("200", "default"))

	str := func(s ...string) []string { return s }
	rules := []struct {
		field string
		cond  elbv2types.RuleCondition
	}{
		{
			"path",
			elbv2types.RuleCondition{
				Field: aws.String(
					"path-pattern",
				),
				PathPatternConfig: &elbv2types.PathPatternConditionConfig{Values: str("/api/*/v?")},
			},
		},
		{
			"host",
			elbv2types.RuleCondition{
				Field: aws.String(
					"host-header",
				),
				HostHeaderConfig: &elbv2types.HostHeaderConditionConfig{Values: str("*.example.com")},
			},
		},
		{"header", elbv2types.RuleCondition{
			Field: aws.String("http-header"),
			HttpHeaderConfig: &elbv2types.HttpHeaderConditionConfig{
				HttpHeaderName: aws.String("X-Tier"), Values: str("gold*", "silver"),
			},
		}},
		{
			"method",
			elbv2types.RuleCondition{
				Field: aws.String(
					"http-request-method",
				),
				HttpRequestMethodConfig: &elbv2types.HttpRequestMethodConditionConfig{Values: str("DELETE")},
			},
		},
		{"query", elbv2types.RuleCondition{
			Field: aws.String("query-string"),
			QueryStringConfig: &elbv2types.QueryStringConditionConfig{
				Values: []elbv2types.QueryStringKeyValuePair{{Key: aws.String("env"), Value: aws.String("pr*")}},
			},
		}},
		{
			"sourceip",
			elbv2types.RuleCondition{
				Field: aws.String(
					"source-ip",
				),
				SourceIpConfig: &elbv2types.SourceIpConditionConfig{Values: str("127.0.0.0/8")},
			},
		},
	}

	for i, r := range rules {
		f.rule(t, l, int32(i+1), []elbv2types.RuleCondition{r.cond}, fixedAction("200", r.field))
	}

	tests := []struct {
		name string
		req  dpReq
		want string
	}{
		{"path hit", dpReq{path: "/api/users/v2", host: ""}, "path"},
		{"path wildcard crosses slash", dpReq{path: "/api/a/b/v1"}, "path"},
		{
			"path case sensitive",
			dpReq{path: "/API/users/v2", headers: map[string]string{"X-Tier": "bronze"}},
			"sourceip",
		},
		{"host hit", dpReq{path: "/other", host: "app.example.com"}, "host"},
		{"host case insensitive", dpReq{path: "/other", host: "APP.Example.COM"}, "host"},
		{"header prefix", dpReq{path: "/x", headers: map[string]string{"x-tier": "goldplus"}}, "header"},
		{"header exact", dpReq{path: "/x", headers: map[string]string{"X-Tier": "silver"}}, "header"},
		{"method", dpReq{path: "/x", method: http.MethodDelete}, "method"},
		{"query", dpReq{path: "/x?env=prod"}, "query"},
		{"source ip falls to last rule", dpReq{path: "/zzz"}, "sourceip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.do(t, tt.req)
			require.Equal(t, http.StatusOK, got.status)
			assert.Equal(t, tt.want, got.body)
		})
	}
}

func TestDataPlane_NoMatchUsesDefault(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	l := f.listener(t, fixedAction("418", "teapot"))
	f.rule(t, l, 1, []elbv2types.RuleCondition{
		{
			Field: aws.String(
				"path-pattern",
			),
			PathPatternConfig: &elbv2types.PathPatternConditionConfig{Values: []string{"/only"}},
		},
	}, fixedAction("200", "rule"))

	got := f.do(t, dpReq{path: "/elsewhere"})
	assert.Equal(t, http.StatusTeapot, got.status)
	assert.Equal(t, "teapot", got.body)
	assert.Equal(t, "text/plain", got.header.Get("Content-Type"))
}

func TestDataPlane_PriorityOrder(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	l := f.listener(t, fixedAction("200", "default"))
	cond := []elbv2types.RuleCondition{
		{
			Field: aws.String(
				"path-pattern",
			),
			PathPatternConfig: &elbv2types.PathPatternConditionConfig{Values: []string{"/p"}},
		},
	}
	f.rule(t, l, 20, cond, fixedAction("200", "twenty"))
	f.rule(t, l, 5, cond, fixedAction("200", "five"))

	assert.Equal(t, "five", f.do(t, dpReq{path: "/p"}).body)
}

func TestDataPlane_Redirect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  elbv2types.RedirectActionConfig
		req  dpReq
		want string
		code int
	}{
		{
			"defaults", elbv2types.RedirectActionConfig{StatusCode: elbv2types.RedirectActionStatusCodeEnumHttp302},
			dpReq{path: "/a/b?x=1", host: "site.example.com"}, "http://site.example.com:80/a/b?x=1", http.StatusFound,
		},
		{
			"templated", elbv2types.RedirectActionConfig{
				StatusCode: elbv2types.RedirectActionStatusCodeEnumHttp301, Protocol: aws.String("HTTPS"),
				Port: aws.String("443"), Host: aws.String("new-#{host}"), Path: aws.String("/v2/#{path}"),
				Query: aws.String("from=#{port}&#{query}"),
			},
			dpReq{path: "/a/b?x=1", host: "site.example.com"}, "https://new-site.example.com:443/v2/a/b?from=80&x=1",
			http.StatusMovedPermanently,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			cfg := tt.cfg
			f.listener(t, elbv2types.Action{Type: elbv2types.ActionTypeEnumRedirect, RedirectConfig: &cfg})

			got := f.do(t, tt.req)
			assert.Equal(t, tt.code, got.status)
			assert.Equal(t, tt.want, got.header.Get("Location"))
		})
	}
}

func TestDataPlane_ForwardIPTargetsAndHeaders(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	be := newBackend(t, "a", http.StatusOK)
	tg := f.tg(t, "tg-a", elbv2types.TargetTypeEnumIp, "/health")
	f.register(t, tg, be.addr())
	f.listener(t, forwardAction(tg))

	got := f.do(t, dpReq{path: "/hello?q=1", headers: map[string]string{"X-Forwarded-For": "9.9.9.9"}})
	require.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, "a:/hello?q=1", got.body)
	assert.Equal(t, "a", got.header.Get("X-Backend"))

	seen := be.last()
	assert.Equal(t, "9.9.9.9, 127.0.0.1", seen.Header.Get("X-Forwarded-For"))
	assert.Equal(t, "http", seen.Header.Get("X-Forwarded-Proto"))
	assert.Equal(t, "80", seen.Header.Get("X-Forwarded-Port"))
	assert.True(t, strings.HasPrefix(seen.Header.Get("X-Amzn-Trace-Id"), "Root=1-"))
}

func TestDataPlane_WeightedForward(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		weights      []int32
		wantA, wantB bool
	}{
		{"split", []int32{1, 1}, true, true},
		{"all to first", []int32{1, 0}, true, false},
		{"all to second", []int32{0, 5}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			a, b := newBackend(t, "a", http.StatusOK), newBackend(t, "b", http.StatusOK)
			tgA, tgB := f.tg(
				t,
				"tg-a",
				elbv2types.TargetTypeEnumIp,
				"/health",
			), f.tg(
				t,
				"tg-b",
				elbv2types.TargetTypeEnumIp,
				"/health",
			)
			f.register(t, tgA, a.addr())
			f.register(t, tgB, b.addr())
			f.listener(t, weightedForward([]string{tgA, tgB}, tt.weights))

			for range 60 {
				require.Equal(t, http.StatusOK, f.do(t, dpReq{path: "/"}).status)
			}

			assert.Equal(t, tt.wantA, a.count() > 0)
			assert.Equal(t, tt.wantB, b.count() > 0)
		})
	}
}

func TestDataPlane_RoundRobinWithinGroup(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	a, b := newBackend(t, "a", http.StatusOK), newBackend(t, "b", http.StatusOK)
	tg := f.tg(t, "tg-rr", elbv2types.TargetTypeEnumIp, "/health")
	f.register(t, tg, a.addr(), b.addr())
	f.listener(t, forwardAction(tg))

	for range 10 {
		f.do(t, dpReq{path: "/"})
	}

	assert.Equal(t, 5, a.count())
	assert.Equal(t, 5, b.count())
}

func TestDataPlane_OnlyHealthyThenFailOpen(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	good, bad := newBackend(t, "good", http.StatusOK), newBackend(t, "bad", http.StatusInternalServerError)
	tg := f.tg(t, "tg-h", elbv2types.TargetTypeEnumIp, "/health")
	f.register(t, tg, good.addr(), bad.addr())
	f.listener(t, forwardAction(tg))

	require.Eventually(t, func() bool {
		out, err := f.client.DescribeTargetHealth(
			t.Context(),
			&elbv2sdk.DescribeTargetHealthInput{TargetGroupArn: aws.String(tg)},
		)
		if err != nil {
			return false
		}

		return len(out.TargetHealthDescriptions) == 2 && !hasState(out.TargetHealthDescriptions, "initial")
	}, 10*time.Second, 50*time.Millisecond)

	for range 20 {
		f.do(t, dpReq{path: "/"})
	}

	assert.Equal(t, 20, good.count())
	assert.Zero(t, bad.count())
}

func hasState(ds []elbv2types.TargetHealthDescription, state string) bool {
	for _, d := range ds {
		if string(d.TargetHealth.State) == state {
			return true
		}
	}

	return false
}

func TestDataPlane_AllUnhealthyFailsOpen(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	bad := newBackend(t, "bad", http.StatusInternalServerError)
	tg := f.tg(t, "tg-u", elbv2types.TargetTypeEnumIp, "/health")
	f.register(t, tg, bad.addr())
	f.listener(t, forwardAction(tg))

	require.Eventually(t, func() bool {
		out, err := f.client.DescribeTargetHealth(
			t.Context(),
			&elbv2sdk.DescribeTargetHealthInput{TargetGroupArn: aws.String(tg)},
		)

		return err == nil && len(out.TargetHealthDescriptions) == 1 &&
			out.TargetHealthDescriptions[0].TargetHealth.State == elbv2types.TargetHealthStateEnumUnhealthy
	}, 10*time.Second, 50*time.Millisecond)

	got := f.do(t, dpReq{path: "/x"})
	assert.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, 1, bad.count())
}

func TestDataPlane_Unroutable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, f *dpFixture)
		name  string
		want  int
	}{
		{name: "no targets", setup: func(t *testing.T, f *dpFixture) {
			t.Helper()

			f.listener(t, forwardAction(f.tg(t, "empty", elbv2types.TargetTypeEnumIp, "")))
		}, want: http.StatusServiceUnavailable},
		{name: "link local target", setup: func(t *testing.T, f *dpFixture) {
			t.Helper()

			tg := f.tg(t, "ll", elbv2types.TargetTypeEnumIp, "")
			f.register(t, tg, "169.254.169.254")
			f.listener(t, forwardAction(tg))
		}, want: http.StatusServiceUnavailable},
		{name: "instance without docker", setup: func(t *testing.T, f *dpFixture) {
			t.Helper()

			tg := f.tg(t, "inst", elbv2types.TargetTypeEnumInstance, "")
			f.register(t, tg, "i-0123456789abcdef0")
			f.listener(t, forwardAction(tg))
		}, want: http.StatusServiceUnavailable},
		{name: "authenticate", setup: func(t *testing.T, f *dpFixture) {
			t.Helper()

			f.listener(t, elbv2types.Action{
				Type: elbv2types.ActionTypeEnumAuthenticateOidc, Order: aws.Int32(1),
				AuthenticateOidcConfig: &elbv2types.AuthenticateOidcActionConfig{
					Issuer: aws.String("https://i"), AuthorizationEndpoint: aws.String("https://a"),
					TokenEndpoint: aws.String("https://t"), UserInfoEndpoint: aws.String("https://u"),
					ClientId: aws.String("c"), ClientSecret: aws.String("s"),
				},
			}, elbv2types.Action{Type: elbv2types.ActionTypeEnumFixedResponse, Order: aws.Int32(2),
				FixedResponseConfig: &elbv2types.FixedResponseActionConfig{StatusCode: aws.String("200")}})
		}, want: http.StatusNotImplemented},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			tt.setup(t, f)
			assert.Equal(t, tt.want, f.do(t, dpReq{path: "/"}).status)
		})
	}
}

func TestDataPlane_UnknownLoadBalancer(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	f.lbName = "missing"
	assert.Equal(t, http.StatusNotFound, f.do(t, dpReq{path: "/"}).status)
}

func TestDataPlane_HostHeaderAccess(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	f.listener(t, fixedAction("200", "via-host"))

	tests := []struct {
		name string
		host string
		want int
	}{
		{"dns name", f.lbDNS, http.StatusOK},
		{"dns name with port", f.lbDNS + ":8000", http.StatusOK},
		{"localhost suffix", strings.TrimSuffix(f.lbDNS, ".elb.amazonaws.com") + ".elb.localhost", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.do(t, dpReq{path: "/", host: tt.host})
			assert.Equal(t, tt.want, got.status)
			assert.Equal(t, "via-host", got.body)
		})
	}
}

func TestDataPlane_StickySessions(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	a, b := newBackend(t, "a", http.StatusOK), newBackend(t, "b", http.StatusOK)
	tg := f.tg(t, "tg-s", elbv2types.TargetTypeEnumIp, "/health")
	f.register(t, tg, a.addr(), b.addr())
	_, err := f.client.ModifyTargetGroupAttributes(t.Context(), &elbv2sdk.ModifyTargetGroupAttributesInput{
		TargetGroupArn: aws.String(tg),
		Attributes: []elbv2types.TargetGroupAttribute{
			{Key: aws.String("stickiness.enabled"), Value: aws.String("true")},
			{Key: aws.String("stickiness.type"), Value: aws.String("lb_cookie")},
		},
	})
	require.NoError(t, err)
	f.listener(t, forwardAction(tg))

	first := f.do(t, dpReq{path: "/"})
	cookie := first.header.Get("Set-Cookie")
	require.True(t, strings.HasPrefix(cookie, "AWSALB="), cookie)
	pinned := first.header.Get("X-Backend")

	for range 10 {
		got := f.do(t, dpReq{path: "/", headers: map[string]string{"Cookie": strings.SplitN(cookie, ";", 2)[0]}})
		assert.Equal(t, pinned, got.header.Get("X-Backend"))
	}
}
