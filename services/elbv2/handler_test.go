package elbv2_test

import (
	"encoding/xml"
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

func newBatch1Handler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

func newBatch1Backend() *elbv2.InMemoryBackend {
	return elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)
}

func b1CreateLB(t *testing.T, h *elbv2.Handler, name string, extra ...url.Values) string {
	t.Helper()
	vals := url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"application"},
	}
	if len(extra) > 0 {
		maps.Copy(vals, extra[0])
	}
	rec := doELBv2(t, h, vals)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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

func b1CreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp struct {
		Result struct {
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

func b1CreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
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
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

func newBatch2Handler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

// mustCreateRule creates a forward rule on the given listener and returns its ARN.
func mustCreateRule(t *testing.T, h *elbv2.Handler, listenerArn, tgArn, priority string) string {
	t.Helper()

	rec := doELBv2(t, h, url.Values{
		"Action":                              {"CreateRule"},
		"Version":                             {"2015-12-01"},
		"ListenerArn":                         {listenerArn},
		"Priority":                            {priority},
		"Conditions.member.1.Field":           {"path-pattern"},
		"Conditions.member.1.Values.member.1": {"/p-" + priority + "/*"},
		"Actions.member.1.Type":               {"forward"},
		"Actions.member.1.TargetGroupArn":     {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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

	return resp.Result.Rules.Members[0].RuleArn
}

// itoa converts an int to its decimal string representation.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	b := make([]byte, 0, 10)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}

	return string(b)
}

func newParityBHandler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

// pbCreateLB creates a load balancer for parity_b tests.
func pbCreateLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"application"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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

// pbCreateTG creates a target group for parity_b tests.
func pbCreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
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

// pbCreateListener creates a listener for parity_b tests.
func pbCreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
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
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

// pbCreateRule creates a rule on a listener with the given priority.
func pbCreateRule(t *testing.T, h *elbv2.Handler, listenerArn, tgArn, priority string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {listenerArn},
		"Priority":                  {priority},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + priority + "/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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

	return resp.Result.Rules.Members[0].RuleArn
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
