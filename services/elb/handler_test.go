package elb_test

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
	"github.com/blackbirdworks/gopherstack/services/elb"
)

func newTestHandler() *elb.Handler {
	backend := elb.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return elb.NewHandler(backend)
}

// doELB sends a form-encoded POST to the ELB handler and returns the recorder.
func doELB(t *testing.T, h *elb.Handler, vals url.Values) *httptest.ResponseRecorder {
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

// mustCreateLB is a helper that creates a load balancer and asserts success.
func mustCreateLB(t *testing.T, h *elb.Handler, name string) {
	t.Helper()

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {name},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// mustCreateVPCLB creates a VPC load balancer (with a subnet) and asserts success.
// VPC LBs are required for ApplySecurityGroups and AttachSubnets operations.
func mustCreateVPCLB(t *testing.T, h *elb.Handler, name string) {
	t.Helper()

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {name},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"Subnets.member.1":                    {"subnet-00001"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// parseXMLBody parses raw XML from a recorder body into dst.
func parseXMLBody(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), dst))
}

func newBackend() *elb.InMemoryBackend {
	return elb.NewInMemoryBackend("123456789012", "us-east-1")
}

// TestHandlerMetadata tests metadata methods.
func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "ELB", h.Name())
	assert.Contains(t, h.GetSupportedOperations(), "CreateLoadBalancer")
	assert.Contains(t, h.GetSupportedOperations(), "DescribeLoadBalancers")
	assert.Contains(t, h.GetSupportedOperations(), "DeleteLoadBalancer")
	assert.Contains(t, h.GetSupportedOperations(), "RegisterInstancesWithLoadBalancer")
	assert.Contains(t, h.GetSupportedOperations(), "DeregisterInstancesFromLoadBalancer")
	assert.Contains(t, h.GetSupportedOperations(), "ConfigureHealthCheck")
	assert.Contains(t, h.GetSupportedOperations(), "AddTags")
	assert.Contains(t, h.GetSupportedOperations(), "DescribeTags")
	assert.Contains(t, h.GetSupportedOperations(), "RemoveTags")
	assert.Equal(t, "elasticloadbalancing", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRouteMatcher tests the route matcher.
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
			name:        "matches_elb_form_post",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateLoadBalancer&Version=2012-06-01",
			want:        true,
		},
		{
			name:        "rejects_wrong_version",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateLoadBalancer&Version=2011-01-01",
			want:        false,
		},
		{
			name:        "rejects_get_method",
			method:      http.MethodGet,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Version=2012-06-01",
			want:        false,
		},
		{
			name:        "rejects_json_content_type",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/json",
			body:        "Version=2012-06-01",
			want:        false,
		},
		{
			name:        "rejects_dashboard_path",
			method:      http.MethodPost,
			path:        "/dashboard/elb",
			contentType: "application/x-www-form-urlencoded",
			body:        "Version=2012-06-01",
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

// TestExtractOperation tests operation extraction.
func TestExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "extracts_create",
			body:   "Action=CreateLoadBalancer&Version=2012-06-01",
			wantOp: "CreateLoadBalancer",
		},
		{
			name:   "extracts_describe",
			body:   "Action=DescribeLoadBalancers&Version=2012-06-01",
			wantOp: "DescribeLoadBalancers",
		},
		{
			name:   "empty_action_returns_unknown",
			body:   "Version=2012-06-01",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestExtractResource tests resource extraction.
func TestExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "extracts_lb_name",
			body:     "Action=CreateLoadBalancer&LoadBalancerName=my-lb",
			wantName: "my-lb",
		},
		{
			name:     "missing_name_returns_empty",
			body:     "Action=DescribeLoadBalancers",
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

// TestUnknownAction tests that unknown actions return an error.
func TestUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"UnknownAction"},
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestMissingAction tests that a missing action returns 400.
func TestMissingAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := "Version=2012-06-01"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestMatchPriority tests the match priority value.
func TestMatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestUnknownActionErrorCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"NoSuchAction"},
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidAction", errResp.Error.Code)
}

func TestMissingActionStatusCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestChaosRegions verifies that ChaosRegions returns the region
// configured on the backend, not always the default region.
func TestChaosRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		name   string
	}{
		{"us-east-1", "us_east_1"},
		{"eu-west-1", "eu_west_1"},
		{"ap-southeast-1", "ap_southeast_1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elb.NewInMemoryBackend("123456789012", tt.region)
			h := elb.NewHandler(b)

			regions := h.ChaosRegions()
			assert.Contains(t, regions, tt.region)
		})
	}
}

// TestHandlerReset verifies that Handler.Reset() delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "hr-lb")

	require.Equal(t, 1, b.LoadBalancerCount())
	h.Reset()
	assert.Equal(t, 0, b.LoadBalancerCount())
}

// TestHandlerOpsPreBuilt verifies that the dispatch table is pre-built at
// construction time and has at least 29 entries.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	assert.GreaterOrEqual(t, h.HandlerOpsLen(), 29)
}

// TestGetSupportedOperationsAllOps verifies the supported ops list has all 29 entries.
func TestGetSupportedOperationsAllOps(t *testing.T) {
	t.Parallel()

	h := elb.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateLoadBalancer",
		"DeleteLoadBalancer",
		"DescribeLoadBalancers",
		"CreateLoadBalancerListeners",
		"DeleteLoadBalancerListeners",
		"RegisterInstancesWithLoadBalancer",
		"DeregisterInstancesFromLoadBalancer",
		"ConfigureHealthCheck",
		"ModifyLoadBalancerAttributes",
		"DescribeLoadBalancerAttributes",
		"AddTags",
		"DescribeTags",
		"RemoveTags",
		"ApplySecurityGroupsToLoadBalancer",
		"AttachLoadBalancerToSubnets",
		"DetachLoadBalancerFromSubnets",
		"EnableAvailabilityZonesForLoadBalancer",
		"DisableAvailabilityZonesForLoadBalancer",
		"SetLoadBalancerListenerSSLCertificate",
		"SetLoadBalancerPoliciesOfListener",
		"SetLoadBalancerPoliciesForBackendServer",
		"CreateAppCookieStickinessPolicy",
		"CreateLBCookieStickinessPolicy",
		"CreateLoadBalancerPolicy",
		"DeleteLoadBalancerPolicy",
		"DescribeAccountLimits",
		"DescribeInstanceHealth",
		"DescribeLoadBalancerPolicies",
		"DescribeLoadBalancerPolicyTypes",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "GetSupportedOperations missing %q", op)
	}
}

// TestErrValidationMapping verifies ErrInvalidParameter maps to HTTP 400.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := elb.NewHandler(newBackend())

	rec := doELB(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2012-06-01"},
		// Missing LoadBalancerName → ErrInvalidParameter → 400
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
