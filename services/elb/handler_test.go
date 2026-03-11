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

// parseXMLBody parses raw XML from a recorder body into dst.
func parseXMLBody(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), dst))
}

// TestCreateLoadBalancer tests create and duplicate error.
func TestCreateLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantDNS    string
		wantStatus int
	}{
		{
			name: "creates_successfully",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"my-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
			wantDNS:    "my-lb.us-east-1.elb.amazonaws.com",
		},
		{
			name: "duplicate_returns_conflict",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "missing_name_returns_bad_request",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_scheme",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"internal-lb"},
				"Scheme":                              {"internal"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
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

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDNS != "" {
				var resp struct {
					XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
					Result  struct {
						DNSName string `xml:"DNSName"`
					} `xml:"CreateLoadBalancerResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Equal(t, tt.wantDNS, resp.Result.DNSName)
			}
		})
	}
}

// TestDeleteLoadBalancer tests delete operations.
func TestDeleteLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "delete-me")
			},
			vals: url.Values{
				"Action":           {"DeleteLoadBalancer"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"delete-me"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":           {"DeleteLoadBalancer"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_name",
			vals: url.Values{
				"Action":  {"DeleteLoadBalancer"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeLoadBalancers tests describe operations.
func TestDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lb-a")
				mustCreateLB(t, h, "lb-b")
			},
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "describe_by_name",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "named-lb")
			},
			vals: url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"named-lb"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "describe_not_found",
			vals: url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"missing-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "describe_empty",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp struct {
					XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
					Result  struct {
						LoadBalancerDescriptions struct {
							Members []struct {
								Name string `xml:"LoadBalancerName"`
							} `xml:"member"`
						} `xml:"LoadBalancerDescriptions"`
					} `xml:"DescribeLoadBalancersResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.LoadBalancerDescriptions.Members, tt.wantCount)
			}
		})
	}
}

// TestRegisterAndDeregisterInstances tests instance registration.
func TestRegisterAndDeregisterInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "register_instances",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "reg-lb")
			},
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"reg-lb"},
				"Instances.member.1.InstanceId": {"i-aaa111"},
				"Instances.member.2.InstanceId": {"i-bbb222"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "register_idempotent",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "idem-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"idem-lb"},
					"Instances.member.1.InstanceId": {"i-abc"},
				})
			},
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"idem-lb"},
				"Instances.member.1.InstanceId": {"i-abc"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "register_lb_not_found",
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"no-lb"},
				"Instances.member.1.InstanceId": {"i-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "register_missing_name",
			vals: url.Values{
				"Action":  {"RegisterInstancesWithLoadBalancer"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "deregister_instances",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dereg-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"dereg-lb"},
					"Instances.member.1.InstanceId": {"i-111"},
					"Instances.member.2.InstanceId": {"i-222"},
				})
			},
			vals: url.Values{
				"Action":                        {"DeregisterInstancesFromLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"dereg-lb"},
				"Instances.member.1.InstanceId": {"i-111"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "deregister_lb_not_found",
			vals: url.Values{
				"Action":                        {"DeregisterInstancesFromLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"no-lb"},
				"Instances.member.1.InstanceId": {"i-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "deregister_missing_name",
			vals: url.Values{
				"Action":  {"DeregisterInstancesFromLoadBalancer"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestConfigureHealthCheck tests health check configuration.
func TestConfigureHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantTarget string
		wantStatus int
	}{
		{
			name: "configure_health_check",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "hc-lb")
			},
			vals: url.Values{
				"Action":                         {"ConfigureHealthCheck"},
				"Version":                        {"2012-06-01"},
				"LoadBalancerName":               {"hc-lb"},
				"HealthCheck.Target":             {"HTTP:8080/health"},
				"HealthCheck.Interval":           {"30"},
				"HealthCheck.Timeout":            {"5"},
				"HealthCheck.UnhealthyThreshold": {"3"},
				"HealthCheck.HealthyThreshold":   {"2"},
			},
			wantStatus: http.StatusOK,
			wantTarget: "HTTP:8080/health",
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":             {"ConfigureHealthCheck"},
				"Version":            {"2012-06-01"},
				"LoadBalancerName":   {"no-lb"},
				"HealthCheck.Target": {"HTTP:80/health"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_name",
			vals: url.Values{
				"Action":  {"ConfigureHealthCheck"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTarget != "" {
				var resp struct {
					XMLName xml.Name `xml:"ConfigureHealthCheckResponse"`
					Result  struct {
						HealthCheck struct {
							Target string `xml:"Target"`
						} `xml:"HealthCheck"`
					} `xml:"ConfigureHealthCheckResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Equal(t, tt.wantTarget, resp.Result.HealthCheck.Target)
			}
		})
	}
}

// TestTagOperations tests AddTags, DescribeTags, RemoveTags.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *elb.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "add_and_describe_tags",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "tag-lb")

				rec := doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-lb"},
					"Tags.member.1.Key":          {"Env"},
					"Tags.member.1.Value":        {"prod"},
					"Tags.member.2.Key":          {"Team"},
					"Tags.member.2.Value":        {"platform"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-lb"},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp struct {
					XMLName xml.Name `xml:"DescribeTagsResponse"`
					Result  struct {
						TagDescriptions struct {
							Members []struct {
								Name string `xml:"LoadBalancerName"`
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
				require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TagDescriptions.Members, 1)
				assert.Equal(t, "tag-lb", resp.Result.TagDescriptions.Members[0].Name)
				assert.Len(t, resp.Result.TagDescriptions.Members[0].Tags.Members, 2)
			},
		},
		{
			name: "remove_tags",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "rmtag-lb")

				doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
					"Tags.member.1.Key":          {"Env"},
					"Tags.member.1.Value":        {"prod"},
					"Tags.member.2.Key":          {"Extra"},
					"Tags.member.2.Value":        {"remove-me"},
				})

				rec := doELB(t, h, url.Values{
					"Action":                     {"RemoveTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
					"Tags.member.1.Key":          {"Extra"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"rmtag-lb"},
				})
				var resp struct {
					XMLName xml.Name `xml:"DescribeTagsResponse"`
					Result  struct {
						TagDescriptions struct {
							Members []struct {
								Tags struct {
									Members []struct {
										Key string `xml:"Key"`
									} `xml:"member"`
								} `xml:"Tags"`
							} `xml:"member"`
						} `xml:"TagDescriptions"`
					} `xml:"DescribeTagsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.TagDescriptions.Members[0].Tags.Members, 1)
				assert.Equal(t, "Env", resp.Result.TagDescriptions.Members[0].Tags.Members[0].Key)
			},
		},
		{
			name: "add_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
					"Tags.member.1.Key":          {"k"},
					"Tags.member.1.Value":        {"v"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"DescribeTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "remove_tags_lb_not_found",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                     {"RemoveTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"no-lb"},
					"Tags.member.1.Key":          {"k"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "add_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"AddTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"DescribeTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "remove_tags_missing_lb_name",
			ops: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":  {"RemoveTags"},
					"Version": {"2012-06-01"},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.ops(t, h)
		})
	}
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

// TestDescribeLoadBalancersWithHealthCheck tests that health check is included in describe.
func TestDescribeLoadBalancersWithHealthCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-describe-lb")

	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-describe-lb"},
		"HealthCheck.Target":             {"TCP:80"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"3"},
		"HealthCheck.HealthyThreshold":   {"2"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"hc-describe-lb"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					HealthCheck struct {
						Target string `xml:"Target"`
					} `xml:"HealthCheck"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	parseXMLBody(t, rec, &resp)
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	assert.Equal(t, "TCP:80", resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck.Target)
}

// TestCreateLoadBalancerListeners tests adding listeners to an existing LB.
func TestCreateLoadBalancerListeners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantErrMsg string
		wantStatus int
	}{
		{
			name: "adds_listener_to_existing_lb",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "listeners-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"listeners-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"8443"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"no-such-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"CreateLoadBalancerListeners"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeleteLoadBalancerListeners tests removing listeners from an existing LB.
func TestDeleteLoadBalancerListeners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "deletes_listener",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "del-listener-lb")
			},
			vals: url.Values{
				"Action":                     {"DeleteLoadBalancerListeners"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"del-listener-lb"},
				"LoadBalancerPorts.member.1": {"80"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":                     {"DeleteLoadBalancerListeners"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"no-such-lb"},
				"LoadBalancerPorts.member.1": {"80"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"DeleteLoadBalancerListeners"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestModifyLoadBalancerAttributes tests modifying LB attributes.
func TestModifyLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantXZLB   string
		wantStatus int
	}{
		{
			name: "sets_cross_zone_and_idle_timeout",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "attrs-lb")
			},
			vals: url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"attrs-lb"},
				"LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled":      {"true"},
				"LoadBalancerAttributes.ConnectionDraining.Enabled":          {"false"},
				"LoadBalancerAttributes.ConnectionDraining.Timeout":          {"300"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout":      {"120"},
				"LoadBalancerAttributes.AdditionalAttributes.member.1.Key":   {"elb.http.desyncmitigationmode"},
				"LoadBalancerAttributes.AdditionalAttributes.member.1.Value": {"monitor"},
			},
			wantStatus: http.StatusOK,
			wantXZLB:   "true",
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"ModifyLoadBalancerAttributes"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXZLB != "" {
				var resp struct {
					XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
					Result  struct {
						LoadBalancerAttributes struct {
							CrossZoneLoadBalancing struct {
								Enabled string `xml:"Enabled"`
							} `xml:"CrossZoneLoadBalancing"`
						} `xml:"LoadBalancerAttributes"`
					} `xml:"ModifyLoadBalancerAttributesResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Equal(t, tt.wantXZLB, resp.Result.LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled)
			}
		})
	}
}

// TestDescribeLoadBalancerAttributes tests reading LB attributes.
func TestDescribeLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		checkResp  bool
	}{
		{
			name: "returns_default_attributes",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "descattrs-lb")
			},
			vals: url.Values{
				"Action":           {"DescribeLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"descattrs-lb"},
			},
			wantStatus: http.StatusOK,
			checkResp:  true,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"DescribeLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancerAttributes"},
				"Version": {"2012-06-01"},
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

			rec := doELB(t, h, tt.vals)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp {
				var resp struct {
					XMLName xml.Name `xml:"DescribeLoadBalancerAttributesResponse"`
					Result  struct {
						LoadBalancerAttributes struct {
							ConnectionSettings struct {
								IdleTimeout string `xml:"IdleTimeout"`
							} `xml:"ConnectionSettings"`
						} `xml:"LoadBalancerAttributes"`
					} `xml:"DescribeLoadBalancerAttributesResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Equal(t, "60", resp.Result.LoadBalancerAttributes.ConnectionSettings.IdleTimeout)
			}
		})
	}
}
