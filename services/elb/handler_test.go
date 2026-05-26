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
		"Subnets.member.1":                   {"subnet-00001"},
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
			// DNS now includes a hash suffix: my-lb-<hash>.us-east-1.elb.amazonaws.com
			wantDNS: "my-lb-",
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
				"AvailabilityZones.member.1":          {"us-east-1a"},
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
				assert.Contains(t, resp.Result.DNSName, tt.wantDNS)
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
				"Instances.member.1.InstanceId": {"i-aaa11100"},
				"Instances.member.2.InstanceId": {"i-bbb22200"},
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
					"Instances.member.1.InstanceId": {"i-abc00000"},
				})
			},
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"idem-lb"},
				"Instances.member.1.InstanceId": {"i-abc00000"},
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
				"Instances.member.1.InstanceId": {"i-aaa00000"},
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
					"Instances.member.1.InstanceId": {"i-11100000"},
					"Instances.member.2.InstanceId": {"i-22200000"},
				})
			},
			vals: url.Values{
				"Action":                        {"DeregisterInstancesFromLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"dereg-lb"},
				"Instances.member.1.InstanceId": {"i-11100000"},
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
				"Instances.member.1.InstanceId": {"i-aaa00000"},
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

// TestDescribeLoadBalancersHealthCheckAlwaysPresent verifies that the HealthCheck
// XML element is always included in DescribeLoadBalancers responses, even when no
// health check is configured. This prevents a nil-pointer panic in the Terraform
// AWS provider which accesses lb.HealthCheck.Target without a nil guard.
func TestDescribeLoadBalancersHealthCheckAlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "no-hc-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"no-hc-lb"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					HealthCheck *struct {
						Target string `xml:"Target"`
					} `xml:"HealthCheck"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	parseXMLBody(t, rec, &resp)
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	assert.NotNil(
		t,
		resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck,
		"HealthCheck element must always be present",
	)
	assert.Empty(t, resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck.Target)
}

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

// TestApplySecurityGroupsToLoadBalancer tests replacing security groups on a VPC LB.
func TestApplySecurityGroupsToLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantSGLen  int
		wantStatus int
	}{
		{
			// ApplySecurityGroups requires a VPC LB (created with subnets).
			name: "applies_security_groups",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "sg-lb")
			},
			vals: url.Values{
				"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
				"Version":                 {"2012-06-01"},
				"LoadBalancerName":        {"sg-lb"},
				"SecurityGroups.member.1": {"sg-aaa"},
				"SecurityGroups.member.2": {"sg-bbb"},
			},
			wantStatus: http.StatusOK,
			wantSGLen:  2,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
				"Version":                 {"2012-06-01"},
				"LoadBalancerName":        {"no-lb"},
				"SecurityGroups.member.1": {"sg-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"ApplySecurityGroupsToLoadBalancer"},
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

			if tt.wantSGLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
					Result  struct {
						SecurityGroups struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"SecurityGroups"`
					} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.SecurityGroups.Members, tt.wantSGLen)
			}
		})
	}
}

// TestAttachLoadBalancerToSubnets tests attaching subnets to an existing LB.
func TestAttachLoadBalancerToSubnets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *elb.Handler)
		vals          url.Values
		name          string
		wantSubnetLen int
		wantStatus    int
	}{
		{
			// AttachLoadBalancerToSubnets requires a VPC LB (created with subnets).
			name: "attaches_subnets",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "subnet-lb")
			},
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"subnet-lb"},
				"Subnets.member.1": {"subnet-aaa"},
				"Subnets.member.2": {"subnet-bbb"},
			},
			wantStatus:    http.StatusOK,
			wantSubnetLen: 3, // subnet-00001 (from create) + subnet-aaa + subnet-bbb
		},
		{
			name: "idempotent_attach",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "subnet-idem-lb")
				doELB(t, h, url.Values{
					"Action":           {"AttachLoadBalancerToSubnets"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"subnet-idem-lb"},
					"Subnets.member.1": {"subnet-aaa"},
				})
			},
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"subnet-idem-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus:    http.StatusOK,
			wantSubnetLen: 2, // subnet-00001 (from create) + subnet-aaa
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"AttachLoadBalancerToSubnets"},
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

			if tt.wantSubnetLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"AttachLoadBalancerToSubnetsResponse"`
					Result  struct {
						Subnets struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"Subnets"`
					} `xml:"AttachLoadBalancerToSubnetsResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.Subnets.Members, tt.wantSubnetLen)
			}
		})
	}
}

// TestCreateAppCookieStickinessPolicy tests app cookie stickiness policy creation.
func TestCreateAppCookieStickinessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "creates_app_cookie_policy",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "appcookie-lb")
			},
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"appcookie-lb"},
				"PolicyName":       {"my-app-cookie-policy"},
				"CookieName":       {"JSESSIONID"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate_policy_returns_conflict",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "appcookie-dup-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateAppCookieStickinessPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"appcookie-dup-lb"},
					"PolicyName":       {"dup-policy"},
					"CookieName":       {"SESSION"},
				})
			},
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"appcookie-dup-lb"},
				"PolicyName":       {"dup-policy"},
				"CookieName":       {"SESSION"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"PolicyName":       {"my-policy"},
				"CookieName":       {"SESSION"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":     {"CreateAppCookieStickinessPolicy"},
				"Version":    {"2012-06-01"},
				"PolicyName": {"my-policy"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_policy_name",
			vals: url.Values{
				"Action":           {"CreateAppCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"some-lb"},
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

// TestCreateLBCookieStickinessPolicy tests LB cookie stickiness policy creation.
func TestCreateLBCookieStickinessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "creates_lb_cookie_policy_with_expiration",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lbcookie-lb")
			},
			vals: url.Values{
				"Action":                 {"CreateLBCookieStickinessPolicy"},
				"Version":                {"2012-06-01"},
				"LoadBalancerName":       {"lbcookie-lb"},
				"PolicyName":             {"my-lb-cookie-policy"},
				"CookieExpirationPeriod": {"86400"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "creates_lb_cookie_policy_without_expiration",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lbcookie2-lb")
			},
			vals: url.Values{
				"Action":           {"CreateLBCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lbcookie2-lb"},
				"PolicyName":       {"browser-session-policy"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_expiration_period",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lbcookie3-lb")
			},
			vals: url.Values{
				"Action":                 {"CreateLBCookieStickinessPolicy"},
				"Version":                {"2012-06-01"},
				"LoadBalancerName":       {"lbcookie3-lb"},
				"PolicyName":             {"bad-expiry-policy"},
				"CookieExpirationPeriod": {"not-a-number"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"CreateLBCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"PolicyName":       {"my-policy"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":     {"CreateLBCookieStickinessPolicy"},
				"Version":    {"2012-06-01"},
				"PolicyName": {"my-policy"},
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

// TestCreateLoadBalancerPolicy tests custom LB policy creation.
func TestCreateLoadBalancerPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "creates_policy_with_attributes",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "policy-lb")
			},
			vals: url.Values{
				"Action":           {"CreateLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"policy-lb"},
				"PolicyName":       {"my-proxy-policy"},
				"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				"PolicyAttributes.member.1.AttributeName":  {"ProxyProtocol"},
				"PolicyAttributes.member.1.AttributeValue": {"true"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate_policy_returns_conflict",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "policy-dup-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"policy-dup-lb"},
					"PolicyName":       {"dup-policy"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
			},
			vals: url.Values{
				"Action":           {"CreateLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"policy-dup-lb"},
				"PolicyName":       {"dup-policy"},
				"PolicyTypeName":   {"ProxyProtocolPolicyType"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"CreateLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"PolicyName":       {"my-policy"},
				"PolicyTypeName":   {"ProxyProtocolPolicyType"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":         {"CreateLoadBalancerPolicy"},
				"Version":        {"2012-06-01"},
				"PolicyName":     {"my-policy"},
				"PolicyTypeName": {"ProxyProtocolPolicyType"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_policy_name",
			vals: url.Values{
				"Action":           {"CreateLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"some-lb"},
				"PolicyTypeName":   {"ProxyProtocolPolicyType"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_policy_type_name",
			vals: url.Values{
				"Action":           {"CreateLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"some-lb"},
				"PolicyName":       {"my-policy"},
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

// TestDeleteLoadBalancerPolicy tests deleting a policy from a load balancer.
func TestDeleteLoadBalancerPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "deletes_existing_policy",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "delpol-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"delpol-lb"},
					"PolicyName":       {"policy-to-delete"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
			},
			vals: url.Values{
				"Action":           {"DeleteLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"delpol-lb"},
				"PolicyName":       {"policy-to-delete"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "policy_not_found",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "delpol2-lb")
			},
			vals: url.Values{
				"Action":           {"DeleteLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"delpol2-lb"},
				"PolicyName":       {"nonexistent-policy"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"DeleteLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"PolicyName":       {"some-policy"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":     {"DeleteLoadBalancerPolicy"},
				"Version":    {"2012-06-01"},
				"PolicyName": {"some-policy"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_policy_name",
			vals: url.Values{
				"Action":           {"DeleteLoadBalancerPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"some-lb"},
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

// TestDescribeAccountLimits tests returning account limits.
func TestDescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.NotEmpty(t, resp.Result.Limits.Members)

	names := make([]string, 0, len(resp.Result.Limits.Members))
	for _, m := range resp.Result.Limits.Members {
		names = append(names, m.Name)
	}

	assert.Contains(t, names, "classic-load-balancers")
	assert.Contains(t, names, "classic-listeners")
}

// TestDescribeInstanceHealth tests the health state of registered instances.
func TestDescribeInstanceHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elb.Handler)
		vals         url.Values
		name         string
		wantStatus   int
		wantStateLen int
	}{
		{
			name: "all_instances_inservice",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"health-lb"},
					"Instances.member.1.InstanceId": {"i-aaa00000"},
					"Instances.member.2.InstanceId": {"i-bbb00000"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeInstanceHealth"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"health-lb"},
			},
			wantStatus:   http.StatusOK,
			wantStateLen: 2,
		},
		{
			name: "specific_instance_health",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health2-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"health2-lb"},
					"Instances.member.1.InstanceId": {"i-abcdef01"},
				})
			},
			vals: url.Values{
				"Action":                        {"DescribeInstanceHealth"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"health2-lb"},
				"Instances.member.1.InstanceId": {"i-abcdef01"},
			},
			wantStatus:   http.StatusOK,
			wantStateLen: 1,
		},
		{
			name: "unregistered_instance_returns_invalid_instance",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health3-lb")
			},
			vals: url.Values{
				"Action":                        {"DescribeInstanceHealth"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"health3-lb"},
				"Instances.member.1.InstanceId": {"i-dead0000"},
			},
			wantStatus:   http.StatusBadRequest,
			wantStateLen: 0,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"DescribeInstanceHealth"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"DescribeInstanceHealth"},
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

			if tt.wantStateLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
					Result  struct {
						InstanceStates struct {
							Members []struct {
								InstanceID string `xml:"InstanceId"`
								State      string `xml:"State"`
							} `xml:"member"`
						} `xml:"InstanceStates"`
					} `xml:"DescribeInstanceHealthResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.InstanceStates.Members, tt.wantStateLen)
			}
		})
	}
}

// TestDescribeLoadBalancerPolicies tests retrieving LB policies.
func TestDescribeLoadBalancerPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *elb.Handler)
		vals          url.Values
		name          string
		wantPolicyLen int
		wantStatus    int
	}{
		{
			name: "describe_all_policies_for_lb",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "pol-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"pol-lb"},
					"PolicyName":       {"pol-a"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"pol-lb"},
					"PolicyName":       {"pol-b"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeLoadBalancerPolicies"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"pol-lb"},
			},
			wantStatus:    http.StatusOK,
			wantPolicyLen: 2,
		},
		{
			name: "describe_policies_by_name",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "pol2-lb")
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"pol2-lb"},
					"PolicyName":       {"target-pol"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
				doELB(t, h, url.Values{
					"Action":           {"CreateLoadBalancerPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"pol2-lb"},
					"PolicyName":       {"other-pol"},
					"PolicyTypeName":   {"ProxyProtocolPolicyType"},
				})
			},
			vals: url.Values{
				"Action":               {"DescribeLoadBalancerPolicies"},
				"Version":              {"2012-06-01"},
				"LoadBalancerName":     {"pol2-lb"},
				"PolicyNames.member.1": {"target-pol"},
			},
			wantStatus:    http.StatusOK,
			wantPolicyLen: 1,
		},
		{
			// When no LoadBalancerName is given, AWS returns only the
			// built-in sample SSL policies (not customer policies).
			name: "describe_no_lb_name_returns_sample_policies",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancerPolicies"},
				"Version": {"2012-06-01"},
			},
			wantStatus:    http.StatusOK,
			wantPolicyLen: 4,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"DescribeLoadBalancerPolicies"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantPolicyLen >= 0 && tt.wantStatus == http.StatusOK {
				var resp struct {
					XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
					Result  struct {
						PolicyDescriptions struct {
							Members []struct {
								PolicyName string `xml:"PolicyName"`
							} `xml:"member"`
						} `xml:"PolicyDescriptions"`
					} `xml:"DescribeLoadBalancerPoliciesResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.PolicyDescriptions.Members, tt.wantPolicyLen)
			}
		})
	}
}

// TestDescribeLoadBalancerPolicyTypes tests retrieving policy type descriptions.
func TestDescribeLoadBalancerPolicyTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantTypeName string
		wantTypeLen  int
		wantStatus   int
	}{
		{
			name: "returns_all_policy_types",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancerPolicyTypes"},
				"Version": {"2012-06-01"},
			},
			wantStatus:  http.StatusOK,
			wantTypeLen: 6,
		},
		{
			name: "filters_by_type_name",
			vals: url.Values{
				"Action":                   {"DescribeLoadBalancerPolicyTypes"},
				"Version":                  {"2012-06-01"},
				"PolicyTypeNames.member.1": {"AppCookieStickinessPolicyType"},
			},
			wantStatus:   http.StatusOK,
			wantTypeLen:  1,
			wantTypeName: "AppCookieStickinessPolicyType",
		},
		{
			name: "returns_lb_cookie_type",
			vals: url.Values{
				"Action":                   {"DescribeLoadBalancerPolicyTypes"},
				"Version":                  {"2012-06-01"},
				"PolicyTypeNames.member.1": {"LBCookieStickinessPolicyType"},
			},
			wantStatus:   http.StatusOK,
			wantTypeLen:  1,
			wantTypeName: "LBCookieStickinessPolicyType",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancerPolicyTypesResponse"`
				Result  struct {
					PolicyTypeDescriptions struct {
						Members []struct {
							PolicyTypeName string `xml:"PolicyTypeName"`
						} `xml:"member"`
					} `xml:"PolicyTypeDescriptions"`
				} `xml:"DescribeLoadBalancerPolicyTypesResult"`
			}
			parseXMLBody(t, rec, &resp)
			assert.Len(t, resp.Result.PolicyTypeDescriptions.Members, tt.wantTypeLen)

			if tt.wantTypeName != "" {
				require.NotEmpty(t, resp.Result.PolicyTypeDescriptions.Members)
				assert.Equal(t, tt.wantTypeName, resp.Result.PolicyTypeDescriptions.Members[0].PolicyTypeName)
			}
		})
	}
}
