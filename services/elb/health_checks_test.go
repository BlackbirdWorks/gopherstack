package elb_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

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

func TestHealthCheckConfigure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target     string
		name       string
		wantStatus int
	}{
		{name: "http_with_path", target: "HTTP:80/health", wantStatus: http.StatusOK},
		{name: "https_with_path", target: "HTTPS:443/ping", wantStatus: http.StatusOK},
		{name: "tcp_no_path", target: "TCP:80", wantStatus: http.StatusOK},
		{name: "ssl_no_path", target: "SSL:443", wantStatus: http.StatusOK},
		{name: "http_without_path_rejected", target: "HTTP:80", wantStatus: http.StatusBadRequest},
		{name: "tcp_with_path_rejected", target: "TCP:80/health", wantStatus: http.StatusBadRequest},
		{name: "invalid_protocol_rejected", target: "UDP:80", wantStatus: http.StatusBadRequest},
		{name: "empty_target_rejected", target: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "hc-lb")

			rec := doELB(t, h, url.Values{
				"Action":                         {"ConfigureHealthCheck"},
				"Version":                        {"2012-06-01"},
				"LoadBalancerName":               {"hc-lb"},
				"HealthCheck.Target":             {tt.target},
				"HealthCheck.Interval":           {"30"},
				"HealthCheck.Timeout":            {"5"},
				"HealthCheck.UnhealthyThreshold": {"2"},
				"HealthCheck.HealthyThreshold":   {"3"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHealthCheckTargetProtocolNormalized(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "hc-norm-lb")

	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-norm-lb"},
		"HealthCheck.Target":             {"http:80/health"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})

	lbs, err := b.DescribeLoadBalancers(context.Background(), []string{"hc-norm-lb"})
	require.NoError(t, err)
	require.NotNil(t, lbs[0].HealthCheck)
	assert.Equal(t, "HTTP:80/health", lbs[0].HealthCheck.Target, "protocol must be uppercased")
}

func TestHealthCheckIntervalMustExceedTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-timing-lb")

	rec := doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-timing-lb"},
		"HealthCheck.Target":             {"TCP:80"},
		"HealthCheck.Interval":           {"10"},
		"HealthCheck.Timeout":            {"10"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHealthCheckAlwaysPresentInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-absent-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"hc-absent-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

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
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	// HealthCheck element must be present even without configuration.
	assert.NotNil(t, resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck)
}
