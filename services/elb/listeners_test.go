package elb_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

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
			wantStatus: http.StatusBadRequest,
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
			wantStatus: http.StatusBadRequest,
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

func TestDuplicateListenerCreateListeners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			// Same port + same settings = idempotent (AWS behavior: CreateLoadBalancerListeners
			// is a no-op when listener already exists with identical config).
			name: "same_port_same_config_idempotent",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-idem-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-idem-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			},
			wantStatus: http.StatusOK,
		},
		{
			// Same port + DIFFERENT instance port = DuplicateListener error.
			name: "same_port_different_config_conflict",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-conflict-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-conflict-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"9090"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "DuplicateListener",
		},
		{
			name: "different_port_accepted",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-list-ok")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-list-ok"},
				"Listeners.member.1.Protocol":         {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"8443"},
				"Listeners.member.1.SSLCertificateId": {"arn:aws:iam::123456789012:server-certificate/my-cert"},
			},
			wantStatus: http.StatusOK,
		},
		{
			// Two listeners on the same port in a single request = DuplicateListener.
			name: "duplicate_within_same_request",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-list-req")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-list-req"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"8080"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"Listeners.member.2.Protocol":         {"TCP"},
				"Listeners.member.2.LoadBalancerPort": {"8080"},
				"Listeners.member.2.InstancePort":     {"8888"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "DuplicateListener",
		},
		{
			// A malformed SSLCertificateId on the *initial* listener creation
			// must be rejected the same way SetLoadBalancerListenerSSLCertificate
			// rejects one later -- both paths share validateCertificateID.
			name: "malformed_cert_arn_rejected",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "badcert-list-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"badcert-list-lb"},
				"Listeners.member.1.Protocol":         {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"8443"},
				"Listeners.member.1.SSLCertificateId": {"not-a-valid-arn"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
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

			if tt.wantCode != "" {
				var errResp struct {
					XMLName xml.Name `xml:"ErrorResponse"`
					Error   struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			}
		})
	}
}

// TestCreateLoadBalancerRejectsMalformedInlineCertARN verifies that
// CreateLoadBalancer's inline Listeners.member.N.SSLCertificateId is
// validated with the same ARN-format check as
// SetLoadBalancerListenerSSLCertificate / CreateLoadBalancerListeners,
// instead of being accepted unchecked at LB-creation time.
func TestCreateLoadBalancerRejectsMalformedInlineCertARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"badcert-create-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTPS"},
		"Listeners.member.1.LoadBalancerPort": {"443"},
		"Listeners.member.1.InstancePort":     {"8443"},
		"Listeners.member.1.SSLCertificateId": {"not-a-valid-arn"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationError")
}

func TestListenerProtocols(t *testing.T) {
	t.Parallel()

	const testCertARN = "arn:aws:iam::123456789012:server-certificate/my-cert"

	protocols := []struct {
		name     string
		protocol string
		certARN  string
		port     string
		valid    bool
	}{
		{"http_valid", "HTTP", "", "80", true},
		// HTTPS/SSL require a certificate ARN.
		{"https_valid", "HTTPS", testCertARN, "443", true},
		{"tcp_valid", "TCP", "", "80", true},
		{"ssl_valid", "SSL", testCertARN, "443", true},
		{"udp_invalid", "UDP", "", "80", false},
		{"grpc_invalid", "GRPC", "", "80", false},
	}

	for _, tt := range protocols {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"proto-lb"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
			}
			if tt.certARN != "" {
				vals.Set("Listeners.member.1.SSLCertificateId", tt.certARN)
			}

			rec := doELB(t, h, vals)

			if tt.valid {
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestListenerDeleteAndRecreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "list-del-lb")

	// Add second listener.
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"list-del-lb"},
		"Listeners.member.1.Protocol":         {"HTTPS"},
		"Listeners.member.1.LoadBalancerPort": {"443"},
		"Listeners.member.1.InstancePort":     {"8443"},
	})

	// Delete port 443 listener.
	doELB(t, h, url.Values{
		"Action":                     {"DeleteLoadBalancerListeners"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"list-del-lb"},
		"LoadBalancerPorts.member.1": {"443"},
	})

	// Recreate port 443 — should succeed (no duplicate).
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"list-del-lb"},
		"Listeners.member.1.Protocol":         {"TCP"},
		"Listeners.member.1.LoadBalancerPort": {"443"},
		"Listeners.member.1.InstancePort":     {"443"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestListenerPortBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		// AWS allows: 25, 80, 443, 465, 587, and 1024-65535.
		{name: "port_25_ok", port: "25", wantStatus: http.StatusOK},
		{name: "port_80_ok", port: "80", wantStatus: http.StatusOK},
		{name: "port_1024_ok", port: "1024", wantStatus: http.StatusOK},
		{name: "port_65535_ok", port: "65535", wantStatus: http.StatusOK},
		{name: "port_0_rejected", port: "0", wantStatus: http.StatusBadRequest},
		{name: "port_1_rejected", port: "1", wantStatus: http.StatusBadRequest},
		{name: "port_65536_rejected", port: "65536", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"portb-lb"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestSetSSLCertNonHTTPSListener verifies that setting an SSL cert on
// an HTTP or TCP listener returns InvalidConfigurationRequest.
func TestSetSSLCertNonHTTPSListener(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:acm:us-east-1:123456789012:certificate/abc-123"

	tests := []struct {
		name       string
		protocol   string
		port       string
		wantStatus int
	}{
		{"http_listener_rejected", "HTTP", "80", http.StatusBadRequest},
		{"tcp_listener_rejected", "TCP", "80", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("ssl-proto-%d", i)

			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {lbName},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doELB(t, h, url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"LoadBalancerPort": {tt.port},
				"SSLCertificateId": {certARN},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCertARNValidation verifies that SetLoadBalancerListenerSSLCertificate
// accepts valid ACM/IAM ARNs and rejects invalid ones.
func TestCertARNValidation(t *testing.T) {
	t.Parallel()

	const httpsPort = "443"
	const certARN = "arn:aws:iam::123456789012:server-certificate/initial"

	tests := []struct {
		certID     string
		name       string
		wantStatus int
	}{
		{"arn:aws:acm:us-east-1:123456789012:certificate/abc-123", "acm_arn_valid", http.StatusOK},
		{"arn:aws:iam::123456789012:server-certificate/my-cert", "iam_arn_valid", http.StatusOK},
		{"not-an-arn", "bare_string_rejected", http.StatusBadRequest},
		{"arn:aws:s3:::my-bucket", "s3_arn_rejected", http.StatusBadRequest},
		{"", "empty_rejected", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("certval-%d", i)

			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {lbName},
				"Listeners.member.1.Protocol":         {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort": {httpsPort},
				"Listeners.member.1.InstancePort":     {"8443"},
				"Listeners.member.1.SSLCertificateId": {certARN},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			vals := url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"LoadBalancerPort": {httpsPort},
			}
			if tt.certID != "" {
				vals.Set("SSLCertificateId", tt.certID)
			}

			rec = doELB(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAccountLimitMaxListeners verifies that adding more than 100
// listeners to a single LB returns an InvalidConfigurationRequest error.
func TestAccountLimitMaxListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// CreateLoadBalancer with 1 listener on port 80.
	mustCreateLB(t, h, "maxlist-lb")

	// Add 99 more listeners on ports 1024..1122 to reach the limit of 100.
	for port := 1024; port < 1024+99; port++ {
		rec := doELB(t, h, url.Values{
			"Action":                              {"CreateLoadBalancerListeners"},
			"Version":                             {"2012-06-01"},
			"LoadBalancerName":                    {"maxlist-lb"},
			"Listeners.member.1.Protocol":         {"HTTP"},
			"Listeners.member.1.LoadBalancerPort": {strconv.Itoa(port)},
			"Listeners.member.1.InstancePort":     {"8080"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "listener on port %d should succeed", port)
	}

	// Adding the 101st listener must fail.
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"maxlist-lb"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"2048"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidConfigurationRequest")
}

// TestProtocolPairing verifies that only valid frontend/backend protocol
// pairings are accepted.
func TestProtocolPairing(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:iam::123456789012:server-certificate/my-cert"

	tests := []struct {
		instanceProtocol string
		protocol         string
		certARN          string
		name             string
		port             string
		wantStatus       int
	}{
		// Valid pairings.
		{"HTTP", "HTTP", "", "http_http", "80", http.StatusOK},
		{"HTTPS", "HTTP", "", "http_https", "80", http.StatusOK},
		{"HTTP", "HTTPS", certARN, "https_http", "443", http.StatusOK},
		{"HTTPS", "HTTPS", certARN, "https_https", "443", http.StatusOK},
		{"TCP", "TCP", "", "tcp_tcp", "80", http.StatusOK},
		{"TCP", "SSL", certARN, "ssl_tcp", "443", http.StatusOK},
		{"SSL", "SSL", certARN, "ssl_ssl", "443", http.StatusOK},
		// Invalid pairings.
		{"TCP", "HTTP", "", "http_tcp_invalid", "80", http.StatusBadRequest},
		{"SSL", "HTTP", "", "http_ssl_invalid", "80", http.StatusBadRequest},
		{"HTTP", "TCP", "", "tcp_http_invalid", "80", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"pair-lb"},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.InstanceProtocol": {tt.instanceProtocol},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			}
			if tt.certARN != "" {
				vals.Set("Listeners.member.1.SSLCertificateId", tt.certARN)
			}

			rec := doELB(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestSetLoadBalancerListenerSSLCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantCertID string
		wantStatus int
	}{
		{
			name: "sets_ssl_certificate",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				rec := doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssl-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTPS"},
					"Listeners.member.1.LoadBalancerPort": {"443"},
					"Listeners.member.1.InstancePort":     {"8443"},
					"Listeners.member.1.SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/initial"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusOK,
			wantCertID: "arn:aws:acm:us-east-1:123456789012:certificate/abc-123",
		},
		{
			name: "missing_lb_name_returns_400",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_port_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-missing-port")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-missing-port"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_cert_id_returns_400",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-missing-cert")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-missing-cert"},
				"LoadBalancerPort": {"80"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "port_not_found_returns_404",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "ssl-bad-port")
			},
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-bad-port"},
				"LoadBalancerPort": {"9999"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "lb_not_found_returns_404",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc-123"},
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

			if tt.wantStatus == http.StatusOK && tt.wantCertID != "" {
				lbName := tt.vals.Get("LoadBalancerName")
				lbs, err := h.Backend.DescribeLoadBalancers(context.Background(), []string{lbName})
				require.NoError(t, err)
				require.Len(t, lbs, 1)

				found := false
				for _, l := range lbs[0].Listeners {
					if l.SSLCertificateID == tt.wantCertID {
						found = true
					}
				}

				assert.True(t, found, "SSL certificate ID not set on listener")
			}
		})
	}
}

// TestCreateListenersRejectsEmptyList verifies that CreateLoadBalancerListeners
// rejects an empty listener list.
func TestCreateListenersRejectsEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "empty-listeners-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerListeners"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"empty-listeners-lb"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestListenerNotFoundReturns400 verifies ErrListenerNotFound maps to HTTP 400.
func TestListenerNotFoundReturns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantCode string
	}{
		{
			name: "ssl_cert_no_listener",
			vals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"lnf-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:iam::123456789012:server-certificate/my-cert"},
			},
			wantCode: "ListenerNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "lnf-lb")

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				XMLName xml.Name `xml:"ErrorResponse"`
				Error   struct {
					Code string `xml:"Code"`
				} `xml:"Error"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// TestSSLListenerCertificateWireShape verifies that SSL protocol (not just HTTPS)
// is handled in the DescribeLoadBalancers and SetLoadBalancerListenerSSLCertificate flow.
func TestSSLListenerCertificateWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		checkVals  url.Values
		name       string
		wantCert   string
		wantStatus int
	}{
		{
			name: "ssl_listener_cert_can_be_set",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssl-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"SSL"},
					"Listeners.member.1.LoadBalancerPort": {"443"},
					"Listeners.member.1.InstancePort":     {"443"},
					"Listeners.member.1.SSLCertificateId": {"arn:aws:iam::123456789012:server-certificate/initial"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			checkVals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc123"},
			},
			wantStatus: http.StatusOK,
			wantCert:   "arn:aws:acm:us-east-1:123456789012:certificate/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			got := doELB(t, h, tt.checkVals)
			assert.Equal(t, tt.wantStatus, got.Code)

			if tt.wantCert != "" {
				// Verify cert is reflected in describe.
				descResp := doELB(t, h, url.Values{
					"Action":                     {"DescribeLoadBalancers"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"ssl-lb"},
				})
				require.Equal(t, http.StatusOK, descResp.Code)

				var out struct {
					XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
					Result  struct {
						LBs struct {
							Members []struct {
								ListenerDescriptions struct {
									Members []struct {
										Listener struct {
											SSLCertificateID string `xml:"SSLCertificateId"`
											LoadBalancerPort int32  `xml:"LoadBalancerPort"`
										} `xml:"Listener"`
									} `xml:"member"`
								} `xml:"ListenerDescriptions"`
							} `xml:"member"`
						} `xml:"LoadBalancerDescriptions"`
					} `xml:"DescribeLoadBalancersResult"`
				}

				require.NoError(t, xml.Unmarshal(descResp.Body.Bytes(), &out))
				require.Len(t, out.Result.LBs.Members, 1)
				require.Len(t, out.Result.LBs.Members[0].ListenerDescriptions.Members, 1)
				assert.Equal(
					t,
					tt.wantCert,
					out.Result.LBs.Members[0].ListenerDescriptions.Members[0].Listener.SSLCertificateID,
				)
			}
		})
	}
}
