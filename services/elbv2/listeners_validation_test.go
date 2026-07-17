package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDuplicateListenerPortRejected verifies duplicate listener port returns conflict.
func TestDuplicateListenerPortRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-port-lb")
	tgArn := mustCreateTG(t, h, "dup-port-tg")
	mustCreateListener(t, h, lbArn, tgArn)

	// Attempt to create another listener on the same port.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestPortValidationCreateListener tests port validation for CreateListener.
func TestPortValidationCreateListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		{"valid_port_80", "80", http.StatusBadRequest}, // will fail on nonexistent LB, not port
		{"port_zero", "0", http.StatusBadRequest},
		{"port_negative", "-1", http.StatusBadRequest},
		{"port_65536", "65536", http.StatusBadRequest},
		{"port_65535_valid", "65535", http.StatusBadRequest},
		{"port_1_valid", "1", http.StatusBadRequest},
		{"port_non_numeric", "abc", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateListener"},
				"Version": {"2015-12-01"},
				"LoadBalancerArn": {
					"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no/0",
				},
				"Protocol":                     {"HTTP"},
				"Port":                         {tt.port},
				"DefaultActions.member.1.Type": {"fixed-response"},
				"DefaultActions.member.1.FixedResponseConfig.StatusCode": {"200"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "port=%s", tt.port)
		})
	}
}

// TestCreateListenerNoDefaultActions tests that missing DefaultActions returns 400.
func TestCreateListenerNoDefaultActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "no-actions-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateListener"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"Protocol":        {"HTTP"},
		"Port":            {"80"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestModifyListenerDuplicatePortRejected verifies that ModifyListener rejects a port already in use.
func TestModifyListenerDuplicatePortRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dup-port-lb")
	tgArn := mustCreateTG(t, h, "dup-port-tg")

	// Create two listeners on ports 80 and 443.
	listenerArn1 := mustCreateListener(t, h, lbArn, tgArn)

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/dup-cert"
	rec443 := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {certArn},
	})
	require.Equal(t, http.StatusOK, rec443.Code)

	// Attempt to modify listener on port 80 to use port 443 (already taken).
	rec := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn1},
		"Port":        {"443"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestInvalidActionTypeRejected verifies that unknown action types are rejected.
func TestInvalidActionTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "bad-action-lb")
	tgArn := mustCreateTG(t, h, "bad-action-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"unknown-action-type"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeListenersNotFound verifies that querying non-existent listener ARNs returns an error.
func TestDescribeListenersNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	fakeArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/fake/0000000000000000/00000001"

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeListeners"},
		"Version":               {"2015-12-01"},
		"ListenerArns.member.1": {fakeArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateListener_DuplicatePortRejected(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "dup-port-lb")
	tgArn := b1CreateTG(t, h, "dup-port-tg")

	b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateListener_InvalidPort(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "invalid-port-lb")
	tgArn := b1CreateTG(t, h, "invalid-port-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"99999"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeListeners_UnknownLB verifies that describing listeners for
// a nonexistent LB ARN returns LoadBalancerNotFound.
func TestDescribeListeners_UnknownLB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lbArn       string
		wantErrCode string
		wantCode    int
	}{
		{
			name:        "unknown_lb_arn_returns_not_found",
			lbArn:       "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nonexistent/abcdef123456",
			wantCode:    http.StatusBadRequest,
			wantErrCode: "LoadBalancerNotFound",
		},
		{
			name:     "empty_lb_arn_returns_all",
			lbArn:    "",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			vals := url.Values{
				"Action":  {"DescribeListeners"},
				"Version": {"2015-12-01"},
			}

			if tc.lbArn != "" {
				vals.Set("LoadBalancerArn", tc.lbArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantErrCode != "" {
				var errResp struct {
					Error struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tc.wantErrCode, errResp.Error.Code)
			}
		})
	}
}
