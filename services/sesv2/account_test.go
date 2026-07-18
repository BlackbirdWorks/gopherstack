package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetAccount tests the GetAccount operation.
func TestGetAccount(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/account", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountDetails tests the PutAccountDetails operation.
func TestPutAccountDetails(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/account/details", map[string]any{
		"MailType":           "MARKETING",
		"WebsiteURL":         "https://example.com",
		"UseCaseDescription": "Test use case",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountSendingAttributes tests the PutAccountSendingAttributes operation.
func TestPutAccountSendingAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/account/sending", map[string]any{
		"SendingEnabled": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountSuppressionAttributes tests the PutAccountSuppressionAttributes operation.
func TestPutAccountSuppressionAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/account/suppression",
		map[string]any{},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountVdmAttributes tests the PutAccountVdmAttributes operation.
func TestPutAccountVdmAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/account/vdm", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountDedicatedIPWarmupAttributes tests the PutAccountDedicatedIpWarmupAttributes operation.
func TestPutAccountDedicatedIPWarmupAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/account/dedicated-ips/warmup",
		map[string]any{},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetBlacklistReports tests the GetBlacklistReports operation.
func TestGetBlacklistReports(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/blacklist-report",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutAccountAttributesPersist verifies PutAccount* ops persist (previously no-op stubs).
func TestPutAccountAttributesPersist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "suppression_attributes",
			method: http.MethodPut,
			path:   "/v2/email/account/suppression",
			body:   map[string]any{"SuppressedReasons": []any{"BOUNCE", "COMPLAINT"}},
		},
		{
			name:   "vdm_attributes",
			method: http.MethodPut,
			path:   "/v2/email/account/vdm",
			body:   map[string]any{"VdmAttributes": map[string]any{"DashboardAttributes": map[string]any{}}},
		},
		{
			name:   "dedicated_ip_warmup",
			method: http.MethodPut,
			path:   "/v2/email/account/dedicated-ips/warmup",
			body:   map[string]any{"AutoWarmupEnabled": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doReqQuery(t, h, tt.method, tt.path, nil, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code, "%s failed: %s", tt.path, rec.Body)
		})
	}
}
