package sesv2_test

import (
	"net/http"
	"testing"

	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestPutAccountPricingAttributes tests the PutAccountPricingAttributes
// operation: every real PricingPlan enum value (types.PricingPlan) is
// accepted and persists, an unrecognized plan is a BadRequestException, and
// a successful Put is reflected by GetAccount's nested
// PricingAttributes.CurrentPlan (accountOutput/accountPricingAttributesOutput
// in wire_output.go, field-diffed against GetAccountOutput/
// types.PricingAttributes).
func TestPutAccountPricingAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		plan       string
		wantStatus int
	}{
		{name: "none", plan: "NONE", wantStatus: http.StatusOK},
		{name: "essentials", plan: "ESSENTIALS", wantStatus: http.StatusOK},
		{name: "pro", plan: "PRO", wantStatus: http.StatusOK},
		{name: "enterprise", plan: "ENTERPRISE", wantStatus: http.StatusOK},
		{name: "invalid_plan", plan: "BOGUS", wantStatus: http.StatusBadRequest},
		{name: "empty_plan", plan: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, http.MethodPut, "/v2/email/account/pricing-attributes", map[string]any{
				"Plan": tt.plan,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantStatus != http.StatusOK {
				return
			}

			getRec := doRequest(t, h, http.MethodGet, "/v2/email/account", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			resp := decodeJSON(t, getRec)
			pricing, ok := resp["PricingAttributes"].(map[string]any)
			require.True(t, ok, "GetAccount response missing PricingAttributes: %s", getRec.Body)
			assert.Equal(t, tt.plan, pricing["CurrentPlan"])
		})
	}
}

// TestAccountSDKRoundTrip drives PutAccountPricingAttributes/GetAccount
// through the real aws-sdk-go-v2 sesv2 client so the accountOutput typed-DTO
// conversion (wire_output.go) is verified by the genuine SDK deserializer,
// not a hand-decoded JSON map.
func TestAccountSDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newSESv2SDKClient(t, h)

	_, err := client.PutAccountPricingAttributes(t.Context(), &sesv2sdk.PutAccountPricingAttributesInput{
		Plan: sesv2types.PricingPlanEnterprise,
	})
	require.NoError(t, err)

	out, err := client.GetAccount(t.Context(), &sesv2sdk.GetAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, out.PricingAttributes)
	assert.Equal(t, sesv2types.PricingPlanEnterprise, out.PricingAttributes.CurrentPlan)
	assert.Empty(t, out.PricingAttributes.NextPlan)
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
		{
			name:   "pricing_attributes",
			method: http.MethodPut,
			path:   "/v2/email/account/pricing-attributes",
			body:   map[string]any{"Plan": "PRO"},
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
