package sesv2_test

// handler_accuracy_test.go verifies AWS accuracy improvements from issue #1699:
// - PutConfigurationSet* handlers read bodies and persist per-config-set options
// - PutEmailIdentity* handlers read bodies and persist per-identity attributes
// - GetConfigurationSet returns full option data
// - GetEmailIdentity returns full identity data (DKIM, MailFrom, FeedbackForwarding)
// - CreateEmailIdentity generates DKIM tokens for domain identities

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// ─── helpers ────────────────────────────────────────────────────────────────

// newSESv2Backend returns a fresh InMemoryBackend (convenience alias for accuracy tests).
func newSESv2Backend() *sesv2.InMemoryBackend {
	return sesv2.NewInMemoryBackend()
}

// ─── configuration set accuracy tests ───────────────────────────────────────

// TestAccuracy_ConfigSetTrackingOptions verifies round-trip for tracking options.
func TestAccuracy_ConfigSetTrackingOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("track-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/track-cs/tracking-options",
		map[string]any{
			"CustomRedirectDomain": "click.example.com",
			"HttpsPolicy":          "REQUIRE",
		})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/track-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tracking, ok := out["TrackingOptions"].(map[string]any)
	require.True(t, ok, "TrackingOptions should be present in GetConfigurationSet response")
	assert.Equal(t, "click.example.com", tracking["CustomRedirectDomain"])
	assert.Equal(t, "REQUIRE", tracking["HttpsPolicy"])
}

// TestAccuracy_ConfigSetTrackingOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetTrackingOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/tracking-options",
		map[string]any{"CustomRedirectDomain": "x.com"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetDeliveryOptions verifies round-trip for delivery options.
func TestAccuracy_ConfigSetDeliveryOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("delivery-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/delivery-cs/delivery-options",
		map[string]any{
			"TlsPolicy":       "REQUIRE",
			"SendingPoolName": "my-pool",
		})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/delivery-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	delivery, ok := out["DeliveryOptions"].(map[string]any)
	require.True(t, ok, "DeliveryOptions should be present")
	assert.Equal(t, "REQUIRE", delivery["TlsPolicy"])
	assert.Equal(t, "my-pool", delivery["SendingPoolName"])
}

// TestAccuracy_ConfigSetDeliveryOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetDeliveryOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/delivery-options",
		map[string]any{"TlsPolicy": "REQUIRE"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetSendingOptions verifies round-trip for sending options.
func TestAccuracy_ConfigSetSendingOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("send-cs")
	require.NoError(t, err)

	// Disable sending.
	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/send-cs/sending",
		map[string]any{"SendingEnabled": false})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/send-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	sending, ok := out["SendingOptions"].(map[string]any)
	require.True(t, ok, "SendingOptions should be present")
	assert.Equal(t, false, sending["SendingEnabled"])
}

// TestAccuracy_ConfigSetSendingOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetSendingOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/sending",
		map[string]any{"SendingEnabled": true})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetReputationOptions verifies round-trip for reputation options.
func TestAccuracy_ConfigSetReputationOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("rep-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/rep-cs/reputation-options",
		map[string]any{"ReputationMetricsEnabled": true})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/rep-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	rep, ok := out["ReputationOptions"].(map[string]any)
	require.True(t, ok, "ReputationOptions should be present")
	assert.Equal(t, true, rep["ReputationMetricsEnabled"])
}

// TestAccuracy_ConfigSetReputationOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetReputationOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/reputation-options",
		map[string]any{"ReputationMetricsEnabled": true})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetSuppressionOptions verifies round-trip for suppression options.
func TestAccuracy_ConfigSetSuppressionOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("supp-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/supp-cs/suppression-options",
		map[string]any{"SuppressedReasons": []string{"BOUNCE", "COMPLAINT"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/supp-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	supp, ok := out["SuppressionOptions"].(map[string]any)
	require.True(t, ok, "SuppressionOptions should be present")

	reasons, ok := supp["SuppressedReasons"].([]any)
	require.True(t, ok, "SuppressedReasons should be a list")
	assert.Len(t, reasons, 2)
}

// TestAccuracy_ConfigSetSuppressionOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetSuppressionOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/suppression-options",
		map[string]any{"SuppressedReasons": []string{"BOUNCE"}})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetVdmOptions validates config set exists.
func TestAccuracy_ConfigSetVdmOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("vdm-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/vdm-cs/vdm-options",
		map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAccuracy_ConfigSetVdmOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetVdmOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/vdm-options",
		map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_ConfigSetArchivingOptionsNotFound returns 404 for missing config set.
func TestAccuracy_ConfigSetArchivingOptionsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/configuration-sets/no-such-cs/archiving-options",
		map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_GetConfigurationSetDefaultSendingEnabled verifies SendingEnabled defaults to true.
func TestAccuracy_GetConfigurationSetDefaultSendingEnabled(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("default-cs")
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/default-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	sending, ok := out["SendingOptions"].(map[string]any)
	require.True(t, ok, "SendingOptions always present in GetConfigurationSet")
	assert.Equal(t, true, sending["SendingEnabled"], "SendingEnabled defaults to true")
}

// TestAccuracy_ConfigSetMultipleOptions verifies all options can be set and are returned together.
func TestAccuracy_ConfigSetMultipleOptions(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateConfigurationSet("multi-cs")
	require.NoError(t, err)

	doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/multi-cs/tracking-options",
		map[string]any{"CustomRedirectDomain": "track.example.com"})
	doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/multi-cs/delivery-options",
		map[string]any{"TlsPolicy": "OPTIONAL"})
	doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/multi-cs/reputation-options",
		map[string]any{"ReputationMetricsEnabled": true})
	doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/multi-cs/sending",
		map[string]any{"SendingEnabled": false})
	doReq(t, h, http.MethodPut, "/v2/email/configuration-sets/multi-cs/suppression-options",
		map[string]any{"SuppressedReasons": []string{"BOUNCE"}})

	rec := doReq(t, h, http.MethodGet, "/v2/email/configuration-sets/multi-cs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "multi-cs", out["ConfigurationSetName"])
	assert.NotNil(t, out["TrackingOptions"])
	assert.NotNil(t, out["DeliveryOptions"])
	assert.NotNil(t, out["ReputationOptions"])
	assert.NotNil(t, out["SendingOptions"])
	assert.NotNil(t, out["SuppressionOptions"])
}

// ─── email identity accuracy tests ──────────────────────────────────────────

// TestAccuracy_PutEmailIdentityConfigurationSetAttributes associates a config set with an identity.
func TestAccuracy_PutEmailIdentityConfigurationSetAttributes(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("cs-attr@example.com", "", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/cs-attr%40example.com/configuration-set",
		map[string]any{"ConfigurationSetName": "my-cs"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/cs-attr%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "my-cs", out["ConfigurationSetName"])
}

// TestAccuracy_PutEmailIdentityConfigurationSetAttributesNotFound returns 404 for missing identity.
func TestAccuracy_PutEmailIdentityConfigurationSetAttributesNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/no-such%40example.com/configuration-set",
		map[string]any{"ConfigurationSetName": "my-cs"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_PutEmailIdentityDkimAttributes toggles DKIM signing.
func TestAccuracy_PutEmailIdentityDkimAttributes(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("dkim@example.com", "", nil)
	require.NoError(t, err)

	// Disable DKIM signing.
	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/dkim%40example.com/dkim",
		map[string]any{"SigningEnabled": false})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/dkim%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	dkim, ok := out["DkimAttributes"].(map[string]any)
	require.True(t, ok, "DkimAttributes should be present")
	assert.Equal(t, false, dkim["SigningEnabled"])
}

// TestAccuracy_PutEmailIdentityDkimAttributesNotFound returns 404 for missing identity.
func TestAccuracy_PutEmailIdentityDkimAttributesNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/no-such%40example.com/dkim",
		map[string]any{"SigningEnabled": true})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_PutEmailIdentityDkimSigningAttributesNotFound returns 404 for missing identity.
func TestAccuracy_PutEmailIdentityDkimSigningAttributesNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/no-such%40example.com/dkim/signing",
		map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_PutEmailIdentityFeedbackAttributes toggles feedback forwarding.
func TestAccuracy_PutEmailIdentityFeedbackAttributes(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("feedback@example.com", "", nil)
	require.NoError(t, err)

	// Disable feedback forwarding.
	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/feedback%40example.com/feedback",
		map[string]any{"EmailForwardingEnabled": false})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/feedback%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, false, out["FeedbackForwardingStatus"])
}

// TestAccuracy_PutEmailIdentityFeedbackAttributesNotFound returns 404 for missing identity.
func TestAccuracy_PutEmailIdentityFeedbackAttributesNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/no-such%40example.com/feedback",
		map[string]any{"EmailForwardingEnabled": false})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_PutEmailIdentityMailFromAttributes stores mail-from domain.
func TestAccuracy_PutEmailIdentityMailFromAttributes(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("mailfrom@example.com", "", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/mailfrom%40example.com/mail-from",
		map[string]any{
			"MailFromDomain":      "bounce.example.com",
			"BehaviorOnMxFailure": "REJECT_MESSAGE",
		})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/mailfrom%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	mailFrom, ok := out["MailFromAttributes"].(map[string]any)
	require.True(t, ok, "MailFromAttributes should be present after setting mail-from domain")
	assert.Equal(t, "bounce.example.com", mailFrom["MailFromDomain"])
	assert.Equal(t, "REJECT_MESSAGE", mailFrom["BehaviorOnMxFailure"])
	assert.Equal(t, "PENDING", mailFrom["MailFromDomainStatus"])
}

// TestAccuracy_PutEmailIdentityMailFromAttributesNotFound returns 404 for missing identity.
func TestAccuracy_PutEmailIdentityMailFromAttributesNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPut,
		"/v2/email/identities/no-such%40example.com/mail-from",
		map[string]any{"MailFromDomain": "bounce.example.com"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAccuracy_PutEmailIdentityMailFromClear clears mail-from domain when empty string sent.
func TestAccuracy_PutEmailIdentityMailFromClear(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("mfclear@example.com", "", nil)
	require.NoError(t, err)

	// Set a domain first.
	doReq(t, h, http.MethodPut,
		"/v2/email/identities/mfclear%40example.com/mail-from",
		map[string]any{"MailFromDomain": "bounce.example.com"})

	// Clear by sending empty string.
	doReq(t, h, http.MethodPut,
		"/v2/email/identities/mfclear%40example.com/mail-from",
		map[string]any{"MailFromDomain": ""})

	rec := doReq(t, h, http.MethodGet, "/v2/email/identities/mfclear%40example.com", nil)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// MailFromAttributes should be absent when domain is empty.
	assert.Nil(t, out["MailFromAttributes"])
}

// ─── GetEmailIdentity full-data tests ───────────────────────────────────────

// TestAccuracy_GetEmailIdentityDefaults verifies default values on a fresh identity.
func TestAccuracy_GetEmailIdentityDefaults(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("defaults@example.com", "", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodGet, "/v2/email/identities/defaults%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "defaults@example.com", out["EmailIdentity"])
	assert.Equal(t, "EMAIL_ADDRESS", out["IdentityType"])
	assert.Equal(t, true, out["VerifiedForSendingStatus"])
	assert.Equal(t, true, out["FeedbackForwardingStatus"])
	assert.Equal(t, "SUCCESS", out["VerificationStatus"])

	dkim, ok := out["DkimAttributes"].(map[string]any)
	require.True(t, ok, "DkimAttributes should be present for email address identity")
	assert.Equal(t, true, dkim["SigningEnabled"])
	assert.Equal(t, "SUCCESS", dkim["Status"])
}

// TestAccuracy_GetEmailIdentityDomainHasDkimTokens verifies domain identities have DKIM tokens.
func TestAccuracy_GetEmailIdentityDomainHasDkimTokens(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("example.com", "", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodGet, "/v2/email/identities/example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "DOMAIN", out["IdentityType"])

	dkim, ok := out["DkimAttributes"].(map[string]any)
	require.True(t, ok)

	tokens, ok := dkim["Tokens"].([]any)
	require.True(t, ok, "Domain identity should have DKIM tokens")
	assert.Len(t, tokens, 3, "Should have exactly 3 DKIM tokens")

	for _, tok := range tokens {
		s, isStr := tok.(string)
		require.True(t, isStr)
		assert.Len(t, s, 24, "Each DKIM token should be 24 chars")
	}
}

// TestAccuracy_GetEmailIdentityEmailAddressNoDkimTokens verifies email addresses have no DKIM tokens.
func TestAccuracy_GetEmailIdentityEmailAddressNoDkimTokens(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("addr@example.com", "", nil)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodGet, "/v2/email/identities/addr%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	dkim, ok := out["DkimAttributes"].(map[string]any)
	require.True(t, ok)
	// Email address identities should have no tokens.
	assert.Nil(t, dkim["Tokens"])
}

// TestAccuracy_CreateEmailIdentityWithConfigSet stores config set name.
func TestAccuracy_CreateEmailIdentityWithConfigSet(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity":        "tagged@example.com",
		"ConfigurationSetName": "my-cs",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/tagged%40example.com", nil)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "my-cs", out["ConfigurationSetName"])
}

// TestAccuracy_CreateEmailIdentityWithTags stores tags accessible via GetEmailIdentity.
func TestAccuracy_CreateEmailIdentityWithTags(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "tagged2@example.com",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "email"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doReq(t, h, http.MethodGet, "/v2/email/identities/tagged2%40example.com", nil)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags, ok := out["Tags"].([]any)
	require.True(t, ok, "Tags should be returned")
	assert.Len(t, tags, 2)
}

// TestAccuracy_CreateDomainIdentityReturnsDkimAttributes verifies DkimAttributes in create response.
func TestAccuracy_CreateDomainIdentityReturnsDkimAttributes(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "domain-create.com",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Domain identities should return DkimAttributes with tokens in create response.
	dkim, ok := out["DkimAttributes"].(map[string]any)
	require.True(t, ok, "DkimAttributes should be returned for domain in CreateEmailIdentity")
	tokens, ok := dkim["Tokens"].([]any)
	require.True(t, ok)
	assert.Len(t, tokens, 3)
	assert.Equal(t, "AWS_SES", dkim["SigningAttributesOrigin"])
}

// TestAccuracy_CreateEmailAddressDoesNotReturnDkimAttributes verifies no DkimAttributes for email.
func TestAccuracy_CreateEmailAddressDoesNotReturnDkimAttributes(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	rec := doReq(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "addr-create@example.com",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Email addresses should not return DkimAttributes in create response.
	assert.Nil(t, out["DkimAttributes"], "Email address CreateEmailIdentity should omit DkimAttributes")
}

// TestAccuracy_GetEmailIdentityPoliciesIncludedInGet verifies policies are returned inline.
func TestAccuracy_GetEmailIdentityPoliciesIncludedInGet(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)

	_, err := backend.CreateEmailIdentity("policy-get@example.com", "", nil)
	require.NoError(t, err)

	err = backend.CreateEmailIdentityPolicy("policy-get@example.com", "my-policy", `{"Version":"2012-10-17"}`)
	require.NoError(t, err)

	rec := doReq(t, h, http.MethodGet, "/v2/email/identities/policy-get%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	policies, ok := out["Policies"].(map[string]any)
	require.True(t, ok, "Policies should be returned in GetEmailIdentity")
	assert.Contains(t, policies, "my-policy")
}

// ─── backend unit tests for accuracy methods ────────────────────────────────

// TestAccuracy_BackendPutConfigSetTrackingOptions tests backend method directly.
func TestAccuracy_BackendPutConfigSetTrackingOptions(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateConfigurationSet("track-test")
	require.NoError(t, err)

	err = backend.PutConfigurationSetTrackingOptions("track-test", "click.example.com", "REQUIRE")
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("track-test")
	require.NoError(t, err)
	assert.Equal(t, "click.example.com", cs.TrackingCustomRedirectDomain)
	assert.Equal(t, "REQUIRE", cs.TrackingHTTPSPolicy)
}

// TestAccuracy_BackendPutConfigSetTrackingOptionsNotFound errors for missing set.
func TestAccuracy_BackendPutConfigSetTrackingOptionsNotFound(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	err := backend.PutConfigurationSetTrackingOptions("no-such", "x.com", "")
	require.Error(t, err)
}

// TestAccuracy_BackendPutConfigSetDeliveryOptions tests backend method directly.
func TestAccuracy_BackendPutConfigSetDeliveryOptions(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateConfigurationSet("delivery-test")
	require.NoError(t, err)

	err = backend.PutConfigurationSetDeliveryOptions("delivery-test", "REQUIRE", "my-pool")
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("delivery-test")
	require.NoError(t, err)
	assert.Equal(t, "REQUIRE", cs.DeliveryTLSPolicy)
	assert.Equal(t, "my-pool", cs.DeliverySendingPoolName)
}

// TestAccuracy_BackendPutConfigSetSendingOptions tests backend method directly.
func TestAccuracy_BackendPutConfigSetSendingOptions(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateConfigurationSet("send-test")
	require.NoError(t, err)

	// Default is true; disable.
	err = backend.PutConfigurationSetSendingOptions("send-test", false)
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("send-test")
	require.NoError(t, err)
	assert.False(t, cs.SendingEnabled)
}

// TestAccuracy_BackendPutConfigSetReputationOptions tests backend method directly.
func TestAccuracy_BackendPutConfigSetReputationOptions(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateConfigurationSet("rep-test")
	require.NoError(t, err)

	err = backend.PutConfigurationSetReputationOptions("rep-test", true)
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("rep-test")
	require.NoError(t, err)
	assert.True(t, cs.ReputationMetricsEnabled)
}

// TestAccuracy_BackendPutConfigSetSuppressionOptions tests backend method directly.
func TestAccuracy_BackendPutConfigSetSuppressionOptions(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateConfigurationSet("supp-test")
	require.NoError(t, err)

	err = backend.PutConfigurationSetSuppressionOptions("supp-test", []string{"BOUNCE", "COMPLAINT"})
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("supp-test")
	require.NoError(t, err)
	assert.Equal(t, []string{"BOUNCE", "COMPLAINT"}, cs.SuppressionReasons)
}

// TestAccuracy_BackendPutEmailIdentityConfigSetAttributes tests backend method.
func TestAccuracy_BackendPutEmailIdentityConfigSetAttributes(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateEmailIdentity("csattr@example.com", "", nil)
	require.NoError(t, err)

	err = backend.PutEmailIdentityConfigurationSetAttributes("csattr@example.com", "my-cs")
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("csattr@example.com")
	require.NoError(t, err)
	assert.Equal(t, "my-cs", ei.ConfigurationSetName)
}

// TestAccuracy_BackendPutEmailIdentityDkimAttributes tests backend method.
func TestAccuracy_BackendPutEmailIdentityDkimAttributes(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateEmailIdentity("dkim@example.com", "", nil)
	require.NoError(t, err)

	// Default is true; disable.
	err = backend.PutEmailIdentityDkimAttributes("dkim@example.com", false)
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("dkim@example.com")
	require.NoError(t, err)
	assert.False(t, ei.DkimSigningEnabled)
}

// TestAccuracy_BackendPutEmailIdentityFeedbackAttributes tests backend method.
func TestAccuracy_BackendPutEmailIdentityFeedbackAttributes(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateEmailIdentity("fb@example.com", "", nil)
	require.NoError(t, err)

	// Default is true; disable.
	err = backend.PutEmailIdentityFeedbackAttributes("fb@example.com", false)
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("fb@example.com")
	require.NoError(t, err)
	assert.False(t, ei.FeedbackForwarding)
}

// TestAccuracy_BackendPutEmailIdentityMailFromAttributes tests backend method.
func TestAccuracy_BackendPutEmailIdentityMailFromAttributes(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateEmailIdentity("mf@example.com", "", nil)
	require.NoError(t, err)

	err = backend.PutEmailIdentityMailFromAttributes("mf@example.com", "bounce.example.com", "REJECT_MESSAGE")
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("mf@example.com")
	require.NoError(t, err)
	assert.Equal(t, "bounce.example.com", ei.MailFromDomain)
	assert.Equal(t, "PENDING", ei.MailFromDomainStatus)
	assert.Equal(t, "REJECT_MESSAGE", ei.BehaviorOnMxFailure)
}

// TestAccuracy_BackendPutEmailIdentityMailFromAttributesDefaultBehavior tests default behavior.
func TestAccuracy_BackendPutEmailIdentityMailFromAttributesDefaultBehavior(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	_, err := backend.CreateEmailIdentity("mfdefault@example.com", "", nil)
	require.NoError(t, err)

	// Empty behavior should default to USE_DEFAULT_VALUE.
	err = backend.PutEmailIdentityMailFromAttributes("mfdefault@example.com", "bounce.example.com", "")
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("mfdefault@example.com")
	require.NoError(t, err)
	assert.Equal(t, "USE_DEFAULT_VALUE", ei.BehaviorOnMxFailure)
}

// TestAccuracy_BackendCreateEmailIdentityWithConfigSet tests config set name storage.
func TestAccuracy_BackendCreateEmailIdentityWithConfigSet(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	ei, err := backend.CreateEmailIdentity("cs@example.com", "my-config-set", nil)
	require.NoError(t, err)
	assert.Equal(t, "my-config-set", ei.ConfigurationSetName)

	fetched, err := backend.GetEmailIdentity("cs@example.com")
	require.NoError(t, err)
	assert.Equal(t, "my-config-set", fetched.ConfigurationSetName)
}

// TestAccuracy_BackendCreateEmailIdentityWithTags tests tag storage.
func TestAccuracy_BackendCreateEmailIdentityWithTags(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	tags := map[string]string{"env": "test", "service": "sesv2"}
	ei, err := backend.CreateEmailIdentity("tags@example.com", "", tags)
	require.NoError(t, err)
	assert.Equal(t, "test", ei.Tags["env"])

	fetched, err := backend.GetEmailIdentity("tags@example.com")
	require.NoError(t, err)
	assert.Equal(t, "sesv2", fetched.Tags["service"])
}

// TestAccuracy_BackendCreateEmailIdentityDomainDkimTokens tests DKIM token generation.
func TestAccuracy_BackendCreateEmailIdentityDomainDkimTokens(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	ei, err := backend.CreateEmailIdentity("dkim-tokens.com", "", nil)
	require.NoError(t, err)

	assert.Equal(t, "DOMAIN", ei.IdentityType)
	assert.Len(t, ei.DkimTokens, 3, "Domain should have 3 DKIM tokens")

	for _, tok := range ei.DkimTokens {
		assert.Len(t, tok, 24, "Each token should be 24 chars")
	}

	assert.Equal(t, "SUCCESS", ei.DkimStatus)
	assert.True(t, ei.DkimSigningEnabled)
}

// TestAccuracy_BackendCreateEmailIdentityEmailNoDkimTokens verifies email has no DKIM tokens.
func TestAccuracy_BackendCreateEmailIdentityEmailNoDkimTokens(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	ei, err := backend.CreateEmailIdentity("no-tokens@example.com", "", nil)
	require.NoError(t, err)

	assert.Equal(t, "EMAIL_ADDRESS", ei.IdentityType)
	assert.Empty(t, ei.DkimTokens, "Email address should have no DKIM tokens")
}

// TestAccuracy_BackendIdentityDefaultsVerificationStatus tests default verification status.
func TestAccuracy_BackendIdentityDefaultsVerificationStatus(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	ei, err := backend.CreateEmailIdentity("verify@example.com", "", nil)
	require.NoError(t, err)

	assert.Equal(t, "SUCCESS", ei.VerificationStatus)
	assert.True(t, ei.FeedbackForwarding)
	assert.True(t, ei.VerifiedForSending)
}

// TestAccuracy_BackendConfigSetDefaultSendingEnabled verifies CreateConfigurationSet default.
func TestAccuracy_BackendConfigSetDefaultSendingEnabled(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	cs, err := backend.CreateConfigurationSet("default-send")
	require.NoError(t, err)
	assert.True(t, cs.SendingEnabled, "SendingEnabled should default to true")
}

// TestAccuracy_BackendPutConfigSetArchivingOptionsNotFound errors for missing set.
func TestAccuracy_BackendPutConfigSetArchivingOptionsNotFound(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	err := backend.PutConfigurationSetArchivingOptions("no-such", "")
	require.Error(t, err)
}

// TestAccuracy_BackendPutConfigSetVdmOptionsNotFound errors for missing set.
func TestAccuracy_BackendPutConfigSetVdmOptionsNotFound(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	err := backend.PutConfigurationSetVdmOptions("no-such", nil, nil)
	require.Error(t, err)
}

// TestAccuracy_BackendPutEmailIdentityDkimSigningAttributesNotFound errors for missing identity.
func TestAccuracy_BackendPutEmailIdentityDkimSigningAttributesNotFound(t *testing.T) {
	t.Parallel()

	backend := newSESv2Backend()

	err := backend.PutEmailIdentityDkimSigningAttributes("no-such@example.com")
	require.Error(t, err)
}
