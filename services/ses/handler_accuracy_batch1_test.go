package ses_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ---- Identity CRUD ----

func TestBatch1_VerifyEmailIdentity_Basic(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"alice@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyEmailIdentityResponse")

	attrs := h.Backend.GetIdentityVerificationAttributes([]string{"alice@example.com"})
	assert.Equal(t, "Success", attrs["alice@example.com"])
}

func TestBatch1_VerifyEmailIdentity_MissingAddress(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_VerifyEmailIdentity_Idempotent(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("bob@example.com"))
	require.NoError(t, b.VerifyEmailIdentity("bob@example.com"))

	attrs := b.GetIdentityVerificationAttributes([]string{"bob@example.com"})
	assert.Equal(t, "Success", attrs["bob@example.com"])
}

func TestBatch1_VerifyDomainIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"VerifyDomainIdentity"},
		"Version": {"2010-12-01"},
		"Domain":  {"example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyDomainIdentityResponse")
	assert.Contains(t, rec.Body.String(), "VerificationToken")

	attrs := h.Backend.GetIdentityVerificationAttributes([]string{"example.com"})
	assert.Equal(t, "Success", attrs["example.com"])
}

func TestBatch1_VerifyDomainIdentity_Deterministic(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	tok1, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	tok2, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	assert.Equal(t, tok1, tok2, "token must be deterministic")

	tok3, err := b.VerifyDomainIdentity("other.com")
	require.NoError(t, err)
	assert.NotEqual(t, tok1, tok3, "different domains get different tokens")
}

func TestBatch1_VerifyEmailAddress_LegacyAPI(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":       {"VerifyEmailAddress"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"legacy@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	addrs := h.Backend.ListVerifiedEmailAddresses()
	assert.Contains(t, addrs, "legacy@example.com")
}

func TestBatch1_DeleteVerifiedEmailAddress(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("del@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":       {"DeleteVerifiedEmailAddress"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"del@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	addrs := h.Backend.ListVerifiedEmailAddresses()
	assert.NotContains(t, addrs, "del@example.com")
}

func TestBatch1_DeleteIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("gone@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":   {"DeleteIdentity"},
		"Version":  {"2010-12-01"},
		"Identity": {"gone@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteIdentityResponse")

	p := h.Backend.ListIdentities("", 100)
	assert.NotContains(t, p.Data, "gone@example.com")
}

func TestBatch1_DeleteIdentity_NonExistent_IsIdempotent(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":   {"DeleteIdentity"},
		"Version":  {"2010-12-01"},
		"Identity": {"nobody@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ListIdentities_Pagination(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	for i := range 5 {
		require.NoError(t, b.VerifyEmailIdentity(fmt.Sprintf("user%d@example.com", i)))
	}

	p := b.ListIdentities("", 3)
	assert.Len(t, p.Data, 3)
	assert.NotEmpty(t, p.Next)

	p2 := b.ListIdentities(p.Next, 3)
	assert.Len(t, p2.Data, 2)
	assert.Empty(t, p2.Next)
}

func TestBatch1_ListIdentities_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, h.Backend.VerifyEmailIdentity("b@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":   {"ListIdentities"},
		"Version":  {"2010-12-01"},
		"MaxItems": {"10"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "a@example.com")
	assert.Contains(t, rec.Body.String(), "b@example.com")
}

func TestBatch1_ListVerifiedEmailAddresses_FiltersDomains(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("email@example.com"))
	_, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)

	addrs := b.ListVerifiedEmailAddresses()
	assert.Contains(t, addrs, "email@example.com")
	assert.NotContains(t, addrs, "example.com")
}

func TestBatch1_GetIdentityVerificationAttributes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("v@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityVerificationAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"v@example.com"},
		"Identities.member.2": {"unknown@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Success")
	assert.Contains(t, rec.Body.String(), "NotStarted")
}

// ---- DKIM ----

func TestBatch1_VerifyDomainDkim_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"VerifyDomainDkim"},
		"Version": {"2010-12-01"},
		"Domain":  {"dkim.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyDomainDkimResponse")
	assert.Contains(t, rec.Body.String(), "DkimTokens")
}

func TestBatch1_GetIdentityDkimAttributes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("dkim@example.com"))
	require.NoError(t, h.Backend.SetIdentityDkimEnabled("dkim@example.com", true))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"dkim@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetIdentityDkimAttributesResponse")
}

func TestBatch1_SetIdentityDkimEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("d@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":      {"SetIdentityDkimEnabled"},
		"Version":     {"2010-12-01"},
		"Identity":    {"d@example.com"},
		"DkimEnabled": {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	attrs := h.Backend.GetIdentityDkimAttributes([]string{"d@example.com"})
	assert.True(t, attrs["d@example.com"].DkimEnabled)
}

func TestBatch1_SetIdentityDkimEnabled_False(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("d@example.com"))
	require.NoError(t, b.SetIdentityDkimEnabled("d@example.com", true))
	require.NoError(t, b.SetIdentityDkimEnabled("d@example.com", false))

	attrs := b.GetIdentityDkimAttributes([]string{"d@example.com"})
	assert.False(t, attrs["d@example.com"].DkimEnabled)
}

func TestBatch1_DkimTokens_AfterDomainVerify(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	tokens, err := b.VerifyDomainDkim("tokens.example.com")
	require.NoError(t, err)
	require.Len(t, tokens, 3)
	for _, tok := range tokens {
		assert.NotEmpty(t, tok)
		assert.Len(t, tok, 32)
	}

	attrs := b.GetIdentityDkimAttributes([]string{"tokens.example.com"})
	assert.Equal(t, "Success", attrs["tokens.example.com"].DkimVerificationStatus)
	assert.Equal(t, tokens, attrs["tokens.example.com"].DkimTokens)
}

func TestBatch1_DkimAttributes_Unknown_NotStarted(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	attrs := b.GetIdentityDkimAttributes([]string{"unknown@example.com"})
	assert.Equal(t, "NotStarted", attrs["unknown@example.com"].DkimVerificationStatus)
	assert.Empty(t, attrs["unknown@example.com"].DkimTokens)
}

// ---- MailFrom domain ----

func TestBatch1_SetIdentityMailFromDomain_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("mf@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":         {"SetIdentityMailFromDomain"},
		"Version":        {"2010-12-01"},
		"Identity":       {"mf@example.com"},
		"MailFromDomain": {"mail.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	attrs := h.Backend.GetIdentityMailFromDomainAttributes([]string{"mf@example.com"})
	assert.Equal(t, "mail.example.com", attrs["mf@example.com"].MailFromDomain)
	assert.Equal(t, "Success", attrs["mf@example.com"].MailFromDomainStatus)
}

func TestBatch1_SetIdentityMailFromDomain_Clear(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("mf@example.com"))
	require.NoError(t, b.SetIdentityMailFromDomain("mf@example.com", "mail.example.com"))
	require.NoError(t, b.SetIdentityMailFromDomain("mf@example.com", ""))

	attrs := b.GetIdentityMailFromDomainAttributes([]string{"mf@example.com"})
	assert.Empty(t, attrs["mf@example.com"].MailFromDomain)
	assert.Empty(t, attrs["mf@example.com"].MailFromDomainStatus)
}

func TestBatch1_GetIdentityMailFromDomainAttributes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("mf2@example.com"))
	require.NoError(t, h.Backend.SetIdentityMailFromDomain("mf2@example.com", "bounce.example.com"))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityMailFromDomainAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"mf2@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "bounce.example.com")
}

func TestBatch1_MailFromDomainAttributes_Unknown_Empty(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	attrs := b.GetIdentityMailFromDomainAttributes([]string{"nobody@example.com"})
	assert.Empty(t, attrs["nobody@example.com"].MailFromDomain)
	assert.Empty(t, attrs["nobody@example.com"].MailFromDomainStatus)
}

// ---- Notification attributes ----

func TestBatch1_SetIdentityNotificationTopic_AllTypes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("notif@example.com"))

	for _, notifType := range []string{"Bounce", "Complaint", "Delivery"} {
		rec := postForm(t, h, url.Values{
			"Action":           {"SetIdentityNotificationTopic"},
			"Version":          {"2010-12-01"},
			"Identity":         {"notif@example.com"},
			"NotificationType": {notifType},
			"SnsTopic":         {"arn:aws:sns:us-east-1:123:" + notifType},
		}.Encode())
		assert.Equal(t, http.StatusOK, rec.Code, "type=%s", notifType)
	}

	attrs := h.Backend.GetIdentityNotificationAttributes([]string{"notif@example.com"})
	a := attrs["notif@example.com"]
	assert.Equal(t, "arn:aws:sns:us-east-1:123:Bounce", a.BounceTopic)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:Complaint", a.ComplaintTopic)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:Delivery", a.DeliveryTopic)
}

func TestBatch1_SetIdentityFeedbackForwardingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("fwd@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":            {"SetIdentityFeedbackForwardingEnabled"},
		"Version":           {"2010-12-01"},
		"Identity":          {"fwd@example.com"},
		"ForwardingEnabled": {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	attrs := h.Backend.GetIdentityNotificationAttributes([]string{"fwd@example.com"})
	assert.False(t, attrs["fwd@example.com"].ForwardingEnabled)
}

func TestBatch1_SetIdentityHeadersInNotificationsEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("hdr@example.com"))

	for _, notifType := range []string{"Bounce", "Complaint", "Delivery"} {
		rec := postForm(t, h, url.Values{
			"Action":           {"SetIdentityHeadersInNotificationsEnabled"},
			"Version":          {"2010-12-01"},
			"Identity":         {"hdr@example.com"},
			"NotificationType": {notifType},
			"Enabled":          {"true"},
		}.Encode())
		assert.Equal(t, http.StatusOK, rec.Code, "type=%s", notifType)
	}

	attrs := h.Backend.GetIdentityNotificationAttributes([]string{"hdr@example.com"})
	a := attrs["hdr@example.com"]
	assert.True(t, a.HeadersInBounce)
	assert.True(t, a.HeadersInComplaint)
	assert.True(t, a.HeadersInDelivery)
}

func TestBatch1_GetIdentityNotificationAttributes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("na@example.com"))
	require.NoError(t, h.Backend.SetIdentityNotificationTopic("na@example.com", "Bounce", "arn:sns:bounce"))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityNotificationAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"na@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetIdentityNotificationAttributesResponse")
}

func TestBatch1_NotificationAttributes_Unknown_ForwardingEnabled(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	attrs := b.GetIdentityNotificationAttributes([]string{"unknown@example.com"})
	assert.True(t, attrs["unknown@example.com"].ForwardingEnabled, "forwarding defaults true for unknown identity")
}

// ---- Identity Policy ----

func TestBatch1_PutIdentityPolicy_StoresDocument(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}`
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "send-policy", doc))

	pols, err := b.GetIdentityPolicies("pol@example.com", []string{"send-policy"})
	require.NoError(t, err)
	assert.Equal(t, doc, pols["send-policy"])
}

func TestBatch1_PutIdentityPolicy_OverwritesExisting(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "p1", `{"v":1}`))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "p1", `{"v":2}`))

	pols, err := b.GetIdentityPolicies("pol@example.com", []string{"p1"})
	require.NoError(t, err)
	assert.Equal(t, `{"v":2}`, pols["p1"])
}

func TestBatch1_ListIdentityPolicies_ReturnsSortedNames(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "z-policy", `{}`))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "a-policy", `{}`))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "m-policy", `{}`))

	names, err := b.ListIdentityPolicies("pol@example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"a-policy", "m-policy", "z-policy"}, names)
}

func TestBatch1_DeleteIdentityPolicy_RemovesFromList(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "del-pol", `{}`))
	require.NoError(t, b.DeleteIdentityPolicy("pol@example.com", "del-pol"))

	names, err := b.ListIdentityPolicies("pol@example.com")
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestBatch1_DeleteIdentityPolicy_NonExistent_Idempotent(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, b.DeleteIdentityPolicy("pol@example.com", "nonexistent"))
}

func TestBatch1_GetIdentityPolicies_EmptyNamesList_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "p1", `{"a":1}`))
	require.NoError(t, b.PutIdentityPolicy("pol@example.com", "p2", `{"b":2}`))

	pols, err := b.GetIdentityPolicies("pol@example.com", nil)
	require.NoError(t, err)
	assert.Len(t, pols, 2)
	assert.Contains(t, pols, "p1")
	assert.Contains(t, pols, "p2")
}

func TestBatch1_PutIdentityPolicy_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("pol@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":     {"PutIdentityPolicy"},
		"Version":    {"2010-12-01"},
		"Identity":   {"pol@example.com"},
		"PolicyName": {"my-send-policy"},
		"Policy":     {`{"Version":"2012-10-17","Statement":[]}`},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	names, err := h.Backend.ListIdentityPolicies("pol@example.com")
	require.NoError(t, err)
	assert.Contains(t, names, "my-send-policy")
}

func TestBatch1_GetIdentityPolicies_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, h.Backend.PutIdentityPolicy("pol@example.com", "p1", `{"doc":true}`))

	rec := postForm(t, h, url.Values{
		"Action":               {"GetIdentityPolicies"},
		"Version":              {"2010-12-01"},
		"Identity":             {"pol@example.com"},
		"PolicyNames.member.1": {"p1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetIdentityPoliciesResponse")
}

func TestBatch1_ListIdentityPolicies_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, h.Backend.PutIdentityPolicy("pol@example.com", "pol1", `{}`))
	require.NoError(t, h.Backend.PutIdentityPolicy("pol@example.com", "pol2", `{}`))

	rec := postForm(t, h, url.Values{
		"Action":   {"ListIdentityPolicies"},
		"Version":  {"2010-12-01"},
		"Identity": {"pol@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListIdentityPoliciesResponse")
}

func TestBatch1_DeleteIdentityPolicy_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("pol@example.com"))
	require.NoError(t, h.Backend.PutIdentityPolicy("pol@example.com", "todel", `{}`))

	rec := postForm(t, h, url.Values{
		"Action":     {"DeleteIdentityPolicy"},
		"Version":    {"2010-12-01"},
		"Identity":   {"pol@example.com"},
		"PolicyName": {"todel"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	names, err := h.Backend.ListIdentityPolicies("pol@example.com")
	require.NoError(t, err)
	assert.NotContains(t, names, "todel")
}

func TestBatch1_IdentityPolicy_MissingIdentity_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.Error(t, b.PutIdentityPolicy("", "p", `{}`))
	require.Error(t, b.DeleteIdentityPolicy("", "p"))
	_, err := b.GetIdentityPolicies("", nil)
	require.Error(t, err)
	_, err = b.ListIdentityPolicies("")
	require.Error(t, err)
}

func TestBatch1_PolicyCount_Tracks(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.VerifyEmailIdentity("b@example.com"))
	assert.Equal(t, 0, b.PolicyCount())

	require.NoError(t, b.PutIdentityPolicy("a@example.com", "p1", `{}`))
	require.NoError(t, b.PutIdentityPolicy("a@example.com", "p2", `{}`))
	require.NoError(t, b.PutIdentityPolicy("b@example.com", "p1", `{}`))
	assert.Equal(t, 3, b.PolicyCount())

	require.NoError(t, b.DeleteIdentityPolicy("a@example.com", "p1"))
	assert.Equal(t, 2, b.PolicyCount())
}

// ---- Account Sending Enabled ----

func TestBatch1_UpdateAccountSendingEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	assert.True(t, b.GetAccountSendingEnabled(), "default is enabled")

	b.UpdateAccountSendingEnabled(false)
	assert.False(t, b.GetAccountSendingEnabled())

	b.UpdateAccountSendingEnabled(true)
	assert.True(t, b.GetAccountSendingEnabled())
}

func TestBatch1_GetAccountSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=GetAccountSendingEnabled&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetAccountSendingEnabledResponse")
	assert.Contains(t, rec.Body.String(), "true")
}

func TestBatch1_UpdateAccountSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"UpdateAccountSendingEnabled"},
		"Version": {"2010-12-01"},
		"Enabled": {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.False(t, h.Backend.(*ses.InMemoryBackend).AccountSendingEnabledState())

	rec2 := postForm(t, h, "Action=GetAccountSendingEnabled&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "false")
}

func TestBatch1_AccountSendingEnabled_Reset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.UpdateAccountSendingEnabled(false)
	assert.False(t, b.GetAccountSendingEnabled())

	b.Reset()
	assert.True(t, b.GetAccountSendingEnabled(), "reset restores default true")
}

// ---- Send Email ----

func TestBatch1_SendEmail_BasicHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("from@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"from@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"Test Subject"},
		"Message.Body.Text.Data":           {"Hello World"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendEmailResponse")
	assert.Contains(t, rec.Body.String(), "MessageId")

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "from@example.com", emails[0].From)
	assert.Equal(t, "Test Subject", emails[0].Subject)
}

func TestBatch1_SendEmail_HTMLBody(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("from@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"from@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"HTML Email"},
		"Message.Body.Html.Data":           {"<h1>Hello</h1>"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "<h1>Hello</h1>", emails[0].BodyHTML)
}

func TestBatch1_SendEmail_UnverifiedSource_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"unverified@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"Subject"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_SendEmail_MultipleTo(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"a@example.com"},
		"Destination.ToAddresses.member.2": {"b@example.com"},
		"Destination.ToAddresses.member.3": {"c@example.com"},
		"Message.Subject.Data":             {"Multi"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Len(t, emails[0].To, 3)
}

func TestBatch1_SendEmail_ReturnPath(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"S"},
		"Message.Body.Text.Data":           {"b"},
		"ReturnPath":                       {"bounce@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "bounce@example.com", emails[0].ReturnPath)
}

// ---- SendRawEmail ----

func TestBatch1_SendRawEmail_ParsesHeaders(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("raw@example.com"))

	rawMsg := strings.Join([]string{
		"From: raw@example.com",
		"To: dest@example.com",
		"Subject: Raw Test",
		"",
		"Raw body text",
	}, "\r\n")

	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"RawMessage.Data": {rawMsg},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "raw@example.com", emails[0].From)
	assert.Equal(t, "Raw Test", emails[0].Subject)
	assert.Equal(t, []string{"dest@example.com"}, emails[0].To)
}

func TestBatch1_SendRawEmail_ExplicitSourceOverridesHeader(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("explicit@example.com"))

	rawMsg := strings.Join([]string{
		"From: header@example.com",
		"To: dest@example.com",
		"Subject: Test",
		"",
		"body",
	}, "\r\n")

	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"Source":          {"explicit@example.com"},
		"RawMessage.Data": {rawMsg},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "explicit@example.com", emails[0].From)
}

func TestBatch1_SendRawEmail_EmptyMessage_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"RawMessage.Data": {""},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- SendTemplatedEmail ----

func TestBatch1_SendTemplatedEmail_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "greet",
		SubjectPart:  "Hello",
		TextPart:     "Hi there",
	}))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendTemplatedEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Template":                         {"greet"},
		"TemplateData":                     {`{"name":"Alice"}`},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendTemplatedEmailResponse")
}

func TestBatch1_SendTemplatedEmail_NonExistentTemplate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendTemplatedEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Template":                         {"nonexistent"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- SendBulkTemplatedEmail ----

func TestBatch1_SendBulkTemplatedEmail_MultipleDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "bulk",
		SubjectPart:  "Bulk",
		TextPart:     "Hi",
	}))

	rec := postForm(t, h, url.Values{
		"Action":   {"SendBulkTemplatedEmail"},
		"Version":  {"2010-12-01"},
		"Source":   {"s@example.com"},
		"Template": {"bulk"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
		"Destinations.member.3.Destination.ToAddresses.member.1": {"c@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	assert.Len(t, emails, 3)
}

func TestBatch1_SendBulkTemplatedEmail_PerDestinationData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t",
		SubjectPart:  "Hi",
		TextPart:     "text",
	}))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}, ReplacementTemplateData: `{"name":"Alice"}`},
		{To: []string{"b@example.com"}, Cc: []string{"cc@example.com"}},
	}

	ids, err := b.SendBulkTemplatedEmail("s@example.com", "t", dests)
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	emails := b.ListEmails()
	assert.Len(t, emails, 2)
	assert.Equal(t, []string{"cc@example.com"}, emails[1].Cc)
}

// ---- SendBounce ----

func TestBatch1_SendBounce_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	msgID, err := h.Backend.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "orig",
		BodyText: "body",
	})
	require.NoError(t, err)

	rec := postForm(t, h, url.Values{
		"Action":                  {"SendBounce"},
		"Version":                 {"2010-12-01"},
		"OriginalMessageId":       {msgID},
		"BounceSender":            {"mailer-daemon@example.com"},
		"MessageDsn.ReportingMta": {"dns; example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendBounceResponse")
}

// ---- SendCustomVerificationEmail ----

func TestBatch1_SendCustomVerificationEmail_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv-tmpl",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<p>Click here</p>",
		SuccessRedirectionURL: "https://example.com/success",
		FailureRedirectionURL: "https://example.com/failure",
	}))

	rec := postForm(t, h, url.Values{
		"Action":       {"SendCustomVerificationEmail"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"user@example.com"},
		"TemplateName": {"cv-tmpl"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendCustomVerificationEmailResponse")
}

// ---- Template CRUD ----

func TestBatch1_CreateTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                {"CreateTemplate"},
		"Version":               {"2010-12-01"},
		"Template.TemplateName": {"my-tmpl"},
		"Template.SubjectPart":  {"Subject {{name}}"},
		"Template.TextPart":     {"Hello {{name}}"},
		"Template.HtmlPart":     {"<p>Hello {{name}}</p>"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTemplateResponse")

	tmpl, err := h.Backend.GetTemplate("my-tmpl")
	require.NoError(t, err)
	assert.Equal(t, "Subject {{name}}", tmpl.SubjectPart)
}

func TestBatch1_CreateTemplate_Duplicate_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1", SubjectPart: "s"}))
	assert.Error(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1", SubjectPart: "s2"}))
}

func TestBatch1_UpdateTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "upd-tmpl",
		SubjectPart:  "Old Subject",
		TextPart:     "old text",
	}))

	rec := postForm(t, h, url.Values{
		"Action":                {"UpdateTemplate"},
		"Version":               {"2010-12-01"},
		"Template.TemplateName": {"upd-tmpl"},
		"Template.SubjectPart":  {"New Subject"},
		"Template.TextPart":     {"new text"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	tmpl, err := h.Backend.GetTemplate("upd-tmpl")
	require.NoError(t, err)
	assert.Equal(t, "New Subject", tmpl.SubjectPart)
}

func TestBatch1_GetTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "get-tmpl",
		SubjectPart:  "Subject",
		TextPart:     "Text",
	}))

	rec := postForm(t, h, url.Values{
		"Action":       {"GetTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"get-tmpl"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetTemplateResponse")
	assert.Contains(t, rec.Body.String(), "get-tmpl")
}

func TestBatch1_GetTemplate_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":       {"GetTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"noexist"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_DeleteTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "del-tmpl", SubjectPart: "s"}))

	rec := postForm(t, h, url.Values{
		"Action":       {"DeleteTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"del-tmpl"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).TemplateCount())
}

func TestBatch1_ListTemplates_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()
	for i := range 5 {
		require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
			TemplateName: fmt.Sprintf("tmpl-%d", i),
			SubjectPart:  "s",
		}))
	}

	rec := postForm(t, h, url.Values{
		"Action":   {"ListTemplates"},
		"Version":  {"2010-12-01"},
		"MaxItems": {"3"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListTemplatesResponse")
	assert.Contains(t, rec.Body.String(), "NextToken")
}

// ---- TestRenderTemplate ----

func TestBatch1_TestRenderTemplate_VariableSubstitution(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "render-tmpl",
		SubjectPart:  "Hello {{name}}",
		TextPart:     "Dear {{name}}, welcome to {{company}}",
		HTMLPart:     "<p>Dear {{name}}</p>",
	}))

	result, err := b.TestRenderTemplate("render-tmpl", `{"name":"Alice","company":"ACME"}`)
	require.NoError(t, err)
	assert.Contains(t, result, "Hello Alice")
	assert.Contains(t, result, "Dear Alice")
	assert.Contains(t, result, "welcome to ACME")
}

func TestBatch1_TestRenderTemplate_EmptyData_NoSubstitution(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t",
		SubjectPart:  "Hi {{name}}",
		TextPart:     "body",
	}))

	result, err := b.TestRenderTemplate("t", "")
	require.NoError(t, err)
	assert.Contains(t, result, "Hi {{name}}", "placeholders remain when no data")
}

func TestBatch1_TestRenderTemplate_NotFound_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	_, err := b.TestRenderTemplate("nonexistent", `{}`)
	assert.Error(t, err)
}

func TestBatch1_TestRenderTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "rt",
		SubjectPart:  "Hello {{name}}",
		TextPart:     "Welcome {{name}}",
	}))

	rec := postForm(t, h, url.Values{
		"Action":       {"TestRenderTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"rt"},
		"TemplateData": {`{"name":"Bob"}`},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TestRenderTemplateResponse")
	assert.Contains(t, rec.Body.String(), "Bob")
}

// ---- Custom Verification Email Templates ----

func TestBatch1_CreateCustomVerificationEmailTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                {"CreateCustomVerificationEmailTemplate"},
		"Version":               {"2010-12-01"},
		"TemplateName":          {"cv-new"},
		"FromEmailAddress":      {"noreply@example.com"},
		"TemplateSubject":       {"Verify your account"},
		"TemplateContent":       {"<p>Click here to verify</p>"},
		"SuccessRedirectionURL": {"https://example.com/success"},
		"FailureRedirectionURL": {"https://example.com/failure"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).CustomVerifTemplateCount())
}

func TestBatch1_GetCustomVerificationEmailTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv-get",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "click here",
		SuccessRedirectionURL: "https://success.example.com",
		FailureRedirectionURL: "https://failure.example.com",
	}))

	rec := postForm(t, h, url.Values{
		"Action":       {"GetCustomVerificationEmailTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"cv-get"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "cv-get")
}

func TestBatch1_UpdateCustomVerificationEmailTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv-upd",
		FromEmailAddress:      "old@example.com",
		TemplateSubject:       "Old Subject",
		TemplateContent:       "old content",
		SuccessRedirectionURL: "https://old.example.com/s",
		FailureRedirectionURL: "https://old.example.com/f",
	}))

	rec := postForm(t, h, url.Values{
		"Action":                {"UpdateCustomVerificationEmailTemplate"},
		"Version":               {"2010-12-01"},
		"TemplateName":          {"cv-upd"},
		"FromEmailAddress":      {"new@example.com"},
		"TemplateSubject":       {"New Subject"},
		"TemplateContent":       {"new content"},
		"SuccessRedirectionURL": {"https://new.example.com/s"},
		"FailureRedirectionURL": {"https://new.example.com/f"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	tmpl, err := h.Backend.GetCustomVerificationEmailTemplate("cv-upd")
	require.NoError(t, err)
	assert.Equal(t, "New Subject", tmpl.TemplateSubject)
}

func TestBatch1_DeleteCustomVerificationEmailTemplate_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv-del",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "V",
		TemplateContent:       "c",
		SuccessRedirectionURL: "https://s.example.com",
		FailureRedirectionURL: "https://f.example.com",
	}))

	rec := postForm(t, h, url.Values{
		"Action":       {"DeleteCustomVerificationEmailTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"cv-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).CustomVerifTemplateCount())
}

func TestBatch1_ListCustomVerificationEmailTemplates_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	for i := range 3 {
		require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
			TemplateName:          fmt.Sprintf("cv-%d", i),
			FromEmailAddress:      "noreply@example.com",
			TemplateSubject:       "V",
			TemplateContent:       "c",
			SuccessRedirectionURL: "https://s.example.com",
			FailureRedirectionURL: "https://f.example.com",
		}))
	}

	rec := postForm(t, h, "Action=ListCustomVerificationEmailTemplates&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListCustomVerificationEmailTemplatesResponse")
}

// ---- ConfigurationSet CRUD ----

func TestBatch1_CreateConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSet"},
		"Version":               {"2010-12-01"},
		"ConfigurationSet.Name": {"cs-new"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateConfigurationSetResponse")

	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).ConfigSetCount())
}

func TestBatch1_CreateConfigurationSet_Duplicate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSet"},
		"Version":               {"2010-12-01"},
		"ConfigurationSet.Name": {"cs1"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_DeleteConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-del"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).ConfigSetCount())
}

func TestBatch1_ListConfigurationSets_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-a"))
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-b"))

	rec := postForm(t, h, "Action=ListConfigurationSets&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListConfigurationSetsResponse")
	assert.Contains(t, rec.Body.String(), "cs-a")
}

func TestBatch1_DescribeConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-desc"))
	require.NoError(t, h.Backend.PutConfigurationSetDeliveryOptions("cs-desc", "Require"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs-desc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "cs-desc")
	assert.Contains(t, rec.Body.String(), "Require")
}

func TestBatch1_DescribeConfigurationSet_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"noexist"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_UpdateConfigurationSetSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"UpdateConfigurationSetSendingEnabled"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"Enabled":              {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.False(t, desc.SendingEnabled)
}

func TestBatch1_UpdateConfigurationSetReputationMetricsEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"UpdateConfigurationSetReputationMetricsEnabled"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"Enabled":              {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.ReputationMetricsEnabled)
}

func TestBatch1_PutConfigurationSetDeliveryOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                    {"PutConfigurationSetDeliveryOptions"},
		"Version":                   {"2010-12-01"},
		"ConfigurationSetName":      {"cs1"},
		"DeliveryOptions.TlsPolicy": {"Require"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
}

// ---- ConfigurationSet EventDestinations ----

func TestBatch1_CreateConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                   {"CreateConfigurationSetEventDestination"},
		"Version":                  {"2010-12-01"},
		"ConfigurationSetName":     {"cs1"},
		"EventDestination.Name":    {"ed1"},
		"EventDestination.Enabled": {"true"},
		"EventDestination.MatchingEventTypes.member.1": {"Send"},
		"EventDestination.MatchingEventTypes.member.2": {"Bounce"},
		"EventDestination.SNSDestination.TopicARN":     {"arn:aws:sns:us-east-1:123:topic"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
}

func TestBatch1_CreateConfigurationSetEventDestination_Duplicate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed1",
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSetEventDestination"},
		"Version":               {"2010-12-01"},
		"ConfigurationSetName":  {"cs1"},
		"EventDestination.Name": {"ed1"},
		"EventDestination.MatchingEventTypes.member.1": {"Bounce"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_UpdateConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed1",
		Enabled:            false,
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":                   {"UpdateConfigurationSetEventDestination"},
		"Version":                  {"2010-12-01"},
		"ConfigurationSetName":     {"cs1"},
		"EventDestination.Name":    {"ed1"},
		"EventDestination.Enabled": {"true"},
		"EventDestination.MatchingEventTypes.member.1": {"Send"},
		"EventDestination.MatchingEventTypes.member.2": {"Bounce"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.Len(t, desc.EventDestinations, 1)
	assert.True(t, desc.EventDestinations[0].Enabled)
}

func TestBatch1_DeleteConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed-del",
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSetEventDestination"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"EventDestinationName": {"ed-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
}

// ---- TrackingOptions ----

func TestBatch1_CreateAndDeleteTrackingOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                               {"CreateConfigurationSetTrackingOptions"},
		"Version":                              {"2010-12-01"},
		"ConfigurationSetName":                 {"cs1"},
		"TrackingOptions.CustomRedirectDomain": {"tracking.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())

	rec = postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSetTrackingOptions"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())
}

func TestBatch1_UpdateConfigurationSetTrackingOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs1", "old.example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                               {"UpdateConfigurationSetTrackingOptions"},
		"Version":                              {"2010-12-01"},
		"ConfigurationSetName":                 {"cs1"},
		"TrackingOptions.CustomRedirectDomain": {"new.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.TrackingOptions)
	assert.Equal(t, "new.example.com", desc.TrackingOptions.CustomRedirectDomain)
}

// ---- ReceiptRuleSet CRUD ----

func TestBatch1_CreateReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":      {"CreateReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-new"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateReceiptRuleSetResponse")
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

func TestBatch1_CloneReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-original"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs-original", ses.ReceiptRule{Name: "r1", Enabled: true}, ""))

	rec := postForm(t, h, url.Values{
		"Action":              {"CloneReceiptRuleSet"},
		"Version":             {"2010-12-01"},
		"OriginalRuleSetName": {"rs-original"},
		"RuleSetName":         {"rs-clone"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())

	cloned, err := h.Backend.DescribeReceiptRuleSet("rs-clone")
	require.NoError(t, err)
	require.Len(t, cloned.Rules, 1)
	assert.Equal(t, "r1", cloned.Rules[0].Name)
}

func TestBatch1_DescribeReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-desc"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs-desc", ses.ReceiptRule{Name: "r1", Enabled: true}, ""))

	rec := postForm(t, h, url.Values{
		"Action":      {"DescribeReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-desc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "rs-desc")
	assert.Contains(t, rec.Body.String(), "r1")
}

func TestBatch1_DeleteReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-del"))

	rec := postForm(t, h, url.Values{
		"Action":      {"DeleteReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

func TestBatch1_SetActiveReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-active"))

	rec := postForm(t, h, url.Values{
		"Action":      {"SetActiveReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs-active"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "rs-active", h.Backend.(*ses.InMemoryBackend).ActiveRuleSet())
}

func TestBatch1_DescribeActiveReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs-a"))
	require.NoError(t, h.Backend.SetActiveReceiptRuleSet("rs-a"))

	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeActiveReceiptRuleSetResponse")
	assert.Contains(t, rec.Body.String(), "rs-a")
}

func TestBatch1_DescribeActiveReceiptRuleSet_None_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ListReceiptRuleSets_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs2"))

	rec := postForm(t, h, "Action=ListReceiptRuleSets&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListReceiptRuleSetsResponse")
}

// ---- ReceiptRule CRUD ----

func TestBatch1_CreateReceiptRule_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))

	rec := postForm(t, h, url.Values{
		"Action":                   {"CreateReceiptRule"},
		"Version":                  {"2010-12-01"},
		"RuleSetName":              {"rs1"},
		"Rule.Name":                {"rule1"},
		"Rule.Enabled":             {"true"},
		"Rule.ScanEnabled":         {"true"},
		"Rule.Recipients.member.1": {"user@example.com"},
		"Rule.TlsPolicy":           {"Require"},
		"Rule.Actions.member.1.SNSAction.TopicArn": {"arn:aws:sns:us-east-1:123:t"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rule, err := h.Backend.DescribeReceiptRule("rs1", "rule1")
	require.NoError(t, err)
	assert.Equal(t, "rule1", rule.Name)
	assert.True(t, rule.Enabled)
	assert.True(t, rule.ScanEnabled)
	assert.Equal(t, []string{"user@example.com"}, rule.Recipients)
	assert.Equal(t, "Require", rule.TLSPolicy)
	require.Len(t, rule.Actions, 1)
	assert.Equal(t, ses.ReceiptActionTypeSNS, rule.Actions[0].Type)
}

func TestBatch1_UpdateReceiptRule_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r1", Enabled: false}, ""))

	rec := postForm(t, h, url.Values{
		"Action":       {"UpdateReceiptRule"},
		"Version":      {"2010-12-01"},
		"RuleSetName":  {"rs1"},
		"Rule.Name":    {"r1"},
		"Rule.Enabled": {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rule, err := h.Backend.DescribeReceiptRule("rs1", "r1")
	require.NoError(t, err)
	assert.True(t, rule.Enabled)
}

func TestBatch1_DescribeReceiptRule_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r1", Enabled: true}, ""))

	rec := postForm(t, h, url.Values{
		"Action":      {"DescribeReceiptRule"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs1"},
		"RuleName":    {"r1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeReceiptRuleResponse")
}

func TestBatch1_DeleteReceiptRule_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r1"}, ""))

	rec := postForm(t, h, url.Values{
		"Action":      {"DeleteReceiptRule"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs1"},
		"RuleName":    {"r1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rs, err := h.Backend.DescribeReceiptRuleSet("rs1")
	require.NoError(t, err)
	assert.Empty(t, rs.Rules)
}

func TestBatch1_ReorderReceiptRuleSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r1"}, ""))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r2"}, ""))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r3"}, ""))

	rec := postForm(t, h, url.Values{
		"Action":             {"ReorderReceiptRuleSet"},
		"Version":            {"2010-12-01"},
		"RuleSetName":        {"rs1"},
		"RuleNames.member.1": {"r3"},
		"RuleNames.member.2": {"r1"},
		"RuleNames.member.3": {"r2"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rs, err := h.Backend.DescribeReceiptRuleSet("rs1")
	require.NoError(t, err)
	assert.Equal(t, []string{"r3", "r1", "r2"}, ruleNames(rs.Rules))
}

func TestBatch1_SetReceiptRulePosition_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs1"))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r1"}, ""))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r2"}, ""))
	require.NoError(t, h.Backend.CreateReceiptRule("rs1", ses.ReceiptRule{Name: "r3"}, ""))

	rec := postForm(t, h, url.Values{
		"Action":      {"SetReceiptRulePosition"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"rs1"},
		"RuleName":    {"r3"},
		"After":       {"r1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ReceiptRule_After_Parameter(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	// empty after="" prepends; successive prepends give [r2, r1]
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r1"}, ""))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r2"}, ""))
	// insert r3 after r1: [r2, r1, r3]
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r3"}, "r1"))

	rs, err := b.DescribeReceiptRuleSet("rs")
	require.NoError(t, err)
	names := ruleNames(rs.Rules)
	assert.Equal(t, "r2", names[0])
	assert.Equal(t, "r1", names[1])
	assert.Equal(t, "r3", names[2])
}

// ruleNames extracts rule names from a slice of ReceiptRule.
func ruleNames(rules []ses.ReceiptRule) []string {
	names := make([]string, len(rules))
	for i, r := range rules {
		names[i] = r.Name
	}

	return names
}

// ---- ReceiptFilters ----

func TestBatch1_CreateReceiptFilter_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                 {"CreateReceiptFilter"},
		"Version":                {"2010-12-01"},
		"Filter.Name":            {"my-filter"},
		"Filter.IpFilter.Policy": {"Allow"},
		"Filter.IpFilter.Cidr":   {"10.0.0.0/8"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).ReceiptFilterCount())
}

func TestBatch1_ListReceiptFilters_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	h.Backend.(*ses.InMemoryBackend).AddReceiptFilterInternal(ses.ReceiptFilter{
		Name:   "f1",
		Policy: "Allow",
		CIDR:   "10.0.0.0/8",
	})

	rec := postForm(t, h, "Action=ListReceiptFilters&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "f1")
}

func TestBatch1_DeleteReceiptFilter_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	h.Backend.(*ses.InMemoryBackend).AddReceiptFilterInternal(ses.ReceiptFilter{
		Name:   "del-f",
		Policy: "Block",
		CIDR:   "192.168.0.0/16",
	})

	rec := postForm(t, h, url.Values{
		"Action":     {"DeleteReceiptFilter"},
		"Version":    {"2010-12-01"},
		"FilterName": {"del-f"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).ReceiptFilterCount())
}

func TestBatch1_DeleteReceiptFilter_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":     {"DeleteReceiptFilter"},
		"Version":    {"2010-12-01"},
		"FilterName": {"nonexistent"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_ReceiptFilter_AllowAndBlock_Policies(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{
		Name:   "allow-filter",
		Policy: ses.FilterPolicyAllow,
		CIDR:   "10.0.0.0/8",
	}))
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{
		Name:   "block-filter",
		Policy: ses.FilterPolicyBlock,
		CIDR:   "192.168.0.0/16",
	}))

	filters := b.ListReceiptFilters()
	require.Len(t, filters, 2)

	policies := map[string]string{}
	for _, f := range filters {
		policies[f.Name] = f.Policy
	}
	assert.Equal(t, ses.FilterPolicyAllow, policies["allow-filter"])
	assert.Equal(t, ses.FilterPolicyBlock, policies["block-filter"])
}

// ---- GetSendQuota / GetSendStatistics ----

func TestBatch1_GetSendQuota_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=GetSendQuota&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendQuotaResponse")
	assert.Contains(t, rec.Body.String(), "Max24HourSend")
	assert.Contains(t, rec.Body.String(), "MaxSendRate")
}

func TestBatch1_GetSendQuota_Values(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	q := b.GetSendQuota()
	assert.Greater(t, q.Max24HourSend, 0.0)
	assert.Greater(t, q.MaxSendRate, 0.0)
	assert.GreaterOrEqual(t, q.SentLast24Hours, 0.0)
}

func TestBatch1_GetSendStatistics_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	_, err := h.Backend.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "test",
		BodyText: "body",
	})
	require.NoError(t, err)

	rec := postForm(t, h, "Action=GetSendStatistics&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendStatisticsResponse")
}

// ---- ReceiptRule Actions ----

func TestBatch1_ReceiptRule_S3Action(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	rule := ses.ReceiptRule{
		Name:    "s3-rule",
		Enabled: true,
		Actions: []ses.ReceiptAction{
			{
				Type:         ses.ReceiptActionTypeS3,
				S3BucketName: "my-bucket",
				S3KeyPrefix:  "emails/",
			},
		},
	}
	require.NoError(t, b.CreateReceiptRule("rs", rule, ""))

	described, err := b.DescribeReceiptRule("rs", "s3-rule")
	require.NoError(t, err)
	require.Len(t, described.Actions, 1)
	assert.Equal(t, ses.ReceiptActionTypeS3, described.Actions[0].Type)
	assert.Equal(t, "my-bucket", described.Actions[0].S3BucketName)
}

func TestBatch1_ReceiptRule_LambdaAction(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	rule := ses.ReceiptRule{
		Name:    "lambda-rule",
		Enabled: true,
		Actions: []ses.ReceiptAction{
			{
				Type:              ses.ReceiptActionTypeLambda,
				LambdaFunctionARN: "arn:aws:lambda:us-east-1:123:function:ProcessEmail",
			},
		},
	}
	require.NoError(t, b.CreateReceiptRule("rs", rule, ""))

	described, err := b.DescribeReceiptRule("rs", "lambda-rule")
	require.NoError(t, err)
	require.Len(t, described.Actions, 1)
	assert.Equal(t, ses.ReceiptActionTypeLambda, described.Actions[0].Type)
}

func TestBatch1_ReceiptRule_MultipleActions(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	rule := ses.ReceiptRule{
		Name:    "multi-action",
		Enabled: true,
		Actions: []ses.ReceiptAction{
			{Type: ses.ReceiptActionTypeSNS, SNSTopicARN: "arn:sns:topic"},
			{Type: ses.ReceiptActionTypeAddHeader, HeaderName: "X-Processed", HeaderValue: "true"},
			{Type: ses.ReceiptActionTypeS3, S3BucketName: "bucket"},
		},
	}
	require.NoError(t, b.CreateReceiptRule("rs", rule, ""))

	described, err := b.DescribeReceiptRule("rs", "multi-action")
	require.NoError(t, err)
	assert.Len(t, described.Actions, 3)
}

func TestBatch1_ReceiptRule_BounceAction(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	rule := ses.ReceiptRule{
		Name:    "bounce-rule",
		Enabled: true,
		Actions: []ses.ReceiptAction{
			{
				Type:          ses.ReceiptActionTypeBounce,
				SMTPReplyCode: "550",
				StatusCode:    "5.1.1",
				Message:       "User unknown",
				Sender:        "mailer-daemon@example.com",
			},
		},
	}
	require.NoError(t, b.CreateReceiptRule("rs", rule, ""))

	described, err := b.DescribeReceiptRule("rs", "bounce-rule")
	require.NoError(t, err)
	require.Len(t, described.Actions, 1)
	assert.Equal(t, ses.ReceiptActionTypeBounce, described.Actions[0].Type)
	assert.Equal(t, "550", described.Actions[0].SMTPReplyCode)
}

// ---- Snapshot / Restore with policies + account sending ----

func TestBatch1_Snapshot_IncludesPolicies(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("snap@example.com"))
	require.NoError(t, b.PutIdentityPolicy("snap@example.com", "pol1", `{"v":1}`))
	require.NoError(t, b.PutIdentityPolicy("snap@example.com", "pol2", `{"v":2}`))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	names, err := fresh.ListIdentityPolicies("snap@example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"pol1", "pol2"}, names)

	pols, err := fresh.GetIdentityPolicies("snap@example.com", nil)
	require.NoError(t, err)
	assert.Equal(t, `{"v":1}`, pols["pol1"])
	assert.Equal(t, `{"v":2}`, pols["pol2"])
}

func TestBatch1_Snapshot_IncludesAccountSending(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.UpdateAccountSendingEnabled(false)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))
	assert.False(t, fresh.GetAccountSendingEnabled())
}

func TestBatch1_Snapshot_FullRoundtrip(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.VerifyEmailIdentity("b.com"))
	require.NoError(t, b.PutIdentityPolicy("a@example.com", "p1", `{}`))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.CreateReceiptRuleSet("rs1"))
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f1", Policy: "Allow", CIDR: "10.0.0.0/8"}))
	b.UpdateAccountSendingEnabled(false)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	assert.Equal(t, 2, fresh.IdentityCount())
	assert.Equal(t, 1, fresh.PolicyCount())
	assert.Equal(t, 1, fresh.TemplateCount())
	assert.Equal(t, 1, fresh.ConfigSetCount())
	assert.Equal(t, 1, fresh.ReceiptRuleSetCount())
	assert.Equal(t, 1, fresh.ReceiptFilterCount())
	assert.False(t, fresh.GetAccountSendingEnabled())
}

// ---- Error cases ----

func TestBatch1_PutIdentityPolicy_MissingPolicyName_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	assert.Error(t, b.PutIdentityPolicy("a@example.com", "", `{}`))
}

func TestBatch1_CreateReceiptRuleSet_Duplicate_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	assert.Error(t, b.CreateReceiptRuleSet("rs"))
}

func TestBatch1_DescribeReceiptRule_NotFound_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	_, err := b.DescribeReceiptRule("rs", "nonexistent")
	assert.Error(t, err)
}

func TestBatch1_UpdateReceiptRule_NotFound_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	assert.Error(t, b.UpdateReceiptRule("rs", ses.ReceiptRule{Name: "nonexistent"}))
}

func TestBatch1_ReorderReceiptRuleSet_WrongCount_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r1"}, ""))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r2"}, ""))
	assert.Error(t, b.ReorderReceiptRuleSet("rs", []string{"r1"}))
}

func TestBatch1_VerifyEmailIdentity_AlreadyExists_Idempotent(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityDkimEnabled("a@example.com", true))
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))

	attrs := b.GetIdentityDkimAttributes([]string{"a@example.com"})
	assert.True(t, attrs["a@example.com"].DkimEnabled, "existing attributes preserved on re-verify")
}

func TestBatch1_SendEmail_Tags_ConfigSet(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	tags := []ses.Tag{{Name: "env", Value: "prod"}, {Name: "team", Value: "backend"}}
	msgID, err := b.SendEmail(ses.SendEmailInput{
		From:                 "s@example.com",
		To:                   []string{"to@example.com"},
		Subject:              "tagged",
		BodyText:             "body",
		ConfigurationSetName: "my-cs",
		Tags:                 tags,
	})
	require.NoError(t, err)

	email, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, "my-cs", email.ConfigurationSetName)
	assert.Equal(t, tags, email.Tags)
}

func TestBatch1_SendEmail_SearchByFrom(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("alice@example.com"))
	require.NoError(t, b.VerifyEmailIdentity("bob@example.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "alice@example.com", To: []string{"x@example.com"},
		Subject: "A", BodyText: "a",
	})
	require.NoError(t, err)
	_, err = b.SendEmail(ses.SendEmailInput{
		From: "bob@example.com", To: []string{"x@example.com"},
		Subject: "B", BodyText: "b",
	})
	require.NoError(t, err)

	results := b.SearchEmails("alice")
	assert.Len(t, results, 1)
	assert.Equal(t, "alice@example.com", results[0].From)
}

func TestBatch1_ListIdentities_IncludesBothEmailsAndDomains(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("user@example.com"))
	_, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)

	p := b.ListIdentities("", 100)
	assert.Contains(t, p.Data, "user@example.com")
	assert.Contains(t, p.Data, "example.com")
}

func TestBatch1_IdentityVerification_EmailVsDomain(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("user@example.com"))
	_, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)

	attrs := b.GetIdentityVerificationAttributes([]string{"user@example.com", "example.com"})
	assert.Equal(t, "Success", attrs["user@example.com"])
	assert.Equal(t, "Success", attrs["example.com"])
}

func TestBatch1_Reset_ClearsPoliciesAndAccountSending(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.PutIdentityPolicy("a@example.com", "p1", `{}`))
	b.UpdateAccountSendingEnabled(false)

	b.Reset()

	assert.Equal(t, 0, b.PolicyCount())
	assert.True(t, b.GetAccountSendingEnabled())
	assert.Equal(t, 0, b.IdentityCount())
}

func TestBatch1_AllSupportedOpsListed(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()
	assert.GreaterOrEqual(t, len(ops), 50, "must support 50+ SES operations")
}
