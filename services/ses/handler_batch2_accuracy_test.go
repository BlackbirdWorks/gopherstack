package ses_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newBatch2Handler(t *testing.T) *ses.Handler {
	t.Helper()
	return ses.NewHandler(ses.NewInMemoryBackend())
}

func doBatch2(t *testing.T, h *ses.Handler, vals url.Values) *xmlDoc {
	t.Helper()
	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	var doc xmlDoc
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &doc))
	return &doc
}

// xmlDoc is a flexible XML unmarshaller for response inspection.
type xmlDoc struct {
	XMLName xml.Name
	Attrs   []xml.Attr `xml:",any,attr"`
	Content []byte     `xml:",innerxml"`
}

// text extracts all character data from a simple nested XML path.
// path is a sequence of element names: text("Foo", "Bar") finds <Foo><Bar>...</Bar></Foo>.
func xmlText(data []byte, path ...string) string {
	for _, tag := range path {
		start := "<" + tag + ">"
		end := "</" + tag + ">"
		si := strings.Index(string(data), start)
		if si < 0 {
			return ""
		}
		si += len(start)
		ei := strings.Index(string(data)[si:], end)
		if ei < 0 {
			return ""
		}
		data = []byte(string(data)[si : si+ei])
	}
	return string(data)
}

func xmlContains(data []byte, tag string) bool {
	return strings.Contains(string(data), "<"+tag) || strings.Contains(string(data), "<"+tag+"/>")
}

func sesVerifyAndSend(t *testing.T, h *ses.Handler, from, to string) {
	t.Helper()
	require.NoError(t, h.Backend.VerifyEmailIdentity(from))
	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {from},
		"Destination.ToAddresses.member.1": {to},
		"Message.Subject.Data":             {"subj"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// ── MessageId shape ────────────────────────────────────────────────────────────

func TestSES_Batch2_SendEmail_MessageIdNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		extra  url.Values
	}{
		{
			name:   "SendEmail",
			action: "SendEmail",
			extra: url.Values{
				"Message.Subject.Data":   {"subj"},
				"Message.Body.Text.Data": {"body"},
			},
		},
		{
			name:   "SendRawEmail",
			action: "SendRawEmail",
			extra: url.Values{
				"RawMessage.Data": {"From: s@example.com\r\nTo: t@example.com\r\nSubject: s\r\n\r\nbody"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

			vals := url.Values{
				"Action":  {tt.action},
				"Version": {"2010-12-01"},
				"Source":  {"s@example.com"},
				"Destination.ToAddresses.member.1": {"t@example.com"},
			}
			for k, v := range tt.extra {
				vals[k] = v
			}

			rec := postForm(t, h, vals.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<MessageId>", "response must contain MessageId element")

			msgID := xmlText([]byte(body), "MessageId")
			assert.NotEmpty(t, msgID, "MessageId must not be empty")
			assert.True(t, strings.HasPrefix(msgID, "ses-"),
				"MessageId must start with 'ses-' prefix, got: %q", msgID)
		})
	}
}

// ── MessageId uniqueness ───────────────────────────────────────────────────────

func TestSES_Batch2_SendEmail_MessageIdUnique(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	seen := make(map[string]bool)
	for i := range 5 {
		vals := url.Values{
			"Action":                           {"SendEmail"},
			"Version":                          {"2010-12-01"},
			"Source":                           {"s@example.com"},
			"Destination.ToAddresses.member.1": {"t@example.com"},
			"Message.Subject.Data":             {"subj"},
			"Message.Body.Text.Data":           {"body"},
		}
		rec := postForm(t, h, vals.Encode())
		require.Equal(t, http.StatusOK, rec.Code, "send %d must succeed", i)

		msgID := xmlText(rec.Body.Bytes(), "MessageId")
		require.NotEmpty(t, msgID)
		assert.False(t, seen[msgID], "MessageId must be unique across sends, got duplicate: %q", msgID)
		seen[msgID] = true
	}
}

// ── GetSendQuota: values ───────────────────────────────────────────────────────

func TestSES_Batch2_GetSendQuota_FixedValues(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendQuota"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<Max24HourSend>200</Max24HourSend>",
		"Max24HourSend must be 200")
	assert.Contains(t, body, "<MaxSendRate>1</MaxSendRate>",
		"MaxSendRate must be 1")
}

// ── GetSendQuota: SentLast24Hours tracks sent emails ─────────────────────────

func TestSES_Batch2_GetSendQuota_SentLast24HoursTracked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		numSend int
		want    string
	}{
		{name: "zero_sends", numSend: 0, want: "<SentLast24Hours>0</SentLast24Hours>"},
		{name: "one_send", numSend: 1, want: "<SentLast24Hours>1</SentLast24Hours>"},
		{name: "three_sends", numSend: 3, want: "<SentLast24Hours>3</SentLast24Hours>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

			for i := range tt.numSend {
				rec := postForm(t, h, url.Values{
					"Action":                           {"SendEmail"},
					"Version":                          {"2010-12-01"},
					"Source":                           {"s@example.com"},
					"Destination.ToAddresses.member.1": {"t@example.com"},
					"Message.Subject.Data":             {"subj"},
					"Message.Body.Text.Data":           {"body"},
				}.Encode())
				require.Equal(t, http.StatusOK, rec.Code, "send %d must succeed", i)
			}

			rec := postForm(t, h, url.Values{
				"Action":  {"GetSendQuota"},
				"Version": {"2010-12-01"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

// ── GetSendStatistics: empty state ────────────────────────────────────────────

func TestSES_Batch2_GetSendStatistics_EmptyState(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetSendStatisticsResponse",
		"response envelope must be GetSendStatisticsResponse")
	assert.Contains(t, body, "SendDataPoints",
		"SendDataPoints element must be present even when empty")
	assert.NotContains(t, body, "<member>",
		"no member elements expected when no emails sent")
}

// ── GetSendStatistics: populated after send ───────────────────────────────────

func TestSES_Batch2_GetSendStatistics_PopulatedAfterSend(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	sesVerifyAndSend(t, h, "s@example.com", "t@example.com")
	sesVerifyAndSend(t, h, "s@example.com", "t2@example.com")

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<member>", "must have at least one SendDataPoints member after sends")
	assert.Contains(t, body, "<DeliveryAttempts>", "member must contain DeliveryAttempts")
}

// ── GetSendStatistics: Timestamp is RFC3339 ───────────────────────────────────

func TestSES_Batch2_GetSendStatistics_TimestampRFC3339(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	sesVerifyAndSend(t, h, "s@example.com", "t@example.com")

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	ts := xmlText(rec.Body.Bytes(), "Timestamp")
	require.NotEmpty(t, ts, "Timestamp must be present in SendDataPoints member")
	_, err := time.Parse(time.RFC3339, ts)
	assert.NoError(t, err, "Timestamp must be RFC3339: %q", ts)
}

// ── ListIdentities: empty state ───────────────────────────────────────────────

func TestSES_Batch2_ListIdentities_EmptyState(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":  {"ListIdentities"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ListIdentitiesResponse",
		"response envelope must be ListIdentitiesResponse")
	assert.Contains(t, body, "Identities",
		"Identities element must be present even when empty")
	assert.NotContains(t, body, "<member>",
		"no member elements expected when no identities exist")
}

// ── ListIdentities: NextToken absent when results fit on one page ─────────────

func TestSES_Batch2_ListIdentities_NextTokenAbsentWhenNotTruncated(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, h.Backend.VerifyEmailIdentity("b@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":  {"ListIdentities"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	assert.NotContains(t, rec.Body.String(), "<NextToken>",
		"NextToken must be absent when all results fit on one page")
}

// ── ListTemplates: NextToken absent when not truncated ─────────────────────────

func TestSES_Batch2_ListTemplates_NextTokenAbsentWhenNotTruncated(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t1", SubjectPart: "s", TextPart: "body",
	}))

	rec := postForm(t, h, url.Values{
		"Action":  {"ListTemplates"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	assert.NotContains(t, rec.Body.String(), "<NextToken>",
		"NextToken must be absent when all templates fit on one page")
}

// ── ListConfigurationSets: empty + NextToken absent ────────────────────────────

func TestSES_Batch2_ListConfigurationSets_EmptyAndNextTokenAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		numSets  int
	}{
		{name: "empty_state", numSets: 0},
		{name: "two_sets", numSets: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			for i := range tt.numSets {
				require.NoError(t, h.Backend.CreateConfigurationSet(
					strings.Repeat("cs", i+1),
				))
			}

			rec := postForm(t, h, url.Values{
				"Action":  {"ListConfigurationSets"},
				"Version": {"2010-12-01"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "ConfigurationSets",
				"ConfigurationSets element must be present")
			assert.NotContains(t, body, "<NextToken>",
				"NextToken must be absent when all config sets fit on one page")
		})
	}
}

// ── ForwardingEnabled defaults to true ────────────────────────────────────────

func TestSES_Batch2_GetIdentityNotificationAttributes_ForwardingEnabledDefaultTrue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		setup    func(b *ses.InMemoryBackend) error
	}{
		{
			name:     "email_identity_defaults_forwarding_true",
			identity: "a@example.com",
			setup:    func(b *ses.InMemoryBackend) error { return b.VerifyEmailIdentity("a@example.com") },
		},
		{
			name:     "domain_identity_defaults_forwarding_true",
			identity: "example.com",
			setup: func(b *ses.InMemoryBackend) error {
				_, err := b.VerifyDomainIdentity("example.com")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			require.NoError(t, tt.setup(h.Backend.(*ses.InMemoryBackend)))

			rec := postForm(t, h, url.Values{
				"Action":                    {"GetIdentityNotificationAttributes"},
				"Version":                   {"2010-12-01"},
				"Identities.member.1":       {tt.identity},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Contains(t, rec.Body.String(), "<ForwardingEnabled>true</ForwardingEnabled>",
				"ForwardingEnabled must default to true for newly verified identity")
		})
	}
}

// ── GetIdentityDkimAttributes: email without DKIM setup ──────────────────────

func TestSES_Batch2_GetIdentityDkimAttributes_EmailIdentityNotStarted(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.VerifyEmailIdentity("a@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"a@example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetIdentityDkimAttributesResponse")
	assert.Contains(t, body, "<DkimVerificationStatus>NotStarted</DkimVerificationStatus>",
		"email identity without DKIM setup must have NotStarted status")
}

// ── GetIdentityDkimAttributes: domain after VerifyDomainDkim ─────────────────

func TestSES_Batch2_GetIdentityDkimAttributes_DomainAfterVerifyDkim(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	tokens, err := h.Backend.(*ses.InMemoryBackend).VerifyDomainDkim("example.com")
	require.NoError(t, err)
	require.Len(t, tokens, 3, "VerifyDomainDkim must return 3 tokens")

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<DkimVerificationStatus>Success</DkimVerificationStatus>",
		"domain after VerifyDomainDkim must have Success status")
	assert.Contains(t, body, "<DkimTokens>", "DkimTokens must be present")
}

// ── DescribeConfigurationSet: EventDestinations empty not absent ──────────────

func TestSES_Batch2_DescribeConfigurationSet_EventDestinationsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EventDestinations",
		"EventDestinations must be present in DescribeConfigurationSet even when empty")
	assert.NotContains(t, body, "<MatchingEventTypes>",
		"no MatchingEventTypes expected when config set has no event destinations")
}

// ── DescribeConfigurationSet: ReputationOptions present ──────────────────────

func TestSES_Batch2_DescribeConfigurationSet_ReputationOptionsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		sendingEnabled   bool
		metricsEnabled   bool
		wantSending      string
		wantMetrics      string
	}{
		{
			name:           "defaults",
			sendingEnabled: true,
			metricsEnabled: false,
			wantSending:    "<SendingEnabled>true</SendingEnabled>",
			wantMetrics:    "<ReputationMetricsEnabled>false</ReputationMetricsEnabled>",
		},
		{
			name:           "sending_disabled",
			sendingEnabled: false,
			metricsEnabled: true,
			wantSending:    "<SendingEnabled>false</SendingEnabled>",
			wantMetrics:    "<ReputationMetricsEnabled>true</ReputationMetricsEnabled>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

			if !tt.sendingEnabled {
				require.NoError(t, h.Backend.UpdateConfigurationSetSendingEnabled("cs1", false))
			}
			if tt.metricsEnabled {
				require.NoError(t, h.Backend.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))
			}

			rec := postForm(t, h, url.Values{
				"Action":               {"DescribeConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"cs1"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<ReputationOptions>", "ReputationOptions must be present")
			assert.Contains(t, body, tt.wantSending)
			assert.Contains(t, body, tt.wantMetrics)
		})
	}
}

// ── SendBulkTemplatedEmail: per-destination MessageId and Status ──────────────

func TestSES_Batch2_SendBulkTemplatedEmail_PerDestinationStatus(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t", SubjectPart: "subj", TextPart: "body",
	}))

	rec := postForm(t, h, url.Values{
		"Action":   {"SendBulkTemplatedEmail"},
		"Version":  {"2010-12-01"},
		"Source":   {"s@example.com"},
		"Template": {"t"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "SendBulkTemplatedEmailResponse")
	assert.Contains(t, body, "<Status>", "each destination must have a Status element")
	assert.Contains(t, body, "<MessageId>", "each destination must have a MessageId element")
	assert.Contains(t, body, "Success", "status must be Success for valid sends")
}

// ── Domain-level verification allows subdomain senders ───────────────────────

func TestSES_Batch2_SendEmail_DomainVerificationAllowsSubAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sender string
	}{
		{name: "direct_address", sender: "user@example.com"},
		{name: "subdomain_address", sender: "other@example.com"},
		{name: "plus_address", sender: "user+tag@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			h := ses.NewHandler(b)

			_, err := b.VerifyDomainIdentity("example.com")
			require.NoError(t, err)

			rec := postForm(t, h, url.Values{
				"Action":                           {"SendEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {tt.sender},
				"Destination.ToAddresses.member.1": {"dest@other.com"},
				"Message.Subject.Data":             {"hi"},
				"Message.Body.Text.Data":           {"body"},
			}.Encode())
			assert.Equal(t, http.StatusOK, rec.Code,
				"sender %q under verified domain must be allowed to send", tt.sender)
		})
	}
}

// ── SendEmail: unverified sender returns MessageRejected ──────────────────────

func TestSES_Batch2_SendEmail_UnverifiedSenderRejected(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"unverified@example.com"},
		"Destination.ToAddresses.member.1": {"dest@other.com"},
		"Message.Subject.Data":             {"hi"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"sending from unverified address must be rejected")
	assert.Contains(t, rec.Body.String(), "MessageRejected",
		"error code must be MessageRejected")
}

// ── CloneReceiptRuleSet is a deep copy ─────────────────────────────────────────

func TestSES_Batch2_CloneReceiptRuleSet_IsDeepCopy(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.CreateReceiptRuleSet("original"))
	require.NoError(t, h.Backend.CreateReceiptRule("original", ses.ReceiptRule{Name: "rule1", Enabled: true}, ""))

	// clone
	rec := postForm(t, h, url.Values{
		"Action":               {"CloneReceiptRuleSet"},
		"Version":              {"2010-12-01"},
		"RuleSetName":          {"clone"},
		"OriginalRuleSetName":  {"original"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// add rule to clone only
	require.NoError(t, h.Backend.CreateReceiptRule("clone", ses.ReceiptRule{Name: "rule2", Enabled: true}, ""))

	// original must still have exactly 1 rule
	orig, err := h.Backend.DescribeReceiptRuleSet("original")
	require.NoError(t, err)
	assert.Len(t, orig.Rules, 1, "original must still have 1 rule after modifying clone")

	// clone must have 2 rules
	cloned, err := h.Backend.DescribeReceiptRuleSet("clone")
	require.NoError(t, err)
	assert.Len(t, cloned.Rules, 2, "clone must have 2 rules")
}

// ── DescribeReceiptRuleSet: empty rules list not null ─────────────────────────

func TestSES_Batch2_DescribeReceiptRuleSet_EmptyRulesNotAbsent(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	require.NoError(t, h.Backend.CreateReceiptRuleSet("empty-rs"))

	rec := postForm(t, h, url.Values{
		"Action":      {"DescribeReceiptRuleSet"},
		"Version":     {"2010-12-01"},
		"RuleSetName": {"empty-rs"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "DescribeReceiptRuleSetResponse")
	assert.Contains(t, body, "Rules",
		"Rules element must be present even when rule set has no rules")
}

// ── ListReceiptRuleSets: empty state ──────────────────────────────────────────

func TestSES_Batch2_ListReceiptRuleSets_EmptyState(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":  {"ListReceiptRuleSets"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ListReceiptRuleSetsResponse")
	assert.Contains(t, body, "RuleSets",
		"RuleSets element must be present even when empty")
}

// ── ListReceiptFilters: empty state ───────────────────────────────────────────

func TestSES_Batch2_ListReceiptFilters_EmptyState(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":  {"ListReceiptFilters"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ListReceiptFiltersResponse")
	assert.Contains(t, body, "Filters",
		"Filters element must be present even when empty")
}

// ── RequestId present in every response ───────────────────────────────────────

func TestSES_Batch2_ResponseMetadata_RequestIdPresent(t *testing.T) {
	t.Parallel()

	ops := []url.Values{
		{
			"Action":  {"ListIdentities"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"GetSendQuota"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"GetSendStatistics"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"ListConfigurationSets"},
			"Version": {"2010-12-01"},
		},
	}

	for _, vals := range ops {
		t.Run(vals.Get("Action"), func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler(t)
			rec := postForm(t, h, vals.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "<RequestId>",
				"every response must contain a RequestId in ResponseMetadata")
		})
	}
}

// ── GetIdentityVerificationAttributes: unknown identity returns NotStarted ────

func TestSES_Batch2_GetIdentityVerificationAttributes_UnknownReturnsNotStarted(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityVerificationAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"unknown@example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<VerificationStatus>NotStarted</VerificationStatus>",
		"unknown identity must return NotStarted verification status")
}

// ── GetSendStatistics: aggregates multiple sends into hourly bucket ────────────

func TestSES_Batch2_GetSendStatistics_AggregatesIntoHourlyBuckets(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	h := ses.NewHandler(b)
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	// Send 3 emails
	for range 3 {
		rec := postForm(t, h, url.Values{
			"Action":                           {"SendEmail"},
			"Version":                          {"2010-12-01"},
			"Source":                           {"s@example.com"},
			"Destination.ToAddresses.member.1": {"t@example.com"},
			"Message.Subject.Data":             {"subj"},
			"Message.Body.Text.Data":           {"body"},
		}.Encode())
		require.Equal(t, http.StatusOK, rec.Code)
	}

	stats := b.GetSendStatistics()
	require.Len(t, stats, 1, "3 emails in same hour must produce exactly 1 hourly bucket")
	assert.Equal(t, float64(3), stats[0].DeliveryAttempts,
		"hourly bucket DeliveryAttempts must equal number of emails sent")
}
