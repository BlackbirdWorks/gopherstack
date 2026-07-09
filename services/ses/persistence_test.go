package ses_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ses.InMemoryBackend) string
		verify func(t *testing.T, b *ses.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ses.InMemoryBackend) string {
				err := b.VerifyEmailIdentity("test@example.com")
				if err != nil {
					return ""
				}

				return "test@example.com"
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend, id string) {
				t.Helper()

				identities := b.ListIdentities("", 0).Data
				require.Len(t, identities, 1)
				assert.Equal(t, id, identities[0])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ses.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ses.InMemoryBackend, _ string) {
				t.Helper()

				identities := b.ListIdentities("", 0).Data
				assert.Empty(t, identities)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ses.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ses.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := ses.NewHandler(ses.NewInMemoryBackend())
	require.NoError(t, h.Backend.VerifyEmailIdentity("delegate@test.com"))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := ses.NewHandler(ses.NewInMemoryBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))

	identities := h2.Backend.ListIdentities("", 0).Data
	require.Len(t, identities, 1)
	assert.Equal(t, "delegate@test.com", identities[0])
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched, including the "dirty" tables (identities, configSets,
// trackingOptions, eventDestinations) whose live value type deliberately
// excludes its own identity field from JSON (json:"-") and instead relies on
// a DTO type in persistence.go to carry it through the round trip -- this is
// exactly the class of bug (identity silently dropped on restore) a
// same-named-fields-only test would miss.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := ses.NewInMemoryBackend().WithRegion("us-west-2").WithAccountID("999999999999")

	require.NoError(t, original.VerifyEmailIdentity("verified@example.com"))
	require.NoError(t, original.SetIdentityMailFromDomain("verified@example.com", "mail.example.com", ""))
	require.NoError(t, original.SetIdentityDkimEnabled("verified@example.com", true))

	require.NoError(t, original.CreateTemplate(ses.EmailTemplate{
		TemplateName: "tmpl1", SubjectPart: "Subj", TextPart: "Text", HTMLPart: "<p>HTML</p>",
	}))

	require.NoError(t, original.CreateConfigurationSet("cs1"))
	require.NoError(t, original.PutConfigurationSetDeliveryOptions("cs1", ses.TLSPolicyRequire))
	require.NoError(t, original.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))
	require.NoError(t, original.UpdateConfigurationSetSendingEnabled("cs1", false))

	require.NoError(t, original.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name: "dest1", SNSTopicARN: "arn:aws:sns:us-west-2:999999999999:topic", Enabled: true,
		MatchingEventTypes: []string{"send", "bounce"},
	}))

	require.NoError(t, original.CreateConfigurationSetTrackingOptions("cs1", "track.example.com"))

	require.NoError(t, original.CreateReceiptRuleSet("rs1"))
	require.NoError(t, original.CreateReceiptRule("rs1", ses.ReceiptRule{
		Name: "rule1", Enabled: true, TLSPolicy: ses.TLSPolicyOptional, Recipients: []string{"a@example.com"},
	}, ""))
	require.NoError(t, original.SetActiveReceiptRuleSet("rs1"))

	require.NoError(t, original.CreateReceiptFilter(ses.ReceiptFilter{
		Name: "filter1", Policy: ses.FilterPolicyBlock, CIDR: "10.0.0.0/8",
	}))

	require.NoError(t, original.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName: "cvt1", FromEmailAddress: "verify@example.com", TemplateSubject: "Verify",
		TemplateContent: "click here", SuccessRedirectionURL: "https://ok", FailureRedirectionURL: "https://fail",
	}))

	require.NoError(t, original.PutIdentityPolicy("verified@example.com", "policy1", `{"Version":"2012-10-17"}`))

	original.UpdateAccountSendingEnabled(false)

	_, err := original.SendEmail(ses.SendEmailInput{
		From: "verified@example.com", To: []string{"to@example.com"}, Subject: "Hi", BodyText: "body",
	})
	require.Error(t, err) // sending is paused above; the email should NOT be recorded

	original.UpdateAccountSendingEnabled(true)

	_, err = original.SendEmail(ses.SendEmailInput{
		From: "verified@example.com", To: []string{"to@example.com"}, Subject: "Hi", BodyText: "body",
	})
	require.NoError(t, err)

	original.UpdateAccountSendingEnabled(false) // exercise a non-default bool value across the round trip

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, "us-west-2", fresh.Region())
	assert.Equal(t, "999999999999", fresh.AccountID())
	assert.False(t, fresh.GetAccountSendingEnabled())

	mailFrom := fresh.GetIdentityMailFromDomainAttributes([]string{"verified@example.com"})["verified@example.com"]
	assert.Equal(t, "mail.example.com", mailFrom.MailFromDomain)

	dkim := fresh.GetIdentityDkimAttributes([]string{"verified@example.com"})["verified@example.com"]
	assert.True(t, dkim.DkimEnabled)

	tmpl, err := fresh.GetTemplate("tmpl1")
	require.NoError(t, err)
	assert.Equal(t, "Subj", tmpl.SubjectPart)

	csDesc, err := fresh.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, csDesc.DeliveryOptions)
	assert.Equal(t, ses.TLSPolicyRequire, csDesc.DeliveryOptions.TLSPolicy)
	assert.True(t, csDesc.ReputationMetricsEnabled)
	assert.False(t, csDesc.SendingEnabled)
	require.Len(t, csDesc.EventDestinations, 1)
	assert.Equal(t, "dest1", csDesc.EventDestinations[0].Name)
	assert.ElementsMatch(t, []string{"send", "bounce"}, csDesc.EventDestinations[0].MatchingEventTypes)
	require.NotNil(t, csDesc.TrackingOptions)
	assert.Equal(t, "track.example.com", csDesc.TrackingOptions.CustomRedirectDomain)

	rs, err := fresh.DescribeReceiptRuleSet("rs1")
	require.NoError(t, err)
	require.Len(t, rs.Rules, 1)
	assert.Equal(t, "rule1", rs.Rules[0].Name)
	assert.Equal(t, "rs1", fresh.ActiveRuleSet())

	filters := fresh.ListReceiptFilters()
	require.Len(t, filters, 1)
	assert.Equal(t, "10.0.0.0/8", filters[0].CIDR)

	cvt, err := fresh.GetCustomVerificationEmailTemplate("cvt1")
	require.NoError(t, err)
	assert.Equal(t, "verify@example.com", cvt.FromEmailAddress)

	policies, err := fresh.GetIdentityPolicies("verified@example.com", nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policies["policy1"])

	emails := fresh.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "Hi", emails[0].Subject)

	emailByID, err := fresh.GetEmailByID(emails[0].MessageID)
	require.NoError(t, err)
	assert.Equal(t, emails[0].MessageID, emailByID.MessageID)
}

func TestInMemoryBackend_RestorePreservesEmails(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("persist@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "persist@test.com", To: []string{"to@test.com"}, Subject: "Test", BodyText: "body",
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	emails := fresh.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "persist@test.com", emails[0].From)
	assert.Equal(t, "Test", emails[0].Subject)
}
