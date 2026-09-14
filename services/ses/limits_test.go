package ses_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestLimitExceeded proves gopherstack-ssk: each Create*/Clone*/Update* op
// whose own error switch declares LimitExceededException
// (aws-sdk-go-v2/service/ses@v1.37.4/deserializers.go, per-op citations in
// limits.go) now enforces its real cap and returns ErrLimitExceeded once
// reached. Every backend uses WithResourceLimits to shrink the relevant cap
// to a small number instead of creating the real (up to 20,000/10,000)
// count of resources.
func TestLimitExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *ses.InMemoryBackend)
		trip  func(b *ses.InMemoryBackend) error
		name  string
	}{
		{
			name: "receipt_rule_sets_per_account",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ReceiptRuleSets: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs-1"))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateReceiptRuleSet("rs-2")
			},
		},
		{
			name: "receipt_rule_sets_per_account_via_clone",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ReceiptRuleSets: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs-1"))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CloneReceiptRuleSet("rs-1", "rs-1-clone")
			},
		},
		{
			name: "rules_per_receipt_rule_set",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{RulesPerRuleSet: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs"))
				require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r1"}, ""))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r2"}, "")
			},
		},
		{
			name: "actions_per_receipt_rule_on_create",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ActionsPerRule: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs"))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateReceiptRule("rs", ses.ReceiptRule{
					Name: "r1",
					Actions: []ses.ReceiptAction{
						{Type: ses.ReceiptActionTypeStop},
						{Type: ses.ReceiptActionTypeStop},
					},
				}, "")
			},
		},
		{
			name: "actions_per_receipt_rule_on_update",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ActionsPerRule: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs"))
				require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r1"}, ""))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.UpdateReceiptRule("rs", ses.ReceiptRule{
					Name: "r1",
					Actions: []ses.ReceiptAction{
						{Type: ses.ReceiptActionTypeStop},
						{Type: ses.ReceiptActionTypeStop},
					},
				})
			},
		},
		{
			name: "recipients_per_receipt_rule",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{RecipientsPerRule: 1})
				require.NoError(t, b.CreateReceiptRuleSet("rs"))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateReceiptRule("rs", ses.ReceiptRule{
					Name:       "r1",
					Recipients: []string{"a@example.com", "b@example.com"},
				}, "")
			},
		},
		{
			name: "receipt_filters_per_account",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ReceiptFilters: 1})
				require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f1", CIDR: "10.0.0.0/8"}))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f2", CIDR: "10.0.0.0/8"})
			},
		},
		{
			name: "configuration_sets",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{ConfigurationSets: 1})
				require.NoError(t, b.CreateConfigurationSet("cs-1"))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateConfigurationSet("cs-2")
			},
		},
		{
			name: "event_destinations_per_configuration_set",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{EventDestinationsPerSet: 1})
				require.NoError(t, b.CreateConfigurationSet("cs"))
				require.NoError(t, b.CreateConfigurationSetEventDestination("cs", ses.EventDestination{
					Name: "d1", MatchingEventTypes: []string{"bounce"},
				}))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateConfigurationSetEventDestination("cs", ses.EventDestination{
					Name: "d2", MatchingEventTypes: []string{"bounce"},
				})
			},
		},
		{
			name: "email_templates",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{EmailTemplates: 1})
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1", SubjectPart: "s"}))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateTemplate(ses.EmailTemplate{TemplateName: "t2", SubjectPart: "s"})
			},
		},
		{
			name: "custom_verification_email_templates",
			setup: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(ses.ResourceLimits{CustomVerificationTemplates: 1})
				require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
					TemplateName: "t1", FromEmailAddress: "s@example.com", TemplateSubject: "s",
					TemplateContent: "c", SuccessRedirectionURL: "https://ok", FailureRedirectionURL: "https://no",
				}))
			},
			trip: func(b *ses.InMemoryBackend) error {
				return b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
					TemplateName: "t2", FromEmailAddress: "s@example.com", TemplateSubject: "s",
					TemplateContent: "c", SuccessRedirectionURL: "https://ok", FailureRedirectionURL: "https://no",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			tt.setup(t, b)

			err := tt.trip(b)
			require.Error(t, err)
			assert.ErrorIs(t, err, ses.ErrLimitExceeded)
		})
	}
}

// TestLimitExceeded_HTTPWireShape proves the HTTP-level error shape: real
// AWS SES's LimitExceededException.ErrorCode() (types/errors.go) returns
// the unsuffixed "LimitExceeded" wire code.
func TestLimitExceeded_HTTPWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	backend, ok := h.Backend.(*ses.InMemoryBackend)
	require.True(t, ok)
	backend.WithResourceLimits(ses.ResourceLimits{ConfigurationSets: 1})

	require.NoError(t, backend.CreateConfigurationSet("cs-1"))

	rec := postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=cs-2")
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>LimitExceeded</Code>")
}

// TestWithResourceLimits_SurvivesReset proves an override set via
// WithResourceLimits persists across Reset(), matching WithEmailTTL's
// configuredEmailTTL precedent (store.go).
func TestWithResourceLimits_SurvivesReset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend().WithResourceLimits(ses.ResourceLimits{ConfigurationSets: 1})
	require.NoError(t, b.CreateConfigurationSet("cs-1"))

	b.Reset()

	require.NoError(t, b.CreateConfigurationSet("cs-1"))
	err := b.CreateConfigurationSet("cs-2")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrLimitExceeded)
}
