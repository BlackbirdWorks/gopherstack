package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSESNewOps_CreateReceiptRuleSet covers the CreateReceiptRuleSet handler.
func TestSESNewOps_CreateReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=my-rules",
			wantCode:     http.StatusOK,
			wantContains: "CreateReceiptRuleSetResponse",
		},
		{
			name:         "duplicate_returns_error",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=existing",
			setup:        func(h *ses.Handler) { require.NoError(t, h.Backend.CreateReceiptRuleSet("existing")) },
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name:         "empty_name_returns_error",
			body:         "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CloneReceiptRuleSet covers the CloneReceiptRuleSet handler.
func TestSESNewOps_CloneReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=source&RuleSetName=clone",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("source"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CloneReceiptRuleSetResponse",
		},
		{
			name:         "source_not_found",
			body:         "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=nonexistent&RuleSetName=clone",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name: "destination_already_exists",
			body: "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=src&RuleSetName=existing",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("src"))
				require.NoError(t, h.Backend.CreateReceiptRuleSet("existing"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name:         "empty_original_name",
			body:         "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=&RuleSetName=clone",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CloneReceiptRuleSet_CopiesRules verifies that cloning copies rules.
func TestSESNewOps_CloneReceiptRuleSet_CopiesRules(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("source"))
	require.NoError(t, h.Backend.CreateReceiptRule("source", ses.ReceiptRule{
		Name:    "rule1",
		Enabled: true,
	}, ""))

	rec := postForm(t, h, "Action=CloneReceiptRuleSet&Version=2010-12-01&OriginalRuleSetName=source&RuleSetName=clone")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 2, h.Backend.(*ses.InMemoryBackend).ReceiptRuleSetCount())
}

// TestSESNewOps_CreateReceiptRule covers the CreateReceiptRule handler.
func TestSESNewOps_CreateReceiptRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":       {"CreateReceiptRule"},
				"Version":      {"2010-12-01"},
				"RuleSetName":  {"rules"},
				"Rule.Name":    {"my-rule"},
				"Rule.Enabled": {"true"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("rules"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CreateReceiptRuleResponse",
		},
		{
			name:         "rule_set_not_found",
			body:         "Action=CreateReceiptRule&Version=2010-12-01&RuleSetName=missing&Rule.Name=r1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name: "duplicate_rule_returns_error",
			body: "Action=CreateReceiptRule&Version=2010-12-01&RuleSetName=rules&Rule.Name=existing",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("rules"))
				require.NoError(t, h.Backend.CreateReceiptRule("rules", ses.ReceiptRule{Name: "existing"}, ""))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name: "empty_rule_name",
			body: "Action=CreateReceiptRule&Version=2010-12-01&RuleSetName=rules&Rule.Name=",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateReceiptRuleSet("rules"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CreateReceiptFilter covers the CreateReceiptFilter handler.
func TestSESNewOps_CreateReceiptFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success_allow",
			body: url.Values{
				"Action":                 {"CreateReceiptFilter"},
				"Version":                {"2010-12-01"},
				"Filter.Name":            {"allow-filter"},
				"Filter.IpFilter.Policy": {"Allow"},
				"Filter.IpFilter.Cidr":   {"10.0.0.0/8"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "CreateReceiptFilterResponse",
		},
		{
			name: "duplicate_filter_returns_error",
			body: url.Values{
				"Action":                 {"CreateReceiptFilter"},
				"Version":                {"2010-12-01"},
				"Filter.Name":            {"existing"},
				"Filter.IpFilter.Policy": {"Block"},
				"Filter.IpFilter.Cidr":   {"192.168.0.0/16"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(
					t,
					h.Backend.CreateReceiptFilter(
						ses.ReceiptFilter{Name: "existing", Policy: "Block", CIDR: "0.0.0.0/0"},
					),
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name: "empty_filter_name",
			body: url.Values{
				"Action":                 {"CreateReceiptFilter"},
				"Version":                {"2010-12-01"},
				"Filter.Name":            {""},
				"Filter.IpFilter.Policy": {"Allow"},
				"Filter.IpFilter.Cidr":   {"0.0.0.0/0"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CreateConfigurationSetEventDestination covers event destination creation.
func TestSESNewOps_CreateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":                   {"CreateConfigurationSetEventDestination"},
				"Version":                  {"2010-12-01"},
				"ConfigurationSetName":     {"my-config-set"},
				"EventDestination.Name":    {"my-dest"},
				"EventDestination.Enabled": {"true"},
				"EventDestination.MatchingEventTypes.member.1": {"send"},
				"EventDestination.MatchingEventTypes.member.2": {"bounce"},
				"EventDestination.SNSDestination.TopicARN":     {"arn:aws:sns:us-east-1:123456789012:my-topic"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("my-config-set"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetEventDestinationResponse",
		},
		{
			name: "config_set_not_found",
			body: url.Values{
				"Action":                {"CreateConfigurationSetEventDestination"},
				"Version":               {"2010-12-01"},
				"ConfigurationSetName":  {"nonexistent"},
				"EventDestination.Name": {"dest"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "duplicate_destination_returns_error",
			body: url.Values{
				"Action":                {"CreateConfigurationSetEventDestination"},
				"Version":               {"2010-12-01"},
				"ConfigurationSetName":  {"cs"},
				"EventDestination.Name": {"existing-dest"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(
					t,
					h.Backend.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "existing-dest"}),
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_DeleteConfigurationSetEventDestination covers event destination deletion.
func TestSESNewOps_DeleteConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=cs&EventDestinationName=dest",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(
					t,
					h.Backend.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "dest"}),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetEventDestinationResponse",
		},
		{
			name: "config_set_not_found",
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=missing&EventDestinationName=dest",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "destination_not_found",
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=cs&EventDestinationName=missing",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CreateConfigurationSetTrackingOptions covers tracking options creation.
func TestSESNewOps_CreateConfigurationSetTrackingOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"cs"},
				"TrackingOptions.CustomRedirectDomain": {"track.example.com"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetTrackingOptionsResponse",
		},
		{
			name: "config_set_not_found",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"missing"},
				"TrackingOptions.CustomRedirectDomain": {"track.example.com"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "tracking_options_already_exist",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"cs"},
				"TrackingOptions.CustomRedirectDomain": {"track2.example.com"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "TrackingOptionsAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_DeleteConfigurationSetTrackingOptions covers tracking options deletion.
func TestSESNewOps_DeleteConfigurationSetTrackingOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cs",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))
			},
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetTrackingOptionsResponse",
		},
		{
			name:         "config_set_not_found",
			body:         "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "tracking_options_not_found",
			body: "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cs",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "TrackingOptionsDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_CreateCustomVerificationEmailTemplate covers custom verification template creation.
func TestSESNewOps_CreateCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":                {"CreateCustomVerificationEmailTemplate"},
				"Version":               {"2010-12-01"},
				"TemplateName":          {"my-verif-tmpl"},
				"FromEmailAddress":      {"noreply@example.com"},
				"TemplateSubject":       {"Please verify your email"},
				"TemplateContent":       {"<html>Verify here: {{url}}</html>"},
				"SuccessRedirectionURL": {"https://example.com/success"},
				"FailureRedirectionURL": {"https://example.com/failure"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "CreateCustomVerificationEmailTemplateResponse",
		},
		{
			name: "duplicate_template_returns_error",
			body: url.Values{
				"Action":                {"CreateCustomVerificationEmailTemplate"},
				"Version":               {"2010-12-01"},
				"TemplateName":          {"existing"},
				"FromEmailAddress":      {"noreply@example.com"},
				"TemplateSubject":       {"Verify"},
				"TemplateContent":       {"<html>Click</html>"},
				"SuccessRedirectionURL": {"https://example.com/success"},
				"FailureRedirectionURL": {"https://example.com/failure"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				h.Backend.(*ses.InMemoryBackend).AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{
					TemplateName: "existing",
				})
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateAlreadyExists",
		},
		{
			name: "empty_template_name",
			body: url.Values{
				"Action":       {"CreateCustomVerificationEmailTemplate"},
				"Version":      {"2010-12-01"},
				"TemplateName": {""},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_DeleteCustomVerificationEmailTemplate covers custom verification template deletion.
func TestSESNewOps_DeleteCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=DeleteCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=my-tmpl",
			setup: func(h *ses.Handler) {
				h.Backend.(*ses.InMemoryBackend).AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{
					TemplateName: "my-tmpl",
				})
			},
			wantCode:     http.StatusOK,
			wantContains: "DeleteCustomVerificationEmailTemplateResponse",
		},
		{
			name:         "template_not_found",
			body:         "Action=DeleteCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateDoesNotExist",
		},
		{
			name:         "empty_template_name",
			body:         "Action=DeleteCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_BackendReset verifies that Reset() clears all new maps.
func TestSESNewOps_BackendReset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	require.NoError(t, b.CreateReceiptRuleSet("rs1"))
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f1", Policy: "Allow", CIDR: "0.0.0.0/0"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{Name: "dest1"}))
	require.NoError(t, b.CreateConfigurationSetTrackingOptions("cs1", "track.example.com"))
	b.AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{TemplateName: "tmpl1"})

	assert.Equal(t, 1, b.ReceiptRuleSetCount())
	assert.Equal(t, 1, b.ReceiptFilterCount())
	assert.Equal(t, 1, b.EventDestinationCount())
	assert.Equal(t, 1, b.TrackingOptionsCount())
	assert.Equal(t, 1, b.CustomVerifTemplateCount())

	b.Reset()

	assert.Equal(t, 0, b.ReceiptRuleSetCount())
	assert.Equal(t, 0, b.ReceiptFilterCount())
	assert.Equal(t, 0, b.EventDestinationCount())
	assert.Equal(t, 0, b.TrackingOptionsCount())
	assert.Equal(t, 0, b.CustomVerifTemplateCount())
}

// TestSESNewOps_SnapshotRestore verifies that new maps are included in Snapshot/Restore.
func TestSESNewOps_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	require.NoError(t, b.CreateReceiptRuleSet("rs1"))
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f1", Policy: "Block", CIDR: "192.168.0.0/16"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(
		t,
		b.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{Name: "dest1", Enabled: true}),
	)
	require.NoError(t, b.CreateConfigurationSetTrackingOptions("cs1", "track.example.com"))
	require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "my-tmpl",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Please verify",
		TemplateContent:       "<html>Verify</html>",
		SuccessRedirectionURL: "https://example.com/success",
		FailureRedirectionURL: "https://example.com/failure",
	}))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := ses.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.ReceiptRuleSetCount())
	assert.Equal(t, 1, b2.ReceiptFilterCount())
	assert.Equal(t, 1, b2.EventDestinationCount())
	assert.Equal(t, 1, b2.TrackingOptionsCount())
	assert.Equal(t, 1, b2.CustomVerifTemplateCount())
}
