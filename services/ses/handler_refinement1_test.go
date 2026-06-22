package ses_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestHandler_ListReceiptFilters tests the ListReceiptFilters handler.
func TestHandler_ListReceiptFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_returns_empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListReceiptFiltersResponse",
		},
		{
			name: "with_filters",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "filter1", Policy: "Allow", CIDR: "10.0.0.0/8"})
				b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "filter2", Policy: "Block", CIDR: "192.168.0.0/16"})
			},
			wantCode:     http.StatusOK,
			wantContains: "filter1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=ListReceiptFilters&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DeleteReceiptFilter tests the DeleteReceiptFilter handler.
func TestHandler_DeleteReceiptFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "my-filter", Policy: "Allow", CIDR: "10.0.0.0/8"})
			},
			body:         "Action=DeleteReceiptFilter&Version=2010-12-01&FilterName=my-filter",
			wantCode:     http.StatusOK,
			wantContains: "DeleteReceiptFilterResponse",
		},
		{
			name:         "filter_not_found",
			body:         "Action=DeleteReceiptFilter&Version=2010-12-01&FilterName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "FilterDoesNotExist",
		},
		{
			name:         "empty_filter_name",
			body:         "Action=DeleteReceiptFilter&Version=2010-12-01&FilterName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_ListReceiptRuleSets tests the ListReceiptRuleSets handler.
func TestHandler_ListReceiptRuleSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_returns_empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListReceiptRuleSetsResponse",
		},
		{
			name: "with_rule_sets",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "set1", CreatedAt: time.Now()})
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "set2", CreatedAt: time.Now()})
			},
			wantCode:     http.StatusOK,
			wantContains: "set1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=ListReceiptRuleSets&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DescribeReceiptRuleSet tests the DescribeReceiptRuleSet handler.
func TestHandler_DescribeReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{
					Name:      "my-set",
					CreatedAt: time.Now(),
					Rules:     []ses.ReceiptRule{{Name: "rule1", Enabled: true}},
				})
			},
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "DescribeReceiptRuleSetResponse",
		},
		{
			name:         "not_found",
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name:         "empty_name",
			body:         "Action=DescribeReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DeleteReceiptRule tests the DeleteReceiptRule handler.
func TestHandler_DeleteReceiptRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{
					Name:      "my-set",
					CreatedAt: time.Now(),
					Rules:     []ses.ReceiptRule{{Name: "rule1"}},
				})
			},
			body:         "Action=DeleteReceiptRule&Version=2010-12-01&RuleSetName=my-set&RuleName=rule1",
			wantCode:     http.StatusOK,
			wantContains: "DeleteReceiptRuleResponse",
		},
		{
			name:         "rule_set_not_found",
			body:         "Action=DeleteReceiptRule&Version=2010-12-01&RuleSetName=missing&RuleName=rule1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name: "rule_not_found",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "my-set", CreatedAt: time.Now()})
			},
			body:         "Action=DeleteReceiptRule&Version=2010-12-01&RuleSetName=my-set&RuleName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleDoesNotExist",
		},
		{
			name:         "empty_rule_set_name",
			body:         "Action=DeleteReceiptRule&Version=2010-12-01&RuleSetName=&RuleName=rule1",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DeleteReceiptRuleSet tests the DeleteReceiptRuleSet handler.
func TestHandler_DeleteReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "my-set", CreatedAt: time.Now()})
			},
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "DeleteReceiptRuleSetResponse",
		},
		{
			name:         "not_found",
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name:         "empty_name",
			body:         "Action=DeleteReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_SetActiveReceiptRuleSet tests the SetActiveReceiptRuleSet handler.
func TestHandler_SetActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "my-set", CreatedAt: time.Now()})
			},
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=my-set",
			wantCode:     http.StatusOK,
			wantContains: "SetActiveReceiptRuleSetResponse",
		},
		{
			name:         "clear_active_with_empty_name",
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=",
			wantCode:     http.StatusOK,
			wantContains: "SetActiveReceiptRuleSetResponse",
		},
		{
			name:         "not_found",
			body:         "Action=SetActiveReceiptRuleSet&Version=2010-12-01&RuleSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_DescribeActiveReceiptRuleSet tests the DescribeActiveReceiptRuleSet handler.
func TestHandler_DescribeActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "no_active_rule_set",
			wantCode:     http.StatusOK,
			wantContains: "DescribeActiveReceiptRuleSetResponse",
		},
		{
			name: "with_active_rule_set",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{
					Name:      "active-set",
					CreatedAt: time.Now(),
					Rules:     []ses.ReceiptRule{{Name: "rule1", Enabled: true}},
				})
				require.NoError(t, b.SetActiveReceiptRuleSet("active-set"))
			},
			wantCode:     http.StatusOK,
			wantContains: "active-set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_GetCustomVerificationEmailTemplate tests the GetCustomVerificationEmailTemplate handler.
func TestHandler_GetCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				b.AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{
					TemplateName:          "my-tmpl",
					FromEmailAddress:      "noreply@example.com",
					TemplateSubject:       "Please verify",
					TemplateContent:       "<html>Click here</html>",
					SuccessRedirectionURL: "https://example.com/success",
					FailureRedirectionURL: "https://example.com/failure",
				})
			},
			body:         "Action=GetCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=my-tmpl",
			wantCode:     http.StatusOK,
			wantContains: "GetCustomVerificationEmailTemplateResponse",
		},
		{
			name:         "not_found",
			body:         "Action=GetCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateDoesNotExist",
		},
		{
			name:         "empty_name",
			body:         "Action=GetCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestHandler_ListCustomVerificationEmailTemplates tests the ListCustomVerificationEmailTemplates handler.
func TestHandler_ListCustomVerificationEmailTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *ses.InMemoryBackend)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_returns_empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListCustomVerificationEmailTemplatesResponse",
		},
		{
			name: "with_templates",
			setup: func(b *ses.InMemoryBackend) {
				b.AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{
					TemplateName:          "tmpl1",
					FromEmailAddress:      "noreply@example.com",
					TemplateSubject:       "Verify",
					TemplateContent:       "<html>Click</html>",
					SuccessRedirectionURL: "https://example.com/success",
					FailureRedirectionURL: "https://example.com/failure",
				})
			},
			wantCode:     http.StatusOK,
			wantContains: "tmpl1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h.Backend.(*ses.InMemoryBackend))
			}

			body := "Action=ListCustomVerificationEmailTemplates&Version=2010-12-01"
			rec := postForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestBackend_CreateReceiptFilter_PolicyValidation tests Policy validation.
func TestBackend_CreateReceiptFilter_PolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  ses.ReceiptFilter
		wantErr bool
	}{
		{
			name:    "allow_policy",
			filter:  ses.ReceiptFilter{Name: "f1", Policy: "Allow", CIDR: "10.0.0.0/8"},
			wantErr: false,
		},
		{
			name:    "block_policy",
			filter:  ses.ReceiptFilter{Name: "f2", Policy: "Block", CIDR: "10.0.0.0/8"},
			wantErr: false,
		},
		{
			name:    "empty_policy_is_allowed",
			filter:  ses.ReceiptFilter{Name: "f3", Policy: "", CIDR: "10.0.0.0/8"},
			wantErr: false,
		},
		{
			name:    "invalid_policy",
			filter:  ses.ReceiptFilter{Name: "f4", Policy: "Invalid", CIDR: "10.0.0.0/8"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			err := b.CreateReceiptFilter(tt.filter)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_CreateReceiptRule_TLSPolicyValidation tests TLSPolicy validation.
func TestBackend_CreateReceiptRule_TLSPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tlsPolicy string
		wantErr   bool
	}{
		{name: "optional", tlsPolicy: "Optional", wantErr: false},
		{name: "require", tlsPolicy: "Require", wantErr: false},
		{name: "empty_is_allowed", tlsPolicy: "", wantErr: false},
		{name: "invalid", tlsPolicy: "invalid-val", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.CreateReceiptRuleSet("rs"))
			err := b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r1", TLSPolicy: tt.tlsPolicy}, "")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_CreateReceiptRule_PrependWhenAfterEmpty tests that rules are prepended when after="".
func TestBackend_CreateReceiptRule_PrependWhenAfterEmpty(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "first"}, ""))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "second"}, ""))

	rs, err := b.DescribeReceiptRuleSet("rs")
	require.NoError(t, err)
	require.Len(t, rs.Rules, 2)
	assert.Equal(t, "second", rs.Rules[0].Name)
	assert.Equal(t, "first", rs.Rules[1].Name)
}

// TestBackend_DeleteConfigurationSet_Cascade tests that deleting a config set cascades.
func TestBackend_DeleteConfigurationSet_Cascade(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs"))
	require.NoError(t, b.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "dest"}))
	require.NoError(t, b.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))

	assert.Equal(t, 1, b.EventDestinationCount())
	assert.Equal(t, 1, b.TrackingOptionsCount())

	require.NoError(t, b.DeleteConfigurationSet("cs"))

	assert.Equal(t, 0, b.EventDestinationCount())
	assert.Equal(t, 0, b.TrackingOptionsCount())
}

// TestBackend_SetActiveReceiptRuleSet tests active rule set management.
func TestBackend_SetActiveReceiptRuleSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *ses.InMemoryBackend)
		name       string
		setName    string
		wantActive string
		wantErr    bool
	}{
		{
			name: "set_active",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
			},
			setName:    "rs1",
			wantActive: "rs1",
		},
		{
			name: "clear_active",
			setup: func(b *ses.InMemoryBackend) {
				b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
				require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
			},
			setName:    "",
			wantActive: "",
		},
		{
			name:    "not_found",
			setName: "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.SetActiveReceiptRuleSet(tt.setName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantActive, b.ActiveRuleSet())
		})
	}
}

// TestBackend_DeleteReceiptRuleSet_ClearsActive tests that deleting the active rule set clears it.
func TestBackend_DeleteReceiptRuleSet_ClearsActive(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
	require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
	assert.Equal(t, "rs1", b.ActiveRuleSet())

	require.NoError(t, b.DeleteReceiptRuleSet("rs1"))
	assert.Empty(t, b.ActiveRuleSet())
}

// TestBackend_CreateCustomVerificationEmailTemplate_RequiredFields tests field validation.
func TestBackend_CreateCustomVerificationEmailTemplate_RequiredFields(t *testing.T) {
	t.Parallel()

	validTmpl := ses.CustomVerificationEmailTemplate{
		TemplateName:          "my-tmpl",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<html>Click</html>",
		SuccessRedirectionURL: "https://example.com/success",
		FailureRedirectionURL: "https://example.com/failure",
	}

	tests := []struct {
		mutate  func(t *ses.CustomVerificationEmailTemplate)
		name    string
		wantErr bool
	}{
		{
			name:    "all_fields_present",
			mutate:  func(_ *ses.CustomVerificationEmailTemplate) {},
			wantErr: false,
		},
		{
			name:    "missing_template_name",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.TemplateName = "" },
			wantErr: true,
		},
		{
			name:    "missing_from_email",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.FromEmailAddress = "" },
			wantErr: true,
		},
		{
			name:    "missing_subject",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.TemplateSubject = "" },
			wantErr: true,
		},
		{
			name:    "missing_content",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.TemplateContent = "" },
			wantErr: true,
		},
		{
			name:    "missing_success_url",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.SuccessRedirectionURL = "" },
			wantErr: true,
		},
		{
			name:    "missing_failure_url",
			mutate:  func(t *ses.CustomVerificationEmailTemplate) { t.FailureRedirectionURL = "" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			tmpl := validTmpl
			tt.mutate(&tmpl)
			err := b.CreateCustomVerificationEmailTemplate(tmpl)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_Persistence_ActiveRuleSet tests that ActiveRuleSet survives Snapshot/Restore.
func TestBackend_Persistence_ActiveRuleSet(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
	require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := ses.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, "rs1", b2.ActiveRuleSet())
}

// TestHandler_GetSupportedOperations_Count tests that all operations are supported.
func TestHandler_GetSupportedOperations_Count(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 71, h.HandlerOpsLen())
}

// TestBackend_Region_AccountID tests Region and AccountID methods.
func TestBackend_Region_AccountID(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	assert.Equal(t, "us-east-1", b.Region())
	assert.Equal(t, "123456789012", b.AccountID())
}

// TestProvider_Init_NilContext tests that Init returns error on nil context.
func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()

	p := &ses.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

// TestBackend_DescribeActiveReceiptRuleSet_NoActive tests when no active rule set.
func TestBackend_DescribeActiveReceiptRuleSet_NoActive(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	rs, active, err := b.DescribeActiveReceiptRuleSet()
	require.NoError(t, err)
	assert.False(t, active)
	assert.Empty(t, rs.Name)
}

// TestBackend_GetCustomVerificationEmailTemplate tests get template.
func TestBackend_GetCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ses.InMemoryBackend)
		name     string
		tmplName string
		wantErr  bool
	}{
		{
			name: "found",
			setup: func(b *ses.InMemoryBackend) {
				b.AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{
					TemplateName: "existing",
				})
			},
			tmplName: "existing",
			wantErr:  false,
		},
		{
			name:     "not_found",
			tmplName: "missing",
			wantErr:  true,
		},
		{
			name:     "empty_name",
			tmplName: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			_, err := b.GetCustomVerificationEmailTemplate(tt.tmplName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_Reset_ClearsActiveRuleSet tests that Reset clears active rule set.
func TestBackend_Reset_ClearsActiveRuleSet(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "rs1", CreatedAt: time.Now()})
	require.NoError(t, b.SetActiveReceiptRuleSet("rs1"))
	assert.Equal(t, "rs1", b.ActiveRuleSet())

	b.Reset()
	assert.Empty(t, b.ActiveRuleSet())
}

// TestBackend_ListReceiptFilters_SortedOrder tests that filters are returned sorted.
func TestBackend_ListReceiptFilters_SortedOrder(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "zzz-filter"})
	b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "aaa-filter"})
	b.AddReceiptFilterInternal(ses.ReceiptFilter{Name: "mmm-filter"})

	filters := b.ListReceiptFilters()
	require.Len(t, filters, 3)
	assert.Equal(t, "aaa-filter", filters[0].Name)
	assert.Equal(t, "mmm-filter", filters[1].Name)
	assert.Equal(t, "zzz-filter", filters[2].Name)
}

// TestBackend_ListReceiptRuleSets_SortedOrder tests that rule sets are returned sorted.
func TestBackend_ListReceiptRuleSets_SortedOrder(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "zzz-set", CreatedAt: time.Now()})
	b.AddReceiptRuleSetInternal(ses.ReceiptRuleSet{Name: "aaa-set", CreatedAt: time.Now()})

	sets := b.ListReceiptRuleSets()
	require.Len(t, sets, 2)
	assert.Equal(t, "aaa-set", sets[0].Name)
	assert.Equal(t, "zzz-set", sets[1].Name)
}

// TestHandler_DeleteConfigurationSet_Cascade tests that the delete config set handler cascades.
func TestHandler_DeleteConfigurationSet_Cascade(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "dest"}))
	require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))

	body := url.Values{
		"Action":               {"DeleteConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs"},
	}.Encode()
	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteConfigurationSetResponse")

	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())
}
