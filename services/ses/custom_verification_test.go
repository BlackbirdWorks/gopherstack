package ses_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_SendCustomVerificationEmail_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "missing_email",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=&TemplateName=tmpl",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "missing_template_name",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=test@example.com&TemplateName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			// AWS SES requires the custom verification email template to already
			// exist (CustomVerificationEmailTemplateDoesNotExist otherwise).
			name:         "template_not_found",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=user@example.com&TemplateName=MyTemplate", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateDoesNotExist",
		},
		{
			name:         "valid_request",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=user@example.com&TemplateName=MyTemplate", //nolint:lll // existing issue.
			wantCode:     http.StatusOK,
			wantContains: "SendCustomVerificationEmailResponse",
			setup: func(t *testing.T, h *ses.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
					TemplateName:          "MyTemplate",
					FromEmailAddress:      "noreply@example.com",
					TemplateSubject:       "Verify your email",
					TemplateContent:       "<p>Click here</p>",
					SuccessRedirectionURL: "https://example.com/success",
					FailureRedirectionURL: "https://example.com/failure",
				}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_UpdateCustomVerificationEmailTemplate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "template_not_found",
			body:         "Action=UpdateCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateDoesNotExist",
		},
		{
			name: "valid_update",
			setup: func(h *ses.Handler) {
				postForm(t, h, url.Values{
					"Action":                {"CreateCustomVerificationEmailTemplate"},
					"Version":               {"2010-12-01"},
					"TemplateName":          {"CveTpl"},
					"FromEmailAddress":      {"from@example.com"},
					"TemplateSubject":       {"subj"},
					"TemplateContent":       {"content"},
					"SuccessRedirectionURL": {"https://example.com/success"},
					"FailureRedirectionURL": {"https://example.com/fail"},
				}.Encode())
			},
			body: url.Values{
				"Action":                {"UpdateCustomVerificationEmailTemplate"},
				"Version":               {"2010-12-01"},
				"TemplateName":          {"CveTpl"},
				"FromEmailAddress":      {"updated@example.com"},
				"TemplateSubject":       {"new subj"},
				"TemplateContent":       {"new content"},
				"SuccessRedirectionURL": {"https://example.com/ok"},
				"FailureRedirectionURL": {"https://example.com/fail"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "UpdateCustomVerificationEmailTemplateResponse",
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

func TestSendCustomVerificationEmail_Handler(t *testing.T) {
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

func TestCreateCustomVerificationEmailTemplate_Handler(t *testing.T) {
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

func TestGetCustomVerificationEmailTemplate_Handler(t *testing.T) {
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

func TestUpdateCustomVerificationEmailTemplate_Handler(t *testing.T) {
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

func TestDeleteCustomVerificationEmailTemplate_Handler(t *testing.T) {
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

func TestListCustomVerificationEmailTemplates_Handler(t *testing.T) {
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

// TestSESNewOps_CreateCustomVerificationEmailTemplate covers custom verification template creation.
func TestCreateCustomVerificationEmailTemplate(t *testing.T) {
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
func TestDeleteCustomVerificationEmailTemplate(t *testing.T) {
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
			// Idempotent: the op's own deserializer (ses@v1.37.4 deserializers.go)
			// declares no exception at all, so a missing template is a no-op success.
			name:         "template_not_found_is_idempotent",
			body:         "Action=DeleteCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=nonexistent",
			wantCode:     http.StatusOK,
			wantContains: "DeleteCustomVerificationEmailTemplateResponse",
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

func TestSendCustomVerificationEmail_RegistersIdentity(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<p>click</p>",
		SuccessRedirectionURL: "https://example.com/ok",
		FailureRedirectionURL: "https://example.com/fail",
	}))

	// Before the call, the identity is unregistered / NotStarted.
	before := b.GetIdentityVerificationAttributes([]string{"user@example.com"})
	require.Equal(t, "NotStarted", before["user@example.com"])

	msgID, err := b.SendCustomVerificationEmail("user@example.com", "cv", "")
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)

	after := b.GetIdentityVerificationAttributes([]string{"user@example.com"})
	assert.Equal(t, "Success", after["user@example.com"])
}

func TestSendCustomVerificationEmail_UnknownTemplate_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	_, err := b.SendCustomVerificationEmail("user@example.com", "does-not-exist", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrCustomVerifTemplateNotFound)
}

func TestSendCustomVerificationEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<p>click</p>",
		SuccessRedirectionURL: "https://example.com/ok",
		FailureRedirectionURL: "https://example.com/fail",
	}))

	_, err := b.SendCustomVerificationEmail("user@example.com", "cv", "missing-cs")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
}
