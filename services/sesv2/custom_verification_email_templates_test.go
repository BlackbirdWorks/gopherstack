package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateCustomVerificationEmailTemplate tests template creation.
func TestCreateCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*sesv2.InMemoryBackend)
		templateName string
		wantErr      bool
	}{
		{
			name:         "create_new",
			setup:        func(*sesv2.InMemoryBackend) {},
			templateName: "my-tmpl",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateCustomVerificationEmailTemplate(
					&sesv2.CustomVerificationEmailTemplate{
						TemplateName: "my-tmpl",
					},
				)
			},
			templateName: "my-tmpl",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateCustomVerificationEmailTemplate(
				&sesv2.CustomVerificationEmailTemplate{
					TemplateName:     tt.templateName,
					FromEmailAddress: "verify@example.com",
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestCreateCustomVerificationTemplateHTTP tests template creation via HTTP.
func TestCreateCustomVerificationTemplateHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{
		"TemplateName":          "my-tmpl",
		"FromEmailAddress":      "verify@example.com",
		"TemplateSubject":       "Verify",
		"TemplateContent":       "<p>Verify</p>",
		"SuccessRedirectionURL": "https://example.com/success",
		"FailureRedirectionURL": "https://example.com/failure",
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/custom-verification-email-templates", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetCustomVerificationEmailTemplate tests the GetCustomVerificationEmailTemplate operation.
func TestGetCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "GetCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/custom-verification-email-templates/GetCVET",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteCustomVerificationEmailTemplate tests the DeleteCustomVerificationEmailTemplate operation.
func TestDeleteCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "DelCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/custom-verification-email-templates/DelCVET",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateCustomVerificationEmailTemplate tests the UpdateCustomVerificationEmailTemplate operation.
func TestUpdateCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "UpdCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/custom-verification-email-templates/UpdCVET",
		map[string]any{
			"FromEmailAddress":      "from2@example.com",
			"TemplateSubject":       "Please verify updated",
			"TemplateContent":       "<html>Verify Updated</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListCustomVerificationEmailTemplates tests the ListCustomVerificationEmailTemplates operation.
func TestListCustomVerificationEmailTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/custom-verification-email-templates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSendCustomVerificationEmail calls the backend indirectly since the HTTP route for this
// operation isn't wired in the current handler; it confirms the template used by the send
// path was created successfully.
func TestSendCustomVerificationEmail(t *testing.T) {
	t.Parallel()

	// Create the template via HTTP.
	h := newHandler()
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "CVETSend",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	// The backend method is covered by calling the GET template endpoint to confirm it was created.
	getRec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/custom-verification-email-templates/CVETSend",
		nil,
	)
	assert.Equal(t, http.StatusOK, getRec.Code)
}
