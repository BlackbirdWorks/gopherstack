package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateEmailTemplate tests email template creation.
func TestCreateEmailTemplate(t *testing.T) {
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
			templateName: "my-template",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateEmailTemplate("my-template", nil, nil)
			},
			templateName: "my-template",
			wantErr:      true,
		},
		{
			name:         "empty_name",
			setup:        func(*sesv2.InMemoryBackend) {},
			templateName: "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			content := &sesv2.EmailTemplateContent{Subject: "Hello", Text: "Hello world"}
			_, err := backend.CreateEmailTemplate(tt.templateName, content, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.EmailTemplateCount(backend))
		})
	}
}

// TestCreateEmailTemplateHTTP tests email template creation via HTTP.
func TestCreateEmailTemplateHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{
		"TemplateName": "my-template",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Text":    "Hello world",
			"Html":    "<p>Hello</p>",
		},
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/templates", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateEmailTemplateHTTPDuplicate tests duplicate template error via HTTP.
func TestCreateEmailTemplateHTTPDuplicate(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)
	_, err := backend.CreateEmailTemplate("my-template", nil, nil)
	require.NoError(t, err)

	body := map[string]any{"TemplateName": "my-template", "TemplateContent": map[string]any{}}
	rec := doReq(t, h, http.MethodPost, "/v2/email/templates", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestGetEmailTemplate tests the GetEmailTemplate operation.
func TestGetEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "GetTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/templates/GetTemplate", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetEmailTemplate_RendersTags verifies that GetEmailTemplate echoes the
// Tags supplied at creation, matching the real GetEmailTemplateResponse
// shape (confirmed via botocore sesv2/2019-09-27: TemplateName,
// TemplateContent, Tags -- gopherstack-2mwl).
func TestGetEmailTemplate_RendersTags(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "TaggedTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
		"Tags": []map[string]any{
			{"Key": "team", "Value": "sec"},
		},
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/templates/TaggedTemplate", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 1)
	assert.Equal(t, "team", resp.Tags[0].Key)
	assert.Equal(t, "sec", resp.Tags[0].Value)
}

// TestDeleteEmailTemplate tests the DeleteEmailTemplate operation.
func TestDeleteEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "DelTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/templates/DelTemplate", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateEmailTemplate tests the UpdateEmailTemplate operation.
func TestUpdateEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "UpdTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodPut, "/v2/email/templates/UpdTemplate", map[string]any{
		"TemplateContent": map[string]any{
			"Subject": "Hello Updated",
			"Html":    "<html>Hello Updated</html>",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListEmailTemplates tests the ListEmailTemplates operation.
func TestListEmailTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/templates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRenderEmailTemplate tests the TestRenderEmailTemplate operation.
func TestRenderEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "RenderTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello {{name}}",
			"Html":    "<html>Hello {{name}}</html>",
		},
	})

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/templates/RenderTemplate/render",
		map[string]any{
			"TemplateData": `{"name":"World"}`,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
