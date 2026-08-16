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

func TestHandler_TestRenderTemplate_Errors(t *testing.T) {
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
			body:         "Action=TestRenderTemplate&Version=2010-12-01&TemplateName=nonexistent&TemplateData={}",
			wantCode:     http.StatusBadRequest,
			wantContains: "TemplateDoesNotExist",
		},
		{
			name: "valid_template_render",
			setup: func(h *ses.Handler) {
				postForm(t, h, url.Values{
					"Action":                {"CreateTemplate"},
					"Version":               {"2010-12-01"},
					"Template.TemplateName": {"MyTpl"},
					"Template.SubjectPart":  {"Hello {{name}}"},
					"Template.TextPart":     {"body"},
					"Template.HtmlPart":     {"<p>body</p>"},
				}.Encode())
			},
			body:         "Action=TestRenderTemplate&Version=2010-12-01&TemplateName=MyTpl&TemplateData=%7B%22name%22%3A%22World%22%7D", //nolint:lll // existing issue.
			wantCode:     http.StatusOK,
			wantContains: "TestRenderTemplateResponse",
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

func TestCreateTemplate_Handler(t *testing.T) {
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

func TestCreateTemplate_Duplicate_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1", SubjectPart: "s"}))
	assert.Error(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1", SubjectPart: "s2"}))
}

func TestUpdateTemplate_Handler(t *testing.T) {
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

func TestGetTemplate_Handler(t *testing.T) {
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

func TestGetTemplate_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":       {"GetTemplate"},
		"Version":      {"2010-12-01"},
		"TemplateName": {"noexist"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteTemplate_Handler(t *testing.T) {
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

func TestListTemplates_Pagination(t *testing.T) {
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

func TestTestRenderTemplate_VariableSubstitution(t *testing.T) {
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

func TestTestRenderTemplate_EmptyData_NoSubstitution(t *testing.T) {
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

func TestTestRenderTemplate_NotFound_Error(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	_, err := b.TestRenderTemplate("nonexistent", `{}`)
	assert.Error(t, err)
}

func TestTestRenderTemplate_Handler(t *testing.T) {
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

func TestListTemplates_NextTokenAbsentWhenNotTruncated(t *testing.T) {
	t.Parallel()

	h := newHandler()
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

func TestSESBackend_TemplateCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *ses.InMemoryBackend)
		verify  func(t *testing.T, b *ses.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "create_and_get",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
					TemplateName: "tmpl1",
					SubjectPart:  "Hello {{name}}",
					TextPart:     "Hi {{name}}",
					HTMLPart:     "<p>Hi {{name}}</p>",
				}))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				tmpl, err := b.GetTemplate("tmpl1")
				require.NoError(t, err)
				assert.Equal(t, "Hello {{name}}", tmpl.SubjectPart)
				assert.Equal(t, 1, b.TemplateCount())
			},
		},
		{
			name: "create_duplicate_returns_error",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "dup"}))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateTemplate(ses.EmailTemplate{TemplateName: "dup"})
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrTemplateExists)
			},
		},
		{
			name: "update_existing",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
					TemplateName: "upd",
					SubjectPart:  "old",
				}))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.UpdateTemplate(ses.EmailTemplate{
					TemplateName: "upd",
					SubjectPart:  "new",
				}))

				tmpl, err := b.GetTemplate("upd")
				require.NoError(t, err)
				assert.Equal(t, "new", tmpl.SubjectPart)
			},
		},
		{
			name: "update_missing_returns_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.UpdateTemplate(ses.EmailTemplate{TemplateName: "missing"})
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrTemplateNotFound)
			},
		},
		{
			name: "delete_idempotent",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				b.DeleteTemplate("nonexistent")
				assert.Equal(t, 0, b.TemplateCount())
			},
		},
		{
			name: "list_sorted",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "zzz"}))
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "aaa"}))
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "mmm"}))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListTemplates("", 0)
				require.Len(t, p.Data, 3)
				assert.Equal(t, "aaa", p.Data[0])
				assert.Equal(t, "mmm", p.Data[1])
				assert.Equal(t, "zzz", p.Data[2])
			},
		},
		{
			name: "create_empty_name_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateTemplate(ses.EmailTemplate{TemplateName: ""})
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)
			},
		},
		{
			name: "update_empty_name_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.UpdateTemplate(ses.EmailTemplate{TemplateName: ""})
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)
			},
		},
		{
			name: "get_missing_returns_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				_, err := b.GetTemplate("missing")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrTemplateNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestSESHandler_TemplateCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "CreateTemplate_ok",
			body: url.Values{
				"Action":                {"CreateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"myTmpl"},
				"Template.SubjectPart":  {"Hello {{name}}"},
				"Template.TextPart":     {"Hi {{name}}"},
				"Template.HTMLPart":     {"<p>Hi</p>"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "CreateTemplateResponse",
		},
		{
			name: "CreateTemplate_duplicate",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "dup"}))
			},
			body: url.Values{
				"Action":                {"CreateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"dup"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "AlreadyExists",
		},
		{
			name: "UpdateTemplate_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "upd", SubjectPart: "old"}))
			},
			body: url.Values{
				"Action":                {"UpdateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"upd"},
				"Template.SubjectPart":  {"new"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "UpdateTemplateResponse",
		},
		{
			name: "UpdateTemplate_missing",
			body: url.Values{
				"Action":                {"UpdateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"missing"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "TemplateDoesNotExist",
		},
		{
			name: "GetTemplate_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
					TemplateName: "fetch",
					SubjectPart:  "Hi {{name}}",
				}))
			},
			body: url.Values{
				"Action":       {"GetTemplate"},
				"Version":      {"2010-12-01"},
				"TemplateName": {"fetch"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "GetTemplateResponse",
		},
		{
			name: "GetTemplate_missing",
			body: url.Values{
				"Action":       {"GetTemplate"},
				"Version":      {"2010-12-01"},
				"TemplateName": {"nope"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "TemplateDoesNotExist",
		},
		{
			name: "ListTemplates_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "t1"}))
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "t2"}))
			},
			body: url.Values{
				"Action":  {"ListTemplates"},
				"Version": {"2010-12-01"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "ListTemplatesResponse",
		},
		{
			name: "DeleteTemplate_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{TemplateName: "del"}))
			},
			body: url.Values{
				"Action":       {"DeleteTemplate"},
				"Version":      {"2010-12-01"},
				"TemplateName": {"del"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "DeleteTemplateResponse",
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
