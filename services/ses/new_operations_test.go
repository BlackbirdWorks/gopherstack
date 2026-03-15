package ses_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ---- backend: O(1) email lookup map ----

func TestSESBackend_EmailsByIDMapSync(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@test.com"))

	msgID, err := b.SendEmail("a@test.com", []string{"b@test.com"}, "s", "", "body")
	require.NoError(t, err)

	assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())

	e, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, msgID, e.MessageID)
}

func TestSESBackend_EmailsByIDSyncAfterEviction(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("x@test.com"))

	// Send MaxRetainedEmails+5 so the first 5 are evicted.
	var firstIDs []string

	for i := range ses.MaxRetainedEmails + 5 {
		msgID, err := b.SendEmail("x@test.com", []string{"y@test.com"},
			"s", "", "body")
		require.NoError(t, err)

		if i < 5 {
			firstIDs = append(firstIDs, msgID)
		}
	}

	assert.Equal(t, ses.MaxRetainedEmails, b.EmailCount())
	assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())

	// Evicted IDs must no longer be in the map.
	for _, id := range firstIDs {
		_, err := b.GetEmailByID(id)
		require.Error(t, err)
		assert.ErrorIs(t, err, ses.ErrEmailNotFound)
	}
}

// ---- backend: Reset ----

func TestSESBackend_Reset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("r@test.com"))

	_, err := b.SendEmail("r@test.com", []string{"t@test.com"}, "s", "", "b")
	require.NoError(t, err)

	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))

	// Override TTL before Reset.
	b.SetEmailTTL(time.Millisecond)

	b.Reset()

	assert.Equal(t, 0, b.IdentityCount())
	assert.Equal(t, 0, b.EmailCount())
	assert.Equal(t, 0, b.EmailsByIDCount())
	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ConfigSetCount())
	// TTL must be restored to the default.
	assert.Equal(t, ses.DefaultEmailTTL, b.GetEmailTTL())
}

func TestSESHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("r@test.com"))

	h.Reset()

	assert.Equal(t, 0, h.Backend.IdentityCount())
}

// ---- backend: email TTL + janitor ----

func TestSESJanitor_SweepExpiredEmails(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.SetEmailTTL(time.Millisecond) // very short TTL
	require.NoError(t, b.VerifyEmailIdentity("j@test.com"))

	_, err := b.SendEmail("j@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	require.Equal(t, 1, b.EmailCount())

	// Wait for TTL to expire then sweep.
	time.Sleep(5 * time.Millisecond)

	j := ses.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	assert.Equal(t, 0, b.EmailCount())
	assert.Equal(t, 0, b.EmailsByIDCount())
}

func TestSESJanitor_SweepNoExpired(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("j2@test.com"))

	_, err := b.SendEmail("j2@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	j := ses.NewJanitor(b, 0)
	j.SweepOnce(t.Context())

	assert.Equal(t, 1, b.EmailCount())
}

func TestSESHandler_StartWorker(t *testing.T) {
	t.Parallel()

	h := newHandler()
	h.WithJanitor(time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}

func TestSESHandler_StartWorkerNoJanitor(t *testing.T) {
	t.Parallel()

	h := newHandler()

	err := h.StartWorker(t.Context())
	require.NoError(t, err)
}

// ---- backend: templates ----

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

// ---- backend: configuration sets ----

func TestSESBackend_ConfigSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ses.InMemoryBackend)
		verify func(t *testing.T, b *ses.InMemoryBackend)
		name   string
	}{
		{
			name: "create_and_list",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("cs1"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListConfigurationSets("", 0)
				require.Len(t, p.Data, 1)
				assert.Equal(t, "cs1", p.Data[0])
			},
		},
		{
			name: "create_duplicate_returns_error",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("dup"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateConfigurationSet("dup")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrConfigSetExists)
			},
		},
		{
			name: "delete_existing",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("del"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.DeleteConfigurationSet("del"))
				assert.Equal(t, 0, b.ConfigSetCount())
			},
		},
		{
			name: "delete_missing_returns_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.DeleteConfigurationSet("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
			},
		},
		{
			name: "list_sorted",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("zzz"))
				require.NoError(t, b.CreateConfigurationSet("aaa"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListConfigurationSets("", 0)
				require.Len(t, p.Data, 2)
				assert.Equal(t, "aaa", p.Data[0])
				assert.Equal(t, "zzz", p.Data[1])
			},
		},
		{
			name: "create_empty_name_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateConfigurationSet("")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)
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

// ---- backend: send quota / statistics ----

func TestSESBackend_GetSendQuota(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("q@test.com"))

	_, err := b.SendEmail("q@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	q := b.GetSendQuota()
	assert.InDelta(t, float64(200), q.Max24HourSend, 0)
	assert.InDelta(t, float64(1), q.MaxSendRate, 0)
	assert.InDelta(t, float64(1), q.SentLast24Hours, 0)
}

func TestSESBackend_GetSendStatistics(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@test.com"))

	_, err := b.SendEmail("s@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	points := b.GetSendStatistics()
	require.Len(t, points, 1)
	assert.InDelta(t, float64(1), points[0].DeliveryAttempts, 0)
}

func TestSESBackend_GetSendStatisticsEmpty(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	points := b.GetSendStatistics()
	assert.Empty(t, points)
}

// ---- handler: template actions ----

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

// ---- handler: configuration set actions ----

func TestSESHandler_ConfigSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "CreateConfigurationSet_ok",
			body: url.Values{
				"Action":                {"CreateConfigurationSet"},
				"Version":               {"2010-12-01"},
				"ConfigurationSet.Name": {"myCS"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetResponse",
		},
		{
			name: "CreateConfigurationSet_duplicate",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("dup"))
			},
			body: url.Values{
				"Action":                {"CreateConfigurationSet"},
				"Version":               {"2010-12-01"},
				"ConfigurationSet.Name": {"dup"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetAlreadyExists",
		},
		{
			name: "DeleteConfigurationSet_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("del"))
			},
			body: url.Values{
				"Action":               {"DeleteConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"del"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetResponse",
		},
		{
			name: "DeleteConfigurationSet_missing",
			body: url.Values{
				"Action":               {"DeleteConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"nope"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "ListConfigurationSets_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
				require.NoError(t, h.Backend.CreateConfigurationSet("cs2"))
			},
			body: url.Values{
				"Action":  {"ListConfigurationSets"},
				"Version": {"2010-12-01"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
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

// ---- handler: send quota / statistics ----

func TestSESHandler_GetSendQuota(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendQuota"},
		"Version": {"2010-12-01"},
	}.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendQuotaResponse")
	assert.Contains(t, rec.Body.String(), "Max24HourSend")
}

func TestSESHandler_GetSendStatistics(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("stat@test.com"))

	_, err := h.Backend.SendEmail("stat@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendStatisticsResponse")
}

// ---- persistence: new fields ----

func TestSESPersistence_TemplatesAndConfigSetsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ses.InMemoryBackend)
		verify func(t *testing.T, b *ses.InMemoryBackend)
		name   string
	}{
		{
			name: "templates_persisted",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
					TemplateName: "t1",
					SubjectPart:  "Hello",
				}))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				tmpl, err := b.GetTemplate("t1")
				require.NoError(t, err)
				assert.Equal(t, "Hello", tmpl.SubjectPart)
			},
		},
		{
			name: "config_sets_persisted",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("cs1"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListConfigurationSets("", 0)
				require.Len(t, p.Data, 1)
				assert.Equal(t, "cs1", p.Data[0])
			},
		},
		{
			name: "emails_by_id_rebuilt_on_restore",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.VerifyEmailIdentity("p@test.com"))
				_, err := b.SendEmail("p@test.com", []string{"q@test.com"}, "s", "", "b")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ses.NewInMemoryBackend()
			tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ses.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh)
		})
	}
}

// ---- GetSendQuota 24h window ----

func TestSESBackend_GetSendQuota_CountsOnlyLast24h(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	// Verify that a freshly sent email counts toward the 24h quota.
	require.NoError(t, b.VerifyEmailIdentity("q24@test.com"))

	_, err := b.SendEmail("q24@test.com", []string{"to@test.com"}, "s", "", "b")
	require.NoError(t, err)

	q := b.GetSendQuota()
	assert.InDelta(t, float64(200), q.Max24HourSend, 0)
	assert.InDelta(t, float64(1), q.MaxSendRate, 0)
	assert.InDelta(t, float64(1), q.SentLast24Hours, 0)
}

// ---- GetSendStatistics 14-day window ----

func TestSESBackend_GetSendStatistics_14DayWindow(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("stat14@test.com"))

	// A recently sent email should appear in the stats.
	_, err := b.SendEmail("stat14@test.com", []string{"to@test.com"}, "recent", "", "b")
	require.NoError(t, err)

	points := b.GetSendStatistics()
	require.Len(t, points, 1)
	assert.InDelta(t, float64(1), points[0].DeliveryAttempts, 0)
}

// ---- Restore prunes expired and excess emails ----

func TestSESPersistence_RestorePrunesExpiredEmails(t *testing.T) {
	t.Parallel()

	original := ses.NewInMemoryBackend()
	require.NoError(t, original.VerifyEmailIdentity("prune@test.com"))

	_, err := original.SendEmail("prune@test.com", []string{"to@test.com"}, "keep", "", "b")
	require.NoError(t, err)

	snap := original.Snapshot()
	require.NotNil(t, snap)

	// Restore into a backend with a very short TTL so the snapshot email is
	// considered expired at restore time.
	fresh := ses.NewInMemoryBackend()
	fresh.SetEmailTTL(time.Nanosecond) // instant expiry

	time.Sleep(time.Millisecond) // ensure TTL has passed

	require.NoError(t, fresh.Restore(snap))

	// The expired email must have been pruned.
	assert.Equal(t, 0, fresh.EmailCount())
	assert.Equal(t, 0, fresh.EmailsByIDCount())
}

func TestSESPersistence_RestoreCapsToBound(t *testing.T) {
	t.Parallel()

	original := ses.NewInMemoryBackend()
	require.NoError(t, original.VerifyEmailIdentity("cap@test.com"))

	// Send MaxRetainedEmails+10 emails and snapshot.
	for range ses.MaxRetainedEmails + 10 {
		_, err := original.SendEmail("cap@test.com", []string{"to@test.com"}, "s", "", "b")
		require.NoError(t, err)
	}

	// The original should already be capped by SendEmail eviction.
	assert.Equal(t, ses.MaxRetainedEmails, original.EmailCount())

	snap := original.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	assert.Equal(t, ses.MaxRetainedEmails, fresh.EmailCount())
	assert.Equal(t, fresh.EmailCount(), fresh.EmailsByIDCount())
}
