package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

func TestListTemplates_EmailListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/le1/email", map[string]any{"Subject": "Hello"})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	require.NotEmpty(t, items)

	found := false
	for _, item := range items {
		m := item.(map[string]any)
		if m["TemplateType"] == "EMAIL" {
			found = true

			break
		}
	}
	assert.True(t, found, "EMAIL template must appear in ListTemplates")
}

func TestEmailTemplate_BodyPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody      map[string]any
		updateBody      map[string]any
		name            string
		wantSubject     string
		wantHTMLPart    string
		wantTextPart    string
		wantDescription string
		wantRecommender string
	}{
		{
			name: "create_full_body",
			createBody: map[string]any{
				"Subject":  "Welcome to our service",
				"HtmlPart": "<p>Hello {{user.FirstName}}</p>",
				"TextPart": "Hello {{user.FirstName}}",
			},
			wantSubject:  "Welcome to our service",
			wantHTMLPart: "<p>Hello {{user.FirstName}}</p>",
			wantTextPart: "Hello {{user.FirstName}}",
		},
		{
			name: "create_subject_only",
			createBody: map[string]any{
				"Subject": "Subject Only",
			},
			wantSubject: "Subject Only",
		},
		{
			name: "create_with_description",
			createBody: map[string]any{
				"Subject":             "Hello",
				"TemplateDescription": "Onboarding email template",
			},
			wantSubject:     "Hello",
			wantDescription: "Onboarding email template",
		},
		{
			name: "create_with_recommender",
			createBody: map[string]any{
				"Subject":       "Recommended for you",
				"RecommenderId": "recommender-001",
			},
			wantSubject:     "Recommended for you",
			wantRecommender: "recommender-001",
		},
		{
			name: "update_overwrites_subject",
			createBody: map[string]any{
				"Subject":  "Original",
				"HtmlPart": "<p>Original HTML</p>",
			},
			updateBody: map[string]any{
				"Subject": "Updated Subject",
			},
			wantSubject:  "Updated Subject",
			wantHTMLPart: "<p>Original HTML</p>",
		},
		{
			name: "update_overwrites_html",
			createBody: map[string]any{
				"Subject":  "Subject",
				"HtmlPart": "<p>Old</p>",
				"TextPart": "Old text",
			},
			updateBody: map[string]any{
				"HtmlPart": "<p>New HTML</p>",
			},
			wantSubject:  "Subject",
			wantHTMLPart: "<p>New HTML</p>",
			wantTextPart: "Old text",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("email-body-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/email", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code, "create status")

			if tc.updateBody != nil {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/email", tc.updateBody)
				require.Equal(t, http.StatusAccepted, updateRec.Code, "update status")
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/email", nil)
			require.Equal(t, http.StatusOK, getRec.Code, "get status")

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			if tc.wantSubject != "" {
				assert.Equal(t, tc.wantSubject, resp["Subject"], "Subject")
			}

			if tc.wantHTMLPart != "" {
				assert.Equal(t, tc.wantHTMLPart, resp["HtmlPart"], "HtmlPart")
			}

			if tc.wantTextPart != "" {
				assert.Equal(t, tc.wantTextPart, resp["TextPart"], "TextPart")
			}

			if tc.wantDescription != "" {
				assert.Equal(t, tc.wantDescription, resp["TemplateDescription"], "TemplateDescription")
			}

			if tc.wantRecommender != "" {
				assert.Equal(t, tc.wantRecommender, resp["RecommenderId"], "RecommenderId")
			}
		})
	}
}

func TestEmailTemplate_VersionBumpsOnUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantVersion string
		updates     []map[string]any
	}{
		{
			name:        "no_updates_version_one",
			updates:     nil,
			wantVersion: "1",
		},
		{
			name: "one_update_version_two",
			updates: []map[string]any{
				{"Subject": "Updated"},
			},
			wantVersion: "2",
		},
		{
			name: "three_updates_version_four",
			updates: []map[string]any{
				{"Subject": "v2"},
				{"Subject": "v3"},
				{"Subject": "v4"},
			},
			wantVersion: "4",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("email-ver-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/email",
				map[string]any{"Subject": "initial"})
			require.Equal(t, http.StatusCreated, createRec.Code)

			for _, upd := range tc.updates {
				rec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/email", upd)
				require.Equal(t, http.StatusAccepted, rec.Code)
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/email", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantVersion, resp["Version"], "Version after updates")
		})
	}
}

func TestEmailTemplate_DefaultSubstitutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createSubs map[string]any
		updateSubs map[string]any
		wantSubs   map[string]any
		name       string
	}{
		{
			name: "create_with_substitutions",
			createSubs: map[string]any{
				"first_name": "Customer",
				"product":    "Widget",
			},
			wantSubs: map[string]any{
				"first_name": "Customer",
				"product":    "Widget",
			},
		},
		{
			name: "update_substitutions",
			createSubs: map[string]any{
				"first_name": "Customer",
			},
			updateSubs: map[string]any{
				"first_name": "User",
				"last_name":  "Member",
			},
			wantSubs: map[string]any{
				"first_name": "User",
				"last_name":  "Member",
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("email-subs-%d", i)

			body := map[string]any{
				"Subject":              "Hello {{first_name}}",
				"DefaultSubstitutions": tc.createSubs,
			}

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/email", body)
			require.Equal(t, http.StatusCreated, createRec.Code)

			if tc.updateSubs != nil {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/email",
					map[string]any{"DefaultSubstitutions": tc.updateSubs})
				require.Equal(t, http.StatusAccepted, updateRec.Code)
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/email", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			subs, _ := resp["DefaultSubstitutions"].(map[string]any)

			for k, v := range tc.wantSubs {
				assert.Equal(t, v, subs[k], "DefaultSubstitutions[%s]", k)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: SmsTemplate body persistence
// ──────────────────────────────────────────────────

func TestInAppTemplate_BodyPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody      map[string]any
		updateBody      map[string]any
		name            string
		wantLayout      string
		wantDescription string
		wantHasContent  bool
		wantContentLen  int
	}{
		{
			name: "create_with_content",
			createBody: map[string]any{
				"Content": []any{
					map[string]any{
						"BodyConfig": map[string]any{
							"Body":      "Welcome!",
							"Alignment": "LEFT",
						},
					},
				},
			},
			wantHasContent: true,
			wantContentLen: 1,
		},
		{
			name: "create_with_layout",
			createBody: map[string]any{
				"Layout": "TOP_BANNER",
				"Content": []any{
					map[string]any{"BodyConfig": map[string]any{"Body": "Banner!"}},
				},
			},
			wantLayout:     "TOP_BANNER",
			wantHasContent: true,
		},
		{
			name: "create_with_description",
			createBody: map[string]any{
				"TemplateDescription": "Onboarding in-app template",
			},
			wantDescription: "Onboarding in-app template",
		},
		{
			name: "update_content",
			createBody: map[string]any{
				"Content": []any{
					map[string]any{"BodyConfig": map[string]any{"Body": "Old"}},
				},
			},
			updateBody: map[string]any{
				"Content": []any{
					map[string]any{"BodyConfig": map[string]any{"Body": "New"}},
					map[string]any{"BodyConfig": map[string]any{"Body": "Second"}},
				},
			},
			wantHasContent: true,
			wantContentLen: 2,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("inapp-body-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/inapp", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code, "create status")

			if tc.updateBody != nil {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/inapp", tc.updateBody)
				require.Equal(t, http.StatusAccepted, updateRec.Code, "update status")
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/inapp", nil)
			require.Equal(t, http.StatusOK, getRec.Code, "get status")

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			if tc.wantLayout != "" {
				assert.Equal(t, tc.wantLayout, resp["Layout"])
			}

			if tc.wantDescription != "" {
				assert.Equal(t, tc.wantDescription, resp["TemplateDescription"])
			}

			if tc.wantHasContent {
				content, ok := resp["Content"].([]any)
				require.True(t, ok, "Content should be an array")

				if tc.wantContentLen > 0 {
					assert.Len(t, content, tc.wantContentLen, "Content length")
				}
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 28: ListTemplates prefix filter
// ──────────────────────────────────────────────────

func TestBackend_EmailTemplate_FullCRUD(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	t.Run("create_persists_body", func(t *testing.T) {
		t.Parallel()

		req := pinpoint.ExportedCreateEmailTemplateRequest{
			Subject:             "Backend test subject",
			HTMLPart:            "<p>HTML content</p>",
			TextPart:            "Text content",
			TemplateDescription: "Test template",
			RecommenderID:       "rec-1",
		}

		tmpl, err := b.CreateEmailTemplate("us-east-1", "123456789012", "be-email-1", req)
		require.NoError(t, err)

		assert.Equal(t, "Backend test subject", tmpl.Subject)
		assert.Equal(t, "<p>HTML content</p>", tmpl.HTMLPart)
		assert.Equal(t, "Text content", tmpl.TextPart)
		assert.Equal(t, "Test template", tmpl.TemplateDescription)
		assert.Equal(t, "rec-1", tmpl.RecommenderID)
		assert.Equal(t, "1", tmpl.Version)
		assert.NotEmpty(t, tmpl.CreationDate)
		assert.NotEmpty(t, tmpl.LastModifiedDate)
	})

	t.Run("update_persists_new_body", func(t *testing.T) {
		t.Parallel()

		req := pinpoint.ExportedCreateEmailTemplateRequest{
			Subject:  "Initial subject",
			HTMLPart: "<p>Initial</p>",
		}
		_, err := b.CreateEmailTemplate("us-east-1", "123456789012", "be-email-2", req)
		require.NoError(t, err)

		updateReq := pinpoint.ExportedCreateEmailTemplateRequest{
			Subject:  "Updated subject",
			HTMLPart: "<p>Updated HTML</p>",
		}
		updated, err := b.UpdateEmailTemplate("be-email-2", updateReq)
		require.NoError(t, err)

		assert.Equal(t, "Updated subject", updated.Subject)
		assert.Equal(t, "<p>Updated HTML</p>", updated.HTMLPart)
		assert.Equal(t, "2", updated.Version)
	})
}

func TestInAppTemplate_ContentStructure(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	content := []any{
		map[string]any{
			"BodyConfig": map[string]any{
				"Alignment": "CENTER",
				"Body":      "Upgrade to Premium today!",
				"TextColor": "#FFFFFF",
			},
			"HeaderConfig": map[string]any{
				"Alignment": "CENTER",
				"Header":    "Special Offer",
				"TextColor": "#FFFF00",
			},
			"BackgroundColor": "#0000FF",
			"PrimaryBtn": map[string]any{
				"DefaultConfig": map[string]any{
					"BackgroundColor": "#FFFFFF",
					"BorderRadius":    4,
					"ButtonAction":    "LINK",
					"Link":            "https://example.com/upgrade",
					"Text":            "Upgrade Now",
					"TextColor":       "#000000",
				},
			},
			"SecondaryBtn": map[string]any{
				"DefaultConfig": map[string]any{
					"ButtonAction": "CLOSE",
					"Text":         "Maybe Later",
					"TextColor":    "#888888",
				},
			},
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/promo-inapp/inapp",
		map[string]any{
			"Layout":              "BOTTOM_BANNER",
			"TemplateDescription": "Upgrade promo",
			"Content":             content,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/promo-inapp/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "BOTTOM_BANNER", tmpl["Layout"])
	assert.Equal(t, "Upgrade promo", tmpl["TemplateDescription"])

	gotContent := tmpl["Content"].([]any)
	require.Len(t, gotContent, 1)

	c0 := gotContent[0].(map[string]any)
	assert.Equal(t, "#0000FF", c0["BackgroundColor"])

	header := c0["HeaderConfig"].(map[string]any)
	assert.Equal(t, "Special Offer", header["Header"])
	assert.Equal(t, "#FFFF00", header["TextColor"])

	body := c0["BodyConfig"].(map[string]any)
	assert.Equal(t, "Upgrade to Premium today!", body["Body"])

	primary := c0["PrimaryBtn"].(map[string]any)
	defaultCfg := primary["DefaultConfig"].(map[string]any)
	assert.Equal(t, "LINK", defaultCfg["ButtonAction"])
	assert.Equal(t, "https://example.com/upgrade", defaultCfg["Link"])
}

func TestInAppTemplate_MultipleContentItems(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	content := []any{
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 1"},
			"BackgroundColor": "#FF0000",
		},
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 2"},
			"BackgroundColor": "#00FF00",
		},
		map[string]any{
			"BodyConfig":      map[string]any{"Body": "Slide 3"},
			"BackgroundColor": "#0000FF",
		},
	}

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/multi-slide/inapp",
		map[string]any{
			"Layout":  "CAROUSEL",
			"Content": content,
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/multi-slide/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	gotContent := tmpl["Content"].([]any)
	require.Len(t, gotContent, 3)
	assert.Equal(t, "#FF0000", gotContent[0].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#00FF00", gotContent[1].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#0000FF", gotContent[2].(map[string]any)["BackgroundColor"])
}

func TestInAppTemplate_UpdateContent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/inapp-update/inapp",
		map[string]any{
			"Layout": "MIDDLE_BANNER",
			"Content": []any{
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v1 content"},
					"BackgroundColor": "#111111",
				},
			},
		})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/inapp-update/inapp",
		map[string]any{
			"Layout": "TOP_BANNER",
			"Content": []any{
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v2 slide a"},
					"BackgroundColor": "#AAAAAA",
				},
				map[string]any{
					"BodyConfig":      map[string]any{"Body": "v2 slide b"},
					"BackgroundColor": "#BBBBBB",
				},
			},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/inapp-update/inapp", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "TOP_BANNER", tmpl["Layout"])
	content := tmpl["Content"].([]any)
	require.Len(t, content, 2)
	assert.Equal(t, "#AAAAAA", content[0].(map[string]any)["BackgroundColor"])
	assert.Equal(t, "#BBBBBB", content[1].(map[string]any)["BackgroundColor"])
}

// ──────────────────────────────────────────────────
// Endpoint — metrics field
// ──────────────────────────────────────────────────

func TestPinpoint_EmailTemplate_GetUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-email/email", map[string]any{
		"Subject":  "Hello",
		"HtmlPart": "<p>Hi</p>",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/templates/my-email/email", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/templates/my-email/email", map[string]any{
		"Subject": "Updated",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/templates/my-email/email", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestPinpoint_InAppTemplate_GetUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-inapp/inapp", map[string]any{
		"Layout": "TOP_BANNER",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/templates/my-inapp/inapp", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/templates/my-inapp/inapp", map[string]any{
		"Layout": "BOTTOM_BANNER",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/templates/my-inapp/inapp", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestDuplicateEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-email/email",
		map[string]any{"Subject": "Hello"})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-email/email",
		map[string]any{"Subject": "Hello again"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestDuplicateInAppTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-inapp/inapp", map[string]any{})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-inapp/inapp", map[string]any{})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// ──────────────────────────────────────────────────
// App-existence validation for app-scoped ops
// ──────────────────────────────────────────────────

func TestHandler_CreateEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_email_template",
			body:       map[string]any{"Subject": "Hello", "HtmlPart": "<p>Hi</p>"},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
		{
			name:       "creates_email_template_minimal",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-email-tpl/email", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}

func TestHandler_CreateInAppTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_inapp_template",
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-inapp-tpl/inapp", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}
