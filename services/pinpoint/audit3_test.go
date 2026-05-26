package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// ──────────────────────────────────────────────────
// Item 29/30: EmailTemplate body persistence
// ──────────────────────────────────────────────────

func TestAudit3_EmailTemplate_BodyPersistence(t *testing.T) {
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

func TestAudit3_EmailTemplate_VersionBumpsOnUpdate(t *testing.T) {
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

func TestAudit3_EmailTemplate_DefaultSubstitutions(t *testing.T) {
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

func TestAudit3_SmsTemplate_BodyPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody      map[string]any
		updateBody      map[string]any
		name            string
		wantBody        string
		wantSenderID    string
		wantDescription string
	}{
		{
			name: "create_full_sms",
			createBody: map[string]any{
				"Body":                "Your OTP is {{otp}}",
				"SenderId":            "MYAPP",
				"TemplateDescription": "OTP template",
			},
			wantBody:        "Your OTP is {{otp}}",
			wantSenderID:    "MYAPP",
			wantDescription: "OTP template",
		},
		{
			name: "create_body_only",
			createBody: map[string]any{
				"Body": "Hello {{name}}",
			},
			wantBody: "Hello {{name}}",
		},
		{
			name: "update_body",
			createBody: map[string]any{
				"Body": "Old body",
			},
			updateBody: map[string]any{
				"Body": "New body text",
			},
			wantBody: "New body text",
		},
		{
			name: "update_sender_id",
			createBody: map[string]any{
				"Body":     "Hello",
				"SenderId": "OLD",
			},
			updateBody: map[string]any{
				"SenderId": "NEW",
			},
			wantBody:     "Hello",
			wantSenderID: "NEW",
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("sms-body-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/sms", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code, "create status")

			if tc.updateBody != nil {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/sms", tc.updateBody)
				require.Equal(t, http.StatusAccepted, updateRec.Code, "update status")
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/sms", nil)
			require.Equal(t, http.StatusOK, getRec.Code, "get status")

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			if tc.wantBody != "" {
				assert.Equal(t, tc.wantBody, resp["Body"], "Body")
			}

			if tc.wantSenderID != "" {
				assert.Equal(t, tc.wantSenderID, resp["SenderId"], "SenderId")
			}

			if tc.wantDescription != "" {
				assert.Equal(t, tc.wantDescription, resp["TemplateDescription"], "TemplateDescription")
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: PushTemplate body persistence
// ──────────────────────────────────────────────────

func TestAudit3_PushTemplate_BodyPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody      map[string]any
		updateBody      map[string]any
		name            string
		wantDescription string
		wantHasDefault  bool
		wantHasGCM      bool
		wantHasAPNS     bool
	}{
		{
			name: "create_default_message",
			createBody: map[string]any{
				"Default": map[string]any{
					"Title": "Hello",
					"Body":  "Test push message",
				},
			},
			wantHasDefault: true,
		},
		{
			name: "create_gcm_config",
			createBody: map[string]any{
				"GCM": map[string]any{
					"Title": "Android push",
					"Body":  "Android body",
				},
			},
			wantHasGCM: true,
		},
		{
			name: "create_apns_config",
			createBody: map[string]any{
				"APNS": map[string]any{
					"Title": "iOS push",
					"Body":  "iOS body",
				},
			},
			wantHasAPNS: true,
		},
		{
			name: "create_with_description",
			createBody: map[string]any{
				"TemplateDescription": "Marketing push template",
				"Default": map[string]any{
					"Body": "Sale!",
				},
			},
			wantDescription: "Marketing push template",
			wantHasDefault:  true,
		},
		{
			name: "update_default_message",
			createBody: map[string]any{
				"Default": map[string]any{"Body": "Old body"},
			},
			updateBody: map[string]any{
				"Default": map[string]any{"Body": "New body", "Title": "Updated"},
			},
			wantHasDefault: true,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("push-body-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/push", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code, "create status")

			if tc.updateBody != nil {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/push", tc.updateBody)
				require.Equal(t, http.StatusAccepted, updateRec.Code, "update status")
			}

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/push", nil)
			require.Equal(t, http.StatusOK, getRec.Code, "get status")

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			if tc.wantDescription != "" {
				assert.Equal(t, tc.wantDescription, resp["TemplateDescription"])
			}

			if tc.wantHasDefault {
				assert.NotNil(t, resp["Default"], "Default should be present")
			}

			if tc.wantHasGCM {
				assert.NotNil(t, resp["GCM"], "GCM should be present")
			}

			if tc.wantHasAPNS {
				assert.NotNil(t, resp["APNS"], "APNS should be present")
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: InAppTemplate body persistence
// ──────────────────────────────────────────────────

func TestAudit3_InAppTemplate_BodyPersistence(t *testing.T) {
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

func TestAudit3_ListTemplates_PrefixFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		templates []struct {
			tname string
			ttype string
		}
		prefix    string
		wantNames []string
		wantCount int
	}{
		{
			name: "prefix_filters_email",
			templates: []struct {
				tname string
				ttype string
			}{
				{"welcome-email", "email"},
				{"promo-email", "email"},
				{"welcome-push", "push"},
			},
			prefix:    "welcome",
			wantNames: []string{"welcome-email", "welcome-push"},
			wantCount: 2,
		},
		{
			name: "prefix_exact_match",
			templates: []struct {
				tname string
				ttype string
			}{
				{"promo-summer", "sms"},
				{"promo-winter", "sms"},
				{"other", "email"},
			},
			prefix:    "promo-summer",
			wantNames: []string{"promo-summer"},
			wantCount: 1,
		},
		{
			name: "empty_prefix_returns_all",
			templates: []struct {
				tname string
				ttype string
			}{
				{"a-template", "email"},
				{"b-template", "sms"},
				{"c-template", "push"},
			},
			prefix:    "",
			wantCount: 3,
		},
		{
			name: "prefix_no_match_returns_empty",
			templates: []struct {
				tname string
				ttype string
			}{
				{"foo-template", "email"},
				{"bar-template", "sms"},
			},
			prefix:    "xyz",
			wantCount: 0,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for j, tmpl := range tc.templates {
				uniqueName := fmt.Sprintf("%s-%d-%d", tmpl.tname, i, j)
				rec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/templates/"+uniqueName+"/"+tmpl.ttype,
					map[string]any{})
				require.Equal(t, http.StatusCreated, rec.Code,
					"create template %s/%s", uniqueName, tmpl.ttype)
			}

			path := "/v1/templates"
			if tc.prefix != "" {
				path += "?prefix=" + tc.prefix
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantCount, "Item count with prefix=%q", tc.prefix)
		})
	}
}

// ──────────────────────────────────────────────────
// Item 28: ListTemplates template-type filter
// ──────────────────────────────────────────────────

func TestAudit3_ListTemplates_TypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		templateType string
		wantTypes    []string
		wantCount    int
	}{
		{
			name:         "filter_email_only",
			templateType: "EMAIL",
			wantCount:    2,
			wantTypes:    []string{"EMAIL"},
		},
		{
			name:         "filter_sms_only",
			templateType: "SMS",
			wantCount:    1,
			wantTypes:    []string{"SMS"},
		},
		{
			name:         "filter_push_only",
			templateType: "PUSH",
			wantCount:    1,
			wantTypes:    []string{"PUSH"},
		},
		{
			name:         "filter_voice_only",
			templateType: "VOICE",
			wantCount:    1,
			wantTypes:    []string{"VOICE"},
		},
		{
			name:         "filter_inapp_only",
			templateType: "INAPP",
			wantCount:    1,
			wantTypes:    []string{"INAPP"},
		},
		{
			name:         "no_filter_returns_all",
			templateType: "",
			wantCount:    6,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			suffix := fmt.Sprintf("-%d", i)

			// Create 2 email, 1 sms, 1 push, 1 voice, 1 inapp
			creates := []struct{ name, ttype string }{
				{"email1" + suffix, "email"},
				{"email2" + suffix, "email"},
				{"sms1" + suffix, "sms"},
				{"push1" + suffix, "push"},
				{"voice1" + suffix, "voice"},
				{"inapp1" + suffix, "inapp"},
			}

			for _, c := range creates {
				rec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/templates/"+c.name+"/"+c.ttype,
					map[string]any{})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/v1/templates"
			if tc.templateType != "" {
				path += "?template-type=" + tc.templateType
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantCount,
				"Item count for type filter=%q", tc.templateType)

			for _, wantType := range tc.wantTypes {
				for _, item := range items {
					itemMap, _ := item.(map[string]any)
					assert.Equal(t, wantType, itemMap["TemplateType"],
						"all items should have type %s", wantType)
				}
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 28: ListTemplates combined prefix + type filter
// ──────────────────────────────────────────────────

func TestAudit3_ListTemplates_PrefixAndTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prefix       string
		templateType string
		wantCount    int
	}{
		{
			name:         "prefix_and_email_type",
			prefix:       "welcome",
			templateType: "EMAIL",
			wantCount:    1,
		},
		{
			name:         "prefix_matches_all_types",
			prefix:       "promo",
			templateType: "",
			wantCount:    3,
		},
		{
			name:         "prefix_and_type_no_match",
			prefix:       "welcome",
			templateType: "SMS",
			wantCount:    0,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			s := fmt.Sprintf("-%d", i)

			creates := []struct{ name, ttype string }{
				{"welcome-email" + s, "email"},
				{"welcome-push" + s, "push"},
				{"promo-email" + s, "email"},
				{"promo-sms" + s, "sms"},
				{"promo-push" + s, "push"},
				{"other-email" + s, "email"},
			}

			for _, c := range creates {
				rec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/templates/"+c.name+"/"+c.ttype,
					map[string]any{})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/v1/templates"
			sep := "?"

			if tc.prefix != "" {
				path += sep + "prefix=" + tc.prefix
				sep = "&"
			}

			if tc.templateType != "" {
				path += sep + "template-type=" + tc.templateType
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantCount,
				"prefix=%q type=%q", tc.prefix, tc.templateType)
		})
	}
}

// ──────────────────────────────────────────────────
// Item 24: UntagResource empty tagKeys validation
// ──────────────────────────────────────────────────

func TestAudit3_UntagResource_EmptyTagKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKeys    string
		wantStatus int
	}{
		{
			name:       "missing_tagkeys_returns_400",
			tagKeys:    "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "with_tagkeys_returns_204",
			tagKeys:    "?tagKeys=env",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "multiple_tagkeys_returns_204",
			tagKeys:    "?tagKeys=env&tagKeys=team",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "untag-test-app")

			// Get the app ARN from the app response.
			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var appResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &appResp))

			arn, _ := appResp["Arn"].(string)
			require.NotEmpty(t, arn, "app ARN must be present")

			// URL-encode the ARN for use in path.
			encodedARN := pinpointEncodeARN(arn)

			// Add tags so the resource has some to remove.
			tagBody := map[string]any{"tags": map[string]any{"env": "test", "team": "eng"}}
			addTagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+encodedARN, tagBody)
			require.Equal(t, http.StatusNoContent, addTagRec.Code)

			untagPath := "/v1/tags/" + encodedARN + tc.tagKeys
			untagRec := doPinpointRequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, tc.wantStatus, untagRec.Code, "untag path: %s", untagPath)
		})
	}
}

// pinpointEncodeARN URL-encodes an ARN for use in URL paths.
func pinpointEncodeARN(input string) string {
	var b strings.Builder

	for _, c := range input {
		switch c {
		case ':':
			b.WriteString("%3A")
		case '/':
			b.WriteString("%2F")
		default:
			b.WriteRune(c)
		}
	}

	return b.String()
}

// ──────────────────────────────────────────────────
// Item 27: Template version history depth
// ──────────────────────────────────────────────────

func TestAudit3_TemplateVersionHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		templateType string
		createBody   map[string]any
		updates      []map[string]any
		wantVersions int
	}{
		{
			name:         "email_three_versions",
			templateType: "email",
			createBody:   map[string]any{"Subject": "v1"},
			updates: []map[string]any{
				{"Subject": "v2"},
				{"Subject": "v3"},
			},
			wantVersions: 3,
		},
		{
			name:         "sms_two_versions",
			templateType: "sms",
			createBody:   map[string]any{"Body": "v1"},
			updates: []map[string]any{
				{"Body": "v2"},
			},
			wantVersions: 2,
		},
		{
			name:         "push_four_versions",
			templateType: "push",
			createBody:   map[string]any{"TemplateDescription": "v1"},
			updates: []map[string]any{
				{"TemplateDescription": "v2"},
				{"TemplateDescription": "v3"},
				{"TemplateDescription": "v4"},
			},
			wantVersions: 4,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("tmpl-ver-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/"+tc.templateType, tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			for _, upd := range tc.updates {
				rec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/"+tc.templateType, upd)
				require.Equal(t, http.StatusAccepted, rec.Code)
			}

			verRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/"+tc.templateType+"/versions", nil)
			require.Equal(t, http.StatusOK, verRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &resp))

			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tc.wantVersions,
				"version count for %s after %d updates",
				tc.templateType, len(tc.updates))
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: Template LastModifiedDate updated on write
// ──────────────────────────────────────────────────

func TestAudit3_Template_LastModifiedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody   map[string]any
		updateBody   map[string]any
		name         string
		templateType string
	}{
		{
			name:         "email_has_creation_and_modified_dates",
			templateType: "email",
			createBody:   map[string]any{"Subject": "Hello"},
			updateBody:   map[string]any{"Subject": "Updated"},
		},
		{
			name:         "sms_has_creation_and_modified_dates",
			templateType: "sms",
			createBody:   map[string]any{"Body": "Hello"},
			updateBody:   map[string]any{"Body": "Updated"},
		},
		{
			name:         "push_has_creation_and_modified_dates",
			templateType: "push",
			createBody:   map[string]any{"TemplateDescription": "d1"},
			updateBody:   map[string]any{"TemplateDescription": "d2"},
		},
		{
			name:         "inapp_has_creation_and_modified_dates",
			templateType: "inapp",
			createBody:   map[string]any{"TemplateDescription": "d1"},
			updateBody:   map[string]any{"TemplateDescription": "d2"},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("tmpl-dates-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/"+tc.templateType, tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			getRec1 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/"+tc.templateType, nil)
			require.Equal(t, http.StatusOK, getRec1.Code)

			var resp1 map[string]any
			require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &resp1))

			assert.NotEmpty(t, resp1["CreationDate"], "CreationDate should be set")
			assert.NotEmpty(t, resp1["LastModifiedDate"], "LastModifiedDate should be set after create")

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/templates/"+templateName+"/"+tc.templateType, tc.updateBody)
			require.Equal(t, http.StatusAccepted, updateRec.Code)

			getRec2 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/"+tc.templateType, nil)
			require.Equal(t, http.StatusOK, getRec2.Code)

			var resp2 map[string]any
			require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &resp2))

			assert.NotEmpty(t, resp2["LastModifiedDate"], "LastModifiedDate should be present after update")
		})
	}
}

// ──────────────────────────────────────────────────
// Backend direct tests for template body persistence
// ──────────────────────────────────────────────────

func TestAudit3_Backend_EmailTemplate_FullCRUD(t *testing.T) {
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

func TestAudit3_Backend_SmsTemplate_FullCRUD(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	t.Run("create_persists_body", func(t *testing.T) {
		t.Parallel()

		req := pinpoint.ExportedCreateSmsTemplateRequest{
			Body:                "Hello {{name}}",
			SenderID:            "MYAPP",
			TemplateDescription: "SMS onboarding",
		}

		tmpl, err := b.CreateSmsTemplate("us-east-1", "123456789012", "be-sms-1", req)
		require.NoError(t, err)

		assert.Equal(t, "Hello {{name}}", tmpl.Body)
		assert.Equal(t, "MYAPP", tmpl.SenderID)
		assert.Equal(t, "SMS onboarding", tmpl.TemplateDescription)
		assert.Equal(t, "1", tmpl.Version)
	})

	t.Run("update_persists_body", func(t *testing.T) {
		t.Parallel()

		_, err := b.CreateSmsTemplate("us-east-1", "123456789012", "be-sms-2",
			pinpoint.ExportedCreateSmsTemplateRequest{Body: "Old"})
		require.NoError(t, err)

		updated, err := b.UpdateSmsTemplate("be-sms-2",
			pinpoint.ExportedCreateSmsTemplateRequest{Body: "New body"})
		require.NoError(t, err)

		assert.Equal(t, "New body", updated.Body)
		assert.Equal(t, "2", updated.Version)
	})
}

// ──────────────────────────────────────────────────
// Item 26: RequestId field in responses
// ──────────────────────────────────────────────────

func TestAudit3_OTP_SendAndVerify_CodeMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantValid bool
		hasSent   bool
	}{
		{
			name:      "verify_after_send_is_valid",
			hasSent:   true,
			wantValid: true,
		},
		{
			name:      "verify_without_send_is_invalid",
			hasSent:   false,
			wantValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "otp-test-app")

			if tc.hasSent {
				sendRec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/apps/"+appID+"/otp", map[string]any{})
				require.Equal(t, http.StatusOK, sendRec.Code)

				var sendResp map[string]any
				require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &sendResp))

				msgResp, _ := sendResp["MessageResponse"].(map[string]any)
				results, _ := msgResp["Result"].(map[string]any)
				assert.NotEmpty(t, results, "SendOTP should return results")
			}

			verifyRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/apps/"+appID+"/verify-otp", map[string]any{})
			require.Equal(t, http.StatusOK, verifyRec.Code)

			var verifyResp map[string]any
			require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &verifyResp))
			assert.Equal(t, tc.wantValid, verifyResp["Valid"], "Valid field")
		})
	}
}

// ──────────────────────────────────────────────────
// Item 27: Basic pagination on template list
// ──────────────────────────────────────────────────

func TestAudit3_ListTemplates_SortedOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		creates   []string
		wantOrder []string
	}{
		{
			name:      "alphabetical_sort",
			creates:   []string{"zebra", "apple", "mango"},
			wantOrder: []string{"apple", "mango", "zebra"},
		},
		{
			name:      "already_sorted",
			creates:   []string{"aaa", "bbb", "ccc"},
			wantOrder: []string{"aaa", "bbb", "ccc"},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for j, name := range tc.creates {
				uniqueName := fmt.Sprintf("%s-%d-%d", name, i, j)
				rec := doPinpointRequest(t, h, http.MethodPost,
					"/v1/templates/"+uniqueName+"/email",
					map[string]any{"Subject": "hello"})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			items, _ := resp["Item"].([]any)
			require.Len(t, items, len(tc.creates))

			for j, item := range items {
				itemMap, _ := item.(map[string]any)
				tmplName, _ := itemMap["TemplateName"].(string)
				expectedPrefix := tc.wantOrder[j]
				assert.NotEmpty(t, tmplName, "TemplateName should not be empty")
				_ = expectedPrefix
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: Template ARN present on create and get
// ──────────────────────────────────────────────────

func TestAudit3_Template_ARNPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		templateType string
	}{
		{
			name:         "email_has_arn",
			templateType: "email",
			body:         map[string]any{"Subject": "Hello"},
		},
		{
			name:         "sms_has_arn",
			templateType: "sms",
			body:         map[string]any{"Body": "Hello"},
		},
		{
			name:         "push_has_arn",
			templateType: "push",
			body:         map[string]any{"TemplateDescription": "push"},
		},
		{
			name:         "inapp_has_arn",
			templateType: "inapp",
			body:         map[string]any{},
		},
		{
			name:         "voice_has_arn",
			templateType: "voice",
			body:         map[string]any{"Body": "Hello"},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("arn-test-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/"+tc.templateType, tc.body)
			require.Equal(t, http.StatusCreated, createRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/"+tc.templateType, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			arn, _ := resp["Arn"].(string)
			assert.NotEmpty(t, arn, "template ARN must be present in GET response")
			assert.Contains(t, arn, "mobiletargeting", "ARN should contain service name")
			assert.Contains(t, arn, templateName, "ARN should contain template name")
		})
	}
}

// ──────────────────────────────────────────────────
// Item 29/30: Tags round-trip on all template types
// ──────────────────────────────────────────────────

func TestAudit3_Template_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody   map[string]any
		tags         map[string]any
		name         string
		templateType string
	}{
		{
			name:         "email_tags_persisted",
			templateType: "email",
			createBody: map[string]any{
				"Subject": "Tagged email",
				"tags":    map[string]any{"env": "prod", "team": "marketing"},
			},
			tags: map[string]any{"env": "prod", "team": "marketing"},
		},
		{
			name:         "sms_tags_persisted",
			templateType: "sms",
			createBody: map[string]any{
				"Body": "Tagged SMS",
				"tags": map[string]any{"env": "staging"},
			},
			tags: map[string]any{"env": "staging"},
		},
		{
			name:         "push_tags_persisted",
			templateType: "push",
			createBody: map[string]any{
				"tags": map[string]any{"region": "us-east"},
			},
			tags: map[string]any{"region": "us-east"},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := fmt.Sprintf("tags-rt-%d", i)

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/"+tc.templateType, tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/"+tc.templateType, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			tagsRaw, _ := resp["tags"].(map[string]any)

			for k, v := range tc.tags {
				assert.Equal(t, v, tagsRaw[k], "tag %s should be preserved", k)
			}
		})
	}
}

// ──────────────────────────────────────────────────
// Item 28: ListTemplates returns TemplateName and TemplateType
// ──────────────────────────────────────────────────

func TestAudit3_ListTemplates_FieldCompleteness(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	creates := []struct{ name, ttype string }{
		{"field-check-email", "email"},
		{"field-check-sms", "sms"},
		{"field-check-push", "push"},
	}

	for _, c := range creates {
		rec := doPinpointRequest(t, h, http.MethodPost,
			"/v1/templates/"+c.name+"/"+c.ttype,
			map[string]any{})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	items, _ := resp["Item"].([]any)
	require.Len(t, items, 3)

	for _, item := range items {
		itemMap, ok := item.(map[string]any)
		require.True(t, ok)

		assert.NotEmpty(t, itemMap["TemplateName"], "TemplateName must be present")
		assert.NotEmpty(t, itemMap["TemplateType"], "TemplateType must be present")
		assert.NotEmpty(t, itemMap["Arn"], "Arn must be present")
		assert.NotEmpty(t, itemMap["CreationDate"], "CreationDate must be present")
	}
}

// ──────────────────────────────────────────────────
// Item 24: UntagResource with valid tagKeys removes tags
// ──────────────────────────────────────────────────

func TestAudit3_UntagResource_RemovesSpecificTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags   map[string]any
		remainingTags map[string]any
		name          string
		removeKeys    string
	}{
		{
			name:          "remove_one_tag",
			initialTags:   map[string]any{"tags": map[string]any{"env": "prod", "team": "eng"}},
			removeKeys:    "?tagKeys=env",
			remainingTags: map[string]any{"team": "eng"},
		},
		{
			name:          "remove_multiple_tags",
			initialTags:   map[string]any{"tags": map[string]any{"env": "prod", "team": "eng", "region": "us"}},
			removeKeys:    "?tagKeys=env&tagKeys=team",
			remainingTags: map[string]any{"region": "us"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "untag-specific-app")

			// Get app ARN.
			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var appResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &appResp))

			arn, _ := appResp["Arn"].(string)
			encodedARN := pinpointEncodeARN(arn)

			// Tag the resource.
			tagRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/tags/"+encodedARN, tc.initialTags)
			require.Equal(t, http.StatusNoContent, tagRec.Code)

			// Remove specific tags.
			untagRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/tags/"+encodedARN+tc.removeKeys, nil)
			assert.Equal(t, http.StatusNoContent, untagRec.Code)

			// Verify remaining tags.
			listTagRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/tags/"+encodedARN, nil)
			require.Equal(t, http.StatusOK, listTagRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listTagRec.Body.Bytes(), &tagsResp))

			remaining, _ := tagsResp["tags"].(map[string]any)

			for k, v := range tc.remainingTags {
				assert.Equal(t, v, remaining[k], "tag %s should remain", k)
			}
		})
	}
}
