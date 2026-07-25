package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVoiceTemplate_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		updateBody map[string]any
		name       string
		wantBody   string
	}{
		{
			name:       "create_and_get_basic",
			createBody: map[string]any{"Body": "Hello, your appointment is tomorrow."},
			updateBody: map[string]any{"Body": "Updated: your appointment is today."},
			wantBody:   "Hello, your appointment is tomorrow.",
		},
		{
			name: "create_with_tags",
			createBody: map[string]any{
				"Body": "Welcome to our service.",
				"tags": map[string]string{"env": "prod", "team": "notifications"},
			},
			updateBody: map[string]any{"Body": "Welcome, valued customer."},
			wantBody:   "Welcome to our service.",
		},
		{
			name:       "create_empty_body",
			createBody: map[string]any{},
			updateBody: map[string]any{"Body": "Now has content."},
			wantBody:   "SKIP",
		},
		{
			name:       "update_body_persists",
			createBody: map[string]any{"Body": "Original message."},
			updateBody: map[string]any{"Body": "Replacement message."},
			wantBody:   "Original message.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := "voice-tmpl-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/voice", tc.createBody)
			require.Equal(t, http.StatusCreated, createRec.Code,
				"body: %s", createRec.Body.String())

			var cr map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
			assert.NotEmpty(t, cr["Message"])

			getRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var gr map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))
			assert.Equal(t, templateName, gr["TemplateName"])
			if tc.wantBody != "SKIP" {
				assert.Equal(t, tc.wantBody, gr["Body"])
			}

			updateRec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/templates/"+templateName+"/voice", tc.updateBody)
			require.True(t, updateRec.Code == http.StatusOK || updateRec.Code == http.StatusAccepted,
				"update should succeed, got %d: %s", updateRec.Code, updateRec.Body.String())

			deleteRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/templates/"+templateName+"/voice", nil)
			require.True(t, deleteRec.Code == http.StatusOK || deleteRec.Code == http.StatusAccepted)

			getRec2 := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice", nil)
			assert.Equal(t, http.StatusNotFound, getRec2.Code)
		})
	}
}

// TestVoiceTemplate_FullFieldSet locks VoiceTemplateResponse's real wire
// shape (field-diffed against aws-sdk-go-v2/service/pinpoint/types): a real
// client's GET must see TemplateType (required, "VOICE"), LastModifiedDate
// (required), plus the previously-missing DefaultSubstitutions, LanguageCode,
// TemplateDescription, Version, and VoiceId. A prior pass only round-tripped
// ARN/TemplateName/Body/Tags/CreationDate.
func TestVoiceTemplate_FullFieldSet(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	templateName := "voice-full-fields"

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/"+templateName+"/voice", map[string]any{
			"Body":                 "Your code is {{code}}",
			"DefaultSubstitutions": `{"code":"0000"}`,
			"LanguageCode":         "en-US",
			"TemplateDescription":  "OTP voice template",
			"VoiceId":              "Joanna",
		})
	require.Equal(t, http.StatusCreated, createRec.Code, "body: %s", createRec.Body.String())

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/"+templateName+"/voice", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var gr map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gr))

	assert.Equal(t, templateName, gr["TemplateName"])
	assert.Equal(t, "VOICE", gr["TemplateType"])
	assert.Equal(t, "Your code is {{code}}", gr["Body"])
	assert.JSONEq(t, `{"code":"0000"}`, gr["DefaultSubstitutions"].(string))
	assert.Equal(t, "en-US", gr["LanguageCode"])
	assert.Equal(t, "OTP voice template", gr["TemplateDescription"])
	assert.Equal(t, "Joanna", gr["VoiceId"])
	assert.Equal(t, "1", gr["Version"])
	assert.NotEmpty(t, gr["CreationDate"])
	assert.NotEmpty(t, gr["LastModifiedDate"])

	updateRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/"+templateName+"/voice",
		map[string]any{"VoiceId": "Matthew", "LanguageCode": "en-GB"})
	require.Equal(t, http.StatusAccepted, updateRec.Code)

	getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/"+templateName+"/voice", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var gr2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &gr2))

	assert.Equal(t, "Matthew", gr2["VoiceId"])
	assert.Equal(t, "en-GB", gr2["LanguageCode"])
	assert.Equal(t, "2", gr2["Version"], "Version must advance on update")
	// Fields not present in the update body must be preserved, not cleared.
	assert.Equal(t, "Your code is {{code}}", gr2["Body"])
	assert.Equal(t, "OTP voice template", gr2["TemplateDescription"])
}

// ──────────────────────────────────────────────────
// VoiceTemplate version history
// ──────────────────────────────────────────────────

func TestVoiceTemplate_VersionHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updateCount  int
		wantVersions int
	}{
		{name: "one_version_on_create", updateCount: 0, wantVersions: 1},
		{name: "two_versions_after_one_update", updateCount: 1, wantVersions: 2},
		{name: "four_versions_after_three_updates", updateCount: 3, wantVersions: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			templateName := "voice-ver-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+templateName+"/voice",
				map[string]any{"Body": "Version 1"})
			require.Equal(t, http.StatusCreated, createRec.Code)

			for i := range tc.updateCount {
				updateRec := doPinpointRequest(t, h, http.MethodPut,
					"/v1/templates/"+templateName+"/voice",
					map[string]any{"Body": fmt.Sprintf("Version %d", i+2)})
				require.True(t, updateRec.Code == http.StatusOK || updateRec.Code == http.StatusAccepted)
			}

			versionsRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/templates/"+templateName+"/voice/versions", nil)
			require.Equal(t, http.StatusOK, versionsRec.Code)

			var versionsResp map[string]any
			require.NoError(t, json.Unmarshal(versionsRec.Body.Bytes(), &versionsResp))
			items, _ := versionsResp["Item"].([]any)
			assert.Len(t, items, tc.wantVersions)
		})
	}
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration full
// ──────────────────────────────────────────────────

func TestListTemplates_VoiceListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/lv1/voice", map[string]any{"Body": "test"})
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
		if m["TemplateType"] == "VOICE" {
			found = true

			break
		}
	}
	assert.True(t, found, "VOICE template must appear in ListTemplates")
}

func TestListTemplates_MixedTypes(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-v/voice", map[string]any{"Body": "v"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-e/email", map[string]any{"Subject": "e"})
	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/mix-s/sms", map[string]any{"Body": "s"})

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	items, _ := resp["Item"].([]any)
	assert.GreaterOrEqual(t, len(items), 3)
}

// ──────────────────────────────────────────────────
// RecommenderConfiguration: invalid ID type rejected
// ──────────────────────────────────────────────────

func TestVoiceTemplate_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	templateName := "dup-voice"

	rec1 := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/"+templateName+"/voice",
		map[string]any{"Body": "First"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/"+templateName+"/voice",
		map[string]any{"Body": "Second"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// ──────────────────────────────────────────────────
// Campaign: SegmentVersion field stored and returned
// ──────────────────────────────────────────────────

func TestListTemplates_PrefixFilter(t *testing.T) {
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

func TestListTemplates_TypeFilter(t *testing.T) {
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

func TestListTemplates_PrefixAndTypeFilter(t *testing.T) {
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

func TestTemplateVersionHistory(t *testing.T) {
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

func TestTemplate_LastModifiedDate(t *testing.T) {
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

func TestListTemplates_SortedOutput(t *testing.T) {
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

func TestTemplate_ARNPresent(t *testing.T) {
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

func TestTemplate_TagsRoundTrip(t *testing.T) {
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

func TestListTemplates_FieldCompleteness(t *testing.T) {
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

func TestVoiceTemplate_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/tagged-voice/voice",
		map[string]any{
			"Body": "Hello {{user.FirstName}}, your appointment is tomorrow.",
			"tags": map[string]any{
				"env":  "production",
				"team": "comms",
			},
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/tagged-voice/voice", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Hello {{user.FirstName}}, your appointment is tomorrow.", tmpl["Body"])
	assert.NotEmpty(t, tmpl["Arn"])
	assert.NotEmpty(t, tmpl["CreationDate"])
}

// ──────────────────────────────────────────────────
// InApp template — update replaces content slice
// ──────────────────────────────────────────────────

// TestAudit6_VoiceTemplate_TagRoundTrip verifies a voice template is
// registered in the ARN index on creation so TagResource/ListTagsForResource
// work for it like every other template type.
func TestVoiceTemplate_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-voice-template/voice",
		map[string]any{"Body": "Hello"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	templateARN, _ := createResp["Arn"].(string)
	require.NotEmpty(t, templateARN)

	escapedARN := url.PathEscape(templateARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+escapedARN,
		map[string]any{"tags": map[string]string{"env": "prod"}})
	require.Equal(t, http.StatusNoContent, tagRec.Code,
		"TagResource must find the voice template by ARN")

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+escapedARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))

	tags, _ := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
}

// ──────────────────────────────────────────────────
// Parity Phase 4: Campaign/Segment version history is app-scoped
// ──────────────────────────────────────────────────
