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

func TestListTemplates_SMSListed(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/ls1/sms", map[string]any{"Body": "code"})
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
		if m["TemplateType"] == "SMS" {
			found = true

			break
		}
	}
	assert.True(t, found, "SMS template must appear in ListTemplates")
}

func TestSmsTemplate_BodyPersistence(t *testing.T) {
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

func TestPushTemplate_BodyPersistence(t *testing.T) {
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

func TestBackend_SmsTemplate_FullCRUD(t *testing.T) {
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

func TestPushTemplate_PerPlatformOverrides(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	createRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/templates/promo-push/push",
		map[string]any{
			"Body":  "Default body",
			"Title": "Default title",
			"APNS": map[string]any{
				"Body":  "iOS promo",
				"Title": "iOS title",
				"Sound": "default",
				"Badge": 1,
			},
			"GCM": map[string]any{
				"Body":          "Android promo",
				"Title":         "Android title",
				"Sound":         "notification.mp3",
				"IconReference": "ic_notification",
			},
			"TemplateDescription": "Cross-platform promo push",
		})
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/promo-push/push", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Default body", tmpl["Body"])
	assert.Equal(t, "Default title", tmpl["Title"])

	apns := tmpl["APNS"].(map[string]any)
	assert.Equal(t, "iOS promo", apns["Body"])
	assert.Equal(t, "iOS title", apns["Title"])
	assert.Equal(t, "default", apns["Sound"])

	gcm := tmpl["GCM"].(map[string]any)
	assert.Equal(t, "Android promo", gcm["Body"])
	assert.Equal(t, "Android title", gcm["Title"])
	assert.Equal(t, "notification.mp3", gcm["Sound"])
}

func TestPushTemplate_UpdatePerPlatform(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/push-upd/push",
		map[string]any{
			"Body": "v1 body",
			"APNS": map[string]any{"Body": "v1 ios"},
		})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/push-upd/push",
		map[string]any{
			"Body": "v2 body",
			"APNS": map[string]any{"Body": "v2 ios", "Sound": "ding"},
			"GCM":  map[string]any{"Body": "v2 android"},
		})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/push-upd/push", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "v2 body", tmpl["Body"])
	apns := tmpl["APNS"].(map[string]any)
	assert.Equal(t, "v2 ios", apns["Body"])
	assert.Equal(t, "ding", apns["Sound"])

	gcm := tmpl["GCM"].(map[string]any)
	assert.Equal(t, "v2 android", gcm["Body"])
}

func TestSMSTemplate_SenderID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		senderID string
	}{
		{name: "with_sender_id", senderID: "MyBrand"},
		{name: "empty_sender_id", senderID: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			tmplName := "sms-sender-" + tc.name

			createRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/templates/"+tmplName+"/sms",
				map[string]any{
					"Body":     "Hello from {{sender}}",
					"SenderId": tc.senderID,
				})
			require.Equal(t, http.StatusCreated, createRec.Code)

			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/"+tmplName+"/sms", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var tmpl map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

			assert.Equal(t, "Hello from {{sender}}", tmpl["Body"])

			if tc.senderID != "" {
				assert.Equal(t, tc.senderID, tmpl["SenderId"])
			}
		})
	}
}

func TestSMSTemplate_UpdateSenderID(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	doPinpointRequest(t, h, http.MethodPost, "/v1/templates/sms-update/sms",
		map[string]any{"Body": "Hello", "SenderId": "OldBrand"})

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/templates/sms-update/sms",
		map[string]any{"Body": "Hello v2", "SenderId": "NewBrand"})
	require.Equal(t, http.StatusAccepted, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/templates/sms-update/sms", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var tmpl map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tmpl))

	assert.Equal(t, "Hello v2", tmpl["Body"])
	assert.Equal(t, "NewBrand", tmpl["SenderId"])
}

// ──────────────────────────────────────────────────
// Event streams — Kinesis
// ──────────────────────────────────────────────────

func TestPinpoint_PushTemplate_GetUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-push/push", map[string]any{
		"Default": map[string]any{"Title": "Ping", "Body": "Hello"},
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/templates/my-push/push", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/templates/my-push/push", map[string]any{
		"Default": map[string]any{"Title": "Updated"},
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/templates/my-push/push", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestPinpoint_SmsTemplate_GetUpdateDelete(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-sms/sms", map[string]any{
		"Body": "Your code: {{Code}}",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/templates/my-sms/sms", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/templates/my-sms/sms", map[string]any{
		"Body": "Code: {{Code}}",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/templates/my-sms/sms", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestDuplicatePushTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-push/push", map[string]any{})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-push/push", map[string]any{})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestDuplicateSmsTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-sms/sms",
		map[string]any{"Body": "Hi"})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-sms/sms",
		map[string]any{"Body": "Hi again"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandler_CreatePushTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_push_template",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-push-tpl/push", tt.body)
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

func TestHandler_CreateSmsTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_sms_template",
			body:       map[string]any{"Body": "Hello {{User.UserAttributes.FirstName}}"},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-sms-tpl/sms", tt.body)
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
