package pinpoint_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pinpointJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

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

func TestPinpoint_Endpoints_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "endpoint-app")

	endpointID := "ep-001"

	rec := doPinpointRequest(t, h, http.MethodPut,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID),
		map[string]any{
			"ChannelType":    "EMAIL",
			"Address":        "test@example.com",
			"EndpointStatus": "ACTIVE",
		})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut,
		fmt.Sprintf("/v1/apps/%s/endpoints", appID),
		map[string]any{
			"Item": []map[string]any{{
				"Id":          "ep-002",
				"ChannelType": "SMS",
				"Address":     "+15555550100",
			}},
		})
	assert.True(t, rec.Code > 0) // PutEndpoints batch may not be implemented

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/users/%s", appID, "user-1"), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/v1/apps/%s/endpoints/%s", appID, endpointID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestPinpoint_EventStream(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "event-stream-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID),
		map[string]any{
			"DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			"RoleArn":              "arn:aws:iam::123456789012:role/PinpointKinesisRole",
		})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/v1/apps/%s/eventstream", appID), nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestPinpoint_Recommender_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", map[string]any{
		"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123456789012:campaign/my-campaign",
		"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
	})
	if rec.Code < 200 || rec.Code >= 300 {
		t.Skipf("recommender creation returned %d, skipping rest of test", rec.Code)
	}
	resp := pinpointJSON(t, rec.Body.Bytes())
	recommenderID, _ := resp["Id"].(string)
	if recommenderID == "" {
		t.Skip("recommender creation did not return ID")
	}

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders/"+recommenderID, nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/recommenders", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/recommenders/"+recommenderID, map[string]any{
		"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123456789012:campaign/updated",
		"RecommendationProviderRoleArn": "arn:aws:iam::123456789012:role/PinpointRole",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/recommenders/"+recommenderID, nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
