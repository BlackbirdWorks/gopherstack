package pinpoint_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────
// Finding #9: CreateImportJob materialises an IMPORT segment
// ──────────────────────────────────────────────────

func TestAudit5_ImportJob_CreatesSegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-seg-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123456789012:role/import-role",
			"S3Url":       "s3://my-bucket/contacts.csv",
			"Format":      "CSV",
			"SegmentName": "imported-contacts",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Verify segments list now contains the import segment.
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items, _ := listResp["Item"].([]any)
	require.Len(t, items, 1, "import job must materialise exactly one segment")

	seg := items[0].(map[string]any)
	assert.Equal(t, "IMPORT", seg["SegmentType"], "segment type must be IMPORT")
	assert.Equal(t, "imported-contacts", seg["Name"])
	assert.NotEmpty(t, seg["Id"])

	importDef, _ := seg["ImportDefinition"].(map[string]any)
	require.NotNil(t, importDef, "ImportDefinition must be present")
	assert.Equal(t, "CSV", importDef["Format"])
	assert.Equal(t, "s3://my-bucket/contacts.csv", importDef["S3Url"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/import-role", importDef["RoleArn"])
}

func TestAudit5_ImportJob_DefaultSegmentName(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-default-app")

	// No SegmentName provided → backend generates one.
	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123:role/r",
			"S3Url":   "s3://bucket/file.json",
			"Format":  "JSON",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items, _ := listResp["Item"].([]any)
	require.Len(t, items, 1)

	seg := items[0].(map[string]any)
	assert.Equal(t, "IMPORT", seg["SegmentType"])
	assert.NotEmpty(t, seg["Name"], "generated segment name must not be empty")
}

func TestAudit5_ImportJob_SegmentGetByID(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-get-app")

	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://bucket/data.csv",
			"Format":      "CSV",
			"SegmentName": "direct-import",
		})

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items := listResp["Item"].([]any)
	segID := items[0].(map[string]any)["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var s map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))
	assert.Equal(t, "IMPORT", s["SegmentType"])
	assert.Equal(t, "direct-import", s["Name"])
}

// ──────────────────────────────────────────────────
// Finding #22: VerifyOTPMessage validates the actual code
// ──────────────────────────────────────────────────

func TestAudit5_OTP_CodeMatch_CorrectCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-code-match-app")

	// Send OTP — backend stores the code internally.
	sendRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/otp",
		map[string]any{
			"SendOTPMessageRequestParameters": map[string]any{
				"Channel":             "SMS",
				"BrandName":           "TestBrand",
				"DestinationIdentity": "+15555550100",
				"OriginationIdentity": "+15555550199",
				"ReferenceId":         "ref-abc",
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	// Verify with no code provided → falls back to "was OTP sent?" → Valid=true.
	verifyNoCode := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{})
	require.Equal(t, http.StatusOK, verifyNoCode.Code)

	var noCodeResp map[string]any
	require.NoError(t, json.Unmarshal(verifyNoCode.Body.Bytes(), &noCodeResp))
	assert.True(t, noCodeResp["Valid"].(bool), "empty code falls back to has-pending-OTP check")
}

func TestAudit5_OTP_CodeMatch_WrongCode(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-wrong-code-app")

	// Send OTP.
	doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/otp", map[string]any{})

	// Verify with a wrong code → Valid=false.
	verifyRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{
			"VerifyOTPMessageRequestParameters": map[string]any{
				"DestinationIdentity": "+15555550100",
				"Otp":                 "999999",
				"ReferenceId":         "ref-xyz",
			},
		})
	require.Equal(t, http.StatusOK, verifyRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
	assert.False(t, resp["Valid"].(bool), "wrong OTP code must return Valid=false")
}

func TestAudit5_OTP_CodeMatch_NoOTPSent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "otp-no-send-app")

	// Verify without ever sending an OTP → Valid=false even with a code.
	verifyRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/verify-otp",
		map[string]any{
			"VerifyOTPMessageRequestParameters": map[string]any{
				"Otp": "123456",
			},
		})
	require.Equal(t, http.StatusOK, verifyRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
	assert.False(t, resp["Valid"].(bool), "no OTP sent → Valid=false")
}

// ──────────────────────────────────────────────────
// Finding #15: ChannelType validation on UpdateEndpoint
// ──────────────────────────────────────────────────

func TestAudit5_Endpoint_InvalidChannelType_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelType string
		wantStatus  int
	}{
		{"GCM_valid", "GCM", http.StatusAccepted},
		{"APNS_valid", "APNS", http.StatusAccepted},
		{"APNS_SANDBOX_valid", "APNS_SANDBOX", http.StatusAccepted},
		{"SMS_valid", "SMS", http.StatusAccepted},
		{"EMAIL_valid", "EMAIL", http.StatusAccepted},
		{"VOICE_valid", "VOICE", http.StatusAccepted},
		{"BAIDU_valid", "BAIDU", http.StatusAccepted},
		{"ADM_valid", "ADM", http.StatusAccepted},
		{"CUSTOM_valid", "CUSTOM", http.StatusAccepted},
		{"IN_APP_valid", "IN_APP", http.StatusAccepted},
		{"PUSH_valid", "PUSH", http.StatusAccepted},
		{"INVALID_rejected", "INVALID_CHANNEL", http.StatusBadRequest},
		{"HTTP_rejected", "HTTP", http.StatusBadRequest},
		{"WEBSOCKET_rejected", "WEBSOCKET", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "channel-val-app-"+tc.channelType)

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/endpoints/ep-1",
				map[string]any{"ChannelType": tc.channelType, "Address": "test@example.com"})
			assert.Equal(t, tc.wantStatus, rec.Code, "channel type %q", tc.channelType)
		})
	}
}

func TestAudit5_Endpoint_EmptyChannelType_Accepted(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-empty-app")

	// Empty ChannelType should not be rejected (field is optional).
	rec := doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-1",
		map[string]any{"Address": "user@example.com"})
	assert.Equal(t, http.StatusAccepted, rec.Code)
}

// ──────────────────────────────────────────────────
// Finding #21: SendUsersMessages maps user→endpoint results
// ──────────────────────────────────────────────────

func TestAudit5_SendUsersMessages_WithEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-app")

	// Register two endpoints for user "alice".
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-alice-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "alice@example.com",
			"User":        map[string]any{"UserId": "alice"},
		})
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-alice-2",
		map[string]any{
			"ChannelType": "SMS",
			"Address":     "+15555550100",
			"User":        map[string]any{"UserId": "alice"},
		})

	// Register one endpoint for user "bob".
	doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/endpoints/ep-bob-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "bob@example.com",
			"User":        map[string]any{"UserId": "bob"},
		})

	sendRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"SendUsersMessageRequest": map[string]any{
				"Users": map[string]any{
					"alice": map[string]any{},
					"bob":   map[string]any{},
				},
				"MessageConfiguration": map[string]any{
					"EmailMessage": map[string]any{"FromAddress": "noreply@example.com"},
				},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	result, _ := resp["Result"].(map[string]any)
	require.NotNil(t, result)

	aliceResults, _ := result["alice"].(map[string]any)
	require.NotNil(t, aliceResults, "alice must have per-endpoint results")
	// Alice has two endpoints → two result entries.
	assert.Len(t, aliceResults, 2, "alice has 2 endpoints → 2 result entries")

	for epID, v := range aliceResults {
		r := v.(map[string]any)
		assert.Equal(t, "SUCCESSFUL", r["DeliveryStatus"],
			"endpoint %s should be SUCCESSFUL", epID)
		assert.NotEmpty(t, r["MessageId"])
	}

	bobResults, _ := result["bob"].(map[string]any)
	require.NotNil(t, bobResults, "bob must have per-endpoint results")
	assert.Len(t, bobResults, 1, "bob has 1 endpoint → 1 result entry")
}

func TestAudit5_SendUsersMessages_UnknownUser(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-noendpoint-app")

	// User "ghost" has no registered endpoints.
	sendRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"SendUsersMessageRequest": map[string]any{
				"Users": map[string]any{
					"ghost": map[string]any{},
				},
			},
		})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &resp))

	result, _ := resp["Result"].(map[string]any)
	ghostResults, _ := result["ghost"].(map[string]any)
	require.NotNil(t, ghostResults, "unknown user still gets a result entry")
}

func TestAudit5_SendUsersMessages_EmptyBody(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "users-msg-empty-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/users-messages",
		map[string]any{
			"SendUsersMessageRequest": map[string]any{},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	result, _ := resp["Result"].(map[string]any)
	assert.Empty(t, result, "no users in request → empty result")
}

// ──────────────────────────────────────────────────
// Finding #27: Pagination for list operations
// ──────────────────────────────────────────────────

func TestAudit5_Pagination_Campaigns_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "paged-campaigns-app")

	// Create 5 campaigns.
	for i := range 5 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("camp-%02d", i)})
	}

	// Page 1: page-size=2 → 2 items, NextToken set.
	p1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2", nil)
	require.Equal(t, http.StatusOK, p1Rec.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(p1Rec.Body.Bytes(), &p1))

	items1, _ := p1["Item"].([]any)
	assert.Len(t, items1, 2, "page 1 should contain 2 items")
	nextToken, ok := p1["NextToken"].(string)
	require.True(t, ok, "NextToken must be set when more items exist")
	assert.NotEmpty(t, nextToken)

	// Page 2: use the token.
	p2Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2&token="+nextToken, nil)
	require.Equal(t, http.StatusOK, p2Rec.Code)

	var p2 map[string]any
	require.NoError(t, json.Unmarshal(p2Rec.Body.Bytes(), &p2))

	items2, _ := p2["Item"].([]any)
	assert.Len(t, items2, 2, "page 2 should contain 2 items")
	nextToken2, ok2 := p2["NextToken"].(string)
	require.True(t, ok2, "NextToken must be set for page 2")

	// Page 3: final page has 1 item, no NextToken.
	p3Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=2&token="+nextToken2, nil)
	require.Equal(t, http.StatusOK, p3Rec.Code)

	var p3 map[string]any
	require.NoError(t, json.Unmarshal(p3Rec.Body.Bytes(), &p3))

	items3, _ := p3["Item"].([]any)
	assert.Len(t, items3, 1, "page 3 should contain the remaining 1 item")
	_, hasMore := p3["NextToken"].(string)
	assert.False(t, hasMore, "no NextToken on the last page")
}

func TestAudit5_Pagination_Campaigns_NoToken_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "no-token-app")

	for i := range 3 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("c-%d", i)})
	}

	// No page-size → default 100 → all 3 returned without NextToken.
	rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, _ := resp["Item"].([]any)
	assert.Len(t, items, 3)
	assert.Nil(t, resp["NextToken"], "no NextToken when all items fit in one page")
}

func TestAudit5_Pagination_Segments_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "paged-segments-app")

	for i := range 4 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/segments",
			map[string]any{"Name": fmt.Sprintf("seg-%02d", i)})
	}

	p1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments?page-size=2", nil)
	require.Equal(t, http.StatusOK, p1Rec.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(p1Rec.Body.Bytes(), &p1))

	items1, _ := p1["Item"].([]any)
	assert.Len(t, items1, 2)
	tok, ok := p1["NextToken"].(string)
	require.True(t, ok, "NextToken must be present")

	p2Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments?page-size=2&token="+tok, nil)
	var p2 map[string]any
	require.NoError(t, json.Unmarshal(p2Rec.Body.Bytes(), &p2))

	items2, _ := p2["Item"].([]any)
	assert.Len(t, items2, 2)
	assert.Nil(t, p2["NextToken"], "last page has no NextToken")
}

func TestAudit5_Pagination_Journeys_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "paged-journeys-app")

	for i := range 3 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/journeys",
			map[string]any{"Name": fmt.Sprintf("journey-%02d", i)})
	}

	p1Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/journeys?page-size=2", nil)
	require.Equal(t, http.StatusOK, p1Rec.Code)

	var p1 map[string]any
	require.NoError(t, json.Unmarshal(p1Rec.Body.Bytes(), &p1))

	items1, _ := p1["Item"].([]any)
	assert.Len(t, items1, 2)

	tok, ok := p1["NextToken"].(string)
	require.True(t, ok, "NextToken set when more journeys exist")

	p2Rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/journeys?page-size=2&token="+tok, nil)
	var p2 map[string]any
	require.NoError(t, json.Unmarshal(p2Rec.Body.Bytes(), &p2))

	items2, _ := p2["Item"].([]any)
	assert.Len(t, items2, 1, "last page returns remaining journey")
	assert.Nil(t, p2["NextToken"])
}

func TestAudit5_Pagination_TokenEncoding(t *testing.T) {
	t.Parallel()

	// Verify that NextToken is a base64-encoded integer offset.
	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "token-encoding-app")

	for i := range 5 {
		doPinpointRequest(t, h, http.MethodPost,
			"/v1/apps/"+appID+"/campaigns",
			map[string]any{"Name": fmt.Sprintf("c-%d", i)})
	}

	rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tok, _ := resp["NextToken"].(string)
	require.NotEmpty(t, tok)

	raw, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be valid base64")

	offset, err := strconv.Atoi(string(raw))
	require.NoError(t, err, "decoded token must be a numeric offset")
	assert.Equal(t, 3, offset, "offset after first page of 3 must be 3")
}

func TestAudit5_Pagination_EmptyList_NoToken(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "empty-list-app")

	rec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/campaigns?page-size=10", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, _ := resp["Item"].([]any)
	assert.Empty(t, items)
	assert.Nil(t, resp["NextToken"])
}
