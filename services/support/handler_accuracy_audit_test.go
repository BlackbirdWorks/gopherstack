package support_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func decodeSupportResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

	return result
}

func createAccurateCase(t *testing.T, h *support.Handler, extra map[string]any) string {
	t.Helper()

	body := map[string]any{
		"subject":           "API fidelity case",
		"communicationBody": "Initial customer report",
		"serviceCode":       "amazon-s3",
		"categoryCode":      "general-guidance",
		"severityCode":      "normal",
		"language":          "en",
		"issueType":         "technical",
	}
	maps.Copy(body, extra)
	rec := doSupportRequest(t, h, "CreateCase", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return decodeSupportResponse(t, rec)["caseId"].(string)
}

func TestAccuracy_CreateCaseValidation(t *testing.T) {
	t.Parallel()

	elevenEmails := make([]string, 11)
	for i := range elevenEmails {
		elevenEmails[i] = "copy@example.test"
	}
	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing communication body", body: map[string]any{"subject": "invalid"}},
		{
			name: "invalid language",
			body: map[string]any{"subject": "invalid", "communicationBody": "body", "language": "xx"},
		},
		{
			name: "invalid issue type",
			body: map[string]any{"subject": "invalid", "communicationBody": "body", "issueType": "incident"},
		},
		{
			name: "invalid severity",
			body: map[string]any{"subject": "invalid", "communicationBody": "body", "severityCode": "blocker"},
		},
		{
			name: "too many cc recipients",
			body: map[string]any{
				"subject": "invalid", "communicationBody": "body", "ccEmailAddresses": elevenEmails,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, newTestSupportHandler(t), "CreateCase", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestAccuracy_CaseDetailsInitialCommunicationAndReopen(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	caseID := createAccurateCase(t, h, map[string]any{
		"ccEmailAddresses": []string{"billing@example.test"},
	})

	describeRec := doSupportRequest(t, h, "DescribeCases", map[string]any{"caseIdList": []string{caseID}})
	require.Equal(t, http.StatusOK, describeRec.Code)
	cases := decodeSupportResponse(t, describeRec)["cases"].([]any)
	require.Len(t, cases, 1)
	cs := cases[0].(map[string]any)
	assert.NotEmpty(t, cs["displayId"])
	assert.Equal(t, "en", cs["language"])
	assert.Equal(t, "customer", cs["submittedBy"])
	recent := cs["recentCommunications"].(map[string]any)["communications"].([]any)
	require.Len(t, recent, 1)
	assert.Equal(t, "Initial customer report", recent[0].(map[string]any)["body"])

	resolveRec := doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, resolveRec.Code)
	addRec := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId": caseID, "communicationBody": "Issue persists",
	})
	require.Equal(t, http.StatusOK, addRec.Code)
	resolveAgain := doSupportRequest(t, h, "ResolveCase", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, resolveAgain.Code)
	assert.Equal(t, "reopened", decodeSupportResponse(t, resolveAgain)["initialCaseStatus"])
}

func TestAccuracy_DescribeCasesPaginationAndValidation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	for i := range 11 {
		createAccurateCase(t, h, map[string]any{"subject": "Case " + string(rune('A'+i))})
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "first page emits token", body: map[string]any{"maxResults": 10},
			wantCode: http.StatusOK, wantLen: 10,
		},
		{
			name: "invalid page size", body: map[string]any{"maxResults": 1},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid time filter", body: map[string]any{"afterTime": "not-time"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		rec := doSupportRequest(t, h, "DescribeCases", tt.body)
		require.Equal(t, tt.wantCode, rec.Code, tt.name)
		if tt.wantLen == 0 {
			continue
		}
		body := decodeSupportResponse(t, rec)
		assert.Len(t, body["cases"], tt.wantLen, tt.name)
		token := body["nextToken"].(string)
		second := doSupportRequest(t, h, "DescribeCases", map[string]any{"maxResults": 10, "nextToken": token})
		assert.Len(t, decodeSupportResponse(t, second)["cases"], 1, tt.name)
	}
}

func TestAccuracy_AttachmentSetConsumedIntoCommunication(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	setRec := doSupportRequest(t, h, "AddAttachmentsToSet", map[string]any{
		"attachments": []map[string]any{{"fileName": "trace.txt", "data": []byte("trace bytes")}},
	})
	require.Equal(t, http.StatusOK, setRec.Code)
	setID := decodeSupportResponse(t, setRec)["attachmentSetId"].(string)
	caseID := createAccurateCase(t, h, map[string]any{"attachmentSetId": setID})

	communications := doSupportRequest(t, h, "DescribeCommunications", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, communications.Code)
	comm := decodeSupportResponse(t, communications)["communications"].([]any)[0].(map[string]any)
	attachments := comm["attachmentSet"].([]any)
	require.Len(t, attachments, 1)
	attachmentID := attachments[0].(map[string]any)["attachmentId"].(string)
	attachmentRec := doSupportRequest(t, h, "DescribeAttachment", map[string]any{"attachmentId": attachmentID})
	require.Equal(t, http.StatusOK, attachmentRec.Code)

	reuse := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId": caseID, "communicationBody": "Reuse staged set", "attachmentSetId": setID,
	})
	assert.Equal(t, http.StatusNotFound, reuse.Code)
}

func TestAccuracy_AddAttachmentsRejectsRequestAtomically(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	h := support.NewHandler(b)
	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing filename after valid file",
			body: map[string]any{"attachments": []map[string]any{
				{"fileName": "good.txt", "data": []byte("good")},
				{"data": []byte("missing name")},
			}},
		},
		{
			name: "oversized file after valid file",
			body: map[string]any{"attachments": []map[string]any{
				{"fileName": "good.txt", "data": []byte("good")},
				{"fileName": "large.dat", "data": bytes.Repeat([]byte("x"), 5*1024*1024+1)},
			}},
		},
	}

	for _, tt := range tests {
		rec := doSupportRequest(t, h, "AddAttachmentsToSet", tt.body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		assert.Equal(t, 0, support.AttachmentCount(b), tt.name)
	}
}

func TestAccuracy_AddCommunicationValidationAndPagination(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	caseID := createAccurateCase(t, h, nil)
	for i := range 10 {
		rec := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
			"caseId": caseID, "communicationBody": "Message " + string(rune('A'+i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "paginated communications", body: map[string]any{"caseId": caseID, "maxResults": 10},
			wantCode: http.StatusOK, wantLen: 10,
		},
		{
			name:     "body too large",
			body:     map[string]any{"caseId": caseID, "communicationBody": strings.Repeat("x", 8001)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		action := "AddCommunicationToCase"
		if tt.wantLen > 0 {
			action = "DescribeCommunications"
		}
		rec := doSupportRequest(t, h, action, tt.body)
		require.Equal(t, tt.wantCode, rec.Code, tt.name)
		if tt.wantLen == 0 {
			continue
		}
		body := decodeSupportResponse(t, rec)
		assert.Len(t, body["communications"], tt.wantLen, tt.name)
		second := doSupportRequest(t, h, "DescribeCommunications", map[string]any{
			"caseId": caseID, "maxResults": 10, "nextToken": body["nextToken"],
		})
		assert.Len(t, decodeSupportResponse(t, second)["communications"], 1, tt.name)
	}
}

func TestAccuracy_TrustedAdvisorValidationAndRefreshLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "checks require language",
			action:   "DescribeTrustedAdvisorChecks",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown refresh check",
			action:   "RefreshTrustedAdvisorCheck",
			body:     map[string]any{"checkId": "unknown"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "summaries require ids",
			action:   "DescribeTrustedAdvisorCheckSummaries",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "result validates id",
			action:   "DescribeTrustedAdvisorCheckResult",
			body:     map[string]any{"checkId": "unknown"},
			wantCode: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doSupportRequest(t, newTestSupportHandler(t), tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}

	h := newTestSupportHandler(t)
	refresh := doSupportRequest(t, h, "RefreshTrustedAdvisorCheck", map[string]any{"checkId": "Pfx0RwqBli"})
	require.Equal(t, http.StatusOK, refresh.Code)
	for _, expected := range []string{"enqueued", "processing", "success"} {
		status := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckRefreshStatuses", map[string]any{
			"checkIds": []string{"Pfx0RwqBli"},
		})
		require.Equal(t, http.StatusOK, status.Code)
		statuses := decodeSupportResponse(t, status)["statuses"].([]any)
		assert.Equal(t, expected, statuses[0].(map[string]any)["status"])
	}
	result := doSupportRequest(t, h, "DescribeTrustedAdvisorCheckResult", map[string]any{
		"checkId": "Pfx0RwqBli", "language": "en",
	})
	assert.NotNil(t, decodeSupportResponse(t, result)["result"].(map[string]any)["categorySpecificSummary"])
}

func TestAccuracy_SeverityLocalizationAndResourceExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   map[string]any
		want   string
	}{
		{
			name:   "severity Japanese",
			action: "DescribeSeverityLevels",
			body:   map[string]any{"language": "ja"},
			want:   "一般的なガイダンス",
		},
		{
			name:   "severity English",
			action: "DescribeSeverityLevels",
			body:   map[string]any{"language": "en"},
			want:   "General guidance",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, newTestSupportHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			levels := decodeSupportResponse(t, rec)["severityLevels"].([]any)
			assert.Equal(t, tt.want, levels[0].(map[string]any)["name"])
		})
	}

	h := newTestSupportHandler(t)
	for _, tt := range []struct {
		body string
		want string
	}{
		{body: `{"checkId":"Pfx0RwqBli"}`, want: "Pfx0RwqBli"},
		{body: `{"attachmentId":"att-1"}`, want: "att-1"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			ctx := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(ctx))
		})
	}
}
