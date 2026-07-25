package support_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSupport_AddCommunicationToCase(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	createRec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject":           "Comm test",
		"communicationBody": "Initial message",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	createResp := decodeSupportResponse(t, createRec)
	caseID := createResp["caseId"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "add communication",
			body:     map[string]any{"caseId": caseID, "communicationBody": "Hello support"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing caseId",
			body:     map[string]any{"communicationBody": "Hello"},
			wantCode: http.StatusBadRequest,
		},
		{
			// CaseIdNotFound has no httpError override in the real service
			// model, so awsjson1.1's client-fault default (400) applies.
			name:     "unknown caseId",
			body:     map[string]any{"caseId": "nonexistent", "communicationBody": "Hello"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, h, "AddCommunicationToCase", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSupport_DescribeCommunications(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	createRec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject":           "Comm test",
		"communicationBody": "Initial message",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	createResp := decodeSupportResponse(t, createRec)
	caseID := createResp["caseId"].(string)

	addRec := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "Hello support",
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	tests := []struct {
		body          map[string]any
		name          string
		wantCode      int
		wantCommCount int
	}{
		{
			name:          "describe communications",
			body:          map[string]any{"caseId": caseID},
			wantCode:      http.StatusOK,
			wantCommCount: 2,
		},
		{
			name:     "missing caseId",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// CaseIdNotFound has no httpError override in the real service
			// model, so awsjson1.1's client-fault default (400) applies.
			name:     "unknown caseId",
			body:     map[string]any{"caseId": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSupportRequest(t, h, "DescribeCommunications", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCommCount > 0 {
				resp := decodeSupportResponse(t, rec)
				comms, ok := resp["communications"].([]any)
				require.True(t, ok)
				assert.Len(t, comms, tt.wantCommCount)
			}
		})
	}
}

func TestSupport_AddCommunicationToCase_ValidationAndPagination(t *testing.T) {
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

// TestSupport_AddCommunicationToCase_EmptyBody verifies empty body is rejected.
func TestSupport_AddCommunicationToCase_EmptyBody(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Comm body test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeSupportResponse(t, rec)
	caseID := createResp["caseId"].(string)

	rec2 := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestSupport_AddCommunicationToCase_AttachmentSetID verifies attachmentSetId is stored.
func TestSupport_AddCommunicationToCase_AttachmentSetID(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Attach test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeSupportResponse(t, rec)
	caseID := createResp["caseId"].(string)

	setRec := doSupportRequest(t, h, "AddAttachmentsToSet", map[string]any{
		"attachments": []map[string]any{{"fileName": "detail.txt", "data": []byte("details")}},
	})
	require.Equal(t, http.StatusOK, setRec.Code)
	setResp := decodeSupportResponse(t, setRec)

	rec2 := doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "See attached",
		"attachmentSetId":   setResp["attachmentSetId"],
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Verify through DescribeCommunications.
	rec3 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec3.Code)

	resp := decodeSupportResponse(t, rec3)
	comms := resp["communications"].([]any)
	require.Len(t, comms, 2)
	comm := comms[0].(map[string]any)
	assert.NotEmpty(t, comm["attachmentSet"])
}

// TestSupport_DescribeCommunications_TimeCreated verifies timeCreated in communication view.
func TestSupport_DescribeCommunications_TimeCreated(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "TimeCreated test", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeSupportResponse(t, rec)
	caseID := createResp["caseId"].(string)

	doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "Test body",
	})

	rec2 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{"caseId": caseID})
	require.Equal(t, http.StatusOK, rec2.Code)

	resp := decodeSupportResponse(t, rec2)
	comms := resp["communications"].([]any)
	require.Len(t, comms, 2)
	comm := comms[0].(map[string]any)
	assert.NotEmpty(t, comm["timeCreated"])
}

// TestSupport_DescribeCommunications_MissingCaseID verifies validation.
func TestSupport_DescribeCommunications_MissingCaseID(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "DescribeCommunications", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSupport_DescribeCommunications_Pagination verifies nextToken/maxResults accepted.
func TestSupport_DescribeCommunications_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	rec := doSupportRequest(
		t,
		h,
		"CreateCase",
		map[string]any{"subject": "Comm pagination", "communicationBody": "Initial"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	createResp := decodeSupportResponse(t, rec)
	caseID := createResp["caseId"].(string)

	doSupportRequest(t, h, "AddCommunicationToCase", map[string]any{
		"caseId":            caseID,
		"communicationBody": "First message",
	})

	rec2 := doSupportRequest(t, h, "DescribeCommunications", map[string]any{
		"caseId":     caseID,
		"maxResults": 10,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}
