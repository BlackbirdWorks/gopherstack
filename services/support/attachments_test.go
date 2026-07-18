package support_test

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestSupport_AddAttachmentsToSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "new attachment set",
			body: map[string]any{
				"attachments": []any{map[string]any{"fileName": "new.txt", "data": []byte("data")}},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown existing set id",
			body: map[string]any{
				"attachmentSetId": "existing-set-id",
				"attachments":     []any{map[string]any{"fileName": "new.txt", "data": []byte("data")}},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "AddAttachmentsToSet", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeSupportResponse(t, rec)
				assert.NotEmpty(t, resp["attachmentSetId"])
				assert.NotEmpty(t, resp["expiryTime"])
			}
		})
	}
}

// TestSupport_AddAttachmentsToSet_RejectsEmptyData verifies that
// AddAttachmentsToSet rejects attachments with empty data.
// Real AWS requires each attachment to have non-empty base64 content;
// the emulator previously accepted zero-length data bytes.
func TestSupport_AddAttachmentsToSet_RejectsEmptyData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		attachments []map[string]any
		wantCode    int
	}{
		{
			name: "nil_data_rejected",
			attachments: []map[string]any{
				{"fileName": "empty.txt"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_data_rejected",
			attachments: []map[string]any{
				{"fileName": "empty.txt", "data": []byte{}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "non_empty_data_accepted",
			attachments: []map[string]any{
				{"fileName": "file.txt", "data": []byte("content")},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "AddAttachmentsToSet", map[string]any{
				"attachments": tt.attachments,
			})
			assert.Equal(t, tt.wantCode, rec.Code,
				"AddAttachmentsToSet status for case %q", tt.name)
		})
	}
}

// TestSupport_AttachmentSet_EvictionAtCap verifies the attachment set cap evicts oldest-expiry entries.
func TestSupport_AttachmentSet_EvictionAtCap(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()

	// Fill past the cap of 1000.
	for range 1001 {
		_, _, err := b.AddAttachmentsToSetWithAttachments("", []support.Attachment{
			{FileName: "f.txt", Data: []byte("x")},
		})
		require.NoError(t, err)
	}

	// Must still accept a new set without error.
	setID, expiry, err := b.AddAttachmentsToSetWithAttachments("", []support.Attachment{
		{FileName: "final.txt", Data: []byte("y")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, setID)
	assert.True(t, expiry.After(time.Now()))
}

func TestSupport_DescribeAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *support.InMemoryBackend)
		body         map[string]any
		name         string
		wantFileName string
		wantCode     int
	}{
		{
			name: "found",
			setup: func(b *support.InMemoryBackend) {
				b.AddAttachmentInternal(&support.Attachment{
					AttachmentID: "att-001",
					FileName:     "error.log",
					Data:         []byte("stack trace"),
				})
			},
			body:         map[string]any{"attachmentId": "att-001"},
			wantCode:     http.StatusOK,
			wantFileName: "error.log",
		},
		{
			name:     "not_found",
			setup:    nil,
			body:     map[string]any{"attachmentId": "att-missing"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_attachment_id",
			setup:    nil,
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := support.NewInMemoryBackend()
			h := support.NewHandler(b)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doSupportRequest(t, h, "DescribeAttachment", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantFileName != "" {
				resp := decodeSupportResponse(t, rec)
				att, ok := resp["attachment"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFileName, att["fileName"])
			}
		})
	}
}

// TestSupport_AttachmentSet_ConsumedIntoCommunication verifies a staged
// attachment set is consumed when used to create a case, and cannot be reused.
func TestSupport_AttachmentSet_ConsumedIntoCommunication(t *testing.T) {
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

// TestSupport_AddAttachmentsToSet_RejectsRequestAtomically verifies that a
// request with one invalid attachment among valid ones is rejected wholesale,
// with no partial writes to the backend.
func TestSupport_AddAttachmentsToSet_RejectsRequestAtomically(t *testing.T) {
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

// TestSupport_AddAttachmentInternal_DeepCopyData verifies data bytes are deep-copied.
func TestSupport_AddAttachmentInternal_DeepCopyData(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	data := []byte("original data")
	b.AddAttachmentInternal(&support.Attachment{
		AttachmentID: "att-deep-001",
		FileName:     "test.log",
		Data:         data,
	})

	// Mutate original data after adding.
	data[0] = 'X'

	a, err := b.DescribeAttachment("att-deep-001")
	require.NoError(t, err)
	assert.Equal(t, byte('o'), a.Data[0], "AddAttachmentInternal should deep-copy data bytes")
}
