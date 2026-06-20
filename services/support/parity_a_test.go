package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_AddAttachmentsToSetRequiresNonEmptyData verifies that
// AddAttachmentsToSet rejects attachments with empty data.
// Real AWS requires each attachment to have non-empty base64 content;
// the emulator previously accepted zero-length data bytes.
func TestParity_AddAttachmentsToSetRequiresNonEmptyData(t *testing.T) {
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
