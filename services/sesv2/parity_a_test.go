package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_SendEmailRequiresDestination verifies that SendEmail rejects
// requests with no destination addresses. Real AWS requires at least one
// ToAddress, CcAddress, or BccAddress; the emulator previously sent the
// email silently with an empty destination list.
func TestParity_SendEmailRequiresDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "absent_destination_rejected",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body":    map[string]any{"Text": map[string]any{"Data": "body"}},
					},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_destination_rejected",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination": map[string]any{
					"ToAddresses":  []string{},
					"CcAddresses":  []string{},
					"BccAddresses": []string{},
				},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body":    map[string]any{"Text": map[string]any{"Data": "body"}},
					},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "to_address_accepted",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination":      map[string]any{"ToAddresses": []string{"rcpt@example.com"}},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body":    map[string]any{"Text": map[string]any{"Data": "body"}},
					},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"SendEmail status for case %q", tt.name)
		})
	}
}
