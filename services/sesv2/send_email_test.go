package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

func TestSESv2Backend_SendEmailCap(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()

	_, err := b.CreateEmailIdentity("a@example.com", "", nil)
	require.NoError(t, err)

	// Send beyond 2x the cap so the amortized compaction path runs at least
	// once. After compaction the slice length must stay between
	// maxRetainedEmails and 2*maxRetainedEmails.
	total := sesv2.EmailCompactionHighWater + 5
	for i := range total {
		_, sendErr := b.SendEmail("a@example.com", []string{"b@example.com"},
			"s", "h", "t")
		require.NoError(t, sendErr, "iteration %d", i)
	}

	got := sesv2.EmailCount(b)
	assert.GreaterOrEqual(t, got, sesv2.MaxRetainedEmails,
		"retain at least the cap")
	assert.LessOrEqual(t, got, sesv2.EmailCompactionHighWater,
		"never exceed the compaction high-water mark")
}

// TestSendEmailRequiresDestination verifies that SendEmail rejects requests with no
// destination addresses. Real AWS requires at least one ToAddress, CcAddress, or
// BccAddress; the emulator previously sent the email silently with an empty
// destination list.
func TestSendEmailRequiresDestination(t *testing.T) {
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
			doRequest(t, h, http.MethodPost, "/v2/email/identities",
				map[string]any{"EmailIdentity": "sender@example.com"})
			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"SendEmail status for case %q", tt.name)
		})
	}
}

// TestSendBulkEmail tests the SendBulkEmail operation.
func TestSendBulkEmail(t *testing.T) {
	t.Parallel()

	h, b := newSESv2TestHandler(t)
	_, err := b.CreateEmailIdentity("bulk@example.com", "", nil)
	require.NoError(t, err)

	doReq(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "BulkTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello {{name}}",
			"Html":    "<html>Hi {{name}}</html>",
		},
	})

	rec := doReq(t, h, http.MethodPost, "/v2/email/outbound-bulk-emails", map[string]any{
		"FromEmailAddress": "bulk@example.com",
		"DefaultContent": map[string]any{
			"Template": map[string]any{
				"TemplateName": "BulkTemplate",
				"TemplateData": `{"name":"default"}`,
			},
		},
		"BulkEmailEntries": []map[string]any{
			{
				"Destination": map[string]any{
					"ToAddresses": []string{"to1@example.com"},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSendEmail tests the SendEmail operation via HTTP.
func TestSendEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "sends email successfully",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination": map[string]any{
					"ToAddresses": []string{"recipient@example.com"},
				},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body": map[string]any{
							"Text": map[string]any{"Data": "Hello World"},
							"Html": map[string]any{"Data": "<b>Hello World</b>"},
						},
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing from address returns bad request",
			body: map[string]any{
				"Destination": map[string]any{
					"ToAddresses": []string{"recipient@example.com"},
				},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body":    map[string]any{},
					},
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode == http.StatusOK {
				doRequest(t, h, http.MethodPost, "/v2/email/identities",
					map[string]any{"EmailIdentity": "sender@example.com"})
			}

			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["MessageId"])

				emails := h.Backend.ListEmails()
				require.Len(t, emails, 1)
				assert.Equal(t, "sender@example.com", emails[0].From)
			}
		})
	}
}
