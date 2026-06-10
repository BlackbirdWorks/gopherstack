package account_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_PutAlternateContact_RequiredFields verifies PutAlternateContact
// rejects a request missing any required field (AWS requires
// AlternateContactType, EmailAddress, Name, PhoneNumber, Title).
func TestParity_PutAlternateContact_RequiredFields(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"AlternateContactType": "BILLING",
		"EmailAddress":         "ops@example.com",
		"Name":                 "Ops Team",
		"PhoneNumber":          "+1-555-0100",
		"Title":                "Operations",
	}

	tests := []struct {
		name       string
		omit       string
		wantStatus int
	}{
		{name: "complete_ok", omit: "", wantStatus: http.StatusOK},
		{name: "missing_type", omit: "AlternateContactType", wantStatus: http.StatusBadRequest},
		{name: "missing_email", omit: "EmailAddress", wantStatus: http.StatusBadRequest},
		{name: "missing_name", omit: "Name", wantStatus: http.StatusBadRequest},
		{name: "missing_phone", omit: "PhoneNumber", wantStatus: http.StatusBadRequest},
		{name: "missing_title", omit: "Title", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := make(map[string]any, len(full))
			for k, v := range full {
				body[k] = v
			}
			if tt.omit != "" {
				delete(body, tt.omit)
			}

			rec := doRequest(t, h, http.MethodPut, "/account/alternateContact", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
