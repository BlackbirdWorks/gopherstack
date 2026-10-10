package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList_RegistrationStatusRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{name: "domains_missing", action: "ListDomains", body: map[string]any{}, wantCode: http.StatusBadRequest},
		{
			name: "domains_set", action: "ListDomains",
			body: map[string]any{"registrationStatus": "REGISTERED"}, wantCode: http.StatusOK,
		},
		{
			name: "workflow_types_missing", action: "ListWorkflowTypes",
			body: map[string]any{"domain": "d1"}, wantCode: http.StatusBadRequest,
		},
		{
			name: "activity_types_missing", action: "ListActivityTypes",
			body: map[string]any{"domain": "d1"}, wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d1"})
			rec := doSWFRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
