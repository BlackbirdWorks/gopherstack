package ssm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_PutParameterRequiresValidType verifies that PutParameter rejects
// requests with a missing or invalid Type. Real AWS requires Type to be one of
// String, StringList, or SecureString; the emulator previously accepted any
// string value, including the empty string.
func TestParity_PutParameterRequiresValidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_type_rejected",
			body:     `{"Name":"my-param","Value":"val"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_type_rejected",
			body:     `{"Name":"my-param","Value":"val","Type":""}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_type_rejected",
			body:     `{"Name":"my-param","Value":"val","Type":"BadType"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "string_type_accepted",
			body:     `{"Name":"my-param","Value":"val","Type":"String"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "stringlist_type_accepted",
			body:     `{"Name":"my-list","Value":"a,b","Type":"StringList"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "securestring_type_accepted",
			body:     `{"Name":"my-secret","Value":"pw","Type":"SecureString"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "PutParameter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"PutParameter status for case %q", tt.name)
		})
	}
}
