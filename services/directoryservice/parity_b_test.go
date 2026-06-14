package directoryservice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParity_UnrecognizedOp_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		op          string
		wantCode    int
		wantInBody  string
	}{
		{
			name:       "completely_unknown_op",
			op:         "NonExistentOperation",
			wantCode:   http.StatusBadRequest,
			wantInBody: "InvalidRequestException",
		},
		{
			name:     "malformed_op_name",
			op:       "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "partial_op_name",
			op:       "CreateDirec",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}
