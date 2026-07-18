package rolesanywhere_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---- Subject HTTP ----

func TestHandler_Subject_GetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{"get nonexistent subject → 404", "/subject/no-such-subject", http.StatusNotFound},
		{"list subjects empty → 200", "/subjects", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
