package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_ApplyEnvironmentManagedAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       "Version=2010-12-01&Action=ApplyEnvironmentManagedAction&EnvironmentName=my-env&ActionId=action-1",
			wantStatus: http.StatusOK,
			wantXML:    "ApplyEnvironmentManagedActionResponse",
		},
		{
			name:       "missing action id",
			body:       "Version=2010-12-01&Action=ApplyEnvironmentManagedAction&EnvironmentName=my-env",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}
