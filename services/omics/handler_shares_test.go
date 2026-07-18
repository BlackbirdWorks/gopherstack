package omics_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOmics_Share(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "CreateShare returns 201",
			method: http.MethodPost,
			path:   "/share",
			body: map[string]any{
				"resourceArn":         "arn:aws:omics:us-east-1:000000000000:annotationStore/mystore",
				"principalSubscriber": "123456789012",
				"shareName":           "my-share",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetShare unknown returns 404",
			method:   http.MethodGet,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteShare unknown returns 404",
			method:   http.MethodDelete,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
