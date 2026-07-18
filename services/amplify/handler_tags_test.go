package amplify_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "tags_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusNotFound,
		},
		{
			// body is a JSON string (not an object) — wrong type/shape, not syntax error
			name: "wrong_type_body_returns_400",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			body:       "not-an-object",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			rec := doRequest(t, h, http.MethodPost, "/tags/"+encodedARN, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource_MalformedJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	app, _ := b.CreateApp("TestApp", "", "", "", nil)
	encodedARN := encodeARN(app.ARN)
	rec := doRawRequest(t, h, http.MethodPost, "/tags/"+encodedARN, []byte(malformedJSON))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*amplify.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "returns_tags_for_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			rec := doRequest(t, h, http.MethodGet, "/tags/"+encodedARN, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*amplify.InMemoryBackend) string
		tagKeys    string
		wantStatus int
	}{
		{
			name: "removes_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			tagKeys:    "env",
			wantStatus: http.StatusOK,
		},
		{
			name: "returns_404_for_missing_resource",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagKeys:    "env",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			resourceARN := tt.setup(b)
			encodedARN := encodeARN(resourceARN)
			path := "/tags/" + encodedARN + "?tagKeys=" + tt.tagKeys
			rec := doRequest(t, h, http.MethodDelete, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
